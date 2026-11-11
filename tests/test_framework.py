"""Safety, causality, budget, and paper-accounting regression checks."""

from __future__ import annotations

import copy
import fcntl
import io
import json
import sqlite3
import tempfile
import unittest
import urllib.request
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from decimal import Decimal
from http.client import HTTPMessage
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from papertrade.__main__ import main
from papertrade.ai import Reviewer, parse_codex_events, sandbox_profile, validate_decisions
from papertrade.api import (
    MarketClient,
    NoRedirect,
    private_json,
    secret_file,
    validate_request,
)
from papertrade.core import (
    NEW_YORK,
    UTC,
    SafetyError,
    Session,
    Settings,
    decimal,
    market_date,
    probable_regular,
    regular_session,
    stamp,
)
from papertrade.engine import Engine
from papertrade.store import Store
from papertrade.strategy import (
    Bar,
    aggregate,
    atr,
    book_prices,
    eligible,
    entry_plan,
    exit_reason,
    fill_price,
    signal,
)
from papertrade.usage import quota_report, select_model

ROOT = Path(__file__).resolve().parent.parent
AT = datetime(2026, 11, 2, 16, 0, tzinfo=UTC)


def settings(root: Path) -> Settings:
    data = copy.deepcopy(Settings.load(ROOT / "tests" / "fixtures" / "v2").data)
    data["strategy"].update(
        profile="v1",
        bar_minutes=30,
        breakout_bars=6,
        relative_minutes=60,
        volume_multiple="1.5",
        chase_atr="0.5",
        max_spread_fraction="0.0015",
        time_exit_minutes=120,
        entry_cutoff_minutes=60,
    )
    return Settings(root, data)


def book(at: datetime = AT, bid: str = "100.1", ask: str = "100.2", volume: str = "100") -> dict:
    return {
        "timestamp": at.isoformat(),
        "currency": "USD",
        "bids": [{"price": bid, "volume": volume}],
        "asks": [{"price": ask, "volume": volume}],
    }


def candidate() -> dict:
    return {
        "symbol": "TEST",
        "market": "US",
        "signal_key": "v1:test:bar",
        "bar_at": (AT - timedelta(minutes=30)).isoformat(),
        "atr": "1",
        "support": "98",
        "breakout": "100",
        "volume_ratio": "2",
        "relative_return": "0.01",
        "observed_at": AT.isoformat(),
    }


class BaseCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.settings = settings(self.root)
        self.store = Store(self.root / "state" / "paper.sqlite3")

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def position(self) -> dict:
        plan = entry_plan(
            candidate(),
            book(),
            AT,
            Decimal(1000),
            Decimal(1000),
            Decimal(1),
            self.settings,
            quantity=2,
        )
        plan.update(entered_at=AT.isoformat(), session_date="2026-11-02")
        return plan


class BoundaryTests(BaseCase):
    def test_negative_paper_fees_are_rejected(self) -> None:
        source = (ROOT / "tests" / "fixtures" / "v2" / "settings.toml").read_text()
        (self.root / "settings.toml").write_text(
            source.replace('us_fee_bps = "10"', 'us_fee_bps = "-1"')
        )
        with self.assertRaises(SafetyError):
            Settings.load(self.root)

    def test_codex_metadata_is_not_misclassified_as_an_action(self) -> None:
        events = (
            '{"type":"item.completed","item":{"type":"plan","steps":[]}}\n'
            '{"type":"item.completed","item":{"type":"agent_message","text":"{}"}}\n'
            '{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":10}}'
        )
        usage, text, actions = parse_codex_events(events)
        self.assertEqual(text, "{}")
        assert usage is not None
        self.assertEqual(usage["input_tokens"], 100)
        self.assertEqual(actions, [])

    def test_codex_tool_actions_remain_rejected(self) -> None:
        _usage, _text, actions = parse_codex_events(
            '{"type":"item.completed","item":{"type":"command_execution"}}'
        )
        self.assertEqual(actions, ["command_execution"])

    def test_order_related_paths_are_rejected_before_any_network(self) -> None:
        client = MarketClient(self.settings)
        client.opener = MagicMock()
        for method, path in [
            ("GET", "/api/v1/accounts"),
            ("GET", "/api/v1/holdings"),
            ("GET", "/api/v1/orders"),
            ("POST", "/api/v1/orders"),
            ("DELETE", "/api/v1/orders/1"),
            ("GET", "/api/v1/conditional-orders"),
            ("GET", "/api/v1/order-info"),
            ("POST", "/api/v1/orderbook"),
            ("GET", "/api/v1/stocks/../orders/warnings"),
            ("GET", "/api/v1/stocks/%2e%2e/warnings"),
            ("GET", "https://another-host.example/api/v1/prices"),
            ("GET", "/api/v1/prices?redirect=/orders"),
        ]:
            with self.subTest(method=method, path=path), self.assertRaises(SafetyError):
                client._request(method, path)
        client.opener.open.assert_not_called()

    def test_only_auth_post_and_market_gets_are_allowed(self) -> None:
        for method, path in [
            ("POST", "/oauth2/token"),
            ("GET", "/api/v1/prices"),
            ("GET", "/api/v1/orderbook"),
            ("GET", "/api/v1/stocks/NVDA/warnings"),
        ]:
            validate_request(method, path)

    def test_redirects_cannot_forward_credentials(self) -> None:
        handler: urllib.request.HTTPRedirectHandler = NoRedirect()
        request = urllib.request.Request("https://fixture.invalid")
        with io.BytesIO() as response:
            redirect = handler.redirect_request(
                request, response, 302, "", HTTPMessage(), "https://x"
            )
        self.assertIsNone(redirect)

    def test_secret_mode_and_symlink_checks(self) -> None:
        path = self.root / "secret"
        path.write_text("fixture-secret\n")
        path.chmod(0o600)
        self.assertEqual(secret_file(path), "fixture-secret")
        path.chmod(0o644)
        with self.assertRaises(SafetyError):
            secret_file(path)
        link = self.root / "link"
        link.symlink_to(path)
        with self.assertRaises(SafetyError):
            secret_file(link)

    def test_private_cache_has_mode_600(self) -> None:
        path = self.root / "token.json"
        private_json(path, {"access_token": "fixture"})
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_non_finite_numbers_are_rejected(self) -> None:
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.assertRaises(SafetyError):
                decimal(value)

    def test_sandbox_protects_secret_and_state_paths(self) -> None:
        profile = sandbox_profile(
            Path("/usr/bin/true"), [self.root / "secrets", self.root / "state"]
        )
        self.assertIn("(deny process-exec)", profile)
        self.assertEqual(profile.count("(deny file-read*"), 2)


class StrategyTests(BaseCase):
    def test_incomplete_or_future_minutes_do_not_form_a_bar(self) -> None:
        start = AT - timedelta(minutes=30)
        session = Session("US", "2026-11-02", start, AT + timedelta(hours=1))
        rows = [
            {
                "timestamp": (start + timedelta(minutes=i)).isoformat(),
                "openPrice": "100",
                "highPrice": "101",
                "lowPrice": "99",
                "closePrice": "100",
                "volume": "10",
            }
            for i in range(30)
        ]
        self.assertEqual(len(aggregate(rows, [session], AT)), 1)
        self.assertEqual(aggregate(rows[:-1], [session], AT), [])
        self.assertEqual(aggregate(rows, [session], AT - timedelta(seconds=1)), [])

    def test_atr_includes_the_overnight_gap(self) -> None:
        first = Bar(AT, Decimal(100), Decimal(101), Decimal(99), Decimal(100), Decimal(10))
        second = Bar(
            AT + timedelta(minutes=30),
            Decimal(110),
            Decimal(111),
            Decimal(109),
            Decimal(110),
            Decimal(10),
        )
        self.assertEqual(atr([first, second], 1), 11)

    def test_only_a_completed_breakout_with_volume_and_relative_strength_passes(self) -> None:
        bars = [
            Bar(
                AT - timedelta(minutes=30 * (16 - i)),
                Decimal(99),
                Decimal(100),
                Decimal(98),
                Decimal(99),
                Decimal(100),
            )
            for i in range(15)
        ]
        bars.append(
            Bar(
                AT - timedelta(minutes=30),
                Decimal(99),
                Decimal(102),
                Decimal(99),
                Decimal(101),
                Decimal(200),
            )
        )
        result = signal("TEST", "US", bars, Decimal(0), AT, self.settings)
        self.assertIsNotNone(result)
        self.assertIsNone(signal("TEST", "US", bars, Decimal("0.10"), AT, self.settings))
        bars[-1] = Bar(
            bars[-1].at, Decimal(99), Decimal(102), Decimal(99), Decimal(101), Decimal(100)
        )
        self.assertIsNone(signal("TEST", "US", bars, Decimal(0), AT, self.settings))

    def test_stale_future_and_crossed_books_are_rejected(self) -> None:
        for data in [
            book(AT - timedelta(seconds=31)),
            book(AT + timedelta(seconds=6)),
            book(bid="101", ask="100"),
        ]:
            with self.assertRaises(SafetyError):
                book_prices(data, AT, 30, "USD")

    def test_depth_is_not_invented(self) -> None:
        with self.assertRaises(SafetyError):
            fill_price(book(volume="1"), 2, buying=True)
        self.assertEqual(fill_price(book(), 2, buying=False), Decimal("100.1"))

    def test_ai_size_can_exceed_old_allocation_and_risk_caps_but_not_cash(self) -> None:
        position = entry_plan(
            candidate(),
            book(),
            AT,
            Decimal(1000),
            Decimal(1000),
            Decimal(1),
            self.settings,
            quantity=9,
        )
        self.assertEqual(position["quantity"], 9)
        self.assertGreater(decimal(position["cost_usd"]), 300)
        self.assertLessEqual(decimal(position["cost_usd"]), 1000)
        self.assertGreater(decimal(position["planned_risk_usd"]), 10)
        self.assertEqual(decimal(position["stop"]), 98)
        self.assertEqual(decimal(position["target"]), Decimal("104.6"))
        with self.assertRaises(SafetyError):
            entry_plan(
                candidate(), book(), AT, Decimal(1000), Decimal(10), Decimal(1), self.settings
            )

    def test_chasing_failed_breakouts_and_wide_spreads_are_rejected(self) -> None:
        for data in [
            book(bid="101.9", ask="102"),
            book(bid="99.8", ask="99.9"),
            book(bid="100", ask="100.4"),
        ]:
            with self.assertRaises(SafetyError):
                entry_plan(
                    candidate(), data, AT, Decimal(1000), Decimal(1000), Decimal(1), self.settings
                )

    def test_exits_use_observed_bid_not_intracheck_high_or_stop_price(self) -> None:
        position = self.position()
        session = Session("US", "2026-11-02", AT - timedelta(hours=1), AT + timedelta(hours=4))
        self.assertEqual(
            exit_reason(position, Decimal(90), AT, session, self.settings), "stop_at_observed_quote"
        )
        self.assertIsNone(exit_reason(position, Decimal(103), AT, session, self.settings))
        self.assertEqual(
            exit_reason(position, Decimal(106), AT, session, self.settings),
            "target_at_observed_quote",
        )

    def test_halted_or_closed_session_cannot_be_filled(self) -> None:
        session = Session("US", "2026-11-02", AT + timedelta(hours=1), AT + timedelta(hours=5))
        self.assertIsNone(exit_reason(self.position(), Decimal(90), AT, session, self.settings))

    def test_time_and_session_close_exits(self) -> None:
        session = Session("US", "2026-11-02", AT - timedelta(hours=1), AT + timedelta(hours=4))
        self.assertEqual(
            exit_reason(
                self.position(), Decimal("100.5"), AT + timedelta(hours=2), session, self.settings
            ),
            "time_exit",
        )
        self.assertEqual(
            exit_reason(
                self.position(),
                Decimal(102),
                session.end - timedelta(minutes=15),
                session,
                self.settings,
            ),
            "session_end",
        )

    def test_tradeable_symbols_have_no_price_floor_or_leverage_exclusion(self) -> None:
        stock: dict[str, str | None] = {
            "status": "ACTIVE",
            "currency": "USD",
            "securityType": "ETF",
            "leverageFactor": "2",
        }
        self.assertTrue(
            eligible(stock, Decimal(10), "US", Decimal(1000), Decimal(1), self.settings)
        )
        stock["leverageFactor"] = None
        self.assertTrue(
            eligible(stock, Decimal(10), "US", Decimal(1000), Decimal(1), self.settings)
        )
        stock["leverageFactor"] = "1"
        self.assertTrue(
            eligible(stock, Decimal(500), "US", Decimal(1000), Decimal(1), self.settings)
        )
        self.assertTrue(
            eligible(stock, Decimal("0.5"), "US", Decimal(1000), Decimal(1), self.settings)
        )
        self.assertFalse(
            eligible(stock, Decimal(1001), "US", Decimal(1000), Decimal(1), self.settings)
        )


class LedgerTests(BaseCase):
    def test_unlimited_ai_can_exceed_the_old_call_and_token_caps(self) -> None:
        for number in range(7):
            call = self.store.reserve_ai(str(number), AT)
            self.assertIsNotNone(call)
            self.store.finish_ai(
                call or 0, "completed", {"input_tokens": 30000, "output_tokens": 5000}, {}
            )
        self.assertEqual(len(self.store.summary(AT)["ai_calls"]), 7)

    def test_unlimited_ai_does_not_turn_a_failed_request_into_a_permanent_resource_lock(
        self,
    ) -> None:
        call = self.store.reserve_ai("first", AT)
        self.store.finish_ai(call or 0, "failed", None, {})
        self.assertIsNotNone(self.store.reserve_ai("second", AT))

    def test_only_duplicate_ai_events_are_rejected(self) -> None:
        call = self.store.reserve_ai("first", AT)
        self.assertIsNotNone(self.store.reserve_ai("second", AT))
        self.store.finish_ai(call or 0, "completed", {"input_tokens": 1}, {})
        self.assertIsNone(self.store.reserve_ai("first", AT))

    def test_initial_capital_is_inserted_only_once(self) -> None:
        second = Store(self.root / "state" / "paper.sqlite3")
        self.assertEqual(second.total_cash(), 1000)
        second.close()

    def test_duplicate_buys_roll_back_cash(self) -> None:
        position = self.position()
        cost = decimal(position["cost_usd"])
        self.store.buy(position, cost, "entry:1", AT)
        cash = self.store.total_cash()
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.buy(position, cost, "entry:1", AT)
        self.assertEqual(self.store.total_cash(), cash)
        self.assertEqual(len(self.store.positions()), 1)

    def test_sale_proceeds_keep_withdrawal_dates_without_blocking_buying_power(self) -> None:
        position = self.position()
        self.store.buy(position, decimal(position["cost_usd"]), "entry:1", AT)
        settled = self.store.settled_cash(AT)
        self.store.sell("TEST", Decimal(210), Decimal(105), "exit:1", "target", AT, "2026-11-02")
        self.assertEqual(self.store.settled_cash(AT), settled)
        self.assertEqual(self.store.total_cash(), settled + 210)
        release = AT + timedelta(days=1)
        self.store.db.execute(
            "UPDATE cash SET available_at=? WHERE source='exit:1'", (release.isoformat(),)
        )
        self.assertEqual(self.store.settled_cash(AT), settled)
        self.assertEqual(self.store.settled_cash(release), settled + 210)

    def test_cannot_short_or_duplicate_a_sale(self) -> None:
        with self.assertRaises(SafetyError):
            self.store.sell(
                "NONE", Decimal(100), Decimal(100), "exit:none", "stop", AT, "2026-11-02"
            )

    def test_signal_and_tick_deduplication(self) -> None:
        self.assertTrue(self.store.claim_signal("same", {}))
        self.assertFalse(self.store.claim_signal("same", {}))
        self.assertTrue(self.store.claim_tick(AT))
        self.assertFalse(self.store.claim_tick(AT + timedelta(seconds=30)))
        self.assertTrue(self.store.claim_tick(AT + timedelta(minutes=1)))

    def test_ai_json_cannot_select_unknown_symbols_or_commands(self) -> None:
        for payload in [
            {"decisions": [{"symbol": "OTHER", "action": "BUY", "reason": "x"}]},
            {"decisions": [{"symbol": "TEST", "action": "SELL", "reason": "x"}]},
            {"decisions": [{"symbol": "TEST", "action": "BUY", "reason": "x", "command": "x"}]},
        ]:
            with self.assertRaises(SafetyError):
                validate_decisions(payload, {"TEST"})


class UsageTests(BaseCase):
    def test_status_remains_readable_while_a_trading_check_holds_the_run_lock(self) -> None:
        (self.root / "settings.toml").write_text(
            (ROOT / "tests" / "fixtures" / "v2" / "settings.toml").read_text()
        )
        output = io.StringIO()
        with (self.root / "state" / "run.lock").open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with (
                patch("sys.argv", ["papertrade", "--root", str(self.root), "status"]),
                redirect_stdout(output),
            ):
                self.assertEqual(main(), 0)
        self.assertEqual(json.loads(output.getvalue())["equity_usd"], "1000")

    def catalog(self) -> list[dict]:
        return [
            {"model": model, "efforts": ["low", "medium", "high"], "default_effort": "medium"}
            for model in ("gpt-6-luna", "gpt-6.1-sol", "gpt-6-astra")
        ]

    def test_resource_caps_are_absent_from_configuration(self) -> None:
        loaded = Settings.load(ROOT)
        for key in (
            "daily_ai_calls",
            "daily_reported_tokens",
            "ai_timeout_seconds",
            "ai_input_bytes",
        ):
            self.assertNotIn(key, loaded.data["limits"])

    def test_quota_reports_all_buckets_and_does_not_invent_remaining_tokens(self) -> None:
        result = quota_report(
            {
                "rateLimitsByLimitId": {
                    "codex": {
                        "primary": {
                            "usedPercent": 2,
                            "windowDurationMins": 10080,
                            "resetsAt": 1793776181,
                        }
                    },
                    "other": {
                        "primary": {"usedPercent": 40, "windowDurationMins": 300},
                        "secondary": {"usedPercent": None},
                    },
                }
            }
        )
        self.assertEqual([row["remaining_percent"] for row in result["windows"]], [98, 60, None])
        self.assertIsNone(result["exact_remaining_tokens"])
        self.assertTrue(result["windows"][0]["resets_at_seoul"].endswith("+09:00"))

    def test_quota_falls_back_to_documented_legacy_bucket(self) -> None:
        result = quota_report(
            {
                "rateLimits": {
                    "limitId": "codex",
                    "primary": {"usedPercent": 5, "windowDurationMins": 300},
                }
            }
        )
        self.assertEqual(result["windows"][0]["remaining_percent"], 95)

    def test_missing_quota_is_unknown_rather_than_zero_usage(self) -> None:
        result = quota_report({})
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["windows"], [])
        self.assertIsNone(result["exact_remaining_tokens"])

    def test_light_test_standard_review_and_complex_review_routes(self) -> None:
        row = candidate()
        row["entry_price"] = "100"
        args: tuple[list[dict[str, Any]], bool, str, dict[str, Any]] = (
            self.catalog(), False, "auto", {}
        )
        self.assertEqual(
            select_model([row], self.catalog(), True, "auto", {})["model"], "gpt-6-luna"
        )
        self.assertEqual(select_model([row], *args)["model"], "gpt-6.1-sol")
        row["atr"] = "3"
        self.assertEqual(select_model([row], *args)["model"], "gpt-6-astra")
        row["atr"] = "1"
        second = {**row, "symbol": "OTHER", "relative_return": "0.011"}
        self.assertEqual(select_model([row, second], *args)["reason"], "closely_ranked_candidates")

    def test_explicit_model_and_effort_are_verified_against_live_catalog(self) -> None:
        result = select_model([], self.catalog(), True, "auto", {}, "gpt-6-astra", "high")
        self.assertEqual(result["model"], "gpt-6-astra")
        with self.assertRaises(SafetyError):
            select_model([], self.catalog(), True, "auto", {}, "unknown-model", "low")
        with self.assertRaises(SafetyError):
            select_model([], self.catalog(), True, "auto", {}, "gpt-6-luna", "ultra")

    def test_current_model_policy_preserves_saved_settings(self) -> None:
        result = select_model(
            [],
            self.catalog(),
            True,
            "current",
            {"model": "gpt-6-astra", "model_reasoning_effort": "high"},
        )
        self.assertEqual(
            result,
            {"model": "gpt-6-astra", "effort": "high", "reason": "existing_user_configuration"},
        )

    def test_unlimited_review_records_usage_quota_and_model_without_input_or_timeout_caps(
        self,
    ) -> None:
        reviewer = Reviewer(self.settings, self.store)
        metadata = {
            "quota": {"status": "available", "windows": [{"remaining_percent": 98}]},
            "models": self.catalog(),
        }
        process = MagicMock()
        process.returncode = 0
        process.pid = 12345
        answer = {
            "decisions": [
                {"symbol": "SMOKE", "action": "SKIP", "quantity": 0, "reason": "Test only"}
            ]
        }
        process.communicate.return_value = (
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": json.dumps(answer)},
                }
            )
            + "\n"
            + json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {
                        "input_tokens": 22000,
                        "output_tokens": 100,
                        "cached_input_tokens": 1000,
                    },
                }
            ),
            "",
        )
        with (
            patch.object(reviewer, "metadata", return_value=metadata),
            patch("papertrade.ai.subprocess.Popen", return_value=process) as launch,
            patch("papertrade.ai.sys.stderr"),
            patch("papertrade.ai.process_identity", return_value="fixture-identity"),
            patch.object(
                reviewer,
                "stream",
                return_value=(
                    {"input_tokens": 22000, "output_tokens": 100, "cached_input_tokens": 1000},
                    json.dumps(answer),
                    [],
                ),
            ) as stream,
        ):
            result = reviewer.review(
                [{"symbol": "SMOKE", "signal_key": "synthetic:1", "purpose": "x" * 20000}],
                AT,
                smoke=True,
            )
        self.assertEqual(result["SMOKE"]["action"], "SKIP")
        self.assertNotIn("timeout", stream.call_args.kwargs)
        self.assertIn("gpt-6-luna", launch.call_args.args[0])
        row = self.store.db.execute("SELECT data,input_tokens FROM ai_calls").fetchone()
        detail = json.loads(row[0])
        self.assertEqual(row[1], 22000)
        self.assertEqual(detail["selection"]["model"], "gpt-6-luna")
        self.assertEqual(detail["quota_after"]["windows"][0]["remaining_percent"], 98)
        self.assertEqual(self.store.positions(), [])


class ScheduleTests(BaseCase):
    def test_delayed_korean_close_is_checked_using_the_market_calendar(self) -> None:
        at = stamp("2026-11-02T16:15:00+09:00")
        calendar = {
            "today": {
                "date": "2026-11-02",
                "integrated": {
                    "regularMarket": {
                        "startTime": "2026-11-02T10:00:00+09:00",
                        "endTime": "2026-11-02T16:30:00+09:00",
                    }
                },
            }
        }
        engine = Engine(self.settings, self.store, MagicMock(), MagicMock())
        with (
            patch.object(engine, "calendar", return_value=calendar),
            patch.object(engine, "inspect_positions") as inspect,
            patch.object(engine, "resolve_settlements"),
            patch("papertrade.engine.now_utc", return_value=at),
        ):
            result = engine.tick(at)
        self.assertEqual(result["markets"], ["KR"])
        inspect.assert_not_called()

    def test_lookup_window_before_open_still_runs_hourly_off_hours_observation(self) -> None:
        at = stamp("2026-11-02T08:00:00-05:00")
        calendar = {
            "today": {
                "date": "2026-11-02",
                "regularMarket": {
                    "startTime": "2026-11-02T09:30:00-05:00",
                    "endTime": "2026-11-02T16:00:00-05:00",
                },
            }
        }
        reviewer = MagicMock()
        engine = Engine(self.settings, self.store, MagicMock(), reviewer)
        with (
            patch.object(engine, "calendar", return_value=calendar),
            patch.object(engine, "observe_off_hours") as observe,
        ):
            result = engine.tick(at)
        self.assertEqual(result["status"], "outside_regular_sessions")
        observe.assert_called_once_with(at)
        self.assertEqual(reviewer.method_calls, [])

    def test_daylight_saving_time_changes_automatically(self) -> None:
        for day, expected_hour in [("2026-10-30", 22), ("2026-11-02", 23)]:
            start = datetime.fromisoformat(day + "T09:30:00").replace(tzinfo=NEW_YORK)
            self.assertTrue(probable_regular("US", start))
            self.assertEqual(
                start.astimezone(__import__("papertrade.core", fromlist=["SEOUL"]).SEOUL).hour,
                expected_hour,
            )

    def test_korean_saturday_morning_can_be_us_friday(self) -> None:
        at = stamp("2026-10-31T01:00:00+09:00")
        self.assertTrue(probable_regular("US", at))
        self.assertFalse(probable_regular("KR", at))
        self.assertEqual(market_date("US", at), "2026-10-30")

    def test_holiday_calendar_has_no_regular_session(self) -> None:
        self.assertIsNone(regular_session("KR", {"date": "2026-10-30", "integrated": None}))

    def test_early_close_controls_entry_cutoff_and_flatten_time(self) -> None:
        session = Session(
            "US",
            "2026-11-27",
            stamp("2026-11-27T09:30:00-05:00"),
            stamp("2026-11-27T13:00:00-05:00"),
        )
        self.assertFalse(session.entry_allowed(session.end - timedelta(minutes=30), 60))

    def test_idle_tick_does_not_touch_api_or_ai(self) -> None:
        client, reviewer = MagicMock(), MagicMock()
        engine = Engine(self.settings, self.store, client, reviewer)
        result = engine.tick(stamp("2026-11-01T03:15:00+09:00"))
        self.assertEqual(result["status"], "outside_regular_sessions")
        client.assert_not_called()
        self.assertEqual(client.method_calls, [])
        self.assertEqual(reviewer.method_calls, [])


class EngineIntegrationTests(BaseCase):
    def test_stale_or_missing_rankings_do_not_block_full_universe_screening(self) -> None:
        client = MagicMock()
        listings = [{"symbol": "S1"}, {"symbol": "S2"}]
        client.listed.side_effect = lambda market: listings if market == "NYSE" else []
        client.prices.side_effect = lambda symbols: [
            {"symbol": symbol, "currency": "USD", "lastPrice": "10"} for symbol in symbols
        ]
        engine = Engine(self.settings, self.store, client, MagicMock())
        for ranked_at in (None, (AT - timedelta(minutes=5)).isoformat()):
            with self.subTest(ranked_at=ranked_at):
                client.ranking.return_value = {
                    "rankedAt": ranked_at,
                    "rankings": [{"symbol": "S2"}],
                }
                with patch("papertrade.engine.now_utc", return_value=AT):
                    rows = engine.shortlist("US", AT, Decimal(1), refresh=True)
                self.assertEqual([row["symbol"] for row in rows], ["S1", "S2"])
        skipped = self.store.db.execute(
            "SELECT COUNT(*) FROM events WHERE kind='ranking_not_used'"
        ).fetchone()[0]
        self.assertEqual(skipped, 8)

    def test_entire_listing_is_screened_in_provider_batches_without_a_symbol_cap(self) -> None:
        client = MagicMock()
        listings = [
            {"symbol": f"S{index:03}", "name": "Fixture", "securityType": "STOCK"}
            for index in range(601)
        ]
        client.listed.side_effect = lambda market: listings if market == "NYSE" else []
        client.ranking.return_value = {"rankedAt": AT.isoformat(), "rankings": [{"symbol": "S600"}]}
        client.prices.side_effect = lambda symbols: [
            {"symbol": symbol, "currency": "USD", "lastPrice": "10", "timestamp": AT.isoformat()}
            for symbol in symbols
        ]
        engine = Engine(self.settings, self.store, client, MagicMock())
        with patch("papertrade.engine.now_utc", return_value=AT):
            rows = engine.shortlist("US", AT, Decimal(1), refresh=True)
        self.assertEqual(len(rows), 601)
        self.assertEqual(rows[0]["symbol"], "S600")
        self.assertEqual(
            [len(call.args[0]) for call in client.prices.call_args_list], [200, 200, 200, 1]
        )

    def test_more_than_three_signals_can_be_reviewed_in_one_market_check(self) -> None:
        client, reviewer = MagicMock(), MagicMock()
        reviewer.review.side_effect = lambda rows, at: {
            row["symbol"]: {"action": "SKIP", "quantity": 0, "reason": "Fixture"} for row in rows
        }
        engine = Engine(self.settings, self.store, client, reviewer)
        session = self.session()
        calendar = {
            "today": {
                "date": session.date,
                "regularMarket": {
                    "startTime": session.start.isoformat(),
                    "endTime": session.end.isoformat(),
                },
            }
        }
        candidates = [
            {**candidate(), "symbol": f"S{index}", "signal_key": f"signal:{index}"}
            for index in range(7)
        ]
        with (
            patch.object(engine, "calendar", return_value=calendar),
            patch.object(engine, "inspect_positions"),
            patch.object(engine, "resolve_settlements"),
            patch.object(engine, "scan_iter", return_value=iter(candidates)),
            patch.object(engine, "observe_off_hours"),
            patch("papertrade.engine.now_utc", return_value=AT),
        ):
            engine.tick(AT)
        self.assertEqual(reviewer.review.call_count, 0)
        self.assertEqual(
            self.store.db.execute("SELECT COUNT(*) FROM jobs WHERE status='queued'").fetchone()[0],
            7,
        )

    def test_more_than_two_positions_can_be_held_when_cash_is_available(self) -> None:
        for index in range(2):
            position = self.position()
            position["symbol"] = f"HELD{index}"
            self.store.buy(position, decimal(position["cost_usd"]), f"entry:{index}", AT)
        client = MagicMock()
        client.warnings.return_value = []
        client.stocks.return_value = [{"status": "ACTIVE"}]
        client.book.return_value = book()
        engine = Engine(self.settings, self.store, client, MagicMock())
        with (
            patch("papertrade.engine.now_utc", return_value=AT),
            patch.object(
                engine,
                "action_anchor",
                return_value={"factor": "1", "raw_close": "100", "timestamp": AT.isoformat()},
            ),
        ):
            engine.enter_approved(
                [candidate()],
                {"TEST": {"action": "BUY", "quantity": 2, "reason": "Fixture"}},
                self.session(),
            )
        self.assertEqual(len(self.store.positions()), 3)

    def test_investment_warning_is_context_and_does_not_imitate_a_trading_halt(self) -> None:
        position = self.position()
        self.store.buy(position, decimal(position["cost_usd"]), "entry:1", AT)
        client = MagicMock()
        client.book.return_value = book(bid="90", ask="90.1")
        client.warnings.return_value = [{"warningType": "INVESTMENT_WARNING"}]
        client.stocks.return_value = [{"status": "ACTIVE"}]
        engine = Engine(self.settings, self.store, client, MagicMock())
        with patch("papertrade.engine.now_utc", return_value=AT):
            engine.inspect_positions("US", self.session(), AT)
        self.assertEqual(self.store.positions(), [])

    def session(self) -> Session:
        return Session(
            "US",
            "2026-11-02",
            stamp("2026-11-02T09:30:00-05:00"),
            stamp("2026-11-02T16:00:00-05:00"),
        )

    def test_approved_entry_is_repriced_and_recorded_only_in_the_paper_ledger(self) -> None:
        client = MagicMock()
        client.warnings.return_value = []
        client.stocks.return_value = [{"status": "ACTIVE"}]
        client.book.return_value = book()
        engine = Engine(self.settings, self.store, client, MagicMock())
        with (
            patch("papertrade.engine.now_utc", return_value=AT),
            patch.object(
                engine,
                "action_anchor",
                return_value={"factor": "1", "raw_close": "100", "timestamp": AT.isoformat()},
            ),
        ):
            engine.enter_approved(
                [candidate()],
                {"TEST": {"action": "BUY", "quantity": 2, "reason": "Fixture"}},
                self.session(),
            )
        self.assertEqual(len(self.store.positions()), 1)
        self.assertEqual(self.store.positions()[0]["entry_price"], "100.2")
        self.assertLess(self.store.settled_cash(AT), 1000)
        self.assertEqual({call[0] for call in client.method_calls}, {"warnings", "stocks", "book"})

    def test_price_moved_after_ai_approval_cancels_the_entry(self) -> None:
        client = MagicMock()
        client.warnings.return_value = []
        client.stocks.return_value = [{"status": "ACTIVE"}]
        client.book.return_value = book(bid="101.9", ask="102")
        engine = Engine(self.settings, self.store, client, MagicMock())
        with (
            patch("papertrade.engine.now_utc", return_value=AT),
            patch.object(
                engine,
                "action_anchor",
                return_value={"factor": "1", "raw_close": "100", "timestamp": AT.isoformat()},
            ),
        ):
            engine.enter_approved(
                [candidate()],
                {"TEST": {"action": "BUY", "quantity": 2, "reason": "Fixture"}},
                self.session(),
            )
        self.assertEqual(self.store.positions(), [])
        self.assertEqual(self.store.total_cash(), 1000)

    def test_observed_gap_loss_is_recorded_without_fabricating_a_stop_fill(self) -> None:
        position = self.position()
        self.store.buy(position, decimal(position["cost_usd"]), "entry:1", AT)
        client = MagicMock()
        client.book.return_value = book(AT + timedelta(minutes=15), bid="90", ask="90.1")
        client.warnings.return_value = []
        client.stocks.return_value = [{"status": "ACTIVE"}]
        engine = Engine(self.settings, self.store, client, MagicMock())
        with patch("papertrade.engine.now_utc", return_value=AT + timedelta(minutes=15)):
            engine.inspect_positions("US", self.session(), AT + timedelta(minutes=15))
        summary = self.store.summary(AT + timedelta(minutes=15))
        self.assertEqual(summary["positions"], [])
        self.assertEqual(summary["trades"][0]["fill_price"], "90")
        self.assertLess(decimal(summary["realized_pnl_usd"]), -10)
        self.assertGreater(decimal(summary["unsettled_cash_usd"]), 0)

    def test_warnings_prevent_assuming_a_liquidation_fill(self) -> None:
        position = self.position()
        self.store.buy(position, decimal(position["cost_usd"]), "entry:1", AT)
        client = MagicMock()
        client.book.return_value = book(bid="90", ask="90.1")
        client.warnings.return_value = [{"warningType": "VI_STATIC"}]
        engine = Engine(self.settings, self.store, client, MagicMock())
        with patch("papertrade.engine.now_utc", return_value=AT):
            engine.inspect_positions("US", self.session(), AT)
        self.assertEqual(len(self.store.positions()), 1)

    def test_regular_tick_without_signals_makes_zero_ai_calls(self) -> None:
        client, reviewer = MagicMock(), MagicMock()
        engine = Engine(self.settings, self.store, client, reviewer)
        session = self.session()
        calendar = {
            "today": {
                "date": session.date,
                "regularMarket": {
                    "startTime": session.start.isoformat(),
                    "endTime": session.end.isoformat(),
                },
            }
        }
        with (
            patch.object(engine, "calendar", return_value=calendar),
            patch.object(engine, "inspect_positions"),
            patch.object(engine, "resolve_settlements"),
            patch.object(engine, "scan_iter", return_value=iter(())),
            patch.object(engine, "observe_off_hours") as observe,
            patch("papertrade.engine.now_utc", return_value=AT),
        ):
            engine.tick(AT)
        observe.assert_called_once_with(AT)
        self.assertEqual(reviewer.method_calls, [])
        self.assertEqual(self.store.summary(AT)["ai_calls"], [])

    def test_settlement_respects_market_specific_business_days(self) -> None:
        self.store.db.execute(
            "INSERT INTO cash(amount,market,sale_date,source) VALUES ('100','US','2026-11-06','us-sale')"
        )
        self.store.db.execute(
            "INSERT INTO cash(amount,market,sale_date,source) VALUES ('100','KR','2026-11-06','kr-sale')"
        )
        client = MagicMock()

        def calendar(market: str, day: str) -> dict:
            next_date = "2026-11-09" if day == "2026-11-06" else "2026-11-10"
            raw = {
                "startTime": next_date
                + ("T09:00:00+09:00" if market == "KR" else "T09:30:00-05:00"),
                "endTime": next_date + ("T15:30:00+09:00" if market == "KR" else "T16:00:00-05:00"),
            }
            day_data = {"date": next_date, "regularMarket": raw}
            if market == "KR":
                day_data = {"date": next_date, "integrated": {"regularMarket": raw}}
            return {"nextBusinessDay": day_data}

        client.calendar.side_effect = calendar
        Engine(self.settings, self.store, client, MagicMock()).resolve_settlements(AT)
        releases = {
            row[0]: stamp(row[1])
            for row in self.store.db.execute(
                "SELECT source,available_at FROM cash WHERE source IN ('us-sale','kr-sale')"
            )
        }
        self.assertEqual(releases["us-sale"], stamp("2026-11-09T09:30:00-05:00"))
        self.assertEqual(releases["kr-sale"], stamp("2026-11-10T09:00:00+09:00"))


if __name__ == "__main__":
    unittest.main()
