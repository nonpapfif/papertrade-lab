"""Offline regressions for observed failures and independent worker recovery."""

from __future__ import annotations

import gzip
import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock, patch

from test_framework import AT, BaseCase, book, candidate

from papertrade.ai import Reviewer, validate_decisions
from papertrade.api import ApiError, MarketClient
from papertrade.audit import Audit
from papertrade.core import SafetyError, Session, Settings, decimal, stamp
from papertrade.engine import Engine
from papertrade.service import generate_services, market_once, recover, review_once, risk_once
from papertrade.store import Store
from papertrade.strategy import Bar, aggregate, signal


class RecoveryTests(BaseCase):
    def test_minute_market_check_collects_quotes_without_ai_or_paper_execution(self) -> None:
        engine = self.engine()
        engine.client.calls = 0
        day = {
            "today": {
                "date": self.session().date,
                "regularMarket": {
                    "startTime": self.session().start.isoformat(),
                    "endTime": self.session().end.isoformat(),
                },
            }
        }
        with (
            patch("papertrade.service.now_utc", return_value=AT),
            patch.object(engine, "calendar", return_value=day),
            patch.object(engine, "shortlist", return_value=[{"symbol": "TEST"}]) as shortlist,
        ):
            market_once(engine)
        shortlist.assert_called_once_with("US", AT, decimal(1), refresh=True)
        engine.reviewer.review.assert_not_called()
        self.assertEqual(self.store.positions(), [])
        event = json.loads(
            self.store.db.execute("SELECT data FROM events WHERE kind='market_check'").fetchone()[0]
        )
        self.assertFalse(event["ai_invoked"])

    def test_closed_market_check_does_not_collect_quotes_or_call_ai(self) -> None:
        engine = self.engine()
        with (
            patch("papertrade.service.now_utc", return_value=stamp("2026-11-01T03:00:00+09:00")),
            patch.object(engine, "shortlist") as shortlist,
        ):
            market_once(engine)
        shortlist.assert_not_called()
        engine.reviewer.review.assert_not_called()

    def test_detail_scanner_reuses_the_fresh_minute_universe_snapshot(self) -> None:
        engine = self.engine()
        self.store.put_cache("universe_snapshot:US", [{"symbol": "TEST"}], AT)
        rows = engine.shortlist("US", AT + timedelta(seconds=30), decimal(1))
        self.assertEqual(rows, [{"symbol": "TEST"}])
        engine.client.prices.assert_not_called()
        engine.client.ranking.assert_not_called()

    def test_successful_retry_without_a_signal_still_resolves_the_api_failure(self) -> None:
        engine = self.engine()
        self.store.role = "scan"
        Audit(self.store).record(
            "candidate", "rate-limit-exceeded", {"symbol": "TEST", "market": "US"}
        )
        Audit(self.store).record("service", "rate-limit-exceeded", {"role": "scan"})
        bars = [
            Bar(
                AT - timedelta(minutes=90 - index * 30),
                decimal(99),
                decimal(100),
                decimal(98),
                decimal(99),
                decimal(100),
            )
            for index in range(3)
        ]
        with (
            patch.object(engine, "sessions", return_value=[self.session()]),
            patch.object(engine, "shortlist", return_value=[{"symbol": "TEST"}]),
            patch.object(engine, "history", return_value=bars),
            patch("papertrade.engine.now_utc", return_value=AT),
        ):
            self.assertEqual(list(engine.scan_iter("US", AT)), [])
        self.assertEqual(Audit(self.store).summary(True)["incidents"], [])

    def test_approval_freshness_is_checked_again_after_market_data_wait(self) -> None:
        engine = self.engine()
        engine.client.book.return_value = book(AT + timedelta(minutes=5, seconds=1))
        observed = {**candidate(), "observed_at": AT.isoformat()}
        with (
            patch.object(engine, "action_anchor", return_value={"factor": "1"}),
            patch.object(engine, "fx", return_value=decimal(1)),
            patch(
                "papertrade.engine.now_utc",
                side_effect=[
                    AT + timedelta(minutes=4),
                    AT + timedelta(minutes=4),
                    AT + timedelta(minutes=5, seconds=1),
                ],
            ),
        ):
            engine.enter_approved(
                [observed],
                {"TEST": {"action": "BUY", "quantity": 2, "reason": "Fixture"}},
                self.session(),
            )
        self.assertEqual(self.store.positions(), [])

    def test_new_trading_day_refetches_adjusted_history_without_touching_positions(self) -> None:
        self.owned()
        session = Session("US", "2026-11-02", AT - timedelta(minutes=30), AT)
        old = [
            {
                "timestamp": (session.start + timedelta(minutes=i)).isoformat(),
                "openPrice": "100",
                "highPrice": "101",
                "lowPrice": "99",
                "closePrice": "100",
                "volume": "1",
            }
            for i in range(30)
        ]
        self.store.put_candles("TEST", old)
        self.store.put_cache("history_day:TEST", "2026-10-30", AT - timedelta(days=3))
        engine = self.engine()
        engine.client.candles.return_value = {
            "candles": [
                {
                    **row,
                    "timestamp": (stamp(row["timestamp"]) + timedelta(minutes=1)).isoformat(),
                    "openPrice": "50",
                    "highPrice": "51",
                    "lowPrice": "49",
                    "closePrice": "50",
                }
                for row in old
            ],
            "nextBefore": None,
        }
        bars = engine.history("TEST", [session], AT)
        self.assertEqual(bars[0].close, 50)
        self.assertEqual(len(self.store.positions()), 1)

    def test_overnight_gap_does_not_change_the_existing_breakout_strategy(self) -> None:
        previous = [
            Bar(
                AT - timedelta(days=3, hours=9) + timedelta(minutes=30 * index),
                decimal(99),
                decimal(100),
                decimal(98),
                decimal(99),
                decimal(100),
            )
            for index in range(15)
        ]
        current = [
            Bar(
                AT - timedelta(minutes=90 - index * 30),
                decimal(99),
                decimal(100),
                decimal(98),
                decimal(99),
                decimal(100),
            )
            for index in range(3)
        ]
        current[-1] = Bar(
            current[-1].at, decimal(99), decimal(102), decimal(99), decimal(101), decimal(300)
        )
        self.assertIsNotNone(
            signal("TEST", "US", previous + current, decimal(0), AT, self.settings)
        )

    def test_close_labeled_minutes_include_opening_and_latest_complete_bar(self) -> None:
        session = Session("US", "2026-11-02", AT - timedelta(minutes=30), AT)
        raw = [
            {
                "timestamp": (session.start + timedelta(minutes=index + 1)).isoformat(),
                "openPrice": "100",
                "highPrice": "101",
                "lowPrice": "99",
                "closePrice": "100",
                "volume": "1",
            }
            for index in range(30)
        ]
        engine = self.engine()
        engine.client.candles.return_value = {"candles": raw, "nextBefore": None}
        bars = engine.history("TEST", [session], AT)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].at, session.start)
        self.assertEqual(bars[0].volume, 30)
        self.assertEqual(stamp(engine.client.candles.call_args.args[1]), AT)
        self.assertEqual(
            self.store.candles("TEST", session.start, AT)[0]["source_timestamp"],
            raw[0]["timestamp"],
        )

    def test_candle_cache_migration_preserves_actual_paper_ledger(self) -> None:
        self.owned()
        self.store.put_candles("TEST", [{"timestamp": AT.isoformat()}])
        self.store.db.execute("UPDATE meta SET value='old' WHERE key='candle_semantics'")
        before = self.store.total_cash()
        self.store.close()
        self.store = Store(self.settings.state_dir / "paper.sqlite3")
        self.assertEqual(self.store.total_cash(), before)
        self.assertEqual(len(self.store.positions()), 1)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM candles").fetchone()[0], 0)

    def test_reused_pid_does_not_count_as_the_original_worker(self) -> None:
        from papertrade.service import alive

        with patch("papertrade.service.process_identity", return_value="new-birth"):
            self.assertFalse(alive(os.getpid(), "old-birth"))

    def session(self) -> Session:
        return Session("US", "2026-11-02", AT - timedelta(hours=2), AT + timedelta(hours=4))

    def engine(self) -> Engine:
        client = MagicMock()
        client.warnings.return_value = []
        client.stocks.return_value = [{"status": "ACTIVE"}]
        client.book.return_value = book(bid="90", ask="90.1")
        return Engine(self.settings, self.store, client, MagicMock())

    def owned(self, symbol: str = "TEST") -> dict:
        position = self.position()
        position["symbol"] = symbol
        self.store.buy(position, decimal(position["cost_usd"]), "entry:" + symbol, AT)
        return position

    def test_one_position_transport_failure_does_not_skip_remaining_positions(self) -> None:
        self.owned("BAD")
        self.owned("GOOD")
        engine = self.engine()
        engine.client.warnings.side_effect = [ApiError("temporary"), []]
        with patch("papertrade.engine.now_utc", return_value=AT):
            engine.inspect_positions("US", self.session(), AT)
        self.assertEqual([row["symbol"] for row in self.store.positions()], ["BAD"])
        incident = Audit(self.store).summary(True)["incidents"][0]
        self.assertEqual(incident["data"]["context"]["symbol"], "BAD")

    def test_closing_auction_cannot_generate_an_immediate_fill(self) -> None:
        self.owned()
        engine = self.engine()
        session = Session(
            "US",
            "2026-11-02",
            AT - timedelta(hours=2),
            AT + timedelta(minutes=10),
            AT - timedelta(minutes=1),
        )
        engine.inspect_positions("US", session, AT)
        engine.client.book.assert_not_called()
        self.assertEqual(len(self.store.positions()), 1)

    def test_exit_and_mark_use_complete_visible_depth(self) -> None:
        position = self.owned()
        engine = self.engine()
        depth = book(bid="106", ask="106.1")
        depth["bids"] = [{"price": "106", "volume": "1"}, {"price": "80", "volume": "1"}]
        engine.client.book.return_value = depth
        with patch("papertrade.engine.now_utc", return_value=AT):
            engine.inspect_positions("US", self.session(), AT)
        trade = self.store.summary(AT)["trades"][0]
        self.assertEqual(trade["reason"], "stop_at_observed_quote")
        self.assertEqual(trade["fill_price"], "93")
        self.assertLess(decimal(trade["proceeds_usd"]), decimal(position["cost_usd"]))

    def test_failed_slot_is_retryable_and_completed_slot_is_not(self) -> None:
        engine = self.engine()
        with (
            patch.object(engine, "collect", side_effect=ApiError("temporary")),
            self.assertRaises(ApiError),
        ):
            engine.tick(AT)
        with patch.object(engine, "collect", return_value={"status": "checked"}):
            self.assertEqual(engine.tick(AT)["status"], "checked")
        self.assertEqual(engine.tick(AT)["status"], "duplicate_slot")

    def test_fx_cache_honors_server_expiry(self) -> None:
        self.store.put_cache(
            "fx:USD:KRW",
            {"rate": "1400", "valid_until": (AT + timedelta(seconds=1)).isoformat()},
            AT,
        )
        engine = self.engine()
        engine.client.get.return_value = {
            "midRate": "1410",
            "validUntil": (AT + timedelta(minutes=1)).isoformat(),
        }
        with patch("papertrade.engine.now_utc", return_value=AT + timedelta(seconds=2)):
            self.assertEqual(engine.fx("KR", AT + timedelta(seconds=2)), 1410)
        engine.client.get.assert_called_once()

    def test_fx_delay_cannot_hide_a_stale_entry_book(self) -> None:
        engine = self.engine()
        engine.client.book.return_value = book()
        with (
            patch.object(engine, "action_anchor", return_value={"factor": "1"}),
            patch.object(engine, "fx", return_value=decimal(1)),
            patch("papertrade.engine.now_utc", side_effect=[AT, AT, AT + timedelta(seconds=32)]),
        ):
            engine.enter_approved(
                [candidate()],
                {"TEST": {"action": "BUY", "quantity": 2, "reason": "Fixture"}},
                self.session(),
            )
        self.assertEqual(self.store.positions(), [])

    def test_backfill_fetches_an_older_hole_even_when_latest_minutes_are_cached(self) -> None:
        session = Session("US", "2026-11-02", AT - timedelta(minutes=60), AT)
        rows = [
            {
                "timestamp": (session.start + timedelta(minutes=i)).isoformat(),
                "openPrice": "100",
                "highPrice": "101",
                "lowPrice": "99",
                "closePrice": "100",
                "volume": "1",
            }
            for i in range(60)
        ]
        self.store.put_candles("TEST", [row for i, row in enumerate(rows) if i != 2])
        self.store.put_cache("history_day:TEST", session.date, AT)
        engine = self.engine()
        engine.client.candles.return_value = {
            "candles": [
                {**row, "timestamp": (stamp(row["timestamp"]) + timedelta(minutes=1)).isoformat()}
                for row in rows
            ],
            "nextBefore": None,
        }
        bars = engine.history("TEST", [session], AT)
        self.assertEqual(len(bars), 2)
        self.assertEqual(len(self.store.candles("TEST", session.start, AT)), 60)
        self.assertLess(
            stamp(engine.client.candles.call_args.args[1]), session.start + timedelta(minutes=4)
        )

    def test_noncontiguous_signal_window_is_rejected(self) -> None:
        bars = [
            Bar(
                AT - timedelta(minutes=30 * (18 - i)),
                decimal(99),
                decimal(100),
                decimal(98),
                decimal(99),
                decimal(100),
            )
            for i in range(18)
        ]
        bars[-1] = Bar(
            AT - timedelta(minutes=30),
            decimal(99),
            decimal(102),
            decimal(99),
            decimal(101),
            decimal(300),
        )
        bars.pop(-3)
        self.assertIsNone(signal("TEST", "US", bars, decimal(0), AT, self.settings))

    def test_bad_candle_ranges_and_negative_volume_are_rejected(self) -> None:
        session = Session("US", "2026-11-02", AT - timedelta(minutes=30), AT)
        rows = [
            {
                "timestamp": (session.start + timedelta(minutes=i)).isoformat(),
                "openPrice": "100",
                "highPrice": "99",
                "lowPrice": "98",
                "closePrice": "100",
                "volume": "1",
            }
            for i in range(30)
        ]
        with self.assertRaises(SafetyError):
            aggregate(rows, [session], AT)

    def test_unhashable_model_symbols_are_rejected_as_data(self) -> None:
        with self.assertRaises(SafetyError):
            validate_decisions(
                {"decisions": [{"symbol": [], "action": "BUY", "quantity": 1, "reason": "x"}]},
                {"TEST"},
            )

    def test_pending_and_interrupted_usage_is_unknown_not_zero(self) -> None:
        call = self.store.reserve_ai("test", AT)
        self.assertTrue(self.store.summary(AT)["ai_usage_unknown"])
        self.store.finish_ai(call, "interrupted", None, {})
        row = self.store.summary(AT)["ai_calls"][0]
        self.assertIsNone(row["input_tokens"])
        self.assertFalse(row["usage_known"])

    def test_recovery_reads_usage_spool_without_replaying_an_approval(self) -> None:
        self.store.owner = "dead-owner"
        self.store.enqueue(candidate())
        with patch("papertrade.store.now_utc", return_value=AT):
            self.store.lease_jobs(self.store.owner)
        call = self.store.reserve_ai("test", AT)
        self.store.heartbeat("review", self.store.owner, "waiting_model")
        self.store.db.execute("UPDATE workers SET pid=99999999")
        directory = self.settings.state_dir / "ai-events"
        directory.mkdir()
        (directory / f"{call}.jsonl").write_text(
            '{"type":"turn.completed","usage":{"input_tokens":123,"output_tokens":4}}\n'
        )
        recover(self.store)
        row = self.store.summary(AT)["ai_calls"][0]
        self.assertEqual(row["status"], "interrupted")
        self.assertEqual(row["input_tokens"], 123)
        self.assertEqual(
            self.store.db.execute("SELECT status FROM jobs").fetchone()[0], "interrupted"
        )

    def test_live_owner_is_never_reclaimed_for_slow_inference(self) -> None:
        self.store.owner = "alive-owner"
        self.store.heartbeat("review", self.store.owner, "waiting_model")
        self.store.reserve_ai("test", AT)
        recover(self.store)
        self.assertEqual(self.store.summary(AT)["ai_calls"][0]["status"], "reserved")

    def test_incidents_recover_without_losing_original_history_or_secrets(self) -> None:
        audit = Audit(self.store)
        key = audit.record(
            "http",
            "limit",
            {"group": "CHART", "authorization": "Bearer secret", "nested": {"api_key": "private"}},
            retry_at=AT.isoformat(),
        )
        audit.record("http", "limit", {"group": "CHART"})
        audit.recovered("http", group="CHART")
        row = audit.summary()["incidents"][0]
        self.assertEqual(row["id"], key)
        self.assertEqual(row["occurrences"], 2)
        self.assertEqual(row["state"], "recovered")
        events = "".join(row[0] for row in self.store.db.execute("SELECT data FROM events"))
        self.assertNotIn("secret", events)
        self.assertNotIn("private", events)

    def test_audit_falls_back_to_private_emergency_journal_when_db_fails(self) -> None:
        self.store.close()
        Audit(self.store).record("db", "unavailable", {"symbol": "TEST"})
        path = self.settings.state_dir / "emergency-audit.jsonl"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(path.read_text())["code"], "unavailable")
        self.store = Store(self.settings.state_dir / "paper.sqlite3")

    def test_verified_backup_preserves_capital_and_positions(self) -> None:
        self.owned()
        path = self.store.backup()
        backup = Store(path)
        try:
            self.assertEqual(backup.total_cash(), self.store.total_cash())
            self.assertEqual(backup.positions(), self.store.positions())
        finally:
            backup.close()

    def test_existing_empty_or_unrelated_database_never_creates_new_capital(self) -> None:
        empty = self.root / "empty.sqlite3"
        empty.touch()
        with self.assertRaises(SafetyError):
            Store(empty)
        other = self.root / "other.sqlite3"
        connection = sqlite3.connect(other)
        connection.execute("CREATE TABLE other(id INTEGER)")
        connection.close()
        with self.assertRaises(SafetyError):
            Store(other)

    def test_summary_stays_in_one_snapshot_during_concurrent_ledger_write(self) -> None:
        ready = threading.Event()
        done = threading.Event()

        def writer() -> None:
            ready.wait()
            connection = sqlite3.connect(self.store.path, isolation_level=None)
            connection.execute(
                "INSERT INTO cash(amount,available_at,source) VALUES ('100',?,'concurrent')",
                (AT.isoformat(),),
            )
            connection.close()
            done.set()

        thread = threading.Thread(target=writer)
        thread.start()
        original = self.store.equity

        def equity():
            value = original()
            ready.set()
            self.assertTrue(done.wait(3))
            return value

        with patch.object(self.store, "equity", side_effect=equity):
            report = self.store.summary(AT)
        thread.join()
        self.assertEqual(report["equity_usd"], report["settled_cash_usd"])
        self.assertEqual(report["equity_usd"], "1000")
        self.assertEqual(self.store.total_cash(), 1100)

    def test_risk_keeps_executing_while_an_independent_ai_owner_is_waiting(self) -> None:
        self.owned()
        self.store.heartbeat("review", "waiting-owner", "waiting_model")
        engine = self.engine()
        with (
            patch.object(
                engine,
                "calendar",
                return_value={
                    "today": {
                        "date": "2026-11-02",
                        "regularMarket": {
                            "startTime": self.session().start.isoformat(),
                            "endTime": self.session().end.isoformat(),
                        },
                    }
                },
            ),
            patch("papertrade.service.now_utc", return_value=AT),
            patch("papertrade.engine.now_utc", return_value=AT),
            patch.object(engine, "resolve_settlements"),
        ):
            risk_once(engine)
        self.assertEqual(self.store.positions(), [])
        engine.reviewer.review.assert_not_called()

    def test_all_fresh_candidates_reach_one_comparative_review_without_a_count_cap(self) -> None:
        for number in range(9):
            self.store.enqueue({**candidate(), "symbol": f"S{number}", "signal_key": f"s{number}"})
        engine = self.engine()
        engine.reviewer.review.return_value = {}
        day = {
            "today": {
                "date": "2026-11-02",
                "regularMarket": {
                    "startTime": self.session().start.isoformat(),
                    "endTime": self.session().end.isoformat(),
                },
            }
        }
        with (
            patch("papertrade.store.now_utc", return_value=AT),
            patch("papertrade.service.now_utc", return_value=AT),
            patch.object(engine, "calendar", return_value=day),
        ):
            review_once(engine, "owner")
        self.assertEqual(len(engine.reviewer.review.call_args.args[0]), 9)
        self.assertEqual(engine.reviewer.review.call_count, 1)

    def test_services_start_at_login_and_restart_without_a_global_ai_lock(self) -> None:
        import plistlib

        definitions = [
            plistlib.loads(path.read_bytes()) for path in generate_services(self.settings)
        ]
        self.assertEqual(len(definitions), 5)
        self.assertEqual(
            {row["ProgramArguments"][-1] for row in definitions},
            {"market", "scan", "risk", "review", "supervise"},
        )
        self.assertTrue(all(row["KeepAlive"] and row["RunAtLoad"] for row in definitions))

    def test_adjusted_price_change_freezes_overnight_position_until_evidence(self) -> None:
        position = self.owned()
        position.update(
            session_date="2026-10-30",
            action_anchor={
                "timestamp": (AT - timedelta(days=3)).isoformat(),
                "factor": "1",
                "raw_close": "100",
            },
        )
        engine = self.engine()
        engine.client.daily.return_value = {
            "candles": [{"timestamp": position["action_anchor"]["timestamp"], "closePrice": "50"}]
        }
        self.assertFalse(engine.check_actions(position, self.session()))
        self.assertEqual(Audit(self.store).summary(True)["incidents"][0]["state"], "needs_evidence")

    def test_split_is_idempotent_and_preserves_cost_and_capital(self) -> None:
        position = self.owned()
        position["action_anchor"] = {"factor": "1", "raw_close": "100", "timestamp": AT.isoformat()}
        self.store.db.execute("UPDATE positions SET data=?", (json.dumps(position),))
        action = {
            "event_key": "split:test",
            "symbol": "TEST",
            "kind": "split",
            "effective_date": "2026-10-01",
            "source": "https://issuer.example/split",
            "new_shares": "2",
            "old_shares": "1",
            "adjustment_factor": "2",
        }
        cash = self.store.total_cash()
        self.store.apply_action(action)
        self.store.apply_action(action)
        updated = self.store.positions()[0]
        self.assertEqual(updated["quantity"], 4)
        self.assertEqual(decimal(updated["entry_price"]), decimal(position["entry_price"]) / 2)
        self.assertEqual(decimal(updated["risk_distance"]), decimal(position["risk_distance"]) / 2)
        self.assertEqual(updated["cost_usd"], position["cost_usd"])
        self.assertEqual(self.store.total_cash(), cash)

    def test_429_cooldown_is_shared_and_success_recovers_incident(self) -> None:
        client = MarketClient(self.settings, store=self.store)
        with (
            patch.object(client, "access_token", return_value="fixture"),
            patch("papertrade.api.time.sleep"),
        ):
            client.opener = MagicMock()
            client.opener.open.side_effect = urllib.error.HTTPError(
                "fixture",
                429,
                "limit",
                {"retry-after": "4", "x-ratelimit-limit": "1"},
                io.BytesIO(b'{"error":{"code":"too-many-requests","requestId":"req-test"}}'),
            )
            with self.assertRaises(ApiError):
                client.get("/api/v1/candles", symbol="TEST")
            other = MarketClient(self.settings, store=self.store)
            with self.assertRaises(ApiError):
                other._reserve("MARKET_DATA_CHART")
            response = MagicMock()
            response.read.return_value = b'{"result":{"candles":[]}}'
            response.headers = {"X-RateLimit-Limit": "1", "X-RateLimit-Remaining": "1"}
            response.status = 200
            client.opener.open.side_effect = None
            client.opener.open.return_value.__enter__.return_value = response
            self.store.db.execute("UPDATE rate_limits SET next_at=0")
            client.get("/api/v1/candles", symbol="TEST")
        self.assertEqual(Audit(self.store).summary()["incidents"][0]["state"], "recovered")
        self.assertEqual(self.store.db.execute("SELECT failures FROM rate_limits").fetchone()[0], 0)

    def test_memory_token_expiry_and_401_trigger_one_refresh(self) -> None:
        directory = self.root / "secrets"
        directory.mkdir(mode=0o700)
        for key, value in (
            ("TOSS_CLIENT_ID", "fixture-id"),
            ("TOSS_CLIENT_SECRET", "fixture-secret"),
        ):
            path = directory / key
            path.write_text(value)
            path.chmod(0o600)
        client = MarketClient(self.settings)
        client.token = "expired"
        client.expires_at = time.time() - 1
        with (
            patch.object(
                Settings, "secrets_dir", new_callable=PropertyMock, return_value=directory
            ),
            patch.object(
                client, "_request", return_value={"access_token": "fresh", "expires_in": 3600}
            ) as request,
        ):
            self.assertEqual(client.access_token(), "fresh")
            self.assertEqual(client.access_token(), "fresh")
            request.assert_called_once()
        response = MagicMock()
        response.read.return_value = b'{"result":[]}'
        response.headers = {}
        response.status = 200
        client.opener = MagicMock()
        client.opener.open.side_effect = [
            urllib.error.HTTPError("fixture", 401, "unauthorized", {}, io.BytesIO(b"{}")),
            MagicMock(__enter__=MagicMock(return_value=response)),
        ]
        with (
            patch.object(client, "access_token", side_effect=["old", "new"]) as access,
            patch.object(client, "_invalidate_token") as invalid,
            patch("papertrade.api.time.sleep"),
        ):
            client.get("/api/v1/prices", symbols="TEST")
        self.assertEqual(access.call_count, 2)
        invalid.assert_called_once()

    def test_gzip_market_response_is_decoded_before_validation(self) -> None:
        payload = MarketClient._payload(io.BytesIO(gzip.compress(b'{"result":[]}')))
        self.assertEqual(payload, {"result": []})

    def test_stream_records_usage_heartbeat_and_final_message_without_timeout(self) -> None:
        call = self.store.reserve_ai("stream", AT)
        program = 'import sys,json;sys.stdin.read();print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"{}"}}));print(json.dumps({"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":2}}))'
        process = subprocess.Popen(
            [sys.executable, "-c", program],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        usage, message, actions = Reviewer(self.settings, self.store).stream(
            process, "x" * 200000, call
        )
        self.assertEqual(usage["input_tokens"], 10)
        self.assertEqual(message, "{}")
        self.assertEqual(actions, [])
        self.assertTrue((self.settings.state_dir / "ai-events" / f"{call}.jsonl").exists())
        self.assertEqual(
            self.store.db.execute("SELECT phase FROM workers").fetchone()[0], "waiting_model"
        )
