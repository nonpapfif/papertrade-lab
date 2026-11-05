"""Live adapter tests without credentials or external market traffic."""

import asyncio
import copy
import hashlib
import importlib.util
import json
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

from papertrade.api import ApiError
from tools.layered_paper import Feature
from tools.paper_market import KR
from tools.run_async_live_validation import (
    SOURCES,
    Runtime,
    campaign,
    latency_summary,
    rank_candidates,
    subscription_set,
    wait_for_rest_cooldown,
    websocket_retry_at,
    write_comparison,
)


class SelectionTests(unittest.TestCase):
    def test_websocket_retry_time_honors_valid_delay_and_http_date(self) -> None:
        self.assertEqual(websocket_retry_at(["8"], 100), 108)
        self.assertEqual(websocket_retry_at(["Thu, 01 Jan 1970 00:02:00 GMT"], 100), 120)
        self.assertEqual(websocket_retry_at(["8", "10"], 100), 110)
        self.assertEqual(websocket_retry_at(["invalid", "-10", "1", "9" * 500], 100), 102)

    def test_default_runtime_preserves_single_account_status(self) -> None:
        runtime = Runtime(100, lambda *_args, **_kwargs: None)
        self.assertEqual(len(runtime.accounts), 1)
        self.assertNotIn("comparison", runtime.status())

    def test_scan_ranks_observed_moves_and_excludes_unaffordable_or_stale(self) -> None:
        at = datetime.now(UTC).isoformat()

        def row(symbol: str, price: float, stamp: str | None = at) -> dict[str, Any]:
            return {"symbol": symbol, "lastPrice": price, "timestamp": stamp, "currency": "USD"}

        rows = [row("A", 10), row("B", 11), row("C", 200), row("D", 5, None)]
        ranked, fresh = rank_candidates(rows, {"A": 10, "B": 10, "C": 100, "D": 1}, at, 100)
        self.assertEqual(ranked, ["B"])
        self.assertNotIn("D", fresh)

    def test_replacement_always_retains_pending_and_held_symbols(self) -> None:
        protected = {"HELD", "PENDING"}
        selected = subscription_set([f"S{i}" for i in range(70)], protected)
        self.assertEqual(len(selected), 50)
        self.assertTrue(protected <= set(selected))
        with self.assertRaises(ValueError):
            subscription_set([], {str(i) for i in range(51)})

    def test_empty_and_observed_latency(self) -> None:
        self.assertEqual(latency_summary([]), {"count": 0})
        self.assertEqual(latency_summary([3.0, 1.0, 2.0])["max"], 3.0)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = Runtime(100, lambda *_args, **_kwargs: None)
        self.worker = asyncio.create_task(self.runtime.consume())
        self.at = datetime.now(UTC)

    async def asyncTearDown(self) -> None:
        self.worker.cancel()
        await asyncio.gather(self.worker, return_exceptions=True)
        await self.runtime.account.close()

    def book(self, at: datetime, price: float = 10) -> dict[str, Any]:
        return {
            "timestamp": at.isoformat(),
            "currency": "USD",
            "asks": [{"price": str(price), "volume": "100"}],
            "bids": [{"price": str(price - 0.01), "volume": "100"}],
        }

    async def drain(self) -> None:
        await asyncio.wait_for(self.runtime.queue.join(), 1)
        if self.worker.done():
            self.worker.result()

    async def test_rest_delay_does_not_block_market_consumer(self) -> None:
        pending = asyncio.create_task(asyncio.to_thread(time.sleep, 0.15))
        self.runtime.publish("book", ("A", self.book(self.at)))
        await self.drain()
        self.assertEqual(self.runtime.counts["book_events"], 1)
        self.assertFalse(pending.done())
        await pending

    async def test_late_rest_snapshot_does_not_overwrite_newer_stream_book(self) -> None:
        self.runtime.publish("book", ("A", self.book(self.at)))
        self.runtime.publish("book", ("A", self.book(self.at - timedelta(seconds=1), 9)))
        await self.drain()
        self.assertEqual(self.runtime.books["A"]["asks"][0]["price"], "10")
        self.assertEqual(self.runtime.counts["old_snapshot_ignored"], 1)

    async def test_pending_symbol_survives_candidate_replacement_and_reconnect_duplicate(
        self,
    ) -> None:
        at = self.at.isoformat()
        r = self.runtime
        r.features["A"] = Feature(r.identifier("A"), 10, at, at, 0.01, 0.02, 9, 9, 0.8)
        r.publish("selection", ["A"])
        r.publish("book", ("A", self.book(self.at)))
        await self.drain()
        self.assertEqual(len(r.account.pending), 1)
        r.publish("selection", ["B"])
        await self.drain()
        self.assertIn("A", r.desired)
        later = self.at + timedelta(seconds=2)
        with patch("tools.run_async_live_validation.now", return_value=later.isoformat()):
            r.publish("book", ("A", self.book(later)))
            await self.drain()
            r.publish("book", ("A", self.book(later)))
            await self.drain()
        self.assertEqual(r.account.book.opened, 1)
        self.assertEqual(len(r.account.book.order_attempts), 1)
        self.assertIn("A", r.desired)

    async def test_delayed_feature_cannot_be_used_before_it_is_published(self) -> None:
        r = self.runtime
        r.publish("selection", ["A"])
        r.publish("book", ("A", self.book(self.at)))
        await self.drain()
        self.assertEqual(r.account.intents, [])
        self.assertEqual(r.counts["unusable_features"], 1)


class ComparisonTests(RuntimeTests):
    async def asyncSetUp(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.runtime = Runtime(
            100,
            lambda kind, **data: self.events.append({"kind": kind, **data}),
            candidate_policy="trend",
        )
        self.worker = asyncio.create_task(self.runtime.consume())
        self.at = datetime.now(UTC)

    async def asyncTearDown(self) -> None:
        self.worker.cancel()
        await asyncio.gather(self.worker, return_exceptions=True)
        await self.runtime.close()

    async def publish_at(self, offset: float, kind: str, payload: Any) -> None:
        at = (self.at + timedelta(seconds=offset)).isoformat()
        with patch("tools.run_async_live_validation.now", return_value=at):
            self.runtime.publish(kind, payload)
            await self.drain()

    async def test_rest_rate_limit_wait_preserves_paired_book_delivery(self) -> None:
        for offset, error in enumerate(
            (
                ApiError("Rate limit", status=429, retry_at=102),
                ApiError("Request cooldown", code="cooldown", retry_at=102),
            )
        ):

            async def deliver_book(delay: float, offset: int = offset) -> None:
                self.assertEqual(delay, 2)
                at = self.at + timedelta(seconds=offset)
                await self.publish_at(offset, "book", ("A", self.book(at)))

            with (
                patch("tools.run_async_live_validation.time") as clock,
                patch("tools.run_async_live_validation.asyncio.sleep", side_effect=deliver_book),
            ):
                clock.time.return_value = 100
                clock.monotonic.return_value = 50
                await wait_for_rest_cooldown(error, deadline=60)
            self.assertEqual(list(self.runtime.book_delivered.values()), [offset + 1] * 2)
            self.assertTrue(self.runtime.status()["comparison"]["inputs_aligned"])

    async def test_rest_rate_limit_wait_stops_at_observation_deadline(self) -> None:
        with (
            patch("tools.run_async_live_validation.time") as clock,
            patch("tools.run_async_live_validation.asyncio.sleep") as sleep,
        ):
            clock.time.return_value = 100
            clock.monotonic.return_value = 50
            await wait_for_rest_cooldown(ApiError("Rate limit", status=429, retry_at=120), 53)
            sleep.assert_awaited_once_with(3)
            clock.monotonic.return_value = 53
            await wait_for_rest_cooldown(ApiError("Rate limit", status=429, retry_at=120), 53)
            sleep.assert_awaited_once()
            with self.assertRaises(ApiError):
                await wait_for_rest_cooldown(ApiError("Unauthorized", status=401, retry_at=120), 60)

    def feature(self, symbol: str, *, prior_high: float = 9, r15: float = 0.02) -> Feature:
        at = self.at.isoformat()
        return Feature(self.runtime.identifier(symbol), 10, at, at, 0.01, r15, prior_high, 9, 0.8)

    async def test_crossed_book_is_rejected_before_both_accounts_and_next_book_continues(
        self,
    ) -> None:
        runtime = self.runtime
        crossed = self.book(self.at, 0.1885)
        crossed["bids"][0]["price"] = "0.1886"
        await self.publish_at(0, "book", ("CENN", crossed))
        self.assertNotIn("CENN", runtime.books)
        self.assertEqual(list(runtime.book_delivered.values()), [0, 0])
        self.assertTrue(all(not account.prices for account in runtime.accounts.values()))
        self.assertIn(
            {
                "kind": "book_rejected",
                "reason": "crossed_orderbook",
                "sequence": 1,
                "symbol": "CENN",
                "source_at": crossed["timestamp"],
            },
            self.events,
        )
        await self.publish_at(1, "book", ("ONDS", self.book(self.at + timedelta(seconds=1))))
        self.assertEqual(list(runtime.book_delivered.values()), [1, 1])
        self.assertTrue(runtime.status()["comparison"]["inputs_aligned"])

    async def test_same_observations_use_independent_accounts_and_clock(self) -> None:
        r = self.runtime
        r.features["A"] = self.feature("A")
        await self.publish_at(0, "selection", ["A"])
        await self.publish_at(0, "book", ("A", self.book(self.at)))
        incumbent, candidate = r.accounts.values()
        self.assertIsNot(incumbent.book, candidate.book)
        self.assertEqual(incumbent.reserved_cash, candidate.reserved_cash)
        self.assertEqual(incumbent.intents[0].decision_at, candidate.intents[0].decision_at)
        await self.publish_at(2, "book", ("A", self.book(self.at + timedelta(seconds=2))))
        self.assertEqual(incumbent.book.positions[0].quantity, 9)
        self.assertEqual(candidate.book.positions[0].quantity, 9)
        self.assertEqual(incumbent.book.cash, candidate.book.cash)
        comparison = r.status()["comparison"]
        self.assertTrue(comparison["inputs_aligned"])
        self.assertTrue(comparison["configuration_unchanged"])
        evidence = [a["input_sha256"] for a in comparison["accounts"].values()]
        self.assertEqual(evidence[0], evidence[1])
        self.assertEqual([a["observations"] for a in comparison["accounts"].values()], [2, 2])

    async def test_candidate_counts_use_adjusted_features_and_own_state(self) -> None:
        r = self.runtime
        r.features.update({s: self.feature(s) for s in ("A", "B")})
        await self.publish_at(0, "book", ("B", self.book(self.at, 7)))
        await self.publish_at(0, "selection", ["A", "B"])
        await self.publish_at(0, "book", ("A", self.book(self.at)))
        for account in r.accounts.values():
            self.assertEqual(account.intents[0].quantity, 9)
            self.assertEqual(set(account.pending), {r.ids["A"]})

    async def test_one_accounts_pending_request_does_not_exclude_another_accounts_candidate(
        self,
    ) -> None:
        r = self.runtime
        at = self.at.isoformat()
        r.features["B"] = Feature(r.identifier("B"), 10, at, at, 0.01, -0.02, 9, 9, 0.8)
        await self.publish_at(0, "selection", ["B"])
        await self.publish_at(0, "book", ("B", self.book(self.at)))
        self.assertIn(r.ids["B"], r.account.pending)
        self.assertEqual(r.accounts["candidate"].pending, {})
        r.features["B"] = self.feature("B")
        r.features["A"] = self.feature("A")
        await self.publish_at(1, "selection", ["A", "B"])
        await self.publish_at(1, "book", ("A", self.book(self.at + timedelta(seconds=1))))
        self.assertEqual(r.accounts["candidate"].pending[r.ids["A"]].quantity, 4)

    async def test_mid_fanout_failure_cannot_claim_aligned_inputs(self) -> None:
        r = self.runtime
        r.features["A"] = self.feature("A")
        await self.publish_at(0, "selection", ["A"])
        with (
            patch.object(r.accounts["candidate"], "submit", side_effect=RuntimeError("fixture")),
            self.assertRaisesRegex(RuntimeError, "fixture"),
        ):
            await self.publish_at(0, "book", ("A", self.book(self.at)))
        self.assertFalse(r.status()["comparison"]["inputs_aligned"])
        failure = next(event for event in self.events if event["kind"] == "runtime_event_failed")
        self.assertEqual(failure["symbol"], "A")
        self.assertEqual(failure["event_kind"], "book")
        self.assertIsInstance(failure["sequence"], int)
        self.assertTrue(failure["failure_frames"])
        self.assertNotIn("fixture", json.dumps(failure))

    async def test_published_observation_is_copied_before_fanout(self) -> None:
        r = self.runtime
        r.features["A"] = self.feature("A")
        await self.publish_at(0, "selection", ["A"])
        book = self.book(self.at)
        with patch("tools.run_async_live_validation.now", return_value=self.at.isoformat()):
            r.publish("book", ("A", book))
            book["asks"][0]["price"] = "500"
            await self.drain()
        self.assertTrue(all(a.intents[0].limit == 10 for a in r.accounts.values()))

    async def test_protected_symbols_are_the_union_of_both_accounts(self) -> None:
        r = self.runtime
        r.features["A"] = self.feature("A", prior_high=9.5, r15=0.2)
        await self.publish_at(0, "selection", ["A"])
        for offset, price in ((0, 10), (2, 10), (3, 9.3), (5, 9.3)):
            await self.publish_at(
                offset, "book", ("A", self.book(self.at + timedelta(seconds=offset), price))
            )
        self.assertNotIn(r.ids["A"], r.account.book.positions)
        self.assertIn(r.ids["A"], r.accounts["candidate"].book.positions)
        await self.publish_at(6, "selection", ["B"])
        self.assertIn("A", r.desired)

    async def test_report_marks_interruption_and_preserves_results_and_hashes(self) -> None:
        r = self.runtime
        r.features["A"] = self.feature("A")
        await self.publish_at(0, "selection", ["A"])
        await self.publish_at(0, "book", ("A", self.book(self.at)))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for name in SOURCES:
                source = output / name
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("fixture\n")
            source = output / SOURCES[0]
            manifest = {
                "source_sha256": {
                    name: hashlib.sha256((output / name).read_bytes()).hexdigest()
                    for name in SOURCES
                },
                "comparison": r.configurations,
            }
            (output / "manifest.json").write_text(json.dumps(manifest))
            (output / "market.jsonl.gz").write_bytes(b"recorded-input")
            (output / "events.jsonl").write_text("observations\n")
            with patch("tools.run_async_live_validation.ROOT", output):
                write_comparison(output, {"state": "running"}, r)
            result = json.loads((output / "comparison.json").read_text())
            self.assertFalse(result["complete"])
            self.assertEqual(result["state"], "interrupted")
            self.assertEqual(result["qualification"], "not_assessed")
            self.assertTrue(result["source_unchanged"])
            self.assertTrue(result["input_files_sha256"])
            self.assertGreater(result["accounts"]["incumbent"]["paper"]["reserved_cash"], 0)
            before = copy.deepcopy(result["accounts"])
            with patch("tools.run_async_live_validation.ROOT", output):
                write_comparison(output, {"state": "complete"}, r)
            completed = json.loads((output / "comparison.json").read_text())
            self.assertTrue(completed["complete"])
            self.assertTrue(completed["freeze_matches"])
            self.assertEqual(completed["observation_status"], "observed_short_sample")
            self.assertIn("ended_at", completed)
            (output / "events.jsonl").unlink()
            with patch("tools.run_async_live_validation.ROOT", output):
                write_comparison(output, {"state": "complete"}, r)
            missing = json.loads((output / "comparison.json").read_text())
            self.assertFalse(missing["complete"])
            self.assertEqual(missing["state"], "incomplete")
            (output / "events.jsonl").write_text("observations\n")
            source.write_text("changed\n")
            with patch("tools.run_async_live_validation.ROOT", output):
                write_comparison(output, {"state": "complete"}, r)
            changed = json.loads((output / "comparison.json").read_text())
            self.assertFalse(changed["complete"])
            self.assertFalse(changed["source_unchanged"])
            self.assertEqual(changed["accounts"], before)

    async def test_clock_expires_both_reservations_and_changed_policy_is_visible(self) -> None:
        r = self.runtime
        r.features["A"] = self.feature("A")
        await self.publish_at(0, "selection", ["A"])
        await self.publish_at(0, "book", ("A", self.book(self.at)))
        await self.publish_at(31, "clock", None)
        self.assertTrue(all(a.reserved_cash == 0 for a in r.accounts.values()))
        r.accounts["candidate"].policy = "rebound"
        self.assertFalse(r.status()["comparison"]["configuration_unchanged"])

    async def test_saved_comparison_preserves_each_accounts_partial_position_and_stale_mark(
        self,
    ) -> None:
        r = self.runtime
        r.features["A"] = self.feature("A", prior_high=9.5, r15=0.2)
        await self.publish_at(0, "selection", ["A"])
        for offset, price, volume in ((0, 10, 100), (2, 10, 100), (3, 9.3, 100), (5, 9.3, 1)):
            book = self.book(self.at + timedelta(seconds=offset), price)
            book["bids"][0]["volume"] = str(volume)
            await self.publish_at(offset, "book", ("A", book))
        await self.publish_at(66, "clock", None)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            write_comparison(output, {"state": "failed"}, r)
            saved = json.loads((output / "comparison.json").read_text())
        self.assertFalse(saved["complete"])
        self.assertEqual(saved["state"], "failed")
        self.assertEqual(saved["symbol_ids"], {"A": 0})
        self.assertTrue(saved["inputs_aligned"])
        for name, quantity, realized, equity in (
            ("incumbent", 8, -0.81, 93.55),
            ("candidate", 9, -0.09, 93.565),
        ):
            paper = saved["accounts"][name]["paper"]
            position = paper["positions"]["0"]
            self.assertEqual(position["quantity"], quantity)
            self.assertEqual(position["strategy"], "breakout" if name == "incumbent" else "trend")
            self.assertEqual(position["entry_price"], 10)
            self.assertEqual(position["entry_cost_usd"], quantity * 10)
            self.assertAlmostEqual(position["remaining_entry_fees_usd"], quantity * 0.01)
            self.assertAlmostEqual(position["mark_price"], 9.295)
            self.assertEqual(position["mark_at"], (self.at + timedelta(seconds=5)).isoformat())
            self.assertEqual(position["mark_age_seconds"], 61)
            self.assertEqual(
                paper["positions_as_of"], (self.at + timedelta(seconds=66)).isoformat()
            )
            account = paper["account"]
            self.assertEqual(account["mark_basis"], "book_midpoint_not_liquidated")
            self.assertEqual(account["stale_open_positions"], 1)
            self.assertAlmostEqual(account["realized"], realized)
            self.assertAlmostEqual(account["equity"], equity)
            self.assertAlmostEqual(account["unrealized"], position["unrealized_usd"])
            self.assertAlmostEqual(
                account["equity"], account["cash"] + position["market_value_usd"]
            )
            self.assertAlmostEqual(
                account["equity"], 100 + account["realized"] + position["unrealized_usd"]
            )

    @unittest.skipUnless(importlib.util.find_spec("websockets"), "Optional WebSocket dependency")
    async def test_configuration_failure_saves_an_incomplete_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            with (
                patch(
                    "tools.run_async_live_validation.Settings.load",
                    side_effect=ValueError("fixture"),
                ),
                self.assertRaisesRegex(SystemExit, "Live validation failed"),
            ):
                await campaign(output, 360, 100, candidate_policy="trend")
            status = json.loads((output / "status.json").read_text())
            result = json.loads((output / "comparison.json").read_text())
            self.assertEqual(status["state"], "failed")
            self.assertEqual(result["state"], "failed")
            self.assertFalse(result["complete"])

    @unittest.skipUnless(importlib.util.find_spec("websockets"), "Optional WebSocket dependency")
    async def test_campaign_setup_failure_saves_an_incomplete_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            with (
                patch("tools.run_async_live_validation.CachedTokenClient") as client,
                self.assertRaisesRegex(SystemExit, "Live validation failed"),
            ):
                client.return_value.access_token.side_effect = RuntimeError("fixture")
                await campaign(output, 360, 100, candidate_policy="trend")
            status = json.loads((output / "status.json").read_text())
            result = json.loads((output / "comparison.json").read_text())
            self.assertEqual(status["state"], "failed")
            self.assertEqual(result["state"], "failed")
            self.assertFalse(result["complete"])
            self.assertFalse(result["source_unchanged"])
            self.assertEqual(result["observation_status"], "inconclusive")

    @unittest.skipUnless(importlib.util.find_spec("websockets"), "Optional WebSocket dependency")
    async def test_adaptive_websocket_handshake_classifies_only_429_and_503(self) -> None:
        from websockets.datastructures import Headers
        from websockets.exceptions import InvalidStatus
        from websockets.http11 import Response

        runner = "tools.run_async_live_validation"

        async def select_and_consume(runtime: Runtime) -> None:
            runtime.desired = ["005930"]
            await asyncio.Future()

        for status in (429, 503, 401, 403, 500):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "run"
                headers = Headers([("Retry-After", "30")])
                connection = AsyncMock()
                connection.__aenter__.side_effect = InvalidStatus(
                    Response(status, "fixture", headers, body=b"private fixture response")
                )
                before = time.time()
                with (
                    patch("websockets.asyncio.client.connect", return_value=connection),
                    patch.object(Runtime, "consume", new=select_and_consume),
                    patch(f"{runner}.DeadlineMarketClient") as client,
                    patch(f"{runner}.LiveFeed") as feed,
                    patch(f"{runner}.read_spec", return_value=b"{}"),
                    self.assertRaisesRegex(SystemExit, "Live validation failed"),
                ):
                    client.return_value.access_token.return_value = "fixture-token"
                    client.return_value.calendar.return_value = {}
                    feed.return_value.universe.return_value = []
                    feed.return_value.snapshot.return_value = []
                    await asyncio.wait_for(
                        campaign(
                            output,
                            10,
                            100_000,
                            profile="broker-constrained",
                            adaptive=True,
                            end_epoch=before + 10,
                            market=KR,
                        ),
                        timeout=3,
                    )
                payload = (output / "status.json").read_text()
                result = json.loads(payload)
                self.assertEqual(result["state"], "failed")
                if status in (429, 503):
                    self.assertEqual(result["error_type"], "ApiError")
                    self.assertEqual(result["api_status"], status)
                    self.assertEqual(result["api_code"], "cooldown")
                    self.assertGreaterEqual(result["api_retry_at"], before + 30)
                    self.assertLessEqual(result["api_retry_at"], time.time() + 30)
                else:
                    self.assertEqual(result["error_type"], "InvalidStatus")
                    self.assertEqual(result["http_status"], status)
                    self.assertNotIn("api_retry_at", result)
                self.assertNotIn("private fixture response", payload)
                self.assertNotIn("fixture-token", payload)
                connection.__aenter__.assert_awaited_once()

    async def test_cancelled_constrained_campaign_persists_terminal_interruption(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            settings = SimpleNamespace(data={"limits": {}})
            websockets = ModuleType("websockets")
            websocket_asyncio = ModuleType("websockets.asyncio")
            websocket_client = ModuleType("websockets.asyncio.client")
            websocket_exceptions = ModuleType("websockets.exceptions")
            websocket_client.connect = AsyncMock()
            websocket_exceptions.ConnectionClosed = type("ConnectionClosed", (Exception,), {})
            websocket_exceptions.InvalidStatus = type("InvalidStatus", (Exception,), {})
            websocket_exceptions.WebSocketException = type("WebSocketException", (Exception,), {})
            with (
                patch.dict(
                    "sys.modules",
                    {
                        "websockets": websockets,
                        "websockets.asyncio": websocket_asyncio,
                        "websockets.asyncio.client": websocket_client,
                        "websockets.exceptions": websocket_exceptions,
                    },
                ),
                patch("tools.run_async_live_validation.Settings.load", return_value=settings),
                patch("tools.run_async_live_validation.CachedTokenClient") as client,
                patch("tools.run_async_live_validation.LiveFeed") as feed,
                patch("tools.run_async_live_validation.read_spec", return_value=b"{}"),
                patch(
                    "tools.run_async_live_validation.asyncio.sleep",
                    side_effect=asyncio.CancelledError,
                ),
                self.assertRaises(asyncio.CancelledError),
            ):
                client.return_value.access_token.return_value = "fixture-token"
                client.return_value.calendar.return_value = {}
                feed.return_value.universe.return_value = []
                await campaign(output, 360, 100, profile="broker-constrained")
            status = json.loads((output / "status.json").read_text())
            result = json.loads((output / "comparison.json").read_text())
            self.assertEqual(status["state"], "interrupted")
            self.assertIn("ended_at", status)
            self.assertEqual(result["state"], "interrupted")
            self.assertFalse(result["complete"])
