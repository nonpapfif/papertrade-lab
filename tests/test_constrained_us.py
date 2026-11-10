"""Offline coverage for the normal US broker-constrained runtime adapter."""

import asyncio
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools.layered_paper import feature_from_bars
from tools.run_async_live_validation import SOURCES, Runtime, us_date, write_comparison


def candles(at: datetime, *, rising: bool = True) -> list[dict[str, str]]:
    rows = []
    for index in range(17):
        price = 7 + (index if rising else -index) / 10
        rows.append(
            {
                "timestamp": (at - timedelta(minutes=16 - index)).isoformat(),
                "available_at": at.isoformat(),
                "closePrice": str(price),
                "highPrice": "100",
            }
        )
    return rows


class ConstrainedUsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.close_at = datetime.fromisoformat("2026-11-28T03:00:00+09:00")
        self.day = "2026-11-27"
        self.calendar = {
            "today": {
                "date": self.day,
                "regularMarket": {
                    "startTime": "2026-11-27T23:30:00+09:00",
                    "endTime": self.close_at.isoformat(),
                },
                "afterMarket": {
                    "startTime": self.close_at.isoformat(),
                    "endTime": "2026-11-28T10:00:00+09:00",
                },
            }
        }
        self.runtime = Runtime(25, lambda *_args, **_kwargs: None, profile="broker-constrained")
        self.consumer = asyncio.create_task(self.runtime.consume())

    async def asyncTearDown(self) -> None:
        self.consumer.cancel()
        await asyncio.gather(self.consumer, return_exceptions=True)
        await self.runtime.close()

    async def publish(self, at: datetime, kind: str, payload: Any) -> None:
        with patch("tools.run_async_live_validation.now", return_value=at.isoformat()):
            self.runtime.publish(kind, payload)
            await asyncio.wait_for(self.runtime.queue.join(), 1)
            if self.consumer.done():
                self.consumer.result()

    async def book(
        self, at: datetime, source: datetime | None = None, volume: int = 10, *, symbol: str = "A"
    ) -> None:
        await self.publish(
            at,
            "book",
            (
                symbol,
                {
                    "timestamp": (source or at).isoformat(),
                    "currency": "USD",
                    "asks": [{"price": "5", "volume": str(volume)}],
                    "bids": [{"price": "4.99", "volume": str(volume)}],
                },
            ),
        )

    async def prime(self, at: datetime, *, rising: bool = True) -> None:
        await self.publish(at, "calendar", (self.day, self.calendar))
        await self.publish(at, "selection", ["A"])
        await self.publish(at, "feature", ("A", candles(at, rising=rising)))

    async def test_provider_bar_end_is_usable_at_publication_and_legacy_keeps_offset(self) -> None:
        at = self.close_at - timedelta(minutes=20)
        rows = candles(at)
        legacy = feature_from_bars(0, rows, at.isoformat())
        constrained = feature_from_bars(0, rows, at.isoformat(), timestamp_is_end=True)
        assert legacy is not None and constrained is not None
        self.assertEqual(legacy.price, 8.5)
        self.assertEqual(constrained.price, 8.6)
        self.assertEqual(constrained.bar_end, at.isoformat())

    async def test_invalid_order_draft_does_not_create_intent_or_reserve_cash(self) -> None:
        at = self.close_at - timedelta(seconds=20)
        symbol = "BAD SYMBOL"
        await self.publish(at, "calendar", (self.day, self.calendar))
        await self.publish(at, "selection", [symbol])
        await self.publish(at, "feature", (symbol, candles(at)))
        await self.book(at, symbol=symbol)
        account = self.runtime.account
        self.assertEqual(account.decisions[-1]["blocked_reason"], "Invalid order symbol")
        self.assertEqual(account.pending, {})
        self.assertEqual(account.reserved_cash, 0)
        self.assertEqual(account.book.cash, 25)

    async def test_regular_trend_buy_uses_fee_budget_and_later_visible_depth(self) -> None:
        at = self.close_at - timedelta(seconds=20)
        await self.prime(at)
        await self.book(at)
        account = self.runtime.account
        intent = account.pending[0]
        self.assertEqual((intent.quantity, intent.strategy), (4, "trend"))
        self.assertEqual(
            intent.order_request,
            {
                "clientOrderId": "live-1",
                "symbol": "A",
                "side": "BUY",
                "orderType": "LIMIT",
                "timeInForce": "DAY",
                "quantity": "4",
                "price": "5",
            },
        )
        self.assertAlmostEqual(intent.reserved, 20.02)
        await self.book(at + timedelta(seconds=2), volume=2)
        self.assertEqual(account.book.positions[0].quantity, 2)
        self.assertEqual(account.owners[0][0], "trend")
        self.assertAlmostEqual(account.book.cash, 14.99)
        self.assertEqual(account.reserved_cash, 0)
        fee_schedule = self.runtime.configurations["incumbent"]["fee_schedule"]
        assert isinstance(fee_schedule, dict)
        self.assertEqual(fee_schedule["commission_exemption_usd"], 0)

        # The owner is still reviewed after leaving candidate selection.
        sell_at = self.close_at - timedelta(seconds=12)
        await self.publish(sell_at, "selection", [])
        await self.publish(sell_at, "feature", ("A", candles(sell_at, rising=False)))
        await self.book(sell_at)
        self.assertEqual(account.pending[0].side, "SELL")
        sell_request = account.pending[0].order_request
        assert sell_request is not None
        self.assertEqual((sell_request["side"], sell_request["quantity"]), ("SELL", "2"))
        await self.book(sell_at + timedelta(seconds=2), volume=1)
        self.assertEqual(account.book.positions[0].quantity, 1)
        self.assertEqual(account.owners[0][0], "trend")
        self.assertAlmostEqual(account.book.cash, 19.96501)

        await self.book(self.close_at - timedelta(seconds=4))
        self.assertEqual(account.pending[0].side, "SELL")
        # The source is in-session, but the delayed publication is after the early close.
        await self.book(self.close_at, self.close_at - timedelta(seconds=1))
        self.assertEqual(account.book.positions[0].quantity, 1)
        await self.publish(self.close_at + timedelta(seconds=30), "clock", None)
        self.assertNotIn(0, account.pending)
        self.assertEqual(account.outcomes[-1]["status"], "expired")

    async def test_unavailable_orderbook_timestamp_is_skipped_and_consumer_continues(self) -> None:
        at = self.close_at - timedelta(seconds=20)
        await self.prime(at)
        books: list[dict[str, Any]] = [
            {"timestamp": None, "currency": "USD", "asks": [], "bids": []},
            {"currency": "USD", "asks": [{"price": "5", "volume": "2"}], "bids": []},
        ]
        for book in books:
            await self.publish(at, "book", ("A", book))
        self.assertFalse(self.consumer.done())
        self.assertNotIn("A", self.runtime.books)
        self.assertEqual(self.runtime.counts["book_unavailable"], 2)
        await self.book(at)
        self.assertFalse(self.consumer.done())
        self.assertEqual(self.runtime.counts["book_events"], 3)

    async def test_single_constrained_run_records_source_integrity_and_freeze(self) -> None:
        runtime = self.runtime
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            source_files = {}
            for name in SOURCES:
                path = output / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("frozen\n")
                source_files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest = {
                "source_sha256": source_files,
                "account": runtime.configurations["incumbent"],
            }
            (output / "manifest.json").write_text(json.dumps(manifest))
            (output / "market.jsonl.gz").write_bytes(b"market")
            (output / "events.jsonl").write_text("events\n")
            with patch("tools.run_async_live_validation.ROOT", output):
                result = write_comparison(output, {"state": "complete"}, runtime)
            assert result is not None
            self.assertTrue(result["source_unchanged"])
            self.assertTrue(result["freeze_matches"])
            runtime.account.policy = "breakout"
            with patch("tools.run_async_live_validation.ROOT", output):
                changed_config = write_comparison(output, {"state": "complete"}, runtime)
            assert changed_config is not None
            self.assertFalse(changed_config["configuration_unchanged"])
            self.assertFalse(changed_config["complete"])
            policy = runtime.configurations["incumbent"]["policy"]
            assert isinstance(policy, str)
            runtime.account.policy = policy
            (output / SOURCES[0]).write_text("changed\n")
            with patch("tools.run_async_live_validation.ROOT", output):
                changed = write_comparison(output, {"state": "complete"}, runtime)
            assert changed is not None
            self.assertFalse(changed["source_unchanged"])
            self.assertFalse(changed["complete"])
            self.assertEqual(changed["state"], "incomplete")
            self.assertNotEqual(changed["result"], "passed")

    async def test_unknown_mismatched_and_holiday_calendars_do_not_dispatch(self) -> None:
        at = self.close_at - timedelta(seconds=20)
        self.assertEqual(us_date(at.isoformat()), self.day)
        await self.book(at)
        await self.publish(at, "calendar", ("2026-11-28", self.calendar))
        await self.book(at)
        holiday = {"today": {"date": self.day, "regularMarket": None}}
        await self.publish(at, "calendar", (self.day, holiday))
        await self.book(at)
        self.assertEqual(self.runtime.counts["outside_regular_session"], 3)
        self.assertEqual(self.runtime.book_delivered["incumbent"], 0)

    async def test_pending_buy_cannot_confirm_at_close(self) -> None:
        at = self.close_at - timedelta(seconds=2)
        await self.prime(at)
        await self.book(at)
        self.assertIn(0, self.runtime.account.pending)
        await self.book(self.close_at)
        self.assertFalse(self.runtime.account.book.positions)
        self.assertIn(0, self.runtime.account.pending)


if __name__ == "__main__":
    unittest.main()
