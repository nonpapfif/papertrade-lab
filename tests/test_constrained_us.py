"""Offline coverage for the normal US broker-constrained runtime adapter."""

import asyncio
import unittest
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import patch

from tools.layered_paper import feature_from_bars
from tools.run_async_live_validation import Runtime, us_date


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

    async def book(self, at: datetime, source: datetime | None = None, volume: int = 10) -> None:
        await self.publish(
            at,
            "book",
            (
                "A",
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

    async def test_regular_trend_buy_uses_fee_budget_and_later_visible_depth(self) -> None:
        at = self.close_at - timedelta(seconds=20)
        await self.prime(at)
        await self.book(at)
        account = self.runtime.account
        intent = account.pending[0]
        self.assertEqual((intent.quantity, intent.strategy), (4, "trend"))
        self.assertAlmostEqual(intent.reserved, 20.02)
        await self.book(at + timedelta(seconds=2), volume=2)
        self.assertEqual(account.book.positions[0].quantity, 2)
        self.assertEqual(account.owners[0][0], "trend")
        self.assertAlmostEqual(account.book.cash, 14.99)
        self.assertEqual(account.reserved_cash, 0)
        self.assertEqual(
            self.runtime.configurations["incumbent"]["fee_schedule"]["commission_exemption_usd"], 0
        )

        # The owner is still reviewed after leaving candidate selection.
        sell_at = self.close_at - timedelta(seconds=12)
        await self.publish(sell_at, "selection", [])
        await self.publish(sell_at, "feature", ("A", candles(sell_at, rising=False)))
        await self.book(sell_at)
        self.assertEqual(account.pending[0].side, "SELL")
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
