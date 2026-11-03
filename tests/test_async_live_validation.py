"""Live adapter tests without credentials or external market traffic."""

import asyncio
import time
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from tools.layered_paper import Feature
from tools.run_async_live_validation import (
    Runtime,
    latency_summary,
    rank_candidates,
    subscription_set,
)


class SelectionTests(unittest.TestCase):
    def test_scan_ranks_observed_moves_and_excludes_unaffordable_or_stale(self):
        at = datetime.now(UTC).isoformat()

        def row(symbol, price, stamp=at):
            return {"symbol": symbol, "lastPrice": price, "timestamp": stamp, "currency": "USD"}

        rows = [row("A", 10), row("B", 11), row("C", 200), row("D", 5, None)]
        ranked, fresh = rank_candidates(rows, {"A": 10, "B": 10, "C": 100, "D": 1}, at, 100)
        self.assertEqual(ranked, ["B"])
        self.assertNotIn("D", fresh)

    def test_replacement_always_retains_pending_and_held_symbols(self):
        protected = {"HELD", "PENDING"}
        selected = subscription_set([f"S{i}" for i in range(70)], protected)
        self.assertEqual(len(selected), 50)
        self.assertTrue(protected <= set(selected))
        with self.assertRaises(ValueError):
            subscription_set([], {str(i) for i in range(51)})

    def test_empty_and_observed_latency(self):
        self.assertEqual(latency_summary([]), {"count": 0})
        self.assertEqual(latency_summary([3.0, 1.0, 2.0])["max"], 3.0)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = Runtime(100, lambda *_args, **_kwargs: None)
        self.worker = asyncio.create_task(self.runtime.consume())
        self.at = datetime.now(UTC)

    async def asyncTearDown(self):
        self.worker.cancel()
        await asyncio.gather(self.worker, return_exceptions=True)
        await self.runtime.account.close()

    def book(self, at, price=10):
        return {
            "timestamp": at.isoformat(),
            "currency": "USD",
            "asks": [{"price": str(price), "volume": "100"}],
            "bids": [{"price": str(price - 0.01), "volume": "100"}],
        }

    async def drain(self):
        await asyncio.wait_for(self.runtime.queue.join(), 1)
        if self.worker.done():
            self.worker.result()

    async def test_rest_delay_does_not_block_market_consumer(self):
        pending = asyncio.create_task(asyncio.to_thread(time.sleep, 0.15))
        self.runtime.publish("book", ("A", self.book(self.at)))
        await self.drain()
        self.assertEqual(self.runtime.counts["book_events"], 1)
        self.assertFalse(pending.done())
        await pending

    async def test_late_rest_snapshot_does_not_overwrite_newer_stream_book(self):
        self.runtime.publish("book", ("A", self.book(self.at)))
        self.runtime.publish("book", ("A", self.book(self.at - timedelta(seconds=1), 9)))
        await self.drain()
        self.assertEqual(self.runtime.books["A"]["asks"][0]["price"], "10")
        self.assertEqual(self.runtime.counts["old_snapshot_ignored"], 1)

    async def test_pending_symbol_survives_candidate_replacement_and_reconnect_duplicate(self):
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

    async def test_delayed_feature_cannot_be_used_before_it_is_published(self):
        r = self.runtime
        r.publish("selection", ["A"])
        r.publish("book", ("A", self.book(self.at)))
        await self.drain()
        self.assertEqual(r.account.intents, [])
        self.assertEqual(r.counts["unusable_features"], 1)
