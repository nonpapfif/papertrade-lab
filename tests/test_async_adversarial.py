"""Adversarial failures, clock semantics, cancellation, and randomized ledger checks."""

import asyncio
import copy
import random
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from tests.test_async_layered_paper import event, stamp
from tools.async_layered_paper import AsyncLayeredPaper


async def exercise(seed: int, steps: int = 150) -> dict:
    rng = random.Random(seed)
    account = AsyncLayeredPaper(100)
    try:
        for tick in range(steps):
            symbol = rng.randrange(8)
            price = rng.choice([1.0, 5.0, 10.0, 25.0, 110.0])
            e = event(symbol, tick, price=price, volume=rng.choice([0, 1, 3, 100]))
            e["feature"] = replace(
                e["feature"],
                available_at=stamp(tick),
                bar_end=stamp(tick),
                r5=rng.choice([-0.03, 0.01]),
                prior_high=price * 0.99,
            )
            await account.submit(e)
            if rng.random() < 0.2:
                await account.submit(copy.deepcopy(e))
            if rng.random() < 0.1:
                await account.advance(stamp(tick))
            assert account.book.cash >= -1e-9
            assert account.reserved_cash <= account.book.cash + 1e-9
            assert set(account.owners) == set(account.book.positions)
            assert all(
                p.quantity > 0 and float(p.quantity).is_integer()
                for p in account.book.positions.values()
            )
            assert abs(sum(account.realized_by_strategy.values()) - account.book.realized) < 1e-7
            account.summary()
        return account.summary()
    finally:
        await account.close()


class AdversarialTests(unittest.IsolatedAsyncioTestCase):
    async def account(self):
        a = AsyncLayeredPaper(100)
        self.addAsyncCleanup(a.close)
        return a

    async def test_malformed_feature_returns_error_instead_of_orphaning_waiter(self):
        a = await self.account()
        with self.assertRaises(TypeError):
            await asyncio.wait_for(a.submit(event(feature={})), 0.2)
        await asyncio.wait_for(a.submit(event(at=1)), 0.2)
        self.assertEqual(len(a.pending), 1)

    async def test_feature_for_other_symbol_cannot_create_a_request(self):
        a = await self.account()
        e = event()
        e["feature"] = replace(e["feature"], symbol=999)
        with self.assertRaises(ValueError):
            await a.submit(e)
        self.assertEqual(a.intents, [])

    async def test_nonfinite_feature_cannot_create_a_request(self):
        a = await self.account()
        e = event()
        e["feature"] = replace(e["feature"], r5=float("inf"))
        with self.assertRaises(ValueError):
            await a.submit(e)
        self.assertEqual(a.intents, [])

    async def test_fresh_held_quote_is_not_stale_merely_because_clock_advanced(self):
        a = await self.account()
        await a.submit(event())
        await a.submit(event(at=2))
        await a.advance(stamp(3))
        self.assertEqual(a.summary()["account"]["stale_open_positions"], 0)
        await a.advance(stamp(63))
        self.assertEqual(a.summary()["account"]["stale_open_positions"], 1)

    async def test_equivalent_timezone_does_not_change_staleness(self):
        a = await self.account()
        await a.submit(event())
        e = event(at=2)
        e["book"]["timestamp"] = (
            datetime.fromisoformat(stamp(2)).astimezone(timezone(timedelta(hours=9))).isoformat()
        )
        await a.submit(e)
        self.assertEqual(a.summary()["account"]["stale_open_positions"], 0)

    async def test_cancelling_ack_wait_does_not_duplicate_accepted_event(self):
        a = await self.account()
        future = a.enqueue(event())
        future.cancel()
        await a.submit(event())
        self.assertEqual(len(a.intents), 1)
        await a.submit(event(at=2))
        self.assertEqual(a.book.opened, 1)

    async def test_randomized_partial_fills_and_duplicate_delivery_reconcile(self):
        for seed in range(5):
            await exercise(seed)

    async def test_replaying_identical_seed_is_deterministic(self):
        self.assertEqual(await exercise(22), await exercise(22))

    async def test_unexpected_worker_exception_reaches_caller_and_keeps_queue_alive(self):
        a = await self.account()
        with (
            patch.object(a, "_process", side_effect=AttributeError("Unexpected failure")),
            self.assertRaises(AttributeError),
        ):
            await asyncio.wait_for(a.submit(event()), 0.2)
        await asyncio.wait_for(a.submit(event(at=1)), 0.2)
        self.assertEqual(len(a.pending), 1)
