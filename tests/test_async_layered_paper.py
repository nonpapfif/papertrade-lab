"""Adversarial causal execution and independent symbol progress checks."""

import asyncio
import copy
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from tools.async_layered_paper import AsyncLayeredPaper
from tools.layered_paper import Feature


def stamp(seconds):
    return (datetime(2026, 11, 3, 15, tzinfo=UTC) + timedelta(seconds=seconds)).isoformat()


def event(symbol=0, at=0, *, volume=100, price=10, event_id=None, **kwargs):
    f = Feature(symbol, price, stamp(0), stamp(0), 0.01, 0.02, 9, 9, 0.8)
    return {
        "id": event_id or f"{symbol}:{at}",
        "symbol": symbol,
        "received_at": stamp(at),
        "feature": f,
        "eligible": True,
        "book": {
            "timestamp": stamp(at),
            "currency": "USD",
            "asks": [{"price": str(price), "volume": str(volume)}],
            "bids": [{"price": str(price - 0.01), "volume": str(volume)}],
        },
        "fx": 1400,
        "fx_at": stamp(at),
        **kwargs,
    }


class AsyncLayerTests(unittest.IsolatedAsyncioTestCase):
    async def account(self, capital=100, **kwargs):
        a = AsyncLayeredPaper(capital, **kwargs)
        self.addAsyncCleanup(a.close)
        return a

    async def test_other_symbol_decides_and_fills_before_delayed_quote(self):
        a = await self.account(200)
        await a.submit(event(0, 0, candidate_count=2))
        await asyncio.wait_for(a.submit(event(1, 1)), timeout=1)
        await asyncio.wait_for(a.submit(event(1, 3)), timeout=1)
        self.assertIn(1, a.book.positions)
        self.assertIn(0, a.pending)
        self.assertEqual(a.intents[1].decision_at, stamp(1))
        await a.submit(event(0, 30))
        self.assertIn(0, a.book.positions)
        self.assertGreaterEqual(a.book.cash, 0)

    async def test_concurrent_requests_never_double_reserve_cash(self):
        a = await self.account(100)
        await asyncio.gather(*(a.submit(event(s, 0)) for s in range(20)))
        self.assertLessEqual(a.reserved_cash, a.book.cash)
        self.assertAlmostEqual(a.available_cash + a.reserved_cash, a.book.cash)
        self.assertEqual(sum(i.quantity for i in a.intents), 9)
        self.assertEqual(a.book.opened, 0)
        await asyncio.gather(*(a.submit(event(s, 2, eligible=False)) for s in range(20)))
        self.assertGreaterEqual(a.book.cash, 0)
        self.assertEqual(a.reserved_cash, 0)

    async def test_future_changes_cannot_change_submitted_intents_or_reservations(self):
        snapshots = []
        for future_volume, future_price in [(100, 10), (1, 10), (100, 11), (0, 10)]:
            a = await self.account(200, allocation="priority")
            await a.submit(event(0, 0))
            await a.submit(event(1, 0.1))
            initial = copy.deepcopy(a.intents)
            reserved = a.reserved_cash
            snapshots.append((initial, reserved))
            await a.submit(event(0, 2, volume=future_volume, price=future_price))
            self.assertEqual(a.intents, initial)
            self.assertEqual(a.book.order_attempts[-1]["requested_quantity"], 19)
            self.assertGreaterEqual(a.book.cash, 0)
        self.assertTrue(all(s == snapshots[0] for s in snapshots))

    async def test_partial_fill_releases_unused_reservation_without_backdating(self):
        a = await self.account()
        await a.submit(event())
        await a.submit(event(at=2, volume=1))
        self.assertEqual(a.book.positions[0].quantity, 1)
        self.assertEqual(a.reserved_cash, 0)
        await a.submit(event(1, 3))
        self.assertEqual(a.intents[-1].decision_at, stamp(3))
        self.assertEqual(a.intents[-1].quantity, 8)

    async def test_duplicate_and_reconnected_snapshot_cannot_double_fill(self):
        a = await self.account()
        initial, confirmed = event(), event(at=2)
        await a.submit(initial)
        await a.submit(initial)
        await a.submit(confirmed)
        before = copy.deepcopy(a.book.order_attempts)
        await a.submit(confirmed)
        duplicate = event(at=3, event_id="reconnected")
        duplicate["book"] = confirmed["book"]
        await a.submit(duplicate)
        self.assertEqual(a.book.order_attempts, before)
        self.assertEqual(len(a.intents), 1)

    async def test_symbol_fifo_with_concurrent_queued_events(self):
        a = await self.account(200)
        events = [event(0, 0), event(1, 0.1), event(0, 2), event(1, 3)]
        await asyncio.gather(*(a.submit(e) for e in events))
        self.assertEqual(a.last_at, stamp(3))
        self.assertEqual(a.book.opened, 1)

    async def test_timeout_releases_cash_without_new_quote(self):
        a = await self.account()
        await a.submit(event())
        await a.advance(stamp(31))
        self.assertEqual(a.reserved_cash, 0)
        self.assertEqual(a.book.cash, 100)
        self.assertEqual(a.outcomes[-1]["status"], "expired")

    async def test_missing_future_quote_keeps_original_request(self):
        a = await self.account()
        await a.submit(event())
        original = copy.deepcopy(a.intents)
        await a.advance(stamp(20))
        self.assertEqual(a.intents, original)
        self.assertEqual(len(a.pending), 1)
        self.assertEqual(a.book.opened, 0)

    async def test_partial_exit_retains_owner_and_does_not_free_unfilled_proceeds(self):
        a = await self.account()
        await a.submit(event())
        await a.submit(event(at=2))
        e = event(at=3, price=8)
        e["feature"] = replace(e["feature"], price=8, r5=-0.01)
        await a.submit(e)
        quantity = a.book.positions[0].quantity
        cash = a.book.cash
        self.assertEqual(a.available_cash, cash)
        await a.submit(event(at=5, price=8, volume=1))
        self.assertEqual(a.book.positions[0].quantity, quantity - 1)
        self.assertIn(0, a.owners)
        self.assertLess(a.book.cash - cash, 8)
        self.assertAlmostEqual(sum(a.realized_by_strategy.values()), a.book.realized)

    async def test_invalid_event_failure_is_visible_and_worker_can_continue(self):
        a = await self.account()
        e = event()
        e["book"]["asks"][0]["price"] = "nan"
        with self.assertRaises(ValueError):
            await a.submit(e)
        await a.submit(event(at=1))
        self.assertEqual(len(a.pending), 1)

    async def test_position_evidence_reconciles_partial_fills_and_mixed_age_marks(self) -> None:
        a = await self.account(200)
        await a.submit(event())
        await a.submit(event(at=2, volume=5))
        invalid = event(at=3, price=8)
        invalid["feature"] = replace(invalid["feature"], r5=-0.01)
        await a.submit(invalid)
        await a.submit(event(at=5, price=8, volume=2))
        await a.submit(event(1, 6, price=20))
        await a.submit(event(1, 8, price=20, volume=2))
        await a.advance(stamp(67))

        result = a.summary()
        first, second = result["positions"]["0"], result["positions"]["1"]
        self.assertEqual(result["positions_as_of"], stamp(67))
        self.assertEqual(first["strategy"], "breakout")
        self.assertEqual(first["quantity"], 3)
        self.assertEqual(first["entry_price"], 10)
        self.assertEqual(first["entry_cost_usd"], 30)
        self.assertAlmostEqual(first["remaining_entry_fees_usd"], 0.03)
        self.assertAlmostEqual(first["mark_price"], 7.995)
        self.assertEqual(first["mark_at"], stamp(5))
        self.assertEqual(first["mark_age_seconds"], 62)
        self.assertAlmostEqual(first["market_value_usd"], 23.985)
        self.assertAlmostEqual(first["unrealized_usd"], -6.015)
        self.assertEqual(second["quantity"], 2)
        self.assertEqual(second["entry_cost_usd"], 40)
        self.assertAlmostEqual(second["remaining_entry_fees_usd"], 0.04)
        self.assertEqual(second["mark_at"], stamp(8))
        self.assertEqual(second["mark_age_seconds"], 59)
        account = result["account"]
        self.assertEqual(account["stale_open_positions"], 1)
        self.assertAlmostEqual(account["cash"], 125.86402)
        self.assertAlmostEqual(account["realized"], -4.13598)
        self.assertAlmostEqual(account["unrealized"], -6.025)
        self.assertAlmostEqual(account["equity"], 189.83902)
        positions = result["positions"].values()
        self.assertAlmostEqual(
            sum(p["unrealized_usd"] for p in positions), account["unrealized"]
        )
        self.assertAlmostEqual(
            account["cash"] + sum(p["market_value_usd"] for p in positions), account["equity"]
        )
        await a.advance(stamp(68))
        self.assertEqual(first["mark_age_seconds"], 62)
        self.assertEqual(a.summary()["positions"]["0"]["mark_age_seconds"], 63)

    async def test_stale_and_future_quotes_cannot_create_intents(self):
        a = await self.account()
        e = event()
        e["book"]["timestamp"] = stamp(1)
        await a.submit(e)
        e = event(at=100)
        e["book"]["timestamp"] = stamp(0)
        await a.submit(e)
        self.assertEqual(a.intents, [])

    async def test_changed_book_with_same_source_timestamp_is_not_discarded(self):
        a = await self.account()
        await a.submit(event(eligible=False))
        changed = event(at=0.1, price=9.9)
        changed["book"]["timestamp"] = stamp(0)
        await a.submit(changed)
        self.assertEqual(len(a.intents), 1)
        self.assertEqual(a.intents[0].limit, 9.9)

    async def test_new_exit_feature_is_processed_with_duplicate_quote(self):
        a = await self.account()
        await a.submit(event())
        await a.submit(event(at=2))
        await a.submit(event(at=3, eligible=False))
        exit_signal = event(at=4, event_id="new-feature")
        exit_signal["book"] = copy.deepcopy(a._books[0])
        exit_signal["feature"] = replace(
            exit_signal["feature"],
            price=8,
            available_at=stamp(4),
            bar_end=stamp(4),
            r5=-0.01,
        )
        await a.submit(exit_signal)
        self.assertEqual(a.intents[-1].side, "SELL")
        self.assertIn(0, a.pending)
        self.assertEqual(a.book.opened, 1)

    async def test_us_buy_limit_is_normalized_without_changing_quote(self):
        a = await self.account(100)
        at_or_above_one = event(price=1.001)
        at_or_above_one["book"]["bids"][0]["price"] = "1.0"
        await a.submit(at_or_above_one)
        self.assertEqual(a.pending[0].limit, 1.0)
        self.assertEqual(a.pending[0].initial["asks"][0]["price"], "1.001")

        sub_dollar = await self.account(100)
        quote = event(price=0.12349)
        quote["book"]["bids"][0]["price"] = "0.1234"
        await sub_dollar.submit(quote)
        self.assertEqual(sub_dollar.pending[0].limit, 0.1234)
        self.assertEqual(sub_dollar.pending[0].initial["asks"][0]["price"], "0.12349")

        unavailable = await self.account(100)
        tiny = event(price=0.00001)
        tiny["book"]["bids"][0]["price"] = "0.00001"
        await unavailable.submit(tiny)
        self.assertEqual(unavailable.intents, [])
        self.assertEqual(float(unavailable._books[0]["asks"][0]["price"]), 0.00001)

    async def test_unfunded_sell_leaves_account_unchanged_and_records_attempt(self):
        a = await self.account(1.006)
        penny = event(price=0.005, volume=1)
        penny["book"]["bids"][0]["price"] = "0.005"
        await a.submit(penny)
        confirmation = event(at=2, price=0.005, volume=1)
        confirmation["book"]["bids"][0]["price"] = "0.005"
        await a.submit(confirmation)
        reserved_entry = event(1, 3, price=1.0, volume=10)
        reserved_entry["feature"] = replace(reserved_entry["feature"], prior_high=0.9)
        await a.submit(reserved_entry)
        self.assertAlmostEqual(a.reserved_cash, 1.0)
        before_cash = a.book.cash
        before_position = copy.deepcopy(a.book.positions[0])
        invalidation = event(at=4, price=0.005, volume=10)
        invalidation["book"]["bids"][0]["price"] = "0.005"
        invalidation["feature"] = replace(
            invalidation["feature"],
            price=0.004,
            available_at=stamp(4),
            bar_end=stamp(4),
            r5=-0.01,
            r15=-0.01,
        )
        await a.submit(invalidation)
        sale = event(at=5, price=0.005, volume=10)
        sale["book"]["bids"][0]["price"] = "0.005"
        await a.submit(sale)
        self.assertEqual(a.book.cash, before_cash)
        self.assertEqual(a.book.positions[0], before_position)
        self.assertAlmostEqual(a.reserved_cash, 1.0)
        self.assertEqual(a.outcomes[-1]["status"], "unfilled")
        self.assertEqual(a.outcomes[-1]["blocked_reason"], "insufficient_cash_after_sell_costs")

    async def test_full_replay_prefix_is_independent_of_future_and_missing_confirmation(self):
        from tools.replay_layered_stream import replay_case

        base = event()
        books = {0: [{"received_at": stamp(t), **event(at=t)["book"]} for t in (0, 2, 4)]}
        config = {
            "policy": "router",
            "selection": "all",
            "allocation": "priority",
            "delay": 1,
            "cadence": 0,
            "depth": 1.0,
        }
        history = {0: [(0.0, base["feature"])]}
        fx = [{"received_at": stamp(0), "result": {"midRate": 1400}}]
        from tools.async_layered_paper import seconds

        async def run(rows):
            return await replay_case(
                100,
                config,
                rows,
                history,
                {0: [0.0]},
                fx,
                [seconds(stamp(0))],
                {0},
                seconds(stamp(0)),
            )

        prefix = await run({0: books[0][:1]})
        full = await run(books)
        altered = copy.deepcopy(books)
        altered[0][1]["asks"][0]["price"] = "15"
        altered[0][1]["asks"][0]["volume"] = "0"
        changed = await run(altered)
        self.assertEqual(prefix.intents, full.intents[:1])
        self.assertEqual(prefix.intents, changed.intents[:1])
        self.assertEqual(prefix.reserved_cash, prefix.intents[0].reserved)
        self.assertEqual(prefix.book.opened, 0)
        self.assertEqual(full.book.opened, 1)

    async def test_zero_depth_top_level_cannot_underreserve_cash(self):
        a = await self.account()
        e = event()
        e["book"]["asks"].insert(0, {"price": "9.99", "volume": "0"})
        await a.submit(e)
        self.assertEqual(a.intents[0].limit, 10)
        self.assertAlmostEqual(a.reserved_cash, 90.09)
        await a.submit(event(at=2))
        self.assertGreaterEqual(a.book.cash, 0)
