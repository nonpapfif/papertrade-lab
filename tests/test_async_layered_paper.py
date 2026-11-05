"""Adversarial causal execution and independent symbol progress checks."""

import asyncio
import copy
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from tools.async_layered_paper import AsyncLayeredPaper
from tools.layered_paper import Feature
from tools.paper_market import KR


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


def kr_event(at: int, ask: int = 5000, bid: int = 4995, volume: int = 100) -> dict[str, Any]:
    stamp = (datetime(2026, 11, 5, 9, tzinfo=KR.timezone) + timedelta(seconds=at)).isoformat()
    return {
        "id": f"kr:{at}",
        "symbol": 0,
        "received_at": stamp,
        "feature": Feature(0, ask, stamp, stamp, 0.01, 0.02, 4950, 4950, 0.8),
        "eligible": True,
        "book": {
            "timestamp": stamp,
            "currency": "KRW",
            "asks": [{"price": str(ask), "volume": str(volume)}],
            "bids": [{"price": str(bid), "volume": str(volume)}],
        },
    }


class KRAsyncLayerTests(unittest.IsolatedAsyncioTestCase):
    async def test_reservation_partial_exit_and_native_krw_ledger(self) -> None:
        account = AsyncLayeredPaper(25_000, market=KR, policy="trend")
        self.addAsyncCleanup(account.close)
        account.entry_policy_version = 2

        await account.submit(kr_event(0))
        self.assertEqual(account.intents[-1].quantity, 4)
        self.assertEqual(account.intents[-1].limit, 5000)
        self.assertAlmostEqual(account.reserved_cash, 20_003)
        self.assertEqual(account.book.cash, 25_000)
        self.assertEqual(account.book.opened, 0)
        await account.submit(kr_event(2, ask=4995, bid=4990, volume=3))
        entry_commission = 4995 * 3 * 0.00015
        self.assertEqual(account.book.positions[0].quantity, 3)
        self.assertEqual(account.reserved_cash, 0)
        self.assertAlmostEqual(account.book.cash, 25_000 - 4995 * 3 - entry_commission)
        self.assertEqual(account.book.trades[-1]["source_at"], kr_event(2)["received_at"])

        account.policy = "rebound"
        account.entry_policy_version = 3
        invalidation = kr_event(3, ask=4990, bid=4985)
        invalidation["feature"] = replace(invalidation["feature"], r15=-0.01)
        await account.submit(invalidation)
        self.assertEqual(account.intents[-1].strategy, "trend")
        self.assertEqual(account.intents[-1].policy_version, 2)
        await account.submit(kr_event(5, ask=4990, bid=4985, volume=1))
        summary = account.summary()
        self.assertEqual(account.outcomes[-1]["status"], "partial")
        self.assertEqual(summary["owned_positions"], {"0": "trend"})
        self.assertEqual(account.owner_policy_versions, {0: 2})
        self.assertEqual(summary["positions"]["0"]["quantity"], 2)
        self.assertAlmostEqual(
            summary["positions"]["0"]["remaining_entry_fees_krw"], 2 * entry_commission / 3
        )
        krw_account = summary["account"]
        self.assertEqual(krw_account["accounting_currency"], "KRW")
        sale_tax = 4985 * 0.002
        sale_commission = 4985 * 0.00015
        self.assertAlmostEqual(krw_account["stock_sale_tax_krw"], sale_tax)
        self.assertAlmostEqual(
            krw_account["fees_krw"], entry_commission + sale_commission + sale_tax
        )
        self.assertAlmostEqual(
            krw_account["cash"], 25_000 - 3 * 4995 + 4985 - krw_account["fees_krw"]
        )
        self.assertAlmostEqual(sum(summary["realized_by_strategy"].values()), account.book.realized)
        self.assertNotIn("fees_usd", krw_account)
        self.assertNotIn("observed_fx_krw_per_usd", account.book.trades[-1])

    async def test_currency_ticks_depth_and_confirmation_limit(self) -> None:
        account = AsyncLayeredPaper(10_000, market=KR, policy="trend")
        self.addAsyncCleanup(account.close)
        foreign_currency = kr_event(0)
        foreign_currency["book"]["currency"] = "USD"
        await account.submit(foreign_currency)
        await account.submit(kr_event(1, ask=5005))
        fractional = kr_event(2)
        fractional["book"]["asks"][0]["volume"] = "1.5"
        await account.submit(fractional)
        await account.submit(kr_event(3, bid=5010))
        self.assertEqual(account.intents, [])

        with_placeholder = kr_event(4)
        with_placeholder["book"]["asks"].insert(0, {"price": "0", "volume": "0"})
        await account.submit(with_placeholder)
        self.assertEqual(account.prices[0], 4997.5)
        self.assertEqual(account.intents[-1].quantity, 1)
        await account.submit(kr_event(6, ask=5010))
        self.assertEqual(account.outcomes[-1]["status"], "unfilled")
        self.assertEqual(account.book.cash, 10_000)
        self.assertEqual(account.reserved_cash, 0)
        self.assertEqual(account.book.quote_failure_counts["quote_invalid"], 2)
        self.assertEqual(account.book.quote_failure_counts["quote_crossed"], 1)

    async def test_unusable_book_clears_executable_depth_but_keeps_last_mark(self) -> None:
        for reason in ("crossed", "empty", "off_tick"):
            with self.subTest(reason=reason):
                account = AsyncLayeredPaper(10_000, market=KR, policy="trend")
                self.addAsyncCleanup(account.close)
                await account.submit(kr_event(0))
                filled = kr_event(2)
                filled["book"]["bids"].insert(0, {"price": "5010", "volume": "0"})
                await account.submit(filled)
                original = account.summary()
                self.assertEqual(original["positions"]["0"]["mark_price"], 4997.5)
                self.assertIsNotNone(
                    original["account"]["bid_liquidation_equity_estimate_krw"]
                )

                unusable = kr_event(4)
                if reason == "crossed":
                    unusable["book"]["bids"][0]["price"] = "5010"
                elif reason == "empty":
                    unusable["book"]["bids"][0]["volume"] = "0"
                else:
                    unusable["book"]["bids"][0]["price"] = "4994"
                await account.submit(unusable)
                delayed = kr_event(5)
                delayed["book"]["timestamp"] = kr_event(3)["received_at"]
                await account.submit(delayed)
                await account.advance(delayed["received_at"])
                summary = account.summary()

                self.assertEqual(
                    summary["positions"]["0"]["mark_price"],
                    original["positions"]["0"]["mark_price"],
                )
                self.assertEqual(
                    summary["positions"]["0"]["mark_at"],
                    original["positions"]["0"]["mark_at"],
                )
                self.assertIsNone(summary["account"]["bid_liquidation_equity_estimate_krw"])
                self.assertEqual(summary["account"]["bid_liquidation_missing_symbols"], [0])

    def test_ordinary_stock_tick_boundaries(self) -> None:
        for upper, tick_below, tick_above in (
            (2000, 1, 5),
            (5000, 5, 10),
            (20_000, 10, 50),
            (50_000, 50, 100),
            (200_000, 100, 500),
            (500_000, 500, 1000),
        ):
            with self.subTest(price=upper):
                self.assertEqual(KR.limit_price(upper - 0.25, "BUY"), upper - tick_below)
                self.assertEqual(KR.limit_price(upper - 0.25, "SELL"), upper)
                self.assertEqual(KR.limit_price(upper + 0.25, "BUY"), upper)
                self.assertEqual(KR.limit_price(upper + 0.25, "SELL"), upper + tick_above)
