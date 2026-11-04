"""Cash, fees, depth, taxes and causal preflight of virtual live execution."""

from __future__ import annotations

import copy
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

from tools.live_execution import (
    DEFAULT_FEES,
    ExecutionBook,
    ExecutionContext,
    LiveWeekEngine,
    transaction_fees,
    us_limit_price,
)
from tools.run_live_parallel import live_design
from tools.run_week_experiment import long_policies
from tools.search_portfolios import configurations, signals

DAY = "2026-11-02"
START = datetime(2026, 11, 2, 14, 30, tzinfo=UTC)


def stamp(minute: int) -> str:
    return (START + timedelta(minutes=minute)).isoformat()


def scope() -> dict[str, Any]:
    return {
        "US": {
            "start": stamp(0),
            "end": stamp(60),
            "sessions": [
                {"start": stamp(0), "end": stamp(0)},
                {"start": stamp(0), "end": stamp(0)},
                {"start": stamp(0), "end": stamp(60)},
            ],
        }
    }


def frame(minute: int = 0, price: float = 10) -> dict[str, Any]:
    at = stamp(minute)
    return {
        "at": at,
        "tick": int(datetime.fromisoformat(at).timestamp() // 300),
        "quotes": {0: price},
        "marks": {0: [price, at]},
        "ranks": [[[], [[0, 1, 0.01]]]],
        "long_features": [[0, [0.01] * 30, [1] * 5, [1] * 5]],
        "fx": 1500,
        "fx_at": at,
        "fx_valid_from": stamp(-60),
        "fx_valid_until": stamp(60),
        "orderbooks": {},
    }


def orderbook(
    minute: int = 0, ask: float = 10, bid: float = 10, volume: int = 100
) -> dict[str, Any]:
    return {
        "timestamp": stamp(minute),
        "currency": "USD",
        "asks": [{"price": ask, "volume": volume}],
        "bids": [{"price": bid, "volume": volume}],
    }


def engine(mode: str = "quotes", **config_changes: Any) -> LiveWeekEngine:
    config = {**configurations(1)[0], **config_changes}
    result = LiveWeekEngine([config], signals()[:1], [{"id": "learn", "mode": "daily"}], mode=mode)
    result.day_start(DAY, scope())
    return result


def book(mode: str = "quotes", capital: float = 100) -> ExecutionBook:
    context = ExecutionContext(mode, dict(DEFAULT_FEES), frame())
    return ExecutionBook(
        configurations(1)[0], "US", "actual", True, context=context, initial_equity=capital
    )


class LiveExecutionTests(unittest.TestCase):
    def test_provider_fx_validity_controls_krw_reporting_only(self) -> None:
        target = book("last")
        target.context.frame = frame(5)
        self.assertEqual(target._current_fx(), 1500)
        for valid_from, valid_until, rate in (
            (stamp(-60), stamp(4), 1500),
            (stamp(6), stamp(60), 1500),
            (stamp(-60), stamp(60), 0),
            (stamp(-60), stamp(60), float("inf")),
        ):
            target.context.frame = {
                **frame(5),
                "fx": rate,
                "fx_valid_from": valid_from,
                "fx_valid_until": valid_until,
            }
            self.assertIsNone(target._current_fx())
        target.context.frame = {**frame(5), "fx_valid_until": stamp(5)}
        self.assertEqual(target._current_fx(), 1500)
        target.context.frame = {**frame(5), "fx_valid_from": stamp(6)}
        target.fill(0, 1, 10, stamp(5), 1, "entry")
        self.assertEqual(target.positions[0].quantity, 1)
        self.assertIsNone(target.trades[-1]["observed_fx_krw_per_usd"])

    def test_full_live_cohort_reconciles_with_confirmed_partial_exits(self) -> None:
        configs, specs, policies = live_design()
        target = LiveWeekEngine(
            configs,
            specs,
            policies,
            mode="quotes",
            fee_schedule={"confirmation_delay_seconds": 1, "allow_partial_exits": 1},
        )
        target.day_start(DAY, scope())
        for minute, bid, volume in ((0, 10, 100), (5, 9, 2), (10, 9, 100)):
            sample = book()
            self.confirmed(sample, minute, bid=bid, initial_bid=bid, volume=volume)
            incoming = sample.context.frame
            incoming["marks"] = {0: [10, incoming["at"]]}
            incoming["ranks"] = [[[], [[0, 1, 0.01]]] for _ in specs]
            target.step(incoming)
            result = target.report()
            self.assertEqual(len(result["hypotheses"]), 59)
            self.assertTrue(all(row["summary"]["cash"] >= 0 for row in result["hypotheses"]))
        self.assertTrue(any(row["summary"]["partial_orders"] for row in result["hypotheses"]))

    def confirmed(
        self,
        target: ExecutionBook,
        minute: int = 0,
        *,
        ask: float = 10,
        bid: float = 10,
        initial_ask: float = 10,
        initial_bid: float = 10,
        volume: int = 100,
        delay: float = 1.1,
    ) -> None:
        target.context.schedule.update(confirmation_delay_seconds=1, allow_partial_exits=1)
        arrival = (START + timedelta(minutes=minute, seconds=delay)).isoformat()
        snapshot = orderbook(minute, ask=ask, bid=bid, volume=volume)
        snapshot["timestamp"] = arrival
        target.context.frame = {
            **frame(minute),
            "at": arrival,
            "decision_at": stamp(minute),
            "decision_orderbooks": {0: orderbook(minute, ask=initial_ask, bid=initial_bid)},
            "confirmation_received_at": {0: arrival},
            "orderbooks": {0: snapshot},
            "fx_at": arrival,
            "fx_valid_from": stamp(-60),
            "fx_valid_until": stamp(60),
        }

    def test_confirmed_buy_cancels_when_ask_moves_beyond_initial_limit(self) -> None:
        target = book()
        self.confirmed(target, ask=11)
        self.assertEqual(target.whole_orders([[0, 1, 1]], {0: 10}, 1, 100, 100), [])
        self.assertEqual(target.cash, 100)
        self.assertFalse(target.positions)
        self.assertEqual(target.order_attempts[-1]["status"], "unfilled")
        self.assertEqual(target.order_attempts[-1]["requested_quantity"], 9)
        self.assertEqual(target.fee_totals["total_usd"], 0)

    def test_us_limit_precision_and_confirmation_use_the_frozen_normalized_limit(self) -> None:
        self.assertEqual(us_limit_price(1.001), 1.0)
        self.assertEqual(us_limit_price(0.12349), 0.1234)

        target = book()
        self.confirmed(target, ask=1.001, bid=1.0, initial_ask=1.001, initial_bid=1.0)
        target.context.frame["order_limits"] = {0: 1.0}
        source = target.context.frame["orderbooks"][0]["timestamp"]
        self.assertEqual(
            target._confirmation_levels(0, "BUY", [(1.001, 5)], source), []
        )
        self.assertEqual(target.context.frame["decision_orderbooks"][0]["asks"][0]["price"], 1.001)

        self.confirmed(
            target,
            ask=0.12345,
            bid=0.1234,
            initial_ask=0.12349,
            initial_bid=0.1234,
        )
        target.context.frame["order_limits"] = {0: 0.1234}
        source = target.context.frame["orderbooks"][0]["timestamp"]
        self.assertEqual(
            target._confirmation_levels(0, "BUY", [(0.12345, 5)], source), []
        )

        self.confirmed(target, ask=1.0, bid=1.009, initial_ask=1.001, initial_bid=1.009)
        target.context.frame["order_limits"] = {0: 1.0}
        source = target.context.frame["orderbooks"][0]["timestamp"]
        self.assertEqual(
            target._confirmation_levels(0, "SELL", [(1.009, 5)], source), [(1.009, 5)]
        )

    def test_confirmed_buy_records_partial_quantity_when_visible_depth_shrinks(self) -> None:
        target = book()
        self.confirmed(target, volume=2)
        orders = target.whole_orders([[0, 1, 1]], {0: 10}, 1, 100, 100)
        self.assertEqual(orders[0][1], 2)
        target.fill(0, 2, 10, target.context.frame["at"], 1, "entry")
        self.assertEqual(target.positions[0].quantity, 2)
        self.assertAlmostEqual(target.cash, 79.98)
        self.assertEqual(target.order_attempts[-1]["status"], "partial")
        self.assertEqual(target.order_attempts[-1]["unfilled_quantity"], 7)

    def test_confirmation_requires_latency_floor_and_a_new_provider_update(self) -> None:
        for cause in ("too_early", "old_provider_time"):
            with self.subTest(cause=cause):
                target = book()
                self.confirmed(target, delay=0.5 if cause == "too_early" else 1.1)
                if cause == "old_provider_time":
                    target.context.frame["orderbooks"][0]["timestamp"] = stamp(0)
                self.assertEqual(target.whole_orders([[0, 1, 1]], {0: 10}, 1, 100, 100), [])
                self.assertEqual(target.cash, 100)
                self.assertFalse(target.trades)

    def test_later_frame_cannot_legitimize_a_quote_from_after_its_receipt(self) -> None:
        target = book()
        self.confirmed(target)
        target.context.frame["orderbooks"][0]["timestamp"] = (
            START + timedelta(seconds=2)
        ).isoformat()
        target.context.frame["at"] = (START + timedelta(seconds=10)).isoformat()
        self.assertIsNone(target._quote(0, "SELL", 10, record_failure=False))

    def test_partial_exits_preserve_basis_remaining_position_and_net_position_win(self) -> None:
        target = book()
        self.confirmed(target)
        target.fill(0, 9, 10, target.context.frame["at"], 1, "entry")
        self.confirmed(target, 5, bid=11, initial_bid=11, volume=4)
        target.fill(0, -9, 11, target.context.frame["at"], 2, "holding_period")
        self.assertEqual(target.positions[0].quantity, 5)
        self.assertEqual(target.closed, 0)
        self.assertEqual(target.wins, 0)
        self.assertAlmostEqual(target.entry_fees[0], 0.05)
        self.assertAlmostEqual(target.entry_cost_krw[0], 75075)
        self.assertFalse(target.trades[-1]["position_closed"])
        summary = target.summary(
            {0: 11}, {0: target.context.frame["at"]}, target.context.frame["at"]
        )
        self.assertAlmostEqual(summary["equity"], 108.856)
        self.confirmed(target, 10, bid=9, initial_bid=9, volume=5)
        target.fill(0, -5, 9, target.context.frame["at"], 3, "holding_period")
        self.assertFalse(target.positions)
        self.assertEqual(target.closed, 1)
        self.assertEqual(target.wins, 0)
        self.assertAlmostEqual(target.cash, 98.801)
        self.assertAlmostEqual(target.realized, -1.199)
        self.assertAlmostEqual(target.context.tax_realized_krw["actual"], -1.199 * 1500)
        self.assertTrue(target.trades[-1]["position_closed"])
        self.assertEqual(target.order_attempts[1]["status"], "partial")

    def test_unfilled_exit_keeps_the_position_and_charges_no_exit_fee(self) -> None:
        target = book()
        self.confirmed(target)
        target.fill(0, 9, 10, target.context.frame["at"], 1, "entry")
        before = target.cash, target.fee_totals["total_usd"]
        self.confirmed(target, 5, bid=8, initial_bid=9)
        target.fill(0, -9, 9, target.context.frame["at"], 2, "stop_at_observed_check")
        self.assertEqual((target.cash, target.fee_totals["total_usd"]), before)
        self.assertEqual(target.positions[0].quantity, 9)
        self.assertEqual(target.order_attempts[-1]["filled_quantity"], 0)

    def test_partial_exit_win_uses_the_complete_position_not_the_last_fill(self) -> None:
        target = book()
        self.confirmed(target)
        target.fill(0, 9, 10, target.context.frame["at"], 1, "entry")
        self.confirmed(target, 5, bid=12, initial_bid=12, volume=4)
        target.fill(0, -9, 12, target.context.frame["at"], 2, "holding_period")
        self.confirmed(target, 10, bid=9, initial_bid=9, volume=5)
        target.fill(0, -5, 9, target.context.frame["at"], 3, "holding_period")
        self.assertLess(target.trades[-1]["realized_pnl"], 0)
        self.assertEqual(target.closed, 1)
        self.assertEqual(target.wins, 1)
        self.assertAlmostEqual(target.cash, 102.797)

    def test_partial_exit_cannot_sell_more_shares_than_the_account_holds(self) -> None:
        target = book()
        self.confirmed(target)
        target.fill(0, 9, 10, target.context.frame["at"], 1, "entry")
        before = target.cash
        with self.assertRaisesRegex(ValueError, "sell more"):
            target.fill(0, -10, 10, target.context.frame["at"], 2, "holding_period")
        self.assertEqual(target.positions[0].quantity, 9)
        self.assertEqual(target.cash, before)

    def test_last_fill_keeps_provider_signal_and_execution_times_separate(self) -> None:
        target = book("last")
        target.context.frame.update(
            source_times={0: stamp(-1)},
            signal_at=stamp(0),
            price_observed_at=stamp(0),
        )
        target.fill(0, 1, 10, stamp(1), 1, "entry")
        self.assertEqual(target.trades[0]["source_at"], stamp(-1))
        self.assertEqual(target.trades[0]["signal_at"], stamp(0))
        self.assertEqual(target.trades[0]["fill_at"], stamp(1))

    def test_fee_threshold_sec_estimate_and_zero_taf_schedule(self) -> None:
        self.assertEqual(transaction_fees(10, 1, "BUY", DEFAULT_FEES)["total_usd"], 0)
        self.assertAlmostEqual(transaction_fees(11, 1, "BUY", DEFAULT_FEES)["total_usd"], 0.011)
        sale = transaction_fees(100, 10, "SELL", DEFAULT_FEES)
        self.assertEqual(sale["sec_usd"], 0.01)
        self.assertEqual(sale["taf_usd"], 0)
        self.assertAlmostEqual(sale["total_usd"], 0.11)

    def test_whole_quantity_includes_commission_without_negative_cash(self) -> None:
        target = book()
        target.context.frame["orderbooks"][0] = orderbook()
        orders = target.whole_orders([[0, 1, 1]], {0: 10}, 1, 100, 100)
        self.assertEqual(orders, [([0, 1, 1], 9)])
        target.fill(0, 9, 10, stamp(0), 1, "entry")
        self.assertAlmostEqual(target.cash, 9.91)
        self.assertEqual(target.positions[0].quantity, 9)
        self.assertAlmostEqual(target.fee_totals["total_usd"], 0.09)

    def test_depth_weighted_ask_and_bid_reconcile_net_realized_pnl(self) -> None:
        target = book()
        snapshot = orderbook(bid=9.5)
        snapshot["asks"] = [{"price": 10, "volume": 2}, {"price": 11, "volume": 10}]
        target.context.frame["orderbooks"][0] = snapshot
        self.assertEqual(target._buy_quantity(0, 100, 10), 9)
        target.fill(0, 9, 10, stamp(0), 1, "entry")
        self.assertAlmostEqual(target.positions[0].entry, 97 / 9)
        self.assertAlmostEqual(target.cash, 2.903)
        target.context.frame = {**frame(5), "orderbooks": {0: orderbook(5, bid=9.5)}}
        target.fill(0, -9, 10, stamp(5), 2, "holding_period")
        summary = target.summary({0: 10}, {0: stamp(5)}, stamp(5))
        self.assertAlmostEqual(summary["equity"], 88.3075)
        self.assertAlmostEqual(summary["realized"], -11.6925)
        self.assertAlmostEqual(target.trades[-1]["realized_pnl"], -11.6925)
        self.assertEqual(target.trades[-1]["source_at"], stamp(5))
        self.assertEqual(summary["debt"], 0)

    def test_missing_stale_future_and_foreign_books_never_execute_actual_entries(self) -> None:
        for failure in ("missing", "stale", "future", "currency"):
            with self.subTest(failure=failure):
                target = engine()
                incoming = frame()
                if failure != "missing":
                    snapshot = orderbook()
                    if failure == "stale":
                        snapshot["timestamp"] = (START - timedelta(seconds=61)).isoformat()
                    elif failure == "future":
                        snapshot["timestamp"] = (START + timedelta(seconds=1)).isoformat()
                    else:
                        snapshot["currency"] = "KRW"
                    incoming["orderbooks"][0] = snapshot
                target.step(incoming)
                actual = target.states[0]["book"]
                self.assertFalse(actual.positions)
                self.assertEqual(actual.cash, 100)
                self.assertFalse(actual.trades)
                self.assertTrue(actual.quote_failure_counts)
                self.assertEqual(target.shadows[0].positions[0].quantity, 9)

    def test_preflight_collects_quotes_without_mutating_real_accounts(self) -> None:
        target = engine(hold=0)
        incoming = frame()
        self.assertEqual(target.quote_requests(incoming), {0})
        self.assertEqual(target.states[0]["book"].cash, 100)
        self.assertFalse(target.states[0]["book"].trades)
        self.assertEqual(target.day_steps, 0)
        incoming["orderbooks"][0] = orderbook()
        self.assertFalse(target.quote_requests(incoming))
        target.step(incoming)
        next_frame = frame(5)
        self.assertEqual(target.required_symbols(next_frame), {0})
        next_frame["orderbook_failures"] = {0: "Provider unavailable"}
        self.assertFalse(target.quote_requests(next_frame))

    def test_insufficient_depth_never_fabricates_partial_exit(self) -> None:
        target = book()
        target.context.frame["orderbooks"][0] = orderbook()
        target.fill(0, 9, 10, stamp(0), 1, "entry")
        target.context.frame = {**frame(5), "orderbooks": {0: orderbook(5, volume=8)}}
        before = target.cash
        target.fill(0, -9, 10, stamp(5), 2, "holding_period")
        self.assertEqual(target.positions[0].quantity, 9)
        self.assertEqual(target.cash, before)
        self.assertEqual(target.closed, 0)
        self.assertEqual(target.quote_failure_counts["insufficient_depth"], 1)

    def test_fresh_bid_can_exit_a_holding_when_the_last_trade_is_stale(self) -> None:
        target = engine()
        incoming = frame()
        incoming["orderbooks"][0] = orderbook()
        target.step(incoming)
        stale_last = frame(5)
        stale_last.update(
            quotes={},
            marks={0: [10, stamp(0)]},
            ranks=[[[], []]],
            long_features=[],
            orderbooks={0: orderbook(5, ask=10, bid=9)},
        )
        target.step(stale_last)
        actual = target.states[0]["book"]
        self.assertFalse(actual.positions)
        self.assertEqual(actual.closed, 1)
        self.assertEqual(actual.trades[-1]["price"], 9)
        self.assertEqual(actual.trades[-1]["source_at"], stamp(5))
        self.assertEqual(actual.trades[-1]["reason"], "holding_period")

    def test_quote_mode_stop_uses_executable_bid_instead_of_a_higher_last_trade(self) -> None:
        target = engine(hold=0, stop=0.05)
        incoming = frame()
        incoming["orderbooks"][0] = orderbook()
        target.step(incoming)
        next_frame = frame(5, price=10.5)
        next_frame["orderbooks"][0] = orderbook(5, ask=11, bid=9)
        target.step(next_frame)
        actual = target.states[0]["book"]
        exits = [trade for trade in actual.trades if trade["realized_pnl"] is not None]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["reason"], "stop_at_observed_check")
        self.assertEqual(exits[0]["price"], 9)
        self.assertTrue(all(trade["cash"] >= 0 for trade in actual.trades))

    def test_same_account_consumes_depth_but_other_virtual_accounts_are_independent(self) -> None:
        first, second = book(), book()
        snapshot = orderbook(ask=1, bid=1, volume=8)
        snapshot["bids"][0]["volume"] = 5
        for target in (first, second):
            target.context.frame["orderbooks"][0] = snapshot
            target.fill(0, 5, 1, stamp(0), 1, "entry")
        first.fill(0, -5, 1, stamp(0), 1, "holding_period")
        self.assertEqual(first._buy_quantity(0, 100, 1), 3)
        first.fill(0, 3, 1, stamp(0), 1, "entry")
        first.fill(0, -3, 1, stamp(0), 1, "holding_period")
        self.assertEqual(first.positions[0].quantity, 3)
        self.assertEqual(second.positions[0].quantity, 5)

    def test_annual_tax_is_reported_without_debiting_trading_cash(self) -> None:
        target = book("last")
        target.context.schedule["annual_realized_before_run_krw"] = 3_000_000
        target.fill(0, 10, 10, stamp(0), 1, "entry")
        target.context.frame = frame(5, price=20)
        target.fill(0, -10, 20, stamp(5), 2, "holding_period")
        summary = target.summary({0: 20}, {0: stamp(5)}, stamp(5))
        self.assertEqual(target.cash, 200)
        self.assertEqual(summary["estimated_annual_realized_krw"], 3_150_000)
        self.assertEqual(summary["estimated_capital_tax_reserve_krw"], 143_000)
        self.assertEqual(summary["capital_tax_cash_deducted_usd"], 0)
        self.assertAlmostEqual(summary["after_tax_mark_equity_estimate_usd"], 200 - 143_000 / 1500)

    def test_tax_tracks_entry_and_exit_fx_on_principal_and_preserves_prior_losses(self) -> None:
        target = book("last")
        target.fill(0, 10, 10, stamp(0), 1, "entry")
        target.context.frame = {**frame(5), "fx": 1600}
        target.fill(0, -10, 10, stamp(5), 2, "holding_period")
        self.assertEqual(target.realized, 0)
        self.assertEqual(target.context.tax_realized_krw["actual"], 10_000)
        self.assertEqual(target.trades[0]["observed_fx_krw_per_usd"], 1500)
        self.assertEqual(target.trades[-1]["entry_cost_basis_krw"], 150_000)
        result = LiveWeekEngine(
            [configurations(1)[0]],
            signals()[:1],
            [{"id": "fixed", "mode": "static"}],
            mode="last",
            fee_schedule={"annual_realized_before_run_krw": -1_000_000},
        )
        result.day_start(DAY, scope())
        result.step(frame())
        summary = result.states[0]["book"].summary({0: 10}, {0: stamp(0)}, stamp(0))
        self.assertEqual(summary["estimated_annual_realized_krw"], -1_000_000)
        self.assertEqual(summary["estimated_capital_tax_reserve_krw"], 0)

    def test_missing_entry_fx_keeps_tax_unknown_after_a_profitable_exit(self) -> None:
        target = book("last")
        del target.context.frame["fx"]
        target.fill(0, 10, 10, stamp(0), 1, "entry")
        target.context.frame = frame(5, price=11)
        target.fill(0, -10, 11, stamp(5), 2, "holding_period")
        summary = target.summary({0: 11}, {0: stamp(5)}, stamp(5))
        self.assertEqual(summary["equity"], 110)
        self.assertIsNone(summary["estimated_capital_tax_reserve_krw"])
        self.assertIsNone(summary["after_tax_mark_equity_estimate_usd"])
        self.assertEqual(summary["capital_tax_cash_deducted_usd"], 0)

    def test_bid_liquidation_is_separate_from_last_mark_and_missing_depth_is_unknown(self) -> None:
        target = book()
        target.context.frame["orderbooks"][0] = orderbook(ask=10, bid=9)
        target.fill(0, 9, 10, stamp(0), 1, "entry")
        summary = target.summary({0: 10}, {0: stamp(0)}, stamp(0))
        self.assertAlmostEqual(summary["equity"], 99.91)
        self.assertAlmostEqual(summary["bid_liquidation_equity_estimate_usd"], 90.819)
        self.assertEqual(summary["bid_liquidation_coverage"], 1)
        self.assertEqual(target.closed, 0)
        self.assertEqual(target.context.tax_realized_krw, {})
        target.context.frame = frame(5)
        summary = target.summary({0: 10}, {0: stamp(5)}, stamp(5))
        self.assertIsNone(summary["bid_liquidation_equity_estimate_usd"])
        self.assertEqual(summary["bid_liquidation_coverage"], 0)

    def test_hypothetical_liquidation_tax_includes_unrealized_gains_and_exit_fees(self) -> None:
        target = book()
        target.context.frame["orderbooks"][0] = orderbook()
        target.fill(0, 9, 10, stamp(0), 1, "entry")
        target.context.frame = {
            **frame(5, price=250),
            "orderbooks": {0: orderbook(5, ask=250, bid=250)},
        }
        summary = target.summary({0: 250}, {0: stamp(5)}, stamp(5))
        net_proceeds = 9 * 250 - transaction_fees(9 * 250, 9, "SELL", DEFAULT_FEES)["total_usd"]
        projected_annual = net_proceeds * 1500 - (9 * 10 + 0.09) * 1500
        projected_tax = (projected_annual - 2_500_000) * 0.22
        self.assertGreater(projected_tax, 0)
        self.assertAlmostEqual(
            summary["estimated_annual_realized_if_liquidated_krw"], projected_annual
        )
        self.assertAlmostEqual(
            summary["estimated_capital_tax_reserve_if_liquidated_krw"], projected_tax
        )
        self.assertAlmostEqual(
            summary["after_tax_bid_liquidation_estimate_usd"],
            target.cash + net_proceeds - projected_tax / 1500,
        )
        self.assertEqual(summary["estimated_capital_tax_reserve_krw"], 0)
        self.assertEqual(summary["after_tax_mark_equity_estimate_usd"], summary["equity"])
        self.assertEqual(target.closed, 0)
        self.assertAlmostEqual(target.cash, 9.91)
        self.assertAlmostEqual(target.fee_totals["total_usd"], 0.09)
        self.assertEqual(target.context.tax_realized_krw, {})

    def test_hypothetical_liquidation_tax_is_unknown_with_missing_basis_or_fx(self) -> None:
        for missing in ("entry_basis", "current_fx"):
            with self.subTest(missing=missing):
                target = book()
                target.context.frame["orderbooks"][0] = orderbook()
                if missing == "entry_basis":
                    del target.context.frame["fx"]
                target.fill(0, 9, 10, stamp(0), 1, "entry")
                target.context.frame = {
                    **frame(5, price=250),
                    "orderbooks": {0: orderbook(5, ask=250, bid=250)},
                }
                if missing == "current_fx":
                    del target.context.frame["fx"]
                summary = target.summary({0: 250}, {0: stamp(5)}, stamp(5))
                self.assertIsNotNone(summary["bid_liquidation_equity_estimate_usd"])
                self.assertIsNone(summary["estimated_annual_realized_if_liquidated_krw"])
                self.assertIsNone(summary["estimated_capital_tax_reserve_if_liquidated_krw"])
                self.assertIsNone(summary["after_tax_bid_liquidation_estimate_usd"])
                self.assertEqual(summary["estimated_capital_tax_reserve_krw"], 0)

    def test_future_path_cannot_change_prior_choices_fills_or_fees(self) -> None:
        first, changed = engine(), engine()
        for target, final in ((first, 100), (changed, 1)):
            for minute, price in ((0, 10), (5, 11), (10, final)):
                incoming = frame(minute, price)
                incoming["orderbooks"][0] = orderbook(minute, ask=price, bid=price)
                target.step(incoming)
            target.day_end()
        for key, clock in (("choices", "at"), ("trades", "fill_at")):
            before, after = [target.report()["hypotheses"][0][key] for target in (first, changed)]
            self.assertEqual(
                [row for row in before if row[clock] <= stamp(5)],
                [row for row in after if row[clock] <= stamp(5)],
            )

    def test_legacy_last_and_quote_models_remain_isolated(self) -> None:
        specs = signals()
        families = ("momentum", "reversal", "breakout", "mean_reversion", "transition")
        configs = []
        for index, family in enumerate(families):
            signal = next(
                i
                for i, spec in enumerate(specs)
                if spec["family"] == family and spec["side"] == "long" and not spec["volume"]
            )
            configs.append({**configurations(1)[0], "id": index, "signal": signal})
        policies = [p for p in long_policies() if "volume_trend" not in p.get("families", [])]
        self.assertEqual(len(policies), 59)
        outputs = []
        for mode in ("last", "quotes"):
            target = LiveWeekEngine(configs, specs, copy.deepcopy(policies), mode=mode)
            target.day_start(DAY, scope())
            incoming = frame()
            incoming["ranks"] = [[[], []] for _ in specs]
            for config in configs:
                incoming["ranks"][config["signal"]][1] = [[0, 1, 0.01]]
            incoming["orderbooks"][0] = orderbook()
            target.step(incoming)
            target.day_end()
            result = target.report()
            self.assertEqual(len(result["hypotheses"]), 59)
            self.assertTrue(all(row["summary"]["debt"] == 0 for row in result["hypotheses"]))
            outputs.append(result)
        self.assertEqual(outputs[0]["hypotheses"][0]["trades"][0]["quantity_normalized"], 10)
        self.assertEqual(outputs[1]["hypotheses"][0]["trades"][0]["quantity_normalized"], 9)


if __name__ == "__main__":
    unittest.main()
