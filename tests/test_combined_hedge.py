"""Single-capital conservation, lagged allocation, aggregated depth and partial rebalances."""

from __future__ import annotations

import copy
import unittest
from datetime import UTC, datetime, timedelta

from tools.combined_hedge import CombinedHedge
from tools.live_execution import LiveWeekEngine
from tools.search_portfolios import configurations, signals


def reference() -> LiveWeekEngine:
    return LiveWeekEngine(
        configurations(1)[:1],
        signals()[:1],
        [{"id": "a", "mode": "static"}, {"id": "b", "mode": "static"}],
        mode="quotes",
        fee_schedule={"confirmation_delay_seconds": 1, "allow_partial_exits": 1},
    )


def frame(minute: int = 0, price: float = 10, volume: int = 100) -> dict:
    decision = datetime(2026, 11, 2, 14, 30, tzinfo=UTC) + timedelta(minutes=minute)
    arrival = decision + timedelta(seconds=1.1)

    def quote(at: datetime) -> dict:
        return {
            "timestamp": at.isoformat(),
            "currency": "USD",
            "asks": [{"price": price, "volume": volume}],
            "bids": [{"price": price, "volume": volume}],
        }

    return {
        "at": arrival.isoformat(),
        "tick": int(arrival.timestamp() // 300),
        "quotes": {0: price},
        "marks": {0: [price, arrival.isoformat()]},
        "orderbooks": {0: quote(arrival)},
        "decision_orderbooks": {0: quote(decision)},
        "decision_at": decision.isoformat(),
        "confirmation_received_at": {0: arrival.isoformat()},
        "fx": 1500,
        "fx_at": arrival.isoformat(),
    }


class CombinedHedgeTests(unittest.TestCase):
    def test_one_capital_pool_and_uniform_initial_weights(self) -> None:
        ref = reference()
        combined = CombinedHedge(ref)
        self.assertEqual(combined.book.cash, 100)
        self.assertEqual(combined.weights(ref, frame()["at"]), [0.5, 0.5])

    def test_lagged_profit_and_wins_shift_weights_without_funding_the_account(self) -> None:
        ref = reference()
        ref.last_at = "2026-11-02T14:30:00+00:00"
        ref.states[0]["book"].equity = 110
        ref.states[0]["book"].wins = 4
        ref.states[0]["book"].closed = 5
        ref.states[1]["book"].equity = 95
        ref.states[1]["book"].wins = 1
        ref.states[1]["book"].closed = 5
        combined = CombinedHedge(ref)
        first = combined.weights(ref, frame(5)["at"])
        self.assertGreater(first[0], first[1])
        self.assertAlmostEqual(sum(first), 1)
        ref.last_at = "2026-11-02T14:35:00+00:00"
        ref.states[0]["book"].equity = 90
        ref.states[1]["book"].equity = 115
        second = combined.weights(ref, frame(10)["at"])
        self.assertGreater(second[1], second[0])
        self.assertEqual(combined.book.cash, 100)

    def test_aggregated_order_consumes_one_shared_cash_and_depth_pool(self) -> None:
        combined = CombinedHedge(reference())
        combined.prices, combined.times, combined.targets = {0: 10}, {0: frame()["at"]}, {0: 9}
        combined.step(frame(volume=2))
        self.assertEqual(combined.book.positions[0].quantity, 2)
        self.assertAlmostEqual(combined.book.cash, 79.98)
        self.assertEqual(len(combined.book.trades), 1)
        self.assertEqual(combined.book.order_attempts[0]["unfilled_quantity"], 7)
        self.assertAlmostEqual(combined.report()["summary"]["equity"], 99.98)

    def test_topups_and_partial_reductions_keep_basis_cash_and_tax_reconciled(self) -> None:
        combined = CombinedHedge(reference())
        combined.prices, combined.times, combined.targets = {0: 10}, {0: frame()["at"]}, {0: 5}
        combined.step(frame())
        combined.targets = {0: 9}
        combined.step(frame(5))
        self.assertEqual(combined.book.positions[0].quantity, 9)
        self.assertEqual(combined.book.opened, 1)
        self.assertAlmostEqual(combined.book.entry_fees[0], 0.09)
        self.assertAlmostEqual(combined.book.cash, 9.91)
        combined.targets = {}
        combined.prices[0] = 11
        combined.step(frame(10, 11, 2))
        self.assertEqual(combined.book.positions[0].quantity, 7)
        self.assertEqual(combined.book.closed, 0)
        combined.report()
        combined.step(frame(15, 11))
        self.assertFalse(combined.book.positions)
        self.assertEqual(combined.book.closed, 1)
        self.assertEqual(combined.book.wins, 1)
        self.assertAlmostEqual(combined.report()["summary"]["equity"], combined.book.cash)
        self.assertAlmostEqual(combined.book.realized, combined.book.cash - 100)

    def test_no_fill_does_not_free_cash_or_change_ownership(self) -> None:
        combined = CombinedHedge(reference())
        combined.prices, combined.times, combined.targets = {0: 10}, {0: frame()["at"]}, {0: 9}
        combined.step(frame())
        before = combined.book.cash
        combined.targets = {}
        incoming = frame(5)
        incoming["orderbooks"] = {}
        combined.step(incoming)
        self.assertEqual(combined.book.cash, before)
        self.assertEqual(combined.book.positions[0].quantity, 9)

    def test_future_weight_changes_cannot_rewrite_prior_fills(self) -> None:
        first = CombinedHedge(reference())
        first.prices, first.times, first.targets = {0: 10}, {0: frame()["at"]}, {0: 9}
        first.step(frame())
        second = copy.deepcopy(first)
        first.targets, second.targets = {}, {0: 9}
        first.step(frame(5))
        second.step(frame(5))
        self.assertEqual(first.book.trades[0], second.book.trades[0])


if __name__ == "__main__":
    unittest.main()
