"""Causal signal isolation, ownership and cash conservation for the layered prototype."""

import copy
import pickle
import unittest
from dataclasses import replace
from datetime import datetime, timedelta

from tests.test_combined_hedge import frame
from tools.layered_paper import Feature, LayeredPaper, assign, feature_from_bars, holding_valid


def feature(minute: int = 0, symbol: int = 0) -> Feature:
    at = frame(minute)["decision_at"]
    return Feature(symbol, 10, at, at, 0.01, 0.02, 9.9, 9.8, 0.8)


class LayerTests(unittest.TestCase):
    def test_assignment_and_maintenance_are_distinct(self) -> None:
        f = feature()
        self.assertEqual(assign(f), "breakout")
        quiet = replace(f, r5=0, price=9.9)
        self.assertFalse(assign(quiet) == "breakout")
        self.assertTrue(holding_valid("breakout", quiet, f))
        self.assertIsNone(assign(replace(f, r5=0, r15=0)))

    def test_whole_share_cash_and_unknown_eligibility(self) -> None:
        for capital in (50, 100, 200, 500):
            a = LayeredPaper(capital)
            a.step("a", frame(), {0: feature()}, set())
            self.assertEqual(a.summary()["account"]["entries"], 0)
            a.step("b", frame(), {0: feature()}, {0})
            self.assertGreater(a.summary()["account"]["entries"], 0)
            self.assertGreaterEqual(a.book.cash, 0)
            self.assertEqual(a.summary()["account"]["debt"], 0)
            self.assertTrue(float(a.book.positions[0].quantity).is_integer())

    def test_same_signal_event_is_idempotent_and_held_symbols_are_not_added(self) -> None:
        a = LayeredPaper(100)
        a.step("a", frame(), {0: feature()}, {0})
        before = copy.deepcopy(a.book.trades)
        a.step("a", frame(), {0: feature()}, {0})
        a.step("b", frame(1), {0: feature(1)}, {0})
        self.assertEqual(a.book.trades, before)
        self.assertEqual(a.owners[0][0], "breakout")

    def test_missing_features_do_not_invalidate_a_holding(self) -> None:
        a = LayeredPaper(100)
        a.step("a", frame(), {0: feature()}, {0})
        a.step("b", frame(1), {}, {0})
        self.assertIn(0, a.book.positions)
        self.assertEqual(a.decisions[-1]["action"], "unknown")

    def test_unfilled_sale_does_not_release_cash(self) -> None:
        a = LayeredPaper(100)
        a.step("a", frame(), {0: feature()}, {0})
        before = a.book.cash
        missing = frame(1)
        missing["orderbooks"] = {}
        a.step("b", missing, {0: replace(feature(1), price=8, r5=-0.1)}, {0})
        self.assertEqual(a.book.cash, before)
        self.assertIn(0, a.book.positions)

    def test_partial_exit_keeps_owner_and_reconciles_strategy_pnl(self) -> None:
        a = LayeredPaper(100)
        a.step("a", frame(), {0: feature()}, {0})
        qty = a.book.positions[0].quantity
        a.step("b", frame(1, 9, 1), {0: replace(feature(1), price=9, r5=-0.1)}, {0})
        self.assertEqual(a.book.positions[0].quantity, qty - 1)
        self.assertIn(0, a.owners)
        self.assertAlmostEqual(sum(a.realized_by_strategy.values()), a.book.realized)
        a.step("c", frame(2, 9, 100), {0: replace(feature(2), price=9, r5=-0.1)}, {0})
        self.assertNotIn(0, a.owners)
        self.assertNotIn(0, a.book.positions)

    def test_cannot_afford_one_share_is_zero_not_debt(self) -> None:
        a = LayeredPaper(5)
        a.step("a", frame(), {0: feature()}, {0})
        self.assertEqual(a.summary()["account"]["entries"], 0)
        self.assertEqual(a.book.cash, 5)

    def test_state_restores_without_changing_future_results(self) -> None:
        a = LayeredPaper(100)
        a.step("a", frame(), {0: feature()}, {0})
        b = pickle.loads(pickle.dumps(a))
        for x in (a, b):
            x.step("b", frame(1, 9), {0: replace(feature(1), price=9)}, {0})
        self.assertEqual(a.book.cash, b.book.cash)
        self.assertEqual(a.book.trades, b.book.trades)

    def test_input_order_does_not_change_account_allocation(self) -> None:
        f = frame()
        for key in (
            "quotes",
            "marks",
            "decision_orderbooks",
            "orderbooks",
            "confirmation_received_at",
        ):
            f[key][1] = copy.deepcopy(f[key][0])
        x, y = LayeredPaper(100), LayeredPaper(100)
        x.step("a", f, {0: feature(), 1: feature(symbol=1)}, {0, 1})
        y.step("a", f, {1: feature(symbol=1), 0: feature()}, {0, 1})
        self.assertEqual(x.book.trades, y.book.trades)
        self.assertGreaterEqual(x.book.cash, 0)

    def test_future_feature_cannot_trigger_entry(self) -> None:
        a = LayeredPaper(100)
        a.step("a", frame(), {0: feature(5)}, {0})
        self.assertEqual(a.summary()["account"]["entries"], 0)

    def test_future_bar_changes_do_not_change_past_features(self) -> None:
        at = datetime.fromisoformat(frame()["decision_at"])
        rows = [
            {
                "timestamp": (at - timedelta(minutes=16 - i)).isoformat(),
                "available_at": (at - timedelta(minutes=15 - i)).isoformat(),
                "closePrice": str(10 + i / 100),
                "highPrice": str(10.1 + i / 100),
            }
            for i in range(16)
        ]
        first = feature_from_bars(0, rows, at.isoformat())
        rows.append(
            {
                "timestamp": at.isoformat(),
                "available_at": (at + timedelta(minutes=1)).isoformat(),
                "closePrice": "99999",
                "highPrice": "99999",
            }
        )
        self.assertEqual(first, feature_from_bars(0, rows, at.isoformat()))
        late = copy.deepcopy(rows)
        late[0]["available_at"] = (at + timedelta(seconds=1)).isoformat()
        self.assertIsNone(feature_from_bars(0, late, at.isoformat()))

    def test_older_decision_cannot_rewind_an_account(self) -> None:
        a = LayeredPaper(100)
        a.step("a", frame(1), {}, set())
        with self.assertRaises(ValueError):
            a.step("b", frame(), {}, set())
