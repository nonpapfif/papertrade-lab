"""Check shared Korean observations and causal fills without market or model calls."""

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from tools.run_kr_paper_comparison import CAPITAL, Comparison


class ComparisonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.comparison = Comparison()
        self.at = datetime(2026, 11, 4, 3, tzinfo=UTC)

    def observe(
        self, seconds: int, price: int, *, source: int | None = None
    ) -> list[dict[str, Any]]:
        observed = self.at + timedelta(seconds=seconds)
        quoted = self.at + timedelta(seconds=seconds if source is None else source)
        return self.comparison.observe(
            [
                {
                    "symbol": "005930",
                    "currency": "KRW",
                    "lastPrice": price,
                    "timestamp": quoted.isoformat(),
                }
            ],
            observed,
        )

    def test_fixed_and_ai_use_independent_krw_balances_and_post_decision_prices(self) -> None:
        accounts = self.comparison.accounts
        self.assertEqual([a.cash for a in accounts.values()], [CAPITAL, CAPITAL])
        self.assertEqual(self.observe(0, 71_000), [])
        self.assertEqual(self.observe(1, 72_000), [])
        signals = list(self.comparison.review_queue.values())
        self.comparison.review_queue.clear()
        self.comparison.approve(signals, {"005930": "Momentum"}, self.at + timedelta(seconds=3))
        (fixed_fill,) = self.observe(4, 73_000, source=3)
        self.assertEqual(fixed_fill["account"], "fixed")
        self.assertEqual(accounts["ai"].cash, CAPITAL)
        (ai_fill,) = self.observe(5, 74_000)
        self.assertEqual(ai_fill["account"], "ai")
        self.assertEqual(ai_fill["assumed_price_krw"], 74_000)
        self.assertEqual(ai_fill["quantity"], CAPITAL / Decimal(74_000))
        self.assertFalse(self.comparison.review_queue)

    def test_reversal_discards_stale_approval_and_keeps_open_positions(self) -> None:
        self.observe(0, 70_000)
        self.observe(1, 71_000)
        signals = list(self.comparison.review_queue.values())
        self.observe(2, 72_000)
        self.observe(3, 69_000)
        self.comparison.approve(
            signals, {"005930": "Previous momentum"}, self.at + timedelta(seconds=4)
        )
        self.assertFalse(self.comparison.accounts["ai"].pending)
        self.assertFalse(self.comparison.review_queue)
        self.assertIn("005930", self.comparison.accounts["fixed"].positions)
        (fill,) = self.observe(5, 68_000)
        self.assertEqual((fill["account"], fill["action"]), ("fixed", "SELL"))
        self.assertLess(self.comparison.accounts["fixed"].realized, 0)
        self.assertEqual(self.comparison.accounts["ai"].cash, CAPITAL)

    def test_declined_signal_reopens_only_on_a_fresh_watch_crossing(self) -> None:
        self.observe(0, 70_000)
        self.observe(1, 71_000)
        signals = list(self.comparison.review_queue.values())
        self.comparison.review_queue.clear()
        self.comparison.approve(
            signals, {}, self.at + timedelta(seconds=2), {"005930": Decimal(73_000)}
        )
        self.observe(3, 72_000, source=1)
        self.assertFalse(self.comparison.review_queue)
        (fixed_fill,) = self.observe(4, 73_000)
        self.assertEqual(fixed_fill["account"], "fixed")
        self.assertEqual(self.comparison.review_queue["005930"]["trigger"], "watch_crossing")
        self.comparison.review_queue.clear()
        self.observe(5, 74_000)
        self.assertFalse(self.comparison.review_queue)
