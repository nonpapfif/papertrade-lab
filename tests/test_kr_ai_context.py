"""Verify the constrained KR paper review contract without invoking Codex."""

from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock

from papertrade.core import Settings
from tools.kr_comparison_ai import SOL, review_cascade, review_payload
from tools.run_kr_paper_comparison import Comparison


class ReviewContextTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.at = datetime.now(UTC) - timedelta(seconds=1)
        self.prices = {
            "A": (Decimal(1090), self.at),
        }
        self.portfolio = {
            "quantity_policy": "whole_shares",
            "cash_krw": Decimal(6000),
            "available_cash_krw": Decimal(4000),
            "reserved_cash_krw": Decimal(2000),
            "commission_rate": Decimal("0.00015"),
            "stock_sale_tax_rate": Decimal("0.002"),
            "cost_assumptions": "KR ordinary stock scenario",
            "execution_model": "newer_last_trade_assumption_not_depth",
            "positions": {},
        }
        self.account = Mock(allocation=Decimal("0.05"))
        self.account.summary.return_value = self.portfolio
        self.comparison = Mock(spec=Comparison, accounts={"ai": self.account}, prices=self.prices)
        self.signal: dict[str, Any] = {
            "symbol": "A",
            "execution_profile": "broker-constrained",
            "eligibility": "verified_ordinary_common_stock",
            "session": "regular_continuous",
            "strategy": "direction_change_baseline",
            "trigger": "direction_change",
            "action": "BUY",
            "strength": Decimal(2),
            "price_history": [
                {
                    "price_krw": price,
                    "source_at": (self.at - timedelta(seconds=age + 1)).isoformat(),
                    "observed_at": (self.at - timedelta(seconds=age)).isoformat(),
                }
                for price, age in (("1000", 3), ("980", 2), ("1020", 1))
            ],
        }

    def test_payload_keeps_signal_cut_separate_from_live_account_snapshot(self) -> None:
        payload = review_payload(self.comparison, [self.signal], self.at)
        self.account.summary.assert_called_once_with(self.prices)
        self.assertEqual(payload["portfolio"], self.portfolio)
        self.assertEqual(payload["observed_at"], self.at)
        self.assertEqual(payload["allocation_fraction"], "0.05")
        self.assertEqual(payload["unavailable_context"], {})
        self.assertEqual(payload["signals"], [self.signal])
        self.assertNotIn(
            str(self.prices["A"][0]), [row["price_krw"] for row in self.signal["price_history"]]
        )
        contract = payload["review_contract"]
        self.assertIn("no OHLCV", contract["history"])
        self.assertIn("latest move is excluded", contract["strength"])
        self.assertIn("round-trip commission and sell tax", " ".join(contract["criteria"]))

    async def test_missing_price_history_never_dispatches_review(self) -> None:
        self.signal["price_history"] = self.signal["price_history"][-2:]
        decide = AsyncMock()
        record = Mock()
        calls: Counter[str] = Counter()
        completed: Counter[str] = Counter()
        await review_cascade(
            Mock(spec=Settings),
            self.comparison,
            [self.signal],
            self.at + timedelta(minutes=1),
            record,
            calls,
            completed,
            decide_signal=decide,
        )
        decide.assert_not_awaited()
        self.assertEqual(calls[SOL], 0)
        self.comparison.approve.assert_not_called()
        record.assert_called_once_with(
            "ai_context_unavailable", symbols={"A": "insufficient_sampled_price_path"}
        )

    def test_missing_reservations_block_review_and_legacy_keeps_shape(self) -> None:
        self.portfolio.pop("reserved_cash_krw")
        payload = review_payload(self.comparison, [self.signal], self.at)
        self.assertEqual(payload["signals"], [])
        self.assertEqual(
            payload["unavailable_context"], {"A": "missing_constrained_account_context"}
        )
        legacy = {"symbol": "B", "action": "BUY"}
        payload = review_payload(self.comparison, [legacy], self.at)
        self.assertEqual(payload["signals"], [legacy])
        self.assertNotIn("review_contract", payload)

    def test_runner_freezes_causal_path_and_does_not_requeue_same_direction(self) -> None:
        start = self.at - timedelta(minutes=1)
        comparison = Comparison(
            Decimal("0.05"),
            profile="broker-constrained",
            classification={"A": ("KOSPI", "STOCK")},
            session_window=(start, self.at + timedelta(minutes=1)),
        )

        def observe(price: int, offset: int, *, warmup: bool = False) -> None:
            observed = start + timedelta(seconds=offset)
            comparison.observe(
                [
                    {
                        "currency": "KRW",
                        "symbol": "A",
                        "lastPrice": price,
                        "timestamp": (observed - timedelta(seconds=1)).isoformat(),
                    }
                ],
                observed,
                warmup=warmup,
            )

        observe(1000, 2, warmup=True)
        observe(980, 4, warmup=True)
        observe(1020, 6)
        signal = comparison.review_queue.pop("A")
        observe(1100, 8)
        observe(1110, 10)
        self.assertEqual(comparison.review_queue, {})

        payload = review_payload(comparison, [signal], self.at)
        history = payload["signals"][0]["price_history"]
        self.assertEqual([row["price_krw"] for row in history], ["1000", "980", "1020"])
        self.assertEqual(history[-1]["observed_at"], signal["observed_at"])
        self.assertEqual(history[-1]["source_at"], signal["source_at"])
        self.assertEqual(
            signal["strength"],
            (Decimal(1020) / Decimal(980) - 1) / Decimal("0.02"),
        )
        self.assertEqual(payload["portfolio"]["reserved_cash_krw"], Decimal(0))
        self.assertGreater(payload["portfolio"]["available_cash_krw"], 0)
        self.assertIn("A", comparison.accounts["fixed"].positions)
        self.assertEqual(payload["portfolio"]["positions"], {})
