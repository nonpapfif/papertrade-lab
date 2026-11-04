"""Check shared Korean observations and causal fills without market or model calls."""

import asyncio
import json
import tempfile
import unittest
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools.kr_comparison_ai import SOL
from tools.run_kr_paper_comparison import CAPITAL, Comparison, campaign


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

    def test_four_accounts_fund_strongest_signals_without_splitting_into_dust(self) -> None:
        pairs = {
            5: Comparison(Decimal("0.05")),
            10: Comparison(Decimal("0.10")),
        }
        for second in range(14):
            at = self.at + timedelta(seconds=second)
            rows = [
                {
                    "symbol": f"S{i:02}",
                    "currency": "KRW",
                    "lastPrice": 100 + second if second < 12 else 115 + i + second,
                    "timestamp": at.isoformat(),
                }
                for i in range(25)
            ]
            for pair in pairs.values():
                pair.observe(rows, at, warmup=second < 12)
                if second < 12:
                    self.assertFalse(pair.review_queue)
                if second == 12:
                    candidates = list(pair.review_queue.values())
                    pair.approve(candidates, {x["symbol"]: "Approved" for x in candidates}, at)
        for percent, pair in pairs.items():
            count = 100 // percent
            expected = {f"S{i:02}" for i in range(25 - count, 25)}
            for account in pair.accounts.values():
                self.assertEqual(set(account.positions), expected)
                self.assertEqual({cost for _, cost in account.positions.values()}, {1000 * percent})
                self.assertEqual(account.cash, 0)


class CampaignTests(unittest.IsolatedAsyncioTestCase):
    async def test_four_account_mode_uses_one_snapshot_per_sweep(self) -> None:
        start = datetime(2026, 11, 4, 3, tzinfo=UTC)
        clock = start
        symbols = [f"S{i:02}" for i in range(25)]
        real_sleep = asyncio.sleep

        async def advance(delay: float) -> None:
            nonlocal clock
            await real_sleep(0)
            clock += timedelta(minutes=1)

        def snapshot() -> list[dict[str, Any]]:
            minute = int((clock - start).total_seconds() // 60)
            return [
                {
                    "symbol": symbol,
                    "currency": "KRW",
                    "timestamp": clock.isoformat(),
                    "lastPrice": 100 + minute if minute < 12 else 115 + minute + i,
                }
                for i, symbol in enumerate(symbols)
            ]

        async def approve(
            settings: Any,
            pair: Comparison,
            signals: list[dict[str, Any]],
            until: datetime,
            record: Callable[..., None],
            calls: Counter[str],
            completed: Counter[str],
        ) -> None:
            calls[SOL] += 1
            pair.approve(signals, {s["symbol"]: "Approved" for s in signals}, clock)
            completed[SOL] += 1

        module = "tools.run_kr_paper_comparison"
        with (
            tempfile.TemporaryDirectory() as temp,
            patch(f"{module}.now", side_effect=lambda: clock),
            patch(f"{module}.asyncio.sleep", side_effect=advance),
            patch(f"{module}.CachedTokenClient") as client,
            patch(f"{module}.LiveFeed") as feed,
            patch(f"{module}.review_cascade", side_effect=approve),
        ):
            client.return_value.listed.return_value = [{"symbol": symbol} for symbol in symbols]
            feed.return_value.snapshot.side_effect = snapshot
            output = Path(temp) / "run"
            await campaign(output, start + timedelta(minutes=14), allocation_comparison=True)
            result = json.loads((output / "comparison.json").read_text())
            self.assertEqual(feed.return_value.snapshot.call_count, 14)
            self.assertEqual(result["sweeps"], 14)
            self.assertTrue(result["complete"])
            self.assertEqual(result["ai_reviews_completed"], 2)
            accounts = result["accounts"]
            self.assertEqual(set(accounts), {"5pct_fixed", "5pct_ai", "10pct_fixed", "10pct_ai"})
            for key, data in accounts.items():
                size = 5 if key.startswith("5pct") else 10
                self.assertEqual(data["fills"], 100 // size)
                self.assertEqual(
                    {Decimal(p["cost_krw"]) for p in data["positions"].values()}, {1000 * size}
                )
