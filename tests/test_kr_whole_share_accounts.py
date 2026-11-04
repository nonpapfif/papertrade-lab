"""Check integer-only KR execution against budget and cash."""

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tools.kr_whole_share_accounts import WholeShareAccount
from tools.replay_kr_paper_costs import Ledger
from tools.run_kr_paper_comparison import Account


class WholeShareTests(unittest.TestCase):
    def test_fee_budget_skips_unaffordable_lot_then_fills_integers(self) -> None:
        at = datetime(2026, 11, 4, 6, tzinfo=UTC)
        symbol = "005930"
        price = Decimal(5000)
        account = WholeShareAccount(
            Account(Decimal("0.05")),
            Ledger(),
            {symbol: ("KOSPI", "STOCK")},
            {symbol: (price, at)},
        )
        buy = {"symbol": symbol, "action": "BUY", "strength": Decimal(1)}
        account.queue(buy, at, "signal", {symbol: (price, at)})
        confirmed = at + timedelta(seconds=1)
        self.assertEqual(
            account.execute({symbol: (price, confirmed)}, confirmed, {symbol: buy}), []
        )
        self.assertEqual(account.cash, 100000)

        price = Decimal(2400)
        account.queue(buy, confirmed, "signal", {symbol: (price, confirmed)})
        confirmed += timedelta(seconds=1)
        (purchase,) = account.execute({symbol: (price, confirmed)}, confirmed, {symbol: buy})
        self.assertEqual(purchase["quantity"], 2)
        self.assertEqual(account.cash, Decimal("95199.28"))
        self.assertEqual(purchase["commission_krw"], Decimal("0.72"))

        sell = {**buy, "action": "SELL"}
        account.queue(sell, confirmed, "signal", {symbol: (price, confirmed)})
        confirmed += timedelta(seconds=1)
        (sale,) = account.execute({symbol: (price, confirmed)}, confirmed, {symbol: sell})
        self.assertEqual(sale["quantity"], 2)
        self.assertEqual(sale["transaction_tax_krw"], Decimal("9.6"))
        self.assertFalse(account.positions)
