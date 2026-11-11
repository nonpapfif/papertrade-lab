"""Verify fresh KR constraints, reservation conservation and causal observations."""

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tools.kr_broker_profile import continuous_window, eligible
from tools.kr_whole_share_accounts import WholeShareAccount
from tools.run_kr_paper_comparison import Comparison


class ConstrainedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 11, 4, 0, tzinfo=UTC)
        self.end = self.start + timedelta(hours=6, minutes=20)
        self.pair = Comparison(
            Decimal("0.10"),
            profile="broker-constrained",
            classification={"A": ("KOSPI", "STOCK"), "B": ("KOSDAQ", "STOCK")},
            session_window=(self.start, self.end),
        )

    def row(self, symbol: str, price: int, at: datetime) -> dict:
        return {
            "symbol": symbol,
            "currency": "KRW",
            "lastPrice": str(price),
            "timestamp": at.isoformat(),
        }

    def test_new_accounts_integer_costs_same_ai_context_and_isolation(self) -> None:
        pair = self.pair
        self.assertEqual(pair.accounts["fixed"].cash, Decimal(100000))
        pair.observe([self.row("A", 2400, self.start)], self.start)
        at = self.start + timedelta(minutes=1)
        pair.observe([self.row("A", 2410, at)], at)
        signal = pair.latest["A"]
        self.assertEqual(len(signal["price_history"]), 2)
        pair.approve([signal], {"A": "Observed movement"}, at, {})
        at += timedelta(seconds=1)
        fills = pair.observe([self.row("A", 2411, at)], at)
        self.assertEqual(len(fills), 2)
        for account in pair.accounts.values():
            self.assertEqual(account.positions["A"][0], 4)
            self.assertEqual(account.cash, Decimal("90354.55340"))
            summary = account.summary(pair.prices)
            self.assertEqual(summary["commission_krw"], Decimal("1.44660"))
            self.assertEqual(summary["available_cash_krw"], account.cash)
            self.assertEqual(summary["reserved_cash_krw"], 0)
        self.assertIsNot(pair.accounts["fixed"].positions, pair.accounts["ai"].positions)
        self.assertEqual(len(signal["price_history"]), 2)

    def test_reservations_do_not_reuse_cash_or_duplicate_intents(self) -> None:
        account = self.pair.accounts["fixed"]
        assert isinstance(account, WholeShareAccount)
        account.cash = account.ledger.cash = Decimal(1000)
        prices = {str(i): (Decimal(600), self.start) for i in range(3)}
        for symbol in prices:
            signal = {"symbol": symbol, "action": "BUY", "strength": Decimal(1)}
            account.queue(signal, self.start, "test", prices)
            account.queue(signal, self.start, "duplicate", prices)
        self.assertEqual(len(account.pending), 1)
        self.assertLessEqual(account.reserved_cash, account.cash)
        at = self.start + timedelta(seconds=1)
        account.classification["0"] = ("KOSPI", "STOCK")
        latest = {s: x[0] for s, x in account.pending.items()}
        fills = account.execute({"0": (Decimal(600), at)}, at, latest)
        self.assertEqual(fills[0]["quantity"], 1)
        self.assertEqual(account.cash, Decimal("399.91000"))
        self.assertEqual(account.reserved_cash, 0)

    def test_session_staleness_unknown_product_and_future_prices_cannot_fill(self) -> None:
        pair = self.pair
        pair.observe([self.row("A", 100, self.start)], self.start)
        at = self.start + timedelta(seconds=60)
        pair.observe([self.row("A", 101, at)], at)
        self.assertTrue(pair.accounts["fixed"].pending)
        self.assertEqual(pair.observe([self.row("A", 102, at + timedelta(seconds=5))], at), [])
        self.assertEqual(pair.observe([self.row("X", 100, at)], at), [])
        self.assertNotIn("X", pair.prices)
        self.assertEqual(pair.observe([self.row("A", 103, at)], at + timedelta(minutes=3)), [])
        self.assertEqual(pair.observe([self.row("A", 102, self.end)], self.end), [])
        account = pair.accounts["fixed"]
        assert isinstance(account, WholeShareAccount)
        self.assertEqual(account.reserved_cash, 0)
        self.assertFalse(account.positions)

    def test_calendar_requires_verified_auction_boundary_and_current_day(self) -> None:
        regular: dict[str, str | None] = {
            "startTime": self.start.isoformat(),
            "endTime": (self.end + timedelta(minutes=10)).isoformat(),
            "singlePriceAuctionStartTime": self.end.isoformat(),
        }
        calendar = {"today": {"date": "2026-11-04", "integrated": {"regularMarket": regular}}}
        self.assertEqual(continuous_window(calendar, self.start), (self.start, self.end))
        regular["singlePriceAuctionStartTime"] = None
        with self.assertRaises(ValueError):
            continuous_window(calendar, self.start)
        self.assertFalse(eligible({"status": "ACTIVE", "currency": "KRW", "securityType": "STOCK"}))

    def test_actual_stock_detail_rejects_unknown_halted_and_liquidation(self) -> None:
        detail = {
            "liquidationTrading": False,
            "krxTradingSuspended": False,
            "nxtSupported": False,
            "nxtTradingSuspended": None,
        }
        stock = {
            "status": "ACTIVE",
            "currency": "KRW",
            "securityType": "STOCK",
            "isCommonShare": True,
            "koreanMarketDetail": detail,
        }
        self.assertTrue(eligible(stock))
        for field in ("liquidationTrading", "krxTradingSuspended"):
            self.assertFalse(eligible({**stock, "koreanMarketDetail": {**detail, field: True}}))
        self.assertFalse(eligible({**stock, "koreanMarketDetail": None}))
        self.assertFalse(
            eligible({**stock, "koreanMarketDetail": {**detail, "nxtSupported": True}})
        )
