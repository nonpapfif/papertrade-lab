"""Seed integer, fee-aware accounts from legacy Korean paper evidence."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import Any

from tools.replay_kr_paper_costs import COMMISSION, STOCK_SELL_TAX, Ledger, listings
from tools.run_kr_paper_comparison import Account, Comparison


class WholeShareAccount(Account):
    def __init__(
        self,
        original: Account,
        ledger: Ledger,
        classification: dict[str, tuple[str, str]],
        prices: dict[str, tuple[Decimal, datetime]],
    ) -> None:
        super().__init__(original.allocation)
        self.ledger = ledger
        self.classification = classification
        self.positions = ledger.positions
        self.cash = ledger.cash
        self.realized = ledger.realized
        self.fills = ledger.counts["fills"]
        for signal, decided, reason in original.pending.values():
            if self.wants(signal):
                self.queue(signal, decided, reason, prices)

    @property
    def reserved_cash(self) -> Decimal:
        return sum(self.budgets.values(), Decimal(0))

    @property
    def available_cash(self) -> Decimal:
        return max(Decimal(0), self.cash - self.reserved_cash)

    def queue(
        self,
        signal: dict[str, Any],
        decided: datetime,
        reason: str,
        prices: dict[str, tuple[Decimal, datetime]],
    ) -> None:
        symbol = signal["symbol"]
        if symbol in self.pending or not self.wants(signal):
            return
        if signal["action"] == "BUY":
            if self.allocation is None:
                raise ValueError("Whole-share entry requires a frozen allocation")
            budget = min(self.summary(prices)["equity_krw"] * self.allocation, self.available_cash)
            price = prices[symbol][0]
            if budget < price * (1 + COMMISSION):
                self.ledger.counts["below_one_share"] += 1
                return
            self.budgets[symbol] = budget
        self.pending[symbol] = signal, decided, reason

    def execute(
        self,
        quotes: dict[str, tuple[Decimal, datetime]],
        observed: datetime,
        latest: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ready = []
        for symbol, (signal, decided, reason) in list(self.pending.items()):
            if latest.get(symbol) != signal or not self.wants(signal):
                del self.pending[symbol]
                self.budgets.pop(symbol, None)
            elif symbol in quotes and decided < quotes[symbol][1] <= observed:
                ready.append((symbol, signal, decided, reason))
                del self.pending[symbol]

        fills = []
        for symbol, signal, decided, reason in sorted(
            ready, key=lambda row: (row[1]["action"] == "BUY", -row[1]["strength"], row[0])
        ):
            price, source = quotes[symbol]
            if signal["action"] == "BUY":
                budget = min(self.budgets.pop(symbol), self.cash - self.reserved_cash)
                proposed = (budget / (price * (1 + COMMISSION))).to_integral_value(
                    rounding=ROUND_DOWN
                )
                if proposed <= 0:
                    self.ledger.counts["below_one_share"] += 1
                    continue
            else:
                proposed = -self.positions[symbol][0]
            result = self.ledger.apply(symbol, proposed, price, self.classification.get(symbol))
            if result["outcome"] != "filled":
                continue
            self.cash = self.ledger.cash
            self.realized = self.ledger.realized
            self.fills = self.ledger.counts["fills"]
            fills.append(
                {
                    "symbol": symbol,
                    "action": signal["action"],
                    "quantity": abs(result["quantity"]),
                    "assumed_price_krw": price,
                    "commission_krw": result["commission_krw"],
                    "transaction_tax_krw": result["transaction_tax_krw"],
                    "source_at": source,
                    "observed_at": observed,
                    "fill_at": observed,
                    "decision_at": decided,
                    "signal": signal,
                    "reason": reason,
                }
            )
        return fills

    def summary(self, prices: dict[str, tuple[Decimal, datetime]]) -> dict[str, Any]:
        return {
            **super().summary(prices),
            "commission_krw": self.ledger.commissions,
            "transaction_tax_krw": self.ledger.taxes,
            "execution_counts": dict(self.ledger.counts),
            "reserved_cash_krw": self.reserved_cash,
            "available_cash_krw": self.available_cash,
            "quantity_policy": "whole_shares",
            "commission_rate": COMMISSION,
            "stock_sale_tax_rate": STOCK_SELL_TAX,
            "cost_assumptions": "Unrounded KR 0.015% commission and ordinary-stock sale tax; "
            "account-specific API commission and broker rounding unverified",
            "execution_model": "newer_last_trade_assumption_not_depth",
            "mark_basis": "last_observed_price_not_closing_or_liquidation_value",
        }


def seed_accounts(
    pairs: dict[str, Comparison], legacy_events: Path, listing_path: Path
) -> dict[str, int]:
    classification = listings(listing_path)
    ledgers = {f"{key}_{name}": Ledger() for key, pair in pairs.items() for name in pair.accounts}
    with legacy_events.open("rb") as events:
        for line in events:
            event = json.loads(line)
            if event["kind"] != "fill":
                continue
            symbol = event["symbol"]
            quantity = Decimal(event["quantity"])
            if event["action"] == "SELL":
                quantity = -quantity
            ledgers[event["account"]].apply(
                symbol, quantity, Decimal(event["assumed_price_krw"]), classification.get(symbol)
            )

    for key, pair in pairs.items():
        for name, original in pair.accounts.items():
            ledger = ledgers[f"{key}_{name}"]
            pair.accounts[name] = WholeShareAccount(original, ledger, classification, pair.prices)
    return {key: ledger.counts["fills"] for key, ledger in ledgers.items()}
