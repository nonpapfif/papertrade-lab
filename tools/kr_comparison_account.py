"""Legacy Korean comparison account state, independent of runner orchestration."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

CAPITAL = Decimal(100_000)


class Account:
    def __init__(self, allocation: Decimal | None = None) -> None:
        self.allocation = allocation
        self.cash = CAPITAL
        self.positions: dict[str, tuple[Decimal, Decimal]] = {}
        self.realized = Decimal(0)
        self.fills = 0
        self.pending: dict[str, tuple[dict[str, Any], datetime, str]] = {}
        self.budgets: dict[str, Decimal] = {}

    def queue(
        self,
        signal: dict[str, Any],
        decided: datetime,
        reason: str,
        prices: dict[str, tuple[Decimal, datetime]],
    ) -> None:
        symbol = signal["symbol"]
        self.pending[symbol] = signal, decided, reason
        if self.allocation is not None and signal["action"] == "BUY":
            self.budgets[symbol] = self.summary(prices)["equity_krw"] * self.allocation

    def wants(self, signal: dict[str, Any]) -> bool:
        return (signal["symbol"] in self.positions) == (signal["action"] == "SELL")

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
                continue
            if symbol in quotes and quotes[symbol][1] > decided:
                ready.append((symbol, signal, decided, reason))
                del self.pending[symbol]

        buy_count = sum(signal["action"] == "BUY" for _, signal, _, _ in ready)
        records = []
        budget = Decimal(0)
        for symbol, signal, decided, reason in sorted(
            ready,
            key=lambda row: (
                row[1]["action"] == "BUY",
                -row[1].get("strength", Decimal(0)) if self.allocation is not None else 0,
                row[0] if self.allocation is not None else "",
            ),
        ):
            price, source = quotes[symbol]
            if signal["action"] == "SELL":
                quantity, basis = self.positions.pop(symbol)
                proceeds = quantity * price
                self.cash += proceeds
                self.realized += proceeds - basis
            else:
                if self.allocation is None:
                    if not budget:
                        budget = self.cash / buy_count
                    cost = min(budget, self.cash)
                else:
                    cost = self.budgets.pop(symbol)
                    if self.cash < cost:
                        continue
                if cost <= 0:
                    continue
                quantity = cost / price
                self.cash -= cost
                self.positions[symbol] = quantity, cost
            self.fills += 1
            records.append(
                {
                    "symbol": symbol,
                    "action": signal["action"],
                    "quantity": quantity,
                    "assumed_price_krw": price,
                    "source_at": source,
                    "observed_at": observed,
                    "fill_at": observed,
                    "decision_at": decided,
                    "signal": signal,
                    "reason": reason,
                }
            )
        return records

    def summary(self, prices: dict[str, tuple[Decimal, datetime]]) -> dict[str, Any]:
        positions = {}
        unrealized = Decimal(0)
        for symbol, (quantity, basis) in self.positions.items():
            price, source = prices[symbol]
            value = quantity * price
            unrealized += value - basis
            positions[symbol] = {
                "quantity": quantity,
                "cost_krw": basis,
                "mark_krw": price,
                "market_value_krw": value,
                "source_at": source,
            }
        return {
            "cash_krw": self.cash,
            "equity_krw": CAPITAL + self.realized + unrealized,
            "realized_krw": self.realized,
            "unrealized_krw": unrealized,
            "positions": positions,
            "fills": self.fills,
            "pending": sorted(self.pending),
        }
