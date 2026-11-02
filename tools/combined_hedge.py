"""A single funded account following causal weighted reference-strategy exposures."""

from __future__ import annotations

import copy
import math
import statistics
from datetime import datetime, timedelta
from typing import Any

from tools.live_execution import ExecutionBook, ExecutionContext, LiveWeekEngine


class CombinedHedge:
    def __init__(self, reference: LiveWeekEngine, capital: float = 100) -> None:
        self.capital = capital
        self.context = ExecutionContext("quotes", dict(reference.execution.schedule))
        self.book = ExecutionBook(
            reference.configs[0],
            "US",
            "combined_hedge",
            True,
            context=self.context,
            initial_equity=capital,
        )
        self.prices: dict[int, float] = {}
        self.times: dict[int, str] = {}
        self.performance: list[dict[str, Any]] = []
        self.allocations: list[dict[str, Any]] = []
        self.targets: dict[int, int] = {}
        self.last_at: str | None = None

    def required_symbols(self, frame: dict[str, Any]) -> set[int]:
        attempted = set(frame.get("orderbook_failures", {}))
        available = set(frame.get("orderbooks", {}))
        return set(self.book.positions) - available - attempted

    def weights(self, reference: LiveWeekEngine, at: str) -> list[float]:
        if reference.last_at is not None and (
            not self.performance or self.performance[-1]["at"] != reference.last_at
        ):
            self.performance.append(
                {
                    "at": reference.last_at,
                    "equities": [s["book"].equity for s in reference.states],
                }
            )
        cutoff = datetime.fromisoformat(at) - timedelta(minutes=60)
        baseline = [reference.initial_equity] * len(reference.states)
        for row in self.performance:
            if datetime.fromisoformat(row["at"]) > cutoff:
                break
            baseline = row["equities"]
        returns = [
            s["book"].equity / before - 1 if before > 0 else -1.0
            for s, before in zip(reference.states, baseline, strict=True)
        ]
        scale = max(statistics.pstdev(returns), 1e-6)
        scores = [
            ret / scale + (s["book"].wins + 1) / (s["book"].closed + 2) - 0.5
            for ret, s in zip(returns, reference.states, strict=True)
        ]
        maximum = max(scores)
        raw = [math.exp(score - maximum) for score in scores]
        total = sum(raw)
        return [value / total for value in raw]

    def prepare(self, reference: LiveWeekEngine, frame: dict[str, Any]) -> None:
        weights = self.weights(reference, frame["at"])
        self.prices.update(reference.prices)
        self.times.update(reference.times)
        for symbol, mark in frame["marks"].items():
            self.prices[int(symbol)] = float(mark[0])
            self.times[int(symbol)] = mark[1]
        self.book.mark(self.prices)
        trial = copy.deepcopy(reference)
        trial.execution.probing = True
        trial.step(frame)
        dollars: dict[int, float] = {}
        for weight, state in zip(weights, trial.states, strict=True):
            book = state["book"]
            if book.equity <= 0:
                continue
            for symbol, position in book.positions.items():
                exposure = position.quantity * trial.prices[symbol] / book.equity
                dollars[symbol] = (
                    dollars.get(symbol, 0.0) + max(self.book.equity, 0) * weight * exposure
                )
        self.targets = {
            symbol: max(0, math.floor(amount / self.prices[symbol]))
            for symbol, amount in dollars.items()
        }
        self.allocations.append(
            {
                "at": frame["at"],
                "performance_through": reference.last_at,
                "weights": {
                    s["policy"]["id"]: w for s, w in zip(reference.states, weights, strict=True)
                },
                "target_quantities": dict(self.targets),
                "concentration_equivalent_count": 1 / sum(w * w for w in weights),
            }
        )

    def _buy(self, symbol: int, quantity: int, frame: dict[str, Any]) -> None:
        old = copy.copy(self.book.positions.get(symbol))
        old_fee = self.book.entry_fees.get(symbol, 0.0)
        old_basis = self.book.entry_cost_krw.get(symbol)
        old_net = self.book.position_net_usd.get(symbol, 0.0)
        if old is not None:
            del self.book.positions[symbol]
        self.book.fill(symbol, quantity, self.prices[symbol], frame["at"], frame["tick"], "entry")
        if old is not None:
            new = self.book.positions[symbol]
            new_basis = self.book.entry_cost_krw[symbol]
            new.entry = (old.quantity * old.entry + new.quantity * new.entry) / (
                old.quantity + new.quantity
            )
            new.quantity += old.quantity
            new.tick = old.tick
            self.book.entry_fees[symbol] += old_fee
            self.book.entry_cost_krw[symbol] = (
                old_basis + new_basis if old_basis is not None and new_basis is not None else None
            )
            self.book.position_net_usd[symbol] = old_net
            self.book.opened -= 1
            self.book.trades[-1]["reason"] = "combined_increase"

    def step(self, frame: dict[str, Any]) -> None:
        self.context.frame = frame
        for symbol, position in list(self.book.positions.items()):
            reduction = max(0, int(position.quantity) - self.targets.get(symbol, 0))
            if reduction:
                self.book.fill(
                    symbol,
                    -reduction,
                    self.prices[symbol],
                    frame["at"],
                    frame["tick"],
                    "combined_rebalance",
                )
        increases = [
            (
                symbol,
                quantity - int(self.book.positions[symbol].quantity)
                if symbol in self.book.positions
                else quantity,
            )
            for symbol, quantity in self.targets.items()
        ]
        for symbol, requested in sorted(
            increases, key=lambda row: (-row[1] * self.prices[row[0]], row[0])
        ):
            if requested <= 0:
                continue
            quantity = (
                min(requested, self.book._buy_quantity(symbol, self.book.cash, self.prices[symbol]))
                if symbol in frame["quotes"]
                else 0
            )
            if quantity:
                self.book.entry_requested[symbol] = requested
                self._buy(symbol, quantity, frame)
            else:
                self.book._attempt(symbol, "BUY", requested, 0)
        self.book.mark(self.prices)
        self.last_at = frame["at"]
        self.book.curve.append(
            {"at": self.last_at, "equity": self.book.equity, "cash": self.book.cash}
        )

    def report(self) -> dict[str, Any]:
        return {
            "id": "combined_hedge",
            "summary": self.book.summary(self.prices, self.times, self.last_at)
            if self.last_at
            else None,
            "trades": list(self.book.trades),
            "order_attempts": list(self.book.order_attempts),
            "curve": list(self.book.curve),
            "positions": {
                s: {"quantity": p.quantity, "entry": p.entry}
                for s, p in self.book.positions.items()
            },
            "allocations": list(self.allocations),
            "allocator": "Softmax of lagged 60-minute reference net-mark return / cross-sectional standard deviation, plus Beta(1,1)-smoothed full-history position win rate minus 0.5.",
            "accounting": "One funded cash account. Net symbol targets are aggregated before whole-share execution; fees, depth consumption and annual tax allowance apply once to this account.",
            "attribution": "Weights are target allocations driven by independent reference performance, not separately funded or realized strategy sleeves. No reference profits are credited to combined cash.",
            "limitations": "Reference returns use fee-bearing last-price marks, not guaranteed liquidation values. Duplicate policies can overweight similar exposures. This is an allocation experiment, not proof of downside hedging.",
            "ai_calls": 0,
        }
