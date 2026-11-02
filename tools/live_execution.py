"""Virtual last-price and live-depth execution models; never submit broker orders."""

from __future__ import annotations

import copy
import math
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_CEILING, Decimal
from typing import Any

from tools.search_portfolios import Book
from tools.week_hypotheses import WeekEngine

DEFAULT_FEES: dict[str, float] = {
    "commission_rate": 0.001,
    "commission_exemption_usd": 10.0,
    "sec_sell_rate": 0.0000206,
    "sec_minimum_usd": 0.01,
    "taf_rate": 0.0,
    "annual_allowance_krw": 2_500_000.0,
    "capital_gains_tax_rate": 0.22,
    "annual_realized_before_run_krw": 0.0,
    "max_quote_age_seconds": 60.0,
    "max_fx_age_seconds": 60.0,
}


def transaction_fees(
    notional: float, quantity: int, side: str, schedule: dict[str, float]
) -> dict[str, float]:
    """SEC cent rounding and its minimum are pass-through estimates, not confirmed Toss rules."""
    if not math.isfinite(notional) or notional < 0 or quantity < 0 or side not in {"BUY", "SELL"}:
        raise ValueError("Invalid virtual fee calculation")
    commission = (
        notional * schedule["commission_rate"]
        if notional > schedule["commission_exemption_usd"]
        else 0.0
    )
    sec = 0.0
    if side == "SELL" and notional:
        amount = Decimal(str(notional)) * Decimal(str(schedule["sec_sell_rate"]))
        sec = max(
            schedule["sec_minimum_usd"], float(amount.quantize(Decimal("0.01"), ROUND_CEILING))
        )
    taf = quantity * schedule["taf_rate"] if side == "SELL" else 0.0
    return {
        "commission_usd": commission,
        "sec_usd": sec,
        "taf_usd": taf,
        "total_usd": commission + sec + taf,
    }


@dataclass
class ExecutionContext:
    mode: str
    schedule: dict[str, float]
    frame: dict[str, Any] = field(default_factory=dict)
    probing: bool = False
    requested: set[int] = field(default_factory=set)
    tax_realized_krw: dict[str, float] = field(default_factory=dict)
    tax_missing_fx: set[str] = field(default_factory=set)


class ExecutionBook(Book):
    """One independent paper account with fees, exact depth and original whole-share holdings."""

    def __init__(
        self,
        config: dict[str, Any],
        market: str,
        phase: str,
        detailed: bool = False,
        *,
        context: ExecutionContext,
        initial_equity: float = 100.0,
    ) -> None:
        super().__init__(
            config, market, phase, detailed, initial_equity=initial_equity, whole_shares=True
        )
        self.context = context
        self.training_proxy = phase == "daily_shadow"
        self.fee_totals = {
            key: 0.0 for key in ("commission_usd", "sec_usd", "taf_usd", "total_usd")
        }
        self.entry_fees: dict[int, float] = {}
        self.entry_cost_krw: dict[int, float | None] = {}
        self.quote_failure_counts: Counter[str] = Counter()
        self.depth_at: str | None = None
        self.depth_used: dict[tuple[int, str], int] = {}
        self.closed_net_usd = 0.0

    def _failure(self, reason: str) -> None:
        if not self.context.probing:
            self.quote_failure_counts[reason] += 1

    def _quote(
        self,
        symbol: int,
        side: str,
        last: float,
        *,
        record_failure: bool = True,
    ) -> tuple[list[tuple[float, int]], str] | None:
        frame = self.context.frame
        at = frame["at"]
        if self.depth_at != at:
            self.depth_at, self.depth_used = at, {}
        capacity = (
            (abs(int(self.positions[symbol].quantity)) if symbol in self.positions else 0)
            + math.ceil(max(self.cash, 0) / last)
            + self.depth_used.get((symbol, side), 0)
        )
        if self.context.mode == "last" or self.training_proxy:
            source = frame.get("source_times", {}).get(symbol, at)
            return [(last, max(1, capacity))], source
        books = frame.get("orderbooks", {})
        failures = frame.get("orderbook_failures", {})
        snapshot = books.get(symbol, books.get(str(symbol)))
        attempted = symbol in failures or str(symbol) in failures
        reason = None
        if snapshot is None:
            reason = "quote_unavailable" if attempted else "quote_missing"
        else:
            try:
                source = snapshot["timestamp"]
                age = (datetime.fromisoformat(at) - datetime.fromisoformat(source)).total_seconds()
                if age < 0:
                    reason = "quote_future"
                elif age > self.context.schedule["max_quote_age_seconds"]:
                    reason = "quote_stale"
                elif snapshot.get("currency") != "USD":
                    reason = "quote_currency"
                else:
                    levels = []
                    for row in snapshot["asks" if side == "BUY" else "bids"]:
                        price, volume = float(row["price"]), float(row["volume"])
                        if not math.isfinite(price) or price <= 0 or not math.isfinite(volume):
                            raise ValueError("Invalid depth level")
                        if volume < 0:
                            raise ValueError("Negative depth volume")
                        if math.floor(volume):
                            levels.append((price, math.floor(volume)))
                    levels.sort(reverse=side == "SELL")
                    if levels:
                        return levels, source
                    reason = "quote_empty_depth"
            except (KeyError, TypeError, ValueError, OverflowError):
                reason = "quote_invalid"
        if self.context.probing and not attempted and reason in {"quote_missing", "quote_stale"}:
            self.context.requested.add(symbol)
            return [(last, max(1, capacity))], at
        if record_failure:
            self._failure(reason or "quote_invalid")
        return None

    def _execution(
        self,
        symbol: int,
        side: str,
        quantity: int,
        last: float,
        *,
        record_failure: bool = True,
    ) -> tuple[float, dict[str, float], str] | None:
        quote = self._quote(symbol, side, last, record_failure=record_failure)
        if quote is None:
            return None
        levels, source = quote
        used = self.depth_used.get((symbol, side), 0)
        remaining, notional = quantity, 0.0
        for price, volume in levels:
            skip = min(used, volume)
            used -= skip
            take = min(remaining, volume - skip)
            notional += take * price
            remaining -= take
            if remaining == 0:
                break
        if remaining:
            if record_failure:
                self._failure("insufficient_depth")
            return None
        if not math.isfinite(notional):
            if record_failure:
                self._failure("quote_notional_invalid")
            return None
        fees = (
            transaction_fees(notional, quantity, side, self.context.schedule)
            if self.context.mode == "quotes"
            else {key: 0.0 for key in self.fee_totals}
        )
        return notional / quantity, fees, source

    def _buy_quantity(self, symbol: int, amount: float, last: float) -> int:
        quote = self._quote(symbol, "BUY", last)
        if quote is None:
            return 0
        levels, _ = quote
        available = max(
            0, sum(volume for _, volume in levels) - self.depth_used.get((symbol, "BUY"), 0)
        )
        budget = max(0.0, min(amount, self.cash))
        low, high = 0, min(available, math.floor(budget / levels[0][0]))
        while low < high:
            quantity = (low + high + 1) // 2
            execution = self._execution(symbol, "BUY", quantity, last)
            if execution is None:
                high = quantity - 1
                continue
            price, fees, _ = execution
            if quantity * price + fees["total_usd"] <= budget:
                low = quantity
            else:
                high = quantity - 1
        return low

    def whole_orders(
        self,
        candidates: Iterable[list[Any]],
        prices: dict[int, float],
        free: int,
        slot_budget: float,
        budget: float,
    ) -> list[tuple[list[Any], int]]:
        available = max(0.0, min(budget, self.cash))
        if free <= 0 or slot_budget <= 0 or available <= 0:
            return []
        iterator, exhausted = iter(candidates), False
        selected: list[list[Any]] = []
        while True:
            while len(selected) < free and not exhausted:
                try:
                    row = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                if self._quote(row[0], "BUY", prices[row[0]]) is not None:
                    selected.append(row)
            if not selected:
                return []
            spend = min(available, slot_budget * len(selected))
            scale = max(row[2] for row in selected)
            weights = [
                row[2] / scale if self.config["sizing"] == "score" else 1.0 for row in selected
            ]
            total = sum(weights)
            if total <= 0:
                self.zero_quantity += len(selected)
                return []
            orders, remaining_cash = [], self.cash
            for row, weight in zip(selected, weights, strict=True):
                amount = min(spend * weight / total, remaining_cash)
                quantity = self._buy_quantity(row[0], amount, prices[row[0]])
                if quantity:
                    execution = self._execution(row[0], "BUY", quantity, prices[row[0]])
                    if execution is None:
                        raise RuntimeError("A planned virtual entry lost its immutable depth")
                    price, fees, _ = execution
                    remaining_cash -= quantity * price + fees["total_usd"]
                orders.append((row, quantity))
            rejected = sum(quantity == 0 for _, quantity in orders)
            self.zero_quantity += rejected
            if not rejected:
                return orders
            selected = [row for row, quantity in orders if quantity]
            if exhausted and not selected:
                return []

    def _current_fx(self) -> float | None:
        frame, schedule = self.context.frame, self.context.schedule
        try:
            fx = float(frame["fx"])
            age = (
                datetime.fromisoformat(frame["at"]) - datetime.fromisoformat(frame["fx_at"])
            ).total_seconds()
            if not math.isfinite(fx) or fx <= 0 or not 0 <= age <= schedule["max_fx_age_seconds"]:
                raise ValueError("Unavailable trade-time FX")
        except (KeyError, TypeError, ValueError):
            return None
        return fx

    def _record_tax(self, realized_krw: float | None) -> None:
        if self.training_proxy:
            return
        if realized_krw is None:
            self.context.tax_missing_fx.add(self.phase)
            return
        schedule = self.context.schedule
        ledger = self.context.tax_realized_krw
        ledger[self.phase] = (
            ledger.get(self.phase, schedule["annual_realized_before_run_krw"]) + realized_krw
        )

    def fill(
        self,
        symbol: int,
        quantity: float,
        price: float,
        at: str,
        tick: int,
        reason: str,
        strength: float | None = None,
    ) -> None:
        if quantity == 0 or not float(quantity).is_integer():
            raise ValueError("Virtual live execution requires nonzero whole shares")
        if not math.isfinite(price) or price <= 0:
            raise ValueError("Virtual live execution requires positive finite last prices")
        entry = reason == "entry"
        count, side = abs(int(quantity)), "BUY" if entry else "SELL"
        execution = self._execution(symbol, side, count, price)
        if execution is None:
            if entry:
                raise RuntimeError("A virtual entry must be planned against executable depth")
            return
        actual_price, fees, source = execution
        if entry and count * actual_price + fees["total_usd"] > self.cash:
            raise ValueError("Fee-inclusive virtual entry exceeds available cash")
        gross_pnl = (
            None
            if entry
            else self.positions[symbol].quantity * (actual_price - self.positions[symbol].entry)
        )
        entry_fee = 0.0 if entry else self.entry_fees.pop(symbol, 0.0)
        entry_basis = None if entry else self.entry_cost_krw.pop(symbol, None)
        fx = self._current_fx()
        super().fill(symbol, quantity, actual_price, at, tick, reason, strength)
        self.cash -= fees["total_usd"]
        self.realized -= fees["total_usd"]
        for key, value in fees.items():
            self.fee_totals[key] += value
        if entry:
            self.entry_fees[symbol] = fees["total_usd"]
            self.entry_cost_krw[symbol] = (
                (count * actual_price + fees["total_usd"]) * fx if fx is not None else None
            )
        else:
            assert gross_pnl is not None
            net_pnl = gross_pnl - entry_fee - fees["total_usd"]
            self.wins += int(net_pnl > 0) - int(gross_pnl > 0)
            self.closed_net_usd += net_pnl
            self._record_tax(
                (count * actual_price - fees["total_usd"]) * fx - entry_basis
                if fx is not None and entry_basis is not None
                else None
            )
        self.depth_used[(symbol, side)] = self.depth_used.get((symbol, side), 0) + count
        if self.detailed:
            trade = self.trades[-1]
            trade.update(
                source_at=source,
                signal_at=self.context.frame.get("signal_at", at),
                price_observed_at=self.context.frame.get("price_observed_at", at),
                signal_source_at=self.context.frame.get("source_times", {}).get(symbol),
                execution_model=self.context.mode,
                accounting_currency="USD",
                fee_usd=fees["total_usd"],
                fee_components=fees,
                cash=self.cash,
                gross_realized_pnl=gross_pnl,
                realized_pnl=None if entry else net_pnl,
                observed_fx_krw_per_usd=fx,
                entry_cost_basis_krw=self.entry_cost_krw[symbol] if entry else entry_basis,
            )

    def summary(self, prices: dict[int, float], times: dict[int, str], at: str) -> dict[str, Any]:
        result = super().summary(prices, times, at)
        schedule = self.context.schedule
        annual = self.context.tax_realized_krw.get(
            self.phase, schedule["annual_realized_before_run_krw"]
        )
        tax_known = self.phase not in self.context.tax_missing_fx
        reserve = (
            max(0.0, annual - schedule["annual_allowance_krw"]) * schedule["capital_gains_tax_rate"]
            if tax_known
            else None
        )
        fx = self._current_fx()
        missing = []
        liquidation = self.cash
        projected_annual = annual if tax_known and fx is not None else None
        for symbol, position in self.positions.items():
            execution = (
                self._execution(
                    symbol, "SELL", int(position.quantity), prices[symbol], record_failure=False
                )
                if self.context.mode == "quotes"
                else None
            )
            if execution is None:
                missing.append(symbol)
                continue
            price, fees, _ = execution
            net_proceeds = position.quantity * price - fees["total_usd"]
            liquidation += net_proceeds
            entry_basis = self.entry_cost_krw.get(symbol)
            if entry_basis is None:
                projected_annual = None
            elif projected_annual is not None and fx is not None:
                projected_annual += net_proceeds * fx - entry_basis
        bid_equity = liquidation if not missing and self.context.mode == "quotes" else None
        if bid_equity is None:
            projected_annual = None
        projected_reserve = (
            max(0.0, projected_annual - schedule["annual_allowance_krw"])
            * schedule["capital_gains_tax_rate"]
            if projected_annual is not None
            else None
        )
        reserve_usd = reserve / fx if reserve is not None and fx is not None else None
        result.update(
            execution_model=self.context.mode,
            mark_basis="last_price_mark_not_liquidated",
            fees_usd=self.fee_totals["total_usd"],
            commission_usd=self.fee_totals["commission_usd"],
            sec_usd=self.fee_totals["sec_usd"],
            taf_usd=self.fee_totals["taf_usd"],
            closed_net_realized_usd=self.closed_net_usd,
            quote_failure_counts=dict(self.quote_failure_counts),
            estimated_annual_realized_krw=annual if tax_known else None,
            estimated_capital_tax_reserve_krw=reserve,
            estimated_annual_realized_if_liquidated_krw=projected_annual,
            estimated_capital_tax_reserve_if_liquidated_krw=projected_reserve,
            bid_liquidation_equity_estimate_usd=bid_equity,
            bid_liquidation_missing_symbols=missing,
            bid_liquidation_coverage=(len(self.positions) - len(missing)) / len(self.positions)
            if self.positions
            else 1.0,
            bid_liquidation_as_of=self.context.frame.get("at"),
            after_tax_mark_equity_estimate_usd=self.equity - reserve_usd
            if reserve_usd is not None
            else None,
            after_tax_bid_liquidation_estimate_usd=bid_equity - projected_reserve / fx
            if bid_equity is not None and projected_reserve is not None and fx is not None
            else None,
            capital_tax_cash_deducted_usd=0.0,
            tax_basis="Annual KRW sale proceeds net of exit fees at observed exit FX, minus "
            "purchase cost including entry fees at observed entry FX. Trade-time estimate "
            "only, not legal settlement FX or a per-trade cash withdrawal.",
        )
        return result


class LiveWeekEngine(WeekEngine):
    """Use live market-data snapshots for virtual accounts, with a cheap declared training proxy."""

    def __init__(
        self,
        configs: list[dict[str, Any]],
        specs: list[dict[str, Any]],
        policies: list[dict[str, Any]],
        *,
        mode: str,
        capital: float = 100.0,
        fee_schedule: dict[str, float] | None = None,
        market: str = "US",
    ) -> None:
        if mode not in {"last", "quotes"} or market != "US":
            raise ValueError("Virtual live execution supports US last/quotes models")
        schedule = {**DEFAULT_FEES, **(fee_schedule or {})}
        if any(
            not math.isfinite(value) or (value < 0 and key != "annual_realized_before_run_krw")
            for key, value in schedule.items()
        ):
            raise ValueError(
                "Fee rates and freshness must be finite and nonnegative; prior gains finite"
            )
        self.execution = ExecutionContext(mode, schedule)
        super().__init__(
            configs,
            specs,
            {},
            policies,
            market=market,
            long_only=True,
            initial_equity=capital,
            whole_shares=True,
        )

    def _new_book(self, config: dict[str, Any], phase: str, detailed: bool) -> ExecutionBook:
        return ExecutionBook(
            config,
            self.market,
            phase,
            detailed,
            context=self.execution,
            initial_equity=self.initial_equity,
        )

    def step(self, frame: dict[str, Any]) -> None:
        self.execution.frame = frame
        super().step(frame)

    def _actual_step(
        self,
        state: dict[str, Any],
        frame: dict[str, Any],
        fresh: dict[int, float],
        entry_allowed: bool,
    ) -> None:
        if self.execution.mode != "quotes":
            super()._actual_step(state, frame, fresh, entry_allowed)
            return
        book: ExecutionBook = state["book"]
        owners = state["owners"]
        at, tick = frame["at"], frame["tick"]
        for identifier in sorted(set(owners.values())):
            executable = {}
            for symbol, owner in owners.items():
                if owner != identifier or symbol not in book.positions:
                    continue
                execution = book._execution(
                    symbol, "SELL", int(book.positions[symbol].quantity), self.prices[symbol]
                )
                if execution is not None:
                    executable[symbol] = execution[0]
            if not executable:
                continue
            config = self.config_by_id[identifier]
            book.config = config
            curve_length, trade_length = len(book.curve), len(book.trades)
            book.step(
                at,
                tick,
                executable,
                self.prices,
                frame["ranks"][config["signal"]][self.market_index],
                False,
            )
            del book.curve[curve_length:]
            for trade in book.trades[trade_length:]:
                trade.update(configuration=identifier, day=self.day)
            owners = {symbol: owner for symbol, owner in owners.items() if symbol in book.positions}
            state["owners"] = owners
        if state["policy"].get("allocation") == "family_ensemble":
            self._ensemble_entries(state, frame, fresh, entry_allowed)
        else:
            self._winner_entries(state, frame, fresh, entry_allowed)

    def quote_requests(self, frame: dict[str, Any]) -> set[int]:
        if self.execution.mode == "last":
            return set()
        trial = copy.deepcopy(self)
        trial.execution.probing = True
        trial.execution.requested.clear()
        trial.execution.frame = frame
        for state in trial.states:
            book = state["book"]
            for symbol in book.positions:
                book._quote(symbol, "SELL", trial.prices[symbol])
        trial.step(frame)
        return trial.execution.requested

    def required_symbols(self, frame: dict[str, Any]) -> set[int]:
        return self.quote_requests(frame)

    def report(self) -> dict[str, Any]:
        result = super().report()
        result["execution_model"] = self.execution.mode
        result["training_proxy"] = (
            "Last-price whole-share execution with the verified fee schedule; "
            "shadow portfolios do not request live orderbooks."
            if self.execution.mode == "quotes"
            else "Last-price whole-share execution with zero fees."
        )
        result["fee_schedule"] = self.execution.schedule
        result["trading_fees_enabled"] = self.execution.mode == "quotes"
        result["fee_assumptions"] = (
            "US commission 0.1% above USD 10/order; SEC sales rate 0.0000206. "
            "SEC ceil-to-cent and USD 0.01 minimum are estimated broker pass-through. "
            "FINRA TAF is zero for the verified October 1-December 31, 2026 holiday."
        )
        result["depth_assumption"] = (
            "Each virtual account independently uses the supplied visible depth; consumed "
            "depth is shared within that account and frame. Partial exits are not fabricated. "
            "Missing, stale, future or non-USD books cannot execute actual trades. "
            "Last-price affordability screens define eligibility before requesting depth."
        )
        result["exit_decision_basis"] = (
            "Whole-position executable bid depth for stops, targets, trailing and exits, "
            "including held symbols whose last trade is stale. Entry eligibility retains "
            "the fresh last-price feature screen."
            if self.execution.mode == "quotes"
            else "Observed fresh last prices."
        )
        for hypothesis in result["hypotheses"]:
            if hypothesis["policy"].get("reset_daily") and hypothesis["daily"]:
                for key in (
                    "fees_usd",
                    "commission_usd",
                    "sec_usd",
                    "taf_usd",
                    "closed_net_realized_usd",
                ):
                    hypothesis["summary"][key] = sum(
                        row["summary"][key] for row in hypothesis["daily"]
                    )
                failures: Counter[str] = Counter()
                for day in hypothesis["daily"]:
                    failures.update(day["summary"]["quote_failure_counts"])
                hypothesis["summary"]["quote_failure_counts"] = dict(failures)
                summary = hypothesis["summary"]
                reserve, fx = (
                    summary["estimated_capital_tax_reserve_krw"],
                    self.states[0]["book"]._current_fx() if self.states else None,
                )
                summary["after_tax_mark_equity_estimate_usd"] = (
                    summary["equity"] - reserve / fx
                    if reserve is not None and fx is not None and fx > 0
                    else None
                )
                if len(hypothesis["daily"]) > 1:
                    summary["bid_liquidation_equity_estimate_usd"] = None
                    summary["bid_liquidation_coverage"] = None
                    summary["bid_liquidation_as_of"] = None
                    summary["after_tax_bid_liquidation_estimate_usd"] = None
                    summary["estimated_annual_realized_if_liquidated_krw"] = None
                    summary["estimated_capital_tax_reserve_if_liquidated_krw"] = None
                    summary["bid_liquidation_basis"] = (
                        "Independent daily accounts have different valuation times."
                    )
        return result
