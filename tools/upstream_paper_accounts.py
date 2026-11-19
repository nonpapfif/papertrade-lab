"""Isolated ledgers for the pinned upstream paper session."""

from __future__ import annotations

import datetime as dt
import math
from collections import Counter
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from tools.live_execution import DEFAULT_FEES, transaction_fees
from tools.session_research import ResearchReplay
from tools.upstream_strategy import STEP, Upstream, epoch, iso, utc

PROFILES = ("source_only", "source_plus_tradingagents")
FEES = {**DEFAULT_FEES, "commission_exemption_usd": 0.0}


@dataclass(frozen=True)
class Quote:
    source: float
    received: float
    processed: float
    bid: float
    bid_qty: int
    ask: float
    ask_qty: int

    @classmethod
    def parse(cls, at: float, raw: dict[str, Any], received: float) -> Quote:
        if raw.get("currency") != "USD":
            raise ValueError("Non-USD book")
        source = epoch(raw["timestamp"])
        if not source <= received <= at or not 0 <= at - source <= FEES["max_quote_age_seconds"]:
            raise ValueError("Stale or future book")
        levels = []
        for side in ("bids", "asks"):
            rows = []
            for level in raw[side]:
                price, depth = float(level["price"]), float(level["volume"])
                if not math.isfinite(price) or price <= 0 or not math.isfinite(depth) or depth < 0:
                    raise ValueError("Invalid book price or size")
                if depth >= 1:
                    rows.append((price, math.floor(depth)))
            levels.append(rows)
        bid, bid_qty = max(levels[0], key=lambda row: row[0])
        ask, ask_qty = min(levels[1], key=lambda row: row[0])
        if ask < bid:
            raise ValueError("Crossed book")
        return cls(source, received, at, bid, bid_qty, ask, ask_qty)

    def later_than(self, at: float) -> bool:
        return self.received > at and self.source > at


@dataclass(frozen=True)
class Signal:
    seen: float
    trigger: float
    plan: dict[str, Any]
    expires: float


@dataclass(frozen=True)
class Intent:
    at: float
    quantity: int
    plan: dict[str, Any]
    reason: str
    order_number: int
    expires: float
    reservation: Decimal = Decimal(0)


@dataclass
class Position:
    quantity: int
    cost: Decimal
    plan: dict[str, Any]
    opened_at: float
    last_exit_source: float = 0.0


def money(value: Decimal) -> str:
    return format(value.normalize(), "f")


def costs(price: float | Decimal, qty: int, side: str) -> tuple[Decimal, dict[str, float]]:
    gross = Decimal(str(price)) * qty
    fees = transaction_fees(float(gross), qty, side, FEES)
    # Preserve the shared model's unrounded commission without float arithmetic in the ledger.
    commission = gross * Decimal(str(FEES["commission_rate"]))
    fees["commission_usd"] = float(commission)
    net_fees = commission + Decimal(str(fees["sec_usd"])) + Decimal(str(fees["taf_usd"]))
    fees["total_usd"] = float(net_fees)
    return (gross + net_fees if side == "BUY" else gross - net_fees), fees


class Account:
    def __init__(self, source: Upstream, config: dict[str, Any], day: str) -> None:
        self.source, self.config, self.day = source, config, day
        self.seed = self.cash = Decimal(str(config["budget"]))
        self.realized = Decimal(0)
        self.positions: dict[str, Position] = {}
        self.candidates: dict[str, Signal] = {}
        self.buys: dict[str, Intent] = {}
        self.sells: dict[str, Intent] = {}
        self.orders: list[dict[str, Any]] = []
        self.counts: Counter[str] = Counter()
        self.fees = Decimal(0)

    def entry_limit_reached(self) -> bool:
        limit = self.config["max_trades_per_day"]
        return bool(limit and sum(order["side"] == "buy" for order in self.orders) >= limit)

    def _available(self, ignore: int = 0) -> Decimal:
        reserved = sum(
            (order.reservation for order in self.buys.values() if order.order_number != ignore),
            Decimal(0),
        )
        return self.cash - reserved

    def _risk(
        self, order: dict[str, Any], at: float, quotes: dict[str, Quote], *, ignore: int = 0
    ) -> dict[str, Any]:
        scope = {order["symbol"], *self.positions}
        prices = {
            symbol: (
                quote.ask if symbol == order["symbol"] and order["side"] == "buy" else quote.bid
            )
            for symbol, quote in quotes.items()
            if symbol in scope and 0 <= at - quote.source <= FEES["max_quote_age_seconds"]
        }
        times = {symbol: utc(quotes[symbol].source) for symbol in prices}
        result = self.source.risk.check(
            [order],
            self.config,
            portfolio={
                "cash": float(self.cash),
                "positions": [
                    {
                        "symbol": symbol,
                        "qty": position.quantity,
                        "lots": [{"cost": float(position.cost)}],
                    }
                    for symbol, position in self.positions.items()
                ],
            },
            prices=prices,
            state={"orders": [row for row in self.orders if row["number"] != ignore]},
            today=self.day,
            now=utc(at),
            price_times=times,
        )
        return result

    def _size(
        self, signal: Signal, quote: Quote, available: Decimal, *, cap: int | None = None
    ) -> tuple[dict[str, Any], dict[str, Any] | None, Decimal]:
        sizing = self.source.budget.size_trade(
            signal.plan, self.config, cash=float(available), price=quote.ask, today=self.day
        )
        order = self.source.budget.bracket_order(sizing)
        if order is None:
            return sizing, None, Decimal(0)
        # Native sizing rounds the reference to cents. The paper debit uses the full ask
        # and unrounded per-share commission, so reserve at the higher of the two prices.
        available_after_buffer = available - Decimal(str(sizing["cash_buffer"]))
        reserve_price = max(Decimal(str(quote.ask)), Decimal(str(sizing["entry"])))
        per_share, _ = costs(reserve_price, 1, "BUY")
        affordable = max(0, int(available_after_buffer // per_share))
        qty = min(sizing["qty"], affordable, *([cap] if cap is not None else []))
        if not qty:
            return (
                {**sizing, "execution_refusal": "insufficient_cash_after_costs"},
                None,
                Decimal(0),
            )
        if qty != sizing["qty"]:
            order = self.source.budget.bracket_order(
                {
                    **sizing,
                    "qty": qty,
                    "cost": round(sizing["entry"] * qty, 2),
                    "max_loss": round(sizing["risk_per_share"] * qty, 2),
                }
            )
        return sizing, order, per_share * qty

    def _record_order(self, symbol: str, side: str, risk: dict[str, Any]) -> int:
        number = len(self.orders) + 1
        verdict = risk["results"][0]
        self.orders.append(
            {
                "number": number,
                "status": "placed",
                "date": self.day,
                "symbol": symbol,
                "side": side,
                "max_loss": verdict["max_loss"],
                "kind": verdict["kind"],
            }
        )
        return number

    def _activate_buy(
        self, symbol: str, signal: Signal, quote: Quote, quotes: dict[str, Quote]
    ) -> dict[str, Any]:
        base = {"kind": "entry_decision", "symbol": symbol, "trigger": iso(signal.trigger)}
        try:
            sizing, order, reserved = self._size(signal, quote, self._available())
        except self.source.budget.SizeError as error:
            return {**base, "status": "blocked", "source_reason": str(error)}
        if order is None:
            return {**base, "status": "blocked", "sizing": sizing}
        risk = self._risk(order, quote.processed, quotes)
        if not risk["pass"]:
            return {**base, "status": "blocked", "sizing": sizing, "risk": risk}
        qty = int(order["qty"])
        number = self._record_order(symbol, "buy", risk)
        self.buys[symbol] = Intent(
            quote.processed,
            qty,
            signal.plan,
            "opening_range_breakout",
            number,
            signal.expires,
            reserved,
        )
        return {
            **base,
            "status": "approved",
            "sizing": sizing,
            "risk": risk,
            "order": order,
            "number": number,
            "reserved_usd": money(reserved),
            "initial_quote_source": iso(quote.source),
        }

    def _fill_buy(
        self, symbol: str, current: Intent, quote: Quote, quotes: dict[str, Quote]
    ) -> dict[str, Any]:
        del self.buys[symbol]
        base = {"kind": "entry_confirmation", "symbol": symbol, "number": current.order_number}
        signal = Signal(current.at, current.at, current.plan, current.expires)
        try:
            sizing, order, _ = self._size(
                signal, quote, self._available(), cap=min(current.quantity, quote.ask_qty)
            )
        except self.source.budget.SizeError as error:
            return {**base, "status": "cancelled", "source_reason": str(error)}
        if order is None:
            return {**base, "status": "cancelled", "sizing": sizing}
        risk = self._risk(order, quote.processed, quotes, ignore=current.order_number)
        if not risk["pass"]:
            return {**base, "status": "cancelled", "sizing": sizing, "risk": risk}
        qty = int(order["qty"])
        debit, fees = costs(quote.ask, qty, "BUY")
        self.cash -= debit
        self.fees += debit - Decimal(str(quote.ask)) * qty
        self.positions[symbol] = Position(qty, debit, current.plan, quote.processed)
        return {
            **base,
            "status": "filled",
            "quantity": qty,
            "unfilled": current.quantity - qty,
            "price": quote.ask,
            "fees": fees,
            "cash_delta_usd": money(-debit),
            "risk": risk,
            "sizing": sizing,
            "side": "buy",
            "intent_at": iso(current.at),
            "quote_source": iso(quote.source),
            "quote_received": iso(quote.received),
        }

    def _activate_sell(
        self,
        symbol: str,
        position: Position,
        reason: str,
        at: float,
        quotes: dict[str, Quote],
    ) -> dict[str, Any]:
        ticket = {
            "symbol": symbol,
            "side": "sell",
            "type": "market",
            "time_in_force": "day",
            "qty": str(position.quantity),
        }
        if reason == "plan-target":
            ticket.update(type="limit", limit_price=str(position.plan["target"]))
        risk = self._risk(ticket, at, quotes)
        base = {"kind": "exit_decision", "symbol": symbol, "reason": reason, "risk": risk}
        if not risk["pass"]:
            return {**base, "status": "blocked"}
        number = self._record_order(symbol, "sell", risk)
        self.sells[symbol] = Intent(at, position.quantity, position.plan, reason, number, math.inf)
        return {**base, "status": "approved", "number": number, "order": ticket}

    def _fill_sell(
        self, symbol: str, position: Position, intent: Intent, quote: Quote
    ) -> dict[str, Any]:
        qty = min(position.quantity, quote.bid_qty)
        proceeds, fees = costs(quote.bid, qty, "SELL")
        basis = position.cost * qty / position.quantity
        realized = proceeds - basis
        self.cash += proceeds
        self.realized += realized
        self.fees += Decimal(str(quote.bid)) * qty - proceeds
        position.quantity -= qty
        position.cost -= basis
        position.last_exit_source = quote.source
        if not position.quantity:
            del self.positions[symbol], self.sells[symbol]
        return {
            "kind": "exit_confirmation",
            "symbol": symbol,
            "status": "filled",
            "side": "sell",
            "reason": intent.reason,
            "number": intent.order_number,
            "quantity": qty,
            "remaining": position.quantity,
            "price": quote.bid,
            "fees": fees,
            "cash_delta_usd": money(proceeds),
            "realized_usd": money(realized),
            "intent_at": iso(intent.at),
            "quote_source": iso(quote.source),
            "quote_received": iso(quote.received),
        }

    def observe_trade(
        self,
        symbol: str,
        price: float,
        source_at: float,
        at: float,
        quotes: dict[str, Quote],
    ) -> dict[str, Any] | None:
        position = self.positions.get(symbol)
        if position is None or source_at <= position.opened_at:
            return None
        sell = self.sells.get(symbol)
        reason = self.source.exit_reason(position.plan, price, at)
        if reason and (sell is None or sell.reason == "plan-target" and reason != sell.reason):
            action = self._activate_sell(symbol, position, reason, at, quotes)
            return {**action, "trigger_last_trade": price, "trigger_source": iso(source_at)}
        return None

    def observe_book(
        self, symbol: str, quote: Quote, quotes: dict[str, Quote]
    ) -> list[dict[str, Any]]:
        position, buy, sell = (
            self.positions.get(symbol),
            self.buys.get(symbol),
            self.sells.get(symbol),
        )
        if position is not None and quote.later_than(position.opened_at):
            if (
                sell
                and quote.later_than(sell.at)
                and quote.source > position.last_exit_source
                and (sell.reason != "plan-target" or quote.bid >= sell.plan["target"])
            ):
                return [self._fill_sell(symbol, position, sell, quote)]
        if buy is not None:
            if quote.processed >= buy.expires:
                del self.buys[symbol]
                return [self._expired_buy(symbol, buy)]
            if quote.later_than(buy.at):
                return [self._fill_buy(symbol, buy, quote, quotes)]
        signal = self.candidates.get(symbol)
        if signal and quote.processed >= signal.expires:
            del self.candidates[symbol]
            return [self._expired_candidate(symbol)]
        if signal and quote.received >= signal.seen and quote.source >= signal.trigger:
            del self.candidates[symbol]
            return [self._activate_buy(symbol, signal, quote, quotes)]
        return []

    @staticmethod
    def _expired_buy(symbol: str, intent: Intent) -> dict[str, Any]:
        return {
            "kind": "entry_confirmation",
            "symbol": symbol,
            "status": "expired",
            "number": intent.order_number,
            "reason": "no_later_book_in_source_signal_window",
        }

    @staticmethod
    def _expired_candidate(symbol: str) -> dict[str, Any]:
        return {
            "kind": "entry_decision",
            "symbol": symbol,
            "status": "expired",
            "reason": "no_usable_initial_book_in_source_signal_window",
        }

    def clock(self, at: float, quotes: dict[str, Quote], *, close: bool) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        expired_buys = [
            symbol for symbol, intent in self.buys.items() if close or at >= intent.expires
        ]
        expired_candidates = [
            symbol for symbol, signal in self.candidates.items() if close or at >= signal.expires
        ]
        actions.extend(self._expired_buy(symbol, self.buys.pop(symbol)) for symbol in expired_buys)
        for symbol in expired_candidates:
            del self.candidates[symbol]
            actions.append(self._expired_candidate(symbol))
        if close:
            for symbol, intent in self.sells.items():
                actions.append(
                    {
                        "kind": "exit_confirmation",
                        "symbol": symbol,
                        "status": "expired",
                        "number": intent.order_number,
                        "reason": "session_closed_without_fill",
                    }
                )
            self.sells.clear()
            return actions
        for symbol, position in self.positions.items():
            sell = self.sells.get(symbol)
            reason = self.source.exit_reason(position.plan, None, at)
            if reason and (sell is None or sell.reason == "plan-target"):
                actions.append(self._activate_sell(symbol, position, reason, at, quotes))
        return actions

    def summary(self, quotes: dict[str, Quote], at: float) -> dict[str, Any]:
        positions: dict[str, Any] = {}
        marked = self.cash
        complete = True
        for symbol, position in self.positions.items():
            quote = quotes.get(symbol)
            fresh = quote is not None and 0 <= at - quote.source <= FEES["max_quote_age_seconds"]
            net_exit = None
            if quote and fresh and quote.bid_qty >= position.quantity:
                net_exit, _ = costs(quote.bid, position.quantity, "SELL")
                marked += net_exit
            else:
                complete = False
            positions[symbol] = {
                "quantity": position.quantity,
                "remaining_basis_usd": money(position.cost),
                "stop": position.plan["stop"],
                "target": position.plan["target"],
                "flat_by": position.plan["flat_by"],
                "executable_mark_usd": money(net_exit) if net_exit is not None else None,
                "last_quote_source": iso(quote.source) if quote else None,
            }
        return {
            "seed_usd": money(self.seed),
            "cash_usd": money(self.cash),
            "realized_usd": money(self.realized),
            "fees_usd": money(self.fees),
            "liquidation_equity_usd": money(marked) if complete else None,
            "net_pnl_usd": money(marked - self.seed) if complete else None,
            "positions": positions,
            "pending_entries": sorted(self.buys),
            "awaiting_initial_book": sorted(self.candidates),
            "pending_exits": sorted(self.sells),
            "native_order_count": len(self.orders),
            "counts": dict(self.counts),
        }


class Comparison:
    def __init__(
        self,
        source: Upstream,
        plan: dict[str, Any],
        config_file: Path,
        research: ResearchReplay,
    ) -> None:
        self.source, self.plan, self.config_file, self.research = (
            source,
            plan,
            config_file,
            research,
        )
        self.config = plan["config"]
        self.day = dt.date.fromisoformat(plan["day"])
        self.start, self.end = epoch(plan["start"]), epoch(plan["end"])
        self.entry_end = min(self.end, epoch(plan["entry_cutoff"]) + 60)
        self.accounts = {name: Account(source, self.config, plan["day"]) for name in PROFILES}
        self.quotes: dict[str, Quote] = {}
        self.trade_times: dict[str, float] = {}
        self.seen: set[str] = set()
        self.signal_order: list[str] = []
        self.stats: Counter[str] = Counter()
        self.errors: Counter[str] = Counter()
        self.live_since: float | None = None
        self.at = epoch(plan["created_at"])

    def held_symbols(self) -> list[str]:
        return sorted(
            {symbol for account in self.accounts.values() for symbol in account.positions}
        )

    def active_symbols(self) -> list[str]:
        protected = set(self.held_symbols())
        for account in self.accounts.values():
            protected.update(account.buys)
        candidates = set().union(*(account.candidates for account in self.accounts.values()))
        return [symbol for symbol in self.signal_order if symbol in protected] + [
            symbol
            for symbol in self.signal_order
            if symbol in candidates and symbol not in protected
        ]

    def process(self, event: dict[str, Any]) -> dict[str, Any]:
        kind, at = event["kind"], epoch(event["at"])
        payload: Any = event.get("payload")
        if at < self.at:
            raise ValueError("Observation clock regressed")
        self.at = at
        self.stats[kind] += 1
        public: dict[str, Any] = {}
        outputs: dict[str, list[dict[str, Any]]] = {name: [] for name in self.accounts}
        if kind == "capture_started":
            self.live_since = max(at, self.start)
        elif kind == "scan":
            public, outputs = self._scan(payload, at, outputs)
        elif kind == "book":
            symbol, raw, received = payload
            try:
                quote = Quote.parse(at, raw, received)
                if not self.start <= quote.source <= quote.received < self.end:
                    raise ValueError("Outside regular session")
                previous = self.quotes.get(symbol)
                if previous and quote.source <= previous.source:
                    raise ValueError("Duplicate or older book")
            except (ValueError, TypeError, OverflowError, KeyError) as error:
                reason = (
                    str(error)
                    if type(error) is ValueError
                    and str(error)
                    in {
                        "Non-USD book",
                        "Stale or future book",
                        "Invalid book price or size",
                        "Crossed book",
                        "Outside regular session",
                        "Duplicate or older book",
                    }
                    else "Malformed or empty book"
                )
                public = {"status": "rejected", "reason": reason}
                self.errors[reason] += 1
            else:
                self.quotes[symbol] = quote
                self.stats["accepted_books"] += 1
                public = {"status": "accepted"}
                for name, account in self.accounts.items():
                    outputs[name].extend(account.observe_book(symbol, quote, self.quotes))
        elif kind == "trades":
            received = payload["received"]
            for row in payload["rows"]:
                try:
                    symbol, price = row["symbol"], float(row["lastPrice"])
                    source_at = epoch(row["timestamp"])
                    if not (
                        row["currency"] == "USD"
                        and math.isfinite(price)
                        and price > 0
                        and self.start <= source_at <= received <= at < self.end
                        and 0 <= at - source_at <= FEES["max_quote_age_seconds"]
                        and source_at > self.trade_times.get(symbol, 0)
                    ):
                        raise ValueError("Unusable last trade")
                except (TypeError, ValueError, KeyError, OverflowError):
                    self.errors["unusable_trade_observation"] += 1
                    continue
                self.trade_times[symbol] = source_at
                self.stats["accepted_trades"] += 1
                for name, account in self.accounts.items():
                    action = account.observe_trade(symbol, price, source_at, at, self.quotes)
                    if action is not None:
                        outputs[name].append(action)
        elif kind in {"clock", "close"} and at >= self.start:
            for name, account in self.accounts.items():
                outputs[name].extend(account.clock(at, self.quotes, close=kind == "close"))
        elif kind in {"market_data_error", "research_unavailable"}:
            self.errors[payload["code"]] += 1
            symbol = payload.get("symbol")
            if symbol and payload.get("http_status") in (400, 404, 422):
                for name, account in self.accounts.items():
                    if account.candidates.pop(symbol, None):
                        outputs[name].append(
                            {
                                "kind": "entry_decision",
                                "symbol": symbol,
                                "status": "blocked",
                                "reason": "toss_rejected_provider_symbol_or_its_book_request",
                            }
                        )
        for name, actions in outputs.items():
            for action in actions:
                self.accounts[name].counts[action["kind"] + ":" + action["status"]] += 1
        return {"public": public, "accounts": outputs}

    def _scan(
        self,
        payload: dict[str, Any],
        at: float,
        outputs: dict[str, list[dict[str, Any]]],
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        public: dict[str, Any] = {"new_signals": []}
        for name, count in payload["counts"].items():
            self.stats[f"scanner_{name}"] += count
        self.stats["scanner_spark_failures"] += len(payload["spark_failures"])
        self.stats["scanner_confirmation_failures"] += len(payload["confirmation_failures"])
        entries = self.source.evaluate_scan(payload, self.plan["symbols"], self.day, self.config)
        for hit, entry, trigger in entries:
            symbol = entry["symbol"]
            if symbol in self.seen:
                continue
            self.seen.add(symbol)
            self.signal_order.append(symbol)
            self.stats["native_entry_signals"] += 1
            rated = symbol in self.research.at(trigger, "US")
            expires = min(self.entry_end, trigger + STEP)
            native_plan, unavailable = {}, None
            try:
                native_plan = self.source.plan(entry, at, self.config_file)
            except self.source.exit.PlanError as error:
                unavailable = str(error)
            approved_window = not self.source.risk.window_check(
                self.source.risk.to_eastern(utc(at)), self.config
            )[0]
            public["new_signals"].append(
                {
                    "symbol": symbol,
                    "trigger": iso(trigger),
                    "scanner": hit,
                    "entry": entry,
                    "plan": native_plan or None,
                    "plan_error": unavailable,
                    "research_admitted": rated,
                }
            )
            for name, account in self.accounts.items():
                action = {"kind": "signal", "symbol": symbol, "status": "awaiting_book"}
                if self.live_since is None or trigger < self.live_since:
                    action.update(status="skipped", reason="trigger_preceded_live_capture")
                elif trigger != payload["cutoff"] or at >= expires:
                    action.update(status="skipped", reason="source_trigger_older_than_current_bar")
                elif unavailable or not approved_window:
                    action.update(status="skipped", reason="native_plan_or_window_unavailable")
                elif name == PROFILES[1] and not rated:
                    action.update(status="skipped", reason="no_favorable_publication_at_trigger")
                    self.stats["research_gate_declined"] += 1
                elif account.entry_limit_reached():
                    action.update(status="skipped", reason="native_daily_entry_limit_reached")
                else:
                    account.candidates[symbol] = Signal(at, trigger, native_plan, expires)
                outputs[name].append(action)
        return public, outputs

    def complete_decisions(self, outputs: dict[str, Any], completed_at: float) -> None:
        if completed_at < self.at:
            raise ValueError("Decision completion predates its observation")
        self.at = completed_at
        for name, actions in outputs["accounts"].items():
            account = self.accounts[name]
            for action in actions:
                if action["status"] != "approved":
                    continue
                pending = account.buys if action["kind"] == "entry_decision" else account.sells
                symbol = action["symbol"]
                pending[symbol] = replace(pending[symbol], at=completed_at)
                action["intent_at"] = iso(completed_at)

    def summary(self) -> dict[str, Any]:
        return {
            "observed_through": iso(self.at),
            "counts": dict(self.stats),
            "incidents": dict(self.errors),
            "accounts": {
                name: account.summary(self.quotes, self.at)
                for name, account in self.accounts.items()
            },
        }
