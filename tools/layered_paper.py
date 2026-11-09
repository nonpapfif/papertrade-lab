"""Independent per-symbol signals and a single cash-funded virtual execution account."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from itertools import pairwise
from typing import Any

from tools.live_execution import DEFAULT_FEES, KR_FEES, ExecutionBook, ExecutionContext
from tools.paper_market import US, PaperMarket

FAMILIES = ("trend", "breakout", "rebound")
REBOUND_NET_TARGET = "rebound_net_target"


def eligible_stock(row: dict[str, Any]) -> bool:
    return (
        row.get("status") == "ACTIVE"
        and row.get("currency") == "USD"
        and row.get("securityType") == "STOCK"
        and row.get("isCommonShare") is True
        and row.get("leverageFactor") in (None, 1)
    )


@dataclass(frozen=True)
class Feature:
    symbol: int
    price: float
    available_at: str
    bar_end: str
    r5: float
    r15: float
    prior_high: float
    mean: float
    efficiency: float
    observations: int = 0
    missing_minutes: int = 0
    max_gap_seconds: float = 0
    reference5_age_seconds: float = 0
    reference15_age_seconds: float = 0


def feature_from_bars(
    symbol: int, rows: list[dict[str, Any]], at: str, *, timestamp_is_end: bool = False
) -> Feature | None:
    """Use as-of time references without manufacturing missing candles or volume."""
    decision = datetime.fromisoformat(at)
    if decision.tzinfo is None:
        raise ValueError("Decision timestamp requires a timezone")
    cutoff = decision.timestamp()
    end_offset = 0 if timestamp_is_end else 60
    usable: dict[float, tuple[float, dict[str, Any]]] = {}
    for row in rows:
        source = datetime.fromisoformat(row["timestamp"])
        available = datetime.fromisoformat(row["available_at"])
        if source.tzinfo is None or available.tzinfo is None:
            raise ValueError("Bar timestamps require timezones")
        stamp = source.timestamp()
        received = available.timestamp()
        if received <= cutoff and stamp + end_offset <= cutoff:
            old = usable.get(stamp)
            if old is None or old[0] < received:
                usable[stamp] = (received, row)
    stamps = sorted(usable)
    if not stamps:
        return None
    latest = stamps[-1]
    if cutoff - latest - end_offset > 90:
        return None

    def reference(seconds: int) -> float | None:
        target = latest - seconds
        found = next((t for t in reversed(stamps) if t <= target), None)
        return found if found is not None and target - found <= 90 else None

    reference5, reference15 = reference(300), reference(900)
    if reference5 is None or reference15 is None:
        return None
    window = [t for t in stamps if reference15 <= t <= latest]
    bars = [usable[t][1] for t in window]
    prices = [float(b["closePrice"]) for b in bars]
    highs = [float(b["highPrice"]) for b in bars]
    if any(not math.isfinite(p) or p <= 0 for p in (*prices, *highs)):
        return None
    price5 = float(usable[reference5][1]["closePrice"])
    if not math.isfinite(price5) or price5 <= 0:
        return None
    previous = [float(usable[t][1]["closePrice"]) for t in window if latest - 900 <= t < latest]
    if not previous:
        return None
    path = sum(abs(b - a) for a, b in pairwise(prices))
    return Feature(
        symbol,
        prices[-1],
        max((r["available_at"] for r in bars), key=lambda t: datetime.fromisoformat(t)),
        datetime.fromtimestamp(latest + end_offset, tz=decision.tzinfo).isoformat(),
        prices[-1] / price5 - 1,
        prices[-1] / prices[0] - 1,
        max(float(usable[t][1]["highPrice"]) for t in window if latest - 900 <= t < latest),
        sum(previous) / len(previous),
        abs(prices[-1] - prices[0]) / path if path else 0,
        observations=len(window),
        missing_minutes=max(0, int((latest - reference15) // 60) + 1 - len(window)),
        max_gap_seconds=max((b - a for a, b in pairwise(window)), default=0),
        reference5_age_seconds=latest - 300 - reference5,
        reference15_age_seconds=latest - 900 - reference15,
    )


def entry_valid(strategy: str, f: Feature) -> bool:
    if strategy == "breakout":
        return f.r5 > 0 and f.price > f.prior_high
    if strategy == "trend":
        return f.r5 > 0 and f.r15 > 0 and f.efficiency >= 0.5
    if strategy in ("rebound", REBOUND_NET_TARGET):
        return f.r5 > 0 and f.r15 < 0 and f.price < f.mean
    raise ValueError("Unknown strategy")


def assign(f: Feature, policy: str = "router") -> str | None:
    families = ("breakout", "trend", "rebound") if policy == "router" else (policy,)
    return next((s for s in families if entry_valid(s, f)), None)


def holding_valid(strategy: str, f: Feature, entry: Feature) -> bool:
    if strategy == "breakout":
        return f.price >= entry.prior_high * 0.998
    if strategy == "trend":
        return f.r15 >= 0
    if strategy in ("rebound", REBOUND_NET_TARGET):
        return f.r5 >= -0.003 and f.price < entry.mean
    raise ValueError("Unknown strategy")


class LayeredPaper:
    """Prototype rule boundaries are hypotheses, not validated return forecasts."""

    entry_policies: tuple[str, ...] = FAMILIES

    def __init__(
        self,
        capital: float,
        *,
        policy: str = "router",
        selection: str = "all",
        allocation: str = "equal",
        fees: dict[str, float] | None = None,
        market: PaperMarket = US,
    ) -> None:
        if policy not in ("router", *self.entry_policies) or selection not in ("all", "active"):
            raise ValueError("Unknown signal policy")
        if allocation not in ("equal", "priority") or not math.isfinite(capital) or capital <= 0:
            raise ValueError("Invalid allocation or capital")
        self.policy, self.selection, self.allocation = policy, selection, allocation
        schedule = {
            **(KR_FEES if market.code == "KR" else DEFAULT_FEES),
            "allow_partial_exits": 1.0,
            "confirmation_delay_seconds": 1.0,
            **(fees or {}),
        }
        self.context = ExecutionContext("quotes", schedule, market=market)
        config = {
            "slots": 1,
            "leverage": 1,
            "hold": 0,
            "target": 0.0,
            "stop": 0.0,
            "trail": 0.0,
            "sizing": "equal",
            "compound": True,
            "session": "all",
            "rotate": False,
            "cooldown": 0,
            "signal": 0,
            "id": 0,
        }
        self.book = ExecutionBook(
            config, market.code, "layered", True, context=self.context, initial_equity=capital
        )
        self.owners: dict[int, tuple[str, Feature]] = {}
        self.realized_by_strategy = dict.fromkeys(FAMILIES, 0.0)
        self.seen: set[str] = set()
        self.decisions: list[dict[str, Any]] = []
        self.prices: dict[int, float] = {}
        self.times: dict[int, str] = {}
        self.last_at: str | None = None

    def step(
        self,
        event_id: str,
        frame: dict[str, Any],
        features: dict[int, Feature],
        eligible: set[int],
        *,
        entries: bool = True,
    ) -> None:
        if event_id in self.seen:
            return
        at = frame["at"]
        if self.last_at is not None and datetime.fromisoformat(at) < datetime.fromisoformat(
            self.last_at
        ):
            raise ValueError("Cannot rewind an account")
        cutoff = datetime.fromisoformat(frame.get("decision_at", at))
        fresh = {
            s: f
            for s, f in features.items()
            if datetime.fromisoformat(f.available_at) <= cutoff
            and 0 <= (cutoff - datetime.fromisoformat(f.bar_end)).total_seconds() <= 90
        }
        self.context.frame = frame
        self.prices.update(frame["quotes"])
        self.times.update({s: frame.get("source_times", {}).get(s, at) for s in frame["quotes"]})
        held_before = set(self.book.positions)
        for symbol in sorted(held_before):
            if symbol not in fresh:
                self.decisions.append({"at": at, "symbol": symbol, "action": "unknown"})
                continue
            owner, original = self.owners[symbol]
            if not holding_valid(owner, fresh[symbol], original):
                before = self.book.realized
                self.book.fill(
                    symbol,
                    -self.book.positions[symbol].quantity,
                    self.prices[symbol],
                    at,
                    frame["tick"],
                    "invalidated",
                )
                self.realized_by_strategy[owner] += self.book.realized - before
                if symbol not in self.book.positions:
                    del self.owners[symbol]
        candidates = []
        if entries:
            for symbol, f in sorted(fresh.items()):
                if symbol in held_before or symbol not in eligible:
                    continue
                if self.selection == "active" and abs(f.r5) < 0.001 and abs(f.r15) < 0.003:
                    continue
                strategy = assign(f, self.policy)
                if strategy:
                    candidates.append((symbol, strategy, f))
        candidates.sort(key=lambda row: (-abs(row[2].r5), row[0]))
        budget = self.book.cash / len(candidates) if candidates else 0
        for symbol, strategy, f in candidates:
            amount = budget if self.allocation == "equal" else self.book.cash
            orders = self.book.whole_orders([[symbol, 1, 1.0]], self.prices, 1, amount, amount)
            quantity = orders[0][1] if orders else 0
            self.decisions.append(
                {
                    "at": at,
                    "symbol": symbol,
                    "strategy": strategy,
                    "action": "buy",
                    "quantity": quantity,
                }
            )
            if quantity:
                before = self.book.realized
                self.book.fill(symbol, quantity, self.prices[symbol], at, frame["tick"], "entry")
                self.realized_by_strategy[strategy] += self.book.realized - before
                if symbol in self.book.positions:
                    self.owners[symbol] = (strategy, f)
        self.book.mark(self.prices)
        if self.book.cash < -1e-9 or any(
            p.quantity <= 0 or not float(p.quantity).is_integer()
            for p in self.book.positions.values()
        ):
            raise RuntimeError("Unfunded or non-integer position")
        self.seen.add(event_id)
        self.last_at = at

    def summary(self, at: str | None = None) -> dict[str, Any]:
        at = self.last_at if at is None else at
        if at is None:
            raise ValueError("No account event has been processed")
        cutoff = datetime.fromisoformat(at)
        mark_times: dict[int, str] = {}
        mark_ages: dict[int, float] = {}
        for symbol in self.book.positions:
            stamp = self.times.get(symbol)
            if not isinstance(stamp, str):
                continue
            try:
                mark_ages[symbol] = (cutoff - datetime.fromisoformat(stamp)).total_seconds()
            except (ValueError, TypeError):
                continue
            mark_times[symbol] = stamp
        account = self.book.summary(
            self.prices, {symbol: mark_times.get(symbol, "") for symbol in self.book.positions}, at
        )
        if mark_ages:
            account["oldest_open_quote_at"] = mark_times[max(mark_ages, key=mark_ages.__getitem__)]
        else:
            account["oldest_open_quote_at"] = None
        suffix = self.book.currency_suffix
        return {
            "account": account,
            "realized_by_strategy": self.realized_by_strategy,
            "owned_positions": {str(s): owner[0] for s, owner in self.owners.items()},
            "positions_as_of": at,
            "positions": {
                str(symbol): {
                    "strategy": self.owners[symbol][0],
                    "quantity": position.quantity,
                    "entry_price": position.entry,
                    f"entry_cost_{suffix}": position.quantity * position.entry,
                    f"remaining_entry_fees_{suffix}": self.book.entry_fees[symbol],
                    "mark_price": self.prices[symbol],
                    "mark_at": mark_times.get(symbol),
                    "mark_age_seconds": mark_ages.get(symbol),
                    f"market_value_{suffix}": position.quantity * self.prices[symbol],
                    f"unrealized_{suffix}": position.quantity
                    * (self.prices[symbol] - position.entry),
                }
                for symbol, position in sorted(self.book.positions.items())
            },
            "decisions": len(self.decisions),
        }
