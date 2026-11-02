"""Read-only live snapshots and explicitly sampled price-history features."""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Callable
from datetime import date, datetime, timedelta
from itertools import pairwise
from typing import Any

from papertrade.api import SYMBOL, ApiError, MarketClient
from papertrade.core import SafetyError, Settings, stamp

Record = Callable[[str, dict[str, Any]], None]
LOOKBACKS = (5, 10, 15, 30, 60)
MAX_AGE_SECONDS = 90


class LiveFeed:
    """Use only current market data; rate limits remain owned by MarketClient."""

    def __init__(
        self, settings: Settings, record: Record, *, client: MarketClient | None = None
    ) -> None:
        self.record = record
        self.client = client or MarketClient(settings)
        self.client.audit = lambda event: self.record("http", event)
        self.symbols: list[str] | None = None

    def universe(self) -> list[str]:
        symbols: set[str] = set()
        for market in ("NYSE", "NASDAQ", "AMEX", "US_ETC"):
            rows = self.client.listed(market, status="ACTIVE")
            self.record("listings", {"market": market, "status": "ACTIVE", "rows": rows})
            for row in rows:
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
                    raise ApiError("Invalid listed stock symbol", code="invalid_response")
                symbols.add(symbol)
        self.symbols = sorted(symbols)
        return list(self.symbols)

    def snapshot(self) -> list[dict[str, Any]]:
        if self.symbols is None:
            self.universe()
        symbols = self.symbols or []
        result = []
        for offset in range(0, len(symbols), 200):
            batch = symbols[offset : offset + 200]
            rows = self.client.prices(batch)
            received = [row.get("symbol") for row in rows]
            self.record(
                "prices",
                {
                    "symbols": batch,
                    "rows": rows,
                    "missing_symbols": sorted(
                        set(batch) - {s for s in received if isinstance(s, str)}
                    ),
                },
            )
            if any(not isinstance(s, str) or s not in batch for s in received) or len(
                set(received)
            ) != len(received):
                raise ApiError(
                    "Price response does not match its symbol batch", code="invalid_response"
                )
            result.extend(rows)
        return result

    def orderbooks(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        result = {}
        for symbol in dict.fromkeys(symbols):
            book = self.client.book(symbol)
            self.record("orderbook", {"symbol": symbol, "result": book})
            result[symbol] = book
        return result

    def fx(self) -> dict[str, Any]:
        result = self.client.get("/api/v1/exchange-rate", baseCurrency="USD", quoteCurrency="KRW")
        self.record("fx", {"base_currency": "USD", "quote_currency": "KRW", "result": result})
        return result  # type: ignore[no-any-return]

    def calendar(self, day: str) -> dict[str, Any]:
        day = date.fromisoformat(day).isoformat()
        result = self.client.calendar("US", day)
        self.record("calendar", {"market": "US", "date": day, "result": result})
        return result


class SnapshotHistory:
    """Store fresh observed snapshots without treating them as minute OHLCV bars."""

    def __init__(self, record: Record | None = None) -> None:
        self.record = record
        self.rows: dict[str, deque[dict[str, Any]]] = {}
        self.last_at: datetime | None = None

    def add(self, at: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        observed = stamp(at)
        if self.last_at is not None and observed <= self.last_at:
            raise ValueError("Snapshot observation times must strictly increase")
        pending: dict[str, dict[str, Any]] = {}
        issues = []
        for row in rows:
            reason = None
            symbol = row.get("symbol")
            try:
                source = stamp(row["timestamp"])
                price = float(row["lastPrice"])
            except (KeyError, TypeError, ValueError, SafetyError):
                reason = "invalid_quote_fields"
            else:
                if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
                    reason = "invalid_symbol"
                elif row.get("currency") != "USD":
                    reason = "non_usd_quote"
                elif not math.isfinite(price) or price <= 0:
                    reason = "invalid_price"
                elif source > observed:
                    event = {
                        "observed_at": observed.isoformat(),
                        "reason": "future_source_time",
                        "row": row,
                    }
                    if self.record is not None:
                        self.record("invalid_quote", event)
                    raise ValueError("Provider quote timestamp exceeds its observation cutoff")
                elif (observed - source).total_seconds() > MAX_AGE_SECONDS:
                    reason = "stale_source_time"
            if reason is None and isinstance(symbol, str):
                if symbol in pending:
                    raise ValueError("Snapshot contains duplicate symbols")
                history = self.rows.get(symbol)
                if history and source < history[-1]["source"]:
                    reason = "source_time_regressed"
                elif history and source == history[-1]["source"] and price != history[-1]["price"]:
                    reason = "conflicting_source_price"
                else:
                    pending[symbol] = {"at": observed, "source": source, "price": price}
            if reason is not None:
                issue = {"observed_at": observed.isoformat(), "reason": reason, "row": row}
                issues.append(issue)
                if self.record is not None:
                    self.record("invalid_quote", issue)
        oldest = observed - timedelta(minutes=65)
        for symbol, row in pending.items():
            history = self.rows.setdefault(symbol, deque())
            history.append(row)
            while len(history) > 1 and history[1]["at"] <= oldest:
                history.popleft()
        self.last_at = observed
        return {
            "observed_at": observed.isoformat(),
            "accepted": len(pending),
            "rejected": len(issues),
            "issues": issues,
        }

    @staticmethod
    def _reference(rows: list[dict[str, Any]], cutoff: datetime) -> dict[str, Any] | None:
        result = next((row for row in reversed(rows) if row["at"] <= cutoff), None)
        if result is None or (cutoff - result["at"]).total_seconds() > MAX_AGE_SECONDS:
            return None
        return result

    @staticmethod
    def _continuous(rows: list[dict[str, Any]], end: datetime) -> bool:
        return (
            bool(rows)
            and (end - rows[-1]["at"]).total_seconds() <= MAX_AGE_SECONDS
            and all(
                0 < (right["at"] - left["at"]).total_seconds() <= MAX_AGE_SECONDS
                for left, right in pairwise(rows)
            )
        )

    def features(self, at: str) -> list[dict[str, Any]]:
        observed = stamp(at)
        if self.last_at is not None and observed < self.last_at:
            raise ValueError("Snapshot features cannot rewind the observation clock")
        result = []
        five, ten = observed - timedelta(minutes=5), observed - timedelta(minutes=10)
        for symbol, history in sorted(self.rows.items()):
            rows = list(history)
            if not rows or rows[-1]["at"] != observed:
                continue
            last = rows[-1]
            price = last["price"]
            reference5 = self._reference(rows, five)
            previous10 = self._reference(rows, ten)
            r5 = price / reference5["price"] - 1 if reference5 else None
            scores: list[float | None] = [None] * 30
            noises: list[float | None] = []
            reference_times: list[str | None] = []
            for index, lookback in enumerate(LOOKBACKS):
                cutoff, prior_start = (
                    observed - timedelta(minutes=lookback),
                    five - timedelta(minutes=lookback),
                )
                reference = self._reference(rows, cutoff)
                reference_times.append(reference["at"].isoformat() if reference else None)
                current = [row for row in rows if cutoff < row["at"] <= observed]
                prior = [row for row in rows if prior_start < row["at"] <= five]
                before_prior = self._reference(rows, prior_start)
                complete_prior = before_prior is not None and self._continuous(
                    [before_prior, *prior], five
                )
                moves = (
                    [
                        abs(right["price"] / left["price"] - 1)
                        / ((right["at"] - left["at"]).total_seconds() / 60)
                        for left, right in pairwise(prior)
                    ]
                    if complete_prior
                    else []
                )
                noise = sum(moves) / len(moves) if moves else None
                noises.append(
                    abs(r5) / (max(noise, 1e-12) * math.sqrt(5))
                    if r5 is not None and noise is not None
                    else None
                )
                if reference is None or not self._continuous([reference, *current], observed):
                    continue
                change = price / reference["price"] - 1
                scaled = change * 5 / lookback
                mean = sum(row["price"] for row in current) / len(current) if current else price
                transition = None
                if r5 is not None:
                    if lookback > 5:
                        transition = r5 - (change - r5) / (lookback / 5 - 1)
                    elif previous10 and reference5:
                        transition = r5 - (reference5["price"] / previous10["price"] - 1)
                family_scores = (
                    scaled,
                    -scaled,
                    price / max(row["price"] for row in prior) - 1
                    if prior and complete_prior
                    else None,
                    (mean - price) / price * 5 / lookback,
                    transition,
                    None,
                )
                for family, value in enumerate(family_scores):
                    scores[family * 5 + index] = value
            result.append(
                {
                    "market": "US",
                    "symbol": symbol,
                    "price": price,
                    "quote_at": observed.isoformat(),
                    "source_at": last["source"].isoformat(),
                    "price_age_seconds": (observed - last["source"]).total_seconds(),
                    "reference_times": reference_times,
                    "observations": sum(row["at"] > five for row in rows),
                    "scores": scores,
                    "volumes": [None] * 5,
                    "noises": noises,
                    "feature_basis": "sampled_60s_prices_not_ohlcv",
                    "source_times": [
                        [row["at"].isoformat(), row["source"].isoformat()] for row in rows
                    ],
                }
            )
        return result
