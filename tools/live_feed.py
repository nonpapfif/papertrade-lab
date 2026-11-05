"""Read-only live snapshots and explicitly sampled price-history features."""

from __future__ import annotations

import math
import threading
from collections import deque
from collections.abc import Callable
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from queue import SimpleQueue
from typing import Any

from papertrade.api import SYMBOL, ApiError, MarketClient
from papertrade.core import SafetyError, Settings, stamp
from tools.paper_market import US, PaperMarket

Record = Callable[[str, dict[str, Any]], None]
LOOKBACKS = (5, 10, 15, 30, 60)
MAX_AGE_SECONDS = 90
LISTING_EXCHANGES = {
    "US": ("NYSE", "NASDAQ", "AMEX", "US_ETC"),
    "KR": ("KOSPI", "KOSDAQ", "KR_ETC"),
}


class _PacedClient(MarketClient):
    """Keep transport/token state private while sharing one provider rate controller."""

    def __init__(self, settings: Settings, primary: MarketClient, lock: Any, audit: Any) -> None:
        super().__init__(settings, audit=audit)
        self.primary, self.rate_lock = primary, lock

    def _reserve(self, group: str) -> None:
        with self.rate_lock:
            self.primary._reserve(group)

    def _rate_result(self, group: str, headers: Any, failed: bool) -> float:
        with self.rate_lock:
            return self.primary._rate_result(group, headers, failed)


class LiveFeed:
    """Use only current market data; rate limits remain owned by MarketClient."""

    def __init__(
        self,
        settings: Settings,
        record: Record,
        *,
        client: MarketClient | None = None,
        market: PaperMarket = US,
    ) -> None:
        self.record = record
        self.settings = settings
        self.rate_lock = threading.Lock()
        self.client = client or MarketClient(settings)
        self.client.audit = lambda event: self.record("http", event)
        self.symbols: list[str] | None = None
        self.market = market
        self.listing_exchanges: dict[str, set[str]] = {}

    def universe(self) -> list[str]:
        listings: dict[str, set[str]] = {}
        for market in LISTING_EXCHANGES[self.market.code]:
            rows = self.client.listed(market, status="ACTIVE")
            self.record("listings", {"market": market, "status": "ACTIVE", "rows": rows})
            for row in rows:
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
                    raise ApiError("Invalid listed stock symbol", code="invalid_response")
                listings.setdefault(symbol, set()).add(market)
        self.listing_exchanges = listings
        self.symbols = sorted(listings)
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

    def orderbook_batch(self, symbols: list[str]) -> dict[str, dict[str, Any] | ApiError]:
        symbols = list(dict.fromkeys(symbols))
        events: SimpleQueue[tuple[str, dict[str, Any]]] = SimpleQueue()
        results: dict[str, dict[str, Any] | ApiError] = {}
        stopped: ApiError | None = None

        def drain() -> None:
            while not events.empty():
                kind, payload = events.get()
                self.record(kind, payload)

        def fetch(symbol: str) -> dict[str, Any] | ApiError:
            client = _PacedClient(
                self.settings,
                self.client,
                self.rate_lock,
                lambda event: events.put(("http", event)),
            )
            try:
                book = client.book(symbol)
            except ApiError as error:
                return error
            received = datetime.now(UTC).isoformat()
            events.put(("orderbook", {"symbol": symbol, "result": book, "received_at": received}))
            return {**book, "received_at": received}

        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = {pool.submit(fetch, symbol): symbol for symbol in symbols}
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        result = future.result()
                    except CancelledError:
                        if stopped is None:
                            raise
                        result = ApiError(
                            "Quote batch stopped by provider limits",
                            code="batch_cancelled",
                            status=stopped.status,
                            retry_at=stopped.retry_at,
                        )
                    results[symbol] = result
                    drain()
                    if (
                        stopped is None
                        and isinstance(result, ApiError)
                        and (result.status in (401, 403, 429) or result.code == "cooldown")
                    ):
                        stopped = result
                        for pending in futures:
                            pending.cancel()
        finally:
            drain()
        return {symbol: results[symbol] for symbol in symbols}

    def fx(self) -> dict[str, Any]:
        result = self.client.get("/api/v1/exchange-rate", baseCurrency="USD", quoteCurrency="KRW")
        self.record("fx", {"base_currency": "USD", "quote_currency": "KRW", "result": result})
        return result  # type: ignore[no-any-return]

    def calendar(self, day: str) -> dict[str, Any]:
        day = date.fromisoformat(day).isoformat()
        result = self.client.calendar(self.market.code, day)
        self.record("calendar", {"market": self.market.code, "date": day, "result": result})
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
                    reason = "future_source_time"
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
