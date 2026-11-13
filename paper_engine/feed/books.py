"""Shared orderbook intake and request ownership on one asyncio event loop."""

import asyncio
import heapq
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from ..contracts import Quote, require_ids, require_time
from ..execution import book_issue


@dataclass(frozen=True, slots=True)
class Rejected:
    code: str
    retry_at: int | None = None


class BookProvider(Protocol):
    async def book(self, market: str, symbol: str) -> Quote | Rejected | None: ...


BookResult = Quote | Rejected | None
BookKey = tuple[str, str]


class Books:
    """Cache and publish each usable observation once, independently of request waiters."""

    def __init__(
        self, provider: BookProvider, clock: Callable[[], int], max_age: Mapping[str, int]
    ) -> None:
        if not max_age or not max_age.keys() <= {"KR", "US"}:
            raise ValueError("Orderbook intake requires configured paper markets")
        for age in max_age.values():
            require_time(age)
        self._provider = provider
        self._clock = clock
        self._max_age = dict(max_age)
        self._latest: dict[BookKey, Quote] = {}
        self._seen: dict[tuple[str, str], Quote] = {}
        self._expiry: dict[str, list[tuple[int, str]]] = {market: [] for market in max_age}
        self._ready: dict[str, deque[Quote]] = {market: deque() for market in max_age}
        self._available: dict[str, int] = dict.fromkeys(max_age, -1)
        self._pending: dict[BookKey, bool] = {}
        self._inflight: dict[BookKey, asyncio.Future[BookResult | Exception]] = {}
        self._worker: asyncio.Task[None] | None = None

    def latest(self, market: str, symbol: str, max_age: int) -> Quote | None:
        require_time(max_age)
        now = self._clock()
        quote = self._latest.get((market, symbol))
        if quote is not None and (
            quote.available_at <= now
            and 0 <= now - quote.observed_at <= min(max_age, self._max_age[market])
        ):
            return quote
        return None

    def observe(self, market: str, quote: Quote) -> Quote | Rejected:
        now = self._clock()
        if quote.available_at > now:
            return Rejected("future_quote")
        if now - quote.observed_at > self._max_age[market]:
            return Rejected("stale_quote")
        issue = book_issue(quote)
        if issue:
            return Rejected(issue)
        expiry = self._expiry[market]
        while expiry and expiry[0][0] < now:
            _, quote_id = heapq.heappop(expiry)
            self._seen.pop((market, quote_id))
        key = (market, quote.symbol)
        latest = self._latest.get(key)
        if latest is not None and quote.observed_at < latest.observed_at:
            return Rejected("older_quote")
        seen = self._seen.get((market, quote.quote_id))
        if seen is not None:
            if not same_observation(seen, quote):
                return Rejected("conflicting_quote_id")
            return seen
        if quote.available_at < self._available[market]:
            return Rejected("out_of_order_receipt")
        self._available[market] = quote.available_at
        self._latest[key] = quote
        self._seen[(market, quote.quote_id)] = quote
        heapq.heappush(expiry, (quote.observed_at + self._max_age[market], quote.quote_id))
        self._ready[market].append(quote)
        return quote

    def next_quote(self, market: str) -> Quote | None:
        queue = self._ready[market]
        return queue[0] if queue else None

    def acknowledge(self, market: str, quote: Quote) -> None:
        if self.next_quote(market) is not quote:
            raise ValueError("Orderbooks must be acknowledged in arrival order")
        self._ready[market].popleft()

    async def request(
        self, market: str, symbol: str, *, max_age: int, protected: bool = False
    ) -> BookResult:
        require_ids(symbol)
        if market not in self._max_age:
            raise ValueError("Unconfigured market")
        cached = self.latest(market, symbol, max_age)
        if cached is not None:
            return cached

        key = (market, symbol)
        if key not in self._inflight:
            self._inflight[key] = asyncio.get_running_loop().create_future()
            self._pending[key] = protected
        elif key in self._pending and protected:
            self._pending[key] = True
        if self._worker is None:
            self._worker = asyncio.create_task(self._drain())

        result = await asyncio.shield(self._inflight[key])
        if isinstance(result, Exception):
            raise result
        if isinstance(result, Quote):
            return self.latest(market, symbol, max_age) or Rejected("stale_quote")
        return self.latest(market, symbol, max_age) or result

    async def _drain(self) -> None:
        try:
            while self._pending:
                key = max(self._pending, key=self._pending.__getitem__)
                self._pending.pop(key)
                market, symbol = key
                future = self._inflight[key]
                try:
                    quote = await self._provider.book(market, symbol)
                    if isinstance(quote, Quote):
                        quote = (
                            self.observe(market, quote)
                            if quote.symbol == symbol
                            else Rejected("symbol_mismatch")
                        )
                    future.set_result(quote)
                except asyncio.CancelledError:
                    future.cancel()
                    raise
                except Exception as error:  # noqa: BLE001 - propagate to the waiting callers
                    future.set_result(error)
                finally:
                    self._inflight.pop(key)
        finally:
            for future in self._inflight.values():
                future.cancel()
            self._inflight.clear()
            self._pending.clear()
            self._worker = None


def same_observation(first: Quote, second: Quote) -> bool:
    """Compare provider identity and content; repeated delivery can occur later."""
    return (
        first.quote_id == second.quote_id
        and first.symbol == second.symbol
        and first.source == second.source
        and first.observed_at == second.observed_at
        and first.bids == second.bids
        and first.asks == second.asks
    )
