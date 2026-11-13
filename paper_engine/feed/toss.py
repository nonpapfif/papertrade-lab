"""Normalize Toss orderbooks and validate subscription generations."""

import asyncio
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from papertrade.api import SYMBOL, ApiError, MarketClient

from ..contracts import Level, Quote
from .books import BookResult, Books, Rejected

_CURRENCIES = {"KR": "KRW", "US": "USD"}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_KR_SYMBOL = re.compile(r"[A-Za-z0-9]{6}")


def _number(raw: object) -> Decimal:
    if type(raw) not in (int, float, str, Decimal):
        raise ValueError("Invalid book number")
    return Decimal(str(raw))


def _levels(raw: object) -> tuple[Level, ...]:
    if not isinstance(raw, list):
        raise TypeError("Missing book side")
    levels = []
    for row in raw:
        if not isinstance(row, dict):
            raise TypeError("Invalid depth level")
        price, size = _number(row["price"]), _number(row["volume"])
        if size == 0 and price == 0:
            continue
        levels.append(Level(price, size))
    return tuple(sorted(levels, key=lambda level: level.price))


def _exact_number(value: Decimal) -> str:
    sign, digits, exponent = value.as_tuple()
    if not value:
        return "0"
    significant = "".join(map(str, digits)).rstrip("0")
    return f"{sign}:{significant}:{int(exponent) + len(digits) - len(significant)}"


def normalize_book(market: str, symbol: str, raw: object, at: int) -> Quote | Rejected:
    try:
        if not isinstance(raw, dict) or raw["currency"] != _CURRENCIES[market]:
            raise ValueError("Invalid book currency")
        if raw.get("symbol", symbol) != symbol:
            raise ValueError("Book symbol mismatch")
        stamp = datetime.fromisoformat(raw["timestamp"])
        if stamp.utcoffset() is None:
            raise ValueError("Book timestamp has no offset")
        elapsed = stamp - _EPOCH
        observed_at = elapsed // datetime.resolution // 1000
        if observed_at > at:
            return Rejected("future_quote")
        bids, asks = _levels(raw["bids"]), _levels(raw["asks"])
        identity = (
            market,
            symbol,
            elapsed // datetime.resolution,
            [
                [(_exact_number(level.price), _exact_number(level.quantity)) for level in side]
                for side in (bids, asks)
            ],
        )
        digest = hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()
        return Quote(f"toss-book:{digest}", symbol, "toss:orderbook", observed_at, at, bids, asks)
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError):
        return Rejected("invalid_orderbook")


class TossBooks:
    """Own one allowlisted client for the shared MARKET_DATA group."""

    def __init__(self, client: MarketClient, clock: Callable[[], int]) -> None:
        if client.store is not None:
            raise ValueError("The threaded Toss book client requires no SQLite store")
        self._client = client
        self._clock = clock
        self._lock = asyncio.Lock()
        self._next_request = 0.0
        self._retry_at = 0

    async def book(self, market: str, symbol: str) -> BookResult:
        if not _valid_symbol(market, symbol):
            return Rejected("invalid_symbol")
        async with self._lock:
            if self._clock() < self._retry_at:
                return Rejected("cooldown", self._retry_at)
            loop = asyncio.get_running_loop()
            await asyncio.sleep(max(0.0, self._next_request - loop.time()))
            self._next_request = loop.time() + 1 / 15
            try:
                request = asyncio.create_task(asyncio.to_thread(self._client.book, symbol))
                try:
                    raw = await asyncio.shield(request)
                except asyncio.CancelledError:
                    try:
                        await request
                    except ApiError:
                        pass
                    raise
            except ApiError as error:
                retry_at = None if error.retry_at is None else int(error.retry_at * 1000)
                if retry_at is not None:
                    self._retry_at = max(self._retry_at, retry_at)
                return Rejected(error.code, retry_at)
            return normalize_book(market, symbol, raw, self._clock())


def _valid_symbol(market: str, symbol: str) -> bool:
    return market in _CURRENCIES and bool(
        (_KR_SYMBOL if market == "KR" else SYMBOL).fullmatch(symbol)
    )


@dataclass(frozen=True, slots=True)
class Subscription:
    generation: int
    market: str
    symbols: tuple[str, ...]

    def wire(self) -> str:
        return json.dumps([{"type": f"orderbook:{self.market.lower()}", "codes": self.symbols}])


@dataclass(frozen=True, slots=True)
class StreamState:
    generation: int
    status: str


class TossStream:
    """Own one stream generation. The transport closes and reconnects on lost trust."""

    def __init__(self, market: str, books: Books) -> None:
        if market not in _CURRENCIES:
            raise ValueError("Unknown stream market")
        self._market = market
        self._books = books
        self._generation = 0
        self._topics: dict[str, str] = {}
        self._status = "disconnected"

    def begin(self, symbols: tuple[str, ...]) -> Subscription:
        if self._status != "disconnected":
            raise ValueError("Close the previous stream before starting a new generation")
        if (
            not 1 <= len(symbols) <= 100
            or len(set(symbols)) != len(symbols)
            or any(not _valid_symbol(self._market, symbol) for symbol in symbols)
        ):
            raise ValueError("Invalid Toss orderbook subscription")
        self._generation += 1
        self._topics = {f"orderbook:{self._market.lower()}:{symbol}": symbol for symbol in symbols}
        self._status = "pending_ack"
        return Subscription(self._generation, self._market, symbols)

    def disconnect(self, generation: int) -> StreamState:
        if generation == self._generation:
            self._status = "disconnected"
            self._topics.clear()
        return StreamState(self._generation, self._status)

    def receive(
        self, generation: int, frame: str | bytes, at: int
    ) -> Quote | Rejected | StreamState:
        if generation != self._generation or self._status == "disconnected":
            return Rejected("inactive_generation")
        if self._status == "resync_required":
            return Rejected("untrusted_generation")
        try:
            body = json.loads(frame, parse_float=Decimal)
        except (ValueError, InvalidOperation, RecursionError):
            return Rejected("invalid_json")
        if not isinstance(body, dict):
            return Rejected("invalid_frame")
        kind = body.get("type")
        if kind == "subscriptions":
            subscribed, rejected = body.get("subscribed"), body.get("rejected")
            if (
                self._status != "pending_ack"
                or not isinstance(subscribed, list)
                or any(type(topic) is not str for topic in subscribed)
                or len(subscribed) != len(self._topics)
                or set(subscribed) != self._topics.keys()
                or not isinstance(rejected, list)
                or rejected
            ):
                self._status = "resync_required"
            else:
                self._status = "trusted"
            return StreamState(generation, self._status)
        if kind == "error":
            error = body.get("error")
            if (
                not isinstance(error, dict)
                or not isinstance(error.get("code"), str)
                or not isinstance(error.get("message"), str)
            ):
                return Rejected("invalid_provider_error")
            self._status = "resync_required"
            return StreamState(generation, self._status)
        if kind == "pong":
            return StreamState(generation, self._status)
        if kind != "message":
            return Rejected("unknown_frame")
        if self._status != "trusted":
            return Rejected("untrusted_generation")
        topic = body.get("topic")
        if not isinstance(topic, str) or topic not in self._topics:
            return Rejected("unexpected_topic")
        quote = normalize_book(self._market, self._topics[topic], body.get("data"), at)
        if isinstance(quote, Rejected):
            return quote
        return self._books.observe(self._market, quote)
