"""Per-symbol asynchronous paper decisions with cash reserved before future quotes."""

from __future__ import annotations

import asyncio
import copy
import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from tools.layered_paper import Feature, LayeredPaper, assign, holding_valid
from tools.toss_order_contract import whole_share_limit


def seconds(at: str) -> float:
    stamp = datetime.fromisoformat(at)
    if stamp.tzinfo is None:
        raise ValueError("Timestamp requires timezone")
    return stamp.timestamp()


@dataclass(frozen=True)
class Intent:
    id: str
    symbol: int
    side: str
    quantity: int
    limit: float
    reserved: float
    decision_at: str
    strategy: str
    feature: Feature
    initial: dict[str, Any]
    policy_version: int = 0
    order_request: dict[str, str] | None = None


class AsyncLayeredPaper(LayeredPaper):
    """One queue per symbol; only immediate account mutations hold the shared lock.

    Each intent gets one eligible confirmation attempt, then releases unused cash.
    Missing confirmations expire after 30 seconds. Remaining holdings stay managed.
    Equal allocation uses a caller-supplied, decision-time candidate count.
    """

    def __init__(self, capital: float, *, delay: float = 1, **kwargs: Any) -> None:
        super().__init__(capital, **kwargs)
        if not math.isfinite(delay) or delay <= 0:
            raise ValueError("Positive confirmation delay required")
        self.entry_policy_version = 0
        self.owner_policy_versions: dict[int, int] = {}
        self.delay = delay
        self.context.schedule["confirmation_delay_seconds"] = delay
        self.pending: dict[int, Intent] = {}
        self.intents: list[Intent] = []
        self.outcomes: list[dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._turn = asyncio.Condition(self._lock)
        self._submitted = 0
        self._completed = 0
        self._last_enqueued = -math.inf
        self._queues: dict[int, asyncio.Queue] = {}
        self._workers: dict[int, asyncio.Task] = {}
        self._source_times: dict[int, float] = {}
        self._receipts: dict[int, float] = {}
        self._processed: set[str] = set()
        self._books: dict[int, dict[str, Any]] = {}
        self._fx: tuple[float, str, str | None, str | None, bool] = (
            0.0,
            "",
            None,
            None,
            False,
        )

    @property
    def reserved_cash(self) -> float:
        return sum(i.reserved for i in self.pending.values())

    @property
    def available_cash(self) -> float:
        return max(0.0, self.book.cash - self.reserved_cash)

    def enqueue(self, event: dict[str, Any]) -> asyncio.Future:
        """Copy inputs on submission so producers cannot change queued observations."""
        symbol = event["symbol"]
        feature = event.get("feature")
        if feature is not None:
            if not isinstance(feature, Feature):
                raise TypeError("Expected a Feature or None")
            if feature.symbol != symbol:
                raise ValueError("Feature does not belong to event symbol")
            values = (
                feature.price,
                feature.r5,
                feature.r15,
                feature.prior_high,
                feature.mean,
                feature.efficiency,
            )
            if not all(math.isfinite(value) for value in values):
                raise ValueError("Feature values must be finite")
        if event["id"] in self._processed:
            done = asyncio.get_running_loop().create_future()
            done.set_result(None)
            return done
        receipt = seconds(event["received_at"])
        if receipt < self._last_enqueued:
            raise ValueError("Dispatch events in receipt order")
        self._last_enqueued = receipt
        sequence = self._submitted
        self._submitted += 1
        if symbol not in self._queues:
            self._queues[symbol] = asyncio.Queue()
            self._workers[symbol] = asyncio.create_task(self._worker(symbol))
        done = asyncio.get_running_loop().create_future()
        self._queues[symbol].put_nowait((sequence, copy.deepcopy(event), done))
        return done

    async def submit(self, event: dict[str, Any]) -> None:
        await self.enqueue(event)

    async def _worker(self, symbol: int) -> None:
        queue = self._queues[symbol]
        while True:
            sequence, event, done = await queue.get()
            try:
                async with self._turn:
                    while sequence != self._completed:
                        await self._turn.wait()
                    try:
                        self._process(event)
                    finally:
                        self._completed += 1
                        self._turn.notify_all()
            except Exception as error:  # noqa: BLE001 - Forward every worker failure to its caller.
                if not done.done():
                    # Keep the failure traceback without exposing the suspended worker frame.
                    traceback = error.__traceback__
                    done.set_exception(
                        error.with_traceback(traceback.tb_next if traceback else None)
                    )
            else:
                if not done.done():
                    done.set_result(None)
            finally:
                queue.task_done()

    async def close(self) -> None:
        await asyncio.gather(*(q.join() for q in self._queues.values()))
        for worker in self._workers.values():
            worker.cancel()
        await asyncio.gather(*self._workers.values(), return_exceptions=True)
        self._workers.clear()
        self._queues.clear()

    async def advance(self, at: str) -> None:
        """Expire pending intents even when their symbols receive no further quotes."""
        await asyncio.gather(*(q.join() for q in self._queues.values()))
        async with self._lock:
            if seconds(at) < self._last_enqueued:
                raise ValueError("Cannot rewind account clock")
            self._last_enqueued = seconds(at)
            self._expire(at)
            if self.last_at is not None:
                self.last_at = at

    def _expire(self, at: str) -> None:
        for symbol, intent in list(self.pending.items()):
            if seconds(at) - seconds(intent.decision_at) > 30:
                self.outcomes.append({"intent_id": intent.id, "at": at, "status": "expired"})
                del self.pending[symbol]

    def _process(self, event: dict[str, Any]) -> None:
        event_id, symbol, at = event["id"], event["symbol"], event["received_at"]
        if event_id in self._processed:
            return
        tick = seconds(at)
        # Producers stamp receipt time on one clock before dispatching events.
        if tick < self._receipts.get(symbol, -math.inf):
            raise ValueError("Cannot rewind a symbol")
        if self.last_at is not None and tick < seconds(self.last_at):
            raise ValueError("Events must be dispatched in receipt order")
        self._expire(at)
        quote = event["book"]
        source = seconds(quote["timestamp"])
        self._processed.add(event_id)
        self._receipts[symbol] = tick
        if not 0 <= tick - source <= 60 or quote.get("currency") != self.context.market.currency:
            return
        previous_source = self._source_times.get(symbol, -math.inf)
        if source < previous_source:
            return
        previous = self._books.get(symbol)
        duplicate_quote = (
            source == previous_source
            and previous is not None
            and all(
                previous[key] == quote[key] for key in ("timestamp", "currency", "asks", "bids")
            )
        )
        asks = [(float(r["price"]), float(r["volume"])) for r in quote["asks"]]
        bids = [(float(r["price"]), float(r["volume"])) for r in quote["bids"]]
        kr = self.context.market.code == "KR"
        if kr:
            asks = [(price, volume) for price, volume in asks if volume != 0]
            bids = [(price, volume) for price, volume in bids if volume != 0]
        invalid = not asks or not bids or any(
            not math.isfinite(price)
            or price <= 0
            or not math.isfinite(volume)
            or (volume < 0 if kr else int(volume) < 0)
            or not self.context.market.valid_quote_level(price, volume)
            for price, volume in asks + bids
        )
        if invalid:
            if not kr:
                raise ValueError("Invalid orderbook")
            self._source_times[symbol] = source
            self._books.pop(symbol, None)
            self.book._failure("quote_invalid")
            return
        ask, bid = min(p for p, _ in asks), max(p for p, _ in bids)
        if bid > ask:
            if not kr:
                raise ValueError("Crossed orderbook")
            self._source_times[symbol] = source
            self._books.pop(symbol, None)
            self.book._failure("quote_crossed")
            return
        if not duplicate_quote:
            self._source_times[symbol] = source
            self._books[symbol] = copy.deepcopy(quote)
        self._fx = (
            event.get("fx", 0.0),
            event.get("fx_at", at),
            event.get("fx_valid_from"),
            event.get("fx_valid_until"),
            event.get("fx_provider_validity_required", False),
        )
        if not duplicate_quote:
            self.prices[symbol], self.times[symbol] = (ask + bid) / 2, quote["timestamp"]
        intent = self.pending.get(symbol)
        if (
            not duplicate_quote
            and intent
            and tick - seconds(intent.decision_at) >= self.delay
            and source > seconds(intent.decision_at)
        ):
            self._confirm(intent, quote, event)
            self.last_at = at
            self.book.mark(self.prices)
            return
        f = event.get("feature")
        fresh = (
            f is not None
            and seconds(f.available_at) <= tick
            and (0 <= tick - seconds(f.bar_end) <= 90)
        )
        if fresh and f is not None and symbol not in self.pending:
            if symbol in self.book.positions:
                owner, original = self.owners[symbol]
                if not holding_valid(owner, f, original):
                    self._intent(event, f, owner, "SELL")
            elif event.get("entries", True) and event.get("eligible", False):
                active = self.selection != "active" or abs(f.r5) >= 0.001 or abs(f.r15) >= 0.003
                strategy = assign(f, self.policy) if active else None
                if strategy:
                    self._intent(event, f, strategy, "BUY")
        self.last_at = at
        self.book.mark(self.prices)
        if self.reserved_cash > self.book.cash + 1e-9:
            raise RuntimeError("Reservations exceed account cash")

    def _intent(self, event: dict, f: Feature, strategy: str, side: str) -> None:
        symbol = event["symbol"]
        prices = [
            float(row["price"])
            for row in event["book"]["asks" if side == "BUY" else "bids"]
            if float(row["volume"]) >= 1
        ]
        if not prices:
            return
        limit = min(prices) if side == "BUY" else max(prices)
        limit = self.context.market.limit_price(limit, side)
        if limit <= 0:
            return
        if side == "BUY":
            count = event.get("candidate_count", 1) if self.allocation == "equal" else 1
            if not isinstance(count, int) or count < 1:
                raise ValueError("Positive decision-time candidate count required")
            budget = self.available_cash / count
            quantity = self.book.affordable_shares(budget, limit)
            reserve = self.book.entry_cost(quantity, limit)
        else:
            quantity, reserve = int(self.book.positions[symbol].quantity), 0.0
        self.decisions.append(
            {
                "at": event["received_at"],
                "symbol": symbol,
                "strategy": strategy,
                "action": side.lower(),
                "quantity": quantity,
            }
        )
        if not quantity:
            return
        order_request = None
        if "provider_symbol" in event:
            try:
                order_request = whole_share_limit(
                    self.context.market,
                    event["provider_symbol"],
                    side,
                    quantity,
                    Decimal(str(limit)),
                    client_order_id=event["id"].replace(":", "-"),
                    held_quantity=quantity if side == "SELL" else 0,
                )
            except ValueError as error:
                self.decisions[-1]["blocked_reason"] = str(error)
                return
        intent = Intent(
            event["id"],
            symbol,
            side,
            quantity,
            limit,
            reserve,
            event["received_at"],
            strategy,
            f,
            copy.deepcopy(event["book"]),
            self.entry_policy_version if side == "BUY" else self.owner_policy_versions[symbol],
            order_request,
        )
        self.pending[symbol] = intent
        self.intents.append(intent)

    def _confirm(self, intent: Intent, quote: dict, event: dict) -> None:
        symbol, at = intent.symbol, event["received_at"]
        frame = {
            "at": at,
            "decision_at": intent.decision_at,
            "decision_orderbooks": {symbol: intent.initial},
            "order_limits": {symbol: intent.limit},
            "orderbooks": {symbol: quote},
            "confirmation_received_at": {symbol: at},
            "fx": event.get("fx", 0),
            "fx_at": event.get("fx_at", at),
            "fx_valid_from": event.get("fx_valid_from"),
            "fx_valid_until": event.get("fx_valid_until"),
            "fx_provider_validity_required": event.get("fx_provider_validity_required", False),
        }
        self.context.frame = frame
        before = self.book.realized
        requested = intent.quantity
        if intent.side == "BUY":
            levels = self.book._quote(symbol, "BUY", self.prices[symbol])
            capacity = sum(v for _, v in levels[0]) if levels else 0
            count = min(requested, capacity)
            if count:
                self.book.entry_requested[symbol] = requested
                self.book.fill(
                    symbol, count, self.prices[symbol], at, int(seconds(at) * 1000), "entry"
                )
                self.owners[symbol] = (intent.strategy, intent.feature)
                self.owner_policy_versions[symbol] = intent.policy_version
            else:
                self.book._attempt(symbol, "BUY", requested, 0)
        else:
            levels = self.book._quote(symbol, "SELL", self.prices[symbol])
            capacity = sum(v for _, v in levels[0]) if levels else 0
            count = min(requested, capacity)
            execution = (
                self.book._execution(symbol, "SELL", count, self.prices[symbol])
                if count
                else None
            )
            other_reserved = self.reserved_cash - intent.reserved
            reason = "sell_execution_unavailable"
            if execution is not None and (
                self.book.cash + count * execution[0] - execution[1][self.book.total_fee_key]
                < other_reserved - 1e-9
            ):
                reason = "insufficient_cash_after_sell_costs"
                execution = None
            if execution is None:
                self.book._attempt(symbol, "SELL", requested, 0)
                self.outcomes.append(
                    {"intent_id": intent.id, **self.book.order_attempts[-1], "blocked_reason": reason}
                )
                del self.pending[symbol]
                return
            self.book.fill(
                symbol, -count, self.prices[symbol], at, int(seconds(at) * 1000), "invalidated"
            )
            attempt = self.book.order_attempts[-1]
            attempt["requested_quantity"] = requested
            attempt["unfilled_quantity"] = requested - count
            attempt["status"] = "filled" if requested == count else "partial"
            if symbol not in self.book.positions:
                del self.owners[symbol]
                del self.owner_policy_versions[symbol]
        self.realized_by_strategy[intent.strategy] += self.book.realized - before
        self.outcomes.append({"intent_id": intent.id, **self.book.order_attempts[-1]})
        del self.pending[symbol]
        if self.book.cash < -1e-9 or self.reserved_cash > self.book.cash + 1e-9:
            raise RuntimeError("Unfunded paper execution")

    def summary(self) -> dict[str, Any]:
        self.context.frame = {
            "at": self.last_at,
            "orderbooks": self._books,
            "fx": self._fx[0],
            "fx_at": self._fx[1],
            "fx_valid_from": self._fx[2],
            "fx_valid_until": self._fx[3],
            "fx_provider_validity_required": self._fx[4],
        }
        result = super().summary()
        assert self.last_at is not None
        cutoff = seconds(self.last_at)
        result["account"]["stale_open_positions"] = sum(
            not 0
            <= cutoff - seconds(self.times[symbol])
            <= self.context.schedule["max_quote_age_seconds"]
            for symbol in self.book.positions
        )
        result["account"]["oldest_open_quote_at"] = min(
            (self.times[symbol] for symbol in self.book.positions), key=seconds, default=None
        )
        result["account"]["mark_basis"] = "book_midpoint_not_liquidated"
        result.update(
            reserved_cash=self.reserved_cash,
            available_cash=self.available_cash,
            pending_intents=len(self.pending),
            intent_count=len(self.intents),
            intents=[
                {
                    key: getattr(intent, key)
                    for key in (
                        "id",
                        "symbol",
                        "side",
                        "quantity",
                        "limit",
                        "reserved",
                        "decision_at",
                        "strategy",
                    )
                }
                for intent in self.intents
            ],
            outcomes=copy.deepcopy(self.outcomes),
        )
        return result
