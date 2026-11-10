"""Shared asynchronous market-data owner and process-pipe client."""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, replace
from multiprocessing.connection import Connection
from typing import Any, Protocol

from papertrade.api import SYMBOL, ApiError
from papertrade.clock import utc_iso
from tools.market_data_repository import (
    DataCut,
    DataGroup,
    DataRecord,
    DataRequest,
    MarketDataRepository,
    Observation,
    epoch,
    finite_json,
)
from tools.runtime_audit import IncidentSeverity, RuntimeAuditSink


class Provider(Protocol):
    def fetch(self, group: DataGroup, symbols: tuple[str, ...]) -> dict[str, dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class DataFailure:
    symbol: str
    code: str
    status: int | None = None
    retry_at: float | None = None

    def as_error(self) -> ApiError:
        return ApiError(
            "Market-data request failed",
            code=self.code,
            status=self.status,
            retry_at=self.retry_at,
        )


@dataclass(frozen=True, slots=True)
class DataResult:
    records: tuple[DataRecord, ...]
    missing: tuple[str, ...]
    failures: tuple[DataFailure, ...]
    cache_hits: tuple[str, ...] = ()

    def require(self, symbol: str) -> DataRecord:
        for failure in self.failures:
            if failure.symbol == symbol:
                raise failure.as_error()
        for record in self.records:
            if record.symbol == symbol:
                return record
        raise ApiError("Requested market data is unavailable", code="missing_market_data")


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise TypeError("Expected a market-data number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Expected a finite market-data number")
    return number


def _observation(
    group: DataGroup,
    symbol: str,
    payload: dict[str, Any],
    received: str,
    identity: str,
) -> Observation:
    market, kind, parameters = group
    if not isinstance(payload, dict) or payload.get("symbol", symbol) != symbol:
        raise ValueError("Market-data symbol mismatch")
    if kind == "candles":
        candles = payload.get("candles")
        if not isinstance(candles, list) or not candles:
            raise ValueError("Missing candle rows")
        before = epoch(str(dict(parameters)["before"]))
        for row in candles:
            if epoch(row["timestamp"]) > before:
                raise ValueError("Candle source time exceeds query time")
            if any(_number(row[key]) <= 0 for key in ("closePrice", "highPrice")):
                raise ValueError("Invalid candle price")
        source = max((row["timestamp"] for row in candles), key=epoch)
    else:
        if payload.get("currency") != {"US": "USD", "KR": "KRW"}[market]:
            raise ValueError("Market-data currency mismatch")
        source = payload.get("timestamp")
        if kind == "price":
            if _number(payload["lastPrice"]) <= 0:
                raise ValueError("Invalid market price")
        else:
            for side in ("asks", "bids"):
                levels = payload.get(side)
                if not isinstance(levels, list):
                    raise TypeError("Missing book depth")
                for level in levels:
                    price, volume = _number(level["price"]), _number(level["volume"])
                    if market == "KR" and volume == 0:
                        continue
                    if price <= 0 or volume < 0:
                        raise ValueError("Invalid book depth level")
    if not isinstance(source, str) or epoch(source) > epoch(received):
        raise ValueError("Invalid market-data source time")
    finite_json(payload)
    return Observation(symbol, source, received, identity, payload)


Outcome = DataRecord | DataFailure | Exception | None


class MarketDataGateway:
    """Route cache misses from local and remote callers through one provider owner."""

    def __init__(
        self,
        repository: MarketDataRepository,
        provider: Provider,
        audit: RuntimeAuditSink | None = None,
        *,
        clock: Callable[[], str] = utc_iso,
        batch_window: float = 0.005,
    ) -> None:
        self.repository = repository
        self._provider = provider
        self._audit = audit
        self._clock = clock
        self._batch_window = batch_window
        self._pending: dict[DataGroup, list[str]] = {}
        self._inflight: dict[tuple[DataGroup, str], asyncio.Future[Outcome]] = {}
        self._worker: asyncio.Task[None] | None = None
        self._closing = False
        self._services: set[asyncio.Task[Any]] = set()
        self._open_incidents: dict[tuple[DataGroup, tuple[str, ...], float | None], set[str]] = {}

    def cut(self) -> DataCut:
        with self._operation("market_data_store", "cut_failed"):
            return self.repository.cut(self._clock())

    def _operation(self, component: str, code: str) -> AbstractContextManager[None]:
        return (
            self._audit.operation(
                component, code, timeout=120 if component == "market_data_gateway" else 10
            )
            if self._audit is not None
            else nullcontext()
        )

    def cached(self, request: DataRequest) -> dict[str, DataRecord]:
        # Zero age bypasses the latest cache, including equal-clock observations.
        # A historical cut always remains lookup-only.
        if request.max_age == 0 and request.as_of_cut is None:
            return {}
        with self._operation("market_data_store", "lookup_failed"):
            return self.repository.lookup(request, now=self._clock())

    async def request(self, request: DataRequest) -> DataResult:
        with self._operation("market_data_gateway", "request_failed"):
            return await self._request(request)

    async def _request(self, request: DataRequest) -> DataResult:
        if self._closing:
            return _failure_result(request, "gateway_stopped")
        group = request.group
        cached = self.cached(request)
        cache_hits = tuple(symbol for symbol in request.symbols if symbol in cached)
        missing = tuple(symbol for symbol in request.symbols if symbol not in cached)
        if request.as_of_cut is not None or not missing:
            if request.as_of_cut is None:
                for symbol in cache_hits:
                    self._resolve_recovered(group, (symbol,), max_age=request.max_age)
            return DataResult(tuple(cached[s] for s in cache_hits), missing, (), cache_hits)

        waiting = []
        for symbol in missing:
            key = (group, symbol)
            if key not in self._inflight:
                self._inflight[key] = asyncio.get_running_loop().create_future()
                self._pending.setdefault(group, []).append(symbol)
            waiting.append(asyncio.shield(self._inflight[key]))
        if self._worker is None:
            self._worker = asyncio.create_task(self._drain())

        outcomes = dict(zip(missing, await asyncio.gather(*waiting), strict=True))
        now = epoch(self._clock())
        records, absent, failures = [], [], []
        for symbol in request.symbols:
            outcome = cached.get(symbol) or outcomes.get(symbol)
            if isinstance(outcome, DataRecord):
                if request.max_age and not self._fresh(outcome, request, now):
                    failures.append(DataFailure(symbol, "stale_market_data"))
                    self._record(
                        group, "stale_market_data", (symbol,),
                        severity="warning", max_age=request.max_age,
                    )
                else:
                    self._resolve_recovered(group, (symbol,), max_age=request.max_age)
                    records.append(outcome)
            elif isinstance(outcome, DataFailure):
                failures.append(outcome)
            elif isinstance(outcome, Exception):
                raise outcome
            else:
                absent.append(symbol)
        return DataResult(
            tuple(records),
            tuple(absent),
            tuple(failures),
            tuple(record.symbol for record in records if record.symbol in cached),
        )

    @staticmethod
    def _fresh(record: DataRecord, request: DataRequest, at: float) -> bool:
        if not 0 <= at - epoch(record.received_time) <= request.max_age:
            return False
        return request.kind == "candles" or at - epoch(record.source_time) <= request.max_age

    async def prices(
        self, market: str, symbols: Iterable[str], *, max_age: float = 0
    ) -> DataResult:
        return await self.request(DataRequest(market, "price", tuple(symbols), max_age=max_age))

    async def book(self, market: str, symbol: str, *, max_age: float = 0) -> DataResult:
        return await self.request(DataRequest(market, "book", (symbol,), max_age=max_age))

    async def candles(
        self, market: str, symbol: str, before: str, count: int = 200, *, max_age: float = 0
    ) -> DataResult:
        return await self.request(
            DataRequest(
                market, "candles", (symbol,), (("before", before), ("count", count)), max_age
            )
        )

    def ingest_book(
        self,
        market: str,
        symbol: str,
        payload: dict[str, Any],
        *,
        received_time: str,
        source_identity: str = "toss-websocket",
    ) -> DataRecord:
        group = DataRequest(market, "book", (symbol,)).group
        try:
            observation = _observation(group, symbol, payload, received_time, source_identity)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            self._record(group, "invalid_response", (symbol,), error=error)
            raise ApiError("Invalid streamed market data", code="invalid_response") from error
        with self._operation("market_data_store", "publish_failed"):
            record = self.repository.publish(group, [observation])[symbol]
        self._resolve_recovered(group, (symbol,))
        return record

    async def _drain(self) -> None:
        try:
            while self._pending:
                await asyncio.sleep(self._batch_window)
                pending, self._pending = self._pending, {}
                for group, symbols in pending.items():
                    size = 200 if group[1] == "price" else 1
                    for offset in range(0, len(symbols), size):
                        batch = tuple(symbols[offset : offset + size])
                        (fetched,) = await asyncio.gather(
                            self._fetch(group, batch), return_exceptions=True
                        )
                        outcomes: dict[str, Outcome]
                        if isinstance(fetched, Exception):
                            outcomes = dict.fromkeys(batch, fetched)
                        elif isinstance(fetched, BaseException):
                            raise fetched
                        else:
                            outcomes = fetched
                        for symbol in batch:
                            self._inflight.pop((group, symbol)).set_result(outcomes.get(symbol))
        finally:
            for (_, symbol), future in self._inflight.items():
                future.set_result(DataFailure(symbol, "gateway_interrupted"))
            self._inflight.clear()
            self._pending.clear()
            self._worker = None

    async def _fetch(self, group: DataGroup, symbols: tuple[str, ...]) -> dict[str, Outcome]:
        try:
            # The SQLite read has completed before the synchronous transport starts.
            payloads, received = await asyncio.to_thread(self._provider_read, group, symbols)
            if not isinstance(payloads, dict) or set(payloads) - set(symbols):
                raise ApiError("Unexpected market-data result", code="invalid_response")
        except ApiError as error:
            self._record(
                group,
                error.code,
                symbols,
                status=error.status,
                error=error,
                severity="warning" if error.code == "cooldown" else "error",
            )
            return {
                symbol: DataFailure(symbol, error.code, error.status, error.retry_at)
                for symbol in symbols
            }
        except Exception as error:
            self._record(group, "provider_failure", symbols, error=error)
            raise

        observations = []
        outcomes: dict[str, Outcome] = {}
        invalid = []
        for symbol in symbols:
            if symbol not in payloads:
                continue
            try:
                observations.append(
                    _observation(group, symbol, payloads[symbol], received, "toss-rest")
                )
            except (KeyError, TypeError, ValueError, OverflowError):
                invalid.append(symbol)
                outcomes[symbol] = DataFailure(symbol, "invalid_payload")
        if invalid:
            self._record(group, "invalid_response", tuple(invalid))
        absent = tuple(symbol for symbol in symbols if symbol not in payloads)
        if absent:
            self._record(group, "missing_market_data", absent, severity="warning")
        try:
            with self._operation("market_data_store", "publish_failed"):
                outcomes.update(self.repository.publish(group, observations))
        except Exception as error:
            self._record(group, "storage_failure", symbols, error=error)
            raise
        for symbol, outcome in outcomes.items():
            if isinstance(outcome, DataRecord):
                self._resolve_recovered(group, (symbol,))
        if len(symbols) > 1 and all(
            isinstance(outcomes.get(symbol), DataRecord) for symbol in symbols
        ):
            self._resolve_recovered(group, symbols)
        return outcomes

    def _provider_read(
        self, group: DataGroup, symbols: tuple[str, ...]
    ) -> tuple[dict[str, dict[str, Any]], str]:
        return self._provider.fetch(group, symbols), self._clock()

    def _record(
        self,
        group: DataGroup,
        code: str,
        symbols: tuple[str, ...],
        *,
        status: int | None = None,
        error: BaseException | None = None,
        severity: IncidentSeverity = "error",
        resolved: bool = False,
        max_age: float | None = None,
    ) -> None:
        if self._audit is not None:
            ordered = sorted(symbols)
            incident_key = (group, tuple(ordered), max_age)
            identity: list[object] = [group[0], group[1], [list(p) for p in group[2]], ordered]
            if max_age is not None:
                # Normalize equal integer and float ages without rounding either value.
                identity.append(list(max_age.as_integer_ratio()))
            context: dict[str, object] = {
                "market": group[0],
                "request_id": hashlib.sha256(finite_json(identity).encode()).hexdigest(),
            }
            if len(symbols) == 1:
                context["symbol"] = symbols[0]
            if status is not None:
                context["http_status"] = status
            self._audit.record(
                "market_data_gateway",
                code,
                context,
                error=error,
                severity=severity,
                state="resolved" if resolved else "open",
            )
            if not resolved:
                self._open_incidents.setdefault(incident_key, set()).add(code)

    def _resolve_recovered(
        self, group: DataGroup, symbols: tuple[str, ...], *, max_age: float | None = None
    ) -> None:
        for code in self._open_incidents.pop((group, tuple(sorted(symbols)), max_age), ()):
            self._record(group, code, symbols, resolved=True, max_age=max_age)

    async def close(self) -> None:
        self._closing = True
        if self._services:
            results = await asyncio.gather(*tuple(self._services), return_exceptions=True)
            if self._audit is not None:
                self._audit.report_task_failures(
                    "market_data_gateway",
                    "pipe_service_failed",
                    results,
                )
        if self._worker is not None:
            await asyncio.shield(self._worker)

    async def serve(self, connection: Connection) -> None:
        """Serve one trusted child connection with one outstanding request at a time."""
        service = asyncio.current_task()
        if service is None:
            raise RuntimeError("Pipe service needs an asynchronous task")
        self._services.add(service)
        try:
            while not self._closing:
                if not await asyncio.to_thread(connection.poll, 0.05):
                    continue
                request = await asyncio.to_thread(connection.recv)
                if request is None:
                    break
                if not isinstance(request, DataRequest):
                    raise TypeError("Invalid pipe request type")
                response = asyncio.create_task(self._remote_request(request))
                try:
                    while not self._closing:
                        completed, _ = await asyncio.wait((response,), timeout=0.05)
                        if completed:
                            await asyncio.to_thread(connection.send, response.result())
                            break
                        if connection.poll():
                            # EOF and the explicit close marker are ordinary client shutdown.
                            if await asyncio.to_thread(connection.recv) is not None:
                                raise ValueError("Pipe client sent a request before its reply")
                            return
                finally:
                    if not response.done():
                        response.cancel()
                    await asyncio.gather(response, return_exceptions=True)
        except (EOFError, BrokenPipeError, ConnectionResetError):
            pass
        except (OSError, TypeError, ValueError) as error:
            if self._audit is not None:
                self._audit.report_exception("market_data_gateway", "pipe_protocol_failed", error)
            raise
        finally:
            connection.close()
            self._services.discard(service)

    async def _remote_request(self, request: DataRequest) -> DataResult:
        try:
            if request.as_of_cut is not None:
                request = replace(request, as_of_cut=replace(request.as_of_cut))
            request = replace(request)
        except (AttributeError, TypeError, ValueError, OverflowError) as error:
            if self._audit is not None:
                self._audit.report_exception("market_data_gateway", "pipe_request_invalid", error)
            return _failure_result(request, "invalid_request")
        (result,) = await asyncio.gather(self.request(request), return_exceptions=True)
        if isinstance(result, Exception):
            if self._audit is not None:
                self._audit.report_exception("market_data_gateway", "pipe_request_failed", result)
            return _failure_result(request, "gateway_failure")
        if isinstance(result, BaseException):
            raise result
        return result


def _failure_result(request: DataRequest, code: str) -> DataResult:
    symbols = getattr(request, "symbols", ())
    if not isinstance(symbols, tuple):
        return DataResult((), (), ())
    return DataResult(
        (),
        (),
        tuple(DataFailure(s, code) for s in symbols if isinstance(s, str) and SYMBOL.fullmatch(s)),
    )


class GatewayPipeClient:
    """Blocking process client for a dedicated duplex multiprocessing connection."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def request(self, request: DataRequest) -> DataResult:
        try:
            self._connection.send(request)
            result = self._connection.recv()
        except (EOFError, BrokenPipeError, ConnectionResetError):
            return _failure_result(request, "gateway_stopped")
        if not isinstance(result, DataResult):
            raise TypeError("Invalid market-data gateway response")
        return result

    def close(self) -> None:
        self._connection.close()
