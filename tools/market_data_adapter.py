"""Adapt the existing read-only MarketClient to gateway batches."""

from collections.abc import Callable
from contextlib import nullcontext
from threading import Lock
from typing import Any, ParamSpec, TypeVar

from papertrade.api import ApiError, MarketClient
from tools.market_data_repository import DataGroup
from tools.runtime_audit import RuntimeAudit

P = ParamSpec("P")
T = TypeVar("T")


class MarketDataAdapter:
    def __init__(self, client: MarketClient, audit: RuntimeAudit | None = None) -> None:
        self._client = client
        self._lock = Lock()
        self._audit = audit

    def read(self, operation: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        """Serialize existing market-data operations against the same client's limiter."""
        with (
            self._audit.operation(
                "provider_adapter", "read_failed", timeout=30, local_recovery=True,
            )
            if self._audit is not None else nullcontext(),
            self._lock,
        ):
            return operation(*args, **kwargs)

    def fetch(self, group: DataGroup, symbols: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        return self.read(self._fetch, group, symbols)

    def _fetch(self, group: DataGroup, symbols: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        _, kind, parameters = group
        if kind == "price":
            rows = self._client.prices(list(symbols))
            requested = set(symbols)
            result = {}
            for row in rows:
                symbol = row.get("symbol") if isinstance(row, dict) else None
                if not isinstance(symbol, str) or symbol not in requested or symbol in result:
                    raise ApiError(
                        "Price response does not match the requested symbols",
                        code="invalid_response",
                    )
                result[symbol] = row
            return result
        (symbol,) = symbols
        if kind == "book":
            return {symbol: self._client.book(symbol)}
        params = dict(parameters)
        return {
            symbol: self._client.candles(symbol, str(params["before"]), int(params["count"]))
        }
