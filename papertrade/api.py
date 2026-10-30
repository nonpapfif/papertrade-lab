"""Strict allowlist for authentication and public market-data requests."""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import json
import os
import re
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from .core import SafetyError, Settings

BASE_URL = "https://openapi.tossinvest.com"
GET_PATHS = frozenset(
    {
        "/api/v1/prices",
        "/api/v1/orderbook",
        "/api/v1/trades",
        "/api/v1/candles",
        "/api/v1/stocks",
        "/api/v1/stocks/all",
        "/api/v1/rankings",
        "/api/v1/exchange-rate",
        "/api/v1/market-calendar/KR",
        "/api/v1/market-calendar/US",
        "/api/v1/market-indicators/prices",
    }
)
SYMBOL = re.compile(r"[A-Za-z0-9.\-]{1,24}\Z")


class ApiError(Exception):
    """A sanitized, classified transport failure with a durable retry time."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        code: str = "transport_error",
        retry_at: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status, self.code, self.retry_at = status, code, retry_at


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


def validate_request(method: str, path: str) -> None:
    if method == "POST" and path == "/oauth2/token":
        return
    if method == "GET" and path in GET_PATHS:
        return
    if method == "GET" and re.fullmatch(r"/api/v1/stocks/[A-Za-z0-9.\-]{1,24}/warnings", path):
        return
    raise SafetyError("Endpoint is outside the market-data-only allowlist")


def secret_file(path: Path) -> str:
    if path.is_symlink():
        raise SafetyError("Credential symlinks are not allowed")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        info = os.fstat(stream.fileno())
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise SafetyError("Credential files must be owned by this user with mode 600")
        value = stream.read().rstrip("\r\n")
    if not value or value != value.strip() or "\n" in value or "\r" in value:
        raise SafetyError("Credential file must contain exactly one nonempty value")
    return value


def private_json(path: Path, data: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(data, stream)
    os.replace(temporary, path)


class MarketClient:
    def __init__(
        self,
        settings: Settings,
        audit: Callable[[dict[str, Any]], None] | None = None,
        store: Any = None,
    ) -> None:
        self.settings = settings
        self.audit = audit or (lambda record: None)
        self.opener = urllib.request.build_opener(NoRedirect())
        self.last_request = 0.0
        self.calls = 0
        self.token: str | None = None
        self.expires_at = 0.0
        self.store = store
        self.local_rates: dict[str, tuple[float, float, int]] = {}

    @staticmethod
    def group(path: str) -> str:
        if path == "/oauth2/token":
            return "AUTH"
        if path == "/api/v1/stocks/all":
            return "STOCK_ALL"
        if path == "/api/v1/candles":
            return "MARKET_DATA_CHART"
        if path in ("/api/v1/prices", "/api/v1/orderbook", "/api/v1/trades"):
            return "MARKET_DATA"
        if path == "/api/v1/rankings":
            return "RANKING"
        if path.startswith("/api/v1/stocks"):
            return "STOCK"
        return "MARKET_INFO"

    def _reserve(self, group: str) -> None:
        interval = max(
            float(self.settings.get("limits", "minimum_request_interval")),
            1.05 if group == "STOCK_ALL" else 0,
        )
        now = time.time()
        if self.store is not None:
            with self.store.transaction(write=True):
                row = self.store.db.execute(
                    "SELECT next_at,interval FROM rate_limits WHERE name=?", (group,)
                ).fetchone()
                next_at = max(now, row[0]) if row else now
                interval = max(interval, row[1]) if row else interval
                if next_at - now > 1.5:
                    raise ApiError(
                        "Server-directed request cooldown", code="cooldown", retry_at=next_at
                    )
                self.store.db.execute(
                    "INSERT INTO rate_limits(name,next_at,interval) VALUES (?,?,?) ON CONFLICT(name) DO UPDATE SET next_at=excluded.next_at,interval=excluded.interval",
                    (group, next_at + interval, interval),
                )
        else:
            previous, observed, _failures = self.local_rates.get(group, (0, interval, 0))
            next_at = max(now, previous)
            interval = max(interval, observed)
            if next_at - now > 1.5:
                raise ApiError("Request cooldown", code="cooldown", retry_at=next_at)
            self.local_rates[group] = (next_at + interval, interval, 0)
        time.sleep(max(0, next_at - time.time()))

    def _rate_result(self, group: str, headers: Any, failed: bool) -> float:
        now = time.time()
        headers = {str(key).lower(): value for key, value in headers.items()}

        def number(name: str) -> float:
            try:
                return max(0, float(headers.get(name.lower(), 0)))
            except (ValueError, TypeError):
                return 0

        retry = number("Retry-After")
        if not retry and headers.get("retry-after"):
            try:
                retry = max(0, parsedate_to_datetime(headers["retry-after"]).timestamp() - now)
            except (ValueError, TypeError, OverflowError):
                retry = 0
        limit = number("X-RateLimit-Limit")
        reset = number("X-RateLimit-Reset")
        remaining = headers.get("x-ratelimit-remaining")
        if self.store is not None:
            with self.store.transaction(write=True):
                row = self.store.db.execute(
                    "SELECT interval,failures,next_at FROM rate_limits WHERE name=?", (group,)
                ).fetchone()
                interval, failures, next_at = tuple(row) if row else (0.6, 0, 0)
                failures = failures + 1 if failed else 0
                interval = max(interval, 1.05 / limit if limit else 0)
                if failed:
                    interval = max(interval, 1)
                else:
                    interval = max(
                        float(self.settings.get("limits", "minimum_request_interval")),
                        interval * 0.8,
                        1.05 / limit if limit else 0,
                    )
                delay = (
                    max(retry, reset, min(300, 2 ** min(failures, 8)))
                    if failed
                    else (reset if str(remaining) == "0" else 0)
                )
                retry_at = max(next_at, now + delay + 0.05)
                self.store.db.execute(
                    "INSERT INTO rate_limits(name,next_at,interval,failures,data) VALUES (?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET next_at=excluded.next_at,interval=excluded.interval,failures=excluded.failures,data=excluded.data",
                    (
                        group,
                        retry_at,
                        interval,
                        failures,
                        json.dumps(
                            {
                                "limit": limit,
                                "remaining": remaining,
                                "reset_seconds": reset,
                                "retry_after_seconds": retry,
                            }
                        ),
                    ),
                )
                return retry_at
        previous, interval, failures = self.local_rates.get(group, (0, 0.6, 0))
        failures = failures + 1 if failed else 0
        interval = max(interval, 1.05 / limit if limit else 0)
        if failed:
            interval = max(interval, 1)
        else:
            interval = max(
                float(self.settings.get("limits", "minimum_request_interval")),
                interval * 0.8,
                1.05 / limit if limit else 0,
            )
        delay = (
            max(retry, reset, min(300, 2 ** min(failures, 8)))
            if failed
            else (reset if str(remaining) == "0" else 0)
        )
        retry_at = max(previous, now + delay + 0.05)
        self.local_rates[group] = (retry_at, interval, failures)
        return retry_at

    @staticmethod
    def _payload(response: Any) -> dict[str, Any]:
        raw = response.read()
        if raw.startswith(b"\x1f\x8b"):
            raw = gzip.decompress(raw)
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ApiError("Unexpected market-data response", code="invalid_response")
        return payload

    def _request(
        self,
        method: str,
        path: str,
        parameters: dict[str, Any] | None = None,
        form: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        validate_request(method, path)
        from .audit import Audit

        group = self.group(path)
        context = {
            "method": method,
            "path": path,
            "group": group,
            "role": self.store.role if self.store else "manual",
        }
        context.update(
            {
                key: value
                for key, value in (parameters or {}).items()
                if key
                in (
                    "symbol",
                    "symbols",
                    "market",
                    "marketCountry",
                    "date",
                    "interval",
                    "count",
                    "before",
                )
            }
        )
        for attempt in range(2):
            headers = {"Accept": "application/json", "Accept-Encoding": "gzip"}
            if method == "GET":
                headers["Authorization"] = "Bearer " + self.access_token()
            self._reserve(group)
            query = "?" + urllib.parse.urlencode(parameters) if parameters else ""
            body = urllib.parse.urlencode(form).encode() if form is not None else None
            if body is not None:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
            request = urllib.request.Request(
                BASE_URL + path + query, data=body, headers=headers, method=method
            )
            self.calls += 1
            try:
                with self.opener.open(request, timeout=15) as response:
                    payload = self._payload(response)
                    self._rate_result(group, response.headers, False)
                    self.audit(
                        {
                            **context,
                            "status": response.status,
                            "attempt": attempt + 1,
                            "request_id": response.headers.get("X-Request-Id"),
                            "rate_limit": {
                                key: response.headers.get(key)
                                for key in (
                                    "X-RateLimit-Limit",
                                    "X-RateLimit-Remaining",
                                    "X-RateLimit-Reset",
                                    "Retry-After",
                                )
                            },
                        }
                    )
                    if self.store:
                        Audit(self.store).recovered("http", group=group)
                    return payload
            except urllib.error.HTTPError as error:
                try:
                    detail = self._payload(error).get("error", {})
                    code = (
                        detail.get("code", "http_error")
                        if isinstance(detail, dict)
                        else "http_error"
                    )
                    request_id = detail.get("requestId") if isinstance(detail, dict) else None
                    if not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", code):
                        code = "http_error"
                except (ValueError, OSError, ApiError, EOFError):
                    code, request_id = "http_error", None
                rate_headers = dict(error.headers.items())
                if error.code == 403:
                    rate_headers["retry-after"] = "300"
                retry_at = self._rate_result(
                    group, rate_headers, error.code in (403, 429) or error.code >= 500
                )
                record = {
                    **context,
                    "status": error.code,
                    "code": code,
                    "request_id": request_id or error.headers.get("X-Request-Id"),
                    "attempt": attempt + 1,
                    "retry_at": retry_at,
                    "rate_limit": {
                        key: error.headers.get(key)
                        for key in (
                            "X-RateLimit-Limit",
                            "X-RateLimit-Remaining",
                            "X-RateLimit-Reset",
                            "Retry-After",
                        )
                    },
                }
                self.audit(record)
                if self.store:
                    Audit(self.store).record(
                        "http",
                        code,
                        record,
                        state="waiting_external" if error.code == 403 else "retrying",
                        retry_at=datetime.fromtimestamp(retry_at, UTC).isoformat(),
                    )
                if error.code == 401 and method == "GET" and attempt == 0:
                    self._invalidate_token()
                    continue
                raise ApiError(
                    f"HTTP {error.code} at market-data endpoint ({code})",
                    status=error.code,
                    code=code,
                    retry_at=retry_at,
                ) from None
            except (
                urllib.error.URLError,
                TimeoutError,
                OSError,
                ValueError,
                EOFError,
                ApiError,
            ) as error:
                retry_at = self._rate_result(group, {}, True)
                if self.store:
                    Audit(self.store).exception("http", error, **context)
                raise ApiError(
                    "Market-data transport or JSON failure",
                    code="transport_error",
                    retry_at=retry_at,
                ) from None
        raise ApiError(
            "Authentication refresh did not recover access", code="authentication_failed"
        )

    def _invalidate_token(self) -> None:
        path = self.settings.secrets_dir / "TOSS_ACCESS_TOKEN.json"
        with self._token_lock():
            if path.exists():
                try:
                    cached = json.loads(secret_file(path))
                    if cached.get("access_token") == self.token:
                        path.unlink()
                except (ValueError, OSError):
                    pass
            self.token = None
            self.expires_at = 0

    def _token_lock(self) -> Any:
        from contextlib import contextmanager

        @contextmanager
        def locked() -> Any:
            directory = self.settings.secrets_dir
            info = directory.stat()
            if (
                directory.is_symlink()
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o700
            ):
                raise SafetyError("Secrets directory must be owned by this user with mode 700")
            descriptor = os.open(
                directory / "TOSS_TOKEN.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
            )
            with os.fdopen(descriptor, "w") as stream:
                os.fchmod(stream.fileno(), 0o600)
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
                yield

        return locked()

    def access_token(self) -> str:
        if self.token and self.expires_at > time.time() + 120:
            return self.token
        with self._token_lock():
            directory = self.settings.secrets_dir
            client_id = secret_file(directory / "TOSS_CLIENT_ID")
            digest = hashlib.sha256(client_id.encode()).hexdigest()
            path = directory / "TOSS_ACCESS_TOKEN.json"
            if path.exists():
                try:
                    cached = json.loads(secret_file(path))
                    if (
                        cached.get("client_digest") == digest
                        and cached.get("expires_at", 0) > time.time() + 120
                        and isinstance(cached.get("access_token"), str)
                    ):
                        self.token = cached["access_token"]
                        self.expires_at = float(cached["expires_at"])
                        return self.token
                except (ValueError, TypeError, OSError):
                    if self.store:
                        from .audit import Audit

                        Audit(self.store).record(
                            "authentication", "invalid_cache", {}, state="retrying"
                        )
            payload = self._request(
                "POST",
                "/oauth2/token",
                form={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": secret_file(directory / "TOSS_CLIENT_SECRET"),
                },
            )
            if (
                not isinstance(payload.get("access_token"), str)
                or not payload["access_token"]
                or type(payload.get("expires_in")) is not int
                or payload["expires_in"] <= 0
            ):
                raise ApiError("Authentication response is invalid", code="invalid_auth_response")
            self.token = payload["access_token"]
            self.expires_at = time.time() + payload["expires_in"]
            private_json(
                path,
                {
                    "access_token": self.token,
                    "expires_at": self.expires_at,
                    "client_digest": digest,
                },
            )
            return self.token

    def get(self, path: str, **parameters: Any) -> Any:
        validate_request("GET", path)
        payload = self._request("GET", path, parameters=parameters or None)
        if "result" not in payload:
            raise ApiError("Market-data result is missing", code="invalid_response")
        result = payload["result"]
        list_paths = {"/api/v1/prices", "/api/v1/stocks/all"}
        if path in list_paths and (
            not isinstance(result, list) or any(not isinstance(row, dict) for row in result)
        ):
            raise ApiError("Invalid market-data list", code="invalid_response")
        flexible = path == "/api/v1/stocks" or path.endswith("/warnings")
        if path not in list_paths and not isinstance(result, (dict, list) if flexible else dict):
            raise ApiError("Invalid market-data result", code="invalid_response")
        return result

    def calendar(self, market: str, date: str) -> dict[str, Any]:
        return self.get(f"/api/v1/market-calendar/{market}", date=date)

    def prices(self, symbols: list[str]) -> list[dict[str, Any]]:
        self._symbols(symbols)
        return self.get("/api/v1/prices", symbols=",".join(symbols))

    def stocks(self, symbols: list[str]) -> list[dict[str, Any]]:
        self._symbols(symbols)
        result = self.get("/api/v1/stocks", symbols=",".join(symbols))
        return result if isinstance(result, list) else result["stocks"]

    def listed(self, market: str, status: str = "ACTIVE") -> list[dict[str, Any]]:
        if market not in ("KOSPI", "KOSDAQ", "KR_ETC", "NYSE", "NASDAQ", "AMEX", "US_ETC"):
            raise SafetyError("Unknown listing market")
        return self.get("/api/v1/stocks/all", market=market, status=status)

    def book(self, symbol: str) -> dict[str, Any]:
        self._symbols([symbol])
        result = self.get("/api/v1/orderbook", symbol=symbol)
        if result.get("symbol", symbol) != symbol:
            raise ApiError("Quote symbol does not match the request", code="invalid_response")
        return result

    def warnings(self, symbol: str) -> list[dict[str, Any]]:
        self._symbols([symbol])
        result = self.get(f"/api/v1/stocks/{symbol}/warnings")
        if isinstance(result, list):
            return result
        if not isinstance(result.get("warnings"), list):
            raise ApiError("Missing warning list", code="invalid_response")
        return result["warnings"]

    def candles(self, symbol: str, before: str, count: int = 200) -> dict[str, Any]:
        self._symbols([symbol])
        return self.get(
            "/api/v1/candles",
            symbol=symbol,
            interval="1m",
            count=count,
            before=before,
            adjusted="true",
        )

    def daily(self, symbol: str, before: str, adjusted: bool) -> dict[str, Any]:
        self._symbols([symbol])
        result = self.get(
            "/api/v1/candles",
            symbol=symbol,
            interval="1d",
            count=200,
            before=before,
            adjusted=str(adjusted).lower(),
        )
        if not isinstance(result, dict) or not isinstance(result.get("candles"), list):
            raise ApiError("Invalid daily candle response", code="invalid_response")
        return result

    def ranking(self, market: str, kind: str, count: int) -> dict[str, Any]:
        return self.get(
            "/api/v1/rankings",
            marketCountry=market,
            type=kind,
            duration="realtime" if kind == "MARKET_TRADING_AMOUNT" else "1d",
            excludeInvestmentCaution="false",
            count=count,
        )

    @staticmethod
    def _symbols(symbols: list[str]) -> None:
        if not 1 <= len(symbols) <= 200 or any(
            not isinstance(s, str) or not SYMBOL.fullmatch(s) for s in symbols
        ):
            raise SafetyError("Invalid market-data symbol batch")
