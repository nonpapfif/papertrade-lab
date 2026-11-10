"""Durable market dates and continuous windows from the approved market-data API."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from papertrade.api import ApiError, MarketClient
from papertrade.core import SafetyError
from papertrade.jsonio import atomic_json, read_json
from tools.paper_market import PaperMarket
from tools.runtime_audit import RuntimeAudit


@dataclass(frozen=True)
class Window:
    day: str
    start: datetime
    end: datetime


class MarketCalendar:
    def __init__(
        self, client: MarketClient, output: Path, audit: RuntimeAudit | None = None,
    ) -> None:
        self.client = client
        self.output = output
        self.audit = audit

    def window(self, market: PaperMarket, now: datetime) -> Window | None:
        day = market.trading_date(now)
        context = {"market": market.code, "event_id": day}
        with (
            self.audit.operation("calendar", "window_failed", context, timeout=30)
            if self.audit is not None else nullcontext()
        ):
            return self._load_window(market, now, day, context)

    def _load_window(
        self, market: PaperMarket, now: datetime, day: str, context: dict[str, str],
    ) -> Window | None:
        path = self.output / market.code / f"{day}.json"
        saved = read_json(path) if path.exists() else {}
        if "calendar" not in saved:
            if saved.get("retry_at", 0) > now.timestamp():
                return None
            try:
                calendar = self.client.calendar(market.code, day)
                window = self._window(market, day, calendar)
            except (ApiError, SafetyError, OSError, ValueError, TypeError, KeyError) as error:
                if self.audit is not None:
                    self.audit.report_exception(
                        "calendar", "provider_calendar_unavailable", error, context,
                        severity="warning" if isinstance(error, (ApiError, OSError)) else "error",
                    )
                failures = saved.get("failures", 0) + 1
                delay = min(300, 5 * 2 ** min(failures - 1, 6))
                provider_retry = error.retry_at if isinstance(error, ApiError) else None
                atomic_json(
                    path,
                    {
                        "state": "retry",
                        "failures": failures,
                        "error_type": type(error).__name__,
                        "api_code": error.code if isinstance(error, ApiError) else None,
                        "retry_at": max(now.timestamp() + delay, provider_retry or 0),
                    },
                )
                return None
            atomic_json(
                path,
                {
                    "state": "open" if window else "closed",
                    "provider": "toss_market_calendar",
                    "calendar": calendar,
                },
            )
            if self.audit is not None and saved.get("failures", 0):
                self.audit.record(
                    "calendar", "provider_calendar_unavailable", context, state="resolved",
                )
            return window
        return self._window(market, day, saved["calendar"])

    @staticmethod
    def _window(market: PaperMarket, day: str, calendar: dict[str, Any]) -> Window | None:
        today = calendar["today"]
        if today["date"] != day:
            raise ValueError("Calendar date differs from requested market date")
        regular = today
        for key in market.calendar_path:
            if not isinstance(regular, dict):
                raise TypeError("Malformed market calendar")
            if key not in regular:
                previous = calendar["previousBusinessDay"]["date"]
                following = calendar["nextBusinessDay"]["date"]
                if regular is today and set(today) == {"date"} and previous < day < following:
                    return None
                raise ValueError("Continuous market status is unverified")
            regular = regular[key]
            if regular is None:
                return None
        start, end = market.session(calendar, day)
        return Window(day, start, end)
