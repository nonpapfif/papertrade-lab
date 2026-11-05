"""Explicit restrictions for the KR whole-share diagnostic comparison."""

from datetime import datetime
from typing import Any

from tools.paper_market import KR


def continuous_window(calendar: dict[str, Any], at: datetime) -> tuple[datetime, datetime]:
    """Exclude auctions; a union calendar does not establish per-venue routing."""
    return KR.session(calendar, KR.trading_date(at))


def eligible(row: dict[str, Any]) -> bool:
    detail = row.get("koreanMarketDetail") or {}
    return (
        row.get("status") == "ACTIVE"
        and row.get("currency") == KR.currency
        and row.get("securityType") == "STOCK"
        and row.get("isCommonShare") is True
        and row.get("leverageFactor") in (None, 1)
        and detail.get("liquidationTrading") is False
        and detail.get("krxTradingSuspended") is False
        and (
            detail.get("nxtSupported") is False
            or (detail.get("nxtSupported") is True and detail.get("nxtTradingSuspended") is False)
        )
    )
