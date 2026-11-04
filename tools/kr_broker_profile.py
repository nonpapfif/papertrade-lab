"""Explicit restrictions for the KR whole-share diagnostic comparison."""

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


def continuous_window(calendar: dict[str, Any], at: datetime) -> tuple[datetime, datetime]:
    """Exclude auctions; a union calendar does not establish per-venue routing."""
    day = calendar["today"]
    if day["date"] != at.astimezone(ZoneInfo("Asia/Seoul")).date().isoformat():
        raise ValueError("Calendar date does not match the KR observation date")
    regular = (day.get("integrated") or {}).get("regularMarket")
    if not regular or not regular.get("singlePriceAuctionStartTime"):
        raise ValueError("Verified KR continuous session is unavailable")
    start = datetime.fromisoformat(regular["startTime"])
    end = datetime.fromisoformat(regular["singlePriceAuctionStartTime"])
    close = datetime.fromisoformat(regular["endTime"])
    if any(t.tzinfo is None for t in (start, end, close)) or not start < end <= close:
        raise ValueError("Invalid KR continuous session")
    return start, end


def eligible(row: dict[str, Any]) -> bool:
    detail = row.get("koreanMarketDetail") or {}
    return (
        row.get("status") == "ACTIVE"
        and row.get("currency") == "KRW"
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
