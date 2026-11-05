"""Market identity and provider session rules shared by paper runners."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class PaperMarket:
    code: str
    currency: str
    timezone: ZoneInfo
    calendar_path: tuple[str, ...]
    cutoff_field: str

    def trading_date(self, at: datetime) -> str:
        if at.utcoffset() is None:
            raise ValueError("Market observation requires a timezone")
        return at.astimezone(self.timezone).date().isoformat()

    def session(self, calendar: dict[str, Any], day: str) -> tuple[datetime, datetime]:
        today = calendar["today"]
        if today["date"] != day:
            raise ValueError("Calendar date does not match the observation date")
        regular = today
        for key in self.calendar_path:
            regular = regular.get(key) if isinstance(regular, dict) else None
        if not isinstance(regular, dict) or not regular.get(self.cutoff_field):
            raise ValueError("Verified continuous session is unavailable")
        start = datetime.fromisoformat(regular["startTime"])
        cutoff = datetime.fromisoformat(regular[self.cutoff_field])
        close = datetime.fromisoformat(regular["endTime"])
        if (
            any(self.trading_date(at) != day for at in (start, cutoff, close))
            or not start < cutoff <= close
        ):
            raise ValueError("Invalid continuous session")
        return start, cutoff


US = PaperMarket("US", "USD", ZoneInfo("America/New_York"), ("regularMarket",), "endTime")
KR = PaperMarket(
    "KR",
    "KRW",
    ZoneInfo("Asia/Seoul"),
    ("integrated", "regularMarket"),
    "singlePriceAuctionStartTime",
)
