"""Market identity and provider session rules shared by paper runners."""

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_DOWN, Decimal, localcontext
from typing import Any
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class PaperMarket:
    code: str
    currency: str
    timezone: ZoneInfo
    calendar_path: tuple[str, ...]
    cutoff_field: str

    def limit_price(self, price: float, side: str) -> float:
        if self.code == "US":
            return us_limit_price(price)
        if self.code != "KR":
            raise ValueError("No limit-price policy for this market")
        if not math.isfinite(price) or price <= 0 or side not in ("BUY", "SELL"):
            raise ValueError("Invalid KR limit price")
        tick = kr_stock_tick(price)
        units = Decimal(str(price)) / tick
        rounded = math.floor(units) if side == "BUY" else math.ceil(units)
        return float(rounded * tick)

    def valid_quote_level(self, price: float, volume: float) -> bool:
        """KR ordinary-stock depth uses the native tick grid and whole shares."""
        if self.code != "KR":
            return self.code == "US"
        return (
            math.isfinite(price)
            and price > 0
            and price % kr_stock_tick(price) == 0
            and float(volume).is_integer()
            and volume >= 0
        )

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


def kr_stock_tick(price: float | Decimal) -> int:
    """KRX ordinary-stock tick sizes, shared by the modeled KR continuous market."""
    for ceiling, tick in (
        (2_000, 1),
        (5_000, 5),
        (20_000, 10),
        (50_000, 50),
        (200_000, 100),
        (500_000, 500),
    ):
        if price < ceiling:
            return tick
    return 1_000


def us_limit_price(price: float) -> float:
    """Truncate a US quote to the documented limit-price precision."""
    if not math.isfinite(price) or price <= 0:
        raise ValueError("US limit price must be positive and finite")
    precision = 2 if price >= 1 else 4
    decimal_price = Decimal(str(price))
    quantum = Decimal(1).scaleb(-precision)
    with localcontext() as context:
        context.prec = max(
            28, len(decimal_price.as_tuple().digits) + abs(decimal_price.adjusted()) + 4
        )
        return float(decimal_price.quantize(quantum, rounding=ROUND_DOWN))
