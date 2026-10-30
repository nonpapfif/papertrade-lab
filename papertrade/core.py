"""Configuration, numeric validation, and market timestamps."""

from __future__ import annotations

import json
import subprocess
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

SEOUL = ZoneInfo("Asia/Seoul")
NEW_YORK = ZoneInfo("America/New_York")
ZERO = Decimal(0)
ONE = Decimal(1)


class SafetyError(Exception):
    """Reject unsafe data or a request outside the paper contract."""


def decimal(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise SafetyError("Invalid decimal value") from error
    if not result.is_finite():
        raise SafetyError("Non-finite numeric value")
    return result


def stamp(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise SafetyError("Market timestamps must include a timezone")
    return result.astimezone(UTC)


def now_utc() -> datetime:
    return datetime.now(UTC)


def process_identity(pid: int) -> str | None:
    result = subprocess.run(
        ["/bin/ps", "-p", str(pid), "-o", "lstart="],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    return result.stdout.strip() or None


def encode(value: Any) -> str:
    def default(item: Any) -> str:
        if isinstance(item, (Decimal, datetime)):
            return str(item) if isinstance(item, Decimal) else item.isoformat()
        raise TypeError(type(item).__name__)

    return json.dumps(value, default=default, ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class Settings:
    root: Path
    data: dict[str, Any]

    @classmethod
    def load(cls, root: Path) -> Settings:
        root = root.resolve()
        data = tomllib.loads((root / "settings.toml").read_text())
        result = cls(root, data)
        if data.get("simulation", {}).get("enabled"):
            if decimal(data["simulation"]["initial_usd"]) != 1000:
                raise SafetyError("The original virtual capital must remain USD 1000")
            if decimal(data["limits"]["minimum_request_interval"]) <= 0:
                raise SafetyError("API pacing must be positive")
            return result
        if decimal(result.get("strategy", "initial_usd")) != 1000:
            raise SafetyError("This project must start with exactly USD 1000")
        if data["ai"].get("model_selection", "current") not in ("auto", "current"):
            raise SafetyError("Unknown AI model selection policy")
        for field in ("us_fee_bps", "kr_fee_bps", "kr_sell_tax_bps", "kr_fx_bps"):
            if not ZERO <= decimal(result.get("costs", field)) < 10000:
                raise SafetyError("Paper costs must be nonnegative and below 10000 basis points")
        if result.fee_rate("KR", selling=True) >= ONE:
            raise SafetyError("Combined paper sale costs must be below the sale proceeds")
        for field in ("atr_period", "time_exit_minutes", "entry_cutoff_minutes", "flatten_minutes"):
            value = result.get("strategy", field)
            if type(value) is not int or value < 0 or (field == "atr_period" and value == 0):
                raise SafetyError("Strategy periods must be valid nonnegative integers")
        for field in ("atr_multiple", "reward_multiple", "volume_multiple"):
            if decimal(result.get("strategy", field)) <= 0:
                raise SafetyError("Strategy multipliers must be positive")
        minutes = data["strategy"].get("bar_minutes", 30)
        lookback = data["strategy"].get("breakout_bars", 6)
        relative = data["strategy"].get("relative_minutes", 60)
        if (
            type(minutes) is not int
            or minutes <= 0
            or 60 % minutes
            or type(lookback) is not int
            or lookback <= 0
            or type(relative) is not int
            or relative <= 0
            or relative % minutes
        ):
            raise SafetyError("Bar and comparison periods must use complete aligned intervals")
        if (
            decimal(result.get("strategy", "chase_atr")) < 0
            or not ZERO <= decimal(result.get("strategy", "max_spread_fraction")) < ONE
        ):
            raise SafetyError("Invalid entry validation settings")
        if (
            decimal(result.get("limits", "minimum_request_interval")) <= 0
            or decimal(result.get("limits", "quote_age_seconds")) <= 0
        ):
            raise SafetyError("Data validity and API pacing settings must be positive")
        return result

    def get(self, section: str, field: str) -> Any:
        return self.data[section][field]

    @property
    def state_dir(self) -> Path:
        return self.root / "state"

    @property
    def secrets_dir(self) -> Path:
        return Path.home() / ".config" / "secrets"

    def fee_rate(self, market: str, selling: bool = False) -> Decimal:
        rate = decimal(self.get("costs", "us_fee_bps" if market == "US" else "kr_fee_bps"))
        if market == "KR":
            rate += decimal(self.get("costs", "kr_fx_bps"))
            if selling:
                rate += decimal(self.get("costs", "kr_sell_tax_bps"))
        return rate / 10000


@dataclass(frozen=True)
class Session:
    market: str
    date: str
    start: datetime
    end: datetime
    auction_start: datetime | None = None

    def contains(self, at: datetime) -> bool:
        return self.start <= at < self.end

    def entry_allowed(self, at: datetime, cutoff: int) -> bool:
        return self.can_fill(at) and at < self.end - timedelta(minutes=cutoff)

    def can_fill(self, at: datetime) -> bool:
        return self.start <= at < (self.auction_start or self.end)


def regular_session(market: str, day: dict[str, Any]) -> Session | None:
    sessions = day.get("integrated") if market == "KR" else day
    raw = sessions.get("regularMarket") if sessions else None
    if not raw:
        return None
    return Session(
        market,
        day["date"],
        stamp(raw["startTime"]),
        stamp(raw["endTime"]),
        stamp(raw["singlePriceAuctionStartTime"])
        if raw.get("singlePriceAuctionStartTime")
        else None,
    )


def market_date(market: str, at: datetime) -> str:
    return at.astimezone(SEOUL if market == "KR" else NEW_YORK).date().isoformat()


def probable_regular(market: str, at: datetime) -> bool:
    local = at.astimezone(SEOUL if market == "KR" else NEW_YORK)
    minute = local.hour * 60 + local.minute
    # A broad lookup window keeps delayed opens/closes under calendar control.
    return local.weekday() < 5 and 480 <= minute < 1080
