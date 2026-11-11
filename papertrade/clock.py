"""UTC wall-clock timestamps shared by market-data and paper workflows."""

from datetime import UTC, datetime


def now_utc() -> datetime:
    return datetime.now(UTC)


def utc_iso() -> str:
    return now_utc().isoformat()
