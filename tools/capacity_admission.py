"""Validate recorded capacity evidence without importing the live runner."""

from __future__ import annotations

import math
from typing import Any, cast


def finite_number(value: object) -> float | None:
    if type(value) not in (int, float):
        return None
    number = cast(int | float, value)
    try:
        if not math.isfinite(number):
            return None
    except OverflowError:
        return None
    return float(number)


def _valid_measurement(sample: Any) -> bool:
    if not isinstance(sample, dict) or type(sample.get("slots")) is not int:
        return False
    if sample["slots"] < 1:
        return False
    if (
        sample.get("inputs_aligned") is False
        and sample.get("subscriptions_covered") is False
        and sample
        == {
            "slots": sample["slots"],
            "inputs_aligned": False,
            "subscriptions_covered": False,
            "reason": "Protected positions exceed one connection capacity",
        }
    ):
        return True
    for field in (
        "accounts",
        "events",
        "accepted_books_per_account",
        "valid_features_per_slot",
        "peak_rss_bytes",
    ):
        value = sample.get(field)
        if type(value) is not int or value < 0:
            return False
    for field in (
        "observed_seconds",
        "processing_seconds",
        "max_backlog_seconds",
        "p99_event_seconds",
    ):
        value = sample.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            return False
        try:
            if not math.isfinite(value):
                return False
        except OverflowError:
            return False
    return (
        sample["accounts"] == 2 * sample["slots"]
        and sample["events"] >= 2
        and sample["observed_seconds"] > 0
        and type(sample.get("inputs_aligned")) is bool
        and type(sample.get("subscriptions_covered")) is bool
    )


def assess(samples: list[dict[str, Any]], budget_seconds: float) -> int:
    """Return the largest contiguous tested count satisfying the frozen latency contract."""
    budget = finite_number(budget_seconds)
    if budget is None or budget <= 0:
        return 0
    if not isinstance(samples, list) or not all(_valid_measurement(row) for row in samples):
        return 0
    maximum = 0
    for sample in samples:
        if (
            sample["slots"] != maximum + 1
            or not sample["inputs_aligned"]
            or not sample["subscriptions_covered"]
            or sample["max_backlog_seconds"] > budget_seconds
            or sample["processing_seconds"] > sample["observed_seconds"]
        ):
            break
        if sample["accepted_books_per_account"] < 1 or sample["valid_features_per_slot"] < 1:
            break
        maximum = sample["slots"]
    return maximum
