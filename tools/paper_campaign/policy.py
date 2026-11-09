"""Frozen paper strategy admission and schema-bound model selections."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any

from tools.paper_capacity import assess, strategies
from tools.slot_control import encode
from tools.us_adaptive_ai import ENTRY_OPTIONS

BASELINE = {"policy": "trend", "selection": "all", "allocation": "equal"}
CAPITALS = {"KR": 1_000_000, "US": 1_000}
MIN_QUALIFIED_SESSIONS = 3
MIN_CLOSED_TRADES = 3
POLICY_VERSION = "campaign-v1"


def fingerprint(value: Any) -> str:
    return hashlib.sha256(encode(value).encode()).hexdigest()


@dataclass(frozen=True)
class Admission:
    max_slots: int
    candidates: dict[str, dict[str, str]]
    evidence_digest: str

    @classmethod
    def load(cls, report: dict[str, Any], market: str, source_digest: str) -> Admission:
        if (
            not isinstance(report, dict)
            or report.get("market") != market
            or market not in CAPITALS
            or not isinstance(source_digest, str)
            or not source_digest.strip()
            or report.get("source_digest") != source_digest
            or report.get("qualification") != "recorded_feed_capacity"
            or not isinstance(report.get("input_digest"), str)
            or len(report["input_digest"]) != 64
            or any(character not in "0123456789abcdef" for character in report["input_digest"])
            or not isinstance(report.get("machine"), str)
            or not report["machine"].strip()
            or report.get("real_orders") is not False
        ):
            raise ValueError("Capacity evidence does not match the approved market and source")
        limit, candidates = report.get("max_slots"), report.get("candidates")
        measurements = report.get("measurements")
        budget = numeric(report.get("latency_budget_seconds"))
        if (
            type(limit) is not int
            or limit < 1
            or not isinstance(candidates, list)
            or not isinstance(measurements, list)
            or budget is None
            or budget <= 0
            or assess(measurements, budget) != limit
            or limit > len(candidates)
        ):
            raise ValueError("Capacity evidence has no reliable slots")
        configs: dict[str, dict[str, str]] = {}
        for config in candidates:
            if (
                not isinstance(config, dict)
                or config.keys() != ENTRY_OPTIONS.keys()
                or any(config[key] not in choices for key, choices in ENTRY_OPTIONS.items())
            ):
                raise ValueError("Capacity report has an invalid strategy")
            slot_id = "-".join(config[key] for key in ENTRY_OPTIONS)
            if slot_id in configs:
                raise ValueError("Capacity report repeats a strategy")
            configs[slot_id] = config
        if not configs:
            raise ValueError("Capacity report has no eligible strategy")
        if candidates != strategies():
            raise ValueError("Capacity strategies do not match the measured candidate order")
        return cls(limit, configs, fingerprint(report))

    def initial(self) -> list[str]:
        return list(self.candidates)[: self.max_slots]


def next_plan(
    review: dict[str, Any],
    previous: list[str],
    admission: Admission,
    statistics: dict[str, Any],
    carried: set[str],
    retired: set[str] | None = None,
) -> list[str]:
    chosen = review["next_slots"]
    chosen_ids = set(chosen)
    eligible = (admission.candidates.keys() | carried) - (retired or set())
    if (
        not 1 <= len(chosen) <= admission.max_slots
        or len(chosen_ids) != len(chosen)
        or not chosen_ids <= eligible
        or admission.initial()[0] not in chosen
    ):
        raise ValueError("Review returned an invalid capacity-bounded plan")
    previous_ids = set(previous)
    for slot in previous_ids - chosen_ids:
        stats = statistics.get(slot, {})
        if (
            stats.get("has_exposure", True)
            or stats.get("qualified_sessions", 0) < MIN_QUALIFIED_SESSIONS
            or stats.get("qualified_closed_trades", 0) < MIN_CLOSED_TRADES
        ):
            raise ValueError("Review tried to remove a protected or insufficiently tested slot")
    untested = {
        slot
        for slot in eligible
        if statistics.get(slot, {}).get("qualified_sessions", 0) < MIN_QUALIFIED_SESSIONS
    }
    if untested - chosen_ids and (
        len(chosen) < admission.max_slots
        or (chosen_ids != previous_ids and not (chosen_ids - previous_ids) & untested)
    ):
        raise ValueError("Review skipped eligible newcomer exposure")
    return chosen


def numeric(value: Any) -> float | None:
    if type(value) not in (int, float):
        return None
    try:
        if not math.isfinite(value):
            return None
    except OverflowError:
        return None
    return float(value)
