"""Measure shared-feed paper capacity against a recorded, source-bound workload."""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import itertools
import json
import math
import platform
import resource
import sqlite3
import tempfile
import time
from collections import deque
from contextlib import closing
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from tools.adaptive_journal import AdaptiveJournal, _entries, encode
from tools.autotest_jobs import atomic_json
from tools.autotest_supervisor import tree_digest
from tools.run_async_live_validation import MARKETS, Runtime, subscription_set
from tools.shared_feed_slots import SharedFeedSlots
from tools.slot_control import SlotCommand, SlotControl
from tools.us_adaptive_ai import ENTRY_OPTIONS


def strategies() -> list[dict[str, str]]:
    """Enumerate distinct configurations the current engine actually supports."""
    return [
        dict(zip(ENTRY_OPTIONS, values, strict=True))
        for values in itertools.product(*ENTRY_OPTIONS.values())
    ]


def workload(path: Path, window_seconds: float, market: str) -> tuple[list[dict[str, Any]], str]:
    """Keep seed observations and a bounded trailing measurement interval, not account state."""
    if not math.isfinite(window_seconds) or window_seconds <= 0:
        raise ValueError("Measurement window must be positive")
    recent: deque[dict[str, Any]] = deque()
    seeds: dict[str, dict[str, Any]] = {}
    last_book: float | None = None
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        identity = json.loads(db.execute("SELECT identity FROM header").fetchone()[0])
        if (
            identity.get("market") != market
            or identity.get("currency") != MARKETS[market].currency
            or identity.get("profile") != "broker-constrained"
        ):
            raise ValueError("Recorded workload has another market or execution profile")
        for event in _entries(db):
            if event["kind"] not in ("calendar", "selection", "feature", "book", "clock", "fx"):
                continue
            row = {
                "kind": event["kind"],
                "payload": event["payload"],
                "at": event.get("at", event["received"]),
                "received": event["received"],
            }
            recent.append(row)
            if row["kind"] == "book":
                last_book = datetime.fromisoformat(row["received"]).timestamp()
            if last_book is None:
                cutoff = datetime.fromisoformat(row["received"]).timestamp()
            else:
                cutoff = last_book - window_seconds
            while recent and datetime.fromisoformat(recent[0]["received"]).timestamp() < cutoff:
                prior = recent.popleft()
                if prior["kind"] in ("calendar", "selection", "fx", "feature"):
                    key = prior["kind"]
                    if key == "feature":
                        key += ":" + prior["payload"][0]
                    seeds[key] = prior
    if last_book is None:
        raise ValueError("Capacity requires recorded book observations")
    sample = list(recent)
    selected = sorted(seeds.values(), key=lambda row: row["at"]) + sample
    if len([row for row in sample if row["kind"] == "book"]) < 2:
        raise ValueError("Capacity requires multiple observed books within the measurement window")
    return selected, hashlib.sha256(encode(selected).encode()).hexdigest()


class ArrivalBacklog:
    """Simulate service completion without carrying idle-time credit into later bursts."""

    def __init__(self) -> None:
        self.finished: float | None = None
        self.maximum = 0.0

    def observe(self, arrival: float, duration: float) -> None:
        self.finished = max(self.finished or arrival, arrival) + duration
        self.maximum = max(self.maximum, self.finished - arrival)


def assess(samples: list[dict[str, Any]], budget_seconds: float) -> int:
    """Return the largest contiguous tested count satisfying the frozen latency contract."""
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
        if (
            sample.get("accepted_books_per_account", 1) < 1
            or sample.get("valid_features_per_slot", 1) < 1
        ):
            break
        maximum = sample["slots"]
    return maximum


class CapacityBenchmark:
    def __init__(self, market: str, capital: float, latency_budget_seconds: float) -> None:
        if market not in MARKETS or not math.isfinite(capital) or capital <= 0:
            raise ValueError("Invalid market or capital")
        if not math.isfinite(latency_budget_seconds) or latency_budget_seconds <= 0:
            raise ValueError("Invalid latency budget")
        self.market = market
        self.capital = capital
        self.budget = latency_budget_seconds

    async def measure(self, rows: list[dict[str, Any]], count: int) -> dict[str, Any]:
        observed = [
            datetime.fromisoformat(row["received"]).timestamp() for row in rows if row["kind"] == "book"
        ]
        if len(observed) < 2 or max(observed) <= min(observed):
            raise ValueError("No measurable observation duration")
        first, last = min(observed), max(observed)
        candidates = strategies()[:count]
        baseline = candidates[0]
        with tempfile.TemporaryDirectory(prefix="paper-capacity-") as directory:
            root = Path(directory)
            with (
                AdaptiveJournal.open(
                    root / "journal.sqlite3", {"capacity": True}, resume=False
                ) as journal,
                SlotControl.open(root / "slots.sqlite3", initialize=True) as control,
            ):

                def runtime(capital: float, record: Any) -> Runtime:
                    return Runtime(
                        capital,
                        record,
                        profile="broker-constrained",
                        adaptive=True,
                        market=MARKETS[self.market],
                        end_epoch=last + 60,
                    )

                slots = SharedFeedSlots(
                    runtime, subscription_set, lambda *a, **k: None, journal, control
                )
                sequence = 0
                spent = 0.0
                backlog = ArrivalBacklog()
                timings = []
                covered = True
                try:
                    for index, candidate in enumerate(candidates):
                        command = SlotCommand(
                            f"capacity-{index}",
                            "add",
                            f"capacity-{index}",
                            {
                                "capital": self.capital,
                                "incumbent": baseline,
                                "candidate": candidate,
                            },
                        )
                        control.submit(command, end_epoch=time.time() + 3600)
                        sequence += 1
                        await slots._apply(
                            {
                                "sequence": sequence,
                                "kind": "slot_command",
                                "payload": asdict(command),
                                "at": rows[0]["at"],
                                "received": rows[0]["received"],
                            },
                            0,
                        )
                    for row in rows:
                        sequence += 1
                        started = time.perf_counter()
                        await slots._apply({**copy.deepcopy(row), "sequence": sequence}, 0)
                        duration = time.perf_counter() - started
                        at = datetime.fromisoformat(row["received"]).timestamp()
                        if at >= first:
                            spent += duration
                            timings.append(duration)
                            backlog.observe(at, duration)
                        covered = covered and slots.protected().issubset(set(slots.desired))
                    accepted = min(
                        delivered
                        for slot in slots.slots.values()
                        for delivered in slot.runtime.book_delivered.values()
                    )
                    features = min(
                        sum(value is not None for value in slot.runtime.features.values())
                        for slot in slots.slots.values()
                    )
                    aligned = all(
                        slot.runtime.status()["comparison"]["inputs_aligned"]
                        for slot in slots.slots.values()
                    )
                    ordered = sorted(timings)
                    return {
                        "slots": count,
                        "accounts": count * 2,
                        "events": len(rows),
                        "observed_seconds": last - first,
                        "processing_seconds": spent,
                        "max_backlog_seconds": backlog.maximum,
                        "p99_event_seconds": ordered[math.ceil(len(ordered) * 0.99) - 1],
                        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        "inputs_aligned": aligned,
                        "accepted_books_per_account": accepted,
                        "valid_features_per_slot": features,
                        "subscriptions_covered": covered,
                    }
                finally:
                    await slots.close()

    async def run(
        self, rows: list[dict[str, Any]], input_digest: str, source_digest: str
    ) -> dict[str, Any]:
        samples = []
        for count in range(1, len(strategies()) + 1):
            try:
                samples.append(await self.measure(rows, count))
            except ValueError as error:
                if str(error) != "Protected positions exceed one connection capacity":
                    raise
                samples.append(
                    {
                        "slots": count,
                        "inputs_aligned": False,
                        "subscriptions_covered": False,
                        "reason": str(error),
                    }
                )
            if assess(samples, self.budget) != count:
                break
        return {
            "market": self.market,
            "source_digest": source_digest,
            "input_digest": input_digest,
            "max_slots": assess(samples, self.budget),
            "candidates": strategies(),
            "measurements": samples,
            "latency_budget_seconds": self.budget,
            "machine": platform.platform(),
            "qualification": "recorded_feed_capacity",
            "real_orders": False,
            "limits": [
                "Recorded workload only; live coverage and arrival rate may differ.",
                "Two independent paper accounts per slot; not a broker execution test.",
            ],
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--market", choices=MARKETS, required=True)
    parser.add_argument("--capital", type=float, required=True)
    parser.add_argument("--window-seconds", type=float, default=60)
    parser.add_argument("--latency-budget-seconds", type=float, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows, identity = workload(args.journal, args.window_seconds, args.market)
    benchmark = CapacityBenchmark(args.market, args.capital, args.latency_budget_seconds)
    result = asyncio.run(benchmark.run(rows, identity, tree_digest(Path.cwd())))
    atomic_json(args.output, result)
    print(
        json.dumps(
            {
                "market": args.market,
                "max_slots": result["max_slots"],
                "qualification": result["qualification"],
            }
        )
    )


if __name__ == "__main__":
    main()
