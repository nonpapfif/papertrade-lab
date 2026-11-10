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

from papertrade.api import ApiError, private_json
from tools.adaptive_journal import AdaptiveJournal, _entries, encode
from tools.autotest_jobs import atomic_json
from tools.autotest_supervisor import tree_digest
from tools.market_data_gateway import MarketDataGateway
from tools.market_data_repository import DataGroup, MarketDataRepository
from tools.run_async_live_validation import MARKETS, Runtime, subscription_set
from tools.runtime_audit import RuntimeAudit
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
    if type(budget_seconds) not in (int, float) or budget_seconds <= 0:
        return 0
    try:
        if not math.isfinite(budget_seconds):
            return 0
    except OverflowError:
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


def _rss_bytes(who: int) -> int:
    rss = resource.getrusage(who).ru_maxrss
    return rss * 1024 if platform.system() == "Linux" else rss


class RecordedStream:
    def fetch(self, group: DataGroup, symbols: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        raise RuntimeError("Recorded stream measurements cannot fetch external market data")


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
            datetime.fromisoformat(row["received"]).timestamp()
            for row in rows
            if row["kind"] == "book"
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
                closing(MarketDataRepository(root / "market-data.sqlite3")) as repository,
            ):
                audit = RuntimeAudit(root / "runtime-audit.sqlite3", "recorded-capacity")
                gateway = MarketDataGateway(repository, RecordedStream(), audit)

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
                    runtime, subscription_set, lambda *a, **k: None, journal, control,
                    concurrent_accounts=True, audit=audit,
                )
                sequence = 0
                spent = 0.0
                backlog = ArrivalBacklog()
                timings = []
                covered = True
                gateway_rejected = 0
                last_status_at = -math.inf
                account_evidence: dict[str, dict[str, Any]] = {}
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
                        started = time.perf_counter()
                        entry = copy.deepcopy(row)
                        rejected = False
                        if row["kind"] == "book":
                            symbol, payload = row["payload"]
                            try:
                                observation = gateway.ingest_book(
                                    self.market, symbol, payload,
                                    received_time=row["received"], source_identity="recorded-stream",
                                )
                            except ApiError:
                                gateway_rejected += 1
                                rejected = True
                            else:
                                entry["payload"] = [symbol, observation.payload]
                        if not rejected:
                            sequence += 1
                            await slots._apply({**entry, "sequence": sequence}, 0)
                        if row["kind"] == "book" and not rejected:
                            for slot_id, slot in slots.slots.items():
                                for name, result in slot.runtime.last_account_results.items():
                                    if result.accepted_mark is not None:
                                        evidence = account_evidence.setdefault(
                                            f"{slot_id}.{name}", {"accepted_books": 0}
                                        )
                                        evidence["accepted_books"] += 1
                                        evidence["last_accepted_mark"] = result.accepted_mark
                        at = datetime.fromisoformat(row["received"]).timestamp()
                        if at >= first and at - last_status_at >= 1:
                            private_json(root / "status.json", slots.status(row["at"]))
                            last_status_at = at
                        duration = time.perf_counter() - started
                        if at >= first:
                            spent += duration
                            timings.append(duration)
                            backlog.observe(at, duration)
                        covered = covered and slots.protected().issubset(set(slots.desired))
                    status = slots.status(rows[-1]["at"])
                    for slot in status["shared_slots"]:
                        for name, details in slot["comparison"]["accounts"].items():
                            row = account_evidence.setdefault(
                                f"{slot['slot_id']}.{name}", {"accepted_books": 0}
                            )
                            paper = details.get("paper") or {}
                            row["fee_inclusive_account"] = paper.get("account")
                            row["outcomes"] = paper.get("outcomes", [])
                    accepted = min(row["accepted_books"] for row in account_evidence.values())
                    features = min(
                        sum(value is not None for value in slot.runtime.features.values())
                        for slot in slots.slots.values()
                    )
                    aligned = all(
                        row["comparison"]["inputs_aligned"] for row in status["shared_slots"]
                    )
                    ordered = sorted(timings)
                    measurement: dict[str, Any] = {
                        "slots": count,
                        "accounts": count * 2,
                        "events": len(rows),
                        "observed_seconds": last - first,
                        "processing_seconds": spent,
                        "max_backlog_seconds": backlog.maximum,
                        "p99_event_seconds": ordered[math.ceil(len(ordered) * 0.99) - 1],
                        "peak_rss_bytes": _rss_bytes(resource.RUSAGE_SELF),
                        "inputs_aligned": aligned,
                        "accepted_books_per_account": accepted,
                        "valid_features_per_slot": features,
                        "subscriptions_covered": covered,
                        "gateway_rejected_books": gateway_rejected,
                        "market_data_records": repository.cut(rows[-1]["received"]).sequence,
                        "journal": journal.evidence(),
                        "account_execution": status["account_execution"],
                        "account_evidence": account_evidence,
                    }
                finally:
                    await slots.close()
                    await gateway.close()
                child_rss = _rss_bytes(resource.RUSAGE_CHILDREN)
                measurement["reaped_children_max_rss_bytes"] = child_rss
                return measurement

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
            "runtime_path": "recorded_stream_gateway_repository_concurrent_accounts_journal_status",
            "real_orders": False,
            "limits": [
                "Recorded workload only; live coverage and arrival rate may differ.",
                "RSS reports separate process-lifetime parent and reaped-child high-water marks.",
                "The sum is not a simultaneous whole-runtime memory measurement.",
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
    source = tree_digest(Path.cwd())
    try:
        rows, identity = workload(args.journal, args.window_seconds, args.market)
        benchmark = CapacityBenchmark(args.market, args.capital, args.latency_budget_seconds)
        result = asyncio.run(benchmark.run(rows, identity, source))
        if tree_digest(Path.cwd()) != source:
            raise RuntimeError("Capacity source changed during measurement")
    except (ApiError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error) as error:
        atomic_json(args.output, {
            "state": "incomplete",
            "market": args.market,
            "source_digest": source,
            "reason": type(error).__name__,
            "real_orders": False,
        })
        raise
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
