"""Extend a finished KR allocation comparison on an existing shared market feed."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
from collections import Counter
from datetime import datetime
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any

from papertrade.clock import now_utc as now
from papertrade.core import Settings
from papertrade.jsonio import atomic_json
from papertrade.jsonio import paper_json as encode
from tools.autotest_jobs import poll_job
from tools.kr_comparison_ai import SOL, review_cascade
from tools.kr_comparison_events import write_event
from tools.kr_whole_share_accounts import seed_accounts
from tools.run_kr_paper_comparison import ALLOCATION_LOOKBACK, ROOT, Comparison


def reviewable(pair: Comparison) -> list[dict[str, Any]]:
    ai = pair.accounts["ai"]
    signals = [
        signal
        for signal in pair.review_queue.values()
        if ai.wants(signal) and (signal["action"] == "SELL" or ai.cash > 0)
    ]
    pair.review_queue.clear()
    return signals


def summaries(pairs: dict[str, Comparison]) -> dict[str, Any]:
    return json.loads(
        encode(
            {
                f"{key}_{name}": account.summary(pair.prices)
                for key, pair in pairs.items()
                for name, account in pair.accounts.items()
            }
        )
    )


class Replay:
    def __init__(self, source: Path) -> None:
        self.status = json.loads((source / "status.json").read_text())
        self.pairs = {"5pct": Comparison(Decimal("0.05")), "10pct": Comparison(Decimal("0.10"))}
        self.calls: dict[str, Counter[str]] = {key: Counter() for key in self.pairs}
        self.completed: dict[str, Counter[str]] = {key: Counter() for key in self.pairs}
        self.digest = hashlib.sha256()
        self.sweeps = self.status["sweeps"]
        self.last_observed: datetime | None = None
        updated = datetime.fromisoformat(self.status["updated_at"])
        audit = []
        with (source / "events.jsonl").open("rb") as events:
            for line in events:
                if not line.endswith(b"\n"):
                    break
                event = json.loads(line)
                if datetime.fromisoformat(event["recorded_at"]) >= updated:
                    break
                audit.append(event)
        received_at = {
            item["observed_at"]: datetime.fromisoformat(item["recorded_at"])
            for item in audit
            if item["kind"] == "shared_observation"
        }
        ai_events = iter(event for event in audit if event["kind"].startswith("ai_"))
        pending: dict[str, tuple[int, list[dict[str, Any]]]] = {}
        awaiting_review: dict[str, list[dict[str, Any]]] = {}
        fills: list[dict[str, Any]] = []
        event = next(ai_events, None)

        def advance(at: datetime) -> None:
            nonlocal event
            while event is not None and datetime.fromisoformat(event["recorded_at"]) < at:
                key = event["allocation"]
                pair = self.pairs[key]
                kind = event["kind"]
                if kind == "ai_review":
                    signals = event["signals"]
                    if (
                        event.get("parent_review_id") is None
                        and json.loads(encode(awaiting_review.pop(key))) != signals
                    ):
                        raise ValueError("Replayed review request differs")
                    if (
                        json.loads(encode(pair.accounts["ai"].summary(pair.prices)))
                        != event["portfolio"]
                    ):
                        raise ValueError("Replayed review portfolio differs")
                    pending[key] = (
                        event["review_id"],
                        [
                            {
                                **{
                                    name: value
                                    for name, value in sig.items()
                                    if name != "sol_reason"
                                },
                                "strength": Decimal(sig["strength"]),
                            }
                            for sig in signals
                        ],
                    )
                    self.calls[key][event["model"]] += 1
                elif kind == "ai_decision":
                    review_id, review_signals = pending.pop(key)
                    if review_id != event["review_id"]:
                        raise ValueError("Mismatched review decision")
                    if not event["after_cutoff"]:
                        pair.approve(
                            review_signals,
                            event["approvals"],
                            datetime.fromisoformat(event["decision_at"]),
                            {symbol: Decimal(price) for symbol, price in event["watches"].items()},
                        )
                        self.completed[key][event["model"]] += 1
                elif kind in ("ai_cancelled_at_end", "ai_review_failed"):
                    if pending.pop(key)[0] != event["review_id"]:
                        raise ValueError("Mismatched ended review")
                elif kind != "ai_handoff_not_started":
                    raise ValueError("Unexpected AI audit event")
                event = next(ai_events, None)

        with (source / "market.jsonl").open("rb") as market:
            for sweep in range(1, self.sweeps + 1):
                line = market.readline()
                self.digest.update(line)
                snapshot = json.loads(line)
                at = datetime.fromisoformat(snapshot["observed_at"])
                advance(received_at.get(snapshot["observed_at"], at))
                self.last_observed = at
                for key, pair in self.pairs.items():
                    for fill in pair.observe(
                        snapshot["rows"], at, warmup=sweep <= ALLOCATION_LOOKBACK
                    ):
                        fills.append({**fill, "account": f"{key}_{fill['account']}"})
                    if key not in pending:
                        awaiting_review[key] = reviewable(pair)
            if self.status["state"] != "running" and market.read(1):
                raise ValueError("Unexpected market observations after final status")
        advance(updated)
        expected_fills = [
            {key: value for key, value in item.items() if key not in ("kind", "recorded_at")}
            for item in audit
            if item["kind"] == "fill"
        ]
        if (
            event is not None
            or self.digest.hexdigest() != self.status["shared_input_sha256"]
            or summaries(self.pairs) != self.status["accounts"]
            or json.loads(encode(fills)) != expected_fills
            or self.review_counts() != self.status["ai_by_allocation"]
            or (self.status["state"] != "running" and pending)
        ):
            raise ValueError("Historical replay differs from saved comparison")

    def review_counts(self) -> dict[str, Any]:
        return {
            key: {"calls": self.calls[key], "completed": self.completed[key]} for key in self.pairs
        }


async def continue_campaign(
    job: Path,
    feed: Path,
    output: Path,
    until: datetime,
    *,
    recover_expired_cache: bool = False,
    whole_share_source: Path | None = None,
    whole_share_listings: Path | None = None,
) -> None:
    while (exit_record := poll_job(job)) is None:
        if now() >= until:
            raise ValueError("Original comparison did not finish within extension window")
        await asyncio.sleep(0.5)
    source = whole_share_source or job / "run"
    replay = Replay(source)
    initial = replay.status
    cutoff = datetime.fromisoformat(initial["until"])
    completed = exit_record["returncode"] == 0 and initial["state"] == "complete"
    recovered = (
        recover_expired_cache
        and exit_record["returncode"] != 0
        and initial["state"] == "failed"
        and initial["error_type"] == "SafetyError"
        and (job / "stderr.log")
        .read_text()
        .rstrip()
        .endswith("SafetyError: A valid existing cached token is required")
    )
    whole_share = whole_share_source is not None and whole_share_listings is not None
    stopped_for_handoff = (
        whole_share and exit_record["returncode"] in (-2, 130) and initial["state"] == "incomplete"
    )
    if stopped_for_handoff and replay.last_observed is not None:
        cutoff = replay.last_observed
    if (
        not (completed or recovered or stopped_for_handoff)
        or replay.last_observed is None
        or not replay.last_observed <= cutoff <= now() < until
    ):
        raise ValueError("A verified completed comparison is required")
    manifest = json.loads((source / "manifest.json").read_text())
    for filename in ("run_kr_paper_comparison.py", "kr_comparison_ai.py"):
        if (
            hashlib.sha256((ROOT / "tools" / filename).read_bytes()).hexdigest()
            != manifest["sources"][filename]
        ):
            raise ValueError("Original strategy source differs")
    settings = Settings.load(ROOT)
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(source / "market.jsonl", output / "market.jsonl")
    if whole_share_listings is None:
        shutil.copyfile(source / "events.jsonl", output / "events.jsonl")
        seed_fills = None
    else:
        seed_fills = seed_accounts(replay.pairs, source / "events.jsonl", whole_share_listings)
        (output / "events.jsonl").touch(exist_ok=False)
    atomic_json(
        output / "manifest.json",
        {
            **manifest,
            "until": until.isoformat(),
            "extension": {
                "previous_run": str(source),
                "quantity_policy": "Whole shares only" if whole_share else "Legacy fractional",
                "whole_share_seed_fills": seed_fills,
                "legacy_fractional_scope": (
                    "Superseded for Korean execution realism; historical evidence only."
                    if whole_share
                    else None
                ),
                "whole_share_seed_assumption": (
                    "Reexecution of legacy proposals with fee-constrained integer quantities; "
                    "legacy proposals and strategy decisions are retained in previous_run."
                    if whole_share
                    else None
                ),
                "listing_sha256": (
                    hashlib.sha256(whole_share_listings.read_bytes()).hexdigest()
                    if whole_share_listings is not None
                    else None
                ),
                "previous_until": initial["until"],
                "previous_sweeps": replay.sweeps,
                "previous_state": initial["state"],
                "previous_error_type": initial["error_type"],
                "previous_last_observation": replay.last_observed.isoformat(),
                "recovered_expired_cache": recovered,
                "previous_input_sha256": initial["shared_input_sha256"],
                "shared_market_feed": str(feed),
                "continuation_source_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
            },
        },
    )
    jobs: dict[str, asyncio.Task[None]] = {}
    prior_last_observed = replay.last_observed
    first_observed: datetime | None = None
    added_sweeps = 0
    state = "running"
    error = None

    def status() -> None:
        report = {
            **initial,
            "state": state,
            "complete": state == "complete",
            "error_type": error,
            "until": until.isoformat(),
            "updated_at": now().isoformat(),
            "sweeps": replay.sweeps + added_sweeps,
            "extension_sweeps": added_sweeps,
            "quantity_policy": "Whole shares only" if whole_share else "Legacy fractional",
            "whole_share_seed_fills": seed_fills,
            "recovered_expired_cache": recovered or initial.get("recovered_expired_cache", False),
            "previous_last_observation": prior_last_observed.isoformat(),
            "first_extension_observation": first_observed.isoformat() if first_observed else None,
            "shared_input_sha256": replay.digest.hexdigest(),
            "observed_symbols": len(next(iter(replay.pairs.values())).prices),
            "accounts": summaries(replay.pairs),
            "ai_calls": sum(count.total() for count in replay.calls.values()),
            "ai_reviews_completed": sum(count.total() for count in replay.completed.values()),
            "ai_by_allocation": replay.review_counts(),
            "ai_in_flight": any(not task.done() for task in jobs.values()),
        }
        atomic_json(output / "status.json", report)

    with (
        (output / "market.jsonl").open("ab", buffering=0) as market,
        (output / "events.jsonl").open("a", buffering=1) as events,
    ):

        record = partial(write_event, events, now)

        async def review(key: str, signals: list[dict[str, Any]]) -> None:
            def audit(kind: str, **fields: Any) -> None:
                record(kind, allocation=key, **fields)

            await review_cascade(
                settings,
                replay.pairs[key],
                signals,
                until,
                audit,
                replay.calls[key],
                replay.completed[key],
            )

        def read_feed_status() -> dict[str, Any]:
            return json.loads((feed.parent / "status.json").read_text())

        def process(line: bytes) -> None:
            nonlocal added_sweeps, first_observed
            snapshot = json.loads(line)
            observed = datetime.fromisoformat(snapshot["observed_at"])
            received = now()
            if (
                replay.last_observed is None
                or not replay.last_observed < observed
                or not cutoff <= observed < until
                or observed > received
            ):
                raise ValueError("Shared observation falls outside the extension")
            replay.last_observed = observed
            if first_observed is None:
                first_observed = observed
            market.write(line)
            replay.digest.update(line)
            added_sweeps += 1
            record("shared_observation", observed_at=observed, received_at=received)
            for key, pair in replay.pairs.items():
                for fill in pair.observe(snapshot["rows"], observed):
                    record("fill", **{**fill, "account": f"{key}_{fill['account']}"})
                task = jobs.get(key)
                if task is not None and not task.done():
                    continue
                if task is not None:
                    task.result()
                    del jobs[key]
                signals = reviewable(pair)
                if signals and now() < until:
                    jobs[key] = asyncio.create_task(review(key, signals))
            status()

        async def finish_reviews() -> None:
            nonlocal state, error
            for key, task in jobs.items():
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    record(
                        "ai_cancelled_at_end", allocation=key, review_id=replay.calls[key].total()
                    )
                elif not task.cancelled() and (failure := task.exception()) is not None:
                    state, error = "failed", type(failure).__name__
                    record("ai_review_failed", allocation=key, error_type=error)
            jobs.clear()

        try:
            record("extension_started", previous_cutoff=cutoff, extension_cutoff=until)
            status()
            while not feed.exists() and now() < until:
                if read_feed_status()["state"] == "failed":
                    raise RuntimeError("Shared market feed failed before handoff")
                await asyncio.sleep(0.5)
            with feed.open("rb") as reader:
                while True:
                    offset = reader.tell()
                    line = reader.readline()
                    if line.endswith(b"\n"):
                        if (
                            whole_share
                            and datetime.fromisoformat(json.loads(line)["observed_at"]) <= cutoff
                        ):
                            continue
                        process(line)
                        await asyncio.sleep(0)
                        continue
                    reader.seek(offset)
                    feed_state = read_feed_status()["state"]
                    if feed_state == "failed":
                        raise RuntimeError("Shared market feed failed")
                    if feed_state in ("complete", "incomplete") or now() >= until:
                        break
                    await asyncio.sleep(0.5)
            while now() < until:
                await asyncio.sleep(min(0.5, (until - now()).total_seconds()))
            await finish_reviews()
            # The final API sweep may still be returning. The producer discards data at cutoff.
            while (feed_exit := poll_job(feed.parent / "job")) is None:
                await asyncio.sleep(0.5)
            feed_report = read_feed_status()
            if state == "running":
                state = (
                    "complete"
                    if feed_exit["returncode"] == 0
                    and feed_report["state"] == "complete"
                    and added_sweeps > 0
                    and (
                        added_sweeps + initial.get("extension_sweeps", 0)
                        if whole_share
                        else added_sweeps
                    )
                    == feed_report["counts"]["standalone"]
                    and all(count[SOL] for count in replay.completed.values())
                    else "incomplete"
                )
        except (OSError, ValueError, RuntimeError, KeyError) as exc:
            state, error = "failed", type(exc).__name__
            record("failed", error_type=error)
            raise
        finally:
            await finish_reviews()
            if state == "running":
                state = "incomplete"
            status()
            atomic_json(
                output / "comparison.json", json.loads((output / "status.json").read_text())
            )


def main() -> None:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", required=True, type=Path)
    parser.add_argument("--feed", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--until", type=datetime.fromisoformat)
    parser.add_argument("--verify-prefix", action="store_true")
    parser.add_argument("--recover-expired-cache", action="store_true")
    parser.add_argument("--whole-share-source", type=Path)
    parser.add_argument("--whole-share-listings", type=Path)
    args = parser.parse_args()
    job = args.job.resolve()
    if args.verify_prefix:
        replay = Replay(job / "run")
        print(encode({"verified_sweeps": replay.sweeps, "sha256": replay.digest.hexdigest()}))
        return
    if args.feed is None or args.output is None or args.until is None or args.until.tzinfo is None:
        parser.error("--feed, --output and timezone-aware --until are required")
    if (args.whole_share_source is None) != (args.whole_share_listings is None):
        parser.error("--whole-share-source and --whole-share-listings must be supplied together")
    asyncio.run(
        continue_campaign(
            job,
            args.feed.resolve(),
            args.output.resolve(),
            args.until,
            recover_expired_cache=args.recover_expired_cache,
            whole_share_source=args.whole_share_source,
            whole_share_listings=args.whole_share_listings,
        )
    )


if __name__ == "__main__":
    main()
