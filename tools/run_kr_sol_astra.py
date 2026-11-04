"""Run a separate KRW paper account on new snapshots from a shared market recording."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from papertrade.core import SafetyError, Settings, encode
from tools.autotest_jobs import atomic_json
from tools.kr_comparison_ai import decide
from tools.run_kr_paper_comparison import CAPITAL, ROOT, Comparison, now

SOL = "gpt-6.1-sol"
ASTRA = "gpt-6-astra"


async def campaign(market: Path, output: Path, until: datetime) -> None:
    started = now()
    if started >= until:
        raise ValueError("Observation deadline has passed")
    settings = Settings.load(ROOT)
    output.mkdir(parents=True, exist_ok=False)
    comparison = Comparison()
    account = comparison.accounts["ai"]
    calls: Counter[str] = Counter()
    completed: Counter[str] = Counter()
    sweeps = 0
    state = "running"
    job: asyncio.Task[None] | None = None
    atomic_json(
        output / "manifest.json",
        {
            "input": str(market),
            "currency": "KRW",
            "initial_capital_krw": str(CAPITAL),
            "until": until.isoformat(),
            "models": [SOL, ASTRA],
            "effort": "medium",
            "observation_start": "new snapshots only",
        },
    )

    with market.open("rb") as source, (output / "events.jsonl").open("x", buffering=1) as events:
        partial_at_start = False
        if source.seek(0, os.SEEK_END):
            source.seek(-1, os.SEEK_END)
            partial_at_start = source.read(1) != b"\n"

        def record(kind: str, **fields: Any) -> None:
            events.write(encode({"kind": kind, "recorded_at": now(), **fields}) + "\n")

        def status() -> None:
            atomic_json(
                output / "status.json",
                json.loads(
                    encode(
                        {
                            "state": state,
                            "started_at": started,
                            "until": until,
                            "updated_at": now(),
                            "currency": "KRW",
                            "initial_capital_krw": CAPITAL,
                            "real_orders": False,
                            "sweeps": sweeps,
                            "observed_symbols": len(comparison.prices),
                            "ai_calls": dict(calls),
                            "ai_reviews_completed": dict(completed),
                            "ai_in_flight": job is not None and not job.done(),
                            "account": account.summary(comparison.prices),
                        }
                    )
                ),
            )

        async def review(signals: list[dict[str, Any]]) -> None:
            payload = {
                "signals": signals,
                "currency": "KRW",
                "observed_at": now(),
                "portfolio": account.summary(comparison.prices),
            }
            deferred: dict[str, str] = {}
            model = SOL
            try:
                calls[model] += 1
                record("ai_review", model=model, signals=signals)
                approved, watches = await decide(settings, payload, model=model, deferred=deferred)
                decided = now()
                if decided >= until:
                    return
                completed[model] += 1
                comparison.approve(signals, approved, decided, watches)
                record(
                    "ai_decision",
                    model=model,
                    decision_at=decided,
                    approvals=approved,
                    watches=watches,
                    deferred=deferred,
                )
                remaining = [
                    {**signal, "sol_reason": deferred[signal["symbol"]]}
                    for signal in signals
                    if signal["symbol"] in deferred
                    and comparison.ai_latest.get(signal["symbol"]) == signal
                    and account.wants(signal)
                ]
                if not remaining or now() >= until:
                    return
                model = ASTRA
                calls[model] += 1
                record("ai_review", model=model, signals=remaining)
                approved, watches = await decide(
                    settings, {**payload, "signals": remaining}, model=model
                )
                decided = now()
                if decided < until:
                    completed[model] += 1
                    deferred_symbols = {signal["symbol"] for signal in remaining}
                    comparison.approve(
                        [signal for signal in signals if signal["symbol"] in deferred_symbols],
                        approved,
                        decided,
                        watches,
                    )
                    record(
                        "ai_decision",
                        model=model,
                        decision_at=decided,
                        approvals=approved,
                        watches=watches,
                    )
            except SafetyError as error:
                record("ai_review_failed", model=model, reason=str(error))

        try:
            status()
            while now() < until:
                if job is not None and job.done():
                    job.result()
                    job = None
                    status()
                position = source.tell()
                line = source.readline()
                if not line.endswith(b"\n"):
                    source.seek(position)
                    await asyncio.sleep(min(0.5, max(0, (until - now()).total_seconds())))
                    continue
                if partial_at_start:
                    partial_at_start = False
                    continue
                snapshot = json.loads(line)
                observed = datetime.fromisoformat(snapshot["observed_at"])
                if not started <= observed < until or now() >= until:
                    continue
                sweeps += 1
                for fill in comparison.observe(snapshot["rows"], observed):
                    if fill["account"] == "ai":
                        record("fill", **fill)
                if job is None and comparison.review_queue:
                    signals = [
                        signal
                        for signal in comparison.review_queue.values()
                        if account.wants(signal)
                        and (signal["action"] == "SELL" or account.cash > 0)
                    ]
                    comparison.review_queue.clear()
                    if signals and now() < until:
                        job = asyncio.create_task(review(signals))
                status()
                await asyncio.sleep(0)
            state = "complete" if sweeps >= 2 and completed[SOL] else "incomplete"
        except (KeyError, OSError, TypeError, ValueError, RuntimeError) as error:
            state = "failed"
            record("failed", error_type=type(error).__name__)
            raise
        finally:
            if state == "running":
                state = "incomplete"
            if job is not None and not job.done():
                job.cancel()
                await asyncio.gather(job, return_exceptions=True)
                record("ai_cancelled_at_end")
            status()


def main() -> None:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--until", required=True, type=datetime.fromisoformat)
    args = parser.parse_args()
    if args.until.tzinfo is None:
        parser.error("--until requires a timezone offset")
    asyncio.run(campaign(args.market.resolve(), args.output.resolve(), args.until))


if __name__ == "__main__":
    main()
