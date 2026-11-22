"""Advance one market from sealed paper evidence into distinct dated comparisons."""

from __future__ import annotations

import argparse
import fcntl
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from papertrade.api import ApiError
from papertrade.core import SafetyError
from papertrade.jsonio import atomic_json, read_json
from tools.paper_market import KR, US
from tools.strategy_review_cycle import main as run_comparison
from tools.strategy_review_handoff import write_handoff


def status(output: Path, market: str, phase: str, **details: Any) -> None:
    atomic_json(output / f"{market}-lifecycle.json", {
        "market": market, "phase": phase, "updated_at": datetime.now(UTC).isoformat(), **details,
    })


def run(market_code: str, prior: Path, output: Path) -> int:
    market = KR if market_code == "KR" else US
    while True:
        plan = read_json(prior / "plan.json")
        if plan["market"] != market_code:
            raise ValueError("Previous plan belongs to a different market")
        day = date.fromisoformat(plan["day"]).isoformat()
        status(output, market_code, "reviewing", day=day)
        # The archived calendar identifies the next session without guessing holidays.
        saved = read_json(prior / "calendar.json")
        next_day = date.fromisoformat(saved["nextBusinessDay"]["date"]).isoformat()
        if next_day <= day:
            raise ValueError("Archived calendar does not identify a later session")
        start, _ = market.session({"today": saved["nextBusinessDay"]}, next_day)
        handoff = output / "handoffs" / day / market_code
        handoff.mkdir(parents=True, exist_ok=True, mode=0o700)
        status(output, market_code, "reviewing", day=day, next_day=next_day)
        ready = write_handoff(prior, next_day, start, output_dir=handoff)
        if not ready:
            status(output, market_code, "blocked", day=day, next_day=next_day,
                   reason="See the market handoff for evidence or strategy requirements")
            return 1
        if datetime.now(UTC) >= start:
            raise RuntimeError(f"The next {market_code} session has already opened")
        target = output / next_day / market_code
        if target.exists():
            raise RuntimeError("The next dated comparison already has an output directory")
        status(output, market_code, "next_plan_ready", day=day, next_day=next_day)
        argv = [
            "--market", market_code, "--day", next_day, "--next-plan",
            str(handoff / "next_plan.json"), "--output", str(target),
        ]
        if market_code == "US":
            argv += ["--source", "/day-trader"]
        try:
            code = run_comparison(argv)
        except SystemExit as error:
            raise RuntimeError(
                f"Comparison rejected the frozen plan (exit {error.code})"
            ) from error
        if not (target / "decision.json").is_file():
            raise RuntimeError(f"Comparison exited {code} before producing a decision")
        prior = target


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", choices=("KR", "US"), required=True)
    parser.add_argument("--prior", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve(strict=True)
    with (output / f"{args.market}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 2
        try:
            return run(args.market, args.prior.resolve(strict=True), output)
        except (
            ApiError, SafetyError, OSError, RuntimeError, ValueError, KeyError, TypeError,
        ) as error:
            status(output, args.market, "blocked", reason=str(error))
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
