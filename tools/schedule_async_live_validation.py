"""Schedule one finite broker-constrained US paper validation."""

from __future__ import annotations

import argparse
import fcntl
import math
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.autotest_jobs import ROOT, atomic_json, poll_job, start_job
from tools.autotest_supervisor import command, read_json, tree_digest


def run_schedule(output: Path, spec: dict[str, Any]) -> int:
    status = output / "status.json"
    job = output / "job"

    def save(state: str, **details: Any) -> None:
        atomic_json(
            status,
            {"schedule": spec, "state": state, "at": time.time(), **details},
        )

    if status.exists():
        try:
            previous = read_json(status)
        except (OSError, ValueError, TypeError) as error:
            raise RuntimeError("Cannot read saved schedule status") from error
        if previous.get("schedule") != spec:
            raise RuntimeError("Saved schedule differs from the requested schedule")
        previous_state = previous.get("state")
        if previous_state in ("complete", "expired", "failed", "blocked", "incomplete"):
            return int(previous_state in ("failed", "blocked", "incomplete"))
        if previous_state != "waiting" and (
            previous_state != "running" or not (job / "start.claim").exists()
        ):
            raise RuntimeError("Saved schedule state is inconsistent with its launch claim")

    result = None
    try:
        if not (job / "start.claim").exists():
            if (output / "run").exists() or job.exists():
                raise RuntimeError("Run or job already exists without a launch claim")
            save("waiting")
            while (remaining := spec["start_epoch"] - time.time()) > 0:
                time.sleep(min(remaining, 30))
            if time.time() >= spec["end_epoch"]:
                save("expired")
                return 0
            if (
                command(["git", "rev-parse", "HEAD"], ROOT) != spec["source_commit"]
                or tree_digest(ROOT) != spec["source_digest"]
            ):
                raise RuntimeError("Approved source changed before launch")
            if time.time() >= spec["end_epoch"]:
                save("expired")
                return 0
            start_job(job, spec["argv"], ROOT)

        save("running")
        while (result := poll_job(job)) is None:
            time.sleep(2)
        if result["returncode"] != 0:
            save("failed", job_exit=result)
            return 1

        run = read_json(output / "run" / "status.json")
        run_state = run.get("state")
        if run_state in ("failed", "incomplete", "interrupted"):
            state = "failed" if run_state == "failed" else "incomplete"
            save(state, job_exit=result, run_state=run_state)
            return 1
        comparison = run.get("comparison")
        if (
            run_state != "complete"
            or not isinstance(comparison, dict)
            or any(
                comparison.get(key) is not True
                for key in ("complete", "source_unchanged", "freeze_matches")
            )
            or run.get("result") not in ("passed", "inconclusive")
        ):
            raise RuntimeError("Run status does not confirm a completed comparison")
        if tree_digest(ROOT) != spec["source_digest"]:
            save("incomplete", job_exit=result, reason="Approved source changed during the run")
            return 1
        save("complete", job_exit=result, run_result=run["result"])
        return 0
    except (OSError, ValueError, RuntimeError, TypeError, subprocess.CalledProcessError) as error:
        save("blocked", reason=str(error), job_exit=result)
        return 1


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-at", required=True, type=datetime.fromisoformat)
    parser.add_argument("--end-at", required=True, type=datetime.fromisoformat)
    parser.add_argument("--capital", required=True, type=float)
    parser.add_argument("--source-digest", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.start_at.utcoffset() is None or args.end_at.utcoffset() is None:
        parser.error("Start and end require a timezone offset")
    if args.start_at >= args.end_at:
        parser.error("Start must precede end")
    if not math.isfinite(args.capital) or args.capital <= 0:
        parser.error("Capital must be positive and finite")
    if len(args.source_digest) != 64 or any(
        char not in "0123456789abcdef" for char in args.source_digest
    ):
        parser.error("Source digest must be a lowercase SHA-256 digest")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    argv = [
        sys.executable,
        "-m",
        "tools.run_async_live_validation",
        "--profile",
        "broker-constrained",
        "--adaptive",
        "--end-at",
        args.end_at.astimezone(UTC).isoformat(),
        "--capital",
        str(args.capital),
        "--output",
        str(output / "run"),
    ]
    with (output / "schedule.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_schedule(
            output,
            {
                "start_epoch": args.start_at.timestamp(),
                "end_epoch": args.end_at.timestamp(),
                "source_commit": command(["git", "rev-parse", "HEAD"], ROOT),
                "source_digest": args.source_digest,
                "argv": argv,
            },
        )


if __name__ == "__main__":
    raise SystemExit(main())
