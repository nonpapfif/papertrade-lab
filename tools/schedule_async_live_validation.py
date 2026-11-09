"""Schedule one finite broker-constrained paper validation for one market."""

from __future__ import annotations

import argparse
import fcntl
import math
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.adaptive_journal import read_status as read_journal_status
from tools.autotest_jobs import ROOT, STOP_SECONDS, atomic_json, poll_job, start_job
from tools.autotest_supervisor import command, read_json, tree_digest
from tools.slot_control import SPOOL_NAME, SlotCommand, SlotControl

CUTOFF_DRAIN_SECONDS = 60
CAMPAIGN_RESTART_LIMIT = 3


def retryable_exit(result: dict[str, Any], run: dict[str, Any]) -> bool:
    state = run.get("state")
    if state in ("complete", "incomplete", "interrupted"):
        return False
    if result["returncode"] == -signal.SIGKILL:
        return state in (None, "running")
    if result["returncode"] != 1 or state != "failed" or run.get("error_type") != "ApiError":
        return False
    status = run.get("api_status")
    return status in (429, 500, 502, 503, 504) or (
        status is None and run.get("api_code") in ("transport_error", "cooldown")
    )


def run_schedule(output: Path, spec: dict[str, Any]) -> int:
    status = output / "status.json"
    recovery = spec.get("recover") is True
    dynamic_slots = spec.get("dynamic_slots") is True
    recurring = "initial_slots" in spec
    journal = output / "journal.sqlite3"
    slot_spool = output / SPOOL_NAME
    restarts: list[dict[str, Any]] = []

    def paths(attempt: int) -> tuple[Path, Path]:
        base = output / "attempts" / str(attempt) if attempt > 1 else output
        return base / "job", base / "run"

    def save(state: str, **details: Any) -> None:
        if recovery:
            details["restarts"] = restarts
        if state in ("failed", "blocked", "incomplete"):
            attempt = len(restarts) + 1
            job, run_output = paths(attempt)
            details["failure_handoff"] = {
                "attempt": attempt,
                "source_commit": spec.get("source_commit"),
                "source_digest": spec.get("source_digest"),
                "evidence": {
                    label: str(path.relative_to(output)) if path.is_file() else None
                    for label, path in (
                        ("worker_status", run_output / "status.json"),
                        ("exit_record", job / "exit.json"),
                        ("launch_claim", job / "start.claim"),
                    )
                },
            }
        atomic_json(
            status,
            {"schedule": spec, "state": state, "at": time.time(), **details},
        )

    if dynamic_slots and slot_spool.exists() and not status.exists():
        raise RuntimeError("Slot command spool already exists without its saved schedule")
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
        if recovery:
            history = previous.get("restarts")
            if not isinstance(history, list):
                raise RuntimeError("Saved schedule is missing restart evidence")
            cursor = -1
            for restart in history:
                if (
                    not isinstance(restart, dict)
                    or type(restart.get("cursor")) is not int
                    or restart["cursor"] < cursor
                    or (not recurring and restart["cursor"] == cursor)
                    or not isinstance(restart.get("journal_hash"), str)
                    or not isinstance(restart.get("job_exit"), dict)
                ):
                    raise RuntimeError("Saved restart evidence is invalid")
                cursor = restart["cursor"]
            restarts = history
        job, _ = paths(len(restarts) + 1)
        if not (
            (previous_state == "waiting" and not restarts)
            or (previous_state == "restarting" and restarts)
            or (previous_state == "running" and (job / "start.claim").exists())
        ):
            raise RuntimeError("Saved schedule state is inconsistent with its launch claim")

    result = None
    stop_after = spec["end_epoch"] + CUTOFF_DRAIN_SECONDS
    poll_until = stop_after + 2 * STOP_SECONDS
    try:
        while True:
            job, run_output = paths(len(restarts) + 1)
            result = None
            if not (job / "start.claim").exists():
                if run_output.exists() or job.exists():
                    raise RuntimeError("Run or job already exists without a launch claim")
                if not restarts:
                    if recovery and journal.exists():
                        raise RuntimeError("Journal already exists without a launch claim")
                    save("waiting")
                    while (remaining := spec["start_epoch"] - time.time()) > 0:
                        time.sleep(min(remaining, 30))
                if restarts:
                    previous_job, _ = paths(len(restarts))
                    if (
                        not (previous_job / "start.claim").exists()
                        or poll_job(previous_job) != restarts[-1]["job_exit"]
                    ):
                        raise RuntimeError("Previous job exit does not match restart decision")
                    retry_at = restarts[-1].get("retry_at")
                    if retry_at is not None:
                        if not isinstance(retry_at, (int, float)) or not math.isfinite(retry_at):
                            raise RuntimeError("Saved API retry time is invalid")
                        if retry_at >= spec["end_epoch"]:
                            save("incomplete", reason="API cooldown extends past approved window")
                            return 1
                        while (remaining := min(retry_at, spec["end_epoch"]) - time.time()) > 0:
                            time.sleep(min(remaining, 30))
                if time.time() >= spec["end_epoch"]:
                    if restarts:
                        save("incomplete", reason="Approved window ended before restart")
                        return 1
                    save("expired")
                    return 0
                if (
                    command(["git", "rev-parse", "HEAD"], ROOT) != spec["source_commit"]
                    or tree_digest(ROOT) != spec["source_digest"]
                ):
                    raise RuntimeError("Approved source changed before launch")
                argv = list(spec["argv"])
                if dynamic_slots:
                    with SlotControl.open(slot_spool, initialize=not restarts) as control:
                        if not restarts:
                            for item in spec.get("initial_slots", []):
                                control.submit(SlotCommand(**item), end_epoch=spec["end_epoch"])
                    argv.extend(["--slot-control", str(slot_spool)])
                if recovery:
                    if restarts:
                        verified = read_journal_status(journal)
                        if (
                            verified["state"] != "active"
                            or verified["cursor"] != restarts[-1]["cursor"]
                            or verified["hash"] != restarts[-1]["journal_hash"]
                        ):
                            raise RuntimeError("Journal changed after restart decision")
                        argv[argv.index("--output") + 1] = str(run_output)
                        argv.append("--resume")
                    argv.extend(["--journal", str(journal)])
                if time.time() >= spec["end_epoch"]:
                    if restarts:
                        save("incomplete", reason="Approved window ended before restart")
                        return 1
                    save("expired")
                    return 0
                start_job(job, argv, ROOT, stop_after=stop_after)

            save("running")
            # Wall time determines the approved cutoff. Monotonic time also bounds the wait
            # if the wall clock moves backward while the job is running.
            poll_deadline = time.monotonic() + max(0, poll_until - time.time())
            while (result := poll_job(job)) is None:
                remaining = min(poll_until - time.time(), poll_deadline - time.monotonic())
                if remaining <= 0:
                    save(
                        "incomplete",
                        reason="Worker exit unverified after the approved window and drain",
                    )
                    return 1
                time.sleep(min(remaining, 2))
            if result.get("cutoff_stop") is True:
                save("incomplete", job_exit=result, reason="Worker stopped after cutoff drain")
                return 1
            if result["returncode"] == 0:
                break
            if not recovery:
                save("failed", job_exit=result)
                return 1
            status_file = run_output / "status.json"
            run = read_json(status_file) if status_file.exists() else {}
            run_state = run.get("state")
            if not retryable_exit(result, run):
                save("failed", job_exit=result, run_state=run_state)
                return 1
            if time.time() >= spec["end_epoch"]:
                save("incomplete", job_exit=result, reason="Approved window ended after child exit")
                return 1
            verified = read_journal_status(journal)
            cursor = verified["cursor"]
            if verified["state"] != "active" or type(cursor) is not int or cursor < 0:
                raise RuntimeError("Journal does not contain a recoverable account")
            if restarts and cursor <= restarts[-1]["cursor"] and not recurring:
                save("failed", job_exit=result, reason="Journal cursor did not advance")
                return 1
            if recurring and len(restarts) >= CAMPAIGN_RESTART_LIMIT:
                save("failed", job_exit=result, reason="Recurring restart limit reached")
                return 1
            retry_at = run.get("api_retry_at")
            if recurring:
                retry_at = max(retry_at or 0, time.time() + 5 * 2 ** len(restarts))
            restarts.append(
                {
                    "cursor": cursor,
                    "journal_hash": verified["hash"],
                    "job_exit": result,
                    "run_state": run_state,
                    "api_status": run.get("api_status"),
                    "api_code": run.get("api_code"),
                    "retry_at": retry_at,
                    "at": time.time(),
                }
            )
            save("restarting")

        run = read_json(run_output / "status.json")
        run_state = run.get("state")
        if run_state in ("failed", "incomplete", "interrupted"):
            state = "failed" if run_state == "failed" else "incomplete"
            save(state, job_exit=result, run_state=run_state)
            return 1
        complete = run_state == "complete" and run.get("result") in ("passed", "inconclusive")
        if dynamic_slots:
            shared_feed = run.get("shared_feed")
            evidence = run.get("journal")
            verified = read_journal_status(journal)
            identity = verified["identity"]
            if (
                not complete
                or run.get("experiment") != "shared_feed_slots"
                or not isinstance(shared_feed, dict)
                or shared_feed.get("complete") is not True
                or shared_feed.get("source_unchanged") is not True
                or not isinstance(run.get("shared_slots"), list)
                or not isinstance(evidence, dict)
                or identity.get("experiment") != "shared_feed_slots"
                or identity.get("slot_control") != str(slot_spool.resolve())
                or identity.get("end_epoch") != spec["end_epoch"]
                or verified["state"] != "complete"
                or verified["hash"] != evidence.get("chain_sha256")
                or verified["cursor"] != evidence.get("cursor")
                or verified["records"] != evidence.get("records")
            ):
                raise RuntimeError("Run status does not confirm a completed shared feed")
        else:
            comparison = run.get("comparison")
            if (
                not complete
                or not isinstance(comparison, dict)
                or any(
                    comparison.get(key) is not True
                    for key in ("complete", "source_unchanged", "freeze_matches")
                )
            ):
                raise RuntimeError("Run status does not confirm a completed comparison")
        if tree_digest(ROOT) != spec["source_digest"]:
            save("incomplete", job_exit=result, reason="Approved source changed during the run")
            return 1
        if dynamic_slots:
            with SlotControl.open(slot_spool) as control:
                control.close_submissions()
        save("complete", job_exit=result, run_result=run["result"])
        return 0
    except (
        OSError,
        ValueError,
        RuntimeError,
        TypeError,
        sqlite3.Error,
        subprocess.SubprocessError,
    ) as error:
        save("blocked", reason=str(error), job_exit=result)
        return 1


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-at", required=True, type=datetime.fromisoformat)
    parser.add_argument("--end-at", required=True, type=datetime.fromisoformat)
    parser.add_argument("--capital", required=True, type=float)
    parser.add_argument("--market", choices=("US", "KR"), default="US")
    parser.add_argument("--source-digest", required=True)
    parser.add_argument("--source-commit")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--recover", action="store_true", help="Resume verified journal after a fault"
    )
    parser.add_argument(
        "--dynamic-slots", action="store_true", help="Enable local add, stop and list commands"
    )
    parser.add_argument(
        "--campaign-plan", type=Path, help="Frozen recurring campaign plan and carryover seeds"
    )
    args = parser.parse_args()
    if args.start_at.utcoffset() is None or args.end_at.utcoffset() is None:
        parser.error("Start and end require a timezone offset")
    if args.start_at >= args.end_at:
        parser.error("Start must precede end")
    if not math.isfinite(args.capital) or args.capital <= 0:
        parser.error("Capital must be positive and finite")
    if args.dynamic_slots and not args.recover:
        parser.error("Dynamic slots require --recover")
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
    if args.market == "KR":
        argv.extend(["--market", "KR"])
    campaign = {}
    if args.campaign_plan is not None:
        if not args.dynamic_slots:
            parser.error("--campaign-plan requires --dynamic-slots")
        campaign = read_json(args.campaign_plan)
        if set(campaign) != {"initial_slots", "seeds"}:
            parser.error("Invalid campaign plan fields")
        seeds = args.campaign_plan.with_name("campaign-seeds.json").resolve()
        if seeds.exists():
            if read_json(seeds) != campaign["seeds"]:
                parser.error("Saved campaign seeds differ from the plan")
        else:
            atomic_json(seeds, campaign["seeds"])
        argv.extend(["--campaign-seeds", str(seeds)])
    with (output / "schedule.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return run_schedule(
            output,
            {
                "start_epoch": args.start_at.timestamp(),
                "end_epoch": args.end_at.timestamp(),
                "source_commit": args.source_commit or command(["git", "rev-parse", "HEAD"], ROOT),
                "source_digest": args.source_digest,
                "argv": argv,
                **({"recover": True} if args.recover else {}),
                **({"dynamic_slots": True} if args.dynamic_slots else {}),
                **({"initial_slots": campaign["initial_slots"]} if campaign else {}),
            },
        )


if __name__ == "__main__":
    raise SystemExit(main())
