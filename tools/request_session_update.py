"""Request an approved source transition for a running recurring paper session."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from papertrade.core import process_identity
from papertrade.jsonio import atomic_json, read_json
from tools.autotest_jobs import poll_job
from tools.session_update import load_request, validate_source, validate_target
from tools.slot_control import SPOOL_NAME


def active_source(output: Path) -> dict[str, Any]:
    status = read_json(output / "status.json")
    schedule = status.get("schedule")
    source = status.get("update_source")
    restarts = status.get("restarts")
    if (
        status.get("state") != "running"
        or not isinstance(schedule, dict)
        or schedule.get("recover") is not True
        or schedule.get("dynamic_slots") is not True
        or "initial_slots" not in schedule
        or type(schedule.get("end_epoch")) not in (int, float)
        or not time.time() < schedule["end_epoch"]
        or not isinstance(source, dict)
        or not isinstance(restarts, list)
    ):
        raise RuntimeError("No running update-capable recurring schedule")
    attempt = len(restarts) + 1
    worker = output / "attempts" / str(attempt) if restarts else output
    directory = worker / "update"
    job = worker / "job"
    if (
        source.get("attempt") != attempt
        or source.get("directory") != str(directory)
        or directory.resolve() != directory
        or not (job / "start.claim").is_file()
    ):
        raise RuntimeError("Update attempt does not match its launch claim")
    with (output / "schedule.lock").open("rb") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise RuntimeError("Schedule process does not hold its lock")
    job_spec = read_json(job / "spec.json")
    argv = job_spec.get("argv")
    if (
        job_spec.get("cwd") != source.get("root")
        or not isinstance(argv, list)
        or argv[1:3] != ["-m", "tools.run_async_live_validation"]
    ):
        raise RuntimeError("Worker source does not match its launch claim")
    for flag, expected in (
        ("--output", worker / "run"),
        ("--journal", output / "journal.sqlite3"),
        ("--slot-control", output / SPOOL_NAME),
        ("--update-dir", directory),
    ):
        if argv.count(flag) != 1 or argv[argv.index(flag) + 1 : argv.index(flag) + 2] != [
            str(expected)
        ]:
            raise RuntimeError("Worker launch arguments do not match the recurring schedule")
    target = read_json(job / "target.json")
    pid, identity = target.get("pid"), target.get("identity")
    run = read_json(worker / "run" / "status.json")
    if (
        run.get("state") != "running"
        or run.get("experiment") != "shared_feed_slots"
        or type(pid) is not int
        or pid <= 0
        or not isinstance(identity, str)
        or not identity
        or process_identity(pid) != identity
        or poll_job(job) is not None
    ):
        raise RuntimeError("Worker is no longer running or its identity changed")
    return {**source, "pid": pid, "identity": identity}


def submit(
    output: Path, target_root: Path, commit: str, digest: str, report: Path
) -> dict[str, Any]:
    output = output.resolve()
    active = active_source(output)
    source_root = Path(active["root"])
    directory = Path(active["directory"])
    path = directory / "request.json"
    if path.exists():
        raise RuntimeError("This worker already has an update request")
    request = {
        "schema": 1,
        "from_commit": active["commit"],
        "from_digest": active["digest"],
        "target_root": str(target_root.resolve(strict=True)),
        "target_commit": commit,
        "target_digest": digest,
        "validation_report": str(report.resolve(strict=True)),
    }
    validate_source(source_root, request["from_commit"], request["from_digest"])
    validate_target(request, source_root=source_root)
    directory.mkdir(mode=0o700, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".request-", dir=directory) as temporary:
        staged = Path(temporary) / path.name
        atomic_json(staged, request)
        load_request(staged.parent)
        if active_source(output) != active:
            raise RuntimeError("Active worker changed while validating the request")
        try:
            os.link(staged, path)
        except FileExistsError as error:
            raise RuntimeError("This worker already has an update request") from error
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return {"request": str(path), "attempt": active["attempt"], "target_digest": digest}


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True, type=Path)
    parser.add_argument("--target-root", required=True, type=Path)
    parser.add_argument("--target-commit", required=True)
    parser.add_argument("--target-digest", required=True)
    parser.add_argument("--validation-report", required=True, type=Path)
    args = parser.parse_args()
    try:
        receipt = submit(
            args.schedule,
            args.target_root,
            args.target_commit,
            args.target_digest,
            args.validation_report,
        )
    except (OSError, RuntimeError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Update request rejected: {error}\n")
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
