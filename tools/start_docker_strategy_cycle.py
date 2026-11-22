"""Start the prepared Docker comparisons once the previous Mac jobs have exited."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
from contextlib import closing
from pathlib import Path

from papertrade.core import process_identity
from papertrade.jsonio import atomic_json, read_json

ROOT = Path(__file__).resolve().parents[1]


def previous_jobs_ended(prior: Path) -> bool:
    for market in ("KR", "US"):
        job = prior / f"{market}-job"
        if not (job / "exit.json").is_file():
            return False
        if type(read_json(job / "exit.json").get("returncode")) is not int:
            return False
        for filename in ("target.json", "worker.json"):
            path = job / filename
            if path.is_file():
                evidence = read_json(path)
                pid, identity = evidence.get("pid"), evidence.get("identity")
                if type(pid) is not int or not isinstance(identity, str) or not identity:
                    return False
                if process_identity(pid) == identity:
                    return False
    return True


def snapshot_previous(prior: Path, target: Path) -> None:
    if target.exists():
        return
    with tempfile.TemporaryDirectory(dir=target.parent, prefix=".snapshot-") as temp:
        stage = Path(temp) / "prior"
        for market in ("KR", "US"):
            src, dst = prior / market, stage / market
            dst.mkdir(parents=True, mode=0o700)
            files = ["plan.json", "calendar.json", "decision.json"]
            if market == "KR":
                scheduler = src / "schedule/status.json"
                restarts = read_json(scheduler).get("restarts") if scheduler.is_file() else []
                run = (
                    f"schedule/attempts/{len(restarts) + 1}/run" if restarts else "schedule/run"
                )
                files += ["schedule/status.json", f"{run}/status.json"]
                journal = "schedule/journal.sqlite3"
            else:
                files.append("session/comparison.json")
                journal = "session/journal.sqlite3"
            for relative in files + [journal, journal + "-wal"]:
                if (src / relative).is_file():
                    (dst / relative).parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src / relative, dst / relative)
            if (dst / journal).is_file():
                # Checkpoint the copy so Linux can open it without sharing macOS WAL state.
                with closing(sqlite3.connect(dst / journal, isolation_level=None)) as db:
                    checkpoint = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                    if checkpoint is None or checkpoint[0] != 0:
                        raise RuntimeError("Cannot checkpoint the predecessor journal copy")
                    if db.execute("PRAGMA journal_mode=DELETE").fetchone() != ("delete",):
                        raise RuntimeError("Cannot seal the predecessor journal copy")
        stage.replace(target)


def started_services(compose: list[str]) -> set[str]:
    result = subprocess.run(
        [*compose, "ps", "--all", "--format", "json"], capture_output=True, text=True, check=True,
    )
    if not result.stdout.strip():
        return set()
    try:
        records = json.loads(result.stdout)
        if isinstance(records, dict):
            records = [records]
    except json.JSONDecodeError:
        records = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    # An exited market already ran, including a market that wrote a hold and exited.
    return {
        row["Service"] for row in records
        if row["State"] in {"running", "exited", "paused", "restarting", "dead"}
    }


def start(docker: Path, env_file: Path, prior: Path, output: Path) -> int:
    receipt = output / "cutover.json"
    if receipt.exists() or not previous_jobs_ended(prior):
        return 0
    snapshot_previous(prior, output / "prior")
    compose = [
        str(docker), "compose", "--project-directory", str(ROOT),
        "--env-file", str(env_file), "-f", str(ROOT / "compose.yaml"),
    ]
    pending = sorted({"kr", "us"} - started_services(compose))
    if pending:
        subprocess.run(
            [*compose, "up", "-d", "--no-build", "--no-recreate", "--no-deps", *pending],
            check=True,
        )
        if {"kr", "us"} - started_services(compose):
            raise RuntimeError("Compose did not start both market services")
    atomic_json(receipt, {"previous": str(prior), "services": ["kr", "us"]})
    return 0


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--prior", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with (args.output / "cutover.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        return start(args.docker, args.env_file, args.prior, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
