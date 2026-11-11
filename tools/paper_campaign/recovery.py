"""Supervise one verified engineering repair for a failed recurring paper session."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from papertrade.jsonio import atomic_json, read_json
from tools.adaptive_journal import read_status as read_journal
from tools.autotest_jobs import ROOT, poll_job, start_job
from tools.source_identity import command, tree_digest
from tools.supervisor_scopes import REPAIR_SCOPE

# These are runtime faults for which a regression can be established from worker evidence.
REPAIRABLE_ERRORS = frozenset({
    "AttributeError", "IndexError", "KeyError", "RuntimeError", "TypeError", "ValueError",
})
REPAIR_REMOTE = "https://github.com/nonpapfif/papertrade-lab.git"


def collect_failure(output: Path, handoff: dict[str, Any]) -> dict[str, Any] | None:
    """Admit only a verified nonzero worker exit with an active committed journal."""
    schedule = output / "schedule"
    saved = read_json(schedule / "status.json")
    spec = saved["schedule"]
    details = saved.get("failure_handoff") or {}
    scheduler_exit = poll_job(output / "scheduler")
    if (
        saved.get("state") != "failed"
        or spec.get("dynamic_slots") is not True
        or spec.get("recover") is not True
        or "initial_slots" not in spec
        or time.time() >= spec["end_epoch"]
        or spec["source_commit"] != handoff["source_commit"]
        or spec["source_digest"] != handoff["source_digest"]
        or scheduler_exit is None
        or scheduler_exit["returncode"] != 1
        or scheduler_exit.get("cutoff_stop")
    ):
        return None
    attempt = len(saved["restarts"]) + 1
    worker = schedule if attempt == 1 else schedule / "attempts" / str(attempt)
    job = worker / "job"
    if details.get("attempt") != attempt or any(
        not (job / name).is_file()
        for name in ("start.claim", "spec.json", "worker.json", "target.json", "exit.json")
    ):
        return None
    target, worker_identity = read_json(job / "target.json"), read_json(job / "worker.json")
    if any(
        type(identity.get("pid")) is not int or not identity.get("identity")
        for identity in (target, worker_identity)
    ):
        return None
    exit_record = poll_job(job)
    status_file = worker / "run" / "status.json"
    if (
        exit_record is None
        or exit_record != saved.get("job_exit")
        or exit_record["returncode"] != 1
        or exit_record.get("cutoff_stop")
        or not status_file.is_file()
    ):
        return None
    status = read_json(status_file)
    if status.get("state") != "failed" or status.get("error_type") not in REPAIRABLE_ERRORS:
        return None
    journal = read_journal(schedule / "journal.sqlite3")
    identity = journal["identity"]
    if (
        journal["state"] != "active"
        or journal["records"] < 1
        or identity.get("experiment") != "shared_feed_slots"
        or identity.get("market") != handoff["market"]
        or identity.get("end_epoch") != spec["end_epoch"]
    ):
        return None
    return {
        "handoff": handoff,
        "end_epoch": spec["end_epoch"],
        "schedule": saved,
        "scheduler_exit": scheduler_exit,
        "worker_status": str(status_file),
        "worker_identity": worker_identity,
        "target_identity": target,
        "worker_exit": str(job / "exit.json"),
        "worker_stderr": str(job / "stderr.log"),
        "anchor": {field: journal[field] for field in ("records", "cursor", "hash")},
    }



def approved_restart(recovery: Path) -> tuple[Path, dict[str, Any]] | None:
    supervisor = recovery / "supervisor"
    reviewed = read_json(supervisor / "status.json")
    if reviewed["phase"] != "repair_ready":
        return None
    pinned = Path(reviewed["runtime_checkout"])
    if pinned != supervisor / "merged-source":
        raise RuntimeError("Unexpected repair source checkout")
    return pinned, {
        "failure": str(recovery / "failure.json"),
        "source_commit": reviewed["merged_sha"],
        "source_digest": reviewed["merged_digest"],
        "pr": reviewed["pr"],
    }


def require_approved_restart(
    source: Path, schedule: Path, approval: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    recovery = schedule.parent / "engineering-recovery"
    if approval.resolve() != recovery / "restart.json":
        raise RuntimeError("Repair approval does not belong to the failed schedule")
    grant = read_json(approval)
    engineering_exit = poll_job(recovery / "engineering-job")
    if (
        engineering_exit is None
        or engineering_exit["returncode"] != 0
        or approved_restart(recovery) != (source, grant)
        or command(["git", "rev-parse", "HEAD"], source) != grant["source_commit"]
        or tree_digest(source) != grant["source_digest"]
    ):
        raise RuntimeError("Repair approval does not match the published source")
    return grant, read_json(Path(grant["failure"]))


class SessionRecovery:
    def __init__(self, repo: Path, codex: Path) -> None:
        self.repo = repo.resolve()
        self.codex = codex.resolve()
        if (
            command(["git", "remote", "get-url", "origin"], self.repo) != REPAIR_REMOTE
            or command(["git", "remote", "get-url", "--push", "origin"], self.repo) != REPAIR_REMOTE
        ):
            raise ValueError("Repair repository origin does not match Papertrade Lab")

    def poll(self, output: Path, handoff: dict[str, Any]) -> bool:
        """Reattach or launch each detached job once, returning true after durable input."""
        recovery = output / "engineering-recovery"
        blocker = recovery / "blocked.json"
        if blocker.exists():
            return False
        try:
            return self._poll(output, handoff, recovery)
        except (
            OSError, RuntimeError, ValueError, TypeError, KeyError, subprocess.SubprocessError
        ) as error:
            atomic_json(blocker, {
                "state": "blocked", "task_id": handoff["task_id"], "at": time.time(),
                "reason": type(error).__name__,
                "required": "Reconcile archived failure, repair process and published source.",
            })
            return False

    def _poll(self, output: Path, handoff: dict[str, Any], recovery: Path) -> bool:
        archive = recovery / "failure.json"
        supervisor = recovery / "supervisor"
        engineering, resumed = recovery / "engineering-job", recovery / "resumed-scheduler"
        schedule = output / "schedule"
        if not archive.exists():
            if not (schedule / "status.json").is_file():
                return False
            failure = collect_failure(output, handoff)
            if failure is None:
                atomic_json(recovery / "blocked.json", {
                    "state": "blocked", "task_id": handoff["task_id"], "at": time.time(),
                    "reason": "failure_not_eligible",
                    "required": "Inspect the scheduler exit, worker status and journal identity.",
                })
                return False
            atomic_json(archive, failure)
        failure = read_json(archive)
        if failure["handoff"] != handoff:
            raise RuntimeError("Archived repair input differs from the blocked session")
        if not (engineering / "start.claim").exists():
            if time.time() >= failure["end_epoch"]:
                return False
            start_job(
                engineering,
                [
                    sys.executable, "-m", "tools.autotest_supervisor",
                    "--scope", REPAIR_SCOPE, "--repo", str(self.repo),
                    "--output", str(supervisor), "--inherited", str(recovery),
                    "--codex", str(self.codex),
                ],
                ROOT,
            )
            return False
        outcome = poll_job(engineering)
        if outcome is None:
            return False
        if outcome["returncode"] != 0:
            atomic_json(recovery / "blocked.json", {
                "state": "blocked", "task_id": handoff["task_id"], "at": time.time(),
                "reason": "repair_supervisor_exited", "exit": outcome,
                "required": "Reconcile supervisor evidence before taking another action.",
            })
            return False
        approved = approved_restart(recovery)
        if approved is None:
            return False
        pinned, grant = approved
        grant_path = recovery / "restart.json"
        if grant_path.exists() and read_json(grant_path) != grant:
            raise RuntimeError("Saved restart approval differs from the published repair")
        if not (resumed / "start.claim").exists():
            if time.time() >= failure["end_epoch"]:
                return False
            if (
                command(["git", "rev-parse", "HEAD"], pinned) != grant["source_commit"]
                or tree_digest(pinned) != grant["source_digest"]
            ):
                raise RuntimeError("Pinned repair source changed before scheduler launch")
            atomic_json(grant_path, grant)
            start_job(
                resumed,
                [
                    sys.executable, "-m", "tools.schedule_async_live_validation",
                    "--output", str(schedule), "--repair", str(grant_path),
                ],
                pinned,
            )
            return False
        outcome = poll_job(resumed)
        proof_path = recovery / "resumed-input.json"
        if not proof_path.exists():
            if outcome is not None:
                atomic_json(recovery / "blocked.json", {
                    "state": "blocked", "task_id": handoff["task_id"], "at": time.time(),
                    "reason": "resumed_scheduler_exited_without_committed_input",
                    "source": grant, "exit": outcome,
                    "schedule": str(schedule / "status.json"),
                    "required": "Reconcile the resumed worker exit and committed journal.",
                })
            return False
        proof = read_json(proof_path)
        if (
            proof.get("repair") != grant
            or not isinstance(proof.get("first_input"), dict)
            or proof["first_input"]["index"] <= failure["anchor"]["records"]
        ):
            raise RuntimeError("Resumed input proof differs from the approved repair")
        return True
