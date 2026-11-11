"""Production adapter for the existing detached finite shared-feed scheduler."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from papertrade.jsonio import atomic_json, read_json
from tools.autotest_jobs import ROOT, poll_job, start_job
from tools.paper_campaign.recovery import SessionRecovery
from tools.slot_control import SPOOL_NAME, SlotCommand, SlotControl


class SessionRunner:
    def __init__(
        self, source_commit: str, source_digest: str, recovery: SessionRecovery | None = None
    ) -> None:
        self.source_commit = source_commit
        self.source_digest = source_digest
        self.recovery = recovery

    def recover(self, output: Path, handoff: dict[str, Any]) -> bool:
        return self.recovery is not None and self.recovery.poll(output, handoff)

    def poll(self, output: Path, record: dict[str, Any]) -> dict[str, Any] | None:
        job, schedule, plan = output / "scheduler", output / "schedule", output / "plan.json"
        if not (job / "start.claim").exists():
            atomic_json(plan, {"initial_slots": record["slots"], "seeds": record["seeds"]})
            start_job(
                job,
                [
                    sys.executable,
                    "-m",
                    "tools.schedule_async_live_validation",
                    "--start-at",
                    record["start"],
                    "--end-at",
                    record["end"],
                    "--capital",
                    str(record["capital"]),
                    "--market",
                    record["market"],
                    "--source-commit",
                    self.source_commit,
                    "--source-digest",
                    self.source_digest,
                    "--output",
                    str(schedule),
                    "--dynamic-slots",
                    "--recover",
                    "--campaign-plan",
                    str(plan),
                ],
                ROOT,
            )
            return None
        if record.get("repair_resumed"):
            job = output / "engineering-recovery" / "resumed-scheduler"
        result = poll_job(job)
        if result is None:
            return None
        status_path = schedule / "status.json"
        status = read_json(status_path) if status_path.exists() else {}
        spec = status.get("schedule", {})
        if record.get("repair_resumed"):
            approved = read_json(output / "engineering-recovery" / "restart.json")
            if status.get("repair") != approved:
                return {"state": "blocked", "reason": "resumed_scheduler_source_unverified"}
        if spec.get("source_commit") != self.source_commit:
            return {"state": "blocked", "reason": "scheduler_source_unverified"}
        if result["returncode"] != 0 or status.get("state") != "complete":
            return {"state": status.get("state", "blocked"), "reason": "finite_schedule_failed"}
        worker = schedule
        if restarts := status.get("restarts"):
            worker = schedule / "attempts" / str(len(restarts) + 1)
        summary = read_json(worker / "run" / "status.json")
        with SlotControl.open(schedule / SPOOL_NAME) as control:
            commands = control.report()["commands"]
        return {
            "state": "complete",
            "summary": summary,
            "commands": commands,
            "journal": str(schedule / "journal.sqlite3"),
        }

    @staticmethod
    def live(output: Path) -> dict[str, Any] | None:
        schedule = output / "schedule"
        spool, status_path = schedule / SPOOL_NAME, schedule / "status.json"
        if not spool.exists() or not status_path.exists():
            return None
        status = read_json(status_path)
        if status.get("state") != "running":
            return None
        worker = schedule
        if restarts := status.get("restarts"):
            worker = schedule / "attempts" / str(len(restarts) + 1)
        runtime_status = worker / "run" / "status.json"
        if not runtime_status.exists():
            return None
        with SlotControl.open(spool) as control:
            report = control.report()
        if report["closed"]:
            return None
        return {"summary": read_json(runtime_status), **report}

    @staticmethod
    def branch(output: Path, command: SlotCommand, cutoff: float) -> None:
        with SlotControl.open(output / "schedule" / SPOOL_NAME) as control:
            control.submit(command, end_epoch=cutoff)
