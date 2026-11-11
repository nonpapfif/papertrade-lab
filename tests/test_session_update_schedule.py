"""Offline scheduler and submission checks for approved session updates."""

from __future__ import annotations

import fcntl
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from papertrade.jsonio import atomic_json, read_json
from tools import request_session_update as submission
from tools import schedule_async_live_validation as schedule
from tools.adaptive_journal import AdaptiveJournal
from tools.paper_campaign.runner import SessionRunner
from tools.session_update import (
    ECONOMIC_DEFINITIONS,
    ECONOMIC_FILES,
    HELD_NAME,
    HOLD_EVENT,
    cut_marker,
    held_identity,
    source_marker,
)
from tools.slot_control import SPOOL_NAME
from tools.source_identity import command, file_digest, tree_digest


def approval(directory: Path, digest: str) -> Path:
    directory.mkdir()
    checks = []
    for name, argv in (
        ("ruff", ["ruff", "check", "."]),
        ("mypy", ["mypy", "--explicit-package-bases", "papertrade", "tools", "tests"]),
        ("unittest", ["python", "-m", "unittest", "discover", "-s", "tests"]),
    ):
        entry: dict[str, Any] = {
            "name": name, "argv": argv, "state": "passed", "returncode": 0,
        }
        for stream in ("stdout", "stderr"):
            path = directory / f"{name}.{stream}.log"
            path.write_text("approval fixture\n")
            entry[stream] = {"file": path.name, "sha256": file_digest(path)}
        if name == "unittest":
            entry.update(tests=1, skipped=0)
        checks.append(entry)
    report = directory / "validation.json"
    atomic_json(report, {
        "state": "passed", "source_digest": digest, "final_source_digest": digest,
        "checks": checks,
    })
    return report


class UpdateScheduleTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name).resolve()
        self.original = self.base / "original"
        self.original.mkdir()
        command(["git", "init", "-q"], self.original)
        for key, value in (("name", "Test"), ("email", "test@example.invalid")):
            command(["git", "config", f"user.{key}", value], self.original)
        economic_paths = (*ECONOMIC_FILES, *ECONOMIC_DEFINITIONS)
        for name in economic_paths:
            target = self.original / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(schedule.ROOT / name, target)
        command(["git", "add", "--", *economic_paths], self.original)
        self.releases: list[tuple[Path, str, str]] = []
        for release in range(3):
            (self.original / "source.txt").write_text(str(release))
            command(["git", "add", "source.txt"], self.original)
            command(["git", "-c", "commit.gpgsign=false", "commit", "-qm", str(release)],
                    self.original)
            commit = command(["git", "rev-parse", "HEAD"], self.original)
            frozen = self.base / f"release-{release}"
            command(["git", "worktree", "add", "-q", "--detach", str(frozen), commit],
                    self.original)
            self.releases.append((frozen, commit, tree_digest(frozen)))
        root, commit, digest = self.releases[0]
        self.enterContext(patch.object(schedule, "ROOT", root))
        self.output = self.base / "schedule"
        self.output.mkdir()
        self.journal = self.output / "journal.sqlite3"
        self.end = time.time() + 3600
        self.spec: dict[str, Any] = {
            "source_commit": commit, "source_digest": digest,
            "start_epoch": self.end - 3610, "end_epoch": self.end,
            "argv": ["python", "-m", "tools.run_async_live_validation",
                     "--output", str(self.output / "run")],
            "initial_slots": [], "dynamic_slots": True, "recover": True,
        }
        self.identity = {
            "experiment": "shared_feed_slots", "slot_control": str(self.output / SPOOL_NAME),
            "end_epoch": self.end, "campaign_seeds": {},
        }

    def request(self, old: int, new: int) -> dict[str, Any]:
        _, commit, digest = self.releases[old]
        root, target_commit, target_digest = self.releases[new]
        return {
            "schema": 1, "from_commit": commit, "from_digest": digest,
            "target_root": str(root), "target_commit": target_commit,
            "target_digest": target_digest,
            "validation_report": str(approval(self.base / f"gate-{old}-{new}", target_digest)),
        }

    def setup_worker(self, job: Path, argv: list[str]) -> tuple[Path, Path]:
        job.mkdir(parents=True)
        (job / "start.claim").touch()
        run = Path(argv[argv.index("--output") + 1])
        run.mkdir()
        update = Path(argv[argv.index("--update-dir") + 1])
        self.assertEqual(update, job.parent / "update")
        self.assertEqual(argv[argv.index("--journal") + 1], str(self.journal))
        self.assertEqual(argv[argv.index("--slot-control") + 1], str(self.output / SPOOL_NAME))
        return run, update

    def write_handoff(
        self, journal: AdaptiveJournal, directory: Path, request: dict[str, Any], sequence: int
    ) -> dict[str, Any]:
        directory.mkdir()
        atomic_json(directory / "request.json", request)
        journal.append({"kind": "clock", "sequence": sequence})
        journal.append({"kind": HOLD_EVENT, "request": request, "sequence": sequence})
        with AdaptiveJournal.open(
            directory / HELD_NAME, held_identity(request, self.journal), resume=False
        ) as held:
            held.complete()
            ready = {
                "schema": 1, "request": request, "journal_path": str(self.journal),
                "sequence": sequence, "held": held.evidence(),
            }
        journal.append(cut_marker(ready))
        ready["journal"] = journal.evidence()
        atomic_json(directory / "ready.json", ready)
        return ready

    def test_recovers_selected_release_across_fault_and_second_handoff(self) -> None:
        requests = [self.request(0, 1), self.request(1, 2)]
        called: list[Path] = []
        handoffs: list[dict[str, Any]] = []

        def start(job: Path, argv: list[str], cwd: Path, *, stop_after: float) -> None:
            attempt = len(called) + 1
            if attempt == 4 and interrupt:
                raise KeyboardInterrupt
            called.append(cwd)
            run, update = self.setup_worker(job, argv)
            self.assertEqual(stop_after, self.end + schedule.CUTOFF_DRAIN_SECONDS)
            if attempt > 1:
                expected = self.output / "update" if attempt < 4 else (
                    self.output / "attempts/3/update"
                )
                self.assertEqual(argv[argv.index("--resume-update") + 1], str(expected))
                self.assertIn("--resume", argv)
            with AdaptiveJournal.open(self.journal, self.identity, resume=attempt > 1) as journal:
                if attempt == 1:
                    handoffs.append(self.write_handoff(journal, update, requests[0], 1))
                elif attempt == 2:
                    journal.append(source_marker(handoffs[0], {"source.txt": "b" * 64}))
                    journal.append({"kind": "clock", "sequence": 2})
                    atomic_json(update / "request.json", requests[1])
                elif attempt == 3:
                    handoffs.append(self.write_handoff(journal, update, requests[1], 3))
                else:
                    journal.append(source_marker(handoffs[1], {"source.txt": "c" * 64}))
                    journal.complete()
                    atomic_json(run / "status.json", {
                        "state": "complete", "result": "inconclusive", "shared_slots": [],
                        "experiment": "shared_feed_slots", "journal": journal.evidence(),
                        "shared_feed": {"complete": True, "source_unchanged": True},
                    })
            if attempt in (1, 3):
                atomic_json(run / "status.json", {"state": "handoff_ready", "handoff": handoffs[-1]})
            atomic_json(job / "exit.json", {"returncode": -9 if attempt == 2 else 0})

        interrupt = True
        ticks = iter(range(int(time.time()), int(self.end), 10))
        with (
            patch.object(schedule, "start_job", side_effect=start),
            patch.object(schedule.time, "time", side_effect=lambda: next(ticks)),
        ):
            with self.assertRaises(KeyboardInterrupt):
                schedule.run_schedule(self.output, self.spec)
            self.assertEqual(read_json(self.output / "status.json")["state"], "restarting")
            interrupt = False
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 0)
        status = read_json(self.output / "status.json")
        self.assertEqual(status["state"], "complete")
        first, second, third = [row[0] for row in self.releases]
        self.assertEqual(called, [first, second, second, third])
        self.assertEqual(["update" in row for row in status["restarts"]], [True, False, True])
        self.assertEqual(read_json(self.output / "attempts/2/update/request.json"), requests[1])
        self.assertEqual(status["update_source"]["digest"], self.releases[-1][2])
        scheduler = self.base / "scheduler"
        scheduler.mkdir()
        (scheduler / "start.claim").touch()
        atomic_json(scheduler / "exit.json", {"returncode": 0})
        runner = SessionRunner(self.spec["source_commit"], self.spec["source_digest"])
        self.assertEqual(runner.poll(self.base, {}), {
            "state": "blocked",
            "reason": "updated_session_requires_controller_source_reconciliation",
        })

    def test_ready_without_verified_exit_remains_incomplete(self) -> None:
        request = self.request(0, 1)
        wall_time = time.time()

        def start(job: Path, argv: list[str], _cwd: Path, **_: Any) -> None:
            run, update = self.setup_worker(job, argv)
            with AdaptiveJournal.open(self.journal, self.identity, resume=False) as journal:
                ready = self.write_handoff(journal, update, request, 1)
            atomic_json(run / "status.json", {"state": "handoff_ready", "handoff": ready})

        def poll(_job: Path) -> None:
            nonlocal wall_time
            wall_time = self.end + 100

        with (
            patch.object(schedule, "start_job", side_effect=start) as launch,
            patch.object(schedule, "poll_job", side_effect=poll),
            patch.object(schedule.time, "time", side_effect=lambda: wall_time),
        ):
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 1)
            launch.assert_called_once()
        status = read_json(self.output / "status.json")
        self.assertEqual(status["state"], "incomplete")
        self.assertIn("exit unverified", status["reason"])
        self.assertFalse((self.output / "attempts").exists())

    def test_ambiguous_hold_and_invalid_ready_do_not_launch_successor(self) -> None:
        request = self.request(0, 1)
        for mode in ("hold", "invalid", "stale"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(dir=self.base) as directory:
                self.output = Path(directory)
                self.journal = self.output / "journal.sqlite3"
                self.identity["slot_control"] = str(self.output / SPOOL_NAME)
                self.spec["argv"][-1] = str(self.output / "run")
                with patch.object(schedule, "start_job") as start:
                    def stop(
                        job: Path, argv: list[str], _cwd: Path, *, fault: str = mode, **_: Any
                    ) -> None:
                        run, update = self.setup_worker(job, argv)
                        with AdaptiveJournal.open(
                            self.journal, self.identity, resume=False
                        ) as journal:
                            ready = self.write_handoff(journal, update, request, 1)
                        if fault == "invalid":
                            (update / "ready.json").write_text("{")
                        elif fault == "hold":
                            (update / "ready.json").unlink()
                        else:
                            (Path(request["target_root"]) / "source.txt").write_text("stale")
                        atomic_json(run / "status.json", {
                            "state": "holding" if fault == "hold" else "handoff_ready",
                            "handoff": ready,
                        })
                        atomic_json(job / "exit.json", {"returncode": -9 if fault == "hold" else 0})

                    start.side_effect = stop
                    self.assertEqual(schedule.run_schedule(self.output, self.spec), 1)
                    start.assert_called_once()
                self.assertEqual(read_json(self.output / "status.json")["state"], "blocked")
                self.assertFalse((self.output / "attempts").exists())

    def test_submission_verifies_running_attempt_and_claims_request_once(self) -> None:
        job = self.output / "job"
        update = self.output / "update"
        argv = [*self.spec["argv"], "--journal", str(self.journal), "--slot-control",
                str(self.output / SPOOL_NAME), "--update-dir", str(update)]
        run, _ = self.setup_worker(job, argv)
        root, commit, digest = self.releases[0]
        atomic_json(job / "spec.json", {"argv": argv, "cwd": str(root)})
        atomic_json(job / "target.json", {"pid": 42, "identity": "test worker"})
        atomic_json(run / "status.json", {"state": "running", "experiment": "shared_feed_slots"})
        status = {
            "state": "running", "schedule": self.spec, "restarts": [],
            "update_source": {"root": str(root), "commit": commit, "digest": digest,
                              "attempt": 1, "directory": str(update)},
        }
        request = self.request(0, 1)
        target, target_commit, target_digest = self.releases[1]
        args = self.output, target, target_commit, target_digest, Path(request["validation_report"])
        with (
            (self.output / "schedule.lock").open("w") as lock,
            patch.object(submission, "process_identity", return_value="test worker"),
            patch.object(submission, "poll_job", return_value=None) as poll,
        ):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            atomic_json(self.output / "status.json", {**status, "update_source": None})
            with self.assertRaisesRegex(RuntimeError, "No running update-capable"):
                submission.submit(*args)
            atomic_json(self.output / "status.json", status)
            poll.side_effect = [{"returncode": 0}]
            with self.assertRaisesRegex(RuntimeError, "Worker is no longer running"):
                submission.submit(*args)
            poll.side_effect = None
            unrelated = self.base / "other-repository"
            command(["git", "clone", "-q", "--local", str(target), str(unrelated)], self.base)
            with self.assertRaisesRegex(RuntimeError, "target belongs to another repository"):
                submission.submit(self.output, unrelated, *args[2:])
            self.assertFalse((update / "request.json").exists())
            receipt = submission.submit(*args)
            self.assertEqual(receipt["target_digest"], target_digest)
            self.assertEqual(read_json(update / "request.json"), request)
            with self.assertRaisesRegex(RuntimeError, "already has an update request"):
                submission.submit(*args)
