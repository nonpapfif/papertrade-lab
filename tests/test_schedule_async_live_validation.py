"""Offline tests for the one-shot validation schedule."""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools import schedule_async_live_validation as schedule
from tools.adaptive_journal import AdaptiveJournal
from tools.slot_control import SPOOL_NAME, SlotCommand, SlotControl


class ScheduleTest(unittest.TestCase):
    def test_kr_schedule_freezes_market_in_worker_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            argv = [
                "schedule",
                "--market",
                "KR",
                "--start-at",
                "2026-11-05T09:00:00+09:00",
                "--end-at",
                "2026-11-05T15:20:00+09:00",
                "--capital",
                "100000",
                "--source-digest",
                "a" * 64,
                "--output",
                directory,
                "--recover",
            ]
            with (
                patch.object(sys, "argv", argv),
                patch.object(schedule, "command", return_value="approved"),
                patch.object(schedule, "run_schedule", return_value=0) as run,
            ):
                self.assertEqual(schedule.main(), 0)
            self.assertEqual(run.call_args.args[0], output.resolve())
            spec = run.call_args.args[1]
            self.assertEqual(spec["argv"][-2:], ["--market", "KR"])
            self.assertTrue(spec["recover"])

    def test_waits_then_launches_once_and_records_real_child_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            now = time.time()
            run = output / "run"
            final_status = {
                "state": "complete",
                "result": "inconclusive",
                "comparison": {"complete": True, "source_unchanged": True, "freeze_matches": True},
            }
            child = (
                "from pathlib import Path; "
                f"path = Path({str(run)!r}); path.mkdir(); "
                f"(path / 'status.json').write_text({json.dumps(final_status)!r}); "
                "print('offline child')"
            )
            spec = {
                "start_epoch": now + 0.1,
                "end_epoch": now + 30,
                "source_commit": "approved",
                "source_digest": "expected",
                "argv": [sys.executable, "-c", child],
            }
            with (
                patch.object(schedule, "command", return_value="approved"),
                patch.object(schedule, "tree_digest", return_value="expected"),
            ):
                self.assertEqual(schedule.run_schedule(output, spec), 0)
                self.assertEqual(schedule.run_schedule(output, spec), 0)
            status = json.loads((output / "status.json").read_text())
            job = output / "job"
            self.assertEqual(status["state"], "complete")
            self.assertEqual(status["job_exit"]["returncode"], 0)
            self.assertEqual((job / "stdout.log").read_text(), "offline child\n")
            launch = json.loads((job / "spec.json").read_text())
            self.assertGreaterEqual(launch["created_at"], spec["start_epoch"])

    def test_expired_schedule_never_claims_job(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            now = time.time()
            spec = {"start_epoch": now - 10, "end_epoch": now - 1}
            self.assertEqual(schedule.run_schedule(output, spec), 0)
            self.assertFalse((output / "job").exists())
            status = json.loads((output / "status.json").read_text())
            self.assertEqual(status["state"], "expired")

    def test_source_drift_blocks_launch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            now = time.time()
            spec = {
                "start_epoch": now - 1,
                "end_epoch": now + 30,
                "source_commit": "approved",
                "source_digest": "expected",
            }
            with (
                patch.object(schedule, "command", return_value="approved"),
                patch.object(schedule, "tree_digest", return_value="changed"),
            ):
                self.assertEqual(schedule.run_schedule(output, spec), 1)
            self.assertFalse((output / "job").exists())
            status = json.loads((output / "status.json").read_text())
            self.assertEqual(status["state"], "blocked")

    def test_claim_without_identity_is_never_relaunched(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            job = output / "job"
            job.mkdir()
            claim = job / "start.claim"
            claim.write_text("123\n")
            self.assertEqual(schedule.run_schedule(output, {}), 1)
            self.assertEqual(claim.read_text(), "123\n")
            status = json.loads((output / "status.json").read_text())
            self.assertEqual(status["state"], "blocked")

    def test_zero_exit_with_incomplete_evidence_remains_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            job = output / "job"
            job.mkdir()
            (job / "start.claim").write_text("123\n")
            (job / "exit.json").write_text('{"returncode": 0}')
            run = output / "run"
            run.mkdir()
            (run / "status.json").write_text('{"state": "incomplete"}')
            self.assertEqual(schedule.run_schedule(output, {}), 1)
            status = json.loads((output / "status.json").read_text())
            self.assertEqual(status["state"], "incomplete")
            self.assertEqual(status["job_exit"]["returncode"], 0)

    def test_malformed_saved_schedule_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            status = output / "status.json"
            status.write_text('{"state":')
            with self.assertRaisesRegex(RuntimeError, "Cannot read saved schedule status"):
                schedule.run_schedule(output, {})
            self.assertEqual(status.read_text(), '{"state":')
            self.assertFalse((output / "job").exists())


RECOVERY_CHILD = """
import argparse
import json
import os
import signal
from pathlib import Path

from tools.adaptive_journal import AdaptiveJournal

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path)
parser.add_argument("--journal", type=Path)
parser.add_argument("--resume", action="store_true")
parser.add_argument("--kill-resumed", action="store_true")
args = parser.parse_args()
args.output.mkdir()
identity = {"test_account": "original"}
with AdaptiveJournal.open(args.journal, identity, resume=args.resume) as journal:
    if not args.resume:
        journal.append({"kind": "clock", "expired": ["test_intent"]})
    else:
        assert list(journal.events())[0]["kind"] == "clock"
    if not args.resume or args.kill_resumed:
        os.kill(os.getpid(), signal.SIGKILL)
    journal.complete()
    (args.output / "status.json").write_text(json.dumps({
        "state": "complete",
        "result": "inconclusive",
        "comparison": {"complete": True, "source_unchanged": True, "freeze_matches": True},
    }))
"""


class RecoveryScheduleTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        now = time.time()
        self.spec: dict[str, Any] = {
            "start_epoch": now - 1,
            "end_epoch": now + 30,
            "source_commit": "approved",
            "source_digest": "expected",
            "argv": [sys.executable, "-c", RECOVERY_CHILD, "--output", str(self.output / "run")],
            "recover": True,
        }
        sleep = time.sleep
        self.enterContext(patch.object(schedule, "command", return_value="approved"))
        self.enterContext(patch.object(schedule, "tree_digest", return_value="expected"))
        self.enterContext(patch.object(schedule.time, "sleep", side_effect=lambda _: sleep(0.02)))

    def status(self) -> dict[str, Any]:
        return json.loads((self.output / "status.json").read_text())

    def test_killed_child_resumes_same_journal_and_completes_once(self) -> None:
        self.assertEqual(schedule.run_schedule(self.output, self.spec), 0)
        status = self.status()
        self.assertEqual(status["state"], "complete")
        self.assertEqual(status["restarts"][0]["job_exit"]["returncode"], -9)
        self.assertEqual(status["restarts"][0]["cursor"], 1)
        self.assertEqual(status["job_exit"]["returncode"], 0)
        self.assertTrue((self.output / "attempts/2/run/status.json").exists())
        with patch.object(schedule, "start_job") as start:
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 0)
            start.assert_not_called()

    def test_failure_at_same_cursor_does_not_restart_again(self) -> None:
        self.spec["argv"].append("--kill-resumed")
        self.assertEqual(schedule.run_schedule(self.output, self.spec), 1)
        status = self.status()
        self.assertEqual(status["state"], "failed")
        self.assertEqual(status["reason"], "Journal cursor did not advance")
        self.assertEqual(len(status["restarts"]), 1)
        self.assertFalse((self.output / "attempts/3").exists())

    def test_controller_reattaches_before_and_after_restart_claim(self) -> None:
        start_job = schedule.start_job

        def interrupt_before_start(directory: Path, argv: list[str], cwd: Path) -> None:
            if "--resume" in argv:
                raise KeyboardInterrupt
            start_job(directory, argv, cwd)

        def interrupt_after_start(directory: Path, argv: list[str], cwd: Path) -> None:
            start_job(directory, argv, cwd)
            raise KeyboardInterrupt

        with (
            patch.object(schedule, "start_job", side_effect=interrupt_before_start),
            self.assertRaises(KeyboardInterrupt),
        ):
            schedule.run_schedule(self.output, self.spec)
        self.assertEqual(self.status()["state"], "restarting")
        with (
            patch.object(schedule, "start_job", side_effect=interrupt_after_start),
            self.assertRaises(KeyboardInterrupt),
        ):
            schedule.run_schedule(self.output, self.spec)
        self.assertEqual(self.status()["state"], "restarting")
        self.assertTrue((self.output / "attempts/2/job/start.claim").exists())
        with patch.object(schedule, "start_job") as start:
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 0)
            start.assert_not_called()

    def test_deadline_after_child_exit_blocks_recovery(self) -> None:
        job = self.output / "job"
        job.mkdir()
        (job / "start.claim").write_text("123\n")
        (job / "exit.json").write_text('{"returncode": -9}')
        self.spec["end_epoch"] = time.time() - 1
        self.assertEqual(schedule.run_schedule(self.output, self.spec), 1)
        self.assertEqual(self.status()["state"], "incomplete")
        self.assertFalse((self.output / "attempts").exists())

    def test_recovery_refuses_missing_or_corrupt_journal(self) -> None:
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as directory:
                output = Path(directory)
                job = output / "job"
                job.mkdir()
                (job / "start.claim").write_text("123\n")
                (job / "exit.json").write_text('{"returncode": -9}')
                if corrupt:
                    (output / "journal.sqlite3").write_text("invalid journal")
                self.assertEqual(schedule.run_schedule(output, self.spec), 1)
                self.assertFalse((output / "attempts").exists())
                status = json.loads((output / "status.json").read_text())
                self.assertEqual(status["state"], "blocked")

    def test_stop_and_auth_failure_are_terminal(self) -> None:
        for code, run in (
            (-15, {"state": "running"}),
            (1, {"state": "interrupted"}),
            (1, {"state": "failed", "error_type": "ApiError", "api_status": 401}),
        ):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                output = Path(directory)
                job = output / "job"
                job.mkdir()
                (job / "start.claim").write_text("123\n")
                (job / "exit.json").write_text(json.dumps({"returncode": code}))
                run_output = output / "run"
                run_output.mkdir()
                (run_output / "status.json").write_text(json.dumps(run))
                self.assertEqual(schedule.run_schedule(output, self.spec), 1)
                self.assertFalse((output / "attempts").exists())
                status = json.loads((output / "status.json").read_text())
                self.assertEqual(status["state"], "failed")

    def test_saved_restart_requires_matching_previous_exit(self) -> None:
        job = self.output / "job"
        job.mkdir()
        (job / "start.claim").write_text("123\n")
        (job / "exit.json").write_text('{"returncode": -15}')
        schedule.atomic_json(
            self.output / "status.json",
            {
                "schedule": self.spec,
                "state": "restarting",
                "restarts": [{"cursor": 1, "journal_hash": "test", "job_exit": {"returncode": -9}}],
            },
        )
        with patch.object(schedule, "start_job") as start:
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 1)
            start.assert_not_called()
        self.assertEqual(self.status()["state"], "blocked")
        self.assertEqual(
            self.status()["reason"], "Previous job exit does not match restart decision"
        )

    def test_provider_cooldown_past_deadline_prevents_restart(self) -> None:
        job = self.output / "job"
        job.mkdir()
        (job / "start.claim").write_text("123\n")
        (job / "exit.json").write_text('{"returncode": 1}')
        run = self.output / "run"
        run.mkdir()
        schedule.atomic_json(
            run / "status.json",
            {
                "state": "failed",
                "error_type": "ApiError",
                "api_status": 429,
                "api_retry_at": self.spec["end_epoch"] + 1,
            },
        )
        with AdaptiveJournal.open(
            self.output / "journal.sqlite3", {"test_account": "original"}, resume=False
        ) as journal:
            journal.append({"kind": "clock", "expired": ["test_intent"]})
        with patch.object(schedule, "start_job") as start:
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 1)
            start.assert_not_called()
        self.assertEqual(self.status()["state"], "incomplete")
        self.assertEqual(self.status()["reason"], "API cooldown extends past approved window")

    def test_journal_change_at_same_cursor_invalidates_saved_restart(self) -> None:
        job = self.output / "job"
        job.mkdir()
        (job / "start.claim").write_text("123\n")
        (job / "exit.json").write_text('{"returncode": -9}')
        journal_path = self.output / "journal.sqlite3"
        identity = {"test_account": "original"}
        with AdaptiveJournal.open(journal_path, identity, resume=False) as journal:
            journal.append({"kind": "clock", "expired": ["test_intent"]})
        with (
            patch.object(schedule, "start_job", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            schedule.run_schedule(self.output, self.spec)
        with AdaptiveJournal.open(journal_path, identity, resume=True) as journal:
            journal.append({"kind": "clock"})
        with patch.object(schedule, "start_job") as start:
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 1)
            start.assert_not_called()
        self.assertEqual(self.status()["state"], "blocked")
        self.assertEqual(self.status()["reason"], "Journal changed after restart decision")

    def test_dynamic_attempts_reuse_the_same_spool_and_keep_prior_evidence(self) -> None:
        self.spec["dynamic_slots"] = True
        command = SlotCommand("request-1", "stop", "slot")
        spool = self.output / SPOOL_NAME
        first_output = self.output / "run"

        def start(job: Path, argv: list[str], _cwd: Path) -> None:
            job.mkdir(parents=True)
            (job / "start.claim").touch()
            self.assertEqual(argv[argv.index("--slot-control") + 1], str(spool))
            run = Path(argv[argv.index("--output") + 1])
            run.mkdir(parents=True)
            resume = "--resume" in argv
            with (
                SlotControl.open(spool) as control,
                AdaptiveJournal.open(
                    self.output / "journal.sqlite3",
                    {
                        "experiment": "shared_feed_slots",
                        "slot_control": str(spool.resolve()),
                        "end_epoch": self.spec["end_epoch"],
                    },
                    resume=resume,
                ) as journal,
            ):
                if not resume:
                    control.submit(command, end_epoch=self.spec["end_epoch"])
                    journal.append({"kind": "clock"})
                    (run / "status.json").write_text('{"state": "running"}')
                    (job / "exit.json").write_text('{"returncode": -9}')
                    return
                self.assertEqual(control.pending(), [command])
                control.record(command, "rejected", {"reason": "Unknown slot"})
                journal.complete()
                evidence = journal.evidence()
            schedule.atomic_json(
                run / "status.json",
                {
                    "state": "complete",
                    "result": "inconclusive",
                    "experiment": "shared_feed_slots",
                    "shared_feed": {"complete": True, "source_unchanged": True},
                    "shared_slots": [],
                    "journal": evidence,
                },
            )
            (job / "exit.json").write_text('{"returncode": 0}')

        with patch.object(schedule, "start_job", side_effect=start) as started:
            self.assertEqual(schedule.run_schedule(self.output, self.spec), 0)
            self.assertEqual(started.call_count, 2)
        self.assertEqual((first_output / "status.json").read_text(), '{"state": "running"}')
        with SlotControl.open(spool) as control:
            self.assertEqual(control.pending(), [])
            report = control.report()
            self.assertTrue(report["closed"])
            self.assertEqual(report["commands"][0]["disposition"], "rejected")
