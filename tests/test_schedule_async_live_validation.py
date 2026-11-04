"""Offline tests for the one-shot validation schedule."""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import schedule_async_live_validation as schedule


class ScheduleTest(unittest.TestCase):
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
