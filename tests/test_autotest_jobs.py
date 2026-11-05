"""Detached job recovery and exactly-once launch with local Python commands."""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools.autotest_jobs import ROOT, _child, atomic_json, poll_job, start_job


class AutotestJobsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / "job"

    def wait_for_exit(self) -> dict[str, Any]:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            result = poll_job(self.directory)
            if result is not None:
                return result
            time.sleep(0.02)
        self.fail("Local test job did not finish")

    def test_exit_recovery_and_logs_do_not_repeat_command(self) -> None:
        start_job(
            self.directory,
            [
                sys.executable,
                "-c",
                ("import sys; print('output'); print('error', file=sys.stderr); sys.exit(7)"),
            ],
            ROOT,
        )
        result = self.wait_for_exit()
        self.assertEqual(result["returncode"], 7)
        self.assertEqual(poll_job(self.directory), result)
        self.assertEqual((self.directory / "stdout.log").read_text(), "output\n")
        self.assertEqual((self.directory / "stderr.log").read_text(), "error\n")
        with self.assertRaisesRegex(RuntimeError, "already exists"):
            start_job(self.directory, [sys.executable, "-c", "raise SystemExit(0)"], ROOT)
        self.assertEqual((self.directory / "stdout.log").read_text(), "output\n")

    def test_long_running_poll_never_launches_another_command(self) -> None:
        gate = Path(self.temporary.name) / "release"
        start_job(
            self.directory,
            [
                sys.executable,
                "-c",
                (
                    "import pathlib,time; "
                    f"p=pathlib.Path({str(gate)!r}); "
                    "exec('while not p.exists(): time.sleep(0.01)')"
                ),
            ],
            ROOT,
        )
        try:
            deadline = time.monotonic() + 5
            while not (self.directory / "worker.json").exists():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
            with patch("tools.autotest_jobs.subprocess.Popen", wraps=subprocess.Popen) as launch:
                for _ in range(5):
                    self.assertIsNone(poll_job(self.directory))
                for call in launch.call_args_list:
                    self.assertEqual(call.args[0][0], "/bin/ps")
        finally:
            gate.touch()
        self.assertEqual(self.wait_for_exit()["returncode"], 0)

    def test_stdin_and_private_environment_are_preserved(self) -> None:
        source = Path(self.temporary.name) / "input"
        source.write_text("request")
        start_job(
            self.directory,
            [
                sys.executable,
                "-c",
                ("import os,sys; print(sys.stdin.read()); print(os.environ['JOB_MARKER'])"),
            ],
            ROOT,
            stdin=source,
            environment={"JOB_MARKER": "private-value"},
        )
        self.assertEqual(self.wait_for_exit()["returncode"], 0)
        self.assertEqual((self.directory / "stdout.log").read_text(), "request\nprivate-value\n")
        self.assertNotIn("private-value", (self.directory / "spec.json").read_text())
        self.assertEqual((self.directory / "environment.json").stat().st_mode & 0o777, 0o600)

    def test_cutoff_kills_its_child_if_it_ignores_terminate(self) -> None:
        atomic_json(
            self.directory / "spec.json",
            {
                "argv": [
                    sys.executable,
                    "-c",
                    (
                        "import signal, time; "
                        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                        "print('ready', flush=True); time.sleep(60)"
                    ),
                ],
                "cwd": str(ROOT),
                "stdin": None,
                "stop_after": time.time() + 1,
            },
        )
        atomic_json(self.directory / "environment.json", {})
        with patch("tools.autotest_jobs.STOP_SECONDS", 0.1):
            self.assertEqual(_child(self.directory), 0)
        result = poll_job(self.directory)
        assert result is not None
        self.assertTrue(result["cutoff_stop"])
        self.assertEqual(result["returncode"], -signal.SIGKILL)
        self.assertEqual((self.directory / "stdout.log").read_text(), "ready\n")

    def test_dead_identity_fails_closed(self) -> None:
        atomic_json(
            self.directory / "spec.json",
            {
                "argv": [sys.executable],
                "cwd": str(ROOT),
                "stdin": None,
                "created_at": time.time() - 10,
            },
        )
        atomic_json(self.directory / "worker.json", {"pid": 123, "identity": "old"})
        with (
            patch("tools.autotest_jobs.process_identity", return_value="replacement"),
            self.assertRaisesRegex(RuntimeError, "refusing to rerun"),
        ):
            poll_job(self.directory)
        (self.directory / "worker.json").unlink()
        with self.assertRaisesRegex(RuntimeError, "identity is missing"):
            poll_job(self.directory)

    def test_malformed_spec_and_duplicate_child_fail_closed(self) -> None:
        self.directory.mkdir()
        (self.directory / "spec.json").write_text("malformed")
        with self.assertRaisesRegex(RuntimeError, "Invalid job file"):
            poll_job(self.directory)
        self.assertEqual(_child(self.directory), 1)
        result = json.loads((self.directory / "exit.json").read_text())
        self.assertEqual(result["returncode"], -1)
        self.assertEqual(_child(self.directory), 1)
        self.assertEqual(json.loads((self.directory / "exit.json").read_text()), result)

    def test_missing_target_is_recorded_without_rerunning(self) -> None:
        start_job(self.directory, [str(self.directory / "missing-command")], ROOT)
        result = self.wait_for_exit()
        self.assertEqual(result["returncode"], -1)
        self.assertEqual(result["error_type"], "FileNotFoundError")

    def test_invalid_argv_is_rejected_before_reservation(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "argv"):
            start_job(self.directory, [], ROOT)
        self.assertFalse(self.directory.exists())

    def test_startup_window_can_recover_parent_death(self) -> None:
        atomic_json(
            self.directory / "spec.json",
            {
                "argv": [sys.executable],
                "cwd": str(ROOT),
                "stdin": None,
                "created_at": time.time(),
            },
        )
        self.assertIsNone(poll_job(self.directory))


if __name__ == "__main__":
    unittest.main()
