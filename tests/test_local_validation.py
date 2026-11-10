"""Local checks must retain evidence and reject incomplete or stale validation."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.local_validation import require_report, validate


class LocalValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.repo = Path(temp.name)
        self.output = self.repo / "audit"
        self.digest = self.enterContext(
            patch("tools.autotest_supervisor.tree_digest", return_value="source")
        )
        self.tools = self.enterContext(
            patch("tools.local_validation.shutil.which", side_effect=lambda name: f"/bin/{name}")
        )
        self.command = self.enterContext(
            patch("tools.local_validation.subprocess.run", side_effect=self.run_command)
        )

    @staticmethod
    def run_command(
        argv: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        summary = b"Ran 10 tests in 0.10s\n\nOK\n" if argv[0] == "/bin/python" else b""
        return subprocess.CompletedProcess(argv, 0, b"", summary)

    def test_records_exact_commands_and_rejects_changed_log(self) -> None:
        report = validate(self.repo, self.output, "python")
        self.assertEqual(report["state"], "passed")
        commands = [call.args[0] for call in self.command.call_args_list]
        self.assertEqual(commands, [
            ["/bin/ruff", "check", "."],
            ["/bin/mypy", "--explicit-package-bases", "papertrade", "tools", "tests"],
            ["/bin/python", "-m", "unittest", "discover", "-s", "tests"],
        ])
        path = self.output / "validation.json"
        require_report(path, "source")
        (self.output / "unittest.stderr.log").write_text("changed")
        with self.assertRaisesRegex(RuntimeError, "log changed"):
            require_report(path, "source")

    def test_missing_tool_and_nonzero_command_fail_the_gate(self) -> None:
        self.tools.side_effect = lambda name: None if name == "ruff" else f"/bin/{name}"

        def fail_mypy(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            if argv[0] == "/bin/mypy":
                return subprocess.CompletedProcess(argv, 1, b"", b"type errors")
            return self.run_command(argv)

        self.command.side_effect = fail_mypy
        result = validate(self.repo, self.output, "python")
        self.assertEqual(result["state"], "failed")
        self.assertEqual([entry["state"] for entry in result["checks"]],
                         ["missing_tool", "failed", "passed"])

    def test_source_change_fails_an_otherwise_passing_validation(self) -> None:
        self.digest.side_effect = ["before", "after"]
        result = validate(self.repo, self.output, "python")
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["final_source_digest"], "after")

    def test_zero_tests_and_skips_cannot_pass(self) -> None:
        summaries = (
            b"Ran 0 tests in 0.1s\n\nOK\n",
            b"Ran 10 tests in 0.1s\n\nOK (skipped=1)\n",
        )
        for index, summary in enumerate(summaries):
            with self.subTest(summary=summary):
                self.command.side_effect = [
                    subprocess.CompletedProcess([], 0, b"", output)
                    for output in (b"", b"", summary)
                ]
                result = validate(self.repo, self.repo / f"bad-{index}", "python")
                self.assertEqual(result["state"], "failed")
                self.assertEqual(result["checks"][-1]["state"], "failed")
