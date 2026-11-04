"""Deterministic orchestration, waiting and publication safety regressions."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools import autotest_supervisor as supervisor
from tools.autotest_jobs import atomic_json
from tools.autotest_supervisor import tree_digest as real_tree_digest


class AutotestSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.repo = self.directory / "repo"
        self.repo.mkdir()
        self.inherited = self.directory / "inherited"
        self.inherited.mkdir()
        atomic_json(self.inherited / "status.json", {"state": "running"})
        self.runner = supervisor.Supervisor(
            self.repo,
            self.directory / "output",
            Path("/test/codex"),
            self.inherited,
        )
        self.digest = self.enterContext(patch.object(supervisor, "tree_digest", return_value="A"))
        self.launch = self.enterContext(patch.object(supervisor, "start_job"))
        self.metadata = self.enterContext(
            patch.object(
                supervisor,
                "read_metadata",
                return_value={
                    "quota": {"status": "available", "windows": []},
                    "models": [],
                },
            )
        )
        self.command = self.enterContext(
            patch.object(
                supervisor,
                "command",
                side_effect=AssertionError("Unexpected external command"),
            )
        )
        self.enterContext(patch.object(supervisor, "check_changes", return_value=["tools/task.py"]))
        self.enterContext(patch.object(supervisor, "changed_paths", return_value=["tools/task.py"]))

    @staticmethod
    def review(**changes: Any) -> dict[str, Any]:
        return {
            "decision": "continue",
            "summary": "More evidence is needed.",
            "findings": [],
            "evidence": ["test-0/exit.json"],
            "satisfied": [],
            "next_objective": "Shared input",
            "publish_ok": False,
            **changes,
        }

    @staticmethod
    def plan(**changes: Any) -> dict[str, Any]:
        return {
            "kind": "offline",
            "objective": "Verify independent account inputs",
            "acceptance": ["No duplicate fills"],
            "requirement": "shared_input",
            "live_duration_seconds": 360,
            **changes,
        }

    def completed_review(self, value: dict[str, Any], **state: Any) -> None:
        job = self.runner.output / "completed-review"
        job.mkdir()
        atomic_json(job / "spec.json", {})
        (job / "stdout.log").write_text(
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": json.dumps(value)},
                }
            )
            + "\n"
        )
        self.runner.transition(
            "model_wait",
            role="review",
            job=str(job),
            review_digest="A",
            tested_digest="A",
            tests_passed=True,
            **state,
        )

    def test_schema_rejects_extra_fields_wrong_types_and_unknown_requirements(self) -> None:
        worker = {"status": "ready", "summary": "Ready", "experiment": "", "evidence": []}
        valid = {"plan": self.plan(), "review": self.review(), "implement": worker}
        for role, value in valid.items():
            supervisor.validate_result(role, value)
            for malformed in (
                {**value, "extra": True},
                {**value, "summary" if role != "plan" else "objective": 1},
            ):
                with self.subTest(role=role, malformed=malformed), self.assertRaises(ValueError):
                    supervisor.validate_result(role, malformed)
        for role, value in [
            ("plan", self.plan(requirement="credentials")),
            ("plan", self.plan(live_duration_seconds=True)),
            ("plan", self.plan(live_duration_seconds=359)),
            ("review", self.review(satisfied=["credentials"])),
            ("review", self.review(publish_ok=1)),
            ("review", self.review(evidence=[123])),
        ]:
            with self.subTest(role=role, value=value), self.assertRaises(ValueError):
                supervisor.validate_result(role, value)

    def test_model_commands_use_requested_roles_and_sandboxes(self) -> None:
        expected = {
            "plan": ("gpt-6-luna", "read-only"),
            "implement": ("gpt-6.1-sol", "workspace-write"),
            "review": ("gpt-6-astra", "read-only"),
        }
        for role, (model, sandbox) in expected.items():
            with self.subTest(role=role):
                argv = supervisor.model_command(
                    Path("/test/codex"), role, self.repo, self.directory
                )
                self.assertEqual(argv[argv.index("--model") + 1], model)
                self.assertEqual(argv[argv.index("--sandbox") + 1], sandbox)
                self.assertEqual(argv[argv.index("--cd") + 1], str(self.repo))
                self.assertIn('approval_policy="never"', argv)
                self.assertIn('web_search="disabled"', argv)
                self.assertIn("sandbox_workspace_write.network_access=false", argv)
                self.assertIn("mcp_servers={}", argv)
                self.assertIn("--ephemeral", argv)
                disabled = {
                    argv[index + 1] for index, value in enumerate(argv) if value == "--disable"
                }
                self.assertIn("multi_agent", disabled)
                self.assertIn("apps", disabled)
                self.assertNotIn("shell_tool", disabled)
                self.assertEqual(argv[-1], "-")

    def test_saved_scope_can_resume_but_old_scope_cannot_be_reinterpreted(self) -> None:
        resumed = supervisor.Supervisor(
            self.repo, self.runner.output, Path("/test/codex"), self.inherited
        )
        self.assertEqual(resumed.state["phase"], "inherit")
        saved = json.loads(self.runner.path.read_text())
        del saved["requirements"]
        atomic_json(self.runner.path, saved)
        before = self.runner.path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "scope differs"):
            supervisor.Supervisor(
                self.repo, self.runner.output, Path("/test/codex"), self.inherited
            )
        self.assertEqual(self.runner.path.read_bytes(), before)
        self.launch.assert_not_called()

    def test_live_job_uses_frozen_baseline_and_two_account_policy(self) -> None:
        trusted = self.runner.output / "trusted-live"
        trusted.mkdir()
        self.runner.state.update(trusted_live_digest="A", plan=self.plan(kind="live"))
        self.runner.live_start()
        job, argv, cwd = self.launch.call_args.args
        self.assertEqual(cwd, trusted)
        self.assertEqual(argv[argv.index("--candidate-policy") + 1], "trend")
        self.assertEqual(argv[argv.index("--capital") + 1], "100")
        self.assertEqual(self.runner.state["phase"], "live_wait")
        self.assertEqual(self.runner.state["live_cwd"], str(trusted))
        self.assertEqual(self.runner.state["job"], str(job))
        self.command.assert_not_called()

    def test_inherited_running_run_waits_then_triggers_one_review(self) -> None:
        with patch.object(self.runner, "invoke") as invoke:
            for _ in range(4):
                self.runner.step()
            self.assertEqual(self.runner.state["phase"], "inherit")
            invoke.assert_not_called()
            self.launch.assert_not_called()
            atomic_json(self.inherited / "status.json", {"state": "complete"})
            self.runner.step()
            self.assertEqual(self.runner.state["phase"], "review")
            invoke.assert_not_called()
            self.runner.step()
            invoke.assert_called_once_with("review")
        self.command.assert_not_called()

    def test_test_wait_polls_without_models_or_relaunching(self) -> None:
        path = self.directory / "test-job"
        atomic_json(path / "spec.json", {})
        self.runner.transition(
            "test_wait",
            tested_digest="A",
            test_index=0,
            test_jobs=[
                {
                    "path": str(path),
                    "argv": ["test-command"],
                    "started": True,
                }
            ],
        )
        with (
            patch.object(supervisor, "poll_job", return_value=None) as poll,
            patch.object(
                self.runner,
                "invoke",
            ) as invoke,
        ):
            for _ in range(4):
                self.runner.step()
            self.assertEqual(poll.call_count, 4)
            poll.assert_called_with(path)
            invoke.assert_not_called()
        self.launch.assert_not_called()
        self.assertEqual(self.runner.state["test_index"], 0)
        with patch.object(supervisor, "poll_job", return_value={"returncode": 3}):
            self.runner.step()
        self.runner.step()
        self.assertEqual(self.runner.state["phase"], "review")
        self.assertFalse(self.runner.state["tests_passed"])

    def test_live_wait_only_polls_the_existing_job(self) -> None:
        path = self.directory / "live-job"
        atomic_json(path / "spec.json", {})
        self.runner.transition("live_wait", job=str(path))
        with patch.object(supervisor, "poll_job", return_value=None) as poll:
            for _ in range(4):
                self.runner.step()
            self.assertEqual(poll.call_count, 4)
        self.launch.assert_not_called()
        self.assertEqual(self.runner.state["phase"], "live_wait")

    def test_planner_block_proposal_requires_review_without_discarding_failed_work(self) -> None:
        self.runner.transition(
            "prepare",
            cycle=1,
            worktree=str(self.repo),
            branch="codex/preserved-fix",
            plan=self.plan(kind="blocked", objective="Correct the failed assertion"),
            review=self.review(decision="fix"),
            tested_digest="A",
            tests_passed=False,
            test_jobs=[{"returncode": 1}],
        )
        self.runner.step()
        self.assertEqual(self.runner.state["phase"], "review")
        self.assertEqual(self.runner.state["cycle"], 2)
        self.assertEqual(self.runner.state["branch"], "codex/preserved-fix")
        self.assertEqual(self.runner.state["test_jobs"], [{"returncode": 1}])
        self.assertEqual(self.runner.state["tested_digest"], "A")
        with patch.object(self.runner, "invoke") as invoke:
            self.runner.step()
            invoke.assert_called_once_with("review")
        self.launch.assert_not_called()
        self.command.assert_not_called()

    def test_reviewer_confirmed_external_blocker_stops(self) -> None:
        self.completed_review(self.review(decision="blocked", summary="Credentials unavailable"))
        with patch.object(supervisor, "poll_job", return_value={"returncode": 0}):
            self.runner.finish_model()
        self.assertEqual(self.runner.state["phase"], "blocked")
        self.assertEqual(self.runner.state["reason"], "Credentials unavailable")

    def test_failed_checks_cannot_be_overridden_by_astra(self) -> None:
        self.completed_review(self.review(publish_ok=True), worktree=str(self.repo))
        self.runner.state["tests_passed"] = False
        with (
            patch.object(supervisor, "poll_job", return_value={"returncode": 0}),
            self.assertRaisesRegex(RuntimeError, "mechanical checks"),
        ):
            self.runner.finish_model()
        self.assertNotEqual(self.runner.state["phase"], "publish")
        self.command.assert_not_called()

    def test_fix_review_cannot_authorize_publication(self) -> None:
        self.completed_review(self.review(decision="fix", publish_ok=True), worktree=str(self.repo))
        with (
            patch.object(supervisor, "poll_job", return_value={"returncode": 0}),
            self.assertRaisesRegex(RuntimeError, "Contradictory review"),
        ):
            self.runner.finish_model()
        self.assertNotEqual(self.runner.state["phase"], "publish")
        self.command.assert_not_called()

    def test_blocked_review_cannot_authorize_publication(self) -> None:
        self.completed_review(
            self.review(decision="blocked", publish_ok=True), worktree=str(self.repo)
        )
        with (
            patch.object(supervisor, "poll_job", return_value={"returncode": 0}),
            self.assertRaisesRegex(RuntimeError, "Contradictory review"),
        ):
            self.runner.finish_model()
        self.assertNotEqual(self.runner.state["phase"], "publish")
        self.command.assert_not_called()

    def test_failed_checks_cannot_be_marked_complete(self) -> None:
        self.completed_review(
            self.review(decision="complete", satisfied=list(supervisor.REQUIREMENTS))
        )
        self.runner.state["tests_passed"] = False
        self.runner.state["test_jobs"] = [{"returncode": 1}]
        with (
            patch.object(supervisor, "poll_job", return_value={"returncode": 0}),
            self.assertRaisesRegex(RuntimeError, "failed mechanical checks"),
        ):
            self.runner.finish_model()
        self.assertNotEqual(self.runner.state["phase"], "complete")
        self.command.assert_not_called()

    def test_passing_continue_review_can_authorize_publication(self) -> None:
        self.completed_review(self.review(publish_ok=True), worktree=str(self.repo))
        with patch.object(supervisor, "poll_job", return_value={"returncode": 0}):
            self.runner.finish_model()
        self.assertEqual(self.runner.state["phase"], "publish")
        self.assertEqual(self.runner.state["publication_digest"], "A")
        self.command.assert_not_called()

    def test_inherited_evidence_can_complete_without_scheduled_checks(self) -> None:
        self.completed_review(
            self.review(decision="complete", satisfied=list(supervisor.REQUIREMENTS))
        )
        del self.runner.state["tests_passed"]
        with patch.object(supervisor, "poll_job", return_value={"returncode": 0}):
            self.runner.finish_model()
        self.assertEqual(self.runner.state["phase"], "complete")
        self.command.assert_not_called()

    def test_source_change_after_test_preparation_blocks_start(self) -> None:
        self.runner.prepare_tests("")
        self.digest.return_value = "B"
        with self.assertRaisesRegex(RuntimeError, "Source changed"):
            self.runner.step()
        self.launch.assert_not_called()

    def test_source_change_after_last_test_blocks_review(self) -> None:
        self.runner.transition(
            "test_wait",
            tested_digest="A",
            test_index=1,
            test_jobs=[
                {
                    "path": "test-0",
                    "started": True,
                    "returncode": 0,
                }
            ],
        )
        self.digest.return_value = "B"
        with self.assertRaisesRegex(RuntimeError, "Source changed"):
            self.runner.step()
        self.assertNotEqual(self.runner.state["phase"], "review")

    def test_source_change_between_tests_and_review_blocks_publication(self) -> None:
        self.completed_review(self.review(publish_ok=True), worktree=str(self.repo))
        self.runner.state["review_digest"] = "B"
        self.digest.return_value = "B"
        with (
            patch.object(supervisor, "poll_job", return_value={"returncode": 0}),
            self.assertRaises(RuntimeError),
        ):
            self.runner.finish_model()
        self.assertNotEqual(self.runner.state["phase"], "publish")
        self.command.assert_not_called()

    def test_source_change_during_review_is_rejected(self) -> None:
        self.completed_review(self.review())
        self.digest.return_value = "B"
        with (
            patch.object(supervisor, "poll_job", return_value={"returncode": 0}),
            self.assertRaisesRegex(RuntimeError, "Source changed"),
        ):
            self.runner.finish_model()

    def test_source_change_after_review_blocks_all_publication_commands(self) -> None:
        self.runner.transition("publish", publication_digest="A")
        self.digest.return_value = "B"
        with self.assertRaisesRegex(RuntimeError, "source changed"):
            self.runner.step()
        self.command.assert_not_called()

    def test_staged_language_failure_prevents_commit_and_github_publication(self) -> None:
        self.runner.state.update(publication_digest="A", tested_digest="A", base="base-sha")

        def run(argv: list[str], cwd: Path) -> str:
            if argv[:2] == ["git", "ls-files"]:
                return "tools/task.py"
            if argv[:2] == ["git", "rev-parse"]:
                return "base-sha"
            if argv[0] == sys.executable:
                self.assertTrue(argv[1].endswith("scripts/check_staged_language.py"))
                raise subprocess.CalledProcessError(1, argv)
            return ""

        self.command.side_effect = run
        with self.assertRaises(subprocess.CalledProcessError):
            self.runner.publish()
        commands = [call.args[0] for call in self.command.call_args_list]
        self.assertFalse(any(argv[:2] == ["git", "commit"] for argv in commands))
        self.assertFalse(any(argv[0] == "gh" for argv in commands))

    def test_publication_uses_requirement_and_paths_without_copying_model_prose(self) -> None:
        self.runner.state.update(
            publication_digest="A",
            tested_digest="A",
            base="base-sha",
            branch="codex/verified-fix",
            plan=self.plan(),
            worker={"summary": "worker-private-prose", "experiment": ""},
            review=self.review(summary="review-private-prose"),
        )
        cycle = self.runner.output / "cycle-0000"
        cycle.mkdir()

        def run(argv: list[str], cwd: Path) -> str:
            if argv[:2] == ["git", "ls-files"]:
                return "tools/task.py"
            if argv[:2] == ["git", "rev-parse"]:
                return "base-sha"
            if argv[:3] == ["gh", "pr", "create"]:
                return "https://github.com/nonpapfif/papertrade-lab/pull/1"
            return ""

        self.command.side_effect = run
        self.runner.publish()
        body = (cycle / "pr-body.md").read_text()
        self.assertIn(supervisor.REQUIREMENTS["shared_input"], body)
        self.assertIn("tools/task.py", body)
        self.assertIn("No separate offline experiment was run", body)
        self.assertNotIn("private-prose", body)
        self.assertEqual(self.runner.state["phase"], "pr_wait")

    def test_incomplete_requirements_cannot_be_marked_complete(self) -> None:
        self.completed_review(self.review(decision="complete", satisfied=["shared_input"]))
        with (
            patch.object(supervisor, "poll_job", return_value={"returncode": 0}),
            self.assertRaisesRegex(RuntimeError, "pending requirements"),
        ):
            self.runner.finish_model()
        self.assertNotEqual(self.runner.state["phase"], "complete")

    def test_fix_cycle_keeps_unpublished_worktree_and_branch(self) -> None:
        worktree = self.directory / "failed-worktree"
        worktree.mkdir()
        marker = worktree / "unpublished.py"
        marker.write_text("VALUE = 7\n")
        self.runner.transition(
            "prepare",
            worktree=str(worktree),
            branch="autotest/preserved",
            base="initial-sha",
            cycle=4,
            review=self.review(decision="fix"),
            plan=self.plan(),
            tests_passed=False,
            test_jobs=[{"returncode": 1}],
        )
        self.runner.prepare()
        self.assertEqual(self.runner.state["phase"], "implement")
        self.assertEqual(self.runner.state["cycle"], 5)
        self.assertEqual(self.runner.worktree, worktree)
        self.assertEqual(self.runner.state["branch"], "autotest/preserved")
        self.assertEqual(self.runner.state["base"], "initial-sha")
        self.assertEqual(marker.read_text(), "VALUE = 7\n")
        self.assertEqual(self.runner.state["test_jobs"], [])
        self.assertFalse(self.runner.state["tests_passed"])
        self.command.assert_not_called()

    def test_unpublished_fix_cannot_switch_to_live_worktree(self) -> None:
        self.runner.transition(
            "prepare",
            worktree=str(self.repo),
            branch="autotest/preserved",
            review=self.review(decision="fix"),
            plan=self.plan(kind="live"),
        )
        with self.assertRaisesRegex(RuntimeError, "offline first"):
            self.runner.prepare()
        self.assertEqual(self.runner.state["branch"], "autotest/preserved")
        self.assertEqual(self.runner.worktree, self.repo)
        self.command.assert_not_called()

    def test_latest_review_can_invalidate_earlier_satisfied_requirements(self) -> None:
        self.runner.state["satisfied"] = list(supervisor.REQUIREMENTS)
        self.completed_review(self.review(satisfied=["shared_input"]))
        with patch.object(supervisor, "poll_job", return_value={"returncode": 0}):
            self.runner.finish_model()
        self.assertEqual(self.runner.state["satisfied"], ["shared_input"])
        self.assertEqual(self.runner.state["phase"], "plan")
        self.metadata.assert_called_once()

    def test_complete_requires_full_current_requirement_set(self) -> None:
        self.completed_review(
            self.review(
                decision="complete",
                satisfied=list(supervisor.REQUIREMENTS),
            )
        )
        with patch.object(supervisor, "poll_job", return_value={"returncode": 0}):
            self.runner.finish_model()
        self.assertEqual(self.runner.state["phase"], "complete")
        self.assertEqual(set(self.runner.state["satisfied"]), set(supervisor.REQUIREMENTS))

    def test_tree_digest_ignores_umask_but_detects_executable_changes(self) -> None:
        source = self.repo / "fixture.py"
        source.write_text("VALUE = 7\n")
        with patch.object(supervisor, "command", return_value=source.name):
            source.chmod(0o644)
            ordinary = real_tree_digest(self.repo)
            source.chmod(0o600)
            self.assertEqual(real_tree_digest(self.repo), ordinary)
            source.chmod(0o700)
            self.assertNotEqual(real_tree_digest(self.repo), ordinary)
        self.assertEqual(supervisor.tree_digest(self.repo), "A")
        self.command.assert_not_called()

    def test_prepare_tests_keeps_old_generated_and_new_changed_test_modules(self) -> None:
        self.runner.state["generated_files"] = [
            "tests/test_old_generated.py",
            "tests/test_new_generated.py",
        ]
        with patch.object(
            supervisor,
            "changed_paths",
            return_value=[
                "tests/test_new_generated.py",
                "docs/review.md",
                "tests/test_async_layered_paper.py",
            ],
        ):
            self.runner.prepare_tests("")
        jobs = self.runner.state["test_jobs"]
        self.assertEqual(len(jobs), 1)
        argv = jobs[0]["argv"]
        self.assertEqual(argv[:4], [sys.executable, "-m", "unittest", "-q"])
        self.assertEqual(argv.count("tests.test_old_generated"), 1)
        self.assertEqual(argv.count("tests.test_new_generated"), 1)
        self.assertEqual(argv.count("tests.test_async_layered_paper"), 1)
        self.assertTrue(set(supervisor.REGRESSION_MODULES) <= set(argv[4:]))
        self.assertNotIn("docs.review", argv)
        self.assertFalse(jobs[0]["started"])
        self.launch.assert_not_called()

    def test_ensure_started_recovers_unclaimed_job_without_repeating_reserved_jobs(self) -> None:
        job = self.directory / "not-yet-started"
        argv = [sys.executable, "-c", "print('test')"]
        source = self.directory / "prompt.txt"

        def reserve(directory: Path, *args: Any, **kwargs: Any) -> None:
            directory.mkdir()
            (directory / "start.claim").write_text("reserved\n")

        self.launch.side_effect = reserve
        supervisor.ensure_started(job, argv, self.repo, stdin=source)
        supervisor.ensure_started(job, argv, self.repo, stdin=source)
        self.launch.assert_called_once_with(job, argv, self.repo, stdin=source)
        claimed = self.directory / "previously-claimed"
        claimed.mkdir()
        (claimed / "start.claim").write_text("old reservation\n")
        supervisor.ensure_started(claimed, argv, self.repo)
        documented = self.directory / "previously-documented"
        atomic_json(documented / "spec.json", {})
        supervisor.ensure_started(documented, argv, self.repo)
        self.assertEqual(self.launch.call_count, 1)

    def test_identical_model_invocation_is_not_launched_twice(self) -> None:
        self.runner.invoke("plan")
        self.launch.assert_called_once()
        with self.assertRaisesRegex(RuntimeError, "repeat a model invocation"):
            self.runner.invoke("plan")
        self.launch.assert_called_once()


@unittest.skipUnless(
    sys.platform == "darwin" and Path("/usr/bin/sandbox-exec").is_file(),
    "Requires the macOS sandbox used by offline jobs",
)
class OfflineSandboxTests(unittest.TestCase):
    def test_offline_worker_cannot_write_supervisor_state_or_open_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            worktree, artifacts, scratch = (
                root / name for name in ("worktree", "artifacts", "scratch")
            )
            for directory in (worktree, artifacts, scratch):
                directory.mkdir()
            control = root / "status.json"
            control.write_text('{"phase":"test_wait"}\n')
            script = textwrap.dedent("""\
                import pathlib
                import socket
                import sys

                for value in sys.argv[1:4]:
                    (pathlib.Path(value) / "allowed.txt").write_text("allowed")
                try:
                    pathlib.Path(sys.argv[4]).write_text("forged")
                except PermissionError:
                    pass
                else:
                    raise AssertionError("Worker changed supervisor control state")
                try:
                    with socket.socket() as connection:
                        connection.bind(("127.0.0.1", 0))
                except PermissionError:
                    pass
                else:
                    raise AssertionError("Offline worker opened a network socket")
                print("permissions-ok")
                """)
            environment = supervisor.codex_environment()
            environment.update(TMPDIR=str(scratch), PYTHONDONTWRITEBYTECODE="1")
            result = subprocess.run(
                [
                    "/usr/bin/sandbox-exec",
                    "-p",
                    supervisor.offline_profile(worktree, artifacts, scratch),
                    sys.executable,
                    "-c",
                    script,
                    str(worktree),
                    str(artifacts),
                    str(scratch),
                    str(control),
                ],
                cwd=worktree,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            if (
                result.returncode == 71
                and "sandbox_apply: Operation not permitted" in result.stderr
            ):
                self.skipTest("macOS cannot apply a nested sandbox; verify outside the worker")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "permissions-ok")
            self.assertEqual(control.read_text(), '{"phase":"test_wait"}\n')
            for directory in (worktree, artifacts, scratch):
                self.assertEqual((directory / "allowed.txt").read_text(), "allowed")


if __name__ == "__main__":
    unittest.main()
