"""Exercise offline scope authority and source-bound completion evidence."""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.autotest_jobs import atomic_json
from tools.autotest_supervisor import Supervisor, check_changes, offline_profile, read_json
from tools.supervisor_scopes import HOST_ONLY_TEST_IDS, REFACTOR_REQUIREMENTS, REFACTOR_SCOPE


class RefactorSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name).resolve()
        self.inherited = self.root / "r4"
        self.inherited.mkdir()
        atomic_json(self.inherited / "status.json", {"state": "failed"})
        self.runner = Supervisor(
            self.root,
            self.root / "audit",
            Path("/test/codex"),
            self.inherited,
            scope=REFACTOR_SCOPE,
        )
        self.addCleanup(patch.stopall)
        patch("tools.autotest_supervisor.tree_digest", return_value="source").start()
        self.paths = patch("tools.autotest_supervisor.changed_paths", return_value=[]).start()
        self.git = patch("tools.autotest_supervisor.command", return_value="same-tree").start()

    def test_starts_with_review_and_rejects_reinterpreting_saved_scope(self) -> None:
        self.assertEqual(self.runner.state["phase"], "review")
        self.assertEqual(self.runner.requirements, REFACTOR_REQUIREMENTS)
        for scope in ("finite", "recurring-paper"):
            with self.assertRaisesRegex(RuntimeError, "scope differs"):
                Supervisor(
                    self.root,
                    self.runner.output,
                    Path("/test/codex"),
                    self.inherited,
                    scope=scope,
                )

    def test_runner_edit_is_scope_specific_and_protected_files_stay_protected(self) -> None:
        paths = [
            "tools/run_async_live_validation.py",
            "tools/live_feed.py",
            "tools/paper_campaign/review.py",
        ]
        self.git.return_value = "\n".join(paths)
        self.paths.return_value = paths
        self.assertEqual(check_changes(self.root, scope=REFACTOR_SCOPE), paths)
        for scope in ("finite", "recurring-paper"):
            with self.assertRaisesRegex(RuntimeError, "supervisor authority"):
                check_changes(self.root, scope=scope)
        for name in (
            "tools/autotest_supervisor.py",
            "tools/autotest_jobs.py",
            "tools/supervisor_scopes.py",
            "tests/test_refactor_supervisor.py",
            "papertrade/api.py",
            "papertrade/core.py",
            "tools/probe_toss_websocket.py",
            "AGENTS.md",
            "pyproject.toml",
            "docs/concurrent-refactor.md",
        ):
            with self.subTest(name=name):
                self.paths.return_value = [name]
                with self.assertRaisesRegex(RuntimeError, "supervisor authority"):
                    check_changes(self.root, generated=[name], scope=REFACTOR_SCOPE)

    def test_cannot_plan_launch_or_resume_live_job(self) -> None:
        self.runner.state["plan"] = {"kind": "live"}
        with self.assertRaisesRegex(RuntimeError, "forbids live"):
            self.runner.prepare()
        with self.assertRaisesRegex(RuntimeError, "forbids live"):
            self.runner.live_start()
        self.runner.state.update(phase="live_wait", live_argv=["forbidden"])
        with self.assertRaisesRegex(RuntimeError, "forbids live"):
            self.runner.step()
        self.git.assert_not_called()

    def test_suite_keeps_previous_generated_tests_and_discloses_partition(self) -> None:
        self.runner.state["generated_files"] = ["tests/test_previous_gateway.py"]
        self.runner.save()
        resumed = Supervisor(
            self.root,
            self.runner.output,
            Path("/test/codex"),
            self.inherited,
            scope=REFACTOR_SCOPE,
        )
        self.paths.return_value = ["tests/test_current_concurrency.py"]
        resumed.state["plan"] = {"kind": "offline", "tests": []}
        resumed.prepare_tests("")
        targets = resumed.state["test_jobs"][0]["argv"][4:]
        self.assertIn("tests.test_previous_gateway", targets)
        self.assertIn("tests.test_current_concurrency", targets)
        self.assertIn("tests.test_refactor_supervisor", targets)
        self.assertEqual(set(resumed.state["host_checks_not_run"]), HOST_ONLY_TEST_IDS)
        self.assertFalse(HOST_ONLY_TEST_IDS & set(targets))

    def completed_evidence(self) -> Path:
        state = self.runner.state
        state.update(
            cycle=1,
            tested_digest="source",
            review_digest="source",
            tests_passed=True,
            satisfied=sorted(REFACTOR_REQUIREMENTS),
            review={"decision": "complete", "satisfied": sorted(REFACTOR_REQUIREMENTS)},
            host_checks_not_run=sorted(HOST_ONLY_TEST_IDS),
        )
        cycle = self.runner.output / "cycle-0001"
        job = cycle / "test-0"
        job.mkdir(parents=True)
        argv = [sys.executable, "-m", "unittest", "-q", "tests.test_refactor_supervisor"]
        state["test_jobs"] = [
            {"path": str(job), "argv": argv, "started": True, "returncode": 0, "skipped": False}
        ]
        profile = offline_profile(self.root, cycle / "experiment", job / "scratch")
        atomic_json(
            job / "spec.json",
            {
                "argv": ["/usr/bin/sandbox-exec", "-p", profile, *argv],
                "cwd": str(self.root),
            },
        )
        atomic_json(job / "exit.json", {"returncode": 0})
        (job / "stderr.log").write_text("Ran 9 tests in 0.1s\n\nOK\n")
        review = cycle / "review"
        review.mkdir()
        atomic_json(review / "result.json", state["review"])
        atomic_json(review / "evidence.json", self.runner.evidence())
        atomic_json(review / "exit.json", {"returncode": 0})
        return cycle

    def test_completes_offline_without_mutating_or_requiring_inherited_success(self) -> None:
        self.completed_evidence()
        self.runner.after_review()
        report = read_json(Path(self.runner.state["refactor_report"]))
        self.assertEqual(self.runner.state["phase"], "complete")
        self.assertEqual(report["source_digest"], "source")
        self.assertTrue(report["live_validation_pending"])
        self.assertNotIn("service_verified", report)
        self.assertEqual(set(report["host_checks_not_run"]), HOST_ONLY_TEST_IDS)
        self.assertEqual(read_json(self.inherited / "status.json"), {"state": "failed"})

    def test_completion_rejects_missing_stale_and_skipped_evidence(self) -> None:
        cycle = self.completed_evidence()
        state = self.runner.state
        jobs = state.pop("test_jobs")
        with self.assertRaisesRegex(RuntimeError, "controller-owned"):
            self.runner.after_review()
        state["test_jobs"] = jobs
        job = cycle / "test-0"
        (job / "exit.json").unlink()
        with self.assertRaisesRegex(RuntimeError, "fresh passing"):
            self.runner.after_review()
        atomic_json(job / "exit.json", {"returncode": 1})
        with self.assertRaisesRegex(RuntimeError, "do not match"):
            self.runner.after_review()
        atomic_json(job / "exit.json", {"returncode": 0})
        log = job / "stderr.log"
        for summary in ("Ran 9 tests in 0.1s\nOK (skipped=1)\n", "Ran 0 tests in 0.1s\nOK\n"):
            log.write_text(summary)
            with self.assertRaisesRegex(RuntimeError, "unskipped regression"):
                self.runner.after_review()
        log.write_text("Ran 9 tests in 0.1s\n\nOK\n")
        state["review_digest"] = "stale"
        with self.assertRaisesRegex(RuntimeError, "differs"):
            self.runner.after_review()
        state["review_digest"] = "source"
        evidence = cycle / "review" / "evidence.json"
        atomic_json(evidence, {**read_json(evidence), "cycle": 0})
        with self.assertRaisesRegex(RuntimeError, "review evidence is stale"):
            self.runner.after_review()

    def test_completion_requires_all_requirements_and_clean_published_source(self) -> None:
        self.completed_evidence()
        saved = self.runner.state["satisfied"]
        self.runner.state["satisfied"] = saved[:-1]
        with self.assertRaisesRegex(RuntimeError, "omitted pending requirements"):
            self.runner.after_review()
        self.runner.state["satisfied"] = saved
        self.paths.return_value = ["tools/live_feed.py"]
        with self.assertRaisesRegex(RuntimeError, "unpublished changes"):
            self.runner.after_review()
        self.paths.return_value = []
        self.git.side_effect = ["source-commit", "published-tree", "unpublished-tree"]
        with self.assertRaisesRegex(RuntimeError, "published and synced"):
            self.runner.after_review()
        self.assertFalse((self.runner.output / "refactor-report.json").exists())
