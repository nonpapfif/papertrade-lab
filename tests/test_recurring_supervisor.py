"""A new campaign cannot reinterpret finite evidence or bypass deployment gates."""

import tempfile
import unittest
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

from tools.autotest_jobs import atomic_json
from tools.autotest_supervisor import Supervisor, read_json
from tools.supervisor_scopes import (
    HOST_ONLY_TEST_IDS,
    PARTITIONED_TEST_MODULES,
    RECURRING_REQUIREMENTS,
    partition_recurring_tests,
)

SCHEDULE_MODULE = "tests.test_schedule_async_live_validation"
WEBSOCKET_CLASS = "tests.test_websocket_replay.LocalProtocolTests"


def test_ids(suite: unittest.TestSuite) -> Iterator[str]:
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from test_ids(test)
        else:
            yield test.id()


class RecurringSupervisorTests(unittest.TestCase):
    def test_partition_covers_every_case_in_the_partitioned_modules(self) -> None:
        modules = sorted(PARTITIONED_TEST_MODULES)
        all_ids = set(test_ids(unittest.TestLoader().loadTestsFromNames(modules)))
        offline, host_only = partition_recurring_tests(modules)
        self.assertEqual(len(offline), 14)
        self.assertEqual(len(host_only), 7)
        self.assertEqual(set(host_only), HOST_ONLY_TEST_IDS)
        self.assertFalse(set(offline) & set(host_only))
        self.assertEqual(set(offline) | set(host_only), all_ids)
        self.assertEqual(partition_recurring_tests(list(reversed(modules))), (offline, host_only))

    def test_default_plan_persists_excluded_checks_and_keeps_unknown_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = Supervisor(
                root, root / "audit", Path("/test/codex"), root, scope="recurring-paper"
            )
            unknown = WEBSOCKET_CLASS + ".test_unreviewed_unknown"
            runner.state["plan"] = {"kind": "offline", "tests": [unknown]}
            with (
                patch("tools.autotest_supervisor.changed_paths", return_value=[]),
                patch("tools.autotest_supervisor.tree_digest", return_value="digest"),
            ):
                runner.prepare_tests("")
                evidence = runner.evidence()
            saved = read_json(runner.path)
            self.assertEqual(saved["host_checks_not_run"], sorted(HOST_ONLY_TEST_IDS))
            self.assertEqual(evidence["host_checks_not_run"], saved["host_checks_not_run"])
            targets = saved["test_jobs"][0]["argv"][4:]
            offline, _ = partition_recurring_tests([SCHEDULE_MODULE])
            self.assertTrue(set(offline) <= set(targets))
            self.assertIn("tests.test_recurring_supervisor", targets)
            self.assertIn(unknown, targets)
            self.assertEqual(targets, sorted(set(targets)))
            self.assertFalse(HOST_ONLY_TEST_IDS & set(targets))
            self.assertFalse(PARTITIONED_TEST_MODULES & set(targets))

    def test_explicit_test_plan_partitions_class_and_rejects_host_only_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = Supervisor(
                root, root / "audit", Path("/test/codex"), root, scope="recurring-paper"
            )
            scheduler = SCHEDULE_MODULE + ".ScheduleTest"
            runner.state["plan"] = {"kind": "test", "tests": [scheduler, WEBSOCKET_CLASS]}
            with (
                patch("tools.autotest_supervisor.changed_paths", return_value=[]),
                patch("tools.autotest_supervisor.tree_digest", return_value="digest"),
            ):
                runner.prepare_tests("")
                expected, excluded = partition_recurring_tests([scheduler, WEBSOCKET_CLASS])
                self.assertEqual(runner.state["test_jobs"][0]["argv"][4:], expected)
                self.assertEqual(runner.state["host_checks_not_run"], excluded)
                runner.state["plan"]["tests"] = [WEBSOCKET_CLASS]
                with self.assertRaisesRegex(ValueError, "require separate host validation"):
                    runner.prepare_tests("")
                self.assertEqual(
                    runner.state["host_checks_not_run"],
                    sorted(test for test in HOST_ONLY_TEST_IDS if test.startswith(WEBSOCKET_CLASS)),
                )

    def test_scope_requires_a_new_audit_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            Supervisor(root, root / "audit", Path("/test/codex"), root)
            with self.assertRaisesRegex(RuntimeError, "scope differs"):
                Supervisor(root, root / "audit", Path("/test/codex"), root, scope="recurring-paper")

    def test_completion_requires_real_deployment_and_controller_tests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            inherited = root / "deployment"
            inherited.mkdir()
            runner = Supervisor(
                root, root / "audit", Path("/test/codex"), inherited, scope="recurring-paper"
            )
            self.assertEqual(runner.state["phase"], "review")
            runner.state.update(
                review={"decision": "complete"}, satisfied=list(RECURRING_REQUIREMENTS)
            )
            atomic_json(inherited / "status.json", {"state": "complete"})
            with self.assertRaisesRegex(RuntimeError, "controller-owned tests"):
                runner.after_review()
            runner.state["tests_passed"] = True
            with self.assertRaisesRegex(RuntimeError, "deployment evidence"):
                runner.after_review()
            atomic_json(
                inherited / "status.json",
                {
                    "state": "complete",
                    "real_orders": False,
                    "source_digest": "tested",
                    "service_verified": True,
                },
            )
            with (
                patch("tools.autotest_supervisor.tree_digest", return_value="other"),
                self.assertRaisesRegex(RuntimeError, "deployment evidence"),
            ):
                runner.after_review()
            with patch("tools.autotest_supervisor.tree_digest", return_value="tested"):
                runner.after_review()
            self.assertEqual(runner.state["phase"], "complete")
