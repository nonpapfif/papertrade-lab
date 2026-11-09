"""A new campaign cannot reinterpret finite evidence or bypass deployment gates."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.autotest_jobs import atomic_json
from tools.autotest_supervisor import Supervisor
from tools.supervisor_scopes import RECURRING_REQUIREMENTS


class RecurringSupervisorTests(unittest.TestCase):
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
