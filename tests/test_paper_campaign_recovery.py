"""Verify the archived failure, immutable approval and one-time journal continuation."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from papertrade.jsonio import atomic_json, read_json
from tools import adaptive_journal
from tools import schedule_async_live_validation as scheduler
from tools.adaptive_journal import AdaptiveJournal, JournalError, read_resumed_input, read_status
from tools.autotest_supervisor import REPAIR_STATIC_CHECKS, Supervisor
from tools.paper_campaign import recovery, repair_publication
from tools.paper_campaign.controller import Campaign
from tools.paper_campaign.recovery import SessionRecovery
from tools.paper_market import US
from tools.supervisor_scopes import REPAIR_SCOPE


class RepairFixture(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.output = Path(temp.name).resolve()
        self.schedule = self.output / "schedule"
        self.repair = self.output / "engineering-recovery"
        self.journal = self.schedule / "journal.sqlite3"
        self.status = self.schedule / "status.json"
        self.end = time.time() + 120
        self.identity = {
            "experiment": "shared_feed_slots",
            "market": "US",
            "end_epoch": self.end,
            "source_sha256": {"stream.py": "old"},
            "settings_sha256": "frozen",
            "capital": 1000.0,
        }
        self.spec: dict[str, Any] = {
            "start_epoch": self.end - 200,
            "end_epoch": self.end,
            "source_commit": "old-commit",
            "source_digest": "old-source",
            "argv": ["paper-worker", "--output", str(self.schedule / "run")],
            "recover": True,
            "dynamic_slots": True,
            "initial_slots": [],
        }
        self.handoff = {
            "task_id": "a" * 64, "market": "US",
            "source_commit": "old-commit", "source_digest": "old-source",
        }
        self.exit = {"returncode": 1, "finished_at": time.time()}
        self.failure = {
            "schedule": self.spec, "state": "failed", "job_exit": self.exit,
            "restarts": [], "failure_handoff": {"attempt": 1},
        }
        atomic_json(self.status, self.failure)
        atomic_json(self.output / "scheduler" / "exit.json", self.exit)
        job = self.schedule / "job"
        atomic_json(job / "exit.json", self.exit)
        atomic_json(job / "worker.json", {"pid": 123, "identity": "worker"})
        atomic_json(job / "target.json", {"pid": 456, "identity": "target"})
        atomic_json(job / "spec.json", {})
        (job / "start.claim").touch()
        atomic_json(self.schedule / "run" / "status.json", {
            "state": "failed", "error_type": "ValueError",
        })
        with AdaptiveJournal.open(self.journal, self.identity, resume=False) as journal:
            journal.append({"kind": "clock", "expired": ["old-intent"]})
        self.anchor = {key: read_status(self.journal)[key] for key in ("records", "cursor", "hash")}

    def test_failure_requires_exit_identity_and_rejects_auth_failure(self) -> None:
        failure = recovery.collect_failure(self.output, self.handoff)
        assert failure is not None
        self.assertEqual(failure["anchor"], self.anchor)
        self.assertEqual(failure["target_identity"]["identity"], "target")
        status = self.schedule / "run" / "status.json"
        atomic_json(status, {"state": "failed", "error_type": "ApiError", "api_status": 403})
        self.assertIsNone(recovery.collect_failure(self.output, self.handoff))
        atomic_json(status, {"state": "failed", "error_type": "RuntimeError"})
        (self.schedule / "job" / "target.json").unlink()
        self.assertIsNone(recovery.collect_failure(self.output, self.handoff))

    def test_repair_keeps_original_header_and_requires_accepted_new_book(self) -> None:
        identity = {**self.identity, "source_sha256": {"stream.py": "new"}}
        with self.assertRaises(JournalError), AdaptiveJournal.open(
            self.journal, {**identity, "capital": 2000.0}, resume=True, repair_anchor=self.anchor
        ):
            self.fail("Changed capital was accepted")
        with AdaptiveJournal.open(
            self.journal, identity, resume=True, repair_anchor=self.anchor
        ) as journal:
            self.assertEqual([event["expired"] for event in journal.events()], [["old-intent"]])
            self.assertEqual(journal.recorded_identity, self.identity)
            journal.append({"kind": "recovery_gap"})
            journal.append({"kind": "book", "outputs": {"slot": {"accepted_book": False}}})
            with patch.object(adaptive_journal, "_entries") as scan:
                self.assertIsNone(read_resumed_input(self.journal, self.anchor))
                scan.assert_not_called()
            journal.append({"kind": "book", "outputs": {"slot": {"accepted_book": True}}})
        proof = read_resumed_input(self.journal, self.anchor)
        assert proof is not None
        self.assertEqual(proof["index"], 4)
        self.assertEqual(read_status(self.journal)["identity"], self.identity)
        with self.assertRaisesRegex(JournalError, "failure anchor"):
            read_resumed_input(self.journal, {**self.anchor, "hash": "invalid"})

    def test_reattaches_to_supervisor_and_approved_scheduler_without_relaunch(self) -> None:
        job = self.repair / "engineering-job"
        resumed = self.repair / "resumed-scheduler"
        supervisor = self.repair / "supervisor"

        def claim(directory: Path, argv: list[str], cwd: Path, **_: Any) -> None:
            directory.mkdir(parents=True)
            (directory / "start.claim").touch()

        def git(argv: list[str], cwd: Path) -> str:
            return recovery.REPAIR_REMOTE if argv[1] == "remote" else "merged"

        with (
            patch.object(recovery, "start_job", side_effect=claim) as start,
            patch.object(recovery, "command", side_effect=git),
            patch.object(recovery, "tree_digest", return_value="tested"),
        ):
            controller = SessionRecovery(self.output, Path("/fake/codex"))
            self.assertFalse(controller.poll(self.output, self.handoff))
            self.assertEqual(start.call_count, 1)
            atomic_json(job / "exit.json", {"returncode": 0})
            atomic_json(supervisor / "status.json", {
                "phase": "repair_ready", "runtime_checkout": str(supervisor / "merged-source"),
                "merged_sha": "merged", "merged_digest": "tested", "pr": "verified-pr",
            })
            self.assertFalse(controller.poll(self.output, self.handoff))
            self.assertEqual(start.call_count, 2)
            atomic_json(resumed / "exit.json", {"returncode": 0})
            grant = read_json(self.repair / "restart.json")
            atomic_json(self.repair / "resumed-input.json", {
                "repair": grant, "first_input": {"index": 2, "kind": "book"},
            })
            self.assertTrue(SessionRecovery(self.output, Path("/fake/codex")).poll(
                self.output, self.handoff
            ))
            self.assertEqual(start.call_count, 2)
            self.assertEqual(read_json(self.status), self.failure)

    def test_scheduler_passes_one_frozen_anchor_and_preserves_first_exit(self) -> None:
        archived = recovery.collect_failure(self.output, self.handoff)
        archive = self.repair / "failure.json"
        assert archived is not None
        atomic_json(archive, archived)
        repair = {
            "failure": str(archive), "source_commit": "merged",
            "source_digest": "tested", "pr": "approved",
        }

        def resumed(directory: Path, argv: list[str], cwd: Path, **_: Any) -> None:
            self.assertIn("--resume", argv)
            grant = read_json(Path(argv[argv.index("--repair-approval") + 1]))
            anchor = read_json(Path(grant["failure"]))["anchor"]
            self.assertEqual(anchor, self.anchor)
            self.assertEqual(Path(argv[argv.index("--journal") + 1]), self.journal)
            atomic_json(directory / "exit.json", self.exit)
            with AdaptiveJournal.open(
                self.journal, self.identity, resume=True, repair_anchor=anchor
            ) as journal:
                journal.append({"kind": "book", "outputs": {"slot": {"accepted_book": True}}})

        atomic_json(self.repair / "restart.json", repair)
        with (
            patch.object(scheduler, "command", return_value="merged"),
            patch.object(scheduler, "tree_digest", return_value="tested"),
            patch.object(scheduler, "start_job", side_effect=resumed) as launch,
            patch.object(scheduler.SlotControl, "open"),
        ):
            self.assertEqual(scheduler.run_schedule(self.schedule, self.spec, repair=repair), 1)
            self.assertEqual(scheduler.run_schedule(self.schedule, self.spec, repair=repair), 1)
        self.assertEqual(launch.call_count, 1)
        self.assertEqual(read_json(self.schedule / "job" / "exit.json"), self.exit)
        self.assertEqual(read_json(archive)["schedule"], self.failure)
        self.assertEqual(read_json(self.repair / "resumed-input.json")["first_input"]["index"], 2)


    def test_detached_repaired_worker_replays_and_appends_once(self) -> None:
        script = """
import argparse
import json
from pathlib import Path
from tools.adaptive_journal import AdaptiveJournal

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path)
parser.add_argument("--slot-control", type=Path)
parser.add_argument("--journal", type=Path)
parser.add_argument("--repair-approval", type=Path)
parser.add_argument("--resume", action="store_true")
args = parser.parse_args()
identity = json.loads(IDENTITY)
grant = json.loads(args.repair_approval.read_text())
anchor = json.loads(Path(grant["failure"]).read_text())["anchor"]
with AdaptiveJournal.open(args.journal, identity, resume=args.resume, repair_anchor=anchor) as db:
    assert [event["expired"] for event in db.events()] == [["old-intent"]]
    db.append({"kind": "book", "outputs": {"slot": {"accepted_book": True}}})
raise SystemExit(1)
""".replace("IDENTITY", repr(json.dumps(
            {**self.identity, "source_sha256": {"stream.py": "new"}}
        )))
        self.spec["argv"] = [sys.executable, "-c", script, "--output", str(self.schedule / "run")]
        atomic_json(self.status, self.failure)
        archived = recovery.collect_failure(self.output, self.handoff)
        archive = self.repair / "failure.json"
        assert archived is not None
        atomic_json(archive, archived)
        repair = {
            "failure": str(archive), "source_commit": "merged",
            "source_digest": "tested", "pr": "approved",
        }
        atomic_json(self.repair / "restart.json", repair)
        with (
            patch.object(scheduler, "command", return_value="merged"),
            patch.object(scheduler, "tree_digest", return_value="tested"),
            patch.object(scheduler.SlotControl, "open"),
        ):
            self.assertEqual(scheduler.run_schedule(self.schedule, self.spec, repair=repair), 1)
            self.assertEqual(scheduler.run_schedule(self.schedule, self.spec, repair=repair), 1)
        actual = read_json(self.schedule / "attempts/2/job/exit.json")
        self.assertEqual(actual["returncode"], 1)
        self.assertTrue((self.schedule / "attempts/2/job/target.json").is_file())
        self.assertEqual(read_status(self.journal)["records"], 2)
        self.assertEqual(read_json(self.repair / "resumed-input.json")["first_input"]["index"], 2)


    def test_resumed_failure_has_separate_handoff_and_blocks_further_launch(self) -> None:
        runner = Mock(source_commit="old-commit", source_digest="old-source")
        campaign = Campaign(self.output, Mock(), runner, Mock(), {})
        state = {"active": {"day": "test-day", "repair_resumed": True}}
        path = self.output / "US" / "state.json"
        original = self.output / "engineering_handoff.json"
        atomic_json(original, self.handoff)
        atomic_json(self.repair / "restart.json", {
            "source_commit": "merged", "source_digest": "tested",
        })
        campaign._block(path, state, self.output, "finite_schedule_failed")
        blocked = read_json(path)["blocked"]
        self.assertEqual(blocked["reason"], "resumed_finite_schedule_failed")
        self.assertEqual(blocked["source_commit"], "merged")
        self.assertEqual(read_json(original), self.handoff)
        self.assertEqual(read_json(
            self.output / "post-repair" / "engineering_handoff.json"
        ), blocked)

    def test_close_preserves_recovered_lineage_and_blocks_stale_capacity(self) -> None:
        runner = Mock(source_commit="old-commit", source_digest="old-source")
        review = Mock()
        review.poll.return_value = {"quality": "degraded"}
        admission = Mock(candidates={}, max_slots=1)
        campaign = Campaign(self.output, Mock(), runner, review, {"US": admission})
        state = {
            "history": [], "configs": {}, "seeds": {"slot": {"journal": str(self.journal)}},
            "active": {
                "day": "test-day", "repair_resumed": True, "intraday": {},
                "evidence": {"qualified": False, "next_slots": [], "partial_slots": [],
                             "metrics": {}},
            },
        }
        atomic_json(self.repair / "restart.json", {
            "source_commit": "merged", "source_digest": "tested",
        })
        path = self.output / "US" / "state.json"
        campaign._close_review(path, state, self.output, US, admission, datetime.now(UTC))
        saved = read_json(path)
        self.assertIsNone(saved["active"])
        self.assertEqual(saved["seeds"]["slot"]["journal"], str(self.journal))
        self.assertEqual(saved["blocked"]["reason"], "capacity_requires_repaired_source")
        self.assertEqual(saved["blocked"]["source_commit"], "merged")


class RepairPublicationTest(unittest.TestCase):
    def test_reconciles_ambiguous_merge_response_without_second_external_mutation(self) -> None:
        head, merge = "a" * 40, "b" * 40
        supervisor = Mock()
        supervisor.repo = Path("/fake/master")
        supervisor.worktree = Path("/fake/reviewed")
        supervisor.output = Path("/fake/repair")
        supervisor.requirements = {"live_integration": "same journal"}
        supervisor.verify_refactor_evidence.return_value = "tested"
        supervisor.repair_deadline.return_value = time.time() + 60
        state = supervisor.state = {
            "phase": "repair_merge", "base": "base", "branch": "codex/test",
            "publication_digest": "tested", "publish_sha": head,
            "pr": "https://github.com/nonpapfif/papertrade-lab/pull/123",
            "review": {
                "publish_ok": True, "decision": "complete", "satisfied": ["live_integration"],
            },
        }
        supervisor.transition.side_effect = lambda phase, **values: state.update(
            phase=phase, **values
        )
        remote = {"state": "OPEN", "baseRefName": "master", "headRefOid": head,
                  "headRefName": "codex/test", "isCrossRepository": False, "mergeCommit": None}
        local = "base"
        merges = 0

        def command(argv: list[str], cwd: Path) -> str:
            nonlocal merges, local
            match argv[:3]:
                case ["gh", "pr", "view"]:
                    return json.dumps(remote)
                case ["gh", "pr", "merge"]:
                    self.assertEqual(argv[-2:], ["--match-head-commit", head])
                    merges += 1
                    remote.update(state="MERGED", mergeCommit={"oid": merge})
                    raise subprocess.CalledProcessError(1, argv)
                case ["git", "merge", "--ff-only"]:
                    local = merge
                case ["git", "branch", "--show-current"]:
                    return "master"
                case ["git", "rev-parse", "origin/master"]:
                    return merge if remote["state"] == "MERGED" else "base"
                case ["git", "rev-parse", "HEAD"]:
                    if cwd == supervisor.worktree:
                        return head
                    return local
                case ["git", "rev-parse", _]:
                    return "same-tree"
            return ""

        with (
            patch.object(repair_publication, "command", side_effect=command),
            patch.object(repair_publication, "tree_digest", return_value="tested"),
        ):
            repair_publication.finish_publication(supervisor)
            self.assertEqual(state["phase"], "repair_merging")
            repair_publication.finish_publication(supervisor)
        self.assertEqual(merges, 1)
        self.assertEqual(state["phase"], "repair_ready")
        self.assertEqual(state["merged_sha"], merge)

    def test_supervisor_requires_offline_controller_static_checks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            supervisor = Supervisor(root, root / "out", Path("/fake/codex"), root, scope=REPAIR_SCOPE)
            supervisor.state["plan"] = {"kind": "offline", "tests": []}
            with (
                patch("tools.autotest_supervisor.changed_paths", return_value=[]),
                patch("tools.autotest_supervisor.tree_digest", return_value="tested"),
            ):
                supervisor.prepare_tests("")
            self.assertEqual(
                [job["argv"] for job in supervisor.state["test_jobs"][1:]],
                list(REPAIR_STATIC_CHECKS),
            )
