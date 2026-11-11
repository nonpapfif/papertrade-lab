"""Campaign failures and publication are durable without triggering a retry or merge."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from tests.test_paper_campaign import START, account_row, admission
from tools.autotest_supervisor import Supervisor, read_json
from tools.paper_campaign.calendar import Window
from tools.paper_campaign.controller import Campaign
from tools.paper_campaign.evidence import observed_coverage
from tools.paper_campaign.review import AstraReview
from tools.paper_market import US
from tools.runtime_audit import RuntimeAudit


class CampaignAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.path = self.root / "audit.sqlite3"
        self.audit = RuntimeAudit(self.path, "run-a")

    def incidents(self) -> list[tuple[str, str, int]]:
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(
                "SELECT component, code, occurrences FROM runtime_incidents ORDER BY id"
            ).fetchall()

    def test_failed_session_handoff_is_reused_and_readable_by_supervisor(self) -> None:
        admitted = admission(1)
        end = START + timedelta(hours=1)
        calendar = Mock()
        calendar.window.return_value = Window(START.date().isoformat(), START, end)
        runner = Mock(source_commit="commit", source_digest="source")
        runner.poll.return_value = {"state": "failed", "reason": "worker_exit"}
        review = Mock()
        campaign = Campaign(self.root, calendar, runner, review, {"US": admitted}, audit=self.audit)
        campaign.tick(US, START)
        session = self.root / "US" / START.date().isoformat()
        path = session / "engineering_handoff.json"
        task = read_json(path)
        self.assertEqual(task["reason"], "worker_exit")
        campaign.tick(US, START + timedelta(seconds=5))
        runner.poll.assert_called_once()
        review.poll.assert_not_called()
        self.assertEqual(read_json(path), task)
        self.assertIn(("engineering_handoff", "campaign_blocked", 1), self.incidents())
        supervisor = Supervisor(
            self.root, self.root / "supervisor", Path("/fixture/codex"), session,
            scope="concurrent-refactor",
        )
        with patch("tools.autotest_supervisor.tree_digest", return_value="source"):
            self.assertEqual(supervisor.evidence()["engineering_handoff"], task)

    def test_full_capacity_still_reviews_fresh_fills_without_dispatch(self) -> None:
        admitted = admission(1)
        slot = admitted.initial()[0]
        runner = Mock()
        account = {
            "paper": {
                "account": account_row(quote_health_as_of=START.isoformat()),
                "outcomes": [{"status": "filled"}],
                "pending_intents": 0,
            },
        }
        runner.live.return_value = {
            "summary": {
                "committed_sequence": 1,
                "shared_slots": [{
                    "slot_id": slot,
                    "state": "active",
                    "comparison": {"accounts": {
                        name: {**account, "configuration": admitted.candidates[slot]}
                        for name in ("incumbent", "candidate")
                    }},
                }],
            },
            "slots": [{"slot_id": slot, "state": "active"}],
            "commands": [],
        }
        review = Mock()
        review.poll.return_value = {"quality": "sufficient", "next_slots": [slot]}
        campaign = Campaign(self.root, Mock(), runner, review, {"US": admitted}, audit=self.audit)
        state = {
            "active": {
                "day": START.date().isoformat(),
                "end": (START + timedelta(hours=1)).isoformat(),
                "intraday": {"pending": None, "last_fills": {}, "decisions": []},
            },
            "history": [],
            "seeds": {},
        }
        for _ in range(2):
            campaign._intraday(self.root / "state.json", state, self.root, US, admitted, START)
        review.poll.assert_called_once()
        self.assertEqual(review.poll.call_args.args[1]["available_lanes"], 0)
        runner.branch.assert_not_called()

    def test_failed_review_launch_is_deduplicated_and_audited(self) -> None:
        review = AstraReview(self.audit)
        packet = {"market": "US", "sequence": 2}
        with patch("tools.paper_campaign.review.start_job", side_effect=OSError) as start:
            first = review.poll(self.root, packet)
            self.assertEqual(review.poll(self.root, packet), first)
        start.assert_called_once()
        self.assertIn(("strategic_review", "review_launch_unavailable", 1), self.incidents())

    def test_evidence_failure_keeps_original_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive interval"):
            observed_coverage(self.root / "missing", "chain", 100, 100, audit=self.audit)
        self.assertIn(("evidence", "coverage_failed", 1), self.incidents())

    def test_pr_reconciliation_stops_at_open_reviewed_head(self) -> None:
        supervisor = Supervisor(
            self.root, self.root / "supervisor", Path("/fixture/codex"), self.root
        )
        supervisor.transition("pr_wait", pr="fixture-pr", publish_sha="sha", history=[{}])
        with patch(
            "tools.autotest_supervisor.command", return_value='{"headRefOid":"sha","state":"OPEN"}'
        ) as command:
            supervisor.step()
            supervisor.step()
        command.assert_called_once_with(
            ["gh", "pr", "view", "--repo", "nonpapfif/papertrade-lab", "fixture-pr",
             "--json", "headRefOid,state"],
            self.root.resolve(),
        )
        self.assertEqual(supervisor.state["phase"], "pr_ready")
