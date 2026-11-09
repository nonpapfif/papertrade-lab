from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from papertrade.api import ApiError
from tools.adaptive_journal import AdaptiveJournal
from tools.autotest_supervisor import read_json
from tools.paper_campaign.calendar import MarketCalendar, Window
from tools.paper_campaign.controller import Campaign
from tools.paper_campaign.evidence import observed_coverage
from tools.paper_campaign.policy import BASELINE, Admission, fingerprint, next_plan
from tools.paper_campaign.review import AstraReview
from tools.paper_capacity import strategies
from tools.paper_market import US

START = datetime(2026, 11, 9, 14, 30, tzinfo=UTC)
DAY = START.date().isoformat()


def sample(count: int) -> dict:
    return {
        "slots": count,
        "inputs_aligned": True,
        "subscriptions_covered": True,
        "max_backlog_seconds": 0.05,
        "processing_seconds": 1,
        "observed_seconds": 60,
    }


def admission(count: int = 2) -> Admission:
    return Admission.load(
        {
            "market": "US",
            "source_digest": "approved",
            "qualification": "recorded_feed_capacity",
            "max_slots": count,
            "candidates": strategies(),
            "latency_budget_seconds": 1,
            "measurements": [sample(i) for i in range(1, count + 1)],
        },
        "US",
        "approved",
    )


class CampaignTests(unittest.TestCase):
    def test_measured_admission_and_plan_protection(self) -> None:
        admitted = admission()
        active = admitted.initial()
        self.assertEqual(len(active), 2)
        self.assertEqual(admitted.candidates[active[0]], BASELINE)
        replacement = next(iter(admitted.candidates.keys() - set(active)))
        selection = {"next_slots": [active[0], replacement]}
        with self.assertRaisesRegex(ValueError, "protected"):
            next_plan(selection, active, admitted, {}, set())
        qualified = {
            active[1]: {
                "qualified_sessions": 3,
                "qualified_closed_trades": 3,
                "has_exposure": False,
            },
        }
        self.assertEqual(
            next_plan(selection, active, admitted, qualified, set()), selection["next_slots"]
        )

    def test_provider_failure_is_backed_off_and_wrong_market_data_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = Mock()
            client.calendar.side_effect = [
                ApiError("temporary", status=429, retry_at=START.timestamp() + 15),
                {"today": {"date": DAY, "preMarket": {}}},
            ]
            calendar = MarketCalendar(client, Path(directory))
            self.assertIsNone(calendar.window(US, START))
            self.assertIsNone(calendar.window(US, START + timedelta(seconds=10)))
            self.assertEqual(client.calendar.call_count, 1)
            self.assertIsNone(calendar.window(US, START + timedelta(seconds=16)))
            self.assertEqual(read_json(Path(directory) / "US" / f"{DAY}.json")["state"], "retry")

    def test_coverage_includes_all_books_and_penalizes_silence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            with AdaptiveJournal.open(path, {"test": True}, resume=False) as journal:
                for elapsed in (0, 20, 180, 360):
                    at = (START + timedelta(seconds=elapsed)).isoformat()
                    journal.append(
                        {
                            "kind": "book",
                            "received": at,
                            "outputs": {"test": {"accepted_book": True}},
                        }
                    )
                chain = journal.evidence()["chain_sha256"]
                journal.complete()
            coverage = observed_coverage(path, chain, START.timestamp(), START.timestamp() + 420)
            self.assertEqual(coverage["accepted_books"], 4)
            self.assertEqual(coverage["maximum_gap_seconds"], 180)
            self.assertAlmostEqual(coverage["fraction"], 200 / 420)

    def test_invalid_review_does_not_repeat_job_or_abort(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = {"kind": "close", "sequence": 42}
            job = root / fingerprint(evidence)
            (job / "job").mkdir(parents=True)
            (job / "job" / "start.claim").touch()
            (job / "response.json").write_text("{")
            with (
                patch("tools.paper_campaign.review.poll_job", return_value={"returncode": 0}),
                patch("tools.paper_campaign.review.start_job") as start,
            ):
                result = AstraReview().poll(root, evidence)
            assert result is not None
            self.assertEqual(result["reason"], "review_invalid")
            start.assert_not_called()

    def test_earliest_admission_and_same_window_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calendar, runner, review = Mock(), Mock(), Mock()
            runner.source_commit, runner.source_digest = "commit", "digest"
            runner.poll.return_value = None
            runner.live.return_value = None
            calendar.window.return_value = Window(DAY, START, START + timedelta(hours=6))
            campaign = Campaign(root, calendar, runner, review, {"US": admission()}, {"US": START})
            campaign.tick(US, START - timedelta(seconds=1))
            calendar.window.assert_not_called()
            campaign.tick(US, START)
            campaign.tick(US, START + timedelta(seconds=1))
            calendar.window.assert_called_once()
            first, second = runner.poll.call_args_list
            self.assertEqual(first, second)
            self.assertEqual(first.args[1]["capital"], 1000)
            self.assertEqual(first.args[1]["seeds"], {})
            self.assertEqual(len(first.args[1]["slots"]), 2)
            review.poll.assert_not_called()

    def test_next_session_reuses_sealed_slot_even_if_review_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calendar, runner, review = Mock(), Mock(), Mock()
            runner.source_commit, runner.source_digest = "commit", "digest"
            runner.live.return_value = None
            review.poll.return_value = {"quality": "degraded", "reason": "review_unavailable"}
            capacity = admission(1)
            slot = capacity.initial()[0]
            end = START + timedelta(seconds=120)
            calendar.window.return_value = Window(DAY, START, end)
            journal_path = root / "journal.sqlite3"
            with AdaptiveJournal.open(journal_path, {"test": True}, resume=False) as journal:
                for elapsed in (0, 30, 60, 90):
                    at = (START + timedelta(seconds=elapsed)).isoformat()
                    journal.append(
                        {
                            "kind": "book",
                            "received": at,
                            "outputs": {"test": {"accepted_book": True}},
                        }
                    )
                chain = journal.evidence()["chain_sha256"]
                journal.complete()
            metrics = {
                "configuration": BASELINE,
                "paper": {
                    "account": {
                        "equity": 975.0,
                        "initial_equity": 1000.0,
                        "return_pct": -2.5,
                        "open_positions": 0,
                        "entries": 2,
                        "closed": 2,
                    },
                    "pending_intents": 0,
                },
            }
            runner.poll.side_effect = [
                None,
                {
                    "state": "complete",
                    "journal": str(journal_path),
                    "commands": [{"id": f"add-{slot}", "disposition": "applied"}],
                    "summary": {
                        "real_orders": False,
                        "ai_calls": 0,
                        "market": "US",
                        "journal": {"chain_sha256": chain},
                        "shared_slots": [
                            {
                                "slot_id": slot,
                                "state": "active",
                                "comparison": {
                                    "accounts": {"incumbent": metrics, "candidate": metrics}
                                },
                            }
                        ],
                        "checks": dict.fromkeys(
                            (
                                "stream_observed",
                                "features_observed",
                                "queue_p95_under_one_second",
                                "protected_subscription_retained",
                                "cash_not_overreserved",
                                "matched_inputs",
                            ),
                            True,
                        ),
                    },
                },
                None,
            ]
            campaign = Campaign(root, calendar, runner, review, {"US": capacity})
            campaign.tick(US, START)
            campaign.tick(US, end)
            state = read_json(root / "US" / "state.json")
            record = state["history"][0]
            self.assertTrue(record["qualified"])
            self.assertEqual(record["metrics"][slot]["candidate"]["equity"], 975.0)
            self.assertEqual(record["review"]["quality"], "degraded")
            tomorrow = START + timedelta(days=1)
            calendar.window.return_value = Window(
                tomorrow.date().isoformat(), tomorrow, tomorrow + timedelta(hours=6)
            )
            campaign.tick(US, tomorrow)
            plan = runner.poll.call_args.args[1]
            self.assertEqual(plan["seeds"][slot]["chain_sha256"], chain)
            self.assertEqual(plan["seeds"][slot]["journal"], str(journal_path))
            review.poll.assert_called_once()

    def test_retirement_can_leave_only_the_baseline_after_exploration(self) -> None:
        admitted = admission()
        active = admitted.initial()
        baseline, loser = active
        choose_baseline = {"next_slots": [baseline]}
        with self.assertRaisesRegex(ValueError, "insufficiently tested"):
            next_plan(choose_baseline, active, admitted, {}, set())

        tested = {"qualified_sessions": 3, "qualified_closed_trades": 3, "has_exposure": False}
        with self.assertRaisesRegex(ValueError, "newcomer"):
            next_plan(choose_baseline, active, admitted, {loser: tested}, set())

        qualified = dict.fromkeys(admitted.candidates, tested)
        self.assertEqual(next_plan(choose_baseline, active, admitted, qualified, set()), [baseline])
        with self.assertRaisesRegex(ValueError, "invalid"):
            next_plan({"next_slots": active}, [baseline], admitted, qualified, set(), {loser})
