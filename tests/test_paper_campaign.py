from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from papertrade.api import ApiError
from tools.adaptive_journal import AdaptiveJournal
from tools.autotest_jobs import atomic_json
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
        "accounts": 2 * count,
        "events": 60,
        "inputs_aligned": True,
        "subscriptions_covered": True,
        "max_backlog_seconds": 0.05,
        "processing_seconds": 1,
        "observed_seconds": 60,
        "p99_event_seconds": 0.02,
        "peak_rss_bytes": 1024,
        "accepted_books_per_account": 60,
        "valid_features_per_slot": 1,
    }


def capacity_report(count: int = 2) -> dict:
    return {
        "market": "US",
        "source_digest": "approved",
        "input_digest": "a" * 64,
        "machine": "offline-test-machine",
        "real_orders": False,
        "qualification": "recorded_feed_capacity",
        "max_slots": count,
        "candidates": strategies(),
        "latency_budget_seconds": 1,
        "measurements": [sample(i) for i in range(1, count + 1)],
    }


def admission(count: int = 2) -> Admission:
    return Admission.load(capacity_report(count), "US", "approved")


class CampaignTests(unittest.TestCase):
    def test_admission_requires_complete_provenance_and_paper_boundary(self) -> None:
        report = capacity_report()
        for field in report:
            missing = {key: value for key, value in report.items() if key != field}
            with self.subTest(missing=field), self.assertRaises(ValueError):
                Admission.load(missing, "US", "approved")
        invalid: Any
        for field, invalid_values in (
            ("input_digest", (None, True, 1, [], "", "a" * 63, "z" * 64, " " * 64)),
            ("machine", (None, True, 1, [], "", "  ")),
            ("source_digest", (None, "", "another-source")),
            ("real_orders", (None, 0, "false", True)),
            ("market", (None, "KR")),
            ("qualification", (None, "live")),
        ):
            for invalid in invalid_values:
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ValueError):
                    Admission.load({**report, field: invalid}, "US", "approved")
        with self.assertRaises(ValueError):
            Admission.load(report, "US", "changed-source")
        with self.assertRaises(ValueError):
            Admission.load({**report, "source_digest": ""}, "US", "")

    def test_admission_rejects_corrupt_measurements_and_extrapolated_capacity(self) -> None:
        report = capacity_report()
        for field in sample(1):
            missing = {key: value for key, value in sample(1).items() if key != field}
            with self.subTest(missing=field), self.assertRaises(ValueError):
                Admission.load({**report, "measurements": [missing, sample(2)]}, "US", "approved")
        for field in (
            "max_backlog_seconds",
            "processing_seconds",
            "observed_seconds",
            "p99_event_seconds",
            "accepted_books_per_account",
            "valid_features_per_slot",
        ):
            for invalid in (None, True, "1", -1, float("nan"), float("inf")):
                rows = [{**sample(1), field: invalid}, sample(2)]
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ValueError):
                    Admission.load({**report, "measurements": rows}, "US", "approved")
        for invalid in (None, True, "1", -1, float("nan"), float("inf"), 10**400):
            with self.subTest(budget=invalid), self.assertRaises(ValueError):
                Admission.load({**report, "latency_budget_seconds": invalid}, "US", "approved")
        for defect in (
            {"max_slots": 3},
            {"measurements": [sample(2)]},
            {"measurements": [sample(1), sample(1)]},
            {"measurements": [sample(1), {**sample(2), "inputs_aligned": False}]},
            {"max_slots": 1, "measurements": [sample(1), {"slots": 2}]},
        ):
            with self.subTest(defect=defect), self.assertRaises(ValueError):
                Admission.load({**report, **defect}, "US", "approved")

    def test_admission_uses_only_measured_successful_prefix(self) -> None:
        report = capacity_report()
        report["measurements"].extend(
            [
                {**sample(3), "max_backlog_seconds": 2},
                sample(4),
            ]
        )
        first = Admission.load(report, "US", "approved")
        second = Admission.load(report, "US", "approved")
        self.assertEqual(first, second)
        self.assertEqual(first.max_slots, 2)
        self.assertEqual(first.initial(), list(first.candidates)[:2])
        self.assertEqual(first.evidence_digest, fingerprint(report))

    def test_admission_rejects_changed_measured_candidate_order(self) -> None:
        report = capacity_report()
        for candidates in (list(reversed(strategies())), strategies()[1:]):
            with self.subTest(candidates=candidates), self.assertRaises(ValueError):
                Admission.load({**report, "candidates": candidates}, "US", "approved")

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

    def test_approved_close_is_recovered_once_and_carries_selected_accounts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calendar, runner = Mock(), Mock()
            runner.source_commit, runner.source_digest = "commit", "digest"
            runner.live.return_value = None
            runner.poll.return_value = None
            capacity = admission(3)
            baseline, loser, survivor = capacity.initial()
            selected = [survivor, baseline]
            calendar.window.return_value = Window(DAY, START, START + timedelta(hours=6))
            campaign = Campaign(root, calendar, runner, AstraReview(), {"US": capacity})
            campaign.tick(US, START)
            state_path = root / "US" / "state.json"
            state = read_json(state_path)
            journal_path = root / "sealed.sqlite3"
            with AdaptiveJournal.open(journal_path, {"test": True}, resume=False) as journal:
                chain = journal.evidence()["chain_sha256"]
                journal.complete()

            configs = {item["slot_id"]: item["spec"] for item in state["active"]["slots"]}
            seeds = {
                slot: {"journal": str(journal_path), "slot_id": slot, "chain_sha256": chain}
                for slot in configs
            }

            def metrics(closed: int) -> dict[str, Any]:
                return {
                    slot: {
                        name: {
                            "initial_equity": 1000,
                            "equity": 1000 - 10 * closed if slot == loser else 1000,
                            "closed": closed,
                            "open_positions": 0,
                            "pending_intents": 0,
                        }
                        for name in ("incumbent", "candidate")
                    }
                    for slot in configs
                }

            # Reconcile from the durable boundary after the worker's terminal evidence was saved.
            state["active"]["evidence"] = {
                "day": DAY,
                "kind": "close",
                "qualified": True,
                "journal_chain": chain,
                "metrics": metrics(3),
                "next_slots": capacity.initial(),
                "partial_slots": [],
            }
            state.update(
                configs=configs,
                seeds=seeds,
                retired_slots=sorted(capacity.candidates.keys() - configs.keys()),
                history=[
                    {"day": "2026-11-05", "qualified": True, "metrics": metrics(1)},
                    {"day": "2026-11-06", "qualified": True, "metrics": metrics(2)},
                ],
            )
            atomic_json(state_path, state)

            def complete_review(job: Path, *_args: Any) -> None:
                job.mkdir(parents=True)
                (job / "start.claim").touch()
                atomic_json(
                    job.parent / "response.json",
                    {
                        "quality": "sufficient",
                        "reason": "Three qualified sessions and three closed trades support retirement.",
                        "next_slots": selected,
                        "branch_source": "",
                        "branch_account": "",
                        "branch_target": "",
                        "park_slot": "",
                    },
                )

            with (
                patch(
                    "tools.paper_campaign.review.start_job", side_effect=complete_review
                ) as start,
                patch(
                    "tools.paper_campaign.review.poll_job", return_value={"returncode": 0}
                ) as poll,
            ):
                end = START + timedelta(hours=6)
                campaign.tick(US, end)
                with (
                    patch("tools.paper_campaign.controller.atomic_json", side_effect=OSError),
                    self.assertRaises(OSError),
                ):
                    campaign.tick(US, end)
                self.assertEqual(read_json(state_path)["approved_slots"], capacity.initial())

                recovered = Campaign(root, calendar, runner, AstraReview(), {"US": capacity})
                recovered.tick(US, end)
                persisted = read_json(state_path)
                self.assertIsNone(persisted["active"])
                self.assertEqual(persisted["approved_slots"], selected)
                self.assertEqual(persisted["history"][-1]["retired_slots"], [loser])
                self.assertEqual(persisted["history"][-1]["review"]["model"], "gpt-6-astra")
                self.assertEqual(persisted["history"][-1]["review"]["quality"], "sufficient")
                self.assertEqual(
                    persisted["history"][-1]["metrics"][loser]["candidate"]["equity"], 970
                )

                tomorrow = START + timedelta(days=1)
                calendar.window.return_value = Window(
                    tomorrow.date().isoformat(), tomorrow, tomorrow + timedelta(hours=6)
                )
                Campaign(root, calendar, runner, AstraReview(), {"US": capacity}).tick(US, tomorrow)
                next_plan_record = runner.poll.call_args.args[1]
                self.assertEqual(
                    [(item["slot_id"], item["spec"]) for item in next_plan_record["slots"]],
                    [(slot, configs[slot]) for slot in selected],
                )
                self.assertEqual(
                    next_plan_record["seeds"], {slot: seeds[slot] for slot in selected}
                )
                self.assertEqual(read_json(state_path)["seeds"][loser], seeds[loser])
                start.assert_called_once()
                self.assertEqual(poll.call_count, 2)
