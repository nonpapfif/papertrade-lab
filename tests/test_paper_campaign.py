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
from tools.paper_campaign.evidence import aggregate, compact_rows, observed_coverage
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


def account_row(**fields: Any) -> dict[str, Any]:
    return {
        "equity": 1000,
        "initial_equity": 1000,
        "closed": 0,
        "open_positions": 0,
        "pending_intents": 0,
        **fields,
    }


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

    def test_held_quote_health_excludes_only_the_affected_pair(self) -> None:
        def session(equity: int, closed: int, eligible: bool) -> dict[str, Any]:
            cash = account_row(equity=equity, closed=closed)
            holding = account_row(
                equity=equity,
                closed=closed,
                open_positions=1,
                comparison_eligible=eligible,
            )
            return {
                "qualified": True,
                "metrics": {
                    "exposed": {"incumbent": cash, "candidate": holding},
                    "healthy": {"incumbent": cash, "candidate": cash},
                },
            }

        stale = session(2000, 4, False)
        recovery = session(1100, 5, True)
        later = session(1210, 7, True)
        first = aggregate([stale])
        self.assertEqual(first["exposed"]["qualified_sessions"], 0)
        self.assertEqual(first["exposed"]["qualified_closed_trades"], 0)
        self.assertEqual(first["exposed"]["qualified_candidate_return_pct_total"], 0)
        self.assertEqual(first["exposed"]["latest"]["candidate"]["equity"], 2000)
        self.assertTrue(first["exposed"]["has_exposure"])
        self.assertEqual(first["healthy"]["qualified_sessions"], 1)
        second = aggregate([stale, recovery])
        self.assertTrue(second["exposed"]["comparison_eligible"])
        self.assertFalse(second["exposed"]["return_baseline_eligible"])
        self.assertEqual(second["exposed"]["qualified_sessions"], 0)
        self.assertEqual(second["exposed"]["qualified_closed_trades"], 0)
        stats = aggregate([stale, recovery, later])
        self.assertEqual(stats["exposed"]["qualified_sessions"], 1)
        self.assertEqual(stats["exposed"]["qualified_closed_trades"], 2)
        self.assertAlmostEqual(stats["exposed"]["qualified_candidate_return_pct_total"], 10)
        self.assertEqual(stats["healthy"]["qualified_sessions"], 3)

    def test_missing_held_quote_health_fails_closed(self) -> None:
        status = {
            "shared_slots": [
                {
                    "slot_id": "slot",
                    "comparison": {
                        "accounts": {
                            "incumbent": {"paper": {"account": account_row()}},
                            "candidate": {"paper": {"account": account_row(open_positions=1)}},
                        }
                    },
                }
            ]
        }
        accounts = compact_rows(status, "USD")["slot"]
        self.assertTrue(accounts["incumbent"]["comparison_eligible"])
        self.assertFalse(accounts["candidate"]["comparison_eligible"])
        self.assertEqual(
            accounts["candidate"]["comparison_exclusions"][0]["reason"], "missing_quote_health"
        )

    def test_live_quote_health_must_cover_the_decision_time(self) -> None:
        earlier = (START - timedelta(seconds=10)).isoformat()
        later = (START + timedelta(seconds=10)).isoformat()
        now = START.isoformat()
        for cut, expiry, reason in (
            (None, later, "missing_quote_health"),
            (later, later, "future_quote_health"),
            (earlier, None, "missing_quote_health"),
            (earlier, earlier, "stale_quote_health"),
            (earlier, now, None),
            (earlier, later, None),
        ):
            with self.subTest(cut=cut, expiry=expiry):
                account = account_row(
                    open_positions=1,
                    comparison_eligible=True,
                    quote_health_as_of=cut,
                    comparison_valid_until=expiry,
                )
                status = {
                    "shared_slots": [
                        {
                            "slot_id": "slot",
                            "comparison": {
                                "accounts": {
                                    "candidate": {
                                        "paper": {
                                            "account": account,
                                        }
                                    }
                                }
                            },
                        }
                    ]
                }
                live = compact_rows(status, "USD", as_of=START)["slot"]["candidate"]
                self.assertEqual(live["comparison_eligible"], reason is None)
                self.assertEqual(live["equity"], 1000)
                self.assertEqual(
                    [item["reason"] for item in live["comparison_exclusions"]],
                    [] if reason is None else [reason],
                )
                close = compact_rows(status, "USD")["slot"]["candidate"]
                self.assertTrue(close["comparison_eligible"])

    def test_selection_rejects_an_excluded_new_slot_and_protects_its_carryover(self) -> None:
        admitted = admission(3)
        baseline, stale, healthy = admitted.initial()
        tested = {"qualified_sessions": 3, "qualified_closed_trades": 3, "has_exposure": False}
        statistics = dict.fromkeys(admitted.candidates, tested)
        statistics[stale] = {**tested, "comparison_eligible": False, "has_exposure": True}
        with self.assertRaisesRegex(ValueError, "invalid"):
            next_plan(
                {"next_slots": [baseline, stale]}, [baseline, healthy], admitted, statistics, set()
            )
        with self.assertRaisesRegex(ValueError, "protected"):
            next_plan(
                {"next_slots": [baseline, healthy]}, admitted.initial(), admitted, statistics, set()
            )
        self.assertEqual(
            next_plan(
                {"next_slots": [baseline, stale]}, admitted.initial(), admitted, statistics, set()
            ),
            [baseline, stale],
        )

    def test_close_review_uses_healthy_pairs_and_carries_stale_exposure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner, review = Mock(), Mock()
            admitted = admission(3)
            baseline, stale, removable = admitted.initial()
            campaign = Campaign(root, Mock(), runner, review, {"US": admitted})

            def metrics(closed: int, eligible: bool) -> dict[str, Any]:
                flat = account_row(closed=closed)
                held = account_row(
                    equity=1900, closed=closed, open_positions=1, comparison_eligible=eligible
                )
                return {
                    baseline: {"incumbent": flat, "candidate": flat},
                    stale: {"incumbent": flat, "candidate": held},
                    removable: {"incumbent": flat, "candidate": flat},
                }

            seeds = {stale: {"journal": "stale-sealed.sqlite3", "slot_id": stale}}
            state: dict[str, Any] = {
                "history": [
                    {"qualified": True, "metrics": metrics(closed, True)} for closed in (1, 2)
                ],
                "active": {
                    "day": DAY,
                    "evidence": {
                        "qualified": True,
                        "metrics": metrics(3, False),
                        "next_slots": admitted.initial(),
                        "partial_slots": [],
                    },
                    "intraday": {},
                },
                "seeds": seeds,
                "configs": {},
                "retired_slots": sorted(admitted.candidates.keys() - set(admitted.initial())),
            }
            review.poll.return_value = {
                "quality": "sufficient",
                "next_slots": [baseline, stale],
                "branch_source": "",
                "branch_target": "",
                "branch_account": "",
                "park_slot": "",
            }
            campaign._close_review(root / "state.json", state, root, US, admitted, START)
            evidence = review.poll.call_args.args[1]
            self.assertNotIn(stale, evidence["eligible"])
            self.assertEqual(evidence["required_carryover"], [stale])
            self.assertNotIn("equity", evidence["metrics"][stale]["candidate"])
            self.assertNotIn("equity", evidence["history"][stale]["latest"]["incumbent"])
            self.assertEqual(evidence["metrics"][removable]["candidate"]["equity"], 1000)
            self.assertEqual(state["history"][-1]["metrics"][stale]["candidate"]["equity"], 1900)
            self.assertEqual(state["history"][-1]["retired_slots"], [removable])
            self.assertEqual(state["approved_slots"], [baseline, stale])
            self.assertEqual(state["seeds"], seeds)

    def test_intraday_source_must_be_eligible_at_review_and_dispatch(self) -> None:
        admitted = admission()
        source = admitted.initial()[0]
        target = f"fork-{admitted.initial()[1]}"
        for frozen_eligible, current_eligible, delay in (
            (False, True, 0),
            (True, False, 0),
            (True, True, 0),
            (True, True, 2),
        ):
            with (
                self.subTest(frozen=frozen_eligible, current=current_eligible, delay=delay),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                runner, review = Mock(), Mock()
                campaign = Campaign(root, Mock(), runner, review, {"US": admitted})
                decision = {
                    "quality": "sufficient",
                    "next_slots": [source],
                    "branch_source": source,
                    "branch_account": "candidate",
                    "branch_target": target,
                    "park_slot": "",
                }
                account = account_row(
                    open_positions=1,
                    comparison_eligible=frozen_eligible,
                    quote_health_as_of=START.isoformat(),
                    comparison_valid_until=(START + timedelta(seconds=1)).isoformat(),
                )
                details: dict[str, Any] = {
                    "configuration": BASELINE,
                    "paper": {"account": account, "outcomes": [{"status": "filled"}]},
                }
                runner.live.return_value = {
                    "slots": [{"slot_id": source, "state": "active"}],
                    "commands": [],
                    "summary": {
                        "committed_sequence": 2,
                        "shared_slots": [
                            {
                                "slot_id": source,
                                "state": "active",
                                "comparison": {
                                    "accounts": {
                                        "incumbent": {
                                            **details,
                                            "paper": {
                                                **details["paper"],
                                                "account": account_row(
                                                    quote_health_as_of=START.isoformat()
                                                ),
                                            },
                                        },
                                        "candidate": details,
                                    }
                                },
                            }
                        ],
                    },
                }
                state: dict[str, Any] = {
                    "active": {
                        "day": DAY,
                        "end": (START + timedelta(hours=1)).isoformat(),
                        "intraday": {"pending": None, "decisions": [], "last_fills": {}},
                    },
                    "history": [],
                    "seeds": {},
                }
                review.poll.side_effect = [None, decision]
                campaign._intraday(root / "state.json", state, root, US, admitted, START)
                pending = state["active"]["intraday"]["pending"]
                self.assertEqual(
                    "candidate" in pending["eligible_sources"][source], frozen_eligible
                )
                self.assertEqual(
                    "equity" in pending["metrics"][source]["candidate"], frozen_eligible
                )
                account["comparison_eligible"] = current_eligible
                campaign._intraday(
                    root / "state.json", state, root, US, admitted, START + timedelta(seconds=delay)
                )
                self.assertEqual(
                    runner.branch.call_count,
                    int(frozen_eligible and current_eligible and not delay),
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
