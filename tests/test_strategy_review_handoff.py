import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from papertrade.jsonio import atomic_json, read_json
from tools.adaptive_journal import AdaptiveJournal
from tools.source_identity import file_digest
from tools.strategy_review_handoff import load_next_plan, write_handoff

DAY = datetime.now(UTC).date().isoformat()
NEXT_DAY = (datetime.now(UTC).date() + timedelta(days=2)).isoformat()


class StrategyReviewHandoffTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.prior = Path(temp.name) / "run"
        self.output = Path(temp.name) / "handoff"

    def make_plan(self, market: str, strategies: dict[str, Any], capital: int) -> datetime:
        atomic_json(
            self.prior / "plan.json",
            {
                "day": DAY,
                "market": market,
                "strategies": strategies,
                "capital": capital,
                "evidence": "/old/archived-loss.json",
                "evidence_sha256": "a" * 64,
            },
        )
        row: dict[str, Any]
        if market == "US":
            row = {
                "regularMarket": {
                    "startTime": f"{NEXT_DAY}T09:30:00-05:00",
                    "endTime": f"{NEXT_DAY}T16:00:00-05:00",
                },
            }
            start = row["regularMarket"]["startTime"]
        else:
            session = {
                "startTime": f"{NEXT_DAY}T09:00:00+09:00",
                "singlePriceAuctionStartTime": f"{NEXT_DAY}T15:20:00+09:00",
                "endTime": f"{NEXT_DAY}T15:30:00+09:00",
            }
            row = {"integrated": {"regularMarket": session}}
            start = session["startTime"]
        atomic_json(
            self.prior / "calendar.json",
            {
                "today": {"date": DAY},
                "nextBusinessDay": {"date": NEXT_DAY, **row},
            },
        )
        return datetime.fromisoformat(start)

    def close(self, market: str, result: Path, action: str) -> None:
        atomic_json(
            self.prior / "decision.json",
            {
                "day": DAY,
                "market": market,
                "plan_sha256": file_digest(self.prior / "plan.json"),
                "runner_exit": 0,
                "decision": action,
                "result_path": f"/old/host/{result.name}",
                "result_sha256": file_digest(result),
            },
        )

    def make_us(self, *, refusal: bool = True, profitable: bool = True) -> datetime:
        incumbent, candidate = "source_min_reward_risk_1", "source_min_reward_risk_2"
        start = self.make_plan(
            "US",
            {
                "incumbent": {"min_reward_risk": 1.0},
                "candidate": {"min_reward_risk": 2.0},
            },
            1000,
        )
        session = self.prior / "session"
        session.mkdir()
        journal_path = session / "journal.sqlite3"
        sizing = {"entry": 10, "entry_plan": 10, "stop": 8, "target": 13.5, "reward_risk": 1.75}
        with AdaptiveJournal.open(
            journal_path,
            {
                "day": DAY,
                "min_reward_risk_by_profile": {incumbent: 1.0, candidate: 2.0},
            },
            resume=False,
        ) as journal:
            journal.append(
                {
                    "outputs": {
                        "accounts": {
                            incumbent: [
                                {
                                    "kind": "entry_confirmation",
                                    "status": "filled",
                                    "symbol": "ABC",
                                    "quantity": 1,
                                    "price": 10,
                                    "sizing": sizing,
                                }
                            ],
                            candidate: [
                                {
                                    "kind": "entry_confirmation",
                                    "status": "cancelled",
                                    "symbol": "ABC",
                                    "sizing": {
                                        **sizing,
                                        "refusal": (
                                            "reward:risk 1.75 is under your minimum 2, "
                                            "so trader-risk would block it."
                                            if refusal
                                            else "insufficient cash"
                                        ),
                                    },
                                }
                            ],
                        }
                    }
                }
            )
            realized = "2" if profitable else "-2"
            journal.append(
                {
                    "outputs": {
                        "accounts": {
                            incumbent: [
                                {
                                    "kind": "exit_confirmation",
                                    "status": "filled",
                                    "symbol": "ABC",
                                    "quantity": 1,
                                    "remaining": 0,
                                    "reason": "plan-target" if profitable else "plan-stop",
                                    "realized_usd": realized,
                                }
                            ]
                        }
                    }
                }
            )
            # An open candidate position can be marked below the incumbent without an exit.
            journal.append(
                {
                    "outputs": {
                        "accounts": {
                            candidate: [
                                {
                                    "kind": "entry_confirmation",
                                    "status": "filled",
                                    "symbol": "DEF",
                                    "quantity": 1,
                                    "price": 10,
                                    "sizing": sizing,
                                }
                            ]
                        }
                    }
                }
            )
            evidence = journal.evidence()
            journal.complete()
        report = session / "comparison.json"
        atomic_json(
            report,
            {
                **evidence,
                "state": "closed",
                "comparison_quality": "observed",
                "accounts": {
                    incumbent: {
                        "seed_usd": "1000",
                        "realized_usd": realized,
                        "net_pnl_usd": realized,
                        "counts": {"entry_confirmation:filled": 1, "exit_confirmation:filled": 1},
                    },
                    candidate: {
                        "seed_usd": "1000",
                        "realized_usd": "0",
                        "net_pnl_usd": "-3",
                        "counts": {"entry_confirmation:filled": 1},
                    },
                },
            },
        )
        self.close("US", report, "revise")
        return start

    def make_kr(
        self,
        *,
        pnl: int = 2,
        closed: int = 1,
        eligible: bool = True,
        missing_journal: bool = False,
        mismatched_realized: bool = False,
    ) -> datetime:
        start = self.make_plan(
            "KR",
            {
                role: {"policy": policy, "selection": "all", "allocation": "equal"}
                for role, policy in (("incumbent", "rebound"), ("candidate", "rebound_net_target"))
            },
            1000000,
        )
        accounts = {}
        for role, change in (("incumbent", 0), ("candidate", pnl)):
            accounts[role] = {
                "input_sha256": "same",
                "paper": {
                    "account": {
                        "initial_equity": 1000000,
                        "equity": 1000000 + change,
                        "return_pct": change / 10000,
                        "closed_net_realized_krw": (
                            change + int(mismatched_realized) if closed else 0
                        ),
                        "closed": closed,
                        "open_positions": 0,
                        "comparison_eligible": eligible,
                        "mark_basis": "book_midpoint_not_liquidated",
                    },
                    "outcomes": [{"intent_id": "1", "status": "filled", "symbol_id": 10}],
                },
            }
        path = self.prior / "schedule" / "attempts" / "2" / "run" / "status.json"
        atomic_json(self.prior / "schedule" / "status.json", {"restarts": [{}]})
        journal_path = self.prior / "schedule" / "journal.sqlite3"
        with AdaptiveJournal.open(
            journal_path, {"experiment": "shared_feed_slots"}, resume=False
        ) as journal:
            if closed:
                journal.append(
                    {
                        "outputs": {
                            "rebound-entry-cost": {
                                "account_results": {
                                    "incumbent": {"trades": [{"realized_pnl": 0}]},
                                    "candidate": {"trades": [{"realized_pnl": pnl}]},
                                }
                            }
                        },
                    }
                )
            evidence = journal.evidence()
            journal.complete()
        if missing_journal:
            journal_path.unlink()
        atomic_json(
            path,
            {
                "state": "complete",
                "journal": evidence,
                "shared_feed": {"complete": True},
                "shared_slots": [
                    {"slot_id": "rebound-entry-cost", "comparison": {"accounts": accounts}}
                ],
            },
        )
        self.close("KR", path, "retest" if pnl >= 0 else "revise")
        return start

    def test_us_revision_needs_a_matching_profitable_fill_and_threshold_refusal(self) -> None:
        when = self.make_us()
        before = file_digest(self.prior / "decision.json")
        self.assertTrue(write_handoff(self.prior, NEXT_DAY, when, output_dir=self.output))
        result = load_next_plan(self.output / "next_plan.json", "US", NEXT_DAY)
        self.assertEqual(result["strategies"]["candidate"], {"min_reward_risk": 1.5})
        review = read_json(self.output / "review.json")
        self.assertEqual(review["supported_threshold_observations"][0]["exit_events"], [2])
        self.assertEqual(file_digest(self.prior / "decision.json"), before)
        self.assertFalse((self.prior / "lifecycle.json").exists())
        self.assertEqual(read_json(self.output / "lifecycle.json")["phase"], "next_plan_ready")

    def test_us_underperformance_with_unrelated_refusal_holds(self) -> None:
        when = self.make_us(refusal=False, profitable=False)
        self.assertFalse(write_handoff(self.prior, NEXT_DAY, when))
        review = read_json(self.prior / "review.json")
        self.assertEqual(review["action"], "hold_for_new_hypothesis")
        self.assertEqual(review["accounts"]["incumbent"]["symbols"]["ABC"]["loss_fills"], [2])
        with self.assertRaisesRegex(ValueError, "blocked"):
            load_next_plan(self.prior / "next_plan.json", "US", NEXT_DAY)

    def test_kr_retest_is_portable_and_retains_mark_limitations(self) -> None:
        when = self.make_kr()
        self.assertTrue(write_handoff(self.prior, NEXT_DAY, when, output_dir=self.output))
        path = self.output / "next_plan.json"
        result = load_next_plan(path, "KR", NEXT_DAY)
        self.assertEqual(result["strategies"], read_json(self.prior / "plan.json")["strategies"])
        self.assertEqual(result["end"], f"{NEXT_DAY}T15:20:00+09:00")
        review = read_json(self.output / "review.json")
        self.assertEqual(review["accounts"]["candidate"]["fill_outcomes"][0]["symbol_id"], 10)
        self.assertIn("midpoint", review["limitations"][0])
        self.assertFalse((self.prior / "next_plan.json").exists())
        atomic_json(self.output / "prior-decision.json", {"tampered": True})
        with self.assertRaisesRegex(ValueError, "prior-decision.json"):
            load_next_plan(path, "KR", NEXT_DAY)

    def test_kr_unfavorable_comparison_holds_for_a_new_hypothesis(self) -> None:
        when = self.make_kr(pnl=-5)
        self.assertFalse(write_handoff(self.prior, NEXT_DAY, when))
        self.assertEqual(
            read_json(self.prior / "next_plan.json")["action"], "hold_for_new_hypothesis"
        )

    def test_kr_zero_closed_trades_retests_for_new_evidence(self) -> None:
        when = self.make_kr(closed=0)
        self.assertTrue(write_handoff(self.prior, NEXT_DAY, when))
        plan = read_json(self.prior / "next_plan.json")
        self.assertEqual(plan["action"], "retest")
        self.assertIn("no closed-fill performance difference", plan["reason"])

    def test_kr_missing_journal_blocks_the_handoff(self) -> None:
        when = self.make_kr(missing_journal=True)
        self.assertFalse(write_handoff(self.prior, NEXT_DAY, when))
        review = read_json(self.prior / "review.json")
        self.assertEqual(review["action"], "repair_evidence")
        self.assertIn("unavailable", review["limitations"][-1])

    def test_kr_unreconciled_journal_blocks_the_handoff(self) -> None:
        when = self.make_kr(mismatched_realized=True)
        self.assertFalse(write_handoff(self.prior, NEXT_DAY, when))
        review = read_json(self.prior / "review.json")
        self.assertEqual(review["action"], "repair_evidence")
        self.assertIn("realized P&L", review["limitations"][-1])

    def test_elapsed_opening_still_reviews_and_blocks_a_qualified_pair(self) -> None:
        when = self.make_kr()
        with patch("tools.strategy_review_handoff.datetime", wraps=datetime) as clock:
            clock.now.return_value = when
            self.assertFalse(write_handoff(self.prior, NEXT_DAY, when))
        plan = read_json(self.prior / "next_plan.json")
        review = read_json(self.prior / "review.json")
        self.assertEqual(plan["action"], "hold_elapsed")
        self.assertEqual(review["quality"], "qualified")
        self.assertEqual(review["reviewed_action"], "retest")
        self.assertIn("already opened", plan["reason"])

    def test_unmarkable_result_writes_a_blocked_repair_plan(self) -> None:
        when = self.make_kr(eligible=False)
        self.assertFalse(write_handoff(self.prior, NEXT_DAY, when, output_dir=self.output))
        self.assertEqual(read_json(self.output / "review.json")["action"], "repair_evidence")
        status = read_json(self.output / "lifecycle.json")
        self.assertEqual(status["phase"], "blocked")
        self.assertEqual(set(status["phases"]), {"reviewing", "reconsidering", "blocked"})


if __name__ == "__main__":
    unittest.main()
