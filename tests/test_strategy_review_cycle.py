import unittest
from typing import Any

from tools.strategy_review_cycle import SLOT, US_ACCOUNTS, evaluate


class TestStrategyReviewCycle(unittest.TestCase):
    def test_us_observed_comparison_leads_to_followup(self) -> None:
        a, b = US_ACCOUNTS
        report = {
            "state": "closed",
            "comparison_quality": "observed",
            "accounts": {
                a: {
                    "net_pnl_usd": "-20", "seed_usd": "1000",
                    "counts": {"exit_confirmation:filled": 3},
                },
                b: {"net_pnl_usd": "-2", "seed_usd": "1000"},
            },
        }
        result = evaluate("US", report, True)
        self.assertEqual(result["decision"], "retest")
        self.assertAlmostEqual(result["candidate_minus_incumbent_pct"], 1.8)
        report["state"] = "closed_incomplete"
        self.assertEqual(evaluate("US", report, True)["decision"], "repair_evidence")

    def test_kr_requires_matched_observations_and_fresh_marks(self) -> None:
        def account(pnl: float) -> dict[str, Any]:
            return {"input_sha256": "same", "paper": {"account": {
                "comparison_eligible": True, "initial_equity": 1_000_000, "return_pct": pnl,
                "closed": 2,
            }}}

        pair = {"incumbent": account(1.0), "candidate": account(0.5)}
        report = {
            "shared_feed": {"complete": True},
            "shared_slots": [{"slot_id": SLOT, "comparison": {"accounts": pair}}],
        }
        self.assertEqual(evaluate("KR", report, True)["decision"], "revise")
        pair["candidate"]["input_sha256"] = "different"
        self.assertEqual(evaluate("KR", report, True)["decision"], "repair_evidence")
