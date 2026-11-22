import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from papertrade.jsonio import atomic_json, read_json
from tools.paper_market import US
from tools.source_identity import file_digest
from tools.strategy_review_cycle import SLOT, US_ACCOUNTS, evaluate, main, next_market_session
from tools.upstream_paper_accounts import reward_risk_profiles


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

    def test_us_followup_evaluates_the_frozen_threshold_pair(self) -> None:
        profiles = tuple(reward_risk_profiles(1.0, 1.5))
        self.assertEqual(profiles, (US_ACCOUNTS[0], "source_min_reward_risk_1.5"))
        report = {
            "state": "closed", "comparison_quality": "observed",
            "accounts": {
                profiles[0]: {"net_pnl_usd": "-8", "seed_usd": "1000"},
                profiles[1]: {"net_pnl_usd": "-5", "seed_usd": "1000"},
            },
        }
        result = evaluate("US", report, True, us_accounts=profiles)
        self.assertAlmostEqual(result["candidate_minus_incumbent_pct"], 0.3)
        self.assertEqual(evaluate("US", report, True)["decision"], "repair_evidence")

    def test_handoff_uses_provider_next_business_day_and_requires_future_open(self) -> None:
        calendar = {"nextBusinessDay": {"date": "2026-11-27", "regularMarket": {
            "startTime": "2026-11-27T09:30:00-05:00", "endTime": "2026-11-27T13:00:00-05:00",
        }}}
        opens = datetime.fromisoformat("2026-11-27T09:30:00-05:00")
        with patch("tools.strategy_review_cycle.datetime") as clock:
            clock.now.return_value = datetime.fromisoformat("2026-11-26T15:00:00+00:00")
            self.assertEqual(next_market_session(US, "2026-11-25", calendar), ("2026-11-27", opens))
            clock.now.return_value = opens
            with self.assertRaisesRegex(ValueError, "already opened"):
                next_market_session(US, "2026-11-25", calendar)

    def test_runner_freezes_the_followup_before_entering_the_kr_scheduler(self) -> None:
        day = "2099-11-23"
        regular = {
            "startTime": day + "T09:00:00+09:00",
            "singlePriceAuctionStartTime": day + "T15:20:00+09:00",
            "endTime": day + "T15:30:00+09:00",
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "review.json"
            incoming = root / "next_plan.json"
            output = root / "run"
            atomic_json(evidence, {"day": "2099-11-22"})
            atomic_json(incoming, {})
            frozen = {
                "start": regular["startTime"], "end": regular["singlePriceAuctionStartTime"],
                "capital": 200000, "evidence": "/unmounted/bootstrap.json",
                "review": {"path": "review.json", "sha256": file_digest(evidence)},
                "previous_decision": {"path": "prior-decision.json", "sha256": "prior"},
                "strategies": {
                    "incumbent": {"policy": "rebound", "selection": "all", "allocation": "equal"},
                    "candidate": {
                        "policy": "rebound_net_target", "selection": "all", "allocation": "equal",
                    },
                },
            }
            with (
                patch("tools.strategy_review_cycle.os.umask"),
                patch("tools.strategy_review_cycle.load_next_plan", return_value=frozen),
                patch("tools.strategy_review_cycle.Settings"),
                patch("tools.strategy_review_cycle.MarketClient") as client,
                patch("tools.strategy_review_cycle.command", return_value="commit"),
                patch("tools.strategy_review_cycle.tree_digest", return_value="tree"),
                patch("tools.strategy_review_cycle.run_kr") as scheduler,
            ):
                client.return_value.calendar.return_value = {
                    "today": {"date": day, "integrated": {"regularMarket": regular}}
                }

                def inspect_plan(out: Path, plan: dict[str, Any]) -> tuple[int, Path]:
                    self.assertEqual(read_json(out / "plan.json"), plan)
                    self.assertEqual(plan["strategies"], frozen["strategies"])
                    self.assertEqual(plan["capital"], frozen["capital"])
                    self.assertEqual(Path(plan["evidence"]), evidence.resolve())
                    self.assertEqual(plan["next_plan"]["sha256"], file_digest(incoming))
                    return 0, out / "result.json"

                scheduler.side_effect = inspect_plan
                self.assertEqual(main([
                    "--market", "KR", "--day", day,
                    "--output", str(output), "--next-plan", str(incoming),
                ]), 0)
                scheduler.assert_called_once()

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
