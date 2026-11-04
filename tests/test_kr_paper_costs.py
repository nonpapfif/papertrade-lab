"""Check causal fee-aware cash and tax accounting for Korean paper proposals."""

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from papertrade.core import encode
from tools.replay_kr_paper_costs import ALLOCATION_ACCOUNTS, Ledger, follow

STOCK = ("KOSPI", "STOCK")


class CostTests(unittest.TestCase):
    def test_integer_buy_reserves_fee_and_sale_uses_held_quantity(self) -> None:
        ledger = Ledger()
        bought = ledger.apply("005930", Decimal(10), Decimal(10000), STOCK)
        self.assertEqual(bought["outcome"], "filled")
        self.assertEqual(bought["quantity"], 9)
        self.assertGreaterEqual(ledger.cash, 0)
        quantity = bought["quantity"]
        sold = ledger.apply("005930", Decimal(-10), Decimal(11000), STOCK)
        self.assertEqual(sold["quantity"], -quantity)
        self.assertEqual(sold["transaction_tax_krw"], quantity * 11000 * Decimal("0.002"))
        self.assertEqual(ledger.cash, 100000 + ledger.realized)
        self.assertFalse(ledger.positions)
        self.assertEqual(ledger.counts["resized"], 2)

    def test_fractional_legacy_proposal_below_one_share_is_skipped(self) -> None:
        ledger = Ledger()
        outcome = ledger.apply("005930", Decimal("0.9"), Decimal(10000), STOCK)
        self.assertEqual(outcome["outcome"], "below_one_share")
        self.assertEqual(ledger.cash, 100000)
        self.assertFalse(ledger.positions)

    def test_layered_whole_shares_and_unpriced_product_are_separate(self) -> None:
        ledger = Ledger()
        proposal = ledger.apply("005930", Decimal(10), Decimal(10000), STOCK)
        self.assertEqual(proposal["quantity"], Decimal(9))
        self.assertEqual(ledger.cash, Decimal("9986.50"))
        result = ledger.apply("252670", Decimal(1), Decimal(2000), ("KOSPI", "ETF"))
        self.assertEqual(result["outcome"], "unverified_product_withholding")
        self.assertNotIn("252670", ledger.positions)
        sell = ledger.apply("252670", Decimal(-1), Decimal(2500), ("KOSPI", "ETF"))
        self.assertEqual(sell["outcome"], "unverified_product_withholding")
        self.assertEqual(ledger.original_unpriced_sale_value, 2500)
        self.assertEqual(ledger.original_commissions, Decimal("15.675"))

    def test_final_report_reconciles_source_observation_and_proposals(self) -> None:
        at = datetime(2026, 11, 4, 5, 0, tzinfo=UTC)
        quote = {"symbol": "005930", "currency": "KRW", "lastPrice": 10000, "timestamp": at}
        proposal = {
            "symbol": "005930",
            "quantity": 1,
            "source_at": at,
            "observed_at": at,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            comparison, layered, output = (
                root / item for item in ("comparison", "layered", "report")
            )
            comparison.mkdir()
            layered.mkdir()
            listing_path = root / "listings.json"
            listing_path.write_text(
                encode(
                    {
                        "KOSPI": [{"symbol": "005930", "securityType": "STOCK"}],
                        "KOSDAQ": [],
                        "KR_ETC": [],
                    }
                )
            )
            (comparison / "market.jsonl").write_text(
                encode({"observed_at": at, "rows": [quote]}) + "\n"
            )
            (comparison / "events.jsonl").write_text(
                encode(
                    {
                        "kind": "fill",
                        "account": "5pct_fixed",
                        "action": "BUY",
                        "assumed_price_krw": 10000,
                        **proposal,
                    }
                )
                + "\n"
            )
            (layered / "events.jsonl").write_text(
                encode({"kind": "assumed_fill", "price": 10000, **proposal}) + "\n"
            )
            (comparison / "status.json").write_text(
                encode(
                    {
                        "state": "complete",
                        "sweeps": 1,
                        "accounts": {
                            account: {"fills": int(account == "5pct_fixed")}
                            for account in ALLOCATION_ACCOUNTS
                        },
                    }
                )
            )
            (layered / "status.json").write_text(
                encode({"state": "complete", "counts": {"buy_fills": 1}})
            )
            with patch("tools.replay_kr_paper_costs.poll_job", return_value={"returncode": 0}):
                follow(
                    comparison,
                    layered,
                    listing_path,
                    output,
                    at + timedelta(minutes=1),
                    comparison_job=root / "comparison-job",
                    layered_job=root / "layered-job",
                )
            report = json.loads((output / "status.json").read_text())
            self.assertEqual(report["state"], "complete")
            self.assertTrue(report["tax_coverage_complete"])
            self.assertEqual(len(report["accounts"]), 5)
            for account in ("5pct_fixed", "layered"):
                self.assertEqual(
                    Decimal(report["accounts"][account]["cash_krw"]), Decimal("89998.50")
                )
