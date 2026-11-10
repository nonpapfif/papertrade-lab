"""Independent entry attribution and read-only ledger reconciliation."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from tests.test_framework import AT, BaseCase
from tools.review_round import review


class RoundReviewTests(BaseCase):
    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            table: [dict(row) for row in self.store.db.execute(f"SELECT * FROM {table}")]
            for table in ("trades", "ai_calls", "positions", "cash", "events")
        }
        result.update(names={}, quotes=[], job_evidence={}, cutoff_utc="2099-01-01T00:00:00+00:00")
        return result

    def fill(self, quantity: str, price: str, reason: str = "Test") -> None:
        self.store.simulate(
            "TEST", "US", Decimal(quantity), Decimal(price), Decimal(1),
            f"manual:{self.store.db.execute('SELECT COUNT(*) FROM trades').fetchone()[0]}",
            reason, {}, {},
        )

    def test_partial_long_exit_then_reversal_preserves_allocation(self) -> None:
        self.fill("2", "100")
        self.fill("2", "120")
        self.fill("-1", "130")
        self.fill("-5", "90")
        result = review(self.snapshot())
        self.assertEqual(result["summary"]["realized"], Decimal(-40))
        self.assertEqual(result["lots"][0]["realized"], 0)
        self.assertEqual(result["lots"][1]["realized"], Decimal(-40))
        self.assertEqual(result["trades"][-1]["action"], "reverse")
        self.assertNotEqual(
            result["trades"][-1]["closed_episode"], result["trades"][-1]["opened_episode"]
        )
        self.assertEqual(result["summary"]["open_positions"], 1)

    def test_short_addition_has_separate_negative_outcome(self) -> None:
        self.fill("-2", "100")
        self.fill("-2", "80")
        self.fill("1", "70")
        self.fill("3", "95")
        result = review(self.snapshot())
        self.assertEqual(result["summary"]["realized"], Decimal(5))
        self.assertEqual(result["lots"][0]["realized"], Decimal("22.5"))
        self.assertEqual(result["lots"][1]["realized"], Decimal("-17.5"))
        self.assertEqual(result["summary"]["open_positions"], 0)

    def test_execution_gap_uses_the_revised_plan(self) -> None:
        self.fill("1", "100")
        self.store.event(
            "simulation_plan", {"symbol": "TEST", "plan": {"stop": "105", "target": "120"}}
        )
        self.fill("-1", "102", "optional_stop")
        result = review(self.snapshot())
        self.assertEqual(result["summary"]["stop_gap_usd"], Decimal(3))
        self.assertEqual(result["trades"][-1]["active_stop"], "105")

    def test_inconsistent_cash_is_reported_instead_of_balanced_with_a_plug(self) -> None:
        self.fill("1", "100")
        snapshot = self.snapshot()
        snapshot["cash"][0]["amount"] = "999"
        with self.assertRaisesRegex(ValueError, "cash_difference"):
            review(json.loads(json.dumps(snapshot)))

    def test_legacy_cost_loss_is_preserved_even_when_the_share_price_rises(self) -> None:
        self.store.buy(
            {"symbol": "TEST", "market": "US", "quantity": 1,
             "entry_price": "100", "cost_usd": "101"},
            Decimal(101), "entry:legacy", AT,
        )
        self.store.sell(
            "TEST", Decimal(100), Decimal(101), "exit:legacy", "time_exit", AT, "2026-10-30"
        )
        result = review(self.snapshot())
        self.assertEqual(result["summary"]["realized"], Decimal(-1))
        self.assertEqual(result["summary"]["cash"], Decimal(999))
        self.assertGreater(result["trades"][-1]["price"], result["trades"][-1]["book_entry_price"])
