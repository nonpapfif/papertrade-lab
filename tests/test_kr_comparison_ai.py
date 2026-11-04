"""Validate explicit and exhaustive KRW review dispositions."""

import unittest
from decimal import Decimal
from typing import Any

from papertrade.core import SafetyError
from tools.kr_comparison_ai import validate_review


class ReviewValidationTests(unittest.TestCase):
    def test_approval_hold_and_escalation_cover_all_signals(self) -> None:
        holds: dict[str, str] = {}
        deferred: dict[str, str] = {}
        approvals, watches = validate_review(
            {
                "approvals": [{"symbol": "A", "reason": "Momentum"}],
                "holds": [{"symbols": ["B", "C"], "reason": "Weak move"}],
                "deferred": [{"symbol": "D", "reason": "Conflicting data"}],
                "watches": [{"symbol": "B", "price_krw": "100"}],
            },
            {"A", "B", "C", "D"},
            holds=holds,
            deferred=deferred,
        )
        self.assertEqual(approvals, {"A": "Momentum"})
        self.assertEqual(holds, {"B": "Weak move", "C": "Weak move"})
        self.assertEqual(deferred, {"D": "Conflicting data"})
        self.assertEqual(watches, {"B": Decimal(100)})

    def test_omission_and_conflicting_decisions_are_rejected(self) -> None:
        responses: list[dict[str, Any]] = [
            {"approvals": [], "holds": [], "watches": []},
            {
                "approvals": [{"symbol": "A", "reason": "Momentum"}],
                "holds": [{"symbols": ["A"], "reason": "Weak move"}],
                "watches": [],
            },
        ]
        for response in responses:
            with self.subTest(response=response), self.assertRaises(SafetyError):
                validate_review(response, {"A"})
