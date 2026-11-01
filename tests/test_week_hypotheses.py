"""Causality, original-position rules, and funding of weekly hypothesis accounts."""

from __future__ import annotations

import copy
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import Mock

from tools.search_portfolios import configurations
from tools.week_hypotheses import WeekEngine


def stamp(day: str, minute: int) -> str:
    return (datetime.fromisoformat(day).replace(tzinfo=UTC) + timedelta(minutes=minute)).isoformat()


def scopes(day: str) -> dict[str, Any]:
    return {
        "US": {
            "start": stamp(day, 0),
            "end": stamp(day, 30),
            "sessions": [
                {"start": stamp(day, 0), "end": stamp(day, 5)},
                {"start": stamp(day, 5), "end": stamp(day, 10)},
                {"start": stamp(day, 10), "end": stamp(day, 30)},
            ],
        }
    }


def frame(day: str, minute: int, price: float) -> dict[str, Any]:
    at = stamp(day, minute)
    return {
        "at": at,
        "tick": int(datetime.fromisoformat(at).timestamp() / 300),
        "quotes": {0: price},
        "marks": {0: [price, at]},
        "ranks": [[[], [[0, 1, 0.01]]], [[], [[0, -1, 0.01]]]],
    }


def engine(policies: list[dict[str, Any]], **changes: Any) -> WeekEngine:
    configs = [
        {**configurations(1)[0], "id": index, "signal": index, **changes} for index in range(2)
    ]
    specs = [{"family": "momentum"}, {"family": "reversal"}]
    return WeekEngine(configs, specs, {"US": [0, 1]}, policies, market="US")


def replay_day(target: WeekEngine, day: str, prices: list[float]) -> dict[str, Any]:
    target.day_start(day, scopes(day))
    for index, price in enumerate(prices, 1):
        target.step(frame(day, index * 5, price))
    return target.day_end()


class WeekHypothesisTests(unittest.TestCase):
    def test_future_price_changes_cannot_change_earlier_choices_or_trades(self) -> None:
        policies = [
            {"id": "daily", "mode": "daily"},
            {"id": "recent", "mode": "rolling", "window": 3},
        ]
        original = engine(policies)
        changed = engine(policies)
        replay_day(original, "2026-10-26", [100, 110, 105, 130, 80, 100])
        replay_day(changed, "2026-10-26", [100, 110, 105, 1, 900, 2])
        cutoff = stamp("2026-10-26", 15)
        for before, after in zip(
            original.report()["hypotheses"], changed.report()["hypotheses"], strict=True
        ):
            for key, clock in (("choices", "at"), ("trades", "fill_at"), ("curve", "at")):
                self.assertEqual(
                    [item for item in before[key] if item[clock] <= cutoff],
                    [item for item in after[key] if item[clock] <= cutoff],
                )

    def test_previous_day_policy_never_uses_current_day_scores(self) -> None:
        target = engine([{"id": "previous", "mode": "previous_day"}])
        replay_day(target, "2026-10-26", [100, 50, 50, 50, 50, 50])
        first = target.report()["hypotheses"][0]["choices"]
        self.assertEqual({choice["configuration"] for choice in first}, {0})
        replay_day(target, "2026-10-27", [100, 200, 200, 200, 200, 200])
        second = [
            choice
            for choice in target.report()["hypotheses"][0]["choices"]
            if choice["day"] == "2026-10-27"
        ]
        self.assertEqual({choice["configuration"] for choice in second}, {1})
        self.assertEqual({choice["completed_training_days"] for choice in second}, {1})

    def test_daily_learning_resets_but_cumulative_learning_retains_prior_rewards(self) -> None:
        target = engine([{"id": "daily", "mode": "daily"}, {"id": "carry", "mode": "cumulative"}])
        replay_day(target, "2026-10-26", [100, 140, 140, 140, 140, 140])
        replay_day(target, "2026-10-27", [100, 90, 90, 90, 90, 90])
        hypotheses = {row["id"]: row for row in target.report()["hypotheses"]}
        self.assertEqual(hypotheses["daily"]["choices"][-1]["configuration"], 1)
        self.assertEqual(hypotheses["carry"]["choices"][-1]["configuration"], 0)
        for policy in hypotheses.values():
            self.assertEqual(policy["daily"][1]["start_equity"], policy["daily"][0]["end_equity"])

    def test_shadow_profit_never_becomes_actual_account_cash(self) -> None:
        target = engine([{"id": "learner", "mode": "daily"}])
        replay_day(target, "2026-10-26", [100, 50, 50, 50, 50, 50])
        result = target.report()["hypotheses"][0]
        self.assertEqual(result["choices"][1]["configuration"], 1)
        self.assertEqual(result["summary"]["equity"], 500)
        self.assertEqual(target.shadows[1].equity, 1500)

    def test_original_position_exit_rule_survives_algorithm_switch(self) -> None:
        configs = [
            {**configurations(1)[0], "id": 0, "signal": 0, "hold": 0, "stop": 0.1},
            {**configurations(1)[0], "id": 1, "signal": 1, "hold": 1, "stop": 0},
        ]
        target = WeekEngine(
            configs,
            [{"family": "a"}, {"family": "b"}],
            {},
            [{"id": "switch", "mode": "random"}],
            market="US",
        )
        target.states[0]["rng"] = Mock(choice=Mock(side_effect=[0, 1, 1, 1, 1, 1]))
        target.day_start("2026-10-26", scopes("2026-10-26"))
        target.step(frame("2026-10-26", 5, 100))
        target.step(frame("2026-10-26", 10, 100))
        self.assertEqual(target.states[0]["book"].closed, 0)
        target.step(frame("2026-10-26", 15, 80))
        trades = target.states[0]["book"].trades
        self.assertEqual(trades[1]["reason"], "stop_at_observed_check")
        self.assertEqual(trades[1]["configuration"], 0)
        self.assertEqual(trades[2]["configuration"], 1)

    def test_negative_equity_and_debt_carry_without_new_funding(self) -> None:
        target = engine([{"id": "fixed", "mode": "fixed30"}], leverage=10, hold=0)
        replay_day(target, "2026-10-26", [100, 1, 1, 1, 1, 1])
        target.day_start("2026-10-27", scopes("2026-10-27"))
        self.assertEqual(target.states[0]["start_equity"], -8900)
        target.step(frame("2026-10-27", 5, 1))
        target.day_end()
        summary = target.report()["hypotheses"][0]["summary"]
        self.assertEqual(summary["initial_equity"], 1000)
        self.assertEqual(summary["equity"], -8900)
        self.assertEqual(summary["debt"], 9000)

    def test_independent_daily_resets_keep_all_losses_and_funding_visible(self) -> None:
        target = engine(
            [{"id": "cold", "mode": "fixed30", "reset_daily": True}], leverage=10, hold=0
        )
        replay_day(target, "2026-10-26", [100, 1, 1, 1, 1, 1])
        replay_day(target, "2026-10-27", [1, 1, 1, 1, 1, 1])
        result = target.report()["hypotheses"][0]
        self.assertEqual(result["accounting"], "independent_daily_accounts")
        self.assertEqual(result["summary"]["initial_equity"], 2000)
        self.assertEqual(result["summary"]["equity"], -7900)
        self.assertEqual(result["summary"]["pnl"], -9900)
        self.assertIsNone(result["summary"]["continuous_equity"])
        self.assertEqual({trade["day"] for trade in result["trades"]}, {"2026-10-26", "2026-10-27"})

    def test_family_filter_and_stable_random_seeds(self) -> None:
        policies = [
            {"id": "family", "mode": "daily", "families": ["reversal"]},
            {"id": "random", "mode": "random", "seed": 17},
        ]
        first, second = engine(policies), engine(copy.deepcopy(policies))
        for target in (first, second):
            replay_day(target, "2026-10-26", [100, 110, 90, 80, 120, 100])
        reports = [target.report()["hypotheses"] for target in (first, second)]
        self.assertEqual({choice["configuration"] for choice in reports[0][0]["choices"]}, {1})
        self.assertEqual(reports[0][1]["choices"], reports[1][1]["choices"])

    def test_fixed_reference_is_explicitly_retrospective_and_skips_shadow_training(self) -> None:
        target = engine([{"id": "fixed", "mode": "fixed30"}])
        replay_day(target, "2026-10-26", [100, 100, 100, 100, 100, 100])
        self.assertFalse(target.shadows)
        self.assertEqual(target.report()["hypotheses"][0]["classification"], "retrospective_30_fit")

    def test_future_or_nonfinite_prices_and_duplicate_frames_are_rejected(self) -> None:
        for invalid in ("future", "nonfinite", "duplicate"):
            with self.subTest(invalid=invalid):
                target = engine([{"id": "static", "mode": "static"}])
                target.day_start("2026-10-26", scopes("2026-10-26"))
                first = frame("2026-10-26", 5, 100)
                if invalid == "future":
                    first["marks"][0][1] = stamp("2026-10-26", 10)
                elif invalid == "nonfinite":
                    first["marks"][0][0] = float("inf")
                else:
                    target.step(first)
                with self.assertRaises(ValueError):
                    target.step(first)


if __name__ == "__main__":
    unittest.main()
