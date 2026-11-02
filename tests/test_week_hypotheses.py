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


def long_engine(policies: list[dict[str, Any]], **changes: Any) -> WeekEngine:
    configs = [
        {**configurations(1)[0], "id": index, "signal": index, **changes} for index in range(2)
    ]
    specs = [{"family": family, "side": "long"} for family in ("momentum", "reversal")]
    return WeekEngine(configs, specs, {}, policies, market="US", long_only=True)


def long_frame(day: str, minute: int, first: float, second: float) -> dict[str, Any]:
    result = frame(day, minute, first)
    at = result["at"]
    result["quotes"][1] = second
    result["marks"][1] = [second, at]
    result["ranks"] = [[[], [[0, 1, 0.01]]], [[], [[1, 1, 0.02]]]]
    return result


def whole_engine(capital: float, policies: list[dict[str, Any]]) -> WeekEngine:
    configs = [{**configurations(1)[0], "id": i, "signal": i} for i in range(2)]
    specs = [
        {
            "family": family,
            "side": "long",
            "column": i,
            "look": i,
            "move": 0,
            "volume": 0,
            "noise": 0,
            "ranking": "strongest",
        }
        for i, family in enumerate(("momentum", "reversal"))
    ]
    return WeekEngine(
        configs,
        specs,
        {},
        policies,
        market="US",
        long_only=True,
        initial_equity=capital,
        whole_shares=True,
    )


def whole_frame(day: str, minute: int, first: float, second: float) -> dict[str, Any]:
    result = long_frame(day, minute, first, second)
    result["long_features"] = [
        [0, [0.02, None] + [None] * 28, [1] * 5, [1] * 5],
        [1, [None, 0.02] + [None] * 28, [1] * 5, [1] * 5],
    ]
    return result


class WeekHypothesisTests(unittest.TestCase):
    def test_whole_share_learning_recomputes_returns_for_each_starting_capital(self) -> None:
        small = whole_engine(50, [{"id": "learn", "mode": "daily"}])
        large = whole_engine(200, [{"id": "learn", "mode": "daily"}])
        day = "2026-10-26"
        for target in (small, large):
            target.day_start(day, scopes(day))
            target.step(whole_frame(day, 5, 100, 10))
            target.step(whole_frame(day, 10, 200, 12))
            target.day_end()
            for book in [*target.shadows, *(state["book"] for state in target.states)]:
                self.assertEqual(book.initial_equity, target.initial_equity)
                self.assertGreaterEqual(book.cash, 0)
                self.assertTrue(
                    all(float(p.quantity).is_integer() for p in book.positions.values())
                )
        self.assertEqual(small.states[0]["chosen"], 1)
        self.assertEqual(large.states[0]["chosen"], 0)
        self.assertEqual(small.completed_scores[0], [0, 0.2])
        self.assertEqual(large.completed_scores[0], [1, 0.2])
        self.assertIn("USD 50", small.report()["weighted_learning"])

    def test_whole_share_rank_skips_hundred_expensive_symbols_for_affordable_candidate(
        self,
    ) -> None:
        target = whole_engine(50, [{"id": "learn", "mode": "daily"}])
        day = "2026-10-26"
        target.day_start(day, scopes(day))
        incoming = whole_frame(day, 5, 100, 100)
        incoming["quotes"] = {s: 100 if s < 100 else 9 for s in range(101)}
        incoming["marks"] = {s: [price, incoming["at"]] for s, price in incoming["quotes"].items()}
        incoming["ranks"] = [[[], [[s, 1, 101 - s] for s in range(100)]], [[], []]]
        incoming["long_features"] = [
            [s, [101 - s] + [None] * 29, [1] * 5, [1] * 5] for s in range(101)
        ]
        target.step(incoming)
        for book in (target.shadows[0], target.states[0]["book"]):
            self.assertEqual(set(book.positions), {100})
            self.assertEqual(book.positions[100].quantity, 5)
            self.assertEqual(book.cash, 5)
            self.assertEqual(book.equity, 50)
            self.assertEqual(book.rejected_unaffordable, 100)
        self.assertLessEqual(len(target.whole_bases), 2)

    def test_whole_cold_accounts_count_all_funding_and_rejections(self) -> None:
        target = whole_engine(
            50, [{"id": "cold", "mode": "daily", "reset_daily": True, "reset_learning": True}]
        )
        for date in range(26, 31):
            day = f"2026-10-{date}"
            target.day_start(day, scopes(day))
            target.step(whole_frame(day, 5, 100, 10))
            target.step(whole_frame(day, 10, 100, 10))
            target.day_end()
        result = target.report()["hypotheses"][0]
        self.assertEqual(result["summary"]["initial_equity"], 250)
        self.assertEqual(result["summary"]["equity"], 250)
        self.assertEqual(result["summary"]["capital_refills"], 200)
        self.assertEqual(result["summary"]["rejected_unaffordable"], 10)
        self.assertEqual(result["summary"]["zero_quantity"], 0)
        self.assertTrue(all(day["start_equity"] == 50 for day in result["daily"]))

    def test_whole_ensemble_cash_quantities_and_prefix_are_conserved(self) -> None:
        policy = {
            "id": "ensemble",
            "mode": "weighted_equal",
            "window": 1,
            "allocation": "family_ensemble",
        }
        first, changed = whole_engine(50, [policy]), whole_engine(50, [copy.deepcopy(policy)])
        day = "2026-10-26"
        for target, final in ((first, 18), (changed, 2)):
            target.day_start(day, scopes(day))
            for minute, price in ((5, 10), (10, 11), (15, final)):
                target.step(whole_frame(day, minute, price, 8))
            target.day_end()
            result = target.report()["hypotheses"][0]
            self.assertTrue(
                all(float(t["quantity_normalized"]).is_integer() for t in result["trades"])
            )
            self.assertTrue(all(t["cash"] >= 0 for t in result["trades"]))
            self.assertEqual(result["summary"]["debt"], 0)
            self.assertAlmostEqual(
                result["summary"]["equity"],
                50 + result["summary"]["realized"] + result["summary"]["unrealized"],
            )
        for key, clock in (("choices", "at"), ("trades", "fill_at")):
            before, after = [target.report()["hypotheses"][0][key] for target in (first, changed)]
            self.assertEqual(
                [x for x in before if x[clock] <= stamp(day, 10)],
                [x for x in after if x[clock] <= stamp(day, 10)],
            )

    def test_whole_accounts_reject_leverage_and_missing_full_features(self) -> None:
        specs = [{"family": "momentum", "side": "long"}]
        with self.assertRaises(ValueError):
            WeekEngine(
                [{**configurations(1)[0], "leverage": 2}],
                specs,
                {},
                [],
                market="US",
                long_only=True,
                whole_shares=True,
                initial_equity=50,
            )
        target = whole_engine(50, [{"id": "fixed", "mode": "static"}])
        target.day_start("2026-10-26", scopes("2026-10-26"))
        with self.assertRaisesRegex(ValueError, "full current"):
            target.step(long_frame("2026-10-26", 5, 10, 10))
        self.assertEqual(target.states[0]["book"].cash, 50)

    def test_long_only_rejects_mixed_configs_and_non_long_ranks_before_trading(self) -> None:
        config = configurations(1)[0]
        for side in ("short", "both", None):
            with self.subTest(side=side), self.assertRaises(ValueError):
                WeekEngine(
                    [config],
                    [{"family": "momentum", "side": side}],
                    {},
                    [{"id": "fixed", "mode": "static"}],
                    market="US",
                    long_only=True,
                )
        target = long_engine([{"id": "fixed", "mode": "static"}])
        target.day_start("2026-10-26", scopes("2026-10-26"))
        bad = long_frame("2026-10-26", 5, 100, 100)
        bad["ranks"][0][1][0][1] = -1
        with self.assertRaises(ValueError):
            target.step(bad)
        self.assertFalse(target.states[0]["book"].trades)
        self.assertFalse(target.shadows)

    def test_cold_daily_and_rolling_accounts_ignore_previous_day_learning(self) -> None:
        policies = [
            {"id": "cold_daily", "mode": "daily", "reset_daily": True, "reset_learning": True},
            {
                "id": "cold_recent",
                "mode": "rolling",
                "window": 3,
                "reset_daily": True,
                "reset_learning": True,
            },
        ]
        first, changed = long_engine(policies), long_engine(copy.deepcopy(policies))
        for target, price in ((first, 200), (changed, 20)):
            target.day_start("2026-10-26", scopes("2026-10-26"))
            for minute, value in ((5, 100), (10, price), (15, price)):
                target.step(long_frame("2026-10-26", minute, value, 100))
            target.day_end()
        self.assertNotEqual(first.completed_scores, changed.completed_scores)
        for target in (first, changed):
            target.day_start("2026-10-27", scopes("2026-10-27"))
            for minute, price in ((5, 100), (10, 80), (15, 130), (20, 90)):
                target.step(long_frame("2026-10-27", minute, price, 100))
            target.day_end()
        for before, after in zip(
            first.report()["hypotheses"], changed.report()["hypotheses"], strict=True
        ):
            for key, clock in (("choices", "at"), ("trades", "fill_at")):
                self.assertEqual(
                    [x for x in before[key] if x[clock][:10] == "2026-10-27"],
                    [x for x in after[key] if x[clock][:10] == "2026-10-27"],
                )
            self.assertEqual(before["daily"][1], after["daily"][1])
            self.assertEqual(before["summary"]["initial_equity"], 2000)
            self.assertTrue(all(x["completed_training_days"] == 0 for x in before["choices"]))

    def test_weighted_scores_use_exact_completed_days_and_optional_observed_today(self) -> None:
        target = long_engine([{"id": "learn", "mode": "weighted_equal", "window": 0}])
        self.assertEqual(target._score("weighted_equal", 0, [0, 0], ""), [0, 0])
        target.day_start("2026-10-26", scopes("2026-10-26"))
        target.completed_scores = [[1, 4], [3, 2]]
        target.shadows[0].equity = 5000
        target.shadows[1].equity = 1000
        self.assertEqual(target._score("weighted_equal", 0, [900, 900], ""), [2, 3])
        actual = target._score("weighted_linear", 0, [900, 900], "")
        self.assertAlmostEqual(actual[0], 7 / 3)
        self.assertAlmostEqual(actual[1], 8 / 3)
        actual = target._score("weighted_exponential", 1, [900, 900], "")
        self.assertAlmostEqual(actual[0], 23 / 7)
        self.assertAlmostEqual(actual[1], 8 / 7)
        actual = target._score("weighted_linear", 1, [900, 900], "")
        self.assertAlmostEqual(actual[0], 19 / 6)
        self.assertAlmostEqual(actual[1], 8 / 6)

    def test_weighted_previous_days_freeze_selection_during_current_day(self) -> None:
        target = long_engine(
            [{"id": "frozen", "mode": "weighted_equal", "window": 0, "refresh_day": True}]
        )
        target.day_start("2026-10-26", scopes("2026-10-26"))
        for minute, first in ((5, 100), (10, 50), (15, 50)):
            target.step(long_frame("2026-10-26", minute, first, 100))
        target.day_end()
        target.day_start("2026-10-27", scopes("2026-10-27"))
        for minute, first in ((5, 100), (10, 200), (15, 300), (20, 400)):
            target.step(long_frame("2026-10-27", minute, first, 100))
        target.day_end()
        result = target.report()["hypotheses"][0]
        choices = [x for x in result["choices"] if x["day"] == "2026-10-27"]
        self.assertEqual(len(choices), 1)
        self.assertEqual(choices[0]["configuration"], 1)
        self.assertEqual(result["summary"]["initial_equity"], 1000)
        self.assertEqual(result["daily"][1]["start_equity"], result["daily"][0]["end_equity"])

    def test_ensemble_uses_one_account_and_never_copies_shadow_wealth(self) -> None:
        target = long_engine(
            [
                {
                    "id": "ensemble",
                    "mode": "weighted_equal",
                    "window": 1,
                    "allocation": "family_ensemble",
                }
            ]
        )
        target.day_start("2026-10-26", scopes("2026-10-26"))
        target.step(long_frame("2026-10-26", 5, 100, 100))
        book = target.states[0]["book"]
        self.assertEqual(book.cash, 0)
        self.assertEqual([p.quantity for p in book.positions.values()], [5, 5])
        target.step(long_frame("2026-10-26", 10, 150, 50))
        self.assertAlmostEqual(book.equity, 1000)
        self.assertAlmostEqual(book.realized, 0)
        exposure = sum(p.quantity * target.prices[s] for s, p in book.positions.items())
        self.assertLessEqual(exposure, book.equity + 1e-8)
        self.assertGreaterEqual(book.cash, -1e-8)
        self.assertTrue(all(p.quantity > 0 for p in book.positions.values()))
        allocations = target.states[0]["choices"][-1]["allocations"]
        self.assertAlmostEqual(sum(a["weight"] for a in allocations), 1)
        self.assertGreater(allocations[0]["weight"], allocations[1]["weight"])
        target.day_end()
        self.assertEqual(target.report()["hypotheses"][0]["summary"]["equity"], 1000)

    def test_ensemble_respects_surviving_exposure_and_original_exit_configuration(self) -> None:
        target = long_engine(
            [
                {
                    "id": "ensemble",
                    "mode": "weighted_equal",
                    "window": 1,
                    "allocation": "family_ensemble",
                }
            ],
            hold=0,
            stop=0.1,
        )
        target.day_start("2026-10-26", scopes("2026-10-26"))
        target.step(long_frame("2026-10-26", 5, 100, 100))
        target.step(long_frame("2026-10-26", 10, 150, 100))
        book = target.states[0]["book"]
        self.assertEqual(book.opened, 2)
        self.assertEqual(book.closed, 0)
        target.step(long_frame("2026-10-26", 15, 80, 100))
        exits = [x for x in book.trades if x["realized_pnl"] is not None]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["reason"], "stop_at_observed_check")
        self.assertEqual(exits[0]["configuration"], 0)
        exposure = sum(p.quantity * target.prices[s] for s, p in book.positions.items())
        self.assertLessEqual(exposure, max(book.equity, 0) + 1e-8)
        self.assertAlmostEqual(book.equity, 1000 + book.realized)

    def test_long_winner_configuration_switch_cannot_create_extra_leverage(self) -> None:
        target = long_engine([{"id": "switch", "mode": "static"}], hold=0)
        target.configs[1]["slots"] = 2
        day = "2026-10-26"
        target.day_start(day, scopes(day))
        target.step(long_frame(day, 5, 100, 100))
        state = target.states[0]
        self.assertEqual(state["book"].opened, 1)
        state["chosen"] = 1
        target.step(long_frame(day, 10, 100, 100))
        book = state["book"]
        self.assertEqual(book.opened, 1)
        self.assertEqual(book.cash, 0)
        self.assertEqual(book.equity, 1000)
        self.assertEqual(
            sum(p.quantity * target.prices[s] for s, p in book.positions.items()), 1000
        )

    def test_ensemble_full_features_find_candidates_beyond_original_top_hundred(self) -> None:
        families = ["a", "b", "c", "d", "e", "f"]
        configs = [
            {**configurations(1)[0], "id": i, "signal": i, "slots": 20, "hold": 0} for i in range(6)
        ]
        specs = [
            {
                "family": family,
                "side": "long",
                "column": i,
                "look": 0,
                "move": 0,
                "volume": 0,
                "noise": 0,
                "ranking": "strongest",
            }
            for i, family in enumerate(families)
        ]
        target = WeekEngine(
            configs,
            specs,
            {},
            [
                {
                    "id": "ensemble",
                    "mode": "weighted_equal",
                    "window": 1,
                    "allocation": "family_ensemble",
                }
            ],
            market="US",
            long_only=True,
        )
        day, at = "2026-10-26", stamp("2026-10-26", 5)
        target.day_start(day, scopes(day))
        state = target.states[0]
        book = state["book"]
        for symbol in range(100):
            book.config = configs[symbol // 20]
            book.fill(symbol, 1, 5, stamp(day, 0), 0, "entry")
            state["owners"][symbol] = symbol // 20
        ranks = [[[], [[symbol, 1, 120 - symbol] for symbol in range(100)]] for _ in range(6)]
        incoming = {
            "at": at,
            "tick": int(datetime.fromisoformat(at).timestamp() // 300),
            "quotes": {symbol: 5 for symbol in range(120)},
            "marks": {symbol: [5, at] for symbol in range(120)},
            "ranks": ranks,
            "long_features": [
                [symbol, [120 - symbol] * 6 + [None] * 24, [1] * 5, [1] * 5]
                for symbol in range(120)
            ],
        }
        target.step(incoming)
        self.assertEqual(set(book.positions), set(range(120)))
        self.assertEqual(book.opened, 120)
        self.assertTrue(all(state["owners"][s] == 5 for s in range(100, 120)))
        self.assertLessEqual(sum(p.quantity * 5 for p in book.positions.values()), 1000 + 1e-8)

    def test_ensemble_family_sessions_do_not_follow_primary_selected_session(self) -> None:
        target = long_engine(
            [
                {
                    "id": "ensemble",
                    "mode": "weighted_equal",
                    "window": 1,
                    "allocation": "family_ensemble",
                }
            ]
        )
        target.configs[0]["session"] = "extended"
        target.configs[1]["session"] = "regular"
        target.day_start("2026-10-26", scopes("2026-10-26"))
        target.step(long_frame("2026-10-26", 10, 100, 100))
        book = target.states[0]["book"]
        self.assertEqual(set(book.positions), {1})
        self.assertEqual(book.equity, 1000)
        self.assertEqual(book.cash, 500)

    def test_long_ensemble_future_changes_preserve_prefix_choices_and_fills(self) -> None:
        policy = {
            "id": "ensemble",
            "mode": "weighted_exponential",
            "window": 1,
            "allocation": "family_ensemble",
        }
        first, changed = long_engine([policy]), long_engine([copy.deepcopy(policy)])
        for target, final in ((first, 200), (changed, 1)):
            target.day_start("2026-10-26", scopes("2026-10-26"))
            for minute, price in ((5, 100), (10, 90), (15, final)):
                target.step(long_frame("2026-10-26", minute, price, 100))
            target.day_end()
        for key, clock in (("choices", "at"), ("trades", "fill_at")):
            before = first.report()["hypotheses"][0][key]
            after = changed.report()["hypotheses"][0][key]
            cutoff = stamp("2026-10-26", 10)
            self.assertEqual(
                [x for x in before if x[clock] <= cutoff], [x for x in after if x[clock] <= cutoff]
            )

    def test_shared_day_boundary_matches_cold_standalone_without_retrading_carry(self) -> None:
        policies = [
            {
                "id": "cold",
                "mode": "rolling",
                "window": 3,
                "refresh_ticks": 3,
                "reset_daily": True,
                "reset_learning": True,
            },
            {"id": "carry", "mode": "daily"},
        ]
        cumulative = long_engine(policies)
        standalone = long_engine(copy.deepcopy(policies))
        monday, tuesday = "2026-10-26", "2026-10-27"
        previous_scope = {
            "US": {
                "start": stamp(monday, 1430),
                "end": stamp(tuesday, 0),
                "sessions": [{"start": stamp(monday, 1430), "end": stamp(tuesday, 0)}] * 3,
            }
        }
        cumulative.day_start(monday, previous_scope)
        cumulative.step(long_frame(monday, 1435, 100, 100))
        cumulative.step(long_frame(monday, 1440, 100, 100))
        cumulative.day_end()
        carry = cumulative.states[1]["book"]
        previous_count = len(carry.trades)
        self.assertEqual(previous_count, 2)
        for target in (cumulative, standalone):
            target.day_start(tuesday, scopes(tuesday))
            target.step(long_frame(tuesday, 0, 100, 100))
        self.assertEqual(len(carry.trades), previous_count)
        self.assertEqual(cumulative.day_steps, 1)
        with self.assertRaises(ValueError):
            cumulative.step(long_frame(tuesday, 0, 100, 100))
        for target in (cumulative, standalone):
            for minute, price in ((5, 90), (10, 110), (15, 100), (20, 120), (25, 90), (30, 100)):
                target.step(long_frame(tuesday, minute, price, 100))
            target.day_end()
        before = cumulative.report()["hypotheses"][0]
        after = standalone.report()["hypotheses"][0]
        self.assertEqual(before["daily"][1], after["daily"][0])
        for key in ("choices", "trades", "curve"):
            self.assertEqual([x for x in before[key] if x["day"] == tuesday], after[key])
        self.assertEqual(
            [x["at"] for x in after["choices"]], [stamp(tuesday, minute) for minute in (0, 15, 30)]
        )

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
