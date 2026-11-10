"""Exploratory momentum profile and observed-outcome accounting."""

from __future__ import annotations

import copy
import json
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

from papertrade.core import Session, Settings, decimal
from papertrade.engine import Engine
from papertrade.strategy import Bar, aggregate, entry_plan, signal
from tests.test_framework import AT, ROOT, BaseCase, book, candidate


class ExperimentTests(BaseCase):
    def test_unsettled_proceeds_can_fund_an_immediate_new_paper_trade(self) -> None:
        position = self.position()
        self.store.buy(position, decimal(position["cost_usd"]), "entry:first", AT)
        self.store.sell(
            "TEST", decimal(210), decimal(105), "exit:first", "target", AT, "2026-11-02"
        )
        self.store.db.execute("UPDATE cash SET amount='0' WHERE source='initial-paper-capital'")
        self.assertEqual(self.store.settled_cash(AT), 0)
        self.assertEqual(self.store.summary(AT)["buying_power_usd"], "210")
        replacement = self.position()
        replacement["symbol"] = "SECOND"
        self.store.buy(replacement, decimal(replacement["cost_usd"]), "entry:second", AT)
        self.assertEqual(len(self.store.positions()), 1)
        self.assertEqual(self.store.total_cash(), decimal(210) - decimal(replacement["cost_usd"]))
        self.assertEqual(self.store.settled_cash(AT), 0)

    def setUp(self) -> None:
        super().setUp()
        self.settings = Settings(
            self.root, copy.deepcopy(Settings.load(ROOT / "tests" / "fixtures" / "v2").data)
        )

    def test_five_minute_breakout_accepts_average_volume_with_a_falsifiable_setup(self) -> None:
        bars = [
            Bar(
                AT - timedelta(minutes=5 * (16 - i)),
                decimal(99),
                decimal(100),
                decimal(98),
                decimal(99),
                decimal(100),
            )
            for i in range(16)
        ]
        bars[-1] = Bar(
            bars[-1].at, decimal(99), decimal(102), decimal(99), decimal(101), decimal(110)
        )
        diagnostic: dict[str, Any] = {}
        result = signal("TEST", "US", bars, decimal(0), AT, self.settings, diagnostics=diagnostic)
        assert result is not None
        self.assertEqual(result["strategy_profile"], "momentum-experiment-v2")
        self.assertEqual(result["bar_minutes"], 5)
        self.assertEqual(diagnostic["stage"], "passed")
        bars[-1] = Bar(
            bars[-1].at, decimal(99), decimal(102), decimal(99), decimal(101), decimal(90)
        )
        self.assertIsNone(
            signal("TEST", "US", bars, decimal(0), AT, self.settings, diagnostics=diagnostic)
        )
        self.assertEqual(diagnostic["stage"], "volume")

    def test_complete_five_minute_bars_do_not_include_the_unfinished_bucket(self) -> None:
        session = Session("US", "2026-11-02", AT - timedelta(minutes=7), AT + timedelta(hours=1))
        rows = [
            {
                "timestamp": (session.start + timedelta(minutes=i)).isoformat(),
                "openPrice": "100",
                "highPrice": "101",
                "lowPrice": "99",
                "closePrice": "100",
                "volume": "1",
            }
            for i in range(7)
        ]
        bars = aggregate(rows, [session], AT, minutes=5)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].volume, 5)

    def test_current_session_history_needs_one_page_instead_of_three_days(self) -> None:
        sessions = [
            Session(
                "US",
                (AT - timedelta(days=day)).date().isoformat(),
                AT - timedelta(days=day, hours=3),
                AT - timedelta(days=day) + timedelta(hours=3),
            )
            for day in (2, 1, 0)
        ]
        client = MagicMock()
        rows = [
            {
                "timestamp": (AT - timedelta(minutes=i)).isoformat(),
                "openPrice": "100",
                "highPrice": "101",
                "lowPrice": "99",
                "closePrice": "100",
                "volume": "1",
            }
            for i in range(200)
        ]
        client.candles.return_value = {"candles": rows, "nextBefore": rows[-1]["timestamp"]}
        engine = Engine(self.settings, self.store, client, MagicMock())
        bars = engine.history("TEST", sessions, AT)
        client.candles.assert_called_once()
        self.assertEqual(len(bars), 16)
        self.assertEqual(bars[-1].at, AT - timedelta(minutes=5))

    def test_wider_spread_and_later_entry_are_part_of_the_recorded_profile(self) -> None:
        plan = entry_plan(
            candidate(),
            book(bid="100", ask="100.4"),
            AT,
            decimal(1000),
            decimal(1000),
            decimal(1),
            self.settings,
            quantity=2,
        )
        self.assertEqual(plan["exit_rules"]["time_exit_minutes"], 30)
        self.assertEqual(plan["entry_price"], "100.4")

    def test_closed_trade_review_retains_profile_net_loss_and_observed_extremes(self) -> None:
        position = self.position()
        position["strategy_profile"] = "momentum-experiment-v2"
        self.store.buy(position, decimal(position["cost_usd"]), "entry:test", AT)
        initial = decimal(position["mark_usd"])
        self.store.mark("TEST", decimal(210), AT + timedelta(minutes=1))
        self.store.mark("TEST", decimal(190), AT + timedelta(minutes=2))
        self.store.sell(
            "TEST",
            decimal(190),
            decimal(95),
            "exit:test",
            "stop_at_observed_quote",
            AT + timedelta(minutes=2),
            "2026-11-02",
        )
        review = json.loads(
            self.store.db.execute(
                "SELECT data FROM events WHERE kind='paper_trade_review'"
            ).fetchone()[0]
        )
        self.assertEqual(review["outcome"], "loss")
        self.assertEqual(review["strategy_profile"], "momentum-experiment-v2")
        self.assertEqual(decimal(review["max_observed_mark_usd"]), max(initial, decimal(210)))
        self.assertEqual(decimal(review["min_observed_mark_usd"]), 190)
        self.assertFalse(review["ai_invoked"])
