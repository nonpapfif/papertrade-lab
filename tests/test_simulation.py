"""Free simulation: no real market rules and no clock-only model calls."""

from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock

from test_framework import AT, ROOT, BaseCase

from papertrade.ai import validate_simulation
from papertrade.core import Settings, decimal
from papertrade.simulation import Simulation


class SimulationTests(BaseCase):
    def test_repeated_regime_changes_do_not_spend_tokens(self) -> None:
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "100"},
            decimal(1),
            "observed_reference",
            lead=True,
        )
        self.sim.propose()
        self.store.db.execute("UPDATE jobs SET status='reviewed'")
        for index in range(30):
            self.store.put_cache(
                "simulation_regime:US", ("mixed", "rising", "falling")[index % 3], AT
            )
            self.sim.save_quote(
                "US",
                {"symbol": "TEST", "lastPrice": str(100 + index)},
                decimal(1),
                "observed_reference",
            )
            self.assertEqual(self.sim.propose(), [])
        self.sim.reviewer.review.assert_not_called()

    def test_watch_requires_a_new_crossing_and_is_consumed_once(self) -> None:
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "105"},
            decimal(1),
            "observed_reference",
            lead=True,
        )
        self.sim.propose()
        self.store.db.execute("UPDATE jobs SET status='reviewed'")
        self.sim.set_watches(
            [{"symbol": "TEST", "above": "103", "below": None, "reason": "Watch a fresh crossing"}]
        )
        self.assertEqual(self.sim.propose(), [])
        self.sim.save_quote(
            "US", {"symbol": "TEST", "lastPrice": "102"}, decimal(1), "observed_reference"
        )
        self.assertEqual(self.sim.propose(), [])
        self.sim.save_quote(
            "US", {"symbol": "TEST", "lastPrice": "104"}, decimal(1), "observed_reference"
        )
        self.assertEqual(len(self.sim.propose()), 1)
        self.store.db.execute("UPDATE jobs SET status='reviewed'")
        self.assertEqual(self.sim.propose(), [])

    def test_closed_outcome_allows_one_new_consultation(self) -> None:
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "100"},
            decimal(1),
            "observed_reference",
            lead=True,
        )
        self.sim.propose()
        self.store.db.execute("UPDATE jobs SET status='reviewed'")
        self.store.event("paper_trade_review", {"net_pnl_usd": "-1"})
        self.assertEqual(len(self.sim.propose()), 1)
        self.store.db.execute("UPDATE jobs SET status='reviewed'")
        self.assertEqual(self.sim.propose(), [])

    def test_observation_ignores_calendar_status_and_does_not_call_a_model(self) -> None:
        self.sim.client.get.return_value = {"midRate": "1337.2"}
        self.sim.client.listed.side_effect = lambda exchange, status: (
            [{"symbol": "OLD", "status": "DELISTED"}]
            if exchange == "NASDAQ" and status == "DELISTED"
            else []
        )
        self.sim.client.ranking.return_value = {"rankings": []}
        self.sim.client.prices.side_effect = lambda symbols: [
            {"symbol": symbol, "lastPrice": "10", "currency": "USD"} for symbol in symbols
        ]
        self.sim.observe()
        self.assertIsNotNone(
            self.store.db.execute(
                "SELECT data FROM simulation_quotes WHERE symbol='OLD'"
            ).fetchone()
        )
        self.sim.client.calendar.assert_not_called()
        self.sim.client.warnings.assert_not_called()
        self.sim.reviewer.review.assert_not_called()

    def test_review_has_no_candidate_expiry_and_can_open_a_fractional_short(self) -> None:
        self.store.put_cache("simulation_regime:US", "mixed", AT)
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "100"},
            decimal(1),
            "observed_reference",
            lead=True,
        )
        self.sim.propose()
        self.store.db.execute("UPDATE jobs SET observed_at='2000-01-01T00:00:00+00:00'")
        self.sim.reviewer.review.return_value = {
            "0": {
                "symbol": "TEST",
                "action": "SELL",
                "quantity": "20.125",
                "stop": None,
                "target": None,
                "strategy": "reversal",
                "reason": "Test a short hypothesis",
            }
        }
        self.sim.client.prices.return_value = [{"symbol": "TEST", "lastPrice": "100"}]
        self.assertEqual(self.sim.review(), 1)
        self.assertEqual(decimal(self.store.positions()[0]["quantity"]), decimal("-20.125"))
        self.assertEqual(self.store.equity(), 1000)
        self.sim.client.calendar.assert_not_called()

    def setUp(self) -> None:
        super().setUp()
        self.settings = Settings(self.root, copy.deepcopy(Settings.load(ROOT).data))
        self.sim = Simulation(self.settings, self.store, MagicMock(), MagicMock())

    def apply(self, quantity: str, price: str, key: str, **kwargs) -> None:
        self.store.simulate(
            "TEST",
            "US",
            decimal(quantity),
            decimal(price),
            decimal(1),
            key,
            "Test hypothesis",
            {
                "source_timestamp": "2000-01-01T00:00:00+00:00",
                "fill_basis": "assumed_fill_at_reference_price",
            },
            {},
            **kwargs,
        )

    def test_virtual_credit_does_not_create_profit_and_cash_does_not_limit_fills(self) -> None:
        self.apply("20", "100", "open")
        self.assertEqual(self.store.total_cash(), -1000)
        self.assertEqual(self.store.equity(), 1000)
        self.store.db.execute("INSERT INTO meta VALUES ('simulation_mode','unrestricted')")
        self.assertEqual(self.store.summary(AT)["virtual_debt_usd"], "1000")

    def test_fractional_add_and_partial_exit_preserve_weighted_cost(self) -> None:
        self.apply("3.25", "100", "first")
        self.apply("0.75", "120", "add")
        self.assertEqual(decimal(self.store.positions()[0]["entry_price"]), decimal("103.75"))
        self.apply("-1.5", "130", "partial")
        self.assertEqual(decimal(self.store.positions()[0]["quantity"]), decimal("2.5"))
        trade = json.loads(
            self.store.db.execute("SELECT data FROM trades WHERE event_key='partial'").fetchone()[0]
        )
        self.assertEqual(decimal(trade["realized_pnl_usd"]), decimal("39.375"))
        self.assertEqual(self.store.equity(), 1105)

    def test_short_partial_cover_and_final_cover_account_for_the_liability(self) -> None:
        self.apply("-2", "100", "short")
        self.assertEqual(self.store.total_cash(), 1200)
        self.assertEqual(self.store.equity(), 1000)
        self.apply("0.5", "80", "partial-cover")
        self.assertEqual(self.store.equity(), 1040)
        self.apply("1.5", "90", "cover")
        self.assertEqual(self.store.positions(), [])
        self.assertEqual(self.store.total_cash(), 1025)
        self.assertEqual(decimal(self.store.summary(AT)["realized_pnl_usd"]), 25)

    def test_a_long_can_reverse_directly_into_a_short(self) -> None:
        self.apply("2", "100", "long")
        self.apply("-3", "110", "reverse")
        self.assertEqual(self.store.positions()[0]["direction"], "SHORT")
        self.assertEqual(self.store.equity(), 1020)
        self.apply("1", "100", "flat")
        self.assertEqual(self.store.total_cash(), 1030)

    def test_fill_is_idempotent(self) -> None:
        self.apply("0.25", "100", "same")
        self.apply("0.25", "100", "same")
        self.assertEqual(self.store.total_cash(), 975)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM trades").fetchone()[0], 1)

    def test_a_concurrent_exit_cannot_accidentally_open_a_short(self) -> None:
        self.apply("1", "100", "open")
        self.apply("-1", "100", "close", expected_revision="open")
        self.apply("-1", "100", "stale-close", expected_revision="open")
        self.assertEqual(self.store.positions(), [])
        self.assertEqual(self.store.total_cash(), 1000)

    def test_a_closed_market_old_reference_and_fractional_quantity_are_usable(self) -> None:
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "100", "timestamp": "2000-01-01T00:00:00+00:00"},
            decimal(1),
            "observed_reference",
        )
        self.sim.fill(
            "TEST", "US", decimal("0.125"), "weekend", "Exploratory test", {}, refresh=False
        )
        self.assertEqual(decimal(self.store.positions()[0]["quantity"]), decimal("0.125"))
        evidence = self.store.positions()[0]["price_evidence"]
        self.assertTrue(evidence["market_hours_liquidity_settlement_ignored"])
        self.assertEqual(evidence["fill_basis"], "assumed_fill_at_reference_price")
        self.sim.client.book.assert_not_called()
        self.sim.client.calendar.assert_not_called()
        self.sim.client.warnings.assert_not_called()

    def test_no_overnight_or_thirty_minute_forced_exit_exists(self) -> None:
        self.apply("1", "100", "open")
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "100", "timestamp": "2000-01-01T00:00:00+00:00"},
            decimal(1),
            "observed_reference",
        )
        self.sim.inspect()
        self.assertEqual(len(self.store.positions()), 1)
        self.sim.reviewer.review.assert_not_called()

    def test_optional_short_target_uses_the_reference_and_closes_once(self) -> None:
        self.store.simulate(
            "TEST",
            "US",
            decimal("-0.5"),
            decimal(100),
            decimal(1),
            "short",
            "Test",
            {},
            {"target": "90"},
        )
        self.sim.save_quote(
            "US", {"symbol": "TEST", "lastPrice": "80"}, decimal(1), "observed_reference"
        )
        self.sim.inspect()
        self.sim.inspect()
        self.assertEqual(self.store.positions(), [])
        self.assertEqual(self.store.total_cash(), 1010)

    def test_same_context_does_not_create_repeated_model_work(self) -> None:
        self.store.put_cache("simulation_regime:US", "mixed", AT)
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "100"},
            decimal(1),
            "observed_reference",
            lead=True,
        )
        self.assertEqual(len(self.sim.propose()), 1)
        self.assertEqual(self.sim.propose(), [])
        self.sim.save_quote(
            "US", {"symbol": "TEST", "lastPrice": "102"}, decimal(1), "observed_reference"
        )
        self.assertEqual(self.sim.propose(), [])
        self.store.db.execute("UPDATE jobs SET status='reviewed'")
        self.store.put_cache("simulation_regime:US", "rising", AT)
        self.assertEqual(self.sim.propose(), [])
        self.sim.set_watches(
            [{"symbol": "TEST", "above": "103", "below": None, "reason": "New price breakout"}]
        )
        self.sim.save_quote(
            "US", {"symbol": "TEST", "lastPrice": "104"}, decimal(1), "observed_reference"
        )
        self.assertEqual(len(self.sim.propose()), 1)
        self.assertEqual(self.sim.propose(), [])
        self.sim.reviewer.review.assert_not_called()

    def test_simulation_schema_allows_short_fractional_and_plan_actions(self) -> None:
        actions = [
            {
                "symbol": "TEST",
                "action": "SELL",
                "quantity": "0.125",
                "stop": None,
                "target": None,
                "strategy": "reversal",
                "reason": "Test a short hypothesis",
            },
            {
                "symbol": "TEST",
                "action": "PLAN",
                "quantity": "0",
                "stop": "110",
                "target": "90",
                "strategy": "reversal",
                "reason": "Adjust optional exits",
            },
        ]
        self.assertEqual(len(validate_simulation({"decisions": actions}, {"TEST"})), 2)

    def test_a_small_reference_does_not_need_one_share_affordability(self) -> None:
        self.sim.save_quote(
            "US",
            {"symbol": "TEST", "lastPrice": "10000000"},
            decimal(1),
            "observed_reference",
            lead=True,
        )
        self.store.put_cache("simulation_regime:US", "mixed", AT)
        self.assertEqual(len(self.sim.propose()), 1)
        self.apply("0.00001", "10000000", "fraction")
        self.assertEqual(self.store.equity(), 1000)
