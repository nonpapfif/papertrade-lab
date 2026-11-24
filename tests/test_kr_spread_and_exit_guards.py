"""Tests for bid-ask spread guards, explicit exit reasons, and absolute-loss retest gating."""

from __future__ import annotations

import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from papertrade.jsonio import atomic_json, read_json
from tools.async_layered_paper import (
    ENTRY_SPREAD_BLOCKED,
    EXIT_SPREAD_DEFERRED,
    NET_TARGET_BLOCKED,
    AsyncLayeredPaper,
    book_spread_bps,
)
from tools.layered_paper import Feature, holding_exit_reason, holding_valid
from tools.paper_market import KR
from tools.strategy_catalog import REBOUND_SPREAD_GUARDED
from tools.strategy_review_cycle import decide, main as run_cycle_main
from tools.strategy_review_handoff import _recommend


def _feature(
    *,
    price: float = 1500.0,
    r5: float = 0.01,
    r15: float = -0.02,
    mean: float = 1540.0,
    prior_high: float = 1490.0,
) -> Feature:
    return Feature(
        symbol=1,
        price=price,
        available_at="2026-11-23T00:10:00+00:00",
        bar_end="2026-11-23T00:10:00+00:00",
        r5=r5,
        r15=r15,
        prior_high=prior_high,
        mean=mean,
        efficiency=0.7,
    )


def _kr_event(
    seq: int,
    bid: float,
    ask: float,
    feature: Feature | None = None,
    *,
    volume: int = 100,
) -> dict:
    stamp = f"2026-11-23T00:10:{seq:02d}+00:00"
    return {
        "id": f"e:{seq}",
        "symbol": 1,
        "received_at": stamp,
        "source_received_at": stamp,
        "eligible": True,
        "entries": True,
        "candidate_count": 1,
        "feature": feature if feature is not None else _feature(),
        "book": {
            "timestamp": stamp,
            "currency": "KRW",
            "bids": [{"price": str(bid), "volume": str(volume)}],
            "asks": [{"price": str(ask), "volume": str(volume)}],
        },
    }


class TestExitReasonsAndSpreadGuards(unittest.IsolatedAsyncioTestCase):
    def test_holding_exit_reason_separates_target_and_r5_drop(self) -> None:
        entry = _feature(price=1500.0, r5=0.01, r15=-0.02, mean=1540.0)
        self.assertIsNone(holding_exit_reason("rebound", entry, entry))
        self.assertTrue(holding_valid("rebound", entry, entry))

        target_hit = replace(entry, price=1540.0, r5=0.005)
        self.assertEqual(holding_exit_reason("rebound", target_hit, entry), "rebound_target_reached")
        self.assertFalse(holding_valid("rebound", target_hit, entry))

        r5_drop = replace(entry, price=1495.0, r5=-0.004)
        self.assertEqual(holding_exit_reason("rebound", r5_drop, entry), "rebound_r5_drop")

        both = replace(entry, price=1545.0, r5=-0.004)
        self.assertEqual(
            holding_exit_reason("rebound", both, entry), "rebound_target_and_r5_drop"
        )
        self.assertEqual(
            holding_exit_reason("trend", replace(entry, r15=-0.001), entry),
            "trend_r15_negative",
        )
        self.assertEqual(
            holding_exit_reason("breakout", replace(entry, price=1480.0), entry),
            "breakout_below_prior_high",
        )

    def test_spread_guarded_uses_entry_stop_and_time_stop_instead_of_r5_whipsaw(self) -> None:
        entry = _feature(price=1500.0, r5=0.01, r15=-0.02, mean=1540.0)
        # Trailing r5 drop without an actual -1.5% drop from entry does not exit.
        r5_noise = replace(entry, price=1498.0, r5=-0.006, bar_end="2026-11-23T00:11:00+00:00")
        self.assertIsNone(holding_exit_reason(REBOUND_SPREAD_GUARDED, r5_noise, entry))
        # Target hit exits cleanly.
        target_hit = replace(entry, price=1540.0, bar_end="2026-11-23T00:12:00+00:00")
        self.assertEqual(
            holding_exit_reason(REBOUND_SPREAD_GUARDED, target_hit, entry),
            "rebound_target_reached",
        )
        # -1.5% drop below entry price triggers entry-relative stop loss.
        stop_hit = replace(entry, price=1477.0, r5=0.001, bar_end="2026-11-23T00:13:00+00:00")
        self.assertEqual(
            holding_exit_reason(REBOUND_SPREAD_GUARDED, stop_hit, entry),
            "rebound_stop_loss",
        )
        # Holding >= 30 minutes triggers time stop.
        time_hit = replace(entry, price=1505.0, r5=0.001, bar_end="2026-11-23T00:40:00+00:00")
        self.assertEqual(
            holding_exit_reason(REBOUND_SPREAD_GUARDED, time_hit, entry),
            "rebound_time_stop",
        )

    def test_book_spread_bps_computes_top_of_book_spread(self) -> None:
        ev = _kr_event(1, 1500, 1524)
        spread = book_spread_bps(ev["book"])
        self.assertIsNotNone(spread)
        assert spread is not None
        self.assertAlmostEqual(spread, 24 / 1512 * 10_000, places=4)

    async def test_spread_guarded_blocks_wide_entry_and_defers_empty_book_exit(self) -> None:
        account = AsyncLayeredPaper(1_000_000, policy=REBOUND_SPREAD_GUARDED, market=KR)
        self.addAsyncCleanup(account.close)

        # Wide spread entry (152 bps, like KR 432430) is blocked at intent time.
        await account.submit(_kr_event(1, 1501, 1524, _feature(price=1500, mean=1600)))
        self.assertEqual(account.intents, [])
        self.assertEqual(account.decisions[-1]["blocked_reason"], ENTRY_SPREAD_BLOCKED)

        # Tight spread entry (20 bps, like KR 261780) enters and confirms cleanly.
        entry_feat = _feature(price=5090, mean=5180)
        await account.submit(_kr_event(2, 5090, 5100, entry_feat))
        self.assertIn(1, account.pending)
        await account.submit(_kr_event(4, 5090, 5100, entry_feat))
        self.assertIn(1, account.book.positions)

        # Momentarily empty bid book (2712 bps, like KR 373200) defers exit instead of dumping.
        drop_feat = replace(entry_feat, price=5000, r5=-0.005)
        await account.submit(_kr_event(6, 1485, 1951, drop_feat))
        self.assertNotIn(1, account.pending)
        self.assertIn(1, account.book.positions)
        self.assertEqual(account.decisions[-1]["blocked_reason"], EXIT_SPREAD_DEFERRED)
        self.assertEqual(account.decisions[-1]["exit_reason"], "rebound_stop_loss")

        # Once spread recovers (20 bps), exit proceeds and records exact exit reason on the trade.
        await account.submit(_kr_event(8, 5000, 5010, drop_feat))
        self.assertEqual(account.pending[1].exit_reason, "rebound_stop_loss")
        await account.submit(_kr_event(10, 5000, 5010, drop_feat))
        self.assertNotIn(1, account.book.positions)
        self.assertEqual(account.book.trades[-1]["reason"], "rebound_stop_loss")
        summary = account.summary()
        self.assertEqual(summary["intents"][-1]["exit_reason"], "rebound_stop_loss")
        self.assertEqual(
            summary["spread_guard_counts"],
            {ENTRY_SPREAD_BLOCKED: 1, EXIT_SPREAD_DEFERRED: 1},
        )
        self.assertEqual(summary["exit_reason_counts"], {"rebound_stop_loss": 1})

    async def test_spread_guarded_rechecks_spread_on_confirmation_and_half_spread_target(
        self,
    ) -> None:
        account = AsyncLayeredPaper(1_000_000, policy=REBOUND_SPREAD_GUARDED, market=KR)
        self.addAsyncCleanup(account.close)

        # Spread 40 bps (<= 50 bps), ask=1002, bid=998 -> half-spread=2.
        # Target 1005 minus half-spread 2 = 1003; after 0.23% KR costs, proceeds < entry_cost -> blocked.
        tight_low_target = _feature(price=1000, mean=1005)
        await account.submit(_kr_event(1, 998, 1002, tight_low_target))
        self.assertEqual(account.intents, [])
        self.assertEqual(account.decisions[-1]["blocked_reason"], NET_TARGET_BLOCKED)

        # Entry passes at intent (20 bps), but widens to 150 bps at confirmation -> blocked on confirm.
        good_feat = _feature(price=1000, mean=1040)
        await account.submit(_kr_event(2, 998, 1000, good_feat))
        self.assertIn(1, account.pending)
        await account.submit(_kr_event(4, 985, 1000, good_feat))
        self.assertNotIn(1, account.book.positions)
        self.assertEqual(account.outcomes[-1]["blocked_reason"], ENTRY_SPREAD_BLOCKED)

    def test_absolute_loss_guard_blocks_auto_retest_on_severe_kr_loss(self) -> None:
        # Matches 2026-11-23 KR terminal result (-45.86% vs -25.24%, delta +20.62%).
        cycle_res = decide(-45.86, -25.24, complete=True, traded=True)
        self.assertEqual(cycle_res["decision"], "revise")
        self.assertIn("auto-retest floor", cycle_res["reason"])

        review = {
            "candidate_minus_incumbent_pct": 20.62,
            "accounts": {
                "incumbent": {"closed": 405, "fee_inclusive_return_pct": -45.86},
                "candidate": {"closed": 216, "fee_inclusive_return_pct": -25.24},
            },
            "supported_threshold_observations": [],
        }
        action, reason = _recommend("KR", {"incumbent": {}, "candidate": {}}, review, True)
        self.assertEqual(action, "hold_for_new_hypothesis")
        self.assertIn("auto-retest floor", reason)

        # Mild negative candidate above floor (-0.2%) still allows retest.
        mild = decide(-2.0, -0.2, complete=True, traded=True)
        self.assertEqual(mild["decision"], "retest")

    def test_strategy_review_cycle_accepts_kr_candidate_policy_override(self) -> None:
        day = "2099-11-24"
        regular = {
            "startTime": day + "T09:00:00+09:00",
            "singlePriceAuctionStartTime": day + "T15:20:00+09:00",
            "endTime": day + "T15:30:00+09:00",
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "loss.json"
            output = root / "run"
            atomic_json(evidence, {"strict_snapshot_attribution_reconciled": True})
            with (
                patch("tools.strategy_review_cycle.os.umask"),
                patch(
                    "tools.strategy_review_cycle.review_loss",
                    return_value={"day": "2099-11-23", "finding": "test"},
                ),
                patch("tools.strategy_review_cycle.Settings"),
                patch("tools.strategy_review_cycle.MarketClient") as client,
                patch("tools.strategy_review_cycle.command", return_value="commit"),
                patch("tools.strategy_review_cycle.tree_digest", return_value="tree"),
                patch("tools.strategy_review_cycle.run_kr") as scheduler,
            ):
                client.return_value.calendar.return_value = {
                    "today": {"date": day, "integrated": {"regularMarket": regular}}
                }

                def inspect_plan(out: Path, plan: dict) -> tuple[int, Path]:
                    self.assertEqual(
                        plan["strategies"]["candidate"]["policy"], REBOUND_SPREAD_GUARDED
                    )
                    self.assertEqual(plan["strategies"]["incumbent"]["policy"], "rebound")
                    self.assertEqual(read_json(out / "plan.json"), plan)
                    return 0, out / "result.json"

                scheduler.side_effect = inspect_plan
                code = run_cycle_main(
                    [
                        "--market",
                        "KR",
                        "--day",
                        day,
                        "--evidence",
                        str(evidence),
                        "--output",
                        str(output),
                        "--kr-candidate-policy",
                        REBOUND_SPREAD_GUARDED,
                    ]
                )
                self.assertEqual(code, 0)
                scheduler.assert_called_once()


if __name__ == "__main__":
    unittest.main()
