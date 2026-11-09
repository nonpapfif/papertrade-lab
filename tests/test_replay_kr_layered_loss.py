"""Checks for isolated alternatives and fixed-fill loss accounting."""

import gzip
import json
import tempfile
import time
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from tools.adaptive_journal import AdaptiveJournal, JournalError
from tools.async_layered_paper import seconds
from tools.layered_paper import Feature
from tools.paper_market import KR
from tools.replay_kr_layered_loss import (
    Experiment,
    LossRuntime,
    account_attribution,
    compare_status,
    quote_bounds,
    replay,
)
from tools.run_async_live_validation import Runtime, journal_identity, subscription_set
from tools.shared_feed_slots import SharedFeedSlots
from tools.slot_control import SlotCommand, SlotControl

START = "2026-11-09T02:00:00+00:00"
FEATURE = Feature(0, 990, START, START, 0.01, -0.01, 1010, 1000, 0.3)
CONFIG = {
    "capital": 10_000,
    "policy": "rebound",
    "selection": "all",
    "allocation": "equal",
    "delay_seconds": 1,
    "fee_schedule": {},
}


def event(index: int, bid: int, ask: int, feature: Feature = FEATURE) -> dict:
    at = f"2026-11-09T02:00:{index:02}+00:00"
    return {
        "id": f"live:{index}",
        "symbol": 0,
        "provider_symbol": "005930",
        "received_at": at,
        "feature": feature,
        "eligible": True,
        "book": {
            "timestamp": at,
            "currency": "KRW",
            "bids": [{"price": str(bid), "volume": "10"}],
            "asks": [{"price": str(ask), "volume": "10"}],
        },
    }


def experiment(variant: str) -> Experiment:
    return Experiment(CONFIG, variant, lambda *_args: [0])


class ReplayAttributionTests(unittest.TestCase):
    def test_status_comparison_normalizes_numeric_json_keys(self) -> None:
        status = {
            "ai_calls": 0,
            "counts": {},
            "desired": [],
            "protected": [],
            "shared_slots": [{"adaptive": {"position_policy_versions": {256: 1, 1496: 1}}}],
        }
        recorded = json.loads(json.dumps(status))
        compare_status(status, recorded)
        recorded["shared_slots"][0]["adaptive"]["position_policy_versions"]["256"] = True
        with self.assertRaisesRegex(JournalError, r"shared_slots\[0\].adaptive"):
            compare_status(status, recorded)

    def test_equal_sizing_excludes_orders_pending_at_dispatch_even_if_the_book_expires_them(
        self,
    ) -> None:
        account = Experiment(CONFIG, "control", lambda *_args: [0, 1])
        pending = event(1, 995, 996, replace(FEATURE, symbol=1))
        pending.update(symbol=1, provider_symbol="000660")
        account._process(pending)
        self.assertEqual(account.pending[1].quantity, 5)

        account._process(event(32, 995, 996))
        self.assertEqual(account.outcomes[-1]["status"], "expired")
        self.assertEqual(account.pending[0].quantity, 10)

    def test_rebound_net_target_uses_fee_inclusive_limit_cost(self) -> None:
        account = experiment("entry_rebound_net_target")
        account._process(event(1, 998, 999))
        self.assertFalse(account.pending)
        self.assertEqual(account.vetoed_bars, {(0, "BUY", START)})
        account._process(event(2, 996, 997))
        self.assertEqual(account.pending[0].limit, 997)

    def test_exit_uses_executable_target_and_preserves_momentum_exit(self) -> None:
        account = experiment("exit_rebound_executable_target")
        account._process(event(1, 995, 996))
        account._process(event(2, 995, 996))
        target_feature = replace(FEATURE, price=1005)
        account._process(event(3, 998, 999, target_feature))
        self.assertFalse(account.pending)
        momentum_invalidated = replace(target_feature, r5=-0.004)
        account._process(event(4, 997, 998, momentum_invalidated))
        self.assertEqual(account.pending[0].side, "SELL")

    def test_fee_and_midpoint_attribution_reconciles_fixed_fills(self) -> None:
        account = experiment("control")
        entry = event(2, 995, 996)
        exit_ = event(4, 990, 991)
        for row in (
            event(1, 995, 996),
            entry,
            event(3, 990, 991, replace(FEATURE, r5=-0.004)),
            exit_,
        ):
            account._process(row)
        result = account_attribution(
            account,
            [quote_bounds(row["book"]) for row in (entry, exit_)],
            {0: "005930"},
            {"005930": "Example"},
        )
        self.assertEqual(result["median_completed_position_seconds"], 2)
        self.assertEqual(result["by_symbol"][0]["name"], "Example")
        self.assertAlmostEqual(result["totals"]["net_mark_pnl_krw"], account.book.equity - 10_000)
        self.assertAlmostEqual(result["fixed_fill_mark_pnl_without_fees_krw"], -60)
        self.assertAlmostEqual(result["fixed_fill_mark_pnl_at_midpoints_without_fees_krw"], -50)


class SealedReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_sealed_replay_refuses_status_divergence_without_touching_journal(self) -> None:
        with tempfile.TemporaryDirectory() as work:
            root = Path(work)
            run = root / "run"
            run.mkdir()
            with gzip.open(run / "market.jsonl.gz", "wt") as stream:
                stream.write('{"kind":"listings","rows":[]}\n')
            control_path = root / "control.sqlite3"
            journal_path = root / "journal.sqlite3"
            end = seconds(START) + 1800
            identity = journal_identity(10_000, end, KR, slot_control=control_path)

            def make_runtime(capital: float, record: Any) -> Runtime:
                return Runtime(
                    capital,
                    record,
                    profile="broker-constrained",
                    adaptive=True,
                    end_epoch=end,
                    market=KR,
                )

            with (
                SlotControl.open(control_path, initialize=True) as control,
                AdaptiveJournal.open(journal_path, identity, resume=False) as journal,
            ):
                slots = SharedFeedSlots(
                    make_runtime, subscription_set, lambda *_args, **_kwargs: None, journal, control
                )
                day = START[:10]
                calendar = {
                    "today": {
                        "date": day,
                        "integrated": {
                            "regularMarket": {
                                "startTime": START,
                                "endTime": "2026-11-09T06:30:00+00:00",
                                "singlePriceAuctionStartTime": "2026-11-09T06:20:00+00:00",
                            }
                        },
                    }
                }
                strategy = {"policy": "rebound", "selection": "all", "allocation": "equal"}
                command = SlotCommand(
                    "add-one",
                    "add",
                    "one",
                    {"capital": 10_000, "incumbent": strategy, "candidate": strategy},
                )
                control.submit(command, end_epoch=time.time() + 60)
                for sequence, (kind, payload) in enumerate(
                    [
                        ("calendar", (day, calendar)),
                        ("slot_command", asdict(command)),
                        ("book", ("005930", event(0, 995, 996)["book"])),
                        ("final_advance", None),
                    ],
                    1,
                ):
                    await slots._apply(
                        {
                            "sequence": sequence,
                            "kind": kind,
                            "payload": payload,
                            "at": START,
                            "received": START,
                        },
                        0,
                    )
                status = {**slots.status(), "journal": journal.evidence(), "state": "complete"}
                status.pop("committed_sequence", None)
                journal.complete()
                await slots.close()

            recorded = run / "status.json"
            recorded.write_text(json.dumps(status))
            snapshot = journal_path.read_bytes()
            output = root / "report.json"
            await replay(root, output)
            report = json.loads(output.read_text())
            self.assertTrue(report["baseline_stable_status_matched"])
            self.assertEqual(
                report["shadow_control_matches_baseline"], {"incumbent": True, "candidate": True}
            )
            status["shared_slots"][0]["comparison"]["accounts"]["candidate"]["observations"] += 1
            recorded.write_text(json.dumps(status))
            rejected = root / "rejected.json"
            with self.assertRaisesRegex(JournalError, "stable status"):
                await replay(root, rejected)
            self.assertFalse(rejected.exists())
            preserved = json.loads(rejected.with_suffix(".unverified.json").read_text())
            self.assertFalse(preserved["verified"])
            self.assertIn("control", preserved["shadows"]["candidate"])
            self.assertEqual(journal_path.read_bytes(), snapshot)

    async def test_clock_expires_shadow_orders_and_equal_allocation_keeps_other_policy(
        self,
    ) -> None:
        runtime = LossRuntime(
            10_000,
            lambda *_args, **_kwargs: None,
            {"profile": "broker-constrained", "end_epoch": seconds(START) + 1800},
        )
        self.addAsyncCleanup(runtime.close)
        runtime.regular_window = (seconds(START), seconds(START) + 1800)
        change = (
            1,
            0,
            {
                "action": "change",
                "policy": "rebound",
                "selection": "active",
                "allocation": "priority",
                "reason": "Test recorded configuration",
            },
        )
        await runtime._consume_event(
            {
                "sequence": 1,
                "kind": "policy_review",
                "payload": change,
                "received": START,
                "at": START,
            },
            0,
            replaying=True,
        )
        equal = runtime.experiments["candidate"]["allocation_frozen_equal"]
        live = runtime.experiments["candidate"]["entry_rebound_net_target"]
        self.assertEqual(
            (equal.policy, equal.selection, equal.allocation), ("rebound", "active", "equal")
        )
        self.assertEqual(live.allocation, "priority")
        equal._process(event(1, 995, 996))
        self.assertIn(0, equal.pending)
        later = event(32, 995, 996)["received_at"]
        await runtime._consume_event(
            {"sequence": 2, "kind": "clock", "payload": None, "received": later, "at": later},
            0,
            replaying=True,
        )
        self.assertFalse(equal.pending)
        self.assertEqual(equal.outcomes[-1]["status"], "expired")
