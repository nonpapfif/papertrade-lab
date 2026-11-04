"""Offline checks for causal policy application and immutable intent ownership."""

import asyncio
import os
import signal
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from papertrade.core import SafetyError, Settings
from tools.layered_paper import Feature
from tools.run_async_live_validation import Runtime, campaign, main
from tools.us_adaptive_ai import ASTRA, SOL, validate_proposal

CURRENT = {"policy": "trend", "selection": "all", "allocation": "equal"}
CHANGE = {**CURRENT, "action": "change", "reason": "Test the recovery rule", "policy": "rebound"}


class AdaptiveTests(unittest.IsolatedAsyncioTestCase):
    def test_closed_configuration_schema(self) -> None:
        self.assertEqual(validate_proposal(CHANGE, CURRENT, SOL), CHANGE)
        for invalid in (
            {**CHANGE, "fee": 0},
            {**CHANGE, "policy": "breakout"},
            {**CHANGE, "policy": "trend"},
            {**CHANGE, "action": "keep"},
            {**CHANGE, "reason": " "},
            {**CURRENT, "action": "defer", "reason": "Example question"},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(SafetyError):
                validate_proposal(invalid, CURRENT, ASTRA)

    async def test_background_change_affects_only_later_entries(self) -> None:
        at = datetime.now(UTC)
        events: list[dict[str, Any]] = []
        runtime = Runtime(
            100,
            lambda kind, **data: events.append({"kind": kind, **data}),
            profile="broker-constrained",
            adaptive=True,
            end_epoch=time.time() + 60,
        )
        runtime.review_settings = cast(Settings, object())
        runtime.regular_window = (at.timestamp() - 90, at.timestamp() + 60)
        stamp = at.isoformat()
        runtime.features["A"] = Feature(
            runtime.identifier("A"), 10, stamp, stamp, 0.01, 0.02, 9, 9, 0.8
        )
        runtime.features["B"] = Feature(
            runtime.identifier("B"), 10, stamp, stamp, 0.01, -0.02, 11, 11, 0.8
        )
        release = asyncio.Event()
        entered = asyncio.Event()

        async def propose(_settings: Settings, payload: dict, model: str) -> dict:
            self.assertEqual(model, SOL)
            self.assertEqual(payload["base_version"], 0)
            entered.set()
            await release.wait()
            return CHANGE

        async def publish(offset: int, kind: str, payload: Any = None) -> None:
            current = (at + timedelta(seconds=offset)).isoformat()
            if kind == "book":
                payload = (
                    payload,
                    {
                        "timestamp": current,
                        "currency": "USD",
                        "asks": [{"price": "10", "volume": "1"}],
                        "bids": [{"price": "9.99", "volume": "1"}],
                    },
                )
            with patch("tools.run_async_live_validation.now", return_value=current):
                runtime.publish(kind, payload)
                await asyncio.wait_for(runtime.queue.join(), 1)

        worker = asyncio.create_task(runtime.consume())
        try:
            with patch("tools.run_async_live_validation.propose", side_effect=propose):
                await publish(0, "selection", ["A", "B"])
                await publish(1, "book", "A")
                await asyncio.wait_for(entered.wait(), 1)
                a, b = runtime.accounts.values()
                self.assertEqual(a.pending, b.pending)
                await publish(2, "clock")
                await publish(2, "book", "B")
                self.assertEqual(b.entry_policy_version, 0)
                self.assertEqual(sum(runtime.review_calls.values()), 1)

            with patch(
                "tools.run_async_live_validation.now",
                return_value=(at + timedelta(seconds=3)).isoformat(),
            ):
                review = runtime.review_task
                assert review is not None
                release.set()
                await asyncio.wait_for(review, 1)
                await asyncio.wait_for(runtime.queue.join(), 1)
            keep = {**CHANGE, "action": "keep", "reason": "Observe the new configuration"}
            second = AsyncMock(return_value=keep)
            with patch("tools.run_async_live_validation.propose", second):
                await publish(4, "book", "A")
                review = runtime.review_task
                assert review is not None
                await asyncio.wait_for(review, 1)
                await asyncio.wait_for(runtime.queue.join(), 1)
            assert second.await_args is not None
            payload = second.await_args.args[1]
            self.assertEqual(payload["base_version"], 1)
            self.assertEqual(
                [(item["symbol"], item["family"]) for item in payload["new_rule_opportunities"]],
                [("B", "rebound")],
            )
            self.assertEqual(payload["accounts"]["candidate"]["positions"][0]["quantity"], 1)
            runtime.review_settings = None

            self.assertEqual(b.owners[0][0], "trend")
            self.assertEqual(b.owner_policy_versions[0], 0)
            await publish(5, "book", "B")
            self.assertNotIn(1, a.pending)
            self.assertEqual(b.pending[1].strategy, "rebound")
            self.assertEqual(b.pending[1].policy_version, 1)
            self.assertTrue(runtime.status()["comparison"]["inputs_aligned"])
            self.assertEqual([e["kind"] for e in events].count("ai_policy_applied"), 1)
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            await runtime.close()

    async def test_expired_campaign_stops_before_initialization(self) -> None:
        with patch("tools.run_async_live_validation.Settings.load") as load:
            with self.assertRaisesRegex(ValueError, "future cutoff"):
                await campaign(
                    Path("unused"),
                    1200,
                    100,
                    profile="broker-constrained",
                    adaptive=True,
                    end_epoch=time.time() - 1,
                )
            load.assert_not_called()

    async def test_shutdown_cancels_a_review_after_specific_escalation(self) -> None:
        runtime = Runtime(
            100,
            lambda *_args, **_kwargs: None,
            profile="broker-constrained",
            adaptive=True,
            end_epoch=time.time() + 60,
        )
        reviewing, cancelled = asyncio.Event(), asyncio.Event()
        question = "The recovery and trend hypotheses disagree on the same completed bars"

        async def review(_settings: Settings, payload: dict, model: str) -> dict:
            if model == SOL:
                return {**CURRENT, "action": "defer", "reason": question}
            self.assertEqual(model, ASTRA)
            self.assertEqual(payload["sol_question"], question)
            self.assertEqual(payload["evidence_sequence"], 42)
            reviewing.set()
            try:
                return await asyncio.Future()
            finally:
                cancelled.set()

        with patch("tools.run_async_live_validation.propose", side_effect=review):
            runtime.review_task = asyncio.create_task(
                runtime._review(
                    cast(Settings, object()), {"current": CURRENT, "evidence_sequence": 42}
                )
            )
            try:
                await asyncio.wait_for(reviewing.wait(), 1)
            finally:
                await runtime.close()
        self.assertTrue(cancelled.is_set())
        self.assertEqual(runtime.review_calls, {SOL: 1, ASTRA: 1})
        self.assertEqual(runtime.accounts["candidate"].entry_policy_version, 0)

    def test_sigterm_cancels_the_adaptive_campaign(self) -> None:
        reached_cleanup = False
        end = (datetime.now(UTC) + timedelta(minutes=1)).isoformat()

        async def wait_for_signal(*_args: Any, **_kwargs: Any) -> None:
            nonlocal reached_cleanup
            os.kill(os.getpid(), signal.SIGTERM)
            try:
                await asyncio.Future()
            finally:
                reached_cleanup = True

        argv = [
            "validation",
            "--profile",
            "broker-constrained",
            "--adaptive",
            "--end-at",
            end,
            "--output",
            "unused",
        ]
        with (
            patch("sys.argv", argv),
            patch("tools.run_async_live_validation.campaign", side_effect=wait_for_signal),
            self.assertRaisesRegex(SystemExit, "interrupted"),
        ):
            main()
        self.assertTrue(reached_cleanup)
