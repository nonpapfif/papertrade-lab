"""Verify live snapshot consumption while a deferred AI review remains in flight."""

import asyncio
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools.run_kr_sol_astra import ASTRA, SOL, campaign, now


class SolAstraTests(unittest.IsolatedAsyncioTestCase):
    async def test_sol_fill_does_not_wait_for_deferred_astra_review(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            market = Path(temp) / "market.jsonl"
            market.touch()
            output = Path(temp) / "result"
            astra_started = asyncio.Event()
            astra_finished = asyncio.Event()

            def snapshot(price: int) -> None:
                at = now().isoformat()
                rows = [
                    {"symbol": symbol, "currency": "KRW", "lastPrice": price, "timestamp": at}
                    for symbol in ("A", "B", "C")
                ]
                with market.open("a") as stream:
                    stream.write(json.dumps({"observed_at": at, "rows": rows}) + "\n")

            async def decide(
                settings: Any,
                payload: dict[str, Any],
                *,
                model: str,
                deferred: dict[str, str] | None = None,
                holds: dict[str, str] | None = None,
            ) -> tuple[dict, dict]:
                symbols = [item["symbol"] for item in payload["signals"]]
                if model == SOL:
                    self.assertCountEqual(symbols, ["A", "B", "C"])
                    self.assertIsNotNone(deferred)
                    assert deferred is not None
                    deferred["B"] = "Conflicting evidence"
                    assert holds is not None
                    holds["C"] = "Insufficient momentum"
                    return {"A": "Observed momentum"}, {}
                self.assertEqual(model, ASTRA)
                self.assertEqual(symbols, ["B"])
                astra_started.set()
                await astra_finished.wait()
                return {}, {}

            async def wait_for_status(predicate: Any) -> dict[str, Any]:
                while True:
                    path = output / "status.json"
                    if path.exists():
                        state = json.loads(path.read_text())
                        if predicate(state):
                            return state
                    await asyncio.sleep(0.02)

            with patch("tools.run_kr_sol_astra.decide", side_effect=decide):
                task = asyncio.create_task(campaign(market, output, now() + timedelta(seconds=5)))
                try:
                    await asyncio.wait_for(wait_for_status(lambda _: True), 2)
                    snapshot(100)
                    await asyncio.wait_for(wait_for_status(lambda s: s["sweeps"] == 1), 2)
                    snapshot(110)
                    await asyncio.wait_for(astra_started.wait(), 2)
                    snapshot(120)
                    result = await asyncio.wait_for(wait_for_status(lambda s: s["sweeps"] == 3), 2)
                    self.assertEqual(result["ai_calls"], {SOL: 1, ASTRA: 1})
                    self.assertEqual(result["ai_reviews_completed"], {SOL: 1})
                    self.assertTrue(result["ai_in_flight"])
                    self.assertEqual(set(result["account"]["positions"]), {"A"})
                    events = [
                        json.loads(line)
                        for line in (output / "events.jsonl").read_text().splitlines()
                    ]
                    first, decision, escalation = events[:3]
                    self.assertEqual(decision["review_id"], first["review_id"])
                    self.assertEqual(decision["holds"], {"C": "Insufficient momentum"})
                    self.assertEqual(decision["deferred"], {"B": "Conflicting evidence"})
                    self.assertEqual(escalation["parent_review_id"], first["review_id"])
                    self.assertIn("portfolio", first)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
