"""Verify the Korean comparison handoff without market or model calls."""

import asyncio
import hashlib
import json
import tempfile
import unittest
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

from papertrade.core import encode
from tools.continue_kr_paper_comparison import Replay, continue_campaign
from tools.kr_comparison_ai import SOL
from tools.run_kr_paper_comparison import Comparison, campaign


class ContinuationTests(unittest.IsolatedAsyncioTestCase):
    async def test_pending_fills_and_completed_reviews_survive_cutoff(self) -> None:
        runner = "tools.run_kr_paper_comparison"
        extension = "tools.continue_kr_paper_comparison"
        start = datetime(2026, 11, 4, 3, tzinfo=UTC)
        cutoff = start + timedelta(minutes=13)
        until = cutoff + timedelta(minutes=1)
        clock = start
        real_sleep = asyncio.sleep

        def timestamp() -> datetime:
            nonlocal clock
            clock += timedelta(microseconds=1)
            return clock

        async def advance(delay: float) -> None:
            nonlocal clock
            await real_sleep(0)
            clock += timedelta(minutes=1)

        def snapshot() -> list[dict[str, Any]]:
            minute = (clock - start) // timedelta(minutes=1)
            return [
                {
                    "symbol": symbol,
                    "currency": "KRW",
                    "timestamp": clock.isoformat(),
                    "lastPrice": 100 + minute,
                }
                for symbol in ("005930", "000660")
            ]

        async def approve(
            settings: Any,
            pair: Comparison,
            signals: list[dict[str, Any]],
            deadline: datetime,
            record: Callable[..., None],
            calls: Counter[str],
            completed: Counter[str],
        ) -> None:
            calls[SOL] += 1
            record(
                "ai_review",
                review_id=calls.total(),
                model=SOL,
                signals=signals,
                portfolio=pair.accounts["ai"].summary(pair.prices),
            )
            at = timestamp()
            approvals = {"005930": "New directional signal"}
            watches = {"000660": Decimal(113)}
            record(
                "ai_decision",
                review_id=calls.total(),
                model=SOL,
                approvals=approvals,
                watches=watches,
                decision_at=at,
                after_cutoff=False,
            )
            pair.approve(signals, approvals, at, watches)
            completed[SOL] += 1

        with tempfile.TemporaryDirectory() as temp:
            job = Path(temp) / "job"
            old_run = job / "run"
            with (
                patch(f"{runner}.now", side_effect=timestamp),
                patch(f"{runner}.asyncio.sleep", side_effect=advance),
                patch(f"{runner}.CachedTokenClient") as client,
                patch(f"{runner}.LiveFeed") as live_feed,
                patch(f"{runner}.review_cascade", side_effect=approve),
            ):
                client.return_value.listed.return_value = [
                    {"symbol": "005930"},
                    {"symbol": "000660"},
                ]
                live_feed.return_value.snapshot.side_effect = snapshot
                await campaign(old_run, cutoff, allocation_comparison=True)

            replay = Replay(old_run)
            self.assertEqual(replay.sweeps, 13)
            self.assertEqual(replay.review_counts()["5pct"]["completed"][SOL], 1)
            for pair in replay.pairs.values():
                self.assertEqual(pair.watches["000660"][0], Decimal(113))
                for account in pair.accounts.values():
                    self.assertEqual(account.fills, 0)
                    self.assertIn("005930", account.pending)

            feed = Path(temp) / "probe" / "standalone-market.jsonl"
            feed.parent.mkdir()
            feed.write_text(encode({"observed_at": timestamp(), "rows": snapshot()}) + "\n")
            (feed.parent / "status.json").write_text(
                encode({"state": "complete", "counts": {"standalone": 1}})
            )
            combined = Path(temp) / "combined"
            with (
                patch(f"{extension}.now", side_effect=timestamp),
                patch(f"{extension}.asyncio.sleep", side_effect=advance),
                patch(f"{extension}.poll_job", return_value={"returncode": 0}),
                patch(f"{extension}.review_cascade") as review,
            ):
                await continue_campaign(job, feed, combined, until)
                self.assertEqual(review.await_count, 2)
                for invocation in review.await_args_list:
                    (signal,) = invocation.args[2]
                    self.assertEqual(signal["symbol"], "000660")
                    self.assertEqual(signal["trigger"], "watch_crossing")
            report = json.loads((combined / "comparison.json").read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(report["ai_reviews_completed"], 2)
            self.assertEqual(report["ai_calls"], 2)
            self.assertEqual(report["sweeps"], 14)
            self.assertEqual(report["extension_sweeps"], 1)
            for name, account in report["accounts"].items():
                self.assertEqual(account["fills"], 2 if name.endswith("_fixed") else 1)
                self.assertIn("005930", account["positions"])
            market = (old_run / "market.jsonl").read_bytes() + feed.read_bytes()
            self.assertEqual((combined / "market.jsonl").read_bytes(), market)
            self.assertEqual(report["shared_input_sha256"], hashlib.sha256(market).hexdigest())

            stopped = json.loads((combined / "status.json").read_text())
            stopped.update(state="incomplete", complete=False)
            (combined / "status.json").write_text(encode(stopped))
            (feed.parent / "status.json").write_text(
                encode({"state": "complete", "counts": {"standalone": 2}})
            )
            clock = cutoff + timedelta(seconds=10)
            with feed.open("a") as history:
                history.write(encode({"observed_at": timestamp(), "rows": snapshot()}) + "\n")
            listings_path = Path(temp) / "listings.json"
            listings_path.write_text(
                encode(
                    {
                        "KOSPI": [
                            {"symbol": symbol, "securityType": "STOCK"}
                            for symbol in ("005930", "000660")
                        ],
                        "KOSDAQ": [],
                        "KR_ETC": [],
                    }
                )
            )
            integer = Path(temp) / "integer"
            with (
                patch(f"{extension}.now", side_effect=timestamp),
                patch(f"{extension}.asyncio.sleep", side_effect=advance),
                patch(
                    f"{extension}.poll_job",
                    side_effect=lambda path: {"returncode": -2 if path == job else 0},
                ),
                patch(f"{extension}.review_cascade") as review,
            ):
                await continue_campaign(
                    job,
                    feed,
                    integer,
                    until,
                    whole_share_source=combined,
                    whole_share_listings=listings_path,
                )
                review.assert_not_called()
            integer_result = json.loads((integer / "comparison.json").read_text())
            self.assertTrue(integer_result["complete"])
            self.assertEqual(integer_result["sweeps"], 15)
            self.assertEqual(integer_result["whole_share_seed_fills"]["5pct_fixed"], 2)
            for account in integer_result["accounts"].values():
                for position in account["positions"].values():
                    quantity = Decimal(position["quantity"])
                    self.assertEqual(quantity, quantity.to_integral_value())

            status = old_run / "status.json"
            feed.write_bytes(feed.read_bytes().splitlines(keepends=True)[0])
            (feed.parent / "status.json").write_text(
                encode({"state": "complete", "counts": {"standalone": 1}})
            )
            failed = json.loads(status.read_text())
            failed.update(state="failed", complete=False, error_type="SafetyError")
            status.write_text(encode(failed))
            (job / "stderr.log").write_text(
                "papertrade.core.SafetyError: A valid existing cached token is required\n"
            )
            recovered = Path(temp) / "recovered"
            clock = cutoff + timedelta(seconds=1)
            with (
                patch(f"{extension}.now", side_effect=timestamp),
                patch(f"{extension}.asyncio.sleep", side_effect=advance),
                patch(
                    f"{extension}.poll_job",
                    side_effect=lambda path: {"returncode": 1 if path == job else 0},
                ),
                patch(f"{extension}.review_cascade"),
            ):
                with self.assertRaisesRegex(ValueError, "verified completed comparison"):
                    await continue_campaign(job, feed, recovered, until)
                await continue_campaign(job, feed, recovered, until, recover_expired_cache=True)
            recovery = json.loads((recovered / "comparison.json").read_text())
            self.assertTrue(recovery["recovered_expired_cache"])
            self.assertEqual(recovery["accounts"], report["accounts"])
            self.assertEqual(recovery["shared_input_sha256"], report["shared_input_sha256"])
            self.assertLess(
                recovery["previous_last_observation"], recovery["first_extension_observation"]
            )

            tampered = json.loads(status.read_text())
            tampered["accounts"]["5pct_ai"]["cash_krw"] = "1"
            status.write_text(encode(tampered))
            with self.assertRaisesRegex(ValueError, "Historical replay differs"):
                Replay(old_run)
