"""Run the full hypothesis replay on a tiny causally timestamped recording."""

import gzip
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tools.layered_paper import eligible_stock
from tools.replay_layered_stream import evaluate, variations


class ReplayTests(unittest.TestCase):
    def test_unverified_or_leveraged_instruments_are_ineligible(self) -> None:
        base = {
            "symbol": "F",
            "status": "ACTIVE",
            "currency": "USD",
            "securityType": "STOCK",
            "isCommonShare": True,
            "leverageFactor": None,
        }
        self.assertTrue(eligible_stock(base))
        for changes in (
            {"securityType": "ETF"},
            {"leverageFactor": 2},
            {"isCommonShare": False},
            {"status": "DELISTED"},
        ):
            self.assertFalse(eligible_stock({**base, **changes}))
        self.assertFalse(eligible_stock({}))

    def test_end_to_end_replay_keeps_all_hypotheses_and_no_borrowing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture, output = root / "capture", root / "output"
            capture.mkdir()
            output.mkdir()
            start = datetime(2026, 11, 3, 14, 30, tzinfo=UTC)

            def stamp(seconds: float) -> str:
                return (start + timedelta(seconds=seconds)).isoformat()

            rows = [
                {
                    "timestamp": stamp(i * 60),
                    "closePrice": str(10 + i / 100),
                    "highPrice": str(10 + i / 100),
                }
                for i in range(16)
            ]
            with gzip.open(capture / "rest-000.json.gz", "wt") as stream:
                json.dump(
                    {"candles": {"F": {"received_at": stamp(960), "result": {"candles": rows}}}},
                    stream,
                )
            with gzip.open(capture / "core-frames.jsonl.gz", "wt") as stream:
                for offset in (961, 962.1, 964, 965.1, 968):
                    stream.write(
                        json.dumps(
                            {
                                "received_at": stamp(offset),
                                "frame": {
                                    "topic": "orderbook:us:F",
                                    "data": {
                                        "timestamp": stamp(offset),
                                        "currency": "USD",
                                        "asks": [{"price": "10.5", "volume": "100"}],
                                        "bids": [{"price": "10.49", "volume": "100"}],
                                    },
                                },
                            }
                        )
                        + "\n"
                    )
            (output / "fx-history.json").write_text(
                json.dumps(
                    {"observations": [{"received_at": stamp(960), "result": {"midRate": "1350"}}]}
                )
            )
            freeze = {
                "symbols": ["F"],
                "frozen_at": stamp(960),
                "stock_info": [
                    {
                        "symbol": "F",
                        "status": "ACTIVE",
                        "currency": "USD",
                        "securityType": "STOCK",
                        "isCommonShare": True,
                    }
                ],
            }
            evaluate(capture, output, freeze)
            result = json.loads((output / "results.json").read_text())
            self.assertEqual(len(result["results"]), 4 * len(variations()))
            self.assertTrue(any(r["account"]["entries"] > 0 for r in result["results"]))
            for row in result["results"]:
                self.assertGreaterEqual(row["account"]["cash"], 0)
                self.assertEqual(row["account"]["debt"], 0)
