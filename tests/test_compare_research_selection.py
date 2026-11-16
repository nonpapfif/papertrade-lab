"""Exercise research selection with fixed, independent replay accounts."""

import gzip
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from tools.compare_research_selection import compare, load_schedule

_START = datetime(2026, 11, 3, 14, 30, tzinfo=UTC)
_IDS = {"F": 0, "SOFI": 1}


def stamp(offset: float) -> str:
    return (_START + timedelta(seconds=offset)).isoformat()


def write(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def bundle(root: Path, name: str, symbol: str, rating: str, cutoff: float) -> str:
    path = root / (name * 64)
    path.mkdir()
    write(path / "manifest.json", {
        "run_id": path.name,
        "rating": rating,
        "status": "completed",
        "executed_nodes": ["Portfolio Manager"],
        "reports": {"portfolio-manager": "completed"},
    })
    write(path / "evidence.json", {
        "snapshot": {"market": "US", "symbol": symbol, "as_of": stamp(cutoff)},
    })
    return path.name


class ResearchSelectionTests(unittest.IsolatedAsyncioTestCase):
    def test_snapshots_expire_and_never_resurrect_older_ratings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            a = bundle(root, "a", "F", "Buy", 0)
            b = bundle(root, "b", "SOFI", "Overweight", 1)
            c = bundle(root, "c", "F", "Hold", 2)
            schedule_path = root / "selection.json"
            write(schedule_path, {"snapshots": [
                {"published_at": stamp(3), "expires_at": stamp(10), "bundles": [a, b]},
                {"published_at": stamp(5), "expires_at": stamp(7), "bundles": [c]},
            ]})
            schedule = load_schedule(schedule_path, _IDS)
            self.assertEqual(schedule.at(_START.timestamp() + 2), set())
            self.assertEqual(schedule.at(_START.timestamp() + 3), {0, 1})
            self.assertEqual(schedule.at(_START.timestamp() + 6), set())
            self.assertEqual(schedule.at(_START.timestamp() + 7), set())
            write(schedule_path, {"snapshots": [
                {"published_at": stamp(0), "expires_at": stamp(7), "bundles": [b]},
            ]})
            with self.assertRaisesRegex(ValueError, "future evidence"):
                load_schedule(schedule_path, _IDS)

    async def test_same_capture_can_compare_selection_and_then_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            freeze = root / "freeze.json"
            write(freeze, {
                "symbols": list(_IDS), "frozen_at": stamp(960),
                "stock_info": [
                    {
                        "symbol": symbol, "status": "ACTIVE", "currency": "USD",
                        "securityType": "STOCK", "isCommonShare": True,
                    }
                    for symbol in _IDS
                ],
            })
            fx = root / "fx-history.json"
            write(fx, {"observations": [
                {"received_at": stamp(960), "result": {"midRate": "1350"}},
            ]})
            candles = [
                {"timestamp": stamp(i * 60), "closePrice": str(10 + i / 100),
                 "highPrice": str(10 + i / 100)}
                for i in range(16)
            ]
            with gzip.open(root / "rest-000.json.gz", "wt") as stream:
                json.dump({"candles": {
                    symbol: {"received_at": stamp(960), "result": {"candles": candles}}
                    for symbol in _IDS
                }}, stream)
            with gzip.open(root / "core-frames.jsonl.gz", "wt") as stream:
                for elapsed in (961, 963, 965, 967):
                    for symbol in _IDS:
                        stream.write(json.dumps({
                            "received_at": stamp(elapsed),
                            "frame": {"topic": f"orderbook:us:{symbol}", "data": {
                                "timestamp": stamp(elapsed), "currency": "USD",
                                "asks": [{"price": "10.5", "volume": "100"}],
                                "bids": [{"price": "10.49", "volume": "100"}],
                            }},
                        }) + "\n")
            chosen = bundle(root, "a", "SOFI", "Overweight", 960)
            rejected = bundle(root, "b", "F", "Hold", 960)
            selection = root / "selection.json"
            write(selection, {"snapshots": [
                {"published_at": stamp(963), "expires_at": stamp(964),
                 "bundles": [chosen, rejected]},
            ]})
            result = await compare(
                root, freeze, fx, selection, capital=1000,
                strategy="trend", compare_strategy="rebound",
            )
            self.assertEqual(result["common_book_count"], 8)
            control, candidate, alternate = result["accounts"].values()
            self.assertEqual(control["strategy"], candidate["strategy"])
            self.assertEqual(control["fee_schedule"], candidate["fee_schedule"])
            control_intents = control["summary"]["intents"]
            candidate_intents = candidate["summary"]["intents"]
            self.assertEqual({intent["symbol"] for intent in control_intents}, {0})
            self.assertEqual({intent["symbol"] for intent in candidate_intents}, {1})
            self.assertEqual(candidate_intents[0]["decision_at"], stamp(963))
            self.assertGreater(candidate["summary"]["account"]["entries"], 0)
            self.assertEqual(alternate["summary"]["account"]["entries"], 0)
