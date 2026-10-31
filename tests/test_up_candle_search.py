"""Rule-grid coverage, confusion counts, chronology, and resumable extraction."""

from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools.replay_market_day import GRID_BITS, GRID_COMMON, grid_mask, hash_file
from tools.search_up_candles import (
    SCHEMA,
    Bitmaps,
    GridHistory,
    configurations,
    extract,
    get_meta,
    metrics,
    phase_at,
    run,
    selection,
    set_meta,
)

START = datetime(2026, 10, 30, tzinfo=UTC)


class SearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def dataset(self, name: str = "source.sqlite3", jump: bool = False) -> Path:
        path = self.root / name
        db = sqlite3.connect(path)
        db.executescript("""
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE symbols(market TEXT,symbol TEXT,status TEXT);
            CREATE TABLE candles(market TEXT,symbol TEXT,at TEXT,close TEXT,
                                 high TEXT,low TEXT,volume TEXT);
        """)
        scopes = {market: {"start": START.isoformat(),
                           "end": (START + timedelta(minutes=180)).isoformat(),
                           "sessions": [{"start": START.isoformat(),
                                         "end": (START + timedelta(minutes=180)).isoformat()}]}
                  for market in ("KR", "US")}
        for key, value in {"state": "complete", "date": "2026-10-30", "scopes": scopes}.items():
            db.execute("INSERT INTO metadata VALUES (?,?)", (key, json.dumps(value)))
        db.execute("INSERT INTO symbols VALUES ('US','TEST','done')")
        for minute in range(1, 181):
            price = 100 + minute * 0.1 + (1000 if jump and minute >= 150 else 0)
            db.execute("INSERT INTO candles VALUES (?,?,?,?,?,?,?)", (
                "US", "TEST", (START + timedelta(minutes=minute)).isoformat(),
                str(price), str(price + 0.1), str(price - 0.1), "100",
            ))
        db.commit()
        db.close()
        return path

    def cache(self, dataset: Path, folder: Path) -> sqlite3.Connection:
        folder.mkdir()
        source = sqlite3.connect(dataset)
        scopes = json.loads(source.execute("SELECT value FROM metadata WHERE key='scopes'")
                            .fetchone()[0])
        source.close()
        db = sqlite3.connect(folder / "search.sqlite3")
        db.row_factory = sqlite3.Row
        db.executescript(SCHEMA)
        self.addCleanup(db.close)
        with db:
            set_meta(db, "manifest", {"scopes": scopes, "dataset_sha256": hash_file(dataset)})
        return db

    def test_grid_has_3000_unique_variants_and_two_baselines(self) -> None:
        configs = configurations()
        self.assertEqual(len(configs), 3002)
        self.assertEqual(len({c["id"] for c in configs}), 3002)
        self.assertEqual({c["lookback"] for c in configs if c["lookback"]}, {5, 10, 15, 30, 60})

    def test_bit_predicates_agree_with_scalar_rules_for_every_configuration(self) -> None:
        basis = {"scores": [0.004] * 30, "volumes": [2.0] * 5, "noises": [1.5] * 5}
        mask = grid_mask(basis)
        self.assertLess(mask.bit_length(), GRID_BITS + 1)
        self.assertTrue(mask & (1 << GRID_COMMON))
        for config in configurations()[:-2]:
            actual = all(mask & (1 << bit) for bit in config["prediction_bits"])
            expected = (0.004 >= config["move"] and 2 >= config["volume"]
                        and 1.5 >= config["noise"])
            self.assertEqual(actual, expected, config["id"])

    def test_missing_features_never_invent_an_up_prediction(self) -> None:
        mask = grid_mask({"scores": [None] * 30, "volumes": [None] * 5, "noises": [None] * 5})
        for config in configurations()[:-2]:
            self.assertFalse(all(mask & (1 << bit) for bit in config["prediction_bits"]))
        self.assertFalse(mask & (1 << GRID_COMMON))

    def test_f1_uses_positive_class_not_total_accuracy(self) -> None:
        tp, fp, fn, tn, precision, recall, f1 = metrics(20, 25, 100, 1000)
        self.assertEqual((tp, fp, fn, tn), (20, 5, 80, 895))
        self.assertEqual((precision, recall, f1), (0.8, 0.2, 0.32))
        self.assertEqual(metrics(0, 0, 100, 1000)[-1], 0)

    def test_market_clock_splits_do_not_overlap_target_windows(self) -> None:
        scope = {"start": START.isoformat(),
                 "end": (START + timedelta(minutes=100)).isoformat()}
        self.assertEqual(phase_at(START + timedelta(minutes=55), scope), "train")
        self.assertEqual(phase_at(START + timedelta(minutes=60), scope), "validation")
        self.assertEqual(phase_at(START + timedelta(minutes=80), scope), "test")

    def test_extraction_resume_preserves_predictions_and_case_count(self) -> None:
        dataset = self.dataset()
        folder = self.root / "cache"
        db = self.cache(dataset, folder)
        with redirect_stdout(io.StringIO()):
            extract(db, dataset, folder)
        hashes = [tuple(r) for r in db.execute("SELECT input_sha256,output_sha256 FROM frames")]
        count = db.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
        with db:
            set_meta(db, "feature_complete", False)
        with redirect_stdout(io.StringIO()):
            extract(db, dataset, folder)
        self.assertEqual(count, db.execute("SELECT COUNT(*) FROM cases").fetchone()[0])
        self.assertEqual(hashes, [tuple(r) for r in db.execute(
            "SELECT input_sha256,output_sha256 FROM frames")])
        self.assertEqual(db.execute("SELECT COUNT(*) FROM cases WHERE outcome IS NULL")
                         .fetchone()[0], 0)
        self.assertTrue(all(get_meta(db, "isolation").values()))

    def test_future_price_change_cannot_change_earlier_grid_predictions(self) -> None:
        original = self.dataset()
        changed = self.dataset("changed.sqlite3", jump=True)
        caches = []
        for index, dataset in enumerate([original, changed]):
            folder = self.root / str(index)
            db = self.cache(dataset, folder)
            with redirect_stdout(io.StringIO()):
                extract(db, dataset, folder)
            caches.append(db)
        query = "SELECT output_sha256 FROM frames WHERE at<=? ORDER BY id"
        cutoff = (START + timedelta(minutes=145)).isoformat()
        self.assertEqual([tuple(r) for r in caches[0].execute(query, (cutoff,))],
                         [tuple(r) for r in caches[1].execute(query, (cutoff,))])

    def test_test_scores_cannot_change_validation_winner(self) -> None:
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.row_factory = sqlite3.Row
        db.executescript(SCHEMA)
        for identifier, family in [("a", "momentum"), ("b", "reversal")]:
            db.execute("INSERT INTO configurations VALUES (?,?,?,?)",
                       (identifier, family, 5, json.dumps({"id": identifier})))
            for phase, f1 in [("train", 0.7), ("validation", 0.8 if identifier == "a" else 0.5),
                              ("test", 0.1 if identifier == "a" else 0.99)]:
                db.execute("INSERT INTO scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (identifier, "US", 0, phase, "common", 100, 50, 50, 100,
                            30, 20, 20, 30, 0.6, 0.6, f1))
        first = selection(db, "US", 0)
        db.execute("UPDATE scores SET f1=1 WHERE phase='test'")
        second = selection(db, "US", 0)
        self.assertEqual(first["configuration"]["id"], "a")
        self.assertEqual(second["configuration"]["id"], "a")

    def test_small_end_to_end_search_and_restart(self) -> None:
        dataset = self.dataset()
        folder = self.root / "full"
        configs = configurations()
        sample = [configs[0], configs[2400], *configs[-2:]]
        original = dataset.read_bytes()
        with (patch("tools.search_up_candles.configurations", return_value=sample),
              redirect_stdout(io.StringIO())):
            run(dataset, folder)
            run(dataset, folder)
        result = json.loads((folder / "report.json").read_text())
        self.assertEqual(result["configuration_count"], 4)
        self.assertEqual(result["forecast_cases"], 35)
        self.assertEqual(result["ai_calls"], 0)
        self.assertEqual(dataset.read_bytes(), original)
        db = sqlite3.connect(folder / "search.sqlite3")
        self.addCleanup(db.close)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM finished").fetchone()[0], 4)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM scores").fetchone()[0], 4 * 120)

    def test_bitmap_counts_match_direct_labels(self) -> None:
        maps = Bitmaps()
        for index, actual in enumerate([0.01, -0.01, 0, 0.005, None]):
            case: Any = {"id": index, "market": "US", "phase": "train",
                         "outcome": "observed" if actual is not None else "missing_endpoint",
                         "actual_return": actual}
            maps.add(1 << GRID_COMMON, case)
        maps.finish()
        self.assertEqual(maps.group_bits["observed"].bit_count(), 4)
        self.assertEqual(maps.target_bits[0].bit_count(), 2)
        self.assertEqual(maps.target_bits[4].bit_count(), 1)

    def test_future_history_is_rejected(self) -> None:
        history = GridHistory()
        future: Any = {"market": "US", "symbol": "TEST", "at":
                       (START + timedelta(minutes=10)).isoformat(),
                       "close": "100", "high": "101", "low": "99", "volume": "1"}
        history.add(future)
        with self.assertRaisesRegex(ValueError, "Future data"):
            history.features(START + timedelta(minutes=5), {"US": {
                "end": (START + timedelta(minutes=100)).isoformat()}})


if __name__ == "__main__":
    unittest.main()
