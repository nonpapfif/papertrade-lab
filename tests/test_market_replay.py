"""Causal replay, observed outcome labels, event edges, and native isolation."""

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

from tools.replay_market_day import (
    History,
    Worker,
    algorithm_predictions,
    event_conditions,
    replay,
    worker_loop,
)

START = datetime(2026, 10, 30, 0, 0, tzinfo=UTC)


class ReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def dataset(self, name: str = "source.sqlite3", jump: bool = False,
                missing: int | None = None) -> Path:
        path = self.root / name
        db = sqlite3.connect(path)
        db.executescript("""
            CREATE TABLE metadata (key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE symbols (market TEXT,symbol TEXT,status TEXT);
            CREATE TABLE candles (
                market TEXT,symbol TEXT,at TEXT,close TEXT,high TEXT,low TEXT,volume TEXT
            );
        """)
        scopes = {market: {"start": START.isoformat(),
                           "end": (START + timedelta(minutes=30)).isoformat()}
                  for market in ("KR", "US")}
        for key, value in {"state": "complete", "date": "2026-10-30", "scopes": scopes}.items():
            db.execute("INSERT INTO metadata VALUES (?,?)", (key, json.dumps(value)))
        db.execute("INSERT INTO symbols VALUES ('US','TEST','done')")
        for minute in range(1, 31):
            if minute == missing:
                continue
            price = 100 + minute + (1000 if jump and minute >= 20 else 0)
            db.execute("INSERT INTO candles VALUES (?,?,?,?,?,?,?)", (
                "US", "TEST", (START + timedelta(minutes=minute)).isoformat(),
                str(price), str(price + 1), str(price - 1), "10",
            ))
        db.commit()
        db.close()
        return path

    def run_replay(self, dataset: Path, name: str) -> tuple[dict[str, Any], sqlite3.Connection]:
        output = self.root / name
        with redirect_stdout(io.StringIO()):
            report = replay(dataset, output)
        db = sqlite3.connect(output / "replay.sqlite3")
        db.row_factory = sqlite3.Row
        self.addCleanup(db.close)
        return report, db

    def feature(self) -> dict[str, Any]:
        return {"market": "US", "symbol": "TEST", "price": 110.0,
                "return_5m": 0.02, "return_15m": 0.03, "relative_volume": 3.0,
                "prior_minute_noise": 0.001, "prior_high": 105, "prior_low": 95,
                "prior_price_zscore": 4}

    def test_complete_replay_has_one_prediction_per_five_minute_step(self) -> None:
        report, db = self.run_replay(self.dataset(), "run")
        self.assertEqual(report["ticks"], 5)
        self.assertEqual(report["forecast_cases"], 5)
        self.assertEqual(report["outcomes"], {"observed": 5})
        self.assertEqual(report["ai_calls_executed"], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM ticks "
                                   "WHERE evaluated_at IS NULL").fetchone()[0], 0)
        for row in db.execute("SELECT c.*,t.at FROM cases c JOIN ticks t ON t.id=c.tick_id"):
            self.assertLessEqual(row["quote_at"], row["at"])
            self.assertGreater(row["target_quote_at"], row["at"])

    def test_changing_future_cannot_change_already_committed_predictions(self) -> None:
        _, before = self.run_replay(self.dataset("original.sqlite3"), "before")
        _, changed = self.run_replay(self.dataset("changed.sqlite3", jump=True), "changed")
        query = ("SELECT c.features,c.predictions,c.events FROM cases c "
                 "JOIN ticks t ON c.tick_id=t.id WHERE t.at<=?")
        cutoff = (START + timedelta(minutes=15)).isoformat()
        self.assertEqual([tuple(r) for r in before.execute(query, (cutoff,))],
                         [tuple(r) for r in changed.execute(query, (cutoff,))])
        self.assertNotEqual(before.execute("SELECT actual_return FROM cases WHERE tick_id=3")
                            .fetchone()[0], changed.execute(
                                "SELECT actual_return FROM cases WHERE tick_id=3"
                            ).fetchone()[0])

    def test_missing_endpoint_is_not_scored_as_zero_return(self) -> None:
        report, db = self.run_replay(self.dataset(missing=15), "missing")
        self.assertEqual(report["outcomes"]["missing_exact_endpoint"], 2)
        self.assertIsNone(db.execute("SELECT actual_return FROM cases WHERE tick_id=2")
                          .fetchone()[0])
        self.assertEqual(db.execute("SELECT outcome FROM cases WHERE tick_id=2")
                         .fetchone()[0], "missing_exact_endpoint")

    def test_predictor_cannot_read_source_results_or_network(self) -> None:
        dataset = self.dataset()
        output = self.root / "private"
        output.mkdir()
        marker = output / "private.txt"
        marker.write_text("private future results")
        worker = Worker(dataset, output)
        try:
            result = worker.exchange({"kind": "probe", "paths": [str(dataset), str(marker)]})
            self.assertTrue(result)
            self.assertTrue(all(result.values()))
        finally:
            worker.close()

    def test_source_dataset_is_unchanged_after_replay(self) -> None:
        source = self.dataset()
        original = source.read_bytes()
        self.run_replay(source, "unchanged")
        self.assertEqual(source.read_bytes(), original)

    def test_replay_requires_a_new_output_directory(self) -> None:
        source = self.dataset()
        with self.assertRaises(FileExistsError):
            replay(source, self.root)

    def test_momentum_and_reversion_are_opposite_fixed_algorithms(self) -> None:
        values = algorithm_predictions(self.feature())
        self.assertEqual(values["momentum_5m"], 0.02)
        self.assertEqual(values["reversion_5m"], -0.02)

    def test_insufficient_history_is_not_invented(self) -> None:
        row = self.feature()
        row.update(return_5m=None, return_15m=None)
        values = algorithm_predictions(row)
        self.assertIsNone(values["momentum_5m"])
        self.assertIsNone(values["trend_15m"])
        self.assertEqual(event_conditions(row), [])

    def test_identical_active_event_does_not_call_ai_every_step(self) -> None:
        row = self.feature()
        source = io.StringIO("\n".join(json.dumps({"kind": "predict", "at":
            (START + timedelta(minutes=minute)).isoformat(), "rows": [row]})
            for minute in (5, 10)))
        target = io.StringIO()
        worker_loop(source, target)
        responses = [json.loads(line) for line in target.getvalue().splitlines()]
        self.assertTrue(responses[0][0]["events"])
        self.assertEqual(responses[1][0]["events"], [])

    def test_new_direction_rearms_an_event(self) -> None:
        up = self.feature()
        down = {**up, "return_5m": -0.02, "return_15m": -0.03,
                "price": 90, "prior_price_zscore": -4}
        source = io.StringIO("\n".join(json.dumps({"kind": "predict", "at":
            (START + timedelta(minutes=minute)).isoformat(), "rows": [row]})
            for minute, row in [(5, up), (10, down)]))
        target = io.StringIO()
        worker_loop(source, target)
        responses = [json.loads(line) for line in target.getvalue().splitlines()]
        self.assertTrue(responses[0][0]["events"])
        self.assertTrue(responses[1][0]["events"])

    def test_feature_frame_never_uses_future_rows(self) -> None:
        history = History()
        for minute in range(1, 6):
            history.add({"market": "US", "symbol": "TEST", "time": START
                         + timedelta(minutes=minute), "close": 100 + minute,
                         "high": 101 + minute, "low": 99 + minute, "volume": 10})
        scope = {"US": {"end": (START + timedelta(minutes=30)).isoformat()}}
        frame = history.features(START + timedelta(minutes=5), scope)
        self.assertEqual(frame[0]["price"], 105)
        self.assertEqual(frame[0]["minute_observations"], 5)
        self.assertEqual(frame[0]["price_age_seconds"], 0)
        self.assertIsNone(frame[0]["return_5m"])

    def test_feature_frame_rejects_future_data_in_its_history(self) -> None:
        history = History()
        history.add({"market": "US", "symbol": "TEST", "time": START
                     + timedelta(minutes=10), "close": 110, "high": 111, "low": 109,
                     "volume": 10})
        scope = {"US": {"end": (START + timedelta(minutes=30)).isoformat()}}
        with self.assertRaisesRegex(ValueError, "future observations"):
            history.features(START + timedelta(minutes=5), scope)


if __name__ == "__main__":
    unittest.main()
