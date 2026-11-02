"""Live protocol, shared collection, and atomic checkpoint integration."""

from __future__ import annotations

import csv
import gzip
import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from tools.run_live_parallel import Journal, advance_frame, live_design, make_frame, run
from tools.search_portfolios import configurations, signals


class LiveRunnerTests(unittest.TestCase):
    def test_regular_start_collects_warmup_without_trades_and_uses_it_at_open(self) -> None:
        start = datetime(2026, 11, 2, 15, tzinfo=UTC)
        opening = start + timedelta(minutes=5)
        clock = [start.timestamp()]

        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz: object = None) -> datetime:
                return datetime.fromtimestamp(clock[0], UTC)

        def sleep(seconds: float) -> None:
            clock[0] += seconds

        feed = Mock()
        feed.universe.return_value = ["X"]
        bounds = [
            start - timedelta(hours=2),
            start - timedelta(hours=1),
            opening,
            start + timedelta(minutes=6),
            start + timedelta(minutes=7),
        ]
        feed.calendar.return_value = {
            "today": {
                "date": "2026-11-02",
                **{
                    key: {
                        "startTime": bounds[i].isoformat(),
                        "endTime": bounds[i + 1].isoformat(),
                    }
                    for i, key in enumerate(
                        ("dayMarket", "preMarket", "regularMarket", "afterMarket")
                    )
                },
            }
        }
        feed.fx.return_value = {"midRate": "1300"}
        specs = signals()
        config = {**configurations(len(specs))[0], "hold": 0}
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)

            def snapshot() -> list[dict]:
                if clock[0] == opening.timestamp():
                    status = json.loads((output / "status.json").read_text())
                    self.assertEqual(status["state"], "warming_up")
                    journal = Journal(output)
                    state = journal.load()
                    journal.db.close()
                    self.assertEqual(state["evaluations"], 0)
                    self.assertEqual(state["snapshots"], 5)
                    for engine in state["engines"].values():
                        self.assertEqual(engine.day_steps, 0)
                        self.assertFalse(engine.shadows[0].positions)
                        self.assertEqual(engine.states[0]["book"].cash, 100)
                        self.assertFalse(engine.states[0]["book"].trades)
                    feed.orderbooks.assert_not_called()
                    feed.fx.assert_not_called()
                return [
                    {
                        "symbol": "X",
                        "lastPrice": str(10 + (clock[0] - start.timestamp()) / 600),
                        "currency": "USD",
                        "timestamp": FrozenDateTime.now().isoformat(),
                    }
                ]

            def orderbooks(symbols: list[str]) -> dict:
                self.assertGreaterEqual(clock[0], opening.timestamp())
                return {
                    "X": {
                        "timestamp": FrozenDateTime.now().isoformat(),
                        "currency": "USD",
                        "asks": [{"price": 10.6, "volume": 100}],
                        "bids": [{"price": 10.4, "volume": 100}],
                    }
                }

            feed.snapshot.side_effect = snapshot
            feed.orderbooks.side_effect = orderbooks
            with (
                patch("tools.run_live_parallel.LiveFeed", return_value=feed),
                patch("tools.run_live_parallel.datetime", FrozenDateTime),
                patch("tools.run_live_parallel.time.time", side_effect=lambda: clock[0]),
                patch("tools.run_live_parallel.time.sleep", side_effect=sleep),
                patch(
                    "tools.run_live_parallel.live_design",
                    return_value=([config], specs, [{"id": "learn", "mode": "daily"}]),
                ),
            ):
                run(output, "2026-11-02", 100, 0, False, trade_from_regular=True)
            journal = Journal(output)
            frames = journal.db.execute("SELECT at,payload_gzip FROM frames").fetchall()
            self.assertEqual(len(frames), 1)
            self.assertGreaterEqual(datetime.fromisoformat(frames[0][0]), opening)
            first_frame = json.loads(gzip.decompress(frames[0][1]))
            self.assertIsNotNone(first_frame["long_features"][0][1][0])
            final = journal.load()
            self.assertTrue(all(e.states[0]["book"].opened == 1 for e in final["engines"].values()))
            journal.db.close()
            with self.assertRaisesRegex(ValueError, "Resume trading start"):
                run(output, "2026-11-02", 100, 0, True)

    def test_delayed_execution_expires_prices_without_relabeling_their_observation(self) -> None:
        at = "2026-11-02T15:00:00+00:00"
        frame = {
            "quotes": {0: 10, 1: 20},
            "source_times": {0: at, 1: "2026-11-02T14:59:00+00:00"},
            "price_observed_at": at,
            "marks": {0: [10, at], 1: [20, at]},
            "ranks": [[[], [[0, 1, 0.2], [1, 1, 0.1]]]],
            "long_features": [[0, [], [], []], [1, [], [], []]],
        }
        advance_frame(frame, "2026-11-02T15:00:40+00:00")
        self.assertEqual(frame["quotes"], {0: 10})
        self.assertEqual(frame["marks"][1], [20, at])
        self.assertEqual(frame["ranks"][0][1], [[0, 1, 0.2]])
        advance_frame(frame, "2026-11-02T15:02:00+00:00")
        self.assertFalse(frame["quotes"])
        self.assertEqual(frame["marks"][0], [10, at])
        self.assertEqual(frame["expired_price_symbols"], [0, 1])

    def test_design_excludes_unavailable_volume_inputs_without_a_stock_cap(self) -> None:
        configs, specs, policies = live_design()
        self.assertEqual(len(policies), 59)
        self.assertTrue(configs)
        self.assertTrue(
            all(
                c["leverage"] == 1
                and specs[c["signal"]]["side"] == "long"
                and specs[c["signal"]]["volume"] == 0
                for c in configs
            )
        )
        self.assertFalse(any(p.get("families") == ["volume_trend"] for p in policies))

    def test_checkpoint_and_input_commit_roll_back_together_on_duplicate_slot(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            journal = Journal(Path(folder))
            frame = {"at": "2026-11-02T15:00:00+00:00", "signal_at": "2026-11-02T15:00:00+00:00"}
            journal.commit({"last_slot": 1, "counter": 1}, frame)
            with self.assertRaises(sqlite3.IntegrityError):
                journal.commit({"last_slot": 1, "counter": 2}, frame)
            self.assertEqual(journal.load(), {"last_slot": 1, "counter": 1})
            self.assertEqual(journal.db.execute("SELECT COUNT(*) FROM frames").fetchone()[0], 1)
            journal.db.close()

    def test_live_frame_keeps_rank_rotation_inputs_and_source_provenance(self) -> None:
        class History:
            def features(self, at: str) -> list[dict]:
                return [
                    {
                        "market": "US",
                        "symbol": "X",
                        "price": 10,
                        "quote_at": at,
                        "source_at": at,
                        "scores": [0.1] * 30,
                        "volumes": [None] * 5,
                        "noises": [3] * 5,
                    }
                ]

        frame = make_frame(History(), "2026-11-02T15:00:00+00:00", {"X": 0})
        self.assertTrue(frame["ranks"][2700][1])
        self.assertEqual(frame["source_times"][0], frame["signal_at"])
        self.assertIn("not_exchange_OHLCV", frame["feature_basis"])

    def test_full_live_runner_checkpoints_quote_only_accounts(self) -> None:
        start = datetime(2026, 11, 2, 15, tzinfo=UTC)

        class Clock:
            value = start.timestamp()

            @classmethod
            def time(cls) -> float:
                return cls.value

            @classmethod
            def sleep(cls, seconds: float) -> None:
                cls.value += seconds

        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz: object = None) -> datetime:
                return datetime.fromtimestamp(Clock.value, tz=UTC)

        class Feed:
            instance = None

            def __init__(self, settings: object, record: object) -> None:
                self.snapshots = 0
                self.symbols = ["X"]
                Feed.instance = self

            def universe(self) -> list[str]:
                return self.symbols

            def calendar(self, day: str) -> dict:
                times = [
                    start - timedelta(hours=2),
                    start - timedelta(hours=1),
                    start - timedelta(minutes=10),
                    start + timedelta(minutes=10),
                    start + timedelta(minutes=16),
                ]
                return {
                    "today": {
                        "date": day,
                        **{
                            key: {
                                "startTime": times[i].isoformat(),
                                "endTime": times[i + 1].isoformat(),
                            }
                            for i, key in enumerate(
                                ("dayMarket", "preMarket", "regularMarket", "afterMarket")
                            )
                        },
                    }
                }

            def snapshot(self) -> list[dict]:
                self.snapshots += 1
                price = 10 - (self.snapshots % 7) / 10
                return [
                    {
                        "symbol": "X",
                        "lastPrice": str(price),
                        "currency": "USD",
                        "timestamp": datetime.fromtimestamp(Clock.value, UTC).isoformat(),
                    }
                ]

            def fx(self) -> dict:
                return {"midRate": "1300"}

            def orderbooks(self, symbols: list[str]) -> dict:
                price = 10 - (self.snapshots % 7) / 10
                if not getattr(self, "delayed", False):
                    Clock.value += 310
                    self.delayed = True
                return {
                    "X": {
                        "timestamp": datetime.fromtimestamp(Clock.value, UTC).isoformat(),
                        "currency": "USD",
                        "asks": [{"price": price + 0.01, "volume": 100}],
                        "bids": [{"price": price - 0.01, "volume": 100}],
                    }
                }

        specs = signals()
        config = {**configurations(len(specs))[32400]}
        policies = [{"id": "fixed", "mode": "static"}, {"id": "daily", "mode": "daily"}]
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            with (
                patch("tools.run_live_parallel.LiveFeed", Feed),
                patch("tools.run_live_parallel.datetime", FrozenDateTime),
                patch("tools.run_live_parallel.time.time", Clock.time),
                patch("tools.run_live_parallel.time.sleep", Clock.sleep),
                patch(
                    "tools.run_live_parallel.live_design", return_value=([config], specs, policies)
                ),
            ):
                run(output, "2026-11-02", 100, 0, False)
            status = json.loads((output / "status.json").read_text())
            self.assertEqual(status["state"], "complete")
            self.assertEqual(status["accounts"], 2)
            self.assertEqual(status["ai_calls"], 0)
            self.assertGreater(status["evaluations"], 1)
            self.assertLess(Feed.instance.snapshots, 16)
            with (output / "comparison.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 2)
            self.assertEqual({r["mode"] for r in rows}, {"quotes"})
            self.assertFalse((output / "last-report.json.gz").exists())
            self.assertTrue(all(float(r["cash"]) >= 0 for r in rows))
            journal = Journal(output)
            state = journal.load()
            self.assertTrue(state["complete"])
            self.assertEqual(state["last_slot"], state["engines"]["quotes"].last_tick)
            delayed = journal.db.execute(
                "SELECT at,signal_at FROM frames WHERE at > signal_at"
            ).fetchall()
            self.assertTrue(
                any(
                    datetime.fromisoformat(at).timestamp() // 300
                    > datetime.fromisoformat(signal_at).timestamp() // 300
                    for at, signal_at in delayed
                )
            )
            self.assertEqual(
                journal.db.execute("SELECT COUNT(*) FROM frames").fetchone()[0],
                status["evaluations"],
            )
            journal.db.close()


if __name__ == "__main__":
    unittest.main()
