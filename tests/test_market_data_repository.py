"""Offline checks for indexed and causally bounded market-data history."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from tools.market_data_repository import (
    HISTORY_QUERY,
    LATEST_QUERY,
    DataCut,
    DataRequest,
    MarketDataRepository,
    Observation,
    epoch,
)

EARLY = "2026-11-10T15:00:00Z"
LATE = "2026-11-10T15:00:10+00:00"
NOW = "2026-11-10T15:00:12+00:00"
PRICE = DataRequest("US", "price", ("A",), max_age=60)


class RepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "data.sqlite3"
        self.repo = MarketDataRepository(self.path)
        self.addCleanup(self.repo.close)

    def test_preserves_source_stamps_and_immutable_payloads_at_prior_cut(self) -> None:
        payload = {"timestamp": EARLY, "lastPrice": 1, "nested": {"size": [2]}}
        older = self.repo.publish(
            PRICE.group, [Observation("A", EARLY, LATE, "recorded", payload)]
        )["A"]
        cut = self.repo.cut(LATE)
        payload["nested"] = {}
        older.payload["nested"]["size"].append(3)
        self.repo.publish(
            PRICE.group,
            [Observation("A", LATE, LATE, "recorded", {"lastPrice": 2})],
        )
        latest = self.repo.lookup(PRICE, now=NOW)["A"]
        historical = self.repo.lookup(
            DataRequest("US", "price", ("A",), max_age=60, as_of_cut=cut), now=NOW
        )["A"]
        self.assertEqual(historical, older)
        self.assertEqual(historical.payload["nested"], {"size": [2]})
        self.assertEqual(historical.source_time, EARLY)
        self.assertEqual(historical.received_time, LATE)
        self.assertGreater(latest.published_sequence, cut.sequence)

    def test_delayed_response_cannot_replace_newer_source(self) -> None:
        group = DataRequest("US", "book", ("A",)).group
        second = MarketDataRepository(self.path)
        self.addCleanup(second.close)
        newest = self.repo.publish(group, [Observation("A", LATE, LATE, "ws", {})])["A"]
        delayed = second.publish(group, [Observation("A", EARLY, NOW, "rest", {})])["A"]
        self.assertGreater(delayed.published_sequence, newest.published_sequence)
        query = DataRequest("US", "book", ("A",), max_age=60)
        self.assertEqual(second.lookup(query, now=NOW)["A"], newest)
        cut = self.repo.cut(NOW)
        query = DataRequest("US", "book", ("A",), max_age=60, as_of_cut=cut)
        self.assertEqual(self.repo.lookup(query, now=NOW)["A"], newest)

    def test_rejects_nonfinite_non_json_and_future_source_before_transaction(self) -> None:
        for malformed in ({"x": float("nan")}, {"x": {"y": (1, 2)}}):
            with self.subTest(malformed=malformed), self.assertRaises(ValueError):
                self.repo.publish(
                    PRICE.group,
                    [
                        Observation("A", EARLY, LATE, "rest", {}),
                        Observation("B", EARLY, LATE, "rest", malformed),
                    ],
                )
        with self.assertRaises(ValueError):
            self.repo.publish(PRICE.group, [Observation("A", LATE, EARLY, "rest", {})])
        self.assertEqual(self.repo.cut(NOW).sequence, 0)
        bad_cut = DataRequest("US", "price", ("A",), max_age=10, as_of_cut=DataCut(1, NOW))
        with self.assertRaisesRegex(ValueError, "outside the repository"):
            self.repo.lookup(bad_cut, now=NOW)

    def test_latest_and_history_use_batched_indexed_reads(self) -> None:
        args = (json.dumps(["A", "B"]), "US", "price", "[]", epoch(EARLY), epoch(NOW), epoch(EARLY))
        with closing(sqlite3.connect(self.path)) as db:
            latest = list(db.execute("EXPLAIN QUERY PLAN " + LATEST_QUERY, args))
            history = list(db.execute("EXPLAIN QUERY PLAN " + HISTORY_QUERY, (*args, 5)))
        self.assertTrue(any("SEARCH latest USING INDEX" in row[3] for row in latest), latest)
        self.assertTrue(any("SEARCH h USING INDEX market_data_history_key" in row[3]
                            for row in history), history)
