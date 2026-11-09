"""Capacity admission must fail on backlog, unequal input or uncovered holdings."""

import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from tools.adaptive_journal import AdaptiveJournal
from tools.paper_capacity import ArrivalBacklog, CapacityBenchmark, assess, strategies, workload

START = datetime(2026, 11, 6, 15, tzinfo=UTC)


def event(kind: str, payload: Any, at: datetime = START) -> dict[str, Any]:
    return {"kind": kind, "payload": payload, "at": at.isoformat(), "received": at.isoformat()}


def actionable_feed() -> list[dict[str, Any]]:
    day = START.date().isoformat()
    calendar = {
        "today": {
            "date": day,
            "regularMarket": {
                "startTime": (START - timedelta(minutes=30)).isoformat(),
                "endTime": (START + timedelta(hours=2)).isoformat(),
            },
        }
    }
    bars = [
        {
            "timestamp": (START - timedelta(minutes=15 - minute)).isoformat(),
            "closePrice": 9 + minute * 0.05,
            "highPrice": 9 + minute * 0.05,
        }
        for minute in range(16)
    ]
    return [
        event("calendar", [day, calendar]),
        event("selection", ["A"]),
        event("feature", ["A", bars]),
        *(
            event(
                "book",
                [
                    "A",
                    {
                        "timestamp": (at := START + timedelta(seconds=second)).isoformat(),
                        "currency": "USD",
                        "asks": [{"price": "10", "volume": "100"}],
                        "bids": [{"price": "9.99", "volume": "100"}],
                    },
                ],
                at,
            )
            for second in (1, 2, 3)
        ),
    ]


class CapacityTests(unittest.IsolatedAsyncioTestCase):
    def test_idle_time_does_not_hide_a_later_burst(self) -> None:
        backlog = ArrivalBacklog()
        for at in (0, 100, 100.01, 100.02):
            backlog.observe(at, 0.4)
        self.assertAlmostEqual(backlog.maximum, 1.18)

    def test_failed_measurement_prevents_extrapolation(self) -> None:
        sample: dict[str, Any] = {
            "slots": 1,
            "inputs_aligned": True,
            "subscriptions_covered": True,
            "max_backlog_seconds": 0.1,
            "processing_seconds": 0.5,
            "observed_seconds": 10,
            "accepted_books_per_account": 1,
            "valid_features_per_slot": 1,
        }
        for defect in (
            {"inputs_aligned": False},
            {"subscriptions_covered": False},
            {"max_backlog_seconds": 2},
            {"processing_seconds": 11},
            {"accepted_books_per_account": 0},
            {"valid_features_per_slot": 0},
        ):
            with self.subTest(defect=defect):
                self.assertEqual(
                    assess([sample, {**sample, "slots": 2, **defect}, {**sample, "slots": 3}], 1), 1
                )
        self.assertEqual(assess([{**sample, "slots": 2}], 1), 0)

    async def test_real_shared_runtime_receives_equal_paper_inputs(self) -> None:
        rows = actionable_feed()
        measured = await CapacityBenchmark("US", 1000, 1).measure(rows, 2)
        self.assertTrue(measured["inputs_aligned"])
        self.assertTrue(measured["subscriptions_covered"])
        self.assertEqual(measured["accounts"], 4)
        self.assertEqual(measured["events"], len(rows))
        self.assertEqual(measured["accepted_books_per_account"], 3)
        self.assertEqual(measured["valid_features_per_slot"], 1)
        self.assertEqual(measured["observed_seconds"], 2)
        self.assertGreater(measured["processing_seconds"], 0)

    async def test_no_valid_features_or_no_accepted_books_yields_no_capacity(self) -> None:
        benchmark = CapacityBenchmark("US", 1000, 1)
        rows = [row for row in actionable_feed() if row["kind"] != "feature"]
        measured = await benchmark.measure(rows, 1)
        self.assertEqual(measured["accepted_books_per_account"], 3)
        self.assertEqual(measured["valid_features_per_slot"], 0)
        self.assertEqual(assess([measured], 1), 0)

        rows = actionable_feed()
        for row in rows:
            if row["kind"] == "book":
                row["payload"][1]["timestamp"] = (START - timedelta(minutes=2)).isoformat()
        measured = await benchmark.measure(rows, 1)
        self.assertEqual(measured["accepted_books_per_account"], 0)
        self.assertEqual(measured["valid_features_per_slot"], 1)
        self.assertEqual(assess([measured], 1), 0)

    async def test_delayed_decision_times_cannot_hide_a_receive_burst(self) -> None:
        rows = actionable_feed()
        for row in rows:
            row["received"] = (START - timedelta(seconds=1)).isoformat()
        books = [row for row in rows if row["kind"] == "book"]
        for index, row in enumerate(books):
            row["at"] = (START + timedelta(seconds=index)).isoformat()
            row["received"] = (START + timedelta(milliseconds=10 * index)).isoformat()
            row["payload"][1]["timestamp"] = row["received"]

        ticks = (index * 0.5 for index in range(2 * len(rows)))
        with patch("tools.paper_capacity.time.perf_counter", side_effect=ticks):
            measured = await CapacityBenchmark("US", 1000, 1).measure(rows, 1)

        self.assertEqual(measured["accepted_books_per_account"], 3)
        self.assertAlmostEqual(measured["observed_seconds"], 0.02, places=5)
        self.assertAlmostEqual(measured["max_backlog_seconds"], 1.48, places=5)
        self.assertEqual(measured["processing_seconds"], 1.5)
        self.assertEqual(assess([measured], 1), 0)

    def test_workload_requires_matching_market_currency_and_profile(self) -> None:
        identity = {"market": "US", "currency": "USD", "profile": "broker-constrained"}
        with TemporaryDirectory() as directory:
            for index, defect in enumerate(
                ({}, {"market": "KR"}, {"currency": "KRW"}, {"profile": "legacy"})
            ):
                with self.subTest(defect=defect):
                    path = Path(directory) / f"workload-{index}.sqlite3"
                    rows = actionable_feed()
                    rows[0].pop("at")
                    with AdaptiveJournal.open(
                        path, {**identity, **defect}, resume=False
                    ) as journal:
                        for row in rows:
                            journal.append(row)
                        journal.complete()
                    if defect:
                        with self.assertRaisesRegex(
                            ValueError, "another market or execution profile"
                        ):
                            workload(path, 10, "US")
                    else:
                        selected, digest = workload(path, 10, "US")
                        self.assertEqual(selected, actionable_feed())
                        self.assertEqual(len(digest), 64)

    def test_enumeration_contains_no_duplicate_strategies(self) -> None:
        values = strategies()
        self.assertEqual(len(values), len({tuple(sorted(item.items())) for item in values}))
        self.assertEqual({item["policy"] for item in values}, {"trend", "rebound"})
