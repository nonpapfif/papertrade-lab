"""Shared pacing, reverse chronology, holiday handling, and atomic parallel collection."""

from __future__ import annotations

import contextlib
import io
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest.mock import patch

from papertrade.api import ApiError
from papertrade.core import SafetyError, Settings
from tools.download_market_day import SCHEMA, Blocked, summary
from tools.download_market_month import ParallelDay, RateGate, inspect_day, reverse_dates, run_month


def candle(at: str, close: str = "101") -> dict[str, str]:
    return {
        "timestamp": at,
        "openPrice": "100",
        "highPrice": "110",
        "lowPrice": "90",
        "closePrice": close,
        "volume": "5",
        "currency": "USD",
    }


class MonthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = Settings(self.root, {"limits": {"minimum_request_interval": 0.01}})
        self.seed = self.root / "seed.sqlite3"
        with sqlite3.connect(self.seed) as db:
            db.executescript(SCHEMA)
            db.executemany(
                "INSERT INTO metadata VALUES (?,?)",
                [
                    ("date", '"2026-10-30"'),
                    ("state", '"complete"'),
                    ("universe_captured_at", '"2026-10-31T00:00:00Z"'),
                ],
            )
            db.execute("INSERT INTO listing_jobs VALUES ('US','NASDAQ','ACTIVE',1)")
            for market, symbol in (("KR", "005930"), ("US", "TEST"), ("US", "TEST2")):
                db.execute(
                    "INSERT INTO listings VALUES (?,?,?,?,?)",
                    (market, "EXCHANGE", "ACTIVE", symbol, "{}"),
                )
                db.execute(
                    "INSERT INTO symbols(market,symbol,name,before_query) VALUES (?,?,?,?)",
                    (market, symbol, symbol, "2026-10-30T00:00:00Z"),
                )

    def make_day(self) -> ParallelDay:
        day = ParallelDay(
            self.settings, self.root / "day", "2026-10-29", self.seed, RateGate(5), workers=4
        )
        self.addCleanup(day.close)
        return day

    def calendar(self, market: str, closed: bool = False) -> dict[str, Any]:
        today: dict[str, Any] = {"date": "2026-10-29"}
        window = {
            "regularMarket": {
                "startTime": "2026-10-29T13:00:00Z",
                "endTime": "2026-10-29T13:03:00Z",
            }
        }
        today.update(
            {"integrated": None if closed else window}
            if market == "KR"
            else {}
            if closed
            else window
        )
        return {"today": today}

    def initialize(self, day: ParallelDay, closed: tuple[str, ...] = ()) -> None:
        responses = [self.calendar(m, m in closed) for m in ("KR", "US")]
        with (
            patch.object(day.client, "get", side_effect=lambda *args, **kw: responses.pop(0)),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            day.initialize()

    def test_reverse_order_includes_weekends_for_calendar_checks(self) -> None:
        dates = reverse_dates("2026-10", "2026-10-29")
        self.assertEqual(dates[:3], ["2026-10-29", "2026-10-28", "2026-10-27"])
        self.assertEqual(dates[-1], "2026-10-01")
        self.assertEqual(len(dates), 29)
        with self.assertRaises(ValueError):
            reverse_dates("2026-10", "2026-09-30")

    def test_shared_gate_spaces_parallel_reservations(self) -> None:
        gate = RateGate(10)

        def reserve(_: int) -> float:
            gate.reserve("MARKET_DATA_CHART")
            return time.time()

        with ThreadPoolExecutor(max_workers=4) as pool:
            times = sorted(pool.map(reserve, range(4)))
        self.assertGreaterEqual(times[-1] - times[0], 0.28)

    def test_header_reduces_rate_and_429_persists_global_cooldown(self) -> None:
        gate = RateGate(5)
        gate.observe(
            {"path": "/api/v1/candles", "status": 200, "rate_limit": {"X-RateLimit-Limit": "2"}}
        )
        self.assertEqual(gate.interval("MARKET_DATA_CHART"), 1)
        gate.observe({"path": "/api/v1/candles", "status": 429})
        saved = gate.snapshot()
        self.assertGreaterEqual(saved["cooldowns"]["MARKET_DATA_CHART"], time.time() + 59)
        restored = RateGate(5, saved)
        self.assertEqual(restored.interval("MARKET_DATA_CHART"), 2)

    def test_denied_access_stops_every_worker_without_new_requests(self) -> None:
        gate = RateGate(5)
        gate.observe({"path": "/api/v1/candles", "status": 403})
        for group in ("MARKET_DATA_CHART", "MARKET_INFO", "AUTH"):
            with self.assertRaises(Blocked):
                gate.reserve(group)

    def test_cancel_wakes_a_worker_in_a_long_cooldown(self) -> None:
        gate = RateGate(5)
        gate.defer("MARKET_DATA_CHART", 60)
        errors = []

        def reserve() -> None:
            try:
                gate.reserve("MARKET_DATA_CHART")
            except Blocked as error:
                errors.append(error)

        thread = threading.Thread(target=reserve)
        thread.start()
        gate.cancel()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)

    def test_one_closed_market_does_not_stop_the_other(self) -> None:
        day = self.make_day()
        self.initialize(day, ("KR",))
        self.assertEqual(day.closed_markets, ["KR"])
        self.assertEqual(set(day.meta("scopes")), {"US"})
        self.assertEqual(day.db.execute("SELECT COUNT(*) FROM symbols").fetchone()[0], 2)
        self.assertEqual(day.meta("scopes")["US"]["end"], "2026-10-30T13:00:00+00:00")

    def test_both_closed_markets_issue_no_candle_requests(self) -> None:
        day = self.make_day()
        self.initialize(day, ("KR", "US"))
        with patch.object(day, "fetch") as fetch, contextlib.redirect_stdout(io.StringIO()):
            day.run()
        fetch.assert_not_called()
        self.assertEqual(day.meta("state"), "complete")
        self.assertEqual(summary(day.db)["bars"], 0)

    def test_parallel_pages_keep_one_inflight_job_per_symbol(self) -> None:
        day = self.make_day()
        self.initialize(day, ("KR",))
        active: set[str] = set()
        lock = threading.Lock()
        seen = []

        def fetch(job: dict[str, Any]) -> dict[str, Any]:
            with lock:
                self.assertNotIn(job["symbol"], active)
                active.add(job["symbol"])
                seen.append((job["symbol"], job["before_query"]))
            time.sleep(0.005)
            first = job["pages"] == 0
            page = {
                "candles": [candle("2026-10-29T13:03:00Z" if first else "2026-10-29T13:02:00Z")],
                "nextBefore": "2026-10-29T13:02:00Z" if first else None,
            }
            with lock:
                active.remove(job["symbol"])
            return page

        with (
            patch.object(day, "fetch", side_effect=fetch),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            day.run()
        self.assertEqual(day.meta("state"), "complete")
        self.assertEqual(summary(day.db)["bars"], 4)
        self.assertEqual(len(seen), 4)
        self.assertEqual(day.db.execute("SELECT COUNT(*) FROM pages").fetchone()[0], 4)
        self.assertEqual(day.db.execute("PRAGMA quick_check").fetchone()[0], "ok")

    def test_unavailable_symbol_remains_visible(self) -> None:
        day = self.make_day()
        self.initialize(day, ("KR",))
        with (
            patch.object(day, "fetch", side_effect=ApiError("Missing", status=404)),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            day.run()
        self.assertEqual(day.meta("state"), "complete_with_errors")
        self.assertEqual(
            day.db.execute("SELECT COUNT(*) FROM symbols WHERE status='unavailable'").fetchone()[0],
            2,
        )

    def test_invalid_raw_page_is_retained_as_failure(self) -> None:
        day = self.make_day()
        self.initialize(day, ("KR",))
        page = {"candles": [candle("2026-10-29T13:03:00Z", "0")], "nextBefore": None}
        with (
            patch.object(day, "fetch", return_value=page),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            day.run()
        self.assertEqual(day.meta("state"), "complete_with_errors")
        self.assertEqual(day.db.execute("SELECT SUM(valid) FROM pages").fetchone()[0], 0)
        self.assertEqual(summary(day.db)["bars"], 0)

    def test_forbidden_endpoint_never_reaches_client(self) -> None:
        day = self.make_day()
        with patch.object(day.client, "get") as get, self.assertRaises(SafetyError):
            day.request("/not-market-data")
        get.assert_not_called()

    def test_new_program_preserves_seed_database(self) -> None:
        before = self.seed.read_bytes()
        day = self.make_day()
        self.initialize(day)
        self.assertEqual(self.seed.read_bytes(), before)
        self.assertEqual(day.meta("universe_source"), str(self.seed.resolve()))
        self.assertIn("not a reconstructed", day.meta("universe_limitation"))

    def test_completed_day_is_read_only_and_inspectable(self) -> None:
        day = self.make_day()
        self.initialize(day, ("KR", "US"))
        with contextlib.redirect_stdout(io.StringIO()):
            day.run()
        status = inspect_day(self.root / "day" / "market.sqlite3")
        assert status is not None
        self.assertEqual(status["state"], "complete")
        self.assertIsNone(inspect_day(self.root / "missing.sqlite3"))

    def test_month_reuses_completed_days_and_runs_in_descending_order(self) -> None:
        visited = []

        def run(day: ParallelDay) -> None:
            visited.append(day.target)
            day.set_meta("state", "complete")
            day.db.commit()

        output = self.root / "month"
        with patch.object(ParallelDay, "run", run), contextlib.redirect_stdout(io.StringIO()):
            run_month(self.settings, "2026-10", "2026-10-03", output, self.seed)
            run_month(self.settings, "2026-10", "2026-10-03", output, self.seed)
        self.assertEqual(visited, ["2026-10-03", "2026-10-02", "2026-10-01"])
        report = json.loads((output / "status.json").read_text())
        self.assertEqual(report["state"], "complete")
        self.assertTrue(all(d["reused"] for d in report["days"].values()))

    def test_blocked_month_requires_explicit_resume(self) -> None:
        output = self.root / "month"
        with (
            patch.object(ParallelDay, "run", side_effect=Blocked("Denied")),
            self.assertRaises(Blocked),
        ):
            run_month(self.settings, "2026-10", "2026-10-01", output, self.seed)
        with self.assertRaises(Blocked):
            run_month(self.settings, "2026-10", "2026-10-01", output, self.seed)
        self.assertEqual(json.loads((output / "status.json").read_text())["state"], "blocked")


if __name__ == "__main__":
    unittest.main()
