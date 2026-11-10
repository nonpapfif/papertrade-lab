"""Historical pagination, durable checkpoints, and polite market-data requests."""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import io
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from papertrade.api import ApiError, MarketClient
from papertrade.core import SafetyError, Settings
from tools.download_market_day import Blocked, Downloader, summary


class FakeClient(MarketClient):
    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.responses: list[Any] = []
        self.seen: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, **parameters: Any) -> Any:
        self.seen.append((path, parameters))
        result = self.responses.pop(0)
        if isinstance(result, ApiError):
            raise result
        self.audit({"path": path, "status": 200, "rate_limit": {
            "X-RateLimit-Limit": "20", "X-RateLimit-Remaining": "10",
        }})
        return result


def candle(at: str, close: str = "101") -> dict[str, str]:
    return {"timestamp": at, "openPrice": "100", "highPrice": "110", "lowPrice": "90",
            "closePrice": close, "volume": "5", "currency": "USD"}


class DownloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.settings = Settings(self.root, {"limits": {"minimum_request_interval": 0.01}})
        self.client = FakeClient(self.settings)
        self.downloader = Downloader(self.settings, self.root, "2026-10-30", client=self.client)
        self.addCleanup(lambda: self.downloader.close())
        self.scope: dict[str, Any] = {
            "start": "2026-10-30T13:00:00+00:00", "end": "2026-10-30T13:03:00+00:00",
            "sessions": [{"start": "2026-10-30T13:00:00+00:00",
                          "end": "2026-10-30T13:02:00+00:00"}],
        }
        with self.downloader.db:
            self.downloader.set_meta("scopes", {"US": self.scope})
            self.downloader.db.execute(
                "INSERT INTO symbols (market,symbol,name,before_query) VALUES ('US','TEST',?,?)",
                ("Test", self.scope["end"]),
            )

    def job(self) -> Any:
        return self.downloader.db.execute("SELECT * FROM symbols WHERE symbol='TEST'").fetchone()

    def page(self, rows: list[Any], cursor: str | None) -> dict[str, Any]:
        return {"candles": rows, "nextBefore": cursor}

    def save(self, page: Any) -> None:
        with contextlib.redirect_stdout(io.StringIO()):
            self.downloader.save_page(self.job(), page)

    def test_overlap_is_deduplicated_and_window_start_is_excluded(self) -> None:
        self.save(self.page([candle("2026-10-30T13:03:00Z"),
                             candle("2026-10-30T13:02:00Z")], "2026-10-30T13:02:00Z"))
        self.save(self.page([candle("2026-10-30T13:02:00Z"),
                             candle("2026-10-30T13:01:00Z"),
                             candle("2026-10-30T13:00:00Z")], "2026-10-30T12:59:00Z"))
        self.assertEqual((self.job()["status"], self.job()["bars"]), ("done", 3))
        self.assertEqual(self.job()["pages"], 2)
        self.assertEqual(self.downloader.db.execute(
            "SELECT SUM(outside_calendar) FROM candles"
        ).fetchone()[0], 1)

    def test_resume_uses_committed_cursor_and_preserves_first_page(self) -> None:
        self.save(self.page([candle("2026-10-30T13:03:00Z")], "2026-10-30T13:02:00Z"))
        self.downloader.close()
        self.downloader = Downloader(self.settings, self.root, "2026-10-30", client=self.client)
        self.client.responses = [self.page([candle("2026-10-30T13:02:00Z")], None)]
        with patch.object(self.downloader, "wait"), contextlib.redirect_stdout(io.StringIO()):
            self.downloader.download_symbol("US")
        self.assertEqual(self.client.seen[0][1]["before"], "2026-10-30T13:02:00Z")
        self.assertEqual((self.job()["bars"], self.job()["pages"]), (2, 2))
        self.assertEqual(self.client.seen[0][1]["adjusted"], "false")

    def test_raw_result_hash_and_gzip_are_independently_verifiable(self) -> None:
        page = self.page([candle("2026-10-30T13:03:00Z")], None)
        self.save(page)
        row = self.downloader.db.execute("SELECT * FROM pages").fetchone()
        raw = gzip.decompress(row["result_gzip"])
        self.assertEqual(hashlib.sha256(raw).hexdigest(), row["sha256"])
        self.assertEqual(json.loads(raw), page)

    def test_nonadvancing_cursor_is_preserved_as_failure(self) -> None:
        self.save(self.page([candle("2026-10-30T13:03:00Z")], self.scope["end"]))
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["bars"], 0)
        self.assertEqual(self.downloader.db.execute("SELECT valid FROM pages").fetchone()[0], 0)

    def test_cursor_with_later_utc_time_is_rejected(self) -> None:
        self.save(self.page([candle("2026-10-30T13:03:00Z")], "2026-10-30T13:03:00-00:01"))
        # A UTC-offset cursor above is later than the query and must fail validation.
        self.assertEqual(self.job()["status"], "failed")

    def test_conflicting_second_page_is_atomic(self) -> None:
        self.save(self.page([candle("2026-10-30T13:03:00Z"),
                             candle("2026-10-30T13:02:00Z")], "2026-10-30T13:02:00Z"))
        self.save(self.page([candle("2026-10-30T13:02:00Z", "102"),
                             candle("2026-10-30T13:01:00Z")], None))
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["bars"], 2)
        self.assertEqual(self.downloader.db.execute(
            "SELECT close FROM candles WHERE at='2026-10-30T13:02:00+00:00'"
        ).fetchone()[0], "101")

    def test_empty_day_is_distinct_from_unavailable_symbol(self) -> None:
        self.save(self.page([], None))
        self.assertEqual(self.job()["status"], "no_data")
        self.assertEqual(self.job()["pages"], 1)

    def test_403_is_not_retried(self) -> None:
        self.client.responses = [ApiError("Denied", status=403, code="ip-not-allowed")]
        with (patch.object(self.downloader, "wait"),
              contextlib.redirect_stdout(io.StringIO()), self.assertRaises(Blocked)):
            self.downloader.request("/api/v1/candles", symbol="TEST")
        self.assertEqual(len(self.client.seen), 1)

    def test_429_persists_long_delay_and_slower_interval(self) -> None:
        self.client.responses = [ApiError("Limited", status=429, code="rate-limit"), {}]
        with patch.object(self.downloader, "wait"), contextlib.redirect_stdout(io.StringIO()):
            self.downloader.request("/api/v1/candles", symbol="TEST")
        self.assertGreaterEqual(self.downloader.meta("next_request_at"), time.time() + 59)
        self.assertEqual(self.downloader.meta("minimum_interval_seconds"), 2)
        self.assertEqual(len(self.client.seen), 2)

    def test_header_limit_schedules_at_half_of_provider_rate(self) -> None:
        self.downloader.audit({"path": "/api/v1/stocks/all", "status": 200,
                               "rate_limit": {"X-RateLimit-Limit": "1"}})
        self.assertGreaterEqual(self.downloader.meta("next_request_at"), time.time() + 1.9)

    def test_non_market_data_path_is_rejected_before_client_call(self) -> None:
        with self.assertRaises(SafetyError):
            self.downloader.request("/not-a-market-data-path")
        self.assertEqual(self.client.seen, [])

    def test_dataset_lock_prevents_duplicate_downloaders(self) -> None:
        with self.assertRaises(Blocked):
            Downloader(self.settings, self.root, "2026-10-30", client=self.client)

    def test_status_does_not_need_market_data_access(self) -> None:
        report = summary(self.downloader.db)
        self.assertEqual(report["total_symbols"], 1)
        self.assertEqual(report["requests"], 0)
        self.assertEqual(self.client.seen, [])

    def test_price_validation_rejects_impossible_bar(self) -> None:
        self.save(self.page([candle("2026-10-30T13:03:00Z", "120")], None))
        self.assertEqual(self.job()["status"], "failed")
        self.assertEqual(self.job()["bars"], 0)

    def test_outside_day_zero_bar_does_not_reject_valid_target_bar(self) -> None:
        older = candle("2025-06-06T05:01:00Z", "0")
        for field in ("openPrice", "highPrice", "lowPrice", "volume"):
            older[field] = "0"
        page = self.page([candle("2026-10-30T13:01:00Z"), older], None)
        self.save(page)
        self.assertEqual((self.job()["status"], self.job()["bars"]), ("done", 1))
        row = self.downloader.db.execute("SELECT result_gzip FROM pages").fetchone()
        self.assertEqual(json.loads(gzip.decompress(row[0])), page)

    def test_only_outside_day_zero_bars_is_no_data(self) -> None:
        older = candle("2025-06-06T05:01:00Z", "0")
        for field in ("openPrice", "highPrice", "lowPrice", "volume"):
            older[field] = "0"
        self.save(self.page([older], None))
        self.assertEqual((self.job()["status"], self.job()["bars"]), ("no_data", 0))

    def test_zero_bar_inside_target_day_is_still_rejected(self) -> None:
        self.save(self.page([candle("2026-10-30T13:01:00Z", "0")], None))
        self.assertEqual((self.job()["status"], self.job()["bars"]), ("failed", 0))

    def test_database_failure_rolls_back_page_and_cursor_together(self) -> None:
        self.downloader.db.execute(
            "CREATE TRIGGER fail_second BEFORE INSERT ON candles "
            "WHEN NEW.at='2026-10-30T13:02:00+00:00' "
            "BEGIN SELECT RAISE(ABORT,'Simulated storage failure'); END"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.save(self.page([candle("2026-10-30T13:03:00Z"),
                                 candle("2026-10-30T13:02:00Z")], None))
        self.assertEqual(self.job()["before_query"], self.scope["end"])
        self.assertEqual(self.job()["pages"], 0)
        for table in ("pages", "candles"):
            self.assertEqual(self.downloader.db.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0], 0)

    def test_unfinished_symbol_query_uses_partial_index(self) -> None:
        plan = self.downloader.db.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM symbols WHERE market='US' "
            "AND status IN ('pending','downloading') ORDER BY symbol LIMIT 1"
        ).fetchall()
        self.assertTrue(any("symbols_unfinished" in row[3] for row in plan))

    def test_initialization_freezes_all_listing_states_and_wide_us_window(self) -> None:
        with self.downloader.db:
            self.downloader.db.execute("DELETE FROM metadata WHERE key='scopes'")
            self.downloader.db.execute("DELETE FROM symbols")
        kr = {"today": {"date": "2026-10-30", "integrated": {"regularMarket": {
            "startTime": "2026-10-30T09:00:00+09:00",
            "endTime": "2026-10-30T15:30:00+09:00",
        }}}}
        us = {"today": {"date": "2026-10-30", "dayMarket": {
            "startTime": "2026-10-30T09:00:00+09:00",
            "endTime": "2026-10-30T17:00:00+09:00",
        }, "afterMarket": {"startTime": "2026-10-31T05:00:00+09:00",
                            "endTime": "2026-10-31T08:50:00+09:00"}}}
        self.client.responses = [kr, us, [{"symbol": "005930", "name": "Samsung"}]]
        self.client.responses += [[] for _ in range(8)]
        self.client.responses += [[{"symbol": "AAPL", "name": "Apple"}]]
        self.client.responses += [[] for _ in range(11)]
        with patch.object(self.downloader, "wait"), contextlib.redirect_stdout(io.StringIO()):
            self.downloader.initialize()
        self.assertEqual(summary(self.downloader.db)["total_symbols"], 2)
        self.assertEqual(self.downloader.db.execute(
            "SELECT COUNT(*) FROM listing_jobs WHERE complete=1"
        ).fetchone()[0], 21)
        self.assertEqual(self.downloader.meta("scopes")["US"]["end"],
                         "2026-10-31T00:00:00+00:00")
        self.assertEqual(len(self.client.seen), 23)
        with patch.object(self.downloader, "wait"):
            self.downloader.initialize()
        self.assertEqual(len(self.client.seen), 23)


if __name__ == "__main__":
    unittest.main()
