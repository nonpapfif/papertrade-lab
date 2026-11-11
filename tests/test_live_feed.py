"""Live snapshot completeness, freshness, causal features and read-only transport."""

from __future__ import annotations

import copy
import pickle
import threading
import time
import unittest
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, call, patch

from papertrade.api import ApiError, MarketClient
from papertrade.core import Settings
from tools.live_feed import LiveFeed, SnapshotHistory, _PacedClient


def at(minute: int, seconds: int = 0) -> str:
    return (
        datetime(2026, 11, 2, 12, tzinfo=UTC) + timedelta(minutes=minute, seconds=seconds)
    ).isoformat()


def quote(minute: int, price: float = 100, symbol: str = "TEST") -> dict[str, Any]:
    return {"symbol": symbol, "lastPrice": str(price), "timestamp": at(minute), "currency": "USD"}


class LiveFeedTests(unittest.TestCase):
    def test_worker_reservations_and_rate_updates_share_one_lock_and_private_transports(self) -> None:
        active = [0]
        maximum = [0]

        def critical(*args: object) -> float:
            active[0] += 1
            maximum[0] = max(maximum[0], active[0])
            time.sleep(0.01)
            active[0] -= 1
            return 0.0

        self.client._reserve.side_effect = critical
        self.client._rate_result.side_effect = critical
        lock = threading.Lock()
        first = _PacedClient(self.feed.settings, self.client, lock, lambda _: None)
        second = _PacedClient(self.feed.settings, self.client, lock, lambda _: None)
        threads = [
            threading.Thread(target=first._reserve, args=("MARKET_DATA",)),
            threading.Thread(target=second._rate_result, args=("MARKET_DATA", {}, False)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(maximum[0], 1)
        self.assertIsNot(first.opener, second.opener)
        first.token = "synthetic-token"
        self.assertIsNone(second.token)

    def test_parallel_books_keep_transport_private_and_sqlite_callbacks_on_main_thread(
        self,
    ) -> None:
        main_thread = threading.get_ident()
        barrier = threading.Barrier(2)
        clients = []

        class Client:
            def __init__(
                self, settings: object, primary: object, lock: object,
                audit: Callable[[dict[str, Any]], None],
            ) -> None:
                self.audit = audit
                clients.append(self)

            def book(self, symbol: str) -> dict:
                barrier.wait(timeout=3)
                self.audit({"path": "/api/v1/orderbook", "status": 200})
                return {"symbol": symbol, "timestamp": at(0), "currency": "USD"}

        def record(kind: str, payload: dict) -> None:
            self.assertEqual(threading.get_ident(), main_thread)
            self.events.append((kind, payload))

        self.feed.record = record
        with patch("tools.live_feed._PacedClient", Client):
            result = self.feed.orderbook_batch(["A", "B", "A"])
        self.assertEqual(list(result), ["A", "B"])
        self.assertEqual(len(clients), 2)
        self.assertTrue(all(isinstance(row, dict) and "received_at" in row for row in result.values()))
        self.assertEqual(sum(k == "http" for k, _ in self.events), 2)
        self.assertEqual(sum(k == "orderbook" for k, _ in self.events), 2)

    def test_parallel_batch_cancels_pending_work_on_provider_rate_rejection(self) -> None:
        calls = []

        class Client:
            def __init__(self, *args: object) -> None:
                pass

            def book(self, symbol: str) -> dict:
                calls.append(symbol)
                if symbol == "A":
                    raise ApiError("Provider limit", status=429, code="rate-limit", retry_at=123)
                time.sleep(0.1)
                return {"symbol": symbol}

        symbols = ["A", *[f"B{i}" for i in range(63)]]
        with patch("tools.live_feed._PacedClient", Client):
            result = self.feed.orderbook_batch(symbols)
        self.assertEqual(len(result), len(symbols))
        self.assertLess(len(calls), len(symbols))
        self.assertTrue(
            any(isinstance(r, ApiError) and r.code == "batch_cancelled" for r in result.values())
        )

    def setUp(self) -> None:
        self.client = Mock(spec=MarketClient)
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.feed = LiveFeed(
            Settings(Path("/tmp"), {"limits": {"minimum_request_interval": 0.2}}),
            lambda kind, payload: self.events.append((kind, payload)),
            client=self.client,
        )

    def test_full_universe_deduplicates_and_batches_201_without_symbol_cap(self) -> None:
        symbols = [f"S{i:03d}" for i in range(201)]
        self.client.listed.side_effect = [
            [{"symbol": symbol} for symbol in symbols[:100]],
            [{"symbol": symbol} for symbol in symbols[100:]],
            [{"symbol": symbols[0]}],
            [],
        ]
        self.client.prices.side_effect = lambda batch: [quote(0, symbol=symbol) for symbol in batch]
        self.assertEqual(self.feed.universe(), symbols)
        actual = self.feed.snapshot()
        self.assertEqual([row["symbol"] for row in actual], symbols)
        self.assertEqual(
            self.client.listed.call_args_list,
            [call(market, status="ACTIVE") for market in ("NYSE", "NASDAQ", "AMEX", "US_ETC")],
        )
        self.assertEqual(
            self.client.prices.call_args_list, [call(symbols[:200]), call(symbols[200:])]
        )
        self.client.candles.assert_not_called()
        self.client.daily.assert_not_called()
        self.assertEqual(sum(kind == "prices" for kind, _ in self.events), 2)

    def test_restored_universe_avoids_listing_calls_and_records_missing_rows(self) -> None:
        self.feed.symbols = ["A", "B"]
        self.client.prices.return_value = [quote(0, symbol="A")]
        self.assertEqual(len(self.feed.snapshot()), 1)
        self.client.listed.assert_not_called()
        self.assertEqual(self.events[-1][1]["missing_symbols"], ["B"])
        self.client.audit({"path": "/api/v1/prices", "status": 200})
        self.assertEqual(self.events[-1], ("http", {"path": "/api/v1/prices", "status": 200}))

    def test_only_public_market_methods_and_original_results_are_recorded(self) -> None:
        book = {"symbol": "A", "asks": [], "bids": []}
        self.client.book.return_value = book
        self.client.get.return_value = {"rate": "1334.05"}
        self.client.calendar.return_value = {"date": "2026-11-02"}
        self.assertEqual(self.feed.orderbooks(["A", "A"]), {"A": book})
        self.assertEqual(self.feed.fx(), {"rate": "1334.05"})
        self.assertEqual(self.feed.calendar("2026-11-02"), {"date": "2026-11-02"})
        self.client.book.assert_called_once_with("A")
        self.client.get.assert_called_once_with(
            "/api/v1/exchange-rate", baseCurrency="USD", quoteCurrency="KRW"
        )
        self.client.calendar.assert_called_once_with("US", "2026-11-02")
        self.assertEqual([kind for kind, _ in self.events], ["orderbook", "fx", "calendar"])

    def test_malformed_or_unrequested_rows_are_preserved_then_rejected(self) -> None:
        self.feed.symbols = ["A"]
        for rows in [
            [quote(0, symbol="B")],
            [quote(0, symbol="A")] * 2,
            [{"symbol": {"bad": "shape"}}],
        ]:
            self.client.prices.return_value = rows
            with self.assertRaises(ApiError):
                self.feed.snapshot()
            self.assertEqual(self.events[-1][1]["rows"], rows)

    def test_transport_failure_propagates_without_hidden_retry(self) -> None:
        self.feed.symbols = ["A"]
        error = ApiError("Provider cooldown", status=429, code="rate-limit", retry_at=123)
        self.client.prices.side_effect = error
        with self.assertRaises(ApiError) as caught:
            self.feed.snapshot()
        self.assertIs(caught.exception, error)
        self.client.prices.assert_called_once_with(["A"])


class SnapshotHistoryTests(unittest.TestCase):
    def test_stale_and_invalid_rows_are_reported_and_not_freshened(self) -> None:
        events = []
        history = SnapshotHistory(lambda kind, payload: events.append((kind, payload)))
        report = history.add(
            at(2),
            [quote(0), {**quote(2, symbol="BAD"), "lastPrice": "nan"}, quote(2, symbol="FRESH")],
        )
        self.assertEqual((report["accepted"], report["rejected"]), (1, 2))
        self.assertEqual(
            {issue["reason"] for issue in report["issues"]}, {"stale_source_time", "invalid_price"}
        )
        self.assertEqual(len(events), 2)
        self.assertEqual([row["symbol"] for row in history.features(at(2))], ["FRESH"])
        self.assertEqual(history.features(at(3)), [])

    def test_future_source_timestamp_is_quarantined_without_stopping_other_symbols(self) -> None:
        events = []
        history = SnapshotHistory(lambda kind, payload: events.append((kind, payload)))
        history.add(at(0), [quote(0)])
        future = quote(2, symbol="FUTURE")
        result = history.add(at(1), [quote(1, 105), future])
        self.assertEqual((result["accepted"], result["rejected"]), (1, 1))
        self.assertEqual(result["issues"][0]["reason"], "future_source_time")
        self.assertEqual(len(history.rows["TEST"]), 2)
        assert history.last_at is not None
        self.assertEqual(history.last_at.isoformat(), at(1))
        self.assertNotIn("FUTURE", history.rows)
        self.assertEqual(events[-1][1]["row"], future)
        with self.assertRaisesRegex(ValueError, "strictly"):
            history.add(at(0), [])

    def test_provider_time_regression_is_quarantined_instead_of_replacing_history(self) -> None:
        history = SnapshotHistory()
        history.add(at(2), [quote(2)])
        older = {**quote(1, 80), "timestamp": at(1, 30)}
        result = history.add(at(3), [older])
        self.assertEqual(result["issues"][0]["reason"], "source_time_regressed")
        result = history.add(at(3, 30), [quote(2, 90)])
        self.assertEqual(result["issues"][0]["reason"], "conflicting_source_price")
        self.assertEqual(len(history.rows["TEST"]), 1)
        with self.assertRaisesRegex(ValueError, "rewind"):
            history.features(at(2))

    def test_lookback_warmup_and_sampled_features_never_fabricate_volume_or_ohlc(self) -> None:
        history = SnapshotHistory()
        history.add(at(0), [quote(0)])
        self.assertEqual(history.features(at(0))[0]["scores"], [None] * 30)
        for minute in range(1, 66):
            history.add(at(minute), [quote(minute, 100 + minute)])
        feature = history.features(at(65))[0]
        self.assertAlmostEqual(feature["scores"][0], 165 / 160 - 1)
        self.assertAlmostEqual(feature["scores"][4], (165 / 105 - 1) * 5 / 60)
        self.assertAlmostEqual(feature["scores"][10], 165 / 160 - 1)
        self.assertEqual(feature["scores"][25:], [None] * 5)
        self.assertEqual(feature["volumes"], [None] * 5)
        self.assertTrue(all(value is not None for value in feature["noises"]))
        self.assertEqual(feature["feature_basis"], "sampled_60s_prices_not_ohlcv")
        self.assertNotIn("high", feature)
        self.assertNotIn("low", feature)
        self.assertEqual(feature["quote_at"], at(65))
        self.assertEqual(feature["source_at"], at(65))
        self.assertTrue(
            all(
                observed <= at(65) and source <= observed
                for observed, source in feature["source_times"]
            )
        )

    def test_observation_time_is_separate_from_provider_source_time(self) -> None:
        history = SnapshotHistory()
        history.add(at(1), [quote(0)])
        feature = history.features(at(1))[0]
        self.assertEqual(feature["quote_at"], at(1))
        self.assertEqual(feature["source_at"], at(0))
        self.assertEqual(feature["price_age_seconds"], 60)

    def test_missing_snapshot_gap_prevents_complete_window_signals(self) -> None:
        history = SnapshotHistory()
        for minute in [0, 1, 2, 5, 6, 7, 8, 9, 10]:
            history.add(at(minute), [quote(minute, 100 + minute)])
        feature = history.features(at(10))[0]
        self.assertIsNone(feature["scores"][1])
        self.assertIsNotNone(feature["scores"][0])
        self.assertIsNone(feature["scores"][10])

    def test_changed_future_snapshots_cannot_change_prefix_features(self) -> None:
        first, changed = SnapshotHistory(), SnapshotHistory()
        before, after = [], []
        for minute in range(16):
            first.add(at(minute), [quote(minute, 100 + minute)])
            changed.add(at(minute), [quote(minute, 100 + minute if minute < 11 else 500)])
            if minute <= 10:
                before.append(copy.deepcopy(first.features(at(minute))))
                after.append(copy.deepcopy(changed.features(at(minute))))
        self.assertEqual(before, after)

    def test_no_callback_history_round_trips_through_plain_checkpoint_pickle(self) -> None:
        history = SnapshotHistory()
        history.add(at(0), [quote(0)])
        restored = pickle.loads(pickle.dumps(history))
        self.assertIsNone(restored.record)
        self.assertEqual(restored.features(at(0)), history.features(at(0)))


if __name__ == "__main__":
    unittest.main()
