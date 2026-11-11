"""Exercise the runner's shared acquisition path with synthetic observations."""

import asyncio
import gzip
import hashlib
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from papertrade.api import MarketClient
from tools.market_data_adapter import MarketDataAdapter
from tools.market_data_gateway import DataResult, MarketDataGateway
from tools.market_data_repository import DataRequest, MarketDataRepository
from tools.run_async_live_validation import (
    Runtime,
    campaign,
    completed_candle_before,
    fetch_rest_book,
    recover_held_quotes,
)

START = datetime(2026, 11, 10, 15, 0, 15, tzinfo=UTC)


class SharedDataIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.repository = MarketDataRepository(Path(directory.name) / "data.sqlite3")
        self.addCleanup(self.repository.close)
        self.current = START
        self.client = Mock(spec=MarketClient)
        self.gateway = MarketDataGateway(
            self.repository, MarketDataAdapter(self.client), clock=lambda: self.current.isoformat()
        )
        self.addAsyncCleanup(self.gateway.close)

    def book(self, timestamp: datetime) -> dict[str, object]:
        return {
            "timestamp": timestamp.isoformat(),
            "currency": "USD",
            "asks": [{"price": "10", "volume": "100"}],
            "bids": [{"price": "9.99", "volume": "100"}],
        }

    async def test_ws_book_prevents_duplicate_rest_and_account_lag_does_not_trigger_repair(
        self,
    ) -> None:
        ws = self.gateway.ingest_book(
            "US", "A", self.book(START - timedelta(seconds=1)),
            received_time=START.isoformat(),
        )
        raw = StringIO()
        publish = Mock()
        failure = AsyncMock()
        await fetch_rest_book(self.gateway, "A", raw, publish, failure)
        self.client.book.assert_not_called()
        publish.assert_not_called()
        self.assertEqual(raw.getvalue(), "")

        runtime = Mock(spec=Runtime)
        runtime.protected.return_value = {"A"}
        runtime.stale_held_symbols.return_value = {"A"}
        repair = AsyncMock()
        await recover_held_quotes(runtime, repair, {}, float("inf"), gateway=self.gateway)
        repair.assert_not_awaited()
        runtime.stale_held_symbols.assert_not_called()

        self.current += timedelta(seconds=62)
        await recover_held_quotes(runtime, repair, {}, float("inf"), gateway=self.gateway)
        repair.assert_awaited_once_with("A")
        rest = self.book(self.current - timedelta(seconds=1))
        self.client.book.return_value = rest
        await fetch_rest_book(self.gateway, "A", raw, publish, failure, repair=True)
        self.client.book.assert_called_once_with("A")
        publish.assert_called_once_with("book", ("A", rest), received=self.current.isoformat())
        payload = json.loads(raw.getvalue())
        self.assertEqual(payload["received_at"], self.current.isoformat())
        self.assertEqual(payload["source_identity"], "toss-rest")
        self.assertGreater(payload["published_sequence"], ws.published_sequence)

    async def test_fixed_candle_boundary_reuses_provider_call_and_respects_inclusive_end(self) -> None:
        first = completed_candle_before(self.current.isoformat(), timestamp_is_end=True)
        self.client.candles.return_value = {
            "candles": [{"timestamp": first, "closePrice": "10", "highPrice": "10.1"}]
        }
        one = await self.gateway.candles("US", "A", first, 90, max_age=90)
        self.current += timedelta(seconds=30)
        second = completed_candle_before(self.current.isoformat(), timestamp_is_end=True)
        two = await self.gateway.candles("US", "A", second, 90, max_age=90)
        self.assertEqual(second, first)
        self.assertEqual(two.require("A"), one.require("A"))
        self.assertEqual(two.cache_hits, ("A",))
        self.client.candles.assert_called_once_with("A", first, 90)
        self.assertEqual(
            completed_candle_before(self.current.isoformat(), timestamp_is_end=False),
            "2026-11-10T14:59:00+00:00",
        )

    async def test_rest_cannot_replace_newer_stream_source_when_response_arrives_later(self) -> None:
        ws = self.gateway.ingest_book(
            "US", "A", self.book(START), received_time=START.isoformat(),
        )
        self.current += timedelta(seconds=5)
        self.client.book.return_value = self.book(START - timedelta(seconds=1))
        fetched = (await self.gateway.book("US", "A")).require("A")
        self.assertGreater(fetched.published_sequence, ws.published_sequence)
        latest = self.gateway.cached(DataRequest("US", "book", ("A",), max_age=30))
        self.assertEqual(latest["A"], ws)


    async def test_campaign_price_collector_writes_real_gateway_store(self) -> None:
        settings = SimpleNamespace(data={"limits": {}})
        self.client.settings = settings
        self.client.access_token.return_value = "fixture-token"
        self.client.listed.return_value = [{"symbol": "A"}]
        self.client.calendar.return_value = {}
        self.client.get.return_value = {}
        self.client.prices.return_value = [
            {
                "symbol": "A",
                "currency": "USD",
                "timestamp": START.isoformat(),
                "lastPrice": "10",
            }
        ]
        client_module = ModuleType("websockets.asyncio.client")
        vars(client_module)["connect"] = Mock(side_effect=AssertionError("fixture needs no connection"))
        errors = ModuleType("websockets.exceptions")
        vars(errors)["ConnectionClosed"] = type("ConnectionClosed", (Exception,), {})
        vars(errors)["InvalidStatus"] = type("InvalidStatus", (Exception,), {})
        vars(errors)["WebSocketException"] = type("WebSocketException", (Exception,), {})
        modules = {
            "websockets": ModuleType("websockets"),
            "websockets.asyncio": ModuleType("websockets.asyncio"),
            "websockets.asyncio.client": client_module,
            "websockets.exceptions": errors,
        }
        reached = asyncio.Event()
        read_prices = MarketDataGateway.prices

        async def capture_prices(
            gateway: MarketDataGateway, market: str, symbols: list[str],
        ) -> DataResult:
            result = await read_prices(gateway, market, symbols)
            reached.set()
            return result

        async def park(_runtime: Runtime) -> None:
            await asyncio.Future()

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict("sys.modules", modules),
            patch("tools.run_async_live_validation.Settings.load", return_value=settings),
            patch("tools.run_async_live_validation.CachedTokenClient", return_value=self.client),
            patch("tools.run_async_live_validation.read_spec", return_value=b"{}"),
            patch("tools.run_async_live_validation.now", return_value=START.isoformat()),
            patch.object(MarketDataGateway, "prices", new=capture_prices),
            patch.object(Runtime, "consume", new=park),
        ):
            output = Path(directory) / "run"
            task = asyncio.create_task(campaign(output, 30, 100))
            try:
                await asyncio.wait_for(reached.wait(), 3)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.client.prices.assert_called_once_with(["A"])
            repository = MarketDataRepository(output / "market-data.sqlite3")
            try:
                rows = repository.lookup(
                    DataRequest("US", "price", ("A",), max_age=30), now=START.isoformat()
                )
                self.assertEqual(rows["A"].payload["lastPrice"], "10")
                self.assertEqual(rows["A"].received_time, START.isoformat())
                self.assertEqual(rows["A"].source_identity, "toss-rest")
            finally:
                repository.close()
            self.assertTrue((output / "runtime-audit.sqlite3").is_file())


    async def test_rejected_stream_frames_are_recorded_without_payload(self) -> None:
        settings = SimpleNamespace(data={"limits": {}})
        self.client.settings = settings
        self.client.access_token.return_value = "fixture-token"
        self.client.listed.return_value = []
        self.client.calendar.return_value = {}
        self.client.get.return_value = {}
        ws = Mock()
        ws.send = AsyncMock()
        connection = AsyncMock()
        connection.__aenter__.return_value = ws
        client_module = ModuleType("websockets.asyncio.client")
        connect = Mock(return_value=connection)
        vars(client_module)["connect"] = connect
        errors = ModuleType("websockets.exceptions")
        for name in ("ConnectionClosed", "InvalidStatus", "WebSocketException"):
            vars(errors)[name] = type(name, (Exception,), {})
        modules = {
            "websockets": ModuleType("websockets"),
            "websockets.asyncio": ModuleType("websockets.asyncio"),
            "websockets.asyncio.client": client_module,
            "websockets.exceptions": errors,
        }

        async def select(runtime: Runtime) -> None:
            runtime.desired = ["A"]
            await asyncio.Future()

        fixtures = (
            (b'{"secret-fixture"', "invalid_json"),
            ('["secret-fixture"]', "non_object"),
            ('{"type": "secret-fixture"}', "unknown_type"),
        )
        for wire, code in fixtures:
            with (
                self.subTest(code=code),
                tempfile.TemporaryDirectory() as directory,
                patch.dict("sys.modules", modules),
                patch("tools.run_async_live_validation.Settings.load", return_value=settings),
                patch("tools.run_async_live_validation.CachedTokenClient", return_value=self.client),
                patch("tools.run_async_live_validation.read_spec", return_value=b"{}"),
                patch("tools.run_async_live_validation.now", return_value=START.isoformat()),
                patch.object(Runtime, "consume", new=select),
            ):
                connect.reset_mock()
                ws.recv = AsyncMock(return_value=wire)
                output = Path(directory) / "run"
                with self.assertRaisesRegex(SystemExit, "Live validation failed"):
                    await campaign(output, 30, 100)
                connect.assert_called_once()
                ws.recv.assert_awaited_once()
                with gzip.open(output / "market.jsonl.gz", "rt") as stream:
                    raw = stream.read()
                self.assertNotIn("secret-fixture", raw)
                self.assertNotIn("fixture-token", raw)
                observed = [
                    row
                    for line in raw.splitlines()
                    if (row := json.loads(line)).get("kind") == "websocket_frame_failure"
                ]
                data = wire.encode("utf-8") if isinstance(wire, str) else wire
                self.assertEqual(
                    observed,
                    [
                        {
                            "kind": "websocket_frame_failure",
                            "received_at": START.isoformat(),
                            "frame_type": "text" if isinstance(wire, str) else "binary",
                            "byte_length": len(data),
                            "sha256": hashlib.sha256(data).hexdigest(),
                            "code": code,
                        }
                    ],
                )
