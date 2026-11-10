"""Exercise owner-side batching, failures, recovery and cross-process pipe clients."""

import asyncio
import multiprocessing
import tempfile
import unittest
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event
from typing import Any
from unittest.mock import patch

from papertrade.api import ApiError
from tools.market_data_gateway import DataResult, GatewayPipeClient, MarketDataGateway
from tools.market_data_repository import DataGroup, DataRequest, MarketDataRepository

START = datetime(2026, 11, 10, 15, tzinfo=UTC)


class Provider:
    def __init__(self, clock: Callable[[], str]) -> None:
        self.calls: list[tuple[DataGroup, tuple[str, ...]]] = []
        self.clock = clock
        self.error: Exception | None = None
        self.started = Event()
        self.release = Event()
        self.release.set()
        self.omit: set[str] = set()
        self.invalid: set[str] = set()

    def fetch(self, group: DataGroup, symbols: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        self.calls.append((group, symbols))
        self.started.set()
        if not self.release.wait(timeout=10):
            raise TimeoutError("fixture release not signaled")
        if self.error is not None:
            raise self.error
        return {
            symbol: {
                "symbol": symbol,
                "currency": "USD",
                "timestamp": self.clock(),
                "lastPrice": float("nan") if symbol in self.invalid else 100,
            }
            for symbol in symbols
            if symbol not in self.omit
        }


def child_request(
    connection: Connection, control: Connection, request: DataRequest
) -> None:
    client = GatewayPipeClient(connection)
    try:
        control.send("ready")
        control.recv()
        result = client.request(request)
        control.send(tuple(row.symbol for row in result.records))
    finally:
        client.close()
        control.close()


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.repo = MarketDataRepository(Path(directory.name) / "data.sqlite3")
        self.addCleanup(self.repo.close)
        self.current = START
        self.provider = Provider(self.clock)
        self.gateway = MarketDataGateway(self.repo, self.provider, clock=self.clock)
        self.addAsyncCleanup(self.gateway.close)
        self.addCleanup(self.provider.release.set)

    def clock(self) -> str:
        return self.current.isoformat()

    async def test_batches_unique_compatible_prices_and_reuses_hits(self) -> None:
        symbols = tuple(f"P{i}" for i in range(320))
        left, right = await asyncio.gather(
            self.gateway.prices("US", symbols[:210], max_age=10),
            self.gateway.prices("US", symbols[150:], max_age=10),
        )
        self.assertEqual(tuple(row.symbol for row in left.records), symbols[:210])
        self.assertEqual(tuple(row.symbol for row in right.records), symbols[150:])
        self.assertEqual([len(batch) for _, batch in self.provider.calls], [200, 120])
        self.assertEqual(tuple(s for _, batch in self.provider.calls for s in batch), symbols)
        self.current += timedelta(seconds=9)
        hit = await self.gateway.prices("US", symbols, max_age=10)
        self.assertEqual(hit.cache_hits, symbols)
        self.assertEqual(len(self.provider.calls), 2)
        self.current += timedelta(seconds=2)
        miss = await self.gateway.prices("US", ("P0",), max_age=10)
        self.assertEqual(miss.cache_hits, ())
        self.assertEqual(len(self.provider.calls), 3)

    async def test_historical_missing_cut_never_fetches_or_retroactively_fills(self) -> None:
        before = self.gateway.cut()
        await self.gateway.prices("US", ("A",))
        request = DataRequest("US", "price", ("A", "B"), max_age=10, as_of_cut=before)
        result = await self.gateway.request(request)
        self.assertEqual(result.missing, ("A", "B"))
        self.assertEqual(result.records, ())
        self.assertEqual(len(self.provider.calls), 1)
        current = self.gateway.cut()
        result = await self.gateway.request(
            DataRequest("US", "price", ("A", "B"), max_age=10, as_of_cut=current)
        )
        self.assertEqual(result.cache_hits, ("A",))
        self.assertEqual(result.missing, ("B",))
        self.assertEqual(len(self.provider.calls), 1)

    async def test_partial_payload_invalid_and_shared_rate_limit_are_distinct(self) -> None:
        self.provider.omit.add("ABSENT")
        self.provider.invalid.add("BROKEN")
        result = await self.gateway.prices("US", ("A", "ABSENT", "BROKEN"))
        self.assertEqual(tuple(row.symbol for row in result.records), ("A",))
        self.assertEqual(result.missing, ("ABSENT",))
        self.assertEqual(result.failures[0].code, "invalid_payload")
        self.provider.error = ApiError("Rate limited", status=429, retry_at=123.5)
        results = await asyncio.gather(
            self.gateway.prices("US", ("R",)), self.gateway.prices("US", ("R",))
        )
        for result in results:
            with self.assertRaises(ApiError) as failure:
                result.require("R")
            self.assertEqual(failure.exception.status, 429)
            self.assertEqual(failure.exception.retry_at, 123.5)
        self.assertEqual(len(self.provider.calls), 2)
        self.provider.error = None
        self.assertEqual((await self.gateway.prices("US", ("R",))).require("R").symbol, "R")

    async def test_first_fetch_cannot_satisfy_positive_age_with_old_source(self) -> None:
        self.provider.clock = lambda: (self.current - timedelta(seconds=15)).isoformat()
        result = await self.gateway.prices("US", ("A",), max_age=10)
        self.assertEqual(result.records, ())
        self.assertEqual(result.failures[0].code, "stale_market_data")
        historical_row = (await self.gateway.prices("US", ("A",))).require("A")
        self.assertEqual(historical_row.source_time, self.provider.clock())

    async def test_cancellation_does_not_cancel_shared_fetch(self) -> None:
        self.provider.release.clear()
        cancelled = asyncio.create_task(self.gateway.prices("US", ("A",)))
        try:
            self.assertTrue(await asyncio.to_thread(self.provider.started.wait, 2))
            survivor = asyncio.create_task(self.gateway.prices("US", ("A",)))
            cancelled.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await cancelled
        finally:
            self.provider.release.set()
        self.assertEqual((await survivor).require("A").symbol, "A")
        self.assertEqual(len(self.provider.calls), 1)

    async def test_unexpected_local_error_reaches_waiters_and_next_request_works(self) -> None:
        self.provider.error = RuntimeError("local control")
        results = await asyncio.gather(
            self.gateway.prices("US", ("A",)), self.gateway.prices("US", ("A",)),
            return_exceptions=True,
        )
        self.assertEqual([str(error) for error in results], ["local control", "local control"])
        self.assertEqual(len(self.provider.calls), 1)
        self.provider.error = None
        self.assertEqual((await self.gateway.prices("US", ("A",))).require("A").symbol, "A")

    async def test_abandoned_pipe_detaches_without_cancelling_shared_work(self) -> None:
        owner, peer = multiprocessing.Pipe()
        self.provider.release.clear()
        service = asyncio.create_task(self.gateway.serve(owner))
        try:
            peer.send(DataRequest("US", "price", ("A",)))
            self.assertTrue(await asyncio.to_thread(self.provider.started.wait, 2))
            survivor = asyncio.create_task(self.gateway.prices("US", ("A",)))
            peer.close()
            await asyncio.wait_for(service, 2)
            self.assertFalse(survivor.done())
        finally:
            peer.close()
            self.provider.release.set()
            await asyncio.gather(service, return_exceptions=True)
        self.assertEqual((await survivor).require("A").symbol, "A")
        self.assertEqual(len(self.provider.calls), 1)

    async def test_close_releases_idle_pipe_without_waiting_for_child(self) -> None:
        owner, peer = multiprocessing.Pipe()
        self.addCleanup(peer.close)
        service = asyncio.create_task(self.gateway.serve(owner))
        await asyncio.sleep(0)
        await asyncio.wait_for(self.gateway.close(), 2)
        await asyncio.wait_for(service, 2)
        result = await self.gateway.prices("US", ("A",))
        self.assertEqual(result.failures[0].code, "gateway_stopped")
        self.assertEqual(self.provider.calls, [])

    async def test_two_processes_share_one_inflight_provider_call(self) -> None:
        context = multiprocessing.get_context("spawn")
        endpoints = []
        self.provider.release.clear()
        arrivals = 0
        both_entered = asyncio.Event()
        actual_request = self.gateway.request

        async def track(request: DataRequest) -> DataResult:
            nonlocal arrivals
            arrivals += 1
            if arrivals == 2:
                both_entered.set()
            return await actual_request(request)

        with patch.object(self.gateway, "request", new=track):
            try:
                for _ in range(2):
                    owner, child = context.Pipe()
                    parent_control, child_control = context.Pipe()
                    process = context.Process(
                        target=child_request,
                        args=(child, child_control, DataRequest("US", "price", ("A",))),
                    )
                    process.start()
                    child.close()
                    child_control.close()
                    service = asyncio.create_task(self.gateway.serve(owner))
                    endpoints.append((process, parent_control, service))

                for _, channel, _ in endpoints:
                    self.assertTrue(await asyncio.to_thread(channel.poll, 8))
                    self.assertEqual(channel.recv(), "ready")
                    channel.send("go")
                self.assertTrue(await asyncio.to_thread(self.provider.started.wait, 2))
                await asyncio.wait_for(both_entered.wait(), 2)
                self.provider.release.set()
                for _, channel, _ in endpoints:
                    self.assertTrue(await asyncio.to_thread(channel.poll, 8))
                    self.assertEqual(channel.recv(), ("A",))
                self.assertEqual(len(self.provider.calls), 1)
            finally:
                self.provider.release.set()
                for process, channel, service in endpoints:
                    channel.close()
                    await asyncio.to_thread(process.join, 2)
                    if process.is_alive():
                        process.terminate()
                        await asyncio.to_thread(process.join, 2)
                    service.cancel()
                    await asyncio.gather(service, return_exceptions=True)
                    process.close()
