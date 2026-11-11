"""Exercise owner-side batching, failures, recovery and cross-process pipe clients."""

import asyncio
import json
import multiprocessing
import sqlite3
import tempfile
import unittest
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime, timedelta
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event
from typing import Any
from unittest.mock import patch

from papertrade.api import ApiError
from tools.market_data_gateway import DataResult, GatewayPipeClient, MarketDataGateway
from tools.market_data_repository import DataGroup, DataRequest, MarketDataRepository
from tools.runtime_audit import RuntimeAudit

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
        self.root = Path(directory.name)
        self.repo = MarketDataRepository(self.root / "data.sqlite3")
        self.addCleanup(self.repo.close)
        self.current = START
        self.provider = Provider(self.clock)
        self.gateway = MarketDataGateway(self.repo, self.provider, clock=self.clock)
        self.addAsyncCleanup(self.gateway.close)
        self.addCleanup(self.provider.release.set)

    def clock(self) -> str:
        return self.current.isoformat()

    def audited_gateway(self) -> tuple[MarketDataGateway, RuntimeAudit]:
        audit = RuntimeAudit(self.root / "audit.sqlite3", "fixture")
        gateway = MarketDataGateway(self.repo, self.provider, audit, clock=self.clock)
        self.addAsyncCleanup(gateway.close)
        return gateway, audit

    def stale_incidents(self, audit: RuntimeAudit) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(audit.path)) as db:
            db.row_factory = sqlite3.Row
            return db.execute("""
                SELECT context, severity, state, occurrences
                FROM runtime_incidents WHERE code = 'stale_market_data'
            """).fetchall()

    async def test_stream_ingress_writes_only_failure_and_recovery_audit(self) -> None:
        gateway, audit = self.audited_gateway()
        payload = {
            "currency": "USD", "timestamp": self.clock(),
            "asks": [{"price": "10", "volume": "1"}],
            "bids": [{"price": "9", "volume": "1"}],
        }
        with patch.object(audit, "_transaction", wraps=audit._transaction) as transaction:
            gateway.ingest_book("US", "A", payload, received_time=self.clock())
            transaction.assert_not_called()
            with (
                patch.object(self.repo, "publish", side_effect=sqlite3.OperationalError),
                self.assertRaises(sqlite3.OperationalError),
            ):
                gateway.ingest_book("US", "A", payload, received_time=self.clock())
            transaction.assert_called_once_with("record")

        resumed = RuntimeAudit(audit.path, audit.run_id)
        gateway = MarketDataGateway(self.repo, self.provider, resumed, clock=self.clock)
        self.addAsyncCleanup(gateway.close)
        with patch.object(resumed, "_transaction", wraps=resumed._transaction) as transaction:
            gateway.ingest_book("US", "A", payload, received_time=self.clock())
            transaction.assert_called_once_with("record")
            transaction.reset_mock()
            gateway.ingest_book("US", "A", payload, received_time=self.clock())
            transaction.assert_not_called()
        with closing(sqlite3.connect(audit.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT component, code, state, occurrences FROM runtime_incidents"
                ).fetchall(),
                [("market_data_store", "publish_failed", "resolved", 1)],
            )

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

    async def test_stale_responses_aggregate_and_only_fresh_fetch_recovers(self) -> None:
        gateway, audit = self.audited_gateway()
        self.provider.clock = lambda: (self.current - timedelta(seconds=20)).isoformat()
        for age in (10, 10.0):
            result = await gateway.prices("US", ("A",), max_age=age)
            self.assertEqual(result.records, ())
            self.assertEqual(result.failures[0].code, "stale_market_data")
        (incident,) = self.stale_incidents(audit)
        self.assertEqual(tuple(incident)[1:], ("warning", "open", 2))
        context = json.loads(incident["context"])
        self.assertEqual(set(context), {"market", "symbol", "request_id"})
        self.assertEqual((context["market"], context["symbol"]), ("US", "A"))
        self.assertRegex(context["request_id"], r"^[a-f0-9]{64}$")

        self.provider.clock = self.clock
        recovered = await gateway.prices("US", ("A",), max_age=10)
        self.assertEqual(recovered.require("A").source_time, self.clock())
        self.assertEqual(recovered.cache_hits, ())
        (resolved,) = self.stale_incidents(audit)
        self.assertEqual(tuple(resolved), (incident["context"], "warning", "resolved", 2))
        self.assertEqual(len(self.provider.calls), 3)

    async def test_shared_fetch_resolves_only_the_age_policy_it_satisfies(self) -> None:
        gateway, audit = self.audited_gateway()
        self.provider.clock = lambda: (self.current - timedelta(seconds=40)).isoformat()
        strict = DataRequest("US", "price", ("A",), max_age=10)
        relaxed = DataRequest("US", "price", ("A",), max_age=30)
        for request in (strict, relaxed):
            result = await gateway.request(request)
            self.assertEqual(result.failures[0].code, "stale_market_data")
        self.assertEqual(len(self.stale_incidents(audit)), 2)

        self.provider.clock = lambda: (self.current - timedelta(seconds=20)).isoformat()
        strict_result, relaxed_result = await asyncio.gather(
            gateway.request(strict), gateway.request(relaxed)
        )
        self.assertEqual(len(self.provider.calls), 3)
        self.assertEqual(strict_result.failures[0].code, "stale_market_data")
        self.assertEqual(relaxed_result.require("A").source_time, self.provider.clock())
        self.assertEqual(relaxed_result.cache_hits, ())
        relaxed_hit = await gateway.request(relaxed)
        self.assertEqual(relaxed_hit.cache_hits, ("A",))
        self.assertEqual(len(self.provider.calls), 3)
        self.assertEqual(
            {row["occurrences"]: row["state"] for row in self.stale_incidents(audit)},
            {2: "open", 1: "resolved"},
        )

    async def test_fresh_live_cache_hit_recovers_but_historical_hit_is_lookup_only(self) -> None:
        gateway, audit = self.audited_gateway()
        self.provider.clock = lambda: (self.current - timedelta(seconds=20)).isoformat()
        stale = await gateway.prices("US", ("A",), max_age=10)
        self.assertEqual(stale.failures[0].code, "stale_market_data")

        self.provider.clock = self.clock
        self.assertEqual((await gateway.prices("US", ("A",))).require("A").source_time, self.clock())
        historical = await gateway.request(
            DataRequest("US", "price", ("A",), max_age=10, as_of_cut=gateway.cut())
        )
        self.assertEqual(historical.cache_hits, ("A",))
        (open_incident,) = self.stale_incidents(audit)
        self.assertEqual(open_incident["state"], "open")

        live = await gateway.prices("US", ("A",), max_age=10.0)
        self.assertEqual(live.cache_hits, ("A",))
        self.assertEqual(live.require("A"), historical.require("A"))
        self.assertEqual(len(self.provider.calls), 2)
        (resolved,) = self.stale_incidents(audit)
        self.assertEqual(tuple(resolved), (open_incident["context"], "warning", "resolved", 1))

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
