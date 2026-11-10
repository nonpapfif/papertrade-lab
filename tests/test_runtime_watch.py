"""Exercise watchdog detection while the finite runner or market data path is blocked."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from threading import Event
from typing import Any, cast
from unittest.mock import patch

from papertrade.api import MarketClient
from papertrade.core import Settings
from tools.market_data_adapter import MarketDataAdapter
from tools.market_data_gateway import MarketDataGateway
from tools.market_data_repository import DataRequest, MarketDataRepository
from tools.run_async_live_validation import campaign
from tools.runtime_audit import RuntimeAudit, RuntimeHealthIssue

STAMP = "2026-11-10T15:00:00+00:00"


class RuntimeWatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name)

    def monitored_audit(
        self,
        *components: str,
    ) -> tuple[RuntimeAudit, Event, Event, list[RuntimeHealthIssue]]:
        blocked, observed = Event(), Event()
        audit = RuntimeAudit(
            self.path / "monitor.sqlite3",
            "synthetic",
            clock=lambda: 1_000.0 if blocked.is_set() else 0.0,
        )
        issues: list[RuntimeHealthIssue] = []
        scan = audit.scan_health

        def detect() -> tuple[RuntimeHealthIssue, ...]:
            current = scan()
            if {row.component for row in current if row.code == "progress_stalled"} >= set(
                components
            ):
                issues.extend(current)
                observed.set()
            return current

        self.enterContext(patch.object(audit, "scan_health", side_effect=detect))
        return audit, blocked, observed, issues

    def test_nested_operations_have_independent_health(self) -> None:
        audit, blocked, observed, issues = self.monitored_audit("features")
        with (
            audit.watching(interval=0.01),
            audit.operation("features", "compute_failed", timeout=1),
            audit.operation("features", "compute_failed", timeout=2_000),
        ):
            blocked.set()
            self.assertTrue(observed.wait(timeout=3))
        stalled = {row.worker_id for row in issues if row.code == "progress_stalled"}
        self.assertEqual(len(stalled), 1)
        self.assertEqual(audit.scan_health(), ())

    def test_completed_operations_do_not_keep_dead_process_identities_on_resume(self) -> None:
        audit = RuntimeAudit(self.path / "resume.sqlite3", "same-run")
        with audit.operation("features", "compute_failed", timeout=1):
            pass
        resumed = RuntimeAudit(audit.path, audit.run_id)
        with patch("tools.runtime_audit._worker_dead", side_effect=lambda pid: pid is not None):
            self.assertEqual(resumed.scan_health(), ())

    async def test_gateway_and_provider_hang_reported_on_same_watch(self) -> None:
        audit, blocked, observed, issues = self.monitored_audit(
            "market_data_gateway",
            "provider_adapter",
        )

        class ProviderClient:
            def prices(self, symbols: list[str]) -> list[dict[str, Any]]:
                blocked.set()
                if not observed.wait(timeout=3):
                    raise RuntimeError("watch did not report blocked gateway and provider")
                return [
                    {"symbol": symbol, "currency": "USD", "timestamp": STAMP, "lastPrice": 10}
                    for symbol in symbols
                ]

        repo = MarketDataRepository(self.path / "market.sqlite3")
        self.addCleanup(repo.close)
        adapter = MarketDataAdapter(cast(MarketClient, ProviderClient()), audit)
        gateway = MarketDataGateway(repo, adapter, audit, clock=lambda: STAMP, batch_window=0)
        self.addAsyncCleanup(gateway.close)
        with audit.watching(interval=0.01):
            result = await gateway.prices("US", ["A"])
        self.assertEqual(result.require("A").payload["lastPrice"], 10)
        self.assertEqual(
            {row.component for row in issues if row.code == "progress_stalled"},
            {"market_data_gateway", "provider_adapter"},
        )
        self.assertEqual(audit.scan_health(), ())

    async def test_store_hang_reported_even_when_event_loop_is_blocked(self) -> None:
        audit, blocked, observed, issues = self.monitored_audit("market_data_store")
        repo = MarketDataRepository(self.path / "market.sqlite3")
        self.addCleanup(repo.close)
        adapter = MarketDataAdapter(cast(MarketClient, object()), audit)
        gateway = MarketDataGateway(repo, adapter, audit, clock=lambda: STAMP)
        self.addAsyncCleanup(gateway.close)

        def lookup(*args: Any, **kwargs: Any) -> dict[str, Any]:
            blocked.set()
            self.assertTrue(observed.wait(timeout=3))
            return {}

        with patch.object(repo, "lookup", side_effect=lookup), audit.watching(interval=0.01):
            self.assertEqual(gateway.cached(DataRequest("US", "price", ("A",), max_age=10)), {})
        self.assertIn("market_data_store", {row.component for row in issues})
        self.assertEqual(audit.scan_health(), ())

    async def test_finite_campaign_starts_and_joins_its_own_watch_on_failure(self) -> None:
        started, stopped = Event(), Event()
        audit_paths = []
        watch = RuntimeAudit.watch

        def watched(audit: RuntimeAudit, stop: Event, *, interval: float) -> None:
            audit_paths.append(audit.path)
            started.set()
            try:
                watch(audit, stop, interval=interval)
            finally:
                stopped.set()

        def fail_settings(*_args: Any) -> Settings:
            self.assertTrue(started.wait(timeout=3))
            raise OSError("synthetic settings failure before market data")

        output = self.path / "campaign"
        with (
            patch.object(RuntimeAudit, "watch", watched),
            patch.object(Settings, "load", side_effect=fail_settings),
            self.assertRaisesRegex(SystemExit, "Live validation failed"),
        ):
            await campaign(output, 1, 100)
        self.assertTrue(stopped.is_set())
        self.assertEqual(audit_paths, [output / "runtime-audit.sqlite3"])
        self.assertEqual(json.loads((output / "status.json").read_text())["state"], "failed")
