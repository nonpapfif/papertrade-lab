"""Exercise severity, scoped diagnostics and rejected input audit evidence."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import tempfile
import threading
import unittest
import warnings
from contextlib import closing
from multiprocessing.connection import Pipe
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from papertrade.api import ApiError, MarketClient
from tools.market_data_adapter import MarketDataAdapter
from tools.market_data_gateway import MarketDataGateway
from tools.market_data_repository import DataRequest, MarketDataRepository
from tools.run_async_live_validation import Runtime
from tools.runtime_audit import RuntimeAudit

AT = "2026-11-10T15:00:00+00:00"


class RuntimeDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.audit = RuntimeAudit(self.root / "audit.sqlite3", "fixture")

    def incidents(self) -> list[dict[str, Any]]:
        with closing(sqlite3.connect(self.audit.path)) as db:
            db.row_factory = sqlite3.Row
            return [
                dict(row) for row in db.execute("SELECT * FROM runtime_incidents ORDER BY rowid")
            ]

    def test_legacy_schema_preserves_identity_and_severity_can_escalate(self) -> None:
        context = {"symbol": "A"}
        original = self.audit.record("features", "invalid", context)
        with closing(sqlite3.connect(self.audit.path)) as db, db:
            db.execute("ALTER TABLE runtime_incidents DROP COLUMN severity")
        migrated = RuntimeAudit(self.audit.path, "fixture")
        self.assertEqual(self.incidents()[0]["severity"], "error")
        migrated.record("features", "invalid", context, severity="warning")
        self.assertEqual(self.incidents()[0]["severity"], "error")
        migrated.record("features", "invalid", context, state="resolved")
        self.assertEqual(self.incidents()[0]["state"], "resolved")
        self.assertEqual(
            migrated.record("features", "invalid", context, severity="warning"),
            original,
        )
        self.assertEqual(self.incidents()[0]["severity"], "warning")
        migrated.record("features", "invalid", context)
        self.assertEqual(self.incidents()[0]["severity"], "error")

    def test_supervisor_uses_shared_diagnostics_before_initialization(self) -> None:
        from tools.autotest_supervisor import main

        output = self.root / "supervisor"

        def fail_initialization(*args: Any, **kwargs: Any) -> None:
            warnings.warn("fixture startup warning", UserWarning, stacklevel=1)
            raise ValueError("fixture startup failure")

        with (
            patch(
                "sys.argv",
                [
                    "supervisor",
                    "--repo",
                    str(self.root),
                    "--output",
                    str(output),
                    "--inherited",
                    str(self.root),
                    "--codex",
                    "/fixture/codex",
                ],
            ),
            patch("tools.autotest_supervisor.Supervisor", side_effect=fail_initialization),
            patch("warnings.showwarning"),
            self.assertRaisesRegex(ValueError, "fixture startup failure"),
        ):
            main()
        with closing(sqlite3.connect(output / "runtime-audit.sqlite3")) as db:
            rows = db.execute("SELECT component,code FROM runtime_incidents").fetchall()
        self.assertEqual(
            set(rows),
            {
                ("engineering_handoff", "python_warning"),
                ("engineering_handoff", "supervisor_failed"),
            },
        )
        self.assertFalse(
            any(
                thread.name == "paper-audit-engineering-supervisor"
                for thread in threading.enumerate()
            )
        )

    async def test_scoped_hooks_keep_diagnostics_and_restore_existing_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        prior_loop = loop.get_exception_handler()
        prior_factory = logging.getLogRecordFactory()
        seen: list[str] = []

        def existing_async(_loop: asyncio.AbstractEventLoop, _context: dict[str, Any]) -> None:
            seen.append("async")

        def fail() -> None:
            raise RuntimeError("private message")

        log = logging.getLogger("paper-fixture")
        loop.set_exception_handler(existing_async)
        self.addCleanup(loop.set_exception_handler, prior_loop)
        with (
            patch("warnings.showwarning") as existing_warning,
            patch("threading.excepthook") as existing_thread,
            warnings.catch_warnings(),
            patch.object(log, "propagate", False),
            patch.object(log, "handlers", [logging.NullHandler()]),
        ):
            warnings.simplefilter("ignore", DeprecationWarning)
            with self.audit.capture_runtime_failures("outer"):
                with self.audit.capture_runtime_failures("inner"):
                    for _ in range(2):
                        warnings.warn("private warning", DeprecationWarning, stacklevel=1)
                        log.warning("private log")
                    warnings.warn("private second origin", UserWarning, stacklevel=1)
                    log.warning("private second log")
                    loop.call_exception_handler({"exception": RuntimeError("private async")})
                    thread = threading.Thread(target=fail)
                    thread.start()
                    thread.join(timeout=3)
                    self.assertFalse(thread.is_alive())
                    with patch("tools.runtime_audit.os.getpid", return_value=os.getpid() + 100):
                        warnings.warn("off-process", UserWarning, stacklevel=1)
                self.assertIsNot(loop.get_exception_handler(), existing_async)
                warnings.warn("outer scope", UserWarning, stacklevel=1)
            warnings.warn("restored ignored warning", DeprecationWarning, stacklevel=1)
        self.assertIs(loop.get_exception_handler(), existing_async)
        self.assertIs(logging.getLogRecordFactory(), prior_factory)
        self.assertEqual(existing_warning.call_count, 5)
        self.assertEqual(existing_thread.call_count, 1)
        self.assertEqual(seen, ["async"])
        rows = self.incidents()
        inner_warnings = [
            row for row in rows if row["component"] == "inner" and row["severity"] == "warning"
        ]
        self.assertEqual(sorted(row["occurrences"] for row in inner_warnings), [1, 1, 2, 2])
        self.assertEqual(len({json.loads(row["context"])["line"] for row in inner_warnings}), 4)
        self.assertEqual(
            {row["code"] for row in rows if row["severity"] == "error"},
            {
                "async_unhandled",
                "thread_unhandled",
            },
        )
        self.assertEqual(sum(row["component"] == "outer" for row in rows), 1)
        self.assertNotIn("private", repr(rows))

    def test_error_filter_before_ignore_audits_and_propagates_original_warning(self) -> None:
        warning = RuntimeWarning("private warning-as-error detail")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            warnings.filterwarnings(
                "error", message="private warning-as-error", category=RuntimeWarning
            )
            original_filters = list(warnings.filters)
            with (
                self.assertRaises(RuntimeWarning) as raised,
                self.audit.capture_runtime_failures("outer"),
                self.audit.capture_runtime_failures("inner"),
            ):
                warnings.warn(warning, stacklevel=1)
            self.assertIs(raised.exception, warning)
            self.assertEqual(warnings.filters, original_filters)
        (row,) = self.incidents()
        self.assertEqual(
            (row["component"], row["code"], row["severity"], row["exception_type"]),
            ("inner", "python_warning", "error", "RuntimeWarning"),
        )
        self.assertEqual(json.loads(row["context"])["origin"], Path(__file__).name)
        self.assertNotIn("private warning-as-error detail", repr(row))

    def test_ignore_filter_before_error_keeps_warning_nonfatal_and_restores_filter(self) -> None:
        with warnings.catch_warnings(), patch("warnings.showwarning") as display:
            warnings.simplefilter("error", RuntimeWarning)
            warnings.filterwarnings("ignore", message="private ignore", category=RuntimeWarning)
            original_filters = list(warnings.filters)
            with self.audit.capture_runtime_failures("runner"):
                warnings.warn("private ignore detail", RuntimeWarning, stacklevel=1)
            self.assertEqual(warnings.filters, original_filters)
            warnings.warn("private ignore detail", RuntimeWarning, stacklevel=1)
            display.assert_called_once()
        (row,) = self.incidents()
        self.assertEqual((row["code"], row["severity"]), ("python_warning", "warning"))
        self.assertEqual(json.loads(row["context"])["event_type"], "RuntimeWarning")
        self.assertNotIn("private ignore detail", repr(row))

    def test_warning_default_error_is_preserved_when_no_filter_matches(self) -> None:
        with warnings.catch_warnings(), patch("warnings.defaultaction", "error"):
            warnings.resetwarnings()
            with (
                self.assertRaises(RuntimeWarning),
                self.audit.capture_runtime_failures("runner"),
            ):
                warnings.warn("private default detail", RuntimeWarning, stacklevel=1)
            self.assertEqual(list(warnings.filters), [])
        (row,) = self.incidents()
        self.assertEqual((row["severity"], row["exception_type"]), ("error", "RuntimeWarning"))
        self.assertNotIn("private default detail", repr(row))

    def test_reusing_warning_in_separate_operations_records_each_occurrence(self) -> None:
        warning = RuntimeWarning("private repeated detail")
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            with self.audit.capture_runtime_failures("outer"):
                for _ in range(2):
                    with (
                        self.assertRaises(RuntimeWarning),
                        self.audit.capture_runtime_failures("inner"),
                    ):
                        warnings.warn(warning, stacklevel=1)
        (row,) = self.incidents()
        self.assertEqual((row["component"], row["occurrences"]), ("inner", 2))
        self.assertNotIn("private repeated detail", repr(row))

    async def test_other_task_cannot_take_process_diagnostic_ownership(self) -> None:
        async def attempt() -> None:
            with (
                self.assertRaisesRegex(RuntimeError, "different task"),
                self.audit.capture_runtime_failures("second"),
            ):
                self.fail("concurrent scope replaced the owner")

        with self.audit.capture_runtime_failures("first"):
            await asyncio.create_task(attempt())
        with self.audit.capture_runtime_failures("second"):
            pass

    def test_cleanup_records_secondary_and_preserves_primary(self) -> None:
        error = ValueError("primary")
        with self.assertRaises(ValueError) as raised:
            try:
                raise error
            finally:
                with self.audit.cleanup("campaign_runner", "cleanup_failed"):
                    raise OSError("cleanup detail")
        self.assertIs(raised.exception, error)
        self.assertEqual(self.incidents()[0]["exception_type"], "OSError")
        self.assertNotIn("cleanup detail", repr(self.incidents()))

    async def test_invalid_pipe_and_request_failures_are_audited_but_normal_eof_is_not(
        self,
    ) -> None:
        repository = MarketDataRepository(self.root / "market.sqlite3")
        self.addCleanup(repository.close)
        gateway = MarketDataGateway(
            repository,
            MarketDataAdapter(cast(MarketClient, object())),
            self.audit,
        )
        self.addAsyncCleanup(gateway.close)
        parent, child = Pipe()
        self.addCleanup(child.close)
        service = asyncio.create_task(gateway.serve(parent))
        child.send({"untrusted": "payload"})
        with self.assertRaisesRegex(TypeError, "Invalid pipe request"):
            await asyncio.wait_for(service, 3)

        bad_request = DataRequest("US", "price", ("A",))
        object.__setattr__(bad_request, "market", "invalid")
        result = await gateway._remote_request(bad_request)
        self.assertEqual(result.failures[0].code, "invalid_request")
        with patch.object(gateway, "request", side_effect=ValueError("internal detail")):
            result = await gateway._remote_request(DataRequest("US", "price", ("A",)))
        self.assertEqual(result.failures[0].code, "gateway_failure")

        parent, child = Pipe()
        child.close()
        await asyncio.wait_for(gateway.serve(parent), 3)
        self.assertEqual(
            {row["code"] for row in self.incidents()},
            {
                "pipe_protocol_failed",
                "pipe_request_invalid",
                "pipe_request_failed",
            },
        )
        self.assertNotIn("internal detail", repr(self.incidents()))

    async def test_provider_warning_recovers_on_same_request(self) -> None:
        class FixtureClient:
            blocked = True

            def prices(self, symbols: list[str]) -> list[dict[str, Any]]:
                if self.blocked:
                    raise ApiError("private provider detail", status=429, code="cooldown")
                return [
                    {"symbol": s, "currency": "USD", "timestamp": AT, "lastPrice": 10}
                    for s in symbols
                ]

        client = FixtureClient()
        repository = MarketDataRepository(self.root / "market.sqlite3")
        self.addCleanup(repository.close)
        gateway = MarketDataGateway(
            repository,
            MarketDataAdapter(cast(MarketClient, client), self.audit),
            self.audit,
            clock=lambda: AT,
            batch_window=0,
        )
        self.addAsyncCleanup(gateway.close)
        failed = await gateway.prices("US", ["A"])
        self.assertEqual(failed.failures[0].code, "cooldown")
        warning = next(row for row in self.incidents() if row["code"] == "cooldown")
        self.assertEqual((warning["state"], warning["severity"]), ("open", "warning"))
        client.blocked = False
        recovered = await gateway.prices("US", ["A"])
        self.assertEqual(recovered.require("A").payload["lastPrice"], 10)
        warning = next(row for row in self.incidents() if row["code"] == "cooldown")
        self.assertEqual((warning["state"], warning["severity"]), ("resolved", "warning"))
        self.assertNotIn("private provider detail", repr(self.incidents()))

    async def test_bad_fx_and_book_warning_recover_on_valid_observation(self) -> None:
        runtime = Runtime(100, lambda *_a, **_k: None, audit=self.audit)
        self.addAsyncCleanup(runtime.close)

        async def apply(sequence: int, kind: str, payload: Any) -> None:
            await runtime._consume_event(
                {
                    "sequence": sequence,
                    "kind": kind,
                    "payload": payload,
                    "received": AT,
                    "at": AT,
                },
                0,
            )

        await apply(1, "fx", {"midRate": "bad"})
        await apply(2, "book", ("A", {"timestamp": "bad"}))
        self.assertEqual(
            {row["code"]: (row["state"], row["severity"]) for row in self.incidents()},
            {"invalid_fx": ("open", "warning"), "invalid_source_age": ("open", "warning")},
        )
        await apply(3, "fx", {"midRate": 1400})
        await apply(
            4,
            "book",
            (
                "A",
                {
                    "timestamp": AT,
                    "currency": "USD",
                    "bids": [{"price": "9.99", "volume": "100"}],
                    "asks": [{"price": "10", "volume": "100"}],
                },
            ),
        )
        self.assertTrue(all(row["state"] == "resolved" for row in self.incidents()))
