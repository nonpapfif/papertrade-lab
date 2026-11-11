"""Offline failure and isolation checks for shared runtime audit."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import cast
from unittest.mock import patch

from tools.runtime_audit import RuntimeAudit, RuntimeAuditError, RuntimeHealthIssue, RuntimeStatus


class RuntimeAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "audit.sqlite"
        self.at = 100.0
        self.emergency: list[str] = []
        self.audit = RuntimeAudit(
            self.path, "run-a", clock=self.clock, emergency=self.emergency.append, busy_timeout=0.05
        )

    def clock(self) -> float:
        return self.at

    def rows(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory = sqlite3.Row
            return db.execute("SELECT * FROM runtime_incidents ORDER BY first_at").fetchall()

    def test_operation_preserves_exception_and_resolves_matching_incident(self) -> None:
        error = RuntimeError("private-message")
        context = {"request_id": "q1", "account_id": "paper-a"}
        for _ in range(2):
            with (
                self.assertRaises(RuntimeError) as result,
                self.audit.operation("execution", "projection_failed", context),
            ):
                raise error
            self.assertIs(result.exception, error)
        incident = self.rows()[0]
        self.assertEqual(incident["occurrences"], 2)
        self.assertEqual(incident["exception_type"], "RuntimeError")
        self.assertTrue(json.loads(incident["trace"]))
        self.assertNotIn("private-message", repr(tuple(incident)))
        with self.audit.operation("execution", "projection_failed", {"request_id": "q2"}):
            pass
        self.assertEqual(self.rows()[0]["state"], "open")
        self.at += 1
        with self.audit.operation("execution", "projection_failed", context):
            pass
        incident = self.rows()[0]
        self.assertEqual((incident["state"], incident["last_at"]), ("resolved", self.at))
        self.assertEqual(incident["occurrences"], 2)

    def test_redacts_secret_context_and_error_text(self) -> None:
        self.audit.record(
            "gateway",
            "failure",
            {
                "request_id": "Bearer request-secret",
                "account_id": "tsck_tokenvalue",
                "authorization": "authorization-secret",
                "headers": {"Cookie": "header-secret"},
                "symbol": {"token": "nested-secret"},
                "event_id": "event-1",
            },
            error=RuntimeError("arbitrary-secret"),
        )
        incident = self.rows()[0]
        self.assertEqual(
            json.loads(incident["context"]),
            {"account_id": "[redacted]", "request_id": "[redacted]", "event_id": "event-1"},
        )
        self.assertNotIn("secret", repr(tuple(incident)))

    def test_noop_recovery_skips_writer_lock_and_sees_other_client_incidents(self) -> None:
        other = RuntimeAudit(self.path, "run-a", clock=self.clock)
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            key = self.audit.record("gateway", "failure", state="resolved")
        self.assertTrue(self.audit.healthy)
        self.assertEqual(self.rows(), [])

        self.assertEqual(other.record("gateway", "failure"), key)
        self.at += 1
        self.assertEqual(self.audit.record("gateway", "failure", state="resolved"), key)
        self.assertEqual(
            tuple(self.rows()[0][column] for column in ("state", "last_at", "occurrences")),
            ("resolved", self.at, 1),
        )

        self.assertEqual(other.record("gateway", "failure"), key)
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            with self.assertRaises(RuntimeAuditError):
                self.audit.record("gateway", "failure", state="resolved")
        self.assertFalse(self.audit.healthy)
        self.assertEqual(self.rows()[0]["state"], "open")

    def test_completion_batch_rolls_back_worker_updates_and_recovery_on_storage_error(self) -> None:
        self.audit.record("journal", "append_failed")
        for component in ("ordered_dispatch", "journal"):
            self.audit.heartbeat(component, status="active", timeout=10, progress=4)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("""
                CREATE TRIGGER reject_journal_completion
                BEFORE INSERT ON runtime_workers
                WHEN NEW.component='journal' AND NEW.status='idle'
                BEGIN SELECT RAISE(ABORT, 'fixture completion write failure'); END
            """)

        with self.assertRaisesRegex(RuntimeAuditError, "idle_workers"):
            self.audit.idle_workers(
                ("ordered_dispatch", "journal"),
                progress=5,
                recoveries=(("journal", "append_failed", None),),
            )
        self.assertEqual(self.rows()[0]["state"], "open")
        with closing(sqlite3.connect(self.path)) as db:
            rows = db.execute("SELECT status,progress FROM runtime_workers").fetchall()
        self.assertEqual(rows, [("active", 4), ("active", 4)])
        self.assertEqual(json.loads(self.emergency[0])["action"], "idle_workers")
        with (
            self.assertRaisesRegex(RuntimeAuditError, "storage is unhealthy"),
            self.audit.publication_guard(),
        ):
            self.fail("An incomplete audit batch allowed publication")

    def test_watchdog_distinguishes_idle_stall_death_and_backlog(self) -> None:
        self.audit.heartbeat("coordinator", status="waiting")
        self.audit.heartbeat("selector", status="idle")
        self.audit.heartbeat("quotes", status="active", timeout=10, progress=1)
        self.audit.heartbeat("routing", worker_id="queue", oldest_pending_at=100, backlog_timeout=5)
        self.audit.heartbeat("projection", pid=10000, timeout=20)
        with patch("tools.runtime_audit.os.kill") as probe:
            self.assertEqual(self.audit.scan_health(), ())
            self.at = 109
            self.audit.heartbeat("quotes", status="active", timeout=10, progress=1)
            self.at = 110
            probe.side_effect = ProcessLookupError
            expected = {
                RuntimeHealthIssue("projection", "main", "worker_dead"),
                RuntimeHealthIssue("quotes", "main", "progress_stalled"),
                RuntimeHealthIssue("routing", "queue", "backlog_overdue"),
            }
            self.assertEqual(set(self.audit.scan_health()), expected)
            self.assertEqual(set(self.audit.scan_health()), expected)
        self.assertEqual({row["occurrences"] for row in self.rows()}, {1})
        self.audit.heartbeat("quotes", status="active", timeout=10, progress=2)
        self.audit.heartbeat("routing", worker_id="queue")
        self.audit.heartbeat("projection")
        self.assertEqual(self.audit.scan_health(), ())
        self.assertEqual({row["state"] for row in self.rows()}, {"resolved"})
        self.at = 121
        self.assertIn(
            RuntimeHealthIssue("quotes", "main", "worker_missing"),
            self.audit.scan_health(),
        )

    def test_storage_failure_surfaces_and_does_not_mask_original_error(self) -> None:
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("BEGIN IMMEDIATE")
            with self.assertRaises(RuntimeAuditError):
                self.audit.scan_health()
            error = ValueError("arbitrary-secret")
            with self.assertRaises(ValueError) as result, self.audit.operation(
                "gateway", "fetch_failed"
            ):
                raise error
            self.assertIs(result.exception, error)
        self.assertFalse(self.audit.healthy)
        self.assertEqual(len(self.emergency), 2)
        for event in self.emergency:
            self.assertEqual(json.loads(event)["code"], "audit_storage_failed")
            self.assertNotIn("arbitrary-secret", event)
        self.assertEqual(self.audit.scan_health(), ())
        self.assertFalse(self.audit.healthy)

    def test_parallel_clients_preserve_counts_and_run_and_account_identity(self) -> None:
        other = RuntimeAudit(self.path, "run-b", clock=self.clock)

        def record(index: int) -> str:
            client = self.audit if index % 2 else other
            return client.record(
                "gateway", "failure", {"account_id": "paper-1", "request_id": "r1"}
            )

        with ThreadPoolExecutor(max_workers=4) as pool:
            ids = list(pool.map(record, range(20)))
        self.assertEqual(len(set(ids)), 2)
        self.assertEqual({row["occurrences"] for row in self.rows()}, {10})
        account_id = self.audit.record(
            "gateway", "failure", {"account_id": "paper-2", "request_id": "r1"}
        )
        self.assertNotIn(account_id, ids)


    def test_rejects_invalid_heartbeat_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown worker status"):
            self.audit.heartbeat("worker", status=cast(RuntimeStatus, "typo"))
        for progress in (True, 1.5, 2**63):
            with self.subTest(progress=progress), self.assertRaisesRegex(ValueError, "progress"):
                self.audit.heartbeat("worker", progress=cast(int, progress))
        with self.assertRaisesRegex(ValueError, "PID"):
            self.audit.heartbeat("worker", pid=True)

    def test_scan_reports_malformed_worker_and_keeps_checking_others(self) -> None:
        self.audit.heartbeat("broken", status="active", timeout=10)
        self.audit.heartbeat("live", status="active", timeout=10)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(
                "UPDATE runtime_workers SET progress_timeout='malformed' WHERE component=?",
                ("broken",),
            )
        self.at += 11
        issues = self.audit.scan_health()
        self.assertIn(RuntimeHealthIssue("broken", "main", "worker_state_invalid"), issues)
        self.assertIn(RuntimeHealthIssue("live", "main", "worker_missing"), issues)
        self.audit.heartbeat("broken")
        self.assertNotIn(
            RuntimeHealthIssue("broken", "main", "worker_state_invalid"), self.audit.scan_health()
        )
        invalid = next(row for row in self.rows() if row["code"] == "worker_state_invalid")
        self.assertEqual(invalid["state"], "resolved")

    def test_wal_reader_and_brief_writer_contention(self) -> None:
        writer = RuntimeAudit(self.path, "run-a")
        started = threading.Event()

        def record() -> str:
            started.set()
            return writer.record("runtime", "contention")

        with closing(sqlite3.connect(self.path)) as reader, ThreadPoolExecutor(1) as pool:
            self.assertEqual(reader.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            reader.execute("BEGIN")
            reader.execute("SELECT count(*) FROM runtime_incidents").fetchone()
            with closing(sqlite3.connect(self.path)) as locker:
                locker.execute("BEGIN IMMEDIATE")
                future = pool.submit(record)
                self.assertTrue(started.wait(1))
                self.assertFalse(future.done())
            self.assertIsInstance(future.result(timeout=2), str)
        self.assertTrue(writer.healthy)
