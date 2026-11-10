"""Durable incidents and worker health for the shared paper runtime."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import time
import traceback
from collections.abc import Callable, Iterator, Mapping
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Literal

from papertrade.audit import sanitize

RuntimeStatus = Literal["active", "idle", "waiting"]
IncidentState = Literal["open", "resolved"]

DISPATCH_TIMEOUT = 60.0
DISPATCH_BACKLOG_TIMEOUT = 10.0

_IDENTITIES = (
    "account_id",
    "request_id",
    "event_id",
    "cut_id",
    "job_id",
    "worker_id",
    "market",
    "symbol",
)
_CONTEXT_FIELDS = (*_IDENTITIES, "profile_id", "http_status", "queue_depth", "sequence")
_HEALTH_CODES = ("worker_dead", "worker_missing", "progress_stalled", "backlog_overdue")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runtime_incidents (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    component TEXT NOT NULL,
    code TEXT NOT NULL,
    state TEXT NOT NULL,
    first_at REAL NOT NULL,
    last_at REAL NOT NULL,
    occurrences INTEGER NOT NULL,
    context TEXT NOT NULL,
    exception_type TEXT,
    trace TEXT
);
CREATE TABLE IF NOT EXISTS runtime_workers (
    run_id TEXT NOT NULL,
    component TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL,
    heartbeat_at REAL NOT NULL,
    timeout REAL,
    progress INTEGER,
    progress_at REAL NOT NULL,
    progress_timeout REAL,
    oldest_pending_at REAL,
    backlog_timeout REAL,
    pid INTEGER,
    PRIMARY KEY (run_id, component, worker_id)
);
"""


class RuntimeAuditError(RuntimeError):
    """The audit transaction could not be persisted."""


@dataclass(frozen=True)
class RuntimeHealthIssue:
    component: str
    worker_id: str
    code: str


def _label(value: str) -> str:
    if value == "[redacted]":
        return value
    safe = sanitize(value)
    if safe == "[redacted]":
        return "[redacted]"
    if isinstance(safe, str) and re.fullmatch(r"[A-Za-z0-9_.:@/-]{1,128}", safe):
        return safe
    return "[redacted]"


def _safe_context(context: Mapping[str, object] | None) -> dict[str, str | int]:
    safe: dict[str, str | int] = {}
    for key in _CONTEXT_FIELDS:
        value = context.get(key) if context is not None else None
        if isinstance(value, str) and key not in ("http_status", "queue_depth", "sequence"):
            safe[key] = _label(value)
        elif type(value) is int and -(2**63) <= value < 2**63:
            safe[key] = value
    return safe


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and -sys.float_info.max <= value <= sys.float_info.max
    )


def _validate_worker_state(
    status: object,
    timeout: float | None,
    progress: object,
    progress_timeout: float | None,
    oldest_pending_at: float | None,
    backlog_timeout: float | None,
    pid: object,
) -> None:
    if status not in ("active", "idle", "waiting"):
        raise ValueError("Unknown worker status")
    if pid is not None and (type(pid) is not int or not 0 < pid < 2**31):
        raise ValueError("Worker PID must be a positive integer")
    if progress is not None and (type(progress) is not int or not -(2**63) <= progress < 2**63):
        raise ValueError("Worker progress must be a signed 64-bit integer")
    if status == "active" and timeout is None:
        raise ValueError("Active workers require a heartbeat timeout")
    if (oldest_pending_at is None) != (backlog_timeout is None):
        raise ValueError("Backlog timestamp and timeout must be supplied together")
    for duration in (timeout, progress_timeout, backlog_timeout):
        if duration is not None and (not _finite_number(duration) or duration <= 0):
            raise ValueError("Audit timeouts must be finite and positive")
    if oldest_pending_at is not None and not _finite_number(oldest_pending_at):
        raise ValueError("Backlog timestamp must be finite")


def _worker_dead(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


class RuntimeAudit:
    """Use one connection per transaction so threads and processes can share the file."""

    def __init__(
        self,
        path: str | Path,
        run_id: str,
        *,
        clock: Callable[[], float] = time.time,
        emergency: Callable[[str], None] | None = None,
        busy_timeout: float = 5.0,
    ) -> None:
        if not _finite_number(busy_timeout) or not 0.001 <= busy_timeout <= (2**31 - 1) / 1000:
            raise ValueError("SQLite busy timeout must fit within a positive 32-bit millisecond")
        self.path = Path(path)
        self._busy_timeout = busy_timeout
        self.run_id = _label(run_id)
        self._clock = clock
        self._operation_lock = Lock()
        self._operation_lanes: dict[tuple[str, str], set[int]] = {}
        self._emergency_sink = emergency
        self._storage_failed = Event()
        with self._transaction("initialize", initialize=True) as db:
            for statement in _SCHEMA.strip().rstrip(";").split(";"):
                db.execute(statement)

    @property
    def healthy(self) -> bool:
        return not self._storage_failed.is_set()

    def _emergency(self, action: str, error: OSError | sqlite3.Error) -> None:
        self._storage_failed.set()
        event = json.dumps(
            {
                "run_id": self.run_id,
                "component": "runtime_audit",
                "code": "audit_storage_failed",
                "action": action,
                "exception_type": _label(type(error).__name__),
                "at": self._clock(),
            }
        )
        try:
            if self._emergency_sink is not None:
                self._emergency_sink(event)
            else:
                path = self.path.with_suffix(self.path.suffix + ".emergency.jsonl")
                fd = os.open(path, os.O_APPEND | os.O_WRONLY | os.O_CREAT, 0o600)
                with os.fdopen(fd, "a", encoding="utf-8") as stream:
                    stream.write(event + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
        except Exception:  # noqa: BLE001 - A caller-supplied sink must not mask the audit failure.
            print(event, file=sys.stderr, flush=True)

    @contextmanager
    def _transaction(
        self, action: str, *, initialize: bool = False
    ) -> Iterator[sqlite3.Connection]:
        try:
            with closing(sqlite3.connect(self.path, timeout=self._busy_timeout)) as db, db:
                db.row_factory = sqlite3.Row
                if (
                    initialize
                    and db.execute("PRAGMA journal_mode").fetchone()[0] != "wal"
                    and db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal"
                ):
                    raise sqlite3.OperationalError("Could not enable WAL for runtime audit")
                db.execute("BEGIN IMMEDIATE")
                yield db
        except (sqlite3.Error, OSError) as error:
            self._emergency(action, error)
            raise RuntimeAuditError(f"Runtime audit storage failed during {action}") from None

    def _record(
        self,
        db: sqlite3.Connection,
        component: str,
        code: str,
        context: dict[str, str | int],
        at: float,
        state: IncidentState,
        error: BaseException | None = None,
        *,
        aggregate: bool = True,
    ) -> str:
        identity = [context.get(key) for key in _IDENTITIES]
        key = hashlib.sha256(
            json.dumps([self.run_id, component, code, identity]).encode()
        ).hexdigest()[:24]
        if state == "resolved":
            db.execute(
                """UPDATE runtime_incidents SET state='resolved', last_at=?
                   WHERE id=? AND state='open'""",
                (at, key),
            )
        else:
            trace = None
            if error is not None:
                trace = json.dumps(
                    [
                        [_label(frame.name), frame.lineno]
                        for frame in traceback.extract_tb(error.__traceback__, limit=-10)
                    ]
                )
            db.execute(
                """INSERT INTO runtime_incidents
                       (id,run_id,component,code,state,first_at,last_at,occurrences,
                        context,exception_type,trace)
                   VALUES (?,?,?,?,'open',?,?,1,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET state='open', last_at=excluded.last_at,
                       occurrences=runtime_incidents.occurrences
                                   + (runtime_incidents.state='resolved' OR ?),
                       context=excluded.context, exception_type=excluded.exception_type,
                       trace=excluded.trace""",
                (
                    key,
                    self.run_id,
                    component,
                    code,
                    at,
                    at,
                    json.dumps(context),
                    _label(type(error).__name__) if error is not None else None,
                    trace,
                    aggregate,
                ),
            )
        return key

    def record(
        self,
        component: str,
        code: str,
        context: Mapping[str, object] | None = None,
        *,
        state: IncidentState = "open",
        error: BaseException | None = None,
    ) -> str:
        with self._transaction("record") as db:
            return self._record(
                db,
                _label(component),
                _label(code),
                _safe_context(context),
                self._clock(),
                state,
                error,
            )

    @contextmanager
    def operation(
        self,
        component: str,
        code: str,
        context: Mapping[str, object] | None = None,
        *,
        timeout: float | None = None,
    ) -> Iterator[None]:
        identity = (component, code)
        lane = 0
        worker_id = ""
        if timeout is not None:
            with self._operation_lock:
                lanes = self._operation_lanes.setdefault(identity, set())
                lane = next(index for index in range(len(lanes) + 1) if index not in lanes)
                lanes.add(lane)
            label = _label(code)
            suffix = hashlib.sha256(label.encode()).hexdigest()[:8]
            worker_id = f"{label[:48]}-{suffix}-{os.getpid()}-{id(self):x}-{lane}"
        original_error = None
        try:
            if timeout is not None:
                self.heartbeat(
                    component,
                    worker_id=worker_id,
                    status="active",
                    timeout=timeout,
                    pid=os.getpid(),
                )
            yield
        except BaseException as error:
            original_error = error
            try:
                self.record(component, code, context, error=error)
            except RuntimeAuditError:
                error.add_note("The runtime audit could not persist this failure.")
            raise
        else:
            self.record(component, code, context, state="resolved")
        finally:
            if timeout is not None:
                try:
                    self.heartbeat(component, worker_id=worker_id, status="idle")
                except RuntimeAuditError:
                    if original_error is None:
                        raise
                    original_error.add_note("The runtime audit could not clear operation health.")
                finally:
                    with self._operation_lock:
                        self._operation_lanes[identity].remove(lane)

    @contextmanager
    def watching(self, *, interval: float = 1.0) -> Iterator[None]:
        """Scan from an independent thread until the owning runtime exits."""
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("Watch interval must be finite and positive")
        stop = Event()
        thread = Thread(
            target=self.watch,
            args=(stop,),
            kwargs={"interval": interval},
            name=f"paper-audit-{self.run_id}",
            daemon=True,
        )
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join()

    def heartbeat(
        self,
        component: str,
        *,
        worker_id: str = "main",
        status: RuntimeStatus = "idle",
        timeout: float | None = None,
        progress: int | None = None,
        progress_timeout: float | None = None,
        oldest_pending_at: float | None = None,
        backlog_timeout: float | None = None,
        pid: int | None = None,
    ) -> None:
        """Refresh liveness. During active work, advance progress to reset its deadline."""
        _validate_worker_state(
            status, timeout, progress, progress_timeout, oldest_pending_at, backlog_timeout, pid
        )
        if progress_timeout is None:
            progress_timeout = timeout
        timeout, progress_timeout, oldest_pending_at, backlog_timeout = (
            float(value) if value is not None else None
            for value in (timeout, progress_timeout, oldest_pending_at, backlog_timeout)
        )
        with self._transaction("heartbeat") as db:
            at = self._clock()
            db.execute(
                """INSERT INTO runtime_workers
                       (run_id,component,worker_id,status,heartbeat_at,timeout,
                        progress,progress_at,progress_timeout,oldest_pending_at,backlog_timeout,pid)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(run_id,component,worker_id) DO UPDATE SET
                       progress_at=CASE
                           WHEN excluded.status != 'active' OR runtime_workers.status != 'active'
                                OR excluded.progress IS NOT runtime_workers.progress
                           THEN excluded.progress_at ELSE runtime_workers.progress_at END,
                       status=excluded.status, heartbeat_at=excluded.heartbeat_at,
                       timeout=excluded.timeout, progress=excluded.progress,
                       progress_timeout=excluded.progress_timeout,
                       oldest_pending_at=excluded.oldest_pending_at,
                       backlog_timeout=excluded.backlog_timeout, pid=excluded.pid""",
                (
                    self.run_id,
                    _label(component),
                    _label(worker_id),
                    status,
                    at,
                    timeout,
                    progress,
                    at,
                    progress_timeout,
                    oldest_pending_at,
                    backlog_timeout,
                    pid,
                ),
            )

    def scan_health(self) -> tuple[RuntimeHealthIssue, ...]:
        """Persist watchdog findings and resolve prior findings when the condition clears."""
        issues = []
        with self._transaction("scan_health") as db:
            at = self._clock()
            rows = db.execute(
                "SELECT * FROM runtime_workers WHERE run_id=? ORDER BY component, worker_id",
                (self.run_id,),
            ).fetchall()
            for row in rows:
                component, worker_id = row["component"], row["worker_id"]
                context: dict[str, str | int] = {
                    "worker_id": _label(worker_id) if isinstance(worker_id, str) else "[redacted]"
                }
                safe_component = _label(component) if isinstance(component, str) else "[redacted]"
                try:
                    _validate_worker_state(
                        row["status"],
                        row["timeout"],
                        row["progress"],
                        row["progress_timeout"],
                        row["oldest_pending_at"],
                        row["backlog_timeout"],
                        row["pid"],
                    )
                    if not (
                        _finite_number(row["heartbeat_at"])
                        and _finite_number(row["progress_at"])
                        and (row["status"] != "active" or row["progress_timeout"] is not None)
                        and component == safe_component
                        and worker_id == context["worker_id"]
                    ):
                        raise ValueError("Invalid persisted worker state")
                except ValueError as error:
                    self._record(
                        db,
                        safe_component,
                        "worker_state_invalid",
                        context,
                        at,
                        "open",
                        error,
                        aggregate=False,
                    )
                    issues.append(
                        RuntimeHealthIssue(
                            safe_component, str(context["worker_id"]), "worker_state_invalid"
                        )
                    )
                    continue
                self._record(db, component, "worker_state_invalid", context, at, "resolved")
                dead = _worker_dead(row["pid"])
                missing = (
                    not dead
                    and row["timeout"] is not None
                    and at - row["heartbeat_at"] >= row["timeout"]
                )
                stalled = (
                    row["status"] == "active" and at - row["progress_at"] >= row["progress_timeout"]
                )
                backlog = (
                    row["oldest_pending_at"] is not None
                    and at - row["oldest_pending_at"] >= row["backlog_timeout"]
                )
                for code, overdue in zip(
                    _HEALTH_CODES, (dead, missing, stalled, backlog), strict=True
                ):
                    if overdue:
                        issues.append(RuntimeHealthIssue(component, worker_id, code))
                    self._record(
                        db,
                        component,
                        code,
                        context,
                        at,
                        "open" if overdue else "resolved",
                        aggregate=False,
                    )
        return tuple(issues)

    def watch(self, stop_event: Event, *, interval: float = 1.0) -> None:
        """Run health scans in a supervisor thread or process until explicitly stopped."""
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("Watch interval must be finite and positive")
        while not stop_event.is_set():
            try:
                self.scan_health()
            except RuntimeAuditError:
                # The emergency sink and healthy flag expose each failed scan.
                pass
            stop_event.wait(interval)
