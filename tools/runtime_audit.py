"""Durable incidents and worker health for the shared paper runtime."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import operator
import os
import re
import sqlite3
import sys
import threading
import time
import traceback
import warnings
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock, RLock, Thread
from types import CodeType, FrameType
from typing import Any, Literal, Protocol, cast

from papertrade.audit import sanitize

RuntimeStatus = Literal["active", "idle", "waiting"]
IncidentState = Literal["open", "resolved"]
IncidentSeverity = Literal["warning", "error"]


class RuntimeAuditSink(Protocol):
    def operation(
        self,
        component: str,
        code: str,
        context: Mapping[str, object] | None = None,
        *,
        timeout: float | None = None,
        local_recovery: bool = False,
    ) -> AbstractContextManager[None]: ...

    def record(
        self,
        component: str,
        code: str,
        context: Mapping[str, object] | None = None,
        *,
        error: BaseException | None = None,
        severity: IncidentSeverity = "error",
        state: IncidentState = "open",
    ) -> str: ...

    def report_exception(
        self,
        component: str,
        code: str,
        error: BaseException,
        context: Mapping[str, object] | None = None,
        *,
        severity: IncidentSeverity = "error",
    ) -> None: ...

    def report_task_failures(
        self,
        component: str,
        code: str,
        results: list[object] | tuple[object, ...],
    ) -> None: ...


_diagnostics = threading.local()
_diagnostic_lock = Lock()
_diagnostic_owners: dict[int, tuple[tuple[int, asyncio.Task[Any] | None], int]] = {}

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
_CONTEXT_FIELDS = (
    *_IDENTITIES,
    "profile_id",
    "http_status",
    "queue_depth",
    "sequence",
    "origin",
    "line",
    "event_type",
)
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
    trace TEXT,
    severity TEXT NOT NULL DEFAULT 'error' CHECK (severity IN ('warning', 'error'))
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
        if isinstance(value, str) and key not in ("http_status", "queue_depth", "sequence", "line"):
            safe[key] = _label(value)
        elif type(value) is int and -(2**63) <= value < 2**63:
            safe[key] = value
    return safe


def _diagnostic_context(category: str, filename: str, lineno: int) -> dict[str, str | int]:
    category, origin = _label(category), _label(Path(filename).name)
    identity = hashlib.sha256(f"{category}:{origin}:{lineno}".encode()).hexdigest()[:24]
    return {"event_id": identity, "event_type": category, "origin": origin, "line": lineno}


def _importlib_warning_frame(frame: FrameType) -> bool:
    filename = frame.f_code.co_filename
    return "importlib" in filename and "_bootstrap" in filename


def _warning_frame_skipped(frame: FrameType, prefixes: tuple[str, ...]) -> bool:
    # CPython 3.12 checks prefixes before the last character, so a full filename does not skip.
    return _importlib_warning_frame(frame) or frame.f_code.co_filename.startswith(
        prefixes, 0, -1
    )


def _warning_frame(
    frame: FrameType | None, level: int, prefixes: tuple[str, ...], *, internal: bool
) -> FrameType | None:
    for _ in range(level - 1):
        if frame is None:
            break
        frame = frame.f_back
        while not internal and frame is not None and _warning_frame_skipped(frame, prefixes):
            frame = frame.f_back
    return frame


def _wrapped_warning(error: Warning, emitters: tuple[CodeType, CodeType], scope: object) -> bool:
    tb = error.__traceback__
    while tb is not None:
        if tb.tb_frame.f_code in emitters and tb.tb_frame.f_locals.get("audited_scope") is scope:
            return True
        tb = tb.tb_next
    return False


def _warning_escape_context(error: Warning) -> dict[str, str | int] | None:
    tb = error.__traceback__
    if tb is None:
        return None
    # Skip the context manager's yield and stop where a reused exception joins old frames.
    tb = tb.tb_next or tb
    while tb.tb_next is not None and tb.tb_next.tb_frame.f_back is tb.tb_frame:
        tb = tb.tb_next
    return _diagnostic_context(type(error).__name__, tb.tb_frame.f_code.co_filename, tb.tb_lineno)


@contextmanager
def _forward_diagnostic() -> Iterator[None]:
    forwarding = getattr(_diagnostics, "forwarding", False)
    _diagnostics.forwarding = True
    try:
        yield
    finally:
        _diagnostics.forwarding = forwarding


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
        self._publication_lock = RLock()
        self._health_lock = Lock()
        self._live_workers: dict[tuple[str, str], dict[str, Any]] = {}
        with self._transaction("initialize", initialize=True) as db:
            for statement in _SCHEMA.strip().rstrip(";").split(";"):
                db.execute(statement)
            columns = {row["name"] for row in db.execute("PRAGMA table_info(runtime_incidents)")}
            if "trace" not in columns:
                db.execute("ALTER TABLE runtime_incidents ADD COLUMN trace TEXT")
            if "severity" not in columns:
                db.execute(
                    "ALTER TABLE runtime_incidents ADD COLUMN severity TEXT NOT NULL"
                    " DEFAULT 'error' CHECK (severity IN ('warning', 'error'))"
                )
            self._open_incidents = {
                row["id"]
                for row in db.execute(
                    "SELECT id FROM runtime_incidents WHERE run_id=? AND state='open'",
                    (self.run_id,),
                )
            }

    @property
    def healthy(self) -> bool:
        return not self._storage_failed.is_set()

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        """Serialize synchronous publication with local audit transactions."""
        with self._publication_lock:
            if not self.healthy:
                raise RuntimeAuditError("Runtime audit storage is unhealthy")
            yield

    def _emergency(self, action: str, error: OSError | sqlite3.Error) -> None:
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
        self, action: str, *, initialize: bool = False, write: bool = True
    ) -> Iterator[sqlite3.Connection]:
        # Detect a storage failure before allowing the next publication.
        with self._publication_lock:
            try:
                path = self.path if write else self.path.absolute().as_uri() + "?mode=ro"
                with (
                    closing(
                        sqlite3.connect(path, timeout=self._busy_timeout, uri=not write)
                    ) as db,
                    db,
                ):
                    db.row_factory = sqlite3.Row
                    if (
                        initialize
                        and db.execute("PRAGMA journal_mode").fetchone()[0] != "wal"
                        and db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal"
                    ):
                        raise sqlite3.OperationalError("Could not enable WAL for runtime audit")
                    if write:
                        db.execute("BEGIN IMMEDIATE")
                    yield db
            except (sqlite3.Error, OSError) as error:
                self._storage_failed.set()
                failure = error
            else:
                return
        self._emergency(action, failure)
        raise RuntimeAuditError(f"Runtime audit storage failed during {action}") from None

    def _incident_id(self, component: str, code: str, context: dict[str, str | int]) -> str:
        identity = [context.get(key) for key in _IDENTITIES]
        return hashlib.sha256(
            json.dumps([self.run_id, component, code, identity]).encode()
        ).hexdigest()[:24]

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
        severity: IncidentSeverity = "error",
    ) -> str:
        key = self._incident_id(component, code, context)
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
                        context,exception_type,trace,severity)
                   VALUES (?,?,?,?,'open',?,?,1,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET state='open', last_at=excluded.last_at,
                       occurrences=runtime_incidents.occurrences
                                   + (runtime_incidents.state='resolved' OR ?),
                       context=excluded.context, exception_type=excluded.exception_type,
                       trace=excluded.trace,
                       severity=CASE
                           WHEN runtime_incidents.state='open' AND runtime_incidents.severity='error'
                           THEN 'error' ELSE excluded.severity END""",
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
                    severity,
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
        severity: IncidentSeverity = "error",
    ) -> str:
        if severity not in ("warning", "error"):
            raise ValueError("Unknown audit severity")
        with self._publication_lock:
            with self._transaction("record") as db:
                key = self._record(
                    db,
                    _label(component),
                    _label(code),
                    _safe_context(context),
                    self._clock(),
                    state,
                    error,
                    severity=severity,
                )
            if state == "open":
                self._open_incidents.add(key)
            else:
                self._open_incidents.discard(key)
        return key

    def recover(
        self,
        component: str,
        code: str,
        context: Mapping[str, object] | None = None,
        *,
        local: bool = False,
    ) -> None:
        """Resolve an open incident. Use local only for identities owned by this runtime."""
        key = self._incident_id(_label(component), _label(code), _safe_context(context))
        with self._publication_lock:
            if local:
                opened = key in self._open_incidents
            else:
                with self._transaction("read_recovery", write=False) as db:
                    opened = db.execute(
                        "SELECT 1 FROM runtime_incidents WHERE id=? AND state='open'", (key,),
                    ).fetchone() is not None
            if opened:
                self.record(component, code, context, state="resolved")

    def report_exception(
        self,
        component: str,
        code: str,
        error: BaseException,
        context: Mapping[str, object] | None = None,
        *,
        severity: IncidentSeverity = "error",
    ) -> None:
        """Record an existing failure while preserving the caller's exception."""
        try:
            self.record(component, code, context, error=error, severity=severity)
        except RuntimeAuditError:
            error.add_note("The runtime audit could not persist this failure.")

    def report_task_failures(
        self,
        component: str,
        code: str,
        results: list[object] | tuple[object, ...],
    ) -> None:
        for index, result in enumerate(results):
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                self.report_exception(component, code, result, {"job_id": index})

    @contextmanager
    def cleanup(self, component: str, code: str) -> Iterator[None]:
        """Audit cleanup failures without displacing an exception already in flight."""
        primary = sys.exception()
        try:
            yield
        except Exception as error:
            self.report_exception(component, code, error)
            if primary is None:
                raise
            primary.add_note(f"Runtime cleanup failed: {_label(component)}/{_label(code)}.")

    @contextmanager
    def operation(
        self,
        component: str,
        code: str,
        context: Mapping[str, object] | None = None,
        *,
        timeout: float | None = None,
        local_recovery: bool = False,
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
        try:
            if timeout is not None:
                self.heartbeat(
                    component,
                    worker_id=worker_id,
                    status="active",
                    timeout=timeout,
                    pid=os.getpid(),
                    durable=False,
                )
            yield
        except BaseException as error:
            if not isinstance(error, asyncio.CancelledError):
                self.report_exception(component, code, error, context)
            raise
        else:
            self.recover(component, code, context, local=local_recovery)
        finally:
            if timeout is not None:
                self.heartbeat(component, worker_id=worker_id, durable=False)
                with self._operation_lock:
                    self._operation_lanes[identity].remove(lane)

    @contextmanager
    def capture_runtime_failures(self, component: str) -> Iterator[None]:
        """Allow lexical nesting by one runtime owner per process."""
        pid = os.getpid()
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        owner = (threading.get_ident(), task)
        with _diagnostic_lock:
            existing = _diagnostic_owners.get(pid)
            if existing is not None and existing[0] != owner:
                raise RuntimeError("A different task owns this process diagnostic scope")
            depth = existing[1] + 1 if existing is not None else 1
            _diagnostic_owners[pid] = (owner, depth)
        if depth == 1:
            _diagnostics.warning_emitters = warnings.warn, warnings.warn_explicit
        scope = object()
        # A new scope is a new operation for warnings raised outside the hooked emitters.
        _diagnostics.last_warning_error = None
        try:
            with (
                self._capture_runtime_failures(component, scope) as emitters,
                warnings.catch_warnings(),
            ):
                # Keep first-match precedence, including nonfatal filters before error filters.
                filters = cast(list[tuple[Any, ...]], warnings.filters)
                filters[:] = [
                    entry if entry[0] == "error" else ("always", *entry[1:]) for entry in filters
                ]
                fallback: Literal["error", "always"] = (
                    "error" if getattr(warnings, "defaultaction", None) == "error" else "always"
                )
                warnings.simplefilter(fallback, append=True)
                try:
                    yield
                except Warning as error:
                    if _diagnostics.last_warning_error is not error:
                        _diagnostics.last_warning_error = error
                        if not _wrapped_warning(error, emitters, scope):
                            self.report_exception(
                                component, "python_warning", error, _warning_escape_context(error)
                            )
                    raise
        finally:
            with _diagnostic_lock:
                if depth == 1:
                    del _diagnostic_owners[pid]
                    del _diagnostics.last_warning_error
                    del _diagnostics.warning_emitters
                else:
                    _diagnostic_owners[pid] = (owner, depth - 1)

    @contextmanager
    def _capture_runtime_failures(
        self, component: str, scope: object
    ) -> Iterator[tuple[CodeType, CodeType]]:
        pid = os.getpid()
        previous_log_factory = logging.getLogRecordFactory()
        previous_warning = warnings.showwarning
        previous_warn, previous_explicit = warnings.warn, warnings.warn_explicit
        native_warn, native_explicit = _diagnostics.warning_emitters
        previous_thread = threading.excepthook
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        previous_async = loop.get_exception_handler() if loop is not None else None

        def warn(
            message: Warning | str,
            category: type[Warning] | None = None,
            stacklevel: int = 1,
            source: Any = None,
            *,
            skip_file_prefixes: tuple[str, ...] = (),
        ) -> None:
            stacklevel = operator.index(stacklevel)
            level = max(stacklevel, 2 if skip_file_prefixes else 1)
            caller = sys._getframe(1)
            valid_prefixes = isinstance(skip_file_prefixes, tuple) and all(
                isinstance(prefix, str) for prefix in skip_file_prefixes
            )
            internal = _importlib_warning_frame(caller)
            target = _warning_frame(
                caller, level, skip_file_prefixes if valid_prefixes else (), internal=internal
            )
            filename, lineno = (
                (target.f_code.co_filename, target.f_lineno) if target is not None else ("sys", 1)
            )
            valid_level = -sys.maxsize - 1 <= stacklevel <= sys.maxsize
            valid_category = (
                isinstance(message, Warning)
                or category is None
                or (isinstance(category, type) and issubclass(category, Warning))
            )
            try:
                if internal and valid_prefixes and valid_level and valid_category:
                    # CPython uses a raw frame count when warn is called directly from importlib.
                    # Forward that frame's module and registry without the wrapper's stack frame.
                    globals_ = target.f_globals if target is not None else vars(sys)
                    module = globals_.get("__name__", "<string>")
                    if module is not None and not isinstance(module, str):
                        module = "<string>"
                    native_explicit(
                        message, UserWarning if category is None else category, filename, lineno,
                        module=module, registry=globals_.setdefault("__warningregistry__", {}),
                        source=source,
                    )
                else:
                    # The native external-frame count can already skip our caller by prefix.
                    caller_skipped = valid_prefixes and _warning_frame_skipped(
                        caller, skip_file_prefixes
                    )
                    forwarded = (
                        min(level + (not caller_skipped), sys.maxsize) if valid_level else stacklevel
                    )
                    native_warn(
                        message, category, forwarded, source, skip_file_prefixes=skip_file_prefixes
                    )
            except Warning as error:
                audited_scope = scope if os.getpid() == pid and warnings.warn is warn else None
                if audited_scope is not None:
                    self.report_exception(
                        component, "python_warning", error,
                        _diagnostic_context(type(error).__name__, filename, lineno),
                    )
                raise

        def warn_explicit(
            message: Warning | str,
            category: type[Warning],
            filename: str,
            lineno: int,
            *args: Any,
            **kwargs: Any,
        ) -> None:
            try:
                native_explicit(message, category, filename, lineno, *args, **kwargs)
            except Warning as error:
                audited_scope = (
                    scope if os.getpid() == pid and warnings.warn_explicit is warn_explicit else None
                )
                if audited_scope is not None:
                    self.report_exception(
                        component, "python_warning", error,
                        _diagnostic_context(type(error).__name__, filename, lineno),
                    )
                raise

        def warning(
            message: Warning | str,
            category: type[Warning],
            filename: str,
            lineno: int,
            file: Any = None,
            line: str | None = None,
        ) -> None:
            if os.getpid() == pid and warnings.showwarning is warning:
                try:
                    self.record(
                        component,
                        "python_warning",
                        _diagnostic_context(category.__name__, filename, lineno),
                        severity="warning",
                    )
                except RuntimeAuditError:
                    pass
            with _forward_diagnostic():
                previous_warning(message, category, filename, lineno, file, line)

        def thread_failed(args: threading.ExceptHookArgs) -> None:
            if (
                os.getpid() == pid
                and threading.excepthook is thread_failed
                and isinstance(args.exc_value, BaseException)
                and not isinstance(args.exc_value, (SystemExit, asyncio.CancelledError))
            ):
                self.report_exception(component, "thread_unhandled", args.exc_value)
            with _forward_diagnostic():
                previous_thread(args)

        def task_failed(active: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
            if os.getpid() == pid and active.get_exception_handler() is task_failed:
                error = context.get("exception")
                if isinstance(error, BaseException):
                    if not isinstance(error, asyncio.CancelledError):
                        self.report_exception(component, "async_unhandled", error)
                else:
                    try:
                        self.record(component, "async_unhandled")
                    except RuntimeAuditError:
                        pass
            with _forward_diagnostic():
                if previous_async is None:
                    active.default_exception_handler(context)
                else:
                    previous_async(active, context)

        def log_record(*args: Any, **kwargs: Any) -> logging.LogRecord:
            record = previous_log_factory(*args, **kwargs)
            if (
                os.getpid() == pid
                and logging.getLogRecordFactory() is log_record
                and record.levelno >= logging.WARNING
                and not getattr(_diagnostics, "forwarding", False)
            ):
                severity: IncidentSeverity = (
                    "error" if record.levelno >= logging.ERROR else "warning"
                )
                error = record.exc_info[1] if record.exc_info is not None else None
                context = _diagnostic_context(record.name, record.pathname, record.lineno)
                if error is not None:
                    self.report_exception(
                        component,
                        "logging_diagnostic",
                        error,
                        context,
                        severity=severity,
                    )
                else:
                    try:
                        self.record(
                            component,
                            "logging_diagnostic",
                            context,
                            severity=severity,
                        )
                    except RuntimeAuditError:
                        pass
            return record

        logging.setLogRecordFactory(log_record)
        warnings.warn = warn
        warnings.warn_explicit = warn_explicit
        warnings.showwarning = warning
        threading.excepthook = thread_failed
        if loop is not None:
            loop.set_exception_handler(task_failed)
        try:
            yield warn.__code__, warn_explicit.__code__
        finally:
            if logging.getLogRecordFactory() is log_record:
                logging.setLogRecordFactory(previous_log_factory)
            if warnings.showwarning is warning:
                warnings.showwarning = previous_warning
            if warnings.warn is warn:
                warnings.warn = previous_warn
            if warnings.warn_explicit is warn_explicit:
                warnings.warn_explicit = previous_explicit
            if threading.excepthook is thread_failed:
                threading.excepthook = previous_thread
            if loop is not None and loop.get_exception_handler() is task_failed:
                loop.set_exception_handler(previous_async)

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
        durable: bool = True,
    ) -> None:
        """Refresh local liveness or register a worker across processes."""
        _validate_worker_state(
            status, timeout, progress, progress_timeout, oldest_pending_at, backlog_timeout, pid
        )
        if progress_timeout is None:
            progress_timeout = timeout
        timeout, progress_timeout, oldest_pending_at, backlog_timeout = (
            float(value) if value is not None else None
            for value in (timeout, progress_timeout, oldest_pending_at, backlog_timeout)
        )
        if not durable:
            key = _label(component), _label(worker_id)
            with self._health_lock:
                at = self._clock()
                previous = self._live_workers.get(key)
                progress_at = (
                    previous["progress_at"]
                    if previous is not None
                    and previous["status"] == status == "active"
                    and previous["progress"] == progress
                    else at
                )
                self._live_workers[key] = {
                    "component": key[0], "worker_id": key[1],
                    "status": status, "heartbeat_at": at, "timeout": timeout,
                    "progress": progress, "progress_at": progress_at,
                    "progress_timeout": progress_timeout, "oldest_pending_at": oldest_pending_at,
                    "backlog_timeout": backlog_timeout, "pid": pid,
                }
            return
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
            rows: dict[tuple[str, str], sqlite3.Row | dict[str, Any]] = {
                (row["component"], row["worker_id"]): row
                for row in db.execute(
                    "SELECT * FROM runtime_workers WHERE run_id=? ORDER BY component, worker_id",
                    (self.run_id,),
                )
            }
            with self._health_lock:
                rows.update(self._live_workers)
            open_incidents = {
                row["id"]
                for row in db.execute(
                    "SELECT id FROM runtime_incidents WHERE run_id=? AND state='open'",
                    (self.run_id,),
                )
            }
            for row in rows.values():
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
                if self._incident_id(component, "worker_state_invalid", context) in open_incidents:
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
                    if overdue or self._incident_id(component, code, context) in open_incidents:
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
