"""Sanitized incident history with explicit recovery outcomes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import traceback
from typing import Any

from .clock import now_utc
from .core import SafetyError
from .jsonio import paper_json as encode

PRIVATE_KEYS = {"authorization", "access_token", "client_secret", "client_id", "api_key", "cookie"}


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: sanitize(item)
            for key, item in value.items()
            if str(key).lower() not in PRIVATE_KEYS
        }
    if isinstance(value, (list, tuple)):
        return [sanitize(item) for item in value]
    if isinstance(value, str):
        return re.sub(
            r"(?i)(bearer\s+\S+|tsc[ks]_[A-Za-z0-9_-]+|eyJ[A-Za-z0-9_.-]+)", "[redacted]", value
        )
    return value


class Audit:
    def __init__(self, store: Any) -> None:
        self.store = store

    def record(
        self,
        component: str,
        code: str,
        context: dict[str, Any],
        *,
        state: str = "retrying",
        retry_at: str | None = None,
        error: Exception | None = None,
    ) -> str:
        safe = sanitize(context)
        identity = {
            key: safe.get(key)
            for key in ("symbol", "market", "path", "group", "job", "role", "cash_id")
            if key in safe
        }
        key = hashlib.sha256(encode([component, code, identity]).encode()).hexdigest()[:24]
        event = {
            "incident_id": key,
            "component": component,
            "code": code,
            "state": state,
            "context": safe,
            "retry_at": retry_at,
            "at": now_utc().isoformat(),
        }
        if error is not None:
            event["exception_type"] = type(error).__name__
            event["trace"] = [
                f"{frame.filename.rsplit('/', 1)[-1]}:{frame.lineno}:{frame.name}"
                for frame in traceback.extract_tb(error.__traceback__)
            ]
        try:
            with self.store.transaction(write=True):
                self.store.db.execute(
                    "INSERT INTO incidents(id,component,code,state,first_at,last_at,occurrences,retry_at,data) "
                    "VALUES (?,?,?,?,?,?,1,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,"
                    "last_at=excluded.last_at,occurrences=incidents.occurrences+1,"
                    "retry_at=excluded.retry_at,data=excluded.data",
                    (
                        key,
                        component,
                        code,
                        state,
                        event["at"],
                        event["at"],
                        retry_at,
                        encode(event),
                    ),
                )
                self.store.event("incident", event)
        except sqlite3.Error:
            self.emergency(event)
        return key

    def recovered(self, component: str, **identity: Any) -> None:
        rows = self.store.db.execute(
            "SELECT id,data FROM incidents WHERE component=? AND state!='recovered'", (component,)
        ).fetchall()
        for row in rows:
            event = json.loads(row[1])
            if any(event["context"].get(key) != value for key, value in identity.items()):
                continue
            at = now_utc().isoformat()
            with self.store.transaction(write=True):
                updated = self.store.db.execute(
                    "UPDATE incidents SET state='recovered',last_at=?,retry_at=NULL WHERE id=? AND state!='recovered'",
                    (at, row[0]),
                )
                if updated.rowcount:
                    self.store.event(
                        "recovery", {"incident_id": row[0], "component": component, "at": at}
                    )

    def exception(self, component: str, error: Exception, **context: Any) -> str:
        if isinstance(error, SafetyError):
            context["reason"] = str(error)
        status = getattr(error, "status", None)
        if status is not None:
            context["http_status"] = status
        code = getattr(error, "code", None) or type(error).__name__
        retry_at = getattr(error, "retry_at", None)
        if isinstance(retry_at, (int, float)):
            from datetime import UTC, datetime

            retry_at = datetime.fromtimestamp(retry_at, UTC).isoformat()
        rejection = (
            isinstance(error, SafetyError)
            and component in ("candidate", "execution")
            and str(error).startswith(
                (
                    "Spread exceeds",
                    "Breakout failed",
                    "Requested whole shares exceed",
                    "Observed execution depth invalidates",
                    "Insufficient visible depth",
                    "Approval expired",
                )
            )
        )
        return self.record(
            component,
            str(code),
            context,
            retry_at=retry_at,
            error=error,
            state="rejected_observation"
            if rejection
            else "waiting_external"
            if getattr(error, "status", None) == 403
            else "retrying",
        )

    def emergency(self, event: dict[str, Any]) -> None:
        path = self.store.path.parent / "emergency-audit.jsonl"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "a") as stream:
            stream.write(encode(sanitize(event)) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def summary(self, open_only: bool = False) -> dict[str, Any]:
        query = (
            "SELECT * FROM incidents"
            + (" WHERE state NOT IN ('recovered','rejected_observation')" if open_only else "")
            + " ORDER BY last_at DESC"
        )
        return {
            "incidents": [
                {**dict(row), "data": json.loads(row["data"])}
                for row in self.store.db.execute(query)
            ],
            "workers": [
                {**dict(row), "data": json.loads(row["data"])}
                for row in self.store.db.execute("SELECT * FROM workers")
            ],
            "queue": dict(
                self.store.db.execute("SELECT status,COUNT(*) FROM jobs GROUP BY status")
            ),
        }

    def timeline(self, incident_id: str | None = None) -> list[dict[str, Any]]:
        events = []
        for row in self.store.db.execute("SELECT * FROM events ORDER BY id"):
            data = json.loads(row["data"])
            if incident_id and data.get("incident_id") != incident_id:
                continue
            events.append(
                {"id": row["id"], "at": row["at"], "kind": row["kind"], "data": sanitize(data)}
            )
        return events
