"""Durable, ordered replay inputs for an adaptive paired paper campaign."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

from papertrade.jsonio import journal_json as encode

SCHEMA = 1


def _made_progress(event: dict[str, Any]) -> bool:
    return bool(
        event.get("submissions")
        or event.get("applied")
        or event.get("expired")
        or event.get("disposition") == "applied"
        or any(output.get("progress") for output in event.get("outputs", {}).values())
    )


class JournalError(RuntimeError):
    """The journal cannot safely be used for recovery."""


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _header(db: sqlite3.Connection) -> tuple[str, str, int, int, str]:
    rows = db.execute("SELECT schema, identity, state, tail, cursor, hash FROM header").fetchall()
    if len(rows) != 1 or rows[0][0] != SCHEMA or rows[0][2] not in ("active", "complete"):
        raise JournalError("Unsupported journal header")
    _, identity, state, tail, cursor, chain = rows[0]
    return identity, state, tail, cursor, chain


def _entries(
    db: sqlite3.Connection, prefix: dict[str, Any] | None = None
) -> Iterator[dict[str, Any]]:
    identity, _, tail, cursor, anchor = _header(db)
    chain = digest(identity)
    index = progress = 0
    prefix_records = prefix["records"] if prefix is not None else -1
    if prefix_records > tail:
        raise JournalError("Journal is shorter than its required prefix")
    if prefix_records == 0 and prefix != {
        "records": 0, "cursor": 0, "chain_sha256": chain
    }:
        raise JournalError("Journal prefix does not match")
    rows = db.execute("SELECT id, body, hash FROM events ORDER BY id")
    for index, (stored_id, body, stored) in enumerate(rows, 1):
        event = json.loads(body)
        chain = digest(chain + body)
        if index != stored_id or index != event.get("index") or chain != stored:
            raise JournalError("Journal hash chain mismatch")
        progress += _made_progress(event)
        if index == prefix_records and prefix != {
            "records": index, "cursor": progress, "chain_sha256": chain
        }:
            raise JournalError("Journal prefix does not match")
        yield event
    if (index, progress, chain) != (tail, cursor, anchor):
        raise JournalError("Journal anchor mismatch")


def read_status(path: Path) -> dict[str, Any]:
    """Verify the complete committed chain in one read-only snapshot."""
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("BEGIN")
            identity, state, tail, cursor, chain = _header(db)
            for _ in _entries(db):
                pass
            return {
                "identity": json.loads(identity),
                "state": state,
                "cursor": cursor,
                "hash": chain,
                "records": tail,
            }
    except (sqlite3.Error, ValueError, TypeError, KeyError, AttributeError) as error:
        raise JournalError("Cannot read journal") from error


class AdaptiveJournal:
    def __init__(self, db: sqlite3.Connection, identity: dict[str, Any]) -> None:
        self.db = db
        self.identity = identity

    @classmethod
    @contextmanager
    def read_active(cls, path: Path) -> Iterator[AdaptiveJournal]:
        """Read an active journal in one consistent snapshot without taking ownership."""
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
                db.execute("BEGIN")
                identity, state, _, _, _ = _header(db)
                if state != "active":
                    raise JournalError("Handoff journal is no longer active")
                yield cls(db, json.loads(identity))
        except (sqlite3.Error, OSError, ValueError) as error:
            raise JournalError("Cannot read active journal") from error

    @classmethod
    @contextmanager
    def read_completed(cls, path: Path, chain: str) -> Iterator[AdaptiveJournal]:
        """Read a sealed predecessor in one consistent database snapshot."""
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
                db.execute("BEGIN")
                identity, state, _, _, anchor = _header(db)
                if state != "complete" or anchor != chain:
                    raise JournalError("Predecessor is incomplete or has a different hash")
                yield cls(db, json.loads(identity))
        except (sqlite3.Error, OSError, ValueError) as error:
            raise JournalError("Cannot read completed predecessor") from error

    @classmethod
    @contextmanager
    def open(
        cls, path: Path, identity: dict[str, Any], *, resume: bool
    ) -> Iterator[AdaptiveJournal]:
        try:
            with path.with_suffix(path.suffix + ".lock").open("a") as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise JournalError("Journal already has a worker") from error
                if not resume:
                    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                    os.close(fd)
                uri = path.resolve().as_uri() + "?mode=rw"
                with closing(sqlite3.connect(uri, uri=True)) as db:
                    db.execute("PRAGMA synchronous=FULL")
                    if not resume:
                        if db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                            raise JournalError("SQLite WAL mode unavailable")
                        header = encode(identity)
                        with db:
                            db.execute(
                                "CREATE TABLE header (schema INTEGER, identity TEXT, state TEXT,"
                                " tail INTEGER, cursor INTEGER, hash TEXT)"
                            )
                            db.execute(
                                "CREATE TABLE events (id INTEGER PRIMARY KEY, body TEXT, hash TEXT)"
                            )
                            db.execute(
                                "INSERT INTO header VALUES (?, ?, 'active', 0, 0, ?)",
                                (SCHEMA, header, digest(header)),
                            )
                    stored, state, _, _, _ = _header(db)
                    if stored != encode(identity) or state != "active":
                        raise JournalError("Journal identity or state does not permit execution")
                    yield cls(db, identity)
        except (sqlite3.Error, OSError) as error:
            raise JournalError("Cannot open or update journal") from error

    def evidence(self) -> dict[str, Any]:
        _, _, tail, cursor, chain = _header(self.db)
        return {"records": tail, "cursor": cursor, "chain_sha256": chain}

    def events(self) -> Iterator[dict[str, Any]]:
        yield from self.verified_events()

    def verified_events(self, prefix: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        try:
            yield from _entries(self.db, prefix)
        except (sqlite3.Error, ValueError, TypeError, KeyError, AttributeError) as error:
            raise JournalError("Invalid journal entry") from error

    def append(self, event: dict[str, Any]) -> None:
        _, state, tail, cursor, chain = _header(self.db)
        if state != "active":
            raise JournalError("Cannot append to a completed journal")
        body = encode({**event, "index": tail + 1})
        chain = digest(chain + body)
        progress = cursor + _made_progress(event)
        try:
            with self.db:
                self.db.execute("INSERT INTO events VALUES (?, ?, ?)", (tail + 1, body, chain))
                self.db.execute(
                    "UPDATE header SET tail=?, cursor=?, hash=?", (tail + 1, progress, chain)
                )
        except sqlite3.Error as error:
            raise JournalError("Cannot commit paired observation") from error

    def complete(self) -> None:
        with self.db:
            self.db.execute("UPDATE header SET state='complete'")
