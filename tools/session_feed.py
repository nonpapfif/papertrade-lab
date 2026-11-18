"""Read committed feed batches without taking the collector's writer lock."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.adaptive_journal import SCHEMA, JournalError, digest


class JournalFeed:
    def __init__(self, path: Path, market: str, end_epoch: float, capital: float) -> None:
        self.path = path.resolve()
        self.market = market
        self.end_epoch = end_epoch
        self.capital = capital
        self.index = 0
        self.identity = ""
        self.chain = ""
        self.state = ""
        self.caught_up = False

    def read(self, limit: int = 128, *, initializing: bool = False) -> list[dict[str, Any]]:
        """Authenticate one ordered batch and release the WAL snapshot before dispatch."""
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("BEGIN")
            if (
                initializing
                and not self.identity
                and not db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='header'"
                ).fetchone()
            ):
                return []
            header = db.execute("SELECT schema, identity, state, tail, hash FROM header").fetchall()
            if initializing and not self.identity and not header:
                return []
            if len(header) != 1:
                raise JournalError("Missing or ambiguous journal header")
            schema, identity, state, tail, anchor = header[0]
            contract = json.loads(identity)
            if (
                schema != SCHEMA
                or state not in {"complete", "active"}
                or contract.get("profile") != "broker-constrained"
                or contract["market"] != self.market
                or contract["end_epoch"] != self.end_epoch
                or contract["capital"] != self.capital
                or tail < self.index
                or (self.identity and self.identity != identity)
            ):
                raise JournalError("Feed journal does not match this paper session")
            previous = self.chain or digest(identity)
            rows = db.execute(
                "SELECT id, body, hash FROM events WHERE id > ? ORDER BY id LIMIT ?",
                (self.index, limit),
            ).fetchall()
        result = []
        index = self.index
        for key, body, stored in rows:
            previous = digest(previous + body)
            entry = json.loads(body)
            index += 1
            if index != key or key != entry.get("index") or previous != stored:
                raise JournalError("Shared observation chain changed")
            result.append(entry)
        if index > tail or (index == tail and previous != anchor) or (not rows and index < tail):
            raise JournalError("Shared observation tail mismatch")
        self.identity, self.state, self.index, self.chain = identity, state, index, previous
        self.caught_up = index == tail
        return result


def feed_event(entry: dict[str, Any], last_time: float) -> dict[str, Any] | None:
    """Translate only committed public inputs. Never consume another account's decisions."""
    kind = entry["kind"]
    if kind not in {
        "selection",
        "fx",
        "feature",
        "book",
        "clock",
        "recovery_gap",
        "final_advance",
        "session_close",
    }:
        return None
    received = entry.get("received") or entry["at"]
    stamp = datetime.fromisoformat(received)
    if stamp.utcoffset() is None:
        raise JournalError("Missing publication timezone")
    processed = datetime.fromisoformat(entry.get("at") or received)
    if processed.utcoffset() is None:
        raise JournalError("Missing processing timezone")
    at = datetime.fromtimestamp(
        max(stamp.timestamp(), processed.timestamp(), last_time), UTC
    ).isoformat()
    event = {
        "id": str(entry["index"]),
        "sequence": entry["index"],
        "received_at": at,
        "source_received_at": received,
    }
    payload: Any = entry.get("payload")
    if kind == "selection":
        return {**event, "kind": kind, "symbols": payload}
    if kind == "fx":
        return {**event, "kind": kind, "result": payload}
    if kind == "feature":
        symbol, bars = payload
        return {**event, "kind": "candles", "symbol": symbol, "result": {"candles": bars}}
    if kind == "book":
        symbol, book = payload
        return {**event, "kind": "orderbook", "symbol": symbol, "book": book}
    return {**event, "kind": "clock"}
