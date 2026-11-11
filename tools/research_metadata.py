"""Read and write the SQLite metadata table used by historical research tools."""

import json
import sqlite3
from collections.abc import Callable
from typing import Any


def read_metadata(db: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def write_metadata(
    db: sqlite3.Connection, key: str, value: object, *, encode: Callable[[object], str]
) -> None:
    db.execute(
        "INSERT INTO metadata VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, encode(value)),
    )
