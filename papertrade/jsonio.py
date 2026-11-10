"""JSON byte formats and file replacement contracts for paper artifacts."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def _compact_json(
    value: object,
    *,
    ensure_ascii: bool,
    sort_keys: bool = False,
    allow_nan: bool = True,
    default: Callable[[Any], Any] | None = None,
) -> str:
    return json.dumps(
        value,
        ensure_ascii=ensure_ascii,
        sort_keys=sort_keys,
        allow_nan=allow_nan,
        default=default,
        separators=(",", ":"),
    )


def canonical_json(value: object) -> str:
    """Encode stable ASCII keys and reject non-finite numbers."""
    return _compact_json(value, ensure_ascii=True, sort_keys=True, allow_nan=False)


def compact_unicode_json(value: object) -> str:
    """Preserve insertion order and literal Unicode for raw download artifacts."""
    return _compact_json(value, ensure_ascii=False)


def recorded_json(value: object) -> str:
    """Preserve insertion order and Unicode while rejecting non-finite recorded values."""
    return _compact_json(value, ensure_ascii=False, allow_nan=False)


def archive_json(value: object) -> str:
    """Preserve the archive's sorted-key Unicode bytes."""
    return _compact_json(value, ensure_ascii=False, sort_keys=True)


def journal_json(value: object) -> str:
    """Preserve the event-journal digest and dataclass encoding contract."""
    return _compact_json(value, ensure_ascii=True, sort_keys=True, default=asdict)


def paper_json(value: object) -> str:
    """Encode Decimal amounts and datetime stamps without losing their text precision."""

    def default(item: object) -> str:
        if isinstance(item, Decimal):
            return str(item)
        if isinstance(item, datetime):
            return item.isoformat()
        raise TypeError(type(item).__name__)

    return _compact_json(value, ensure_ascii=False, default=default)


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise TypeError(f"Expected JSON object: {path.name}")
    return value


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    """Replace a private JSON file only after its complete contents are durable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def private_json(path: Path, data: dict[str, Any]) -> None:
    """Replace a mode-600 artifact using its existing no-follow temporary path."""
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(data, stream)
    os.replace(temporary, path)


def save_canonical_json(path: Path, value: object) -> None:
    """Replace a legacy report using its canonical bytes and trailing newline."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(canonical_json(value) + "\n")
    temporary.replace(path)
