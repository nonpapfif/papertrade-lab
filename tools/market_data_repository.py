"""SQLite observation history and indexed latest market-data views."""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from papertrade.api import SYMBOL
from papertrade.clock import utc_iso
from papertrade.jsonio import canonical_json

DataKind = Literal["price", "book", "candles"]
type Parameters = tuple[tuple[str, str | int], ...]
type DataGroup = tuple[str, DataKind, Parameters]


def epoch(value: str) -> float:
    if not isinstance(value, str):
        raise TypeError("Market timestamps must be text")
    at = datetime.fromisoformat(value)
    if at.utcoffset() is None:
        raise ValueError("Market timestamps must include a timezone")
    return at.timestamp()


def finite_json(value: object) -> str:
    def validate(item: object) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise TypeError("Market data keys must be strings")
                validate(child)
        elif isinstance(item, list):
            for child in item:
                validate(child)
        elif item is not None and not isinstance(item, (str, int, float, bool)):
            raise ValueError("Market data must contain JSON values")

    validate(value)
    return canonical_json(value)


@dataclass(frozen=True, slots=True)
class DataCut:
    sequence: int
    at: str

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence < 0:
            raise ValueError("Invalid market-data cut sequence")
        epoch(self.at)


@dataclass(frozen=True, slots=True)
class DataRequest:
    market: str
    kind: DataKind
    symbols: tuple[str, ...]
    parameters: Parameters = ()
    max_age: float = 0
    as_of_cut: DataCut | None = None

    def __post_init__(self) -> None:
        if self.market not in ("KR", "US") or self.kind not in ("price", "book", "candles"):
            raise ValueError("Invalid market-data request group")
        if (
            not isinstance(self.symbols, tuple)
            or not self.symbols
            or any(
                not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol)
                for symbol in self.symbols
            )
        ):
            raise ValueError("Invalid market-data request symbols")
        if len(self.symbols) != len(set(self.symbols)):
            raise ValueError("Market-data request symbols must be unique")
        if type(self.max_age) not in (int, float) or not math.isfinite(self.max_age):
            raise ValueError("Market-data maximum age must be finite")
        if self.max_age < 0:
            raise ValueError("Market-data maximum age must be nonnegative")
        if self.as_of_cut is not None and not isinstance(self.as_of_cut, DataCut):
            raise ValueError("Invalid market-data cut")
        if self.kind == "candles":
            if (
                not isinstance(self.parameters, tuple)
                or len(self.parameters) != 2
                or any(not isinstance(item, tuple) or len(item) != 2 for item in self.parameters)
            ):
                raise ValueError("Invalid candle parameters")
            params = dict(self.parameters)
            before, count = params.get("before"), params.get("count")
            if (
                params.keys() != {"before", "count"}
                or not isinstance(before, str)
                or type(count) is not int
                or not 1 <= count <= 200
            ):
                raise ValueError("Invalid candle parameters")
            epoch(before)
            object.__setattr__(self, "parameters", (("before", before), ("count", count)))
        elif self.parameters != ():
            raise ValueError("Unexpected market-data parameters")

    @property
    def group(self) -> DataGroup:
        return self.market, self.kind, self.parameters


@dataclass(frozen=True, slots=True)
class DataRecord:
    market: str
    kind: DataKind
    symbol: str
    parameters: Parameters
    source_time: str
    received_time: str
    published_sequence: int
    source_identity: str
    _payload_json: str

    @property
    def payload(self) -> dict[str, Any]:
        decoded: dict[str, Any] = json.loads(self._payload_json)
        return decoded


@dataclass(frozen=True, slots=True)
class Observation:
    symbol: str
    source_time: str
    received_time: str
    source_identity: str
    payload: dict[str, Any]


SCHEMA = """
CREATE TABLE IF NOT EXISTS market_data_history (
    sequence INTEGER PRIMARY KEY,
    market TEXT NOT NULL CHECK (market IN ('KR', 'US')),
    kind TEXT NOT NULL CHECK (kind IN ('price', 'book', 'candles')),
    parameters TEXT NOT NULL CHECK (json_valid(parameters)),
    symbol TEXT NOT NULL,
    source_time TEXT NOT NULL,
    received_time TEXT NOT NULL,
    source_epoch REAL NOT NULL,
    received_epoch REAL NOT NULL CHECK (source_epoch <= received_epoch),
    source_identity TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload))
) STRICT;
CREATE INDEX IF NOT EXISTS market_data_history_key
    ON market_data_history (market, kind, parameters, symbol, sequence);
CREATE TABLE IF NOT EXISTS market_data_latest (
    market TEXT NOT NULL,
    kind TEXT NOT NULL,
    parameters TEXT NOT NULL,
    symbol TEXT NOT NULL,
    sequence INTEGER NOT NULL REFERENCES market_data_history(sequence),
    source_epoch REAL NOT NULL,
    received_epoch REAL NOT NULL,
    PRIMARY KEY (market, kind, parameters, symbol)
) STRICT;
"""

LATEST_QUERY = """
SELECT h.symbol, h.source_time, h.received_time, h.sequence, h.source_identity, h.payload
FROM json_each(?) AS requested
CROSS JOIN market_data_latest AS latest
    ON latest.market = ? AND latest.kind = ? AND latest.parameters = ?
    AND latest.symbol = requested.value
JOIN market_data_history AS h ON h.sequence = latest.sequence
WHERE latest.received_epoch BETWEEN ? AND ? AND latest.source_epoch >= ?
"""

HISTORY_QUERY = """
SELECT symbol, source_time, received_time, sequence, source_identity, payload
FROM (
    SELECT h.*, row_number() OVER (
        PARTITION BY h.symbol
        ORDER BY h.source_epoch DESC, h.received_epoch DESC, h.sequence DESC
    ) AS rank
    FROM json_each(?) AS requested
    CROSS JOIN market_data_history AS h
        ON h.market = ? AND h.kind = ? AND h.parameters = ? AND h.symbol = requested.value
    WHERE h.received_epoch BETWEEN ? AND ? AND h.source_epoch >= ? AND h.sequence <= ?
)
WHERE rank = 1
"""


class MarketDataRepository:
    """Persist provider data only while committing the result of a completed call."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        self._db.close()

    def cut(self, at: str | None = None) -> DataCut:
        (sequence,) = self._db.execute(
            "SELECT coalesce(max(sequence), 0) FROM market_data_history"
        ).fetchone()
        return DataCut(sequence, at if at is not None else utc_iso())

    def lookup(self, request: DataRequest, *, now: str) -> dict[str, DataRecord]:
        cut = request.as_of_cut
        end = epoch(cut.at if cut else now)
        start = end - request.max_age
        # An old candle series can still be a fresh retrieval for its fixed query parameters.
        source_start = start if request.kind != "candles" else -math.inf
        if cut is not None and cut.sequence > self.cut(now).sequence:
            raise ValueError("Market-data cut sequence is outside the repository")
        query = HISTORY_QUERY if cut else LATEST_QUERY
        args = (
            json.dumps(request.symbols),
            request.market,
            request.kind,
            json.dumps(request.parameters),
            start,
            end,
            source_start,
        )
        rows = self._db.execute(query, (*args, cut.sequence) if cut else args)
        return {
            row[0]: DataRecord(request.market, request.kind, row[0], request.parameters, *row[1:])
            for row in rows
        }

    def publish(
        self, group: DataGroup, observations: Sequence[Observation]
    ) -> dict[str, DataRecord]:
        market, kind, parameters = group
        if not observations:
            return {}
        request = DataRequest(market, kind, tuple(row.symbol for row in observations), parameters)
        parameters = request.parameters
        prepared = []
        for row in observations:
            source, received = epoch(row.source_time), epoch(row.received_time)
            if source > received:
                raise ValueError("Market-data source time follows receipt time")
            if not isinstance(row.source_identity, str) or not row.source_identity.strip():
                raise ValueError("Market-data source identity is required")
            if not isinstance(row.payload, dict):
                raise TypeError("Market-data payload must be an object")
            prepared.append((row, source, received, finite_json(row.payload)))
        result = {}
        encoded_parameters = json.dumps(parameters)
        with self._db:
            for row, source, received, payload in prepared:
                key = (market, kind, encoded_parameters, row.symbol)
                cursor = self._db.execute(
                    """
                    INSERT INTO market_data_history
                    (market, kind, parameters, symbol, source_time, received_time,
                     source_epoch, received_epoch, source_identity, payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        *key,
                        row.source_time,
                        row.received_time,
                        source,
                        received,
                        row.source_identity,
                        payload,
                    ),
                )
                sequence = cursor.lastrowid
                if sequence is None:
                    raise RuntimeError("Market-data insert did not return a sequence")
                self._db.execute(
                    """
                    INSERT INTO market_data_latest VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(market, kind, parameters, symbol) DO UPDATE SET
                        sequence = excluded.sequence,
                        source_epoch = excluded.source_epoch,
                        received_epoch = excluded.received_epoch
                    WHERE (excluded.source_epoch, excluded.received_epoch, excluded.sequence)
                        > (market_data_latest.source_epoch, market_data_latest.received_epoch,
                           market_data_latest.sequence)
                    """,
                    (*key, sequence, source, received),
                )
                result[row.symbol] = DataRecord(
                    market,
                    kind,
                    row.symbol,
                    parameters,
                    row.source_time,
                    row.received_time,
                    sequence,
                    row.source_identity,
                    payload,
                )
        return result
