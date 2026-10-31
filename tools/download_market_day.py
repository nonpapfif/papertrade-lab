"""Download a frozen KR/US market day through market-data-only API paths."""

from __future__ import annotations

import argparse
import copy
import fcntl
import gzip
import hashlib
import json
import math
import os
import random
import signal
import sqlite3
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from papertrade.api import SYMBOL, ApiError, MarketClient
from papertrade.core import SafetyError, Settings, decimal, stamp

EXCHANGES = {
    "KR": ("KOSPI", "KOSDAQ", "KR_ETC"),
    "US": ("NYSE", "NASDAQ", "AMEX", "US_ETC"),
}
STATUSES = ("ACTIVE", "SCHEDULED", "DELISTED")
PATHS = frozenset({
    "/api/v1/stocks/all", "/api/v1/candles",
    "/api/v1/market-calendar/KR", "/api/v1/market-calendar/US",
})
FIELDS = ("openPrice", "highPrice", "lowPrice", "closePrice", "volume", "currency")
SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (at TEXT NOT NULL, kind TEXT NOT NULL, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS requests (at TEXT NOT NULL, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS listing_jobs (
    market TEXT, exchange TEXT, listing_status TEXT, complete INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (exchange, listing_status)
);
CREATE TABLE IF NOT EXISTS listings (
    market TEXT, exchange TEXT, listing_status TEXT, symbol TEXT, data TEXT NOT NULL,
    PRIMARY KEY (exchange, listing_status, symbol)
);
CREATE TABLE IF NOT EXISTS symbols (
    market TEXT, symbol TEXT, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    before_query TEXT NOT NULL, pages INTEGER NOT NULL DEFAULT 0,
    bars INTEGER NOT NULL DEFAULT 0, problem TEXT,
    PRIMARY KEY (market, symbol)
);
CREATE INDEX IF NOT EXISTS symbols_unfinished ON symbols (market, symbol)
WHERE status IN ('pending','downloading');
CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY, market TEXT, symbol TEXT, before_query TEXT,
    fetched_at TEXT NOT NULL, sha256 TEXT NOT NULL, result_gzip BLOB NOT NULL,
    valid INTEGER NOT NULL, problem TEXT,
    UNIQUE (market, symbol, before_query)
);
CREATE TABLE IF NOT EXISTS candles (
    market TEXT, symbol TEXT, at TEXT, source_timestamp TEXT NOT NULL,
    open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL, close TEXT NOT NULL,
    volume TEXT NOT NULL, currency TEXT NOT NULL, outside_calendar INTEGER NOT NULL,
    page_id INTEGER NOT NULL REFERENCES pages(id), PRIMARY KEY (market, symbol, at)
) WITHOUT ROWID;
"""


def encoded(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class Blocked(Exception):
    """Stop without an automatic relaunch after access/configuration rejection."""


def summary(db: sqlite3.Connection) -> dict[str, Any]:
    meta = {key: json.loads(value) for key, value in db.execute("SELECT * FROM metadata")}
    counts = [dict(row) for row in db.execute(
        "SELECT market,status,COUNT(*) AS symbols,SUM(pages) AS pages,SUM(bars) AS bars "
        "FROM symbols GROUP BY market,status ORDER BY market,status"
    )]
    return {
        "date": meta.get("date"), "state": meta.get("state"),
        "updated_at": meta.get("updated_at"), "pid": meta.get("pid"),
        "minimum_interval_seconds": meta.get("minimum_interval_seconds"),
        "scopes": meta.get("scopes"), "progress": counts,
        "total_symbols": sum(row["symbols"] for row in counts),
        "finished_symbols": sum(row["symbols"] for row in counts
                                if row["status"] not in ("pending", "downloading")),
        "bars": sum(row["bars"] for row in counts),
        "pages": sum(row["pages"] for row in counts),
        "requests": db.execute("SELECT COUNT(*) FROM requests").fetchone()[0],
        "last_event": dict(row) if (row := db.execute(
            "SELECT * FROM events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()) else None,
    }


class Downloader:
    def __init__(
        self, settings: Settings, output: Path, target: str, interval: float = 0.6,
        client: MarketClient | None = None,
    ) -> None:
        self.output = output
        self.target = date.fromisoformat(target).isoformat()
        if not math.isfinite(interval) or interval < 0.6:
            raise ValueError("The download interval must be at least 0.6 seconds")
        output.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = (output / "download.lock").open("a+")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            raise Blocked("Another downloader owns this dataset") from None
        self.db = sqlite3.connect(output / "market.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript(SCHEMA)
        if self.meta("date", self.target) != self.target:
            self.close()
            raise ValueError("The dataset belongs to another date")
        self.interval = max(interval, self.meta("minimum_interval_seconds", interval))
        self.set_meta("date", self.target)
        self.set_meta("minimum_interval_seconds", self.interval)
        data = copy.deepcopy(settings.data)
        data["limits"]["minimum_request_interval"] = self.interval
        self.client = client or MarketClient(Settings(settings.root, data))
        self.client.audit = self.audit
        self.last_progress = 0.0
        self.db.commit()

    def close(self) -> None:
        self.db.close()
        self.lock.close()

    def meta(self, key: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO metadata VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, encoded(value)),
        )

    def event(self, kind: str, **data: Any) -> None:
        record = {"at": utc_now(), "kind": kind, **data}
        with self.db:
            self.db.execute("INSERT INTO events VALUES (?,?,?)",
                            (record["at"], kind, encoded(data)))
            self.set_meta("updated_at", record["at"])
        print(encoded(record), flush=True)

    def audit(self, record: dict[str, Any]) -> None:
        path = record["path"]
        if path not in PATHS and path != "/oauth2/token":
            raise SafetyError("Downloader observed a forbidden endpoint")
        headers = record.get("rate_limit", {})

        def number(key: str) -> float:
            try:
                value = float(headers.get(key) or 0)
                return value if math.isfinite(value) and value >= 0 else 0.0
            except (ValueError, TypeError):
                return 0.0

        limit = number("X-RateLimit-Limit")
        delay = max(self.interval, 2 / limit if limit else 0)
        if str(headers.get("X-RateLimit-Remaining")) == "0":
            delay = max(delay, number("X-RateLimit-Reset") + 0.5)
        with self.db:
            self.db.execute("INSERT INTO requests VALUES (?,?)", (utc_now(), encoded(record)))
            self.set_meta("next_request_at", max(
                self.meta("next_request_at", 0), time.time() + delay,
                float(record.get("retry_at") or 0),
            ))

    def wait(self) -> None:
        while (remaining := self.meta("next_request_at", 0) - time.time()) > 0:
            time.sleep(min(remaining, 30))

    def request(self, path: str, **params: Any) -> Any:
        if path not in PATHS:
            raise SafetyError("Downloader permits only listings, calendars, and candles")
        failures = 0
        while True:
            self.wait()
            with self.db:
                self.set_meta("next_request_at", max(
                    self.meta("next_request_at", 0), time.time() + self.interval,
                ))
            try:
                result = self.client.get(path, **params)
                return result
            except ApiError as error:
                if error.code == "cooldown":
                    with self.db:
                        self.set_meta("next_request_at", max(
                            self.meta("next_request_at", 0), error.retry_at or 0,
                        ))
                    continue
                if error.status in (401, 403):
                    self.event("access_blocked", path=path, status=error.status, code=error.code)
                    raise Blocked("Access rejected; no automatic retry") from None
                if error.status and error.status < 500 and error.status != 429:
                    raise
                failures += 1
                if error.status == 429:
                    self.interval = min(60, max(self.interval * 2, 2.0))
                    with self.db:
                        self.set_meta("minimum_interval_seconds", self.interval)
                    self.client.settings.data["limits"]["minimum_request_interval"] = self.interval
                delay = max(
                    60 if error.status == 429 else 15,
                    min(900, 15 * 2 ** min(failures - 1, 6)),
                    (error.retry_at or 0) - time.time(),
                ) + random.uniform(0.5, 3)
                with self.db:
                    self.set_meta("next_request_at", max(
                        self.meta("next_request_at", 0), time.time() + delay,
                    ))
                self.event("retry_wait", path=path, code=error.code, status=error.status,
                           seconds=round(delay, 2), symbol=params.get("symbol"))

    def initialize(self) -> None:
        if not self.meta("scopes"):
            calendars = {}
            scopes = {}
            for market in EXCHANGES:
                calendar = self.request(f"/api/v1/market-calendar/{market}", date=self.target)
                today = calendar["today"]
                if today["date"] != self.target:
                    raise ValueError("Calendar returned a different trading date")
                sessions = today.get("integrated", today)
                windows = [{"start": stamp(v["startTime"]).isoformat(),
                            "end": stamp(v["endTime"]).isoformat()}
                           for v in sessions.values() if isinstance(v, dict)]
                if not windows:
                    raise ValueError("No sessions are available for this market day")
                start = min(stamp(w["start"]) for w in windows)
                end = max(stamp(w["end"]) for w in windows)
                if market == "US":
                    end = max(end, start + timedelta(days=1))
                if end > datetime.now(UTC):
                    raise ValueError("Only completed historical days can be frozen")
                scopes[market] = {"start": start.isoformat(), "end": end.isoformat(),
                                  "sessions": windows}
                calendars[market] = calendar
            with self.db:
                self.set_meta("calendars", calendars)
                self.set_meta("scopes", scopes)
                self.set_meta("format", "market-day-v1")
                self.set_meta("adjusted", False)
                self.set_meta("universe_captured_at", utc_now())
        with self.db:
            self.db.executemany("INSERT OR IGNORE INTO listing_jobs VALUES (?,?,?,0)",
                                [(m, e, s) for m, es in EXCHANGES.items()
                                 for e in es for s in STATUSES])
        for job in self.db.execute("SELECT * FROM listing_jobs WHERE complete=0").fetchall():
            rows = self.request("/api/v1/stocks/all", market=job["exchange"],
                                status=job["listing_status"])
            if not isinstance(rows, list) or any(
                not isinstance(r, dict) or not SYMBOL.fullmatch(str(r.get("symbol", "")))
                or not isinstance(r.get("name"), str) for r in rows
            ):
                raise ValueError("Invalid listing response")
            with self.db:
                for row in rows:
                    self.db.execute("INSERT OR IGNORE INTO listings VALUES (?,?,?,?,?)", (
                        job["market"], job["exchange"], job["listing_status"],
                        row["symbol"], encoded(row),
                    ))
                    self.db.execute(
                        "INSERT OR IGNORE INTO symbols (market,symbol,name,before_query) "
                        "VALUES (?,?,?,?)", (job["market"], row["symbol"], row["name"],
                                               self.meta("scopes")[job["market"]]["end"]),
                    )
                self.db.execute("UPDATE listing_jobs SET complete=1 "
                                "WHERE exchange=? AND listing_status=?",
                                (job["exchange"], job["listing_status"]))
            self.event("listing_saved", exchange=job["exchange"],
                       listing_status=job["listing_status"], count=len(rows))

    def normalize(self, page: Any, job: sqlite3.Row) -> tuple[list[tuple[Any, ...]], bool]:
        if not isinstance(page, dict) or not isinstance(page.get("candles"), list):
            raise TypeError("Missing candle array")
        rows = page["candles"]
        if len(rows) > 200:
            raise ValueError("Candle page exceeds the documented size")
        scope = self.meta("scopes")[job["market"]]
        start, end, before = (stamp(t) for t in
                              (scope["start"], scope["end"], job["before_query"]))
        next_before = page.get("nextBefore")
        if next_before is not None and stamp(next_before) >= before:
            raise ValueError("Pagination cursor did not advance")
        values: dict[str, tuple[Any, ...]] = {}
        timestamps = []
        for row in rows:
            at = stamp(row["timestamp"])
            if at > before or at.second or at.microsecond:
                raise ValueError("Candle timestamp violates the requested minute range")
            timestamps.append(at)
            if not start < at <= end:
                continue
            o, h, low, c, volume = (decimal(row[k]) for k in FIELDS[:-1])
            if min(o, h, low, c) <= 0 or not low <= o <= h or not low <= c <= h or volume < 0:
                raise ValueError("Invalid OHLCV values")
            if row["currency"] != ("KRW" if job["market"] == "KR" else "USD"):
                raise ValueError("Unexpected candle currency")
            outside = not any(stamp(w["start"]) < at <= stamp(w["end"])
                              for w in scope["sessions"])
            fields = tuple(str(row[k]) for k in FIELDS)
            key = at.isoformat()
            if key in values and values[key][4:10] != fields:
                raise ValueError("Conflicting candles within a page")
            values[key] = (job["market"], job["symbol"], key, row["timestamp"],
                           *fields, int(outside))
        if timestamps != sorted(timestamps, reverse=True):
            raise ValueError("Candle page is not in descending timestamp order")
        finished = not rows or min(timestamps) <= start or next_before is None
        return list(values.values()), finished

    def save_page(self, job: sqlite3.Row, page: Any) -> None:
        raw = encoded(page).encode()
        digest = hashlib.sha256(raw).hexdigest()
        compressed = gzip.compress(raw, mtime=0)
        problem = None
        values: list[tuple[Any, ...]] = []
        finished = False
        try:
            values, finished = self.normalize(page, job)
            for value in values:
                prior = self.db.execute(
                    "SELECT open,high,low,close,volume,currency FROM candles "
                    "WHERE market=? AND symbol=? AND at=?", value[:3],
                ).fetchone()
                if prior and tuple(prior) != value[4:10]:
                    raise ValueError("Conflicting candles across pages")
        except (ValueError, KeyError, TypeError, SafetyError) as error:
            problem = f"{type(error).__name__}: {error}"
        with self.db:
            cursor = self.db.execute(
                "INSERT INTO pages (market,symbol,before_query,fetched_at,sha256,"
                "result_gzip,valid,problem) VALUES (?,?,?,?,?,?,?,?)",
                (job["market"], job["symbol"], job["before_query"], utc_now(), digest,
                 compressed, int(problem is None), problem),
            )
            added = 0
            if problem is None:
                for value in values:
                    added += self.db.execute(
                        "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        (*value, cursor.lastrowid),
                    ).rowcount
            total = job["bars"] + added
            state = "downloading"
            if problem:
                state = "failed"
            elif finished:
                state = "done" if total else "no_data"
            self.db.execute(
                "UPDATE symbols SET status=?,before_query=?,pages=pages+1,bars=?,problem=? "
                "WHERE market=? AND symbol=?", (
                    state, page.get("nextBefore") or job["before_query"]
                    if isinstance(page, dict) else job["before_query"],
                    total, problem, job["market"], job["symbol"],
                ),
            )
            self.set_meta("updated_at", utc_now())
        if problem:
            self.event("invalid_page", market=job["market"], symbol=job["symbol"],
                       problem=problem)

    def download_symbol(self, market: str) -> bool:
        while job := self.db.execute(
            "SELECT * FROM symbols WHERE market=? AND status IN ('pending','downloading') "
            "ORDER BY symbol LIMIT 1", (market,),
        ).fetchone():
            try:
                page = self.request("/api/v1/candles", symbol=job["symbol"], interval="1m",
                                    count=200, before=job["before_query"], adjusted="false")
            except ApiError as error:
                if error.status == 404 or error.code == "unsupported-symbol":
                    with self.db:
                        self.db.execute("UPDATE symbols SET status='unavailable',problem=? "
                                        "WHERE market=? AND symbol=?",
                                        (error.code, market, job["symbol"]))
                    self.event("symbol_unavailable", market=market, symbol=job["symbol"],
                               status=error.status, code=error.code)
                    return True
                raise Blocked("Unexpected client rejection; inspect the request audit") from None
            self.save_page(job, page)
            self.progress()
            state = self.db.execute("SELECT status FROM symbols WHERE market=? AND symbol=?",
                                    (market, job["symbol"])).fetchone()[0]
            if state != "downloading":
                return True
        return False

    def progress(self, force: bool = False) -> None:
        if not force and time.monotonic() - self.last_progress < 60:
            return
        self.last_progress = time.monotonic()
        record = summary(self.db)
        temporary = self.output / "status.json.tmp"
        temporary.write_text(encoded(record) + "\n")
        temporary.replace(self.output / "status.json")
        print(encoded({"at": utc_now(), "kind": "progress", **record}), flush=True)

    def run(self) -> None:
        with self.db:
            self.set_meta("pid", os.getpid())
            self.set_meta("state", "running")
        self.event("started", date=self.target, interval=self.interval)
        try:
            self.initialize()
            while True:
                advanced = [self.download_symbol(market) for market in EXCHANGES]
                if not any(advanced):
                    break
            if self.db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Dataset failed SQLite integrity verification")
            failed = self.db.execute("SELECT COUNT(*) FROM symbols "
                                     "WHERE status IN ('failed','unavailable')").fetchone()[0]
            with self.db:
                self.set_meta("state", "complete_with_errors" if failed else "complete")
            self.event("completed", symbols_with_errors=failed)
        except (Blocked, SafetyError, ValueError, KeyError, TypeError):
            with self.db:
                self.set_meta("state", "blocked")
            self.event("stopped_for_inspection")
            raise
        except KeyboardInterrupt:
            with self.db:
                self.set_meta("state", "paused")
            self.event("paused")
        finally:
            self.progress(force=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--interval", type=float, default=0.6)
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--resume-blocked", action="store_true")
    args = parser.parse_args()
    target = date.fromisoformat(args.date).isoformat()
    output = args.output or args.root / "state" / "datasets" / target
    os.umask(0o077)
    if args.status:
        db = sqlite3.connect((output / "market.sqlite3").resolve().as_uri() + "?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        try:
            print(json.dumps(summary(db), ensure_ascii=False, indent=2))
        finally:
            db.close()
        return 0

    def interrupted(signum: int, frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    downloader = None
    try:
        downloader = Downloader(Settings.load(args.root), output, target, args.interval)
        if downloader.meta("state") == "blocked" and not args.resume_blocked:
            raise Blocked("Inspect the dataset and use --resume-blocked for manual recovery")
        downloader.run()
        return 0
    except (Blocked, SafetyError, ValueError, KeyError, TypeError) as error:
        print(encoded({"at": utc_now(), "kind": "blocked", "error": type(error).__name__,
                       "message": str(error)}), flush=True)
        return 0
    finally:
        if downloader is not None:
            downloader.close()


if __name__ == "__main__":
    raise SystemExit(main())
