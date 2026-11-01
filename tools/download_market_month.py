"""Download historical market days in reverse order with one shared request limiter."""

from __future__ import annotations

import argparse
import copy
import fcntl
import json
import math
import os
import queue
import signal
import sqlite3
import threading
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from papertrade.api import ApiError, MarketClient
from papertrade.core import SafetyError, Settings, stamp
from tools.download_market_day import (
    EXCHANGES,
    PATHS,
    Blocked,
    Downloader,
    encoded,
    summary,
    utc_now,
)


def reverse_dates(month: str, through: str) -> list[str]:
    start = date.fromisoformat(month + "-01")
    end = date.fromisoformat(through)
    if (start.year, start.month) != (end.year, end.month):
        raise ValueError("The final date must belong to the requested month")
    if end >= datetime.now(UTC).date():
        raise ValueError("Only completed historical dates are supported")
    return [(end - timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


class RateGate:
    def __init__(self, rate: float, saved: dict[str, Any] | None = None) -> None:
        if not math.isfinite(rate) or not 0 < rate <= 10:
            raise ValueError("Choose a finite request rate between zero and ten")
        self.rate = rate
        self.condition = threading.Condition()
        saved = saved or {}
        self.intervals: dict[str, float] = saved.get("intervals", {})
        self.cooldowns: dict[str, float] = saved.get("cooldowns", {})
        self.next_at: dict[str, float] = {}
        self.blocked: str | None = saved.get("blocked")
        self.cancelled = False

    def interval(self, group: str) -> float:
        minimum = (
            1 / self.rate
            if group == "MARKET_DATA_CHART"
            else 2
            if group in ("STOCK_ALL", "AUTH")
            else 2 / 3
        )
        return max(minimum, self.intervals.get(group, 0))

    def reserve(self, group: str) -> None:
        with self.condition:
            while True:
                if self.blocked or self.cancelled:
                    raise Blocked(self.blocked or "Download interrupted")
                now = time.time()
                remaining = max(self.next_at.get(group, 0), self.cooldowns.get(group, 0)) - now
                if remaining <= 0:
                    self.next_at[group] = now + self.interval(group)
                    return
                self.condition.wait(min(remaining, 0.25))

    def observe(self, record: dict[str, Any]) -> None:
        headers = record.get("rate_limit", {})
        group = MarketClient.group(record["path"])
        with self.condition:
            limit = float(headers.get("X-RateLimit-Limit") or 0)
            if math.isfinite(limit) and limit > 0:
                self.intervals[group] = max(self.intervals.get(group, 0), 2 / limit)
            refreshing = (
                record.get("status") == 401
                and record.get("method") == "GET"
                and record.get("attempt") == 1
                and record.get("code") in ("token-revoked", "expired-token", "invalid-token")
            )
            if record.get("status") in (401, 403) and not refreshing:
                self.blocked = f"Access rejected with HTTP {record['status']}"
            if record.get("status") == 429:
                self.intervals[group] = self.interval(group) * 2
                self.cooldowns[group] = max(
                    self.cooldowns.get(group, 0),
                    time.time() + 60,
                    float(record.get("retry_at") or 0),
                )
            elif str(headers.get("X-RateLimit-Remaining")) == "0":
                self.cooldowns[group] = max(
                    self.cooldowns.get(group, 0),
                    time.time() + float(headers.get("X-RateLimit-Reset") or 1) + 0.1,
                )
            self.condition.notify_all()

    def defer(self, group: str, seconds: float, until: float = 0) -> None:
        with self.condition:
            self.cooldowns[group] = max(self.cooldowns.get(group, 0), time.time() + seconds, until)
            self.condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self.condition:
            return {
                "intervals": dict(self.intervals),
                "cooldowns": dict(self.cooldowns),
                "blocked": self.blocked,
            }

    def cancel(self) -> None:
        with self.condition:
            self.cancelled = True
            self.condition.notify_all()


class GatedClient(MarketClient):
    def __init__(self, settings: Settings, gate: RateGate, audit: Any) -> None:
        super().__init__(settings, audit=audit)
        self.gate = gate

    def _reserve(self, group: str) -> None:
        self.gate.reserve(group)


def inspect_day(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        return summary(db)


class ParallelDay(Downloader):
    def __init__(
        self,
        settings: Settings,
        output: Path,
        target: str,
        seed: Path,
        gate: RateGate,
        workers: int = 4,
    ) -> None:
        super().__init__(settings, output, target)
        self.gate, self.seed, self.workers = gate, seed, workers
        self.interval = gate.interval("MARKET_DATA_CHART")
        data = copy.deepcopy(settings.data)
        data["limits"]["minimum_request_interval"] = 0
        self.fast_settings = Settings(settings.root, data)
        self.records: queue.Queue[dict[str, Any]] = queue.Queue()
        self.local = threading.local()
        self.client = self.new_client()
        self.closed_markets: list[str] = self.meta("closed_markets", [])
        self.progress_sink: Callable[[dict[str, Any]], None] | None = None
        self.last_month_progress = 0.0
        self.set_meta("minimum_interval_seconds", self.interval)
        self.db.commit()

    def new_client(self) -> GatedClient:
        def audit(record: dict[str, Any]) -> None:
            if record["path"] not in PATHS and record["path"] != "/oauth2/token":
                raise SafetyError("Unexpected endpoint in the monthly downloader")
            self.gate.observe(record)
            self.records.put({"observed_at": utc_now(), **record})

        return GatedClient(self.fast_settings, self.gate, audit)

    def drain(self) -> None:
        with self.db:
            while True:
                try:
                    record = self.records.get_nowait()
                except queue.Empty:
                    break
                self.db.execute(
                    "INSERT INTO requests VALUES (?,?)", (record["observed_at"], encoded(record))
                )
            self.set_meta("rate_gate", self.gate.snapshot())
            self.set_meta("minimum_interval_seconds", self.gate.interval("MARKET_DATA_CHART"))

    def request(self, path: str, **params: Any) -> Any:
        if path not in PATHS:
            raise SafetyError("Monthly downloader permits only public market-data paths")
        while True:
            try:
                return self.client.get(path, **params)
            except ApiError as error:
                self.retry(error, MarketClient.group(path))
            finally:
                self.drain()

    def retry(self, error: ApiError, group: str) -> None:
        if error.status in (401, 403) or self.gate.blocked:
            raise Blocked(self.gate.blocked or "Access rejected") from None
        if error.status and error.status < 500 and error.status != 429:
            raise Blocked(f"Unexpected market-data response: HTTP {error.status}") from None
        self.gate.defer(group, 60 if error.status == 429 else 15, error.retry_at or 0)
        self.event(
            "retry_wait", code=error.code, status=error.status, rate_gate=self.gate.snapshot()
        )

    def initialize(self) -> None:
        if self.meta("initialized", False):
            return
        calendars, scopes = {}, {}
        for market in EXCHANGES:
            calendar = self.request(f"/api/v1/market-calendar/{market}", date=self.target)
            today = calendar["today"]
            if today["date"] != self.target:
                raise ValueError("Calendar returned another date")
            sessions = today.get("integrated") if market == "KR" else today
            if sessions is not None and not isinstance(sessions, dict):
                raise TypeError("Invalid market calendar")
            windows = [
                {"start": stamp(v["startTime"]).isoformat(), "end": stamp(v["endTime"]).isoformat()}
                for v in (sessions or {}).values()
                if isinstance(v, dict) and "startTime" in v and "endTime" in v
            ]
            calendars[market] = calendar
            if not windows:
                self.closed_markets.append(market)
                continue
            start = min(stamp(w["start"]) for w in windows)
            end = max(stamp(w["end"]) for w in windows)
            if market == "US":
                end = max(end, start + timedelta(days=1))
            if end > datetime.now(UTC):
                raise ValueError("The requested session has not finished")
            scopes[market] = {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "sessions": windows,
            }
        source = sqlite3.connect(self.seed.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            source_meta = {k: json.loads(v) for k, v in source.execute("SELECT * FROM metadata")}
            if source_meta["state"] not in ("complete", "complete_with_errors"):
                raise ValueError("The seed universe is not complete")
            if source.execute("SELECT COUNT(*) FROM listing_jobs WHERE complete=0").fetchone()[0]:
                raise ValueError("The seed listing snapshot is not complete")
            listings = source.execute("SELECT * FROM listings").fetchall()
            jobs = source.execute("SELECT * FROM listing_jobs").fetchall()
            with self.db:
                self.set_meta("calendars", calendars)
                self.set_meta("scopes", scopes)
                self.set_meta("closed_markets", sorted(set(self.closed_markets)))
                self.set_meta("format", "market-day-v1")
                self.set_meta("adjusted", False)
                self.set_meta("universe_captured_at", source_meta["universe_captured_at"])
                self.set_meta("universe_source", str(self.seed.resolve()))
                self.set_meta(
                    "universe_limitation",
                    "Snapshot supplied by the provider; "
                    "not a reconstructed historical listing universe",
                )
                self.db.executemany("INSERT INTO listings VALUES (?,?,?,?,?)", listings)
                self.db.executemany("INSERT INTO listing_jobs VALUES (?,?,?,?)", jobs)
                self.db.executemany(
                    "INSERT OR IGNORE INTO symbols(market,symbol,name,before_query) VALUES (?,?,?,?)",
                    [
                        (market, symbol, name, scopes[market]["end"])
                        for market, symbol, name in source.execute(
                            "SELECT market,symbol,name FROM symbols ORDER BY market,symbol"
                        )
                        if market in scopes
                    ],
                )
                self.set_meta("initialized", True)
        finally:
            source.close()
        self.event(
            "calendar_and_universe_ready",
            open_markets=sorted(scopes),
            closed_markets=self.closed_markets,
            symbols=self.db.execute("SELECT COUNT(*) FROM symbols").fetchone()[0],
        )

    def fetch(self, job: dict[str, Any]) -> Any:
        if not hasattr(self.local, "client"):
            self.local.client = self.new_client()
        return self.local.client.get(
            "/api/v1/candles",
            symbol=job["symbol"],
            interval="1m",
            count=200,
            before=job["before_query"],
            adjusted="false",
        )

    def progress(self, force: bool = False) -> None:
        super().progress(force)
        if self.progress_sink and (force or time.monotonic() - self.last_month_progress >= 10):
            self.last_month_progress = time.monotonic()
            self.progress_sink(summary(self.db))

    def run(self) -> None:
        self.set_meta("state", "running")
        self.set_meta("pid", os.getpid())
        self.db.commit()
        executor = ThreadPoolExecutor(max_workers=self.workers)
        pending: dict[Future[Any], dict[str, Any]] = {}
        try:
            self.initialize()
            ready = deque(
                dict(row)
                for row in self.db.execute(
                    "SELECT * FROM symbols WHERE status IN ('pending','downloading') "
                    "ORDER BY market,symbol"
                )
            )
            while ready or pending:
                if self.gate.blocked:
                    raise Blocked(self.gate.blocked)
                while ready and len(pending) < self.workers:
                    job = ready.popleft()
                    pending[executor.submit(self.fetch, job)] = job
                finished, _ = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                self.drain()
                for future in finished:
                    job = pending.pop(future)
                    try:
                        page = future.result()
                    except ApiError as error:
                        if error.status == 404 or error.code == "unsupported-symbol":
                            with self.db:
                                self.db.execute(
                                    "UPDATE symbols SET status='unavailable',problem=? "
                                    "WHERE market=? AND symbol=?",
                                    (error.code, job["market"], job["symbol"]),
                                )
                        else:
                            self.retry(error, "MARKET_DATA_CHART")
                            ready.appendleft(job)
                        continue
                    stored = self.db.execute(
                        "SELECT * FROM symbols WHERE market=? AND symbol=?",
                        (job["market"], job["symbol"]),
                    ).fetchone()
                    if stored["before_query"] != job["before_query"]:
                        raise ValueError("An in-flight symbol cursor changed")
                    self.save_page(stored, page)
                    current = self.db.execute(
                        "SELECT * FROM symbols WHERE market=? AND symbol=?",
                        (job["market"], job["symbol"]),
                    ).fetchone()
                    if current["status"] == "downloading":
                        ready.appendleft(dict(current))
                self.progress()
            self.drain()
            if self.db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Dataset integrity verification failed")
            problems = self.db.execute(
                "SELECT COUNT(*) FROM symbols WHERE status IN ('failed','unavailable')"
            ).fetchone()[0]
            self.set_meta("state", "complete_with_errors" if problems else "complete")
            self.db.commit()
            self.event(
                "completed", symbols_with_errors=problems, closed_markets=self.closed_markets
            )
        except BaseException as error:
            self.gate.cancel()
            self.set_meta("state", "paused" if isinstance(error, KeyboardInterrupt) else "blocked")
            self.db.commit()
            self.event(
                "stopped_for_inspection", error_type=type(error).__name__, message=str(error)
            )
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
            self.drain()
            self.progress(force=True)


def run_month(
    settings: Settings,
    month: str,
    through: str,
    output: Path,
    seed: Path,
    rate: float = 5,
    workers: int = 4,
    resume_blocked: bool = False,
) -> None:
    if workers < 1:
        raise ValueError("At least one network worker is required")
    dates = reverse_dates(month, through)
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (output / "download.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        status_path = output / "status.json"
        previous = json.loads(status_path.read_text()) if status_path.exists() else {}
        if previous and any(
            previous.get(k) != v
            for k, v in (("month", month), ("through", through), ("seed", str(seed.resolve())))
        ):
            raise ValueError("Monthly job arguments changed")
        if previous.get("state") == "blocked" and not resume_blocked:
            raise Blocked("Inspect the audit and explicitly resume the blocked job")
        saved = previous.get("rate_gate", {})
        if resume_blocked:
            saved.pop("blocked", None)
        gate = RateGate(rate, saved)
        record = {
            **previous,
            "month": month,
            "through": through,
            "seed": str(seed.resolve()),
            "state": "running",
            "pid": os.getpid(),
            "ai_calls": 0,
            "target_requests_per_second": rate,
            "workers": workers,
            "order": "descending",
            "days": previous.get("days", {}),
        }

        def persist() -> None:
            record.update(updated_at=utc_now(), rate_gate=gate.snapshot())
            temporary = output / "status.json.tmp"
            temporary.write_text(encoded(record) + "\n")
            temporary.replace(status_path)

        day = None
        try:
            persist()
            for target in dates:
                record["current_date"] = target
                path = settings.root / "state" / "datasets" / target
                existing = inspect_day(path / "market.sqlite3")
                if existing and existing["state"] in ("complete", "complete_with_errors"):
                    record["days"][target] = {
                        "state": existing["state"],
                        "reused": True,
                        "bars": existing["bars"],
                    }
                    persist()
                    continue
                if existing and existing["state"] == "blocked" and not resume_blocked:
                    raise Blocked(f"Inspect the blocked daily dataset: {target}")
                persist()
                day = ParallelDay(settings, path, target, seed, gate, workers)

                def update(state: dict[str, Any]) -> None:
                    record["current_day"] = state
                    persist()

                day.progress_sink = update
                day.run()
                state = summary(day.db)
                record["days"][target] = {
                    "state": state["state"],
                    "bars": state["bars"],
                    "closed_markets": day.closed_markets,
                }
                day.close()
                day = None
                persist()
            record["state"] = (
                "complete_with_errors"
                if any(
                    value["state"] == "complete_with_errors" for value in record["days"].values()
                )
                else "complete"
            )
            persist()
        except BaseException as error:
            record.update(
                state="paused" if isinstance(error, KeyboardInterrupt) else "blocked",
                error_type=type(error).__name__,
                message=str(error),
            )
            persist()
            raise
        finally:
            if day is not None:
                day.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument("--through", required=True)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rate", type=float, default=5)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--resume-blocked", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if args.status:
        print((args.output / "status.json").read_text())
        return

    def interrupted(signum: int, frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    run_month(
        Settings.load(args.root),
        args.month,
        args.through,
        args.output,
        args.seed,
        args.rate,
        args.workers,
        args.resume_blocked,
    )


if __name__ == "__main__":
    main()
