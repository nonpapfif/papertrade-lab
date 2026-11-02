"""Run independent real-time US paper accounts from shared market-data observations."""

from __future__ import annotations

import argparse
import copy
import csv
import fcntl
import gzip
import hashlib
import json
import math
import os
import pickle
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from papertrade.api import ApiError
from papertrade.core import Settings
from tools.live_execution import LiveWeekEngine
from tools.live_feed import LiveFeed, SnapshotHistory
from tools.run_week_experiment import long_policies
from tools.search_portfolios import configurations, rank_frame, signals

ROOT = Path(__file__).resolve().parents[1]
SOURCES = (
    "tools/run_live_parallel.py",
    "tools/live_execution.py",
    "tools/live_feed.py",
    "tools/week_hypotheses.py",
    "tools/run_week_experiment.py",
    "tools/search_portfolios.py",
    "papertrade/api.py",
    "papertrade/core.py",
)
COST_SOURCES = {
    "brokerage": "https://www.toss.market/service/securities",
    "sec": "https://www.sec.gov/rules-regulations/fee-rate-advisories/2026-2",
    "taf_holiday": "https://www.govinfo.gov/content/pkg/FR-2026-09-23/html/2026-19392.htm",
    "korean_capital_gains": "https://home-files.tossinvest.com/files/notice/8c287d57-0108-4a0a-987e-e91bcc90c8a1.pdf",
}


def encode(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def now() -> str:
    return datetime.now(UTC).isoformat()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encode(value) + "\n")
    temporary.replace(path)


def live_design() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    specs = signals()
    configs = [
        c
        for c in configurations(len(specs))
        if c["leverage"] == 1
        and specs[c["signal"]]["side"] == "long"
        and specs[c["signal"]]["volume"] == 0
        and specs[c["signal"]]["family"] != "volume_trend"
    ]
    policies = [p for p in long_policies() if p.get("families") != ["volume_trend"]]
    return configs, specs, policies


def scope(calendar: dict[str, Any]) -> dict[str, Any]:
    day = calendar["today"]
    sessions = [day[key] for key in ("dayMarket", "preMarket", "regularMarket", "afterMarket")]
    intervals = [
        {
            "start": datetime.fromisoformat(s["startTime"]).astimezone(UTC).isoformat(),
            "end": datetime.fromisoformat(s["endTime"]).astimezone(UTC).isoformat(),
        }
        for s in sessions
    ]
    return {
        "US": {"start": intervals[0]["start"], "end": intervals[-1]["end"], "sessions": intervals}
    }


class Journal:
    def __init__(self, output: Path) -> None:
        self.db = sqlite3.connect(output / "live.sqlite3")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS observations("
            "id INTEGER PRIMARY KEY,kind TEXT,received_at TEXT,payload_gzip BLOB);"
            "CREATE TABLE IF NOT EXISTS frames("
            "slot INTEGER PRIMARY KEY,at TEXT,signal_at TEXT,sha256 TEXT,payload_gzip BLOB);"
            "CREATE TABLE IF NOT EXISTS checkpoint("
            "id INTEGER PRIMARY KEY CHECK(id=1),sha256 TEXT,payload BLOB);"
            "CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,at TEXT,kind TEXT,data TEXT);"
        )
        self.db.commit()

    def record(self, kind: str, payload: dict[str, Any]) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO observations(kind,received_at,payload_gzip) VALUES (?,?,?)",
                (kind, now(), gzip.compress(encode(payload).encode(), mtime=0)),
            )

    def event(self, kind: str, **fields: Any) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO events(at,kind,data) VALUES (?,?,?)", (now(), kind, encode(fields))
            )

    def load(self) -> dict[str, Any] | None:
        row = self.db.execute("SELECT sha256,payload FROM checkpoint WHERE id=1").fetchone()
        if row is None:
            return None
        if hashlib.sha256(row[1]).hexdigest() != row[0]:
            raise ValueError("Local checkpoint checksum mismatch")
        result = pickle.loads(row[1])
        if not isinstance(result, dict):
            raise TypeError("Invalid local checkpoint")
        return result

    def commit(self, state: dict[str, Any], frame: dict[str, Any] | None = None) -> None:
        payload = pickle.dumps(state, protocol=5)
        with self.db:
            if frame is not None:
                raw = encode(frame).encode()
                self.db.execute(
                    "INSERT INTO frames VALUES (?,?,?,?,?)",
                    (
                        state["last_slot"],
                        frame["at"],
                        frame["signal_at"],
                        hashlib.sha256(raw).hexdigest(),
                        gzip.compress(raw, mtime=0),
                    ),
                )
            self.db.execute(
                "INSERT OR REPLACE INTO checkpoint VALUES (1,?,?)",
                (hashlib.sha256(payload).hexdigest(), payload),
            )


def reports(output: Path, state: dict[str, Any]) -> None:
    summaries = []
    for mode, engine in state["engines"].items():
        report = engine.report()
        for h in report["hypotheses"]:
            signature = [
                {
                    k: t.get(k)
                    for k in (
                        "symbol_id",
                        "quantity_normalized",
                        "price",
                        "fill_at",
                        "realized_pnl",
                    )
                }
                for t in h["trades"]
            ]
            group = hashlib.sha256(encode(signature).encode()).hexdigest()[:16]
            summaries.append(
                {
                    "mode": mode,
                    "policy": h["id"],
                    "behavior_group": group,
                    "evaluation_equity_usd": h["summary"].get(
                        "after_tax_bid_liquidation_estimate_usd"
                    ),
                    "evaluation_basis": "conditional_after_tax_bid_liquidation_not_actual_fill",
                    **{k: v for k, v in h["summary"].items() if not isinstance(v, (list, dict))},
                }
            )
        raw = encode(report).encode()
        temporary = output / (mode + "-report.json.gz.tmp")
        temporary.write_bytes(gzip.compress(raw, compresslevel=1, mtime=0))
        temporary.replace(output / (mode + "-report.json.gz"))
    if summaries:
        temporary = output / "comparison.csv.tmp"
        with temporary.open("w", newline="") as stream:
            writer = csv.DictWriter(
                stream, fieldnames=list(dict.fromkeys(k for r in summaries for k in r))
            )
            writer.writeheader()
            writer.writerows(summaries)
        temporary.replace(output / "comparison.csv")


def make_frame(history: SnapshotHistory, at: str, ids: dict[str, int]) -> dict[str, Any]:
    features = history.features(at)
    quotes = {ids[r["symbol"]]: float(r["price"]) for r in features}
    return {
        "kind": "frame",
        "at": at,
        "signal_at": at,
        "price_observed_at": at,
        "tick": int(datetime.fromisoformat(at).timestamp() // 300),
        "quotes": quotes,
        "marks": {key: [price, at] for key, price in quotes.items()},
        "ranks": rank_frame(features, signals(), {("US", s): i for s, i in ids.items()}, at),
        "long_features": [
            [ids[r["symbol"]], r["scores"], r["volumes"], r["noises"]] for r in features
        ],
        "source_times": {ids[r["symbol"]]: r.get("source_at") for r in features},
        "feature_basis": "observed_60s_price_snapshots_not_exchange_OHLCV",
        "orderbooks": {},
        "orderbook_failures": {},
    }


def advance_frame(frame: dict[str, Any], at: str) -> None:
    frame["at"] = at
    cutoff = datetime.fromisoformat(at)
    frame["tick"] = int(cutoff.timestamp() // 300)
    stale = {
        symbol
        for symbol in frame["quotes"]
        if not 0
        <= (cutoff - datetime.fromisoformat(frame["source_times"][symbol])).total_seconds()
        <= 90
    }
    for symbol in stale:
        del frame["quotes"][symbol]
        frame["marks"][symbol][1] = frame["price_observed_at"]
    for symbol, price in frame["quotes"].items():
        frame["marks"][symbol] = [price, at]
    frame["long_features"] = [row for row in frame["long_features"] if row[0] in frame["quotes"]]
    for buckets in frame["ranks"]:
        buckets[1][:] = [row for row in buckets[1] if row[0] in frame["quotes"]]
    frame["expired_price_symbols"] = sorted(set(frame.get("expired_price_symbols", [])) | stale)


def collect_books(
    feed: LiveFeed,
    engine: LiveWeekEngine,
    frame: dict[str, Any],
    symbols: list[str],
    journal: Journal,
) -> None:
    while True:
        advance_frame(frame, now())
        missing = engine.quote_requests(frame)
        if not missing:
            return
        for symbol in sorted(missing):
            try:
                books = feed.orderbooks([symbols[symbol]])
                book = books.get(symbols[symbol])
                if book is None:
                    frame["orderbook_failures"][symbol] = "provider_returned_no_book"
                else:
                    frame["orderbooks"][symbol] = book
            except ApiError as error:
                if error.status in (401, 403):
                    raise
                frame["orderbook_failures"][symbol] = error.code
                journal.event(
                    "orderbook_unavailable",
                    symbol=symbols[symbol],
                    code=error.code,
                    status=error.status,
                    retry_at=error.retry_at,
                )
                if error.status == 429:
                    for remaining in missing:
                        frame["orderbook_failures"][remaining] = "provider_cooldown"
                    return
        cutoff = datetime.now(UTC).timestamp()
        for symbol, book in frame["orderbooks"].items():
            stamp = book.get("timestamp")
            if stamp is None or cutoff - datetime.fromisoformat(stamp).timestamp() > 60:
                frame["orderbook_failures"][symbol] = "quote_aged_during_collection"
        frame["fx"] = float(feed.fx()["midRate"])
        frame["fx_at"] = now()


def confirm_books(feed: LiveFeed, frame: dict[str, Any], symbols: list[str]) -> None:
    frame["decision_orderbooks"] = frame["orderbooks"]
    frame["decision_at"] = now()
    frame["orderbooks"] = {}
    frame["confirmation_received_at"] = {}
    if frame["decision_orderbooks"]:
        time.sleep(1)
    for symbol in sorted(frame["decision_orderbooks"]):
        try:
            result = feed.orderbooks([symbols[symbol]])
            book = result.get(symbols[symbol])
            if book is not None:
                frame["orderbooks"][symbol] = book
                frame["confirmation_received_at"][symbol] = now()
            else:
                frame["orderbook_failures"][symbol] = "confirmation_empty"
        except ApiError as error:
            if error.status in (401, 403):
                raise
            frame["orderbook_failures"][symbol] = error.code
            if error.status == 429:
                break
    for symbol in frame["decision_orderbooks"]:
        if symbol not in frame["orderbooks"]:
            frame["orderbook_failures"].setdefault(symbol, "confirmation_not_received")
    frame["fx"] = float(feed.fx()["midRate"])
    frame["fx_at"] = now()


def run(
    output: Path,
    day: str,
    capital: float,
    prior_gain: float,
    resume: bool,
    trade_from_regular: bool = False,
) -> None:
    if not math.isfinite(capital) or capital <= 0 or not math.isfinite(prior_gain):
        raise ValueError("Choose positive finite capital and finite prior realized gain")
    if not "2026-10-01" <= datetime.fromisoformat(day).date().isoformat() <= "2026-12-31":
        raise ValueError("The verified fee schedule covers October-December 2026 only")
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (output / "run.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        journal = Journal(output)
        path = output / "manifest.json"
        settings = Settings.load(ROOT)
        data = copy.deepcopy(settings.data)
        data["limits"]["minimum_request_interval"] = 0.2
        feed = LiveFeed(Settings(settings.root, data), journal.record)
        try:
            if path.exists():
                if not resume:
                    raise ValueError("Existing live output requires explicit --resume")
                manifest = json.loads(path.read_text())
                if (
                    manifest["day"],
                    manifest["capital_per_account"],
                    manifest["fee_schedule"]["annual_realized_before_run_krw"],
                ) != (day, capital, prior_gain):
                    raise ValueError("Resume arguments do not match the frozen live experiment")
                if manifest.get("trade_from_regular", False) != trade_from_regular:
                    raise ValueError("Resume trading start does not match the frozen experiment")
                for name, expected in manifest["code_sha256"].items():
                    if digest(ROOT / name) != expected:
                        raise ValueError("Frozen live source changed: " + name)
                state = journal.load()
                if state is None:
                    raise ValueError("Live run has no committed checkpoint")
                if set(state["engines"]) != {"quotes"}:
                    raise ValueError("This live runner requires quote-only account checkpoints")
                symbols = manifest["symbols"]
                feed.symbols = list(symbols)
                journal.event("resumed", last_slot=state["last_slot"])
            else:
                if resume:
                    raise ValueError("No live experiment exists to resume")
                calendar = feed.calendar(day)
                if calendar["today"]["date"] != day:
                    raise ValueError("Market calendar date mismatch")
                scopes = scope(calendar)
                if datetime.fromisoformat(scopes["US"]["start"]).timestamp() > time.time():
                    raise ValueError("Requested live session has not opened yet")
                if datetime.fromisoformat(scopes["US"]["end"]).timestamp() <= time.time():
                    raise ValueError("Requested live session has already ended")
                symbols = feed.universe()
                if not symbols:
                    raise ValueError("No live US universe is available")
                configs, specs, policies = live_design()
                fees = {
                    "commission_rate": 0.001,
                    "commission_exemption_usd": 10,
                    "sec_sell_rate": 0.0000206,
                    "sec_minimum_usd": 0.01,
                    "taf_rate": 0,
                    "annual_allowance_krw": 2500000,
                    "capital_gains_tax_rate": 0.22,
                    "annual_realized_before_run_krw": prior_gain,
                    "max_quote_age_seconds": 60,
                    "max_fx_age_seconds": 60,
                    "confirmation_delay_seconds": 1,
                    "allow_partial_exits": 1,
                }
                manifest = {
                    "created_at": now(),
                    "day": day,
                    "capital_per_account": capital,
                    "symbols": symbols,
                    "scopes": scopes,
                    "policies": policies,
                    "configs": len(configs),
                    "accounts": len(policies),
                    "execution_models": ["quotes"],
                    "trade_from_regular": trade_from_regular,
                    "code_sha256": {name: digest(ROOT / name) for name in SOURCES},
                    "fee_schedule": fees,
                    "cost_sources": COST_SOURCES,
                    "commission_basis": "Published general rate, not private account verification",
                    "regulatory_rounding": "SEC per-sale minimum and cent rounding are an "
                    "explicit customer-cost estimate, not verified private Toss billing",
                    "tax_assumptions": "Korean resident individual; prior realized gain supplied "
                    "in KRW; annual allowance applies per independent research account; "
                    "trade-time midrate estimate is not the statutory settlement-date FX basis; "
                    "tax reserve does not reduce trading cash; dividends and ADR charges unmodeled",
                    "feature_basis": "Received minute price snapshots, not exchange OHLCV. "
                    "No volume is fabricated; volume-dependent settings are excluded. Sampled "
                    "breakout and noise differ from historical candle features.",
                    "learning_proxy": "Quote-mode shadows learn at last prices plus fees; "
                    "actual quote-mode books use observed depth. Shadow spread is not modeled.",
                    "redundancy": "Cold/carry and several weighting variants overlap on the "
                    "first day. Behavior groups expose repeated outcomes; accounts are not "
                    "independent probability samples.",
                    "ai_calls": 0,
                    "real_orders": False,
                }
                engines = {
                    "quotes": LiveWeekEngine(
                        configs, specs, policies, mode="quotes", capital=capital, fee_schedule=fees
                    )
                }
                for engine in engines.values():
                    engine.day_start(day, scopes)
                state = {
                    "engines": engines,
                    "history": SnapshotHistory(),
                    "last_slot": -1,
                    "snapshots": 0,
                    "evaluations": 0,
                    "complete": False,
                }
                save(path, manifest)
                journal.commit(state)
            ids = {symbol: index for index, symbol in enumerate(symbols)}
            us_scope = manifest["scopes"]["US"]
            trading_start = (
                us_scope["sessions"][2]["start"] if trade_from_regular else us_scope["start"]
            )
            end = datetime.fromisoformat(manifest["scopes"]["US"]["end"]).timestamp()
            if state["complete"]:
                raise ValueError("Completed live runs cannot be restarted")
            while time.time() < end:
                started = time.time()
                try:
                    rows = feed.snapshot()
                    observed = now()
                    coverage = state["history"].add(observed, rows)
                    journal.event("snapshot_coverage", **coverage)
                    state["snapshots"] += 1
                    slot = int(datetime.fromisoformat(observed).timestamp() // 300)
                    warming = datetime.fromisoformat(observed) < datetime.fromisoformat(
                        trading_start
                    )
                    if not warming and slot > state["last_slot"]:
                        frame = make_frame(state["history"], observed, ids)
                        frame["signal_slot"] = slot
                        frame["fx"] = float(feed.fx()["midRate"])
                        frame["fx_at"] = now()
                        collect_books(feed, state["engines"]["quotes"], frame, symbols, journal)
                        confirm_books(feed, frame, symbols)
                        advance_frame(frame, now())
                        if datetime.fromisoformat(frame["at"]).timestamp() >= end:
                            break
                        candidate = copy.deepcopy(state)
                        for engine in candidate["engines"].values():
                            engine.step(frame)
                        candidate["last_slot"] = frame["tick"]
                        candidate["evaluations"] += 1
                        journal.commit(candidate, frame)
                        state = candidate
                        reports(output, state)
                    else:
                        journal.commit(state)
                    save(
                        output / "status.json",
                        {
                            "state": "warming_up" if warming else "running",
                            "at": now(),
                            "ai_calls": 0,
                            "pid": os.getpid(),
                            "accounts": manifest["accounts"],
                            "symbols": len(symbols),
                            "snapshots": state["snapshots"],
                            "evaluations": state["evaluations"],
                            "last_execution_slot": state["last_slot"],
                            "session_end": manifest["scopes"]["US"]["end"],
                            "trading_start": trading_start,
                        },
                    )
                except ApiError as error:
                    blocked = error.status in (401, 403)
                    journal.event(
                        "api_error", code=error.code, status=error.status, retry_at=error.retry_at
                    )
                    save(
                        output / "status.json",
                        {
                            "state": "blocked" if blocked else "waiting_api",
                            "at": now(),
                            "code": error.code,
                            "status": error.status,
                            "ai_calls": 0,
                        },
                    )
                    if blocked:
                        return
                    started = max(started, (error.retry_at or time.time()) - 60)
                remaining = min(end - time.time(), max(0, started + 60 - time.time()))
                while remaining > 0:
                    time.sleep(min(remaining, 30))
                    remaining = min(end - time.time(), started + 60 - time.time())
            if not state["evaluations"]:
                raise ValueError("No live evaluation frames were committed")
            for engine in state["engines"].values():
                if engine.day_steps:
                    engine.day_end()
            state["complete"] = True
            journal.commit(state)
            reports(output, state)
            save(
                output / "status.json",
                {
                    "state": "complete",
                    "at": now(),
                    "ai_calls": 0,
                    "evaluations": state["evaluations"],
                    "accounts": manifest["accounts"],
                    "open_positions_preserved": True,
                },
            )
        except BaseException as error:
            journal.event("failed", error_type=type(error).__name__)
            save(
                output / "status.json",
                {"state": "failed", "at": now(), "error_type": type(error).__name__, "ai_calls": 0},
            )
            raise
        finally:
            journal.db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--date", default="2026-11-02")
    parser.add_argument("--capital", type=float, default=100)
    parser.add_argument("--prior-realized-krw", type=float, default=0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--trade-from-regular", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if args.status:
        print((args.output / "status.json").read_text())
    else:
        run(
            args.output.resolve(),
            args.date,
            args.capital,
            args.prior_realized_krw,
            args.resume,
            args.trade_from_regular,
        )


if __name__ == "__main__":
    main()
