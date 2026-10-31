"""Compare diverse virtual portfolios on sealed causal five-minute feature frames."""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import json
import math
import os
import random
import sqlite3
import subprocess
import sys
import time
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import product
from pathlib import Path
from typing import IO, Any

FAMILIES = ("momentum", "reversal", "breakout", "mean_reversion", "transition", "volume_trend")
LOOKBACKS = (5, 10, 15, 30, 60)
MOVES = (0.0, 0.001, 0.003, 0.005, 0.01)
VOLUMES = (0.0, 1.5, 3.0)
NOISES = (0.0, 2.0)
SLOTS = (1, 3, 5, 10, 20)
LEVERAGES = (1, 2, 5, 10)
HOLDS = (1, 2, 3, 6, 12, 24, 0)
EXITS = (
    (0.0, 0.0, 0.0),
    (0.01, 0.0, 0.0),
    (0.03, 0.0, 0.0),
    (0.0, 0.01, 0.0),
    (0.01, 0.01, 0.0),
    (0.03, 0.03, 0.0),
    (0.0, 0.0, 0.01),
    (0.0, 0.0, 0.03),
)
INITIAL = 1000.0
SEED = 20261101
SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS configurations (id INTEGER PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS frames (
    id INTEGER PRIMARY KEY, at TEXT NOT NULL, sha256 TEXT NOT NULL, payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS results (
    configuration INTEGER, market TEXT, phase TEXT, equity REAL NOT NULL,
    return_pct REAL NOT NULL, data TEXT NOT NULL,
    PRIMARY KEY(configuration,market,phase)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS result_rank ON results(market,phase,equity DESC);
CREATE TABLE IF NOT EXISTS details (
    configuration INTEGER, market TEXT, phase TEXT, data BLOB NOT NULL,
    PRIMARY KEY(configuration,market,phase)
) WITHOUT ROWID;
"""


def encode(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def parse(value: str) -> datetime:
    return datetime.fromisoformat(value)


def meta(db: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def put(db: sqlite3.Connection, key: str, value: Any) -> None:
    db.execute(
        "INSERT INTO metadata VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, encode(value)),
    )


def progress(db: sqlite3.Connection, output: Path, stage: str, **extra: Any) -> None:
    record = {
        "stage": stage,
        "updated_at": datetime.now(UTC).isoformat(),
        "pid": os.getpid(),
        "ai_calls": 0,
        **extra,
    }
    with db:
        put(db, "progress", record)
    temporary = output / "status.json.tmp"
    temporary.write_text(encode(record) + "\n")
    temporary.replace(output / "status.json")
    print(encode(record), flush=True)


def signals() -> list[dict[str, Any]]:
    return [
        {
            "family": FAMILIES[f],
            "lookback": LOOKBACKS[look],
            "column": f * 5 + look,
            "look": look,
            "side": side,
            "move": move,
            "volume": volume,
            "noise": noise,
            "ranking": ranking,
        }
        for f, look, side, move, volume, noise, ranking in product(
            range(6),
            range(5),
            ("long", "short", "both"),
            MOVES,
            VOLUMES,
            NOISES,
            ("strongest", "nearest_threshold"),
        )
    ]


def configurations(signal_count: int) -> list[dict[str, Any]]:
    rng = random.Random(SEED)
    configs: list[dict[str, Any]] = []
    for signal in range(signal_count):
        profiles: list[dict[str, Any]] = [
            {
                "slots": 1,
                "leverage": 1,
                "hold": 1,
                "target": 0.0,
                "stop": 0.0,
                "trail": 0.0,
                "sizing": "equal",
                "compound": True,
                "session": "all",
                "rotate": False,
                "cooldown": 0,
            },
            {
                "slots": 5,
                "leverage": 1,
                "hold": 3,
                "target": 0.0,
                "stop": 0.0,
                "trail": 0.0,
                "sizing": "equal",
                "compound": True,
                "session": "all",
                "rotate": False,
                "cooldown": 0,
            },
        ]
        seen = {encode(p) for p in profiles}
        while len(profiles) < 12:
            target, stop, trail = rng.choice(EXITS)
            profile = {
                "slots": rng.choice(SLOTS),
                "leverage": rng.choice(LEVERAGES),
                "hold": rng.choice(HOLDS),
                "target": target,
                "stop": stop,
                "trail": trail,
                "sizing": rng.choice(("equal", "score")),
                "compound": rng.choice((False, True)),
                "session": rng.choice(("all", "regular", "extended")),
                "rotate": rng.choice((False, True)),
                "cooldown": rng.choice((0, 1, 3)),
            }
            if encode(profile) not in seen:
                seen.add(encode(profile))
                profiles.append(profile)
        first_id = len(configs)
        configs.extend(
            {"id": first_id + i, "signal": signal, **profile} for i, profile in enumerate(profiles)
        )
    return configs


def rank_frame(
    features: list[dict[str, Any]],
    specs: list[dict[str, Any]],
    ids: dict[tuple[str, str], int],
    at: str,
) -> list[list[list[Any]]]:
    """Use only exact current quotes; keep enough ranks for every tested slot policy."""
    bases: dict[tuple[int, str, int], list[list[Any]]] = {}
    for row in features:
        if row["quote_at"] != at:
            continue
        symbol = ids[(row["market"], row["symbol"])]
        market = int(row["market"] == "US")
        for column, score in enumerate(row["scores"]):
            if score is None or not math.isfinite(score) or score == 0:
                continue
            look = column % 5
            side = 1 if score > 0 else -1
            record = [symbol, side, abs(score), row["volumes"][look], row["noises"][look]]
            bases.setdefault((column, "long" if side > 0 else "short", market), []).append(record)
            bases.setdefault((column, "both", market), []).append(record)
    ascending = {}
    descending = {}
    strengths = {}
    for key, records in bases.items():
        ascending[key] = sorted(records, key=lambda r: (r[2], r[0]))
        descending[key] = sorted(records, key=lambda r: (-r[2], r[0]))
        strengths[key] = [r[2] for r in ascending[key]]
    result = []
    # At most slots holdings plus three checks of cooldown exclusions can precede new entries.
    # This bound preserves the exact tested selections; it is not a universe screening limit.
    limit = max(SLOTS) * (3 + 2)
    for spec in specs:
        buckets: list[list[Any]] = [[], []]
        for market in (0, 1):
            key = (spec["column"], spec["side"], market)
            if spec["ranking"] == "strongest":
                records = descending.get(key, [])
                start = 0
            else:
                records = ascending.get(key, [])
                start = bisect_left(strengths.get(key, []), spec["move"])
            bucket = buckets[market]
            for index in range(start, len(records)):
                candidate = records[index]
                if candidate[2] < spec["move"]:
                    break
                if spec["volume"] and (candidate[3] is None or candidate[3] < spec["volume"]):
                    continue
                if spec["noise"] and (candidate[4] is None or candidate[4] < spec["noise"]):
                    continue
                bucket.append(candidate[:3])
                if len(bucket) == limit:
                    break
        result.append(buckets)
    return result


@dataclass(slots=True)
class Position:
    quantity: float
    entry: float
    tick: int
    peak_return: float = 0.0


@dataclass(slots=True)
class Book:
    config: dict[str, Any]
    market: str
    phase: str
    detailed: bool = False
    cash: float = INITIAL
    equity: float = INITIAL
    peak: float = INITIAL
    drawdown: float = 0.0
    realized: float = 0.0
    gross_peak: float = 0.0
    debt_peak: float = 0.0
    turnover: float = 0.0
    opened: int = 0
    closed: int = 0
    wins: int = 0
    bankrupt: bool = False
    positions: dict[int, Position] = field(default_factory=dict)
    cooldowns: dict[int, int] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)
    curve: list[dict[str, Any]] = field(default_factory=list)

    def mark(self, prices: dict[int, float]) -> None:
        values = [p.quantity * prices[s] for s, p in self.positions.items()]
        self.equity = self.cash + sum(values)
        self.peak = max(self.peak, self.equity)
        self.drawdown = max(self.drawdown, (self.peak - self.equity) / self.peak)
        self.gross_peak = max(self.gross_peak, sum(abs(v) for v in values))
        self.debt_peak = max(self.debt_peak, -self.cash, 0)
        self.bankrupt |= self.equity <= 0
        if not math.isfinite(self.equity):
            raise ValueError("Non-finite virtual equity")

    def fill(
        self,
        symbol: int,
        quantity: float,
        price: float,
        at: str,
        tick: int,
        reason: str,
        strength: float | None = None,
    ) -> None:
        self.cash -= quantity * price
        self.turnover += abs(quantity * price)
        pnl = None
        entry_price = price
        if reason == "entry":
            self.positions[symbol] = Position(quantity, price, tick)
            self.opened += 1
        else:
            position = self.positions.pop(symbol)
            entry_price = position.entry
            pnl = position.quantity * (price - position.entry)
            self.realized += pnl
            self.closed += 1
            self.wins += pnl > 0
            if self.config["cooldown"]:
                self.cooldowns[symbol] = tick + self.config["cooldown"]
        if self.detailed:
            self.trades.append(
                {
                    "symbol_id": symbol,
                    "quantity_normalized": quantity,
                    "price": price,
                    "source_at": at,
                    "fill_at": at,
                    "reason": reason,
                    "strength": strength,
                    "cash": self.cash,
                    "entry_price": entry_price,
                    "realized_pnl": pnl,
                }
            )

    def step(
        self,
        at: str,
        tick: int,
        fresh: dict[int, float],
        prices: dict[int, float],
        candidates: list[list[Any]],
        entry_allowed: bool,
    ) -> None:
        cfg = self.config
        for symbol, until in list(self.cooldowns.items()):
            if until <= tick:
                del self.cooldowns[symbol]
        self.mark(prices)
        targets = {r[0]: r[1] for r in candidates[: cfg["slots"]]}
        for symbol, position in list(self.positions.items()):
            if symbol not in fresh:
                continue
            price = fresh[symbol]
            side = 1 if position.quantity > 0 else -1
            change = side * (price / position.entry - 1)
            position.peak_return = max(position.peak_return, change)
            reason = None
            if cfg["stop"] and change <= -cfg["stop"]:
                reason = "stop_at_observed_check"
            elif cfg["target"] and change >= cfg["target"]:
                reason = "target_at_observed_check"
            elif cfg["trail"] and position.peak_return - change >= cfg["trail"]:
                reason = "trailing_at_observed_check"
            elif cfg["hold"] and tick - position.tick >= cfg["hold"]:
                reason = "holding_period"
            elif cfg["rotate"] and targets.get(symbol) != side:
                reason = "rank_or_side_changed"
            if reason:
                self.fill(symbol, -position.quantity, price, at, tick, reason)
        self.mark(prices)
        if entry_allowed:
            free = cfg["slots"] - len(self.positions)
            chosen = [
                r
                for r in candidates
                if r[0] not in self.positions
                and self.cooldowns.get(r[0], -1) <= tick
                and r[0] in fresh
            ][:free]
            capital = max(self.equity, 0) if cfg["compound"] else INITIAL
            budget = capital * cfg["leverage"] / cfg["slots"] * len(chosen)
            scale = max((r[2] for r in chosen), default=1)
            weights = [r[2] / scale if cfg["sizing"] == "score" else 1 for r in chosen]
            total = sum(weights)
            if budget > 0 and total > 0:
                for (symbol, side, strength), weight in zip(chosen, weights, strict=True):
                    quantity = side * budget * weight / total / fresh[symbol]
                    self.fill(symbol, quantity, fresh[symbol], at, tick, "entry", strength)
        self.mark(prices)
        if self.detailed:
            self.curve.append(
                {
                    "at": at,
                    "equity": self.equity,
                    "cash": self.cash,
                    "open_positions": len(self.positions),
                }
            )

    def summary(self, prices: dict[int, float], times: dict[int, str], at: str) -> dict[str, Any]:
        self.mark(prices)
        unrealized = sum(p.quantity * (prices[s] - p.entry) for s, p in self.positions.items())
        if not math.isclose(
            self.equity, INITIAL + self.realized + unrealized, rel_tol=1e-9, abs_tol=1e-6
        ):
            raise ValueError("Virtual ledger does not reconcile")
        return {
            "configuration": self.config["id"],
            "market": self.market,
            "phase": self.phase,
            "initial_equity": INITIAL,
            "equity": self.equity,
            "return_pct": (self.equity / INITIAL - 1) * 100,
            "cash": self.cash,
            "debt": max(-self.cash, 0),
            "realized": self.realized,
            "unrealized": unrealized,
            "max_drawdown_pct": self.drawdown * 100,
            "gross_exposure_peak": self.gross_peak,
            "debt_peak": self.debt_peak,
            "turnover": self.turnover,
            "entries": self.opened,
            "closed": self.closed,
            "wins": self.wins,
            "open_positions": len(self.positions),
            "ever_nonpositive_equity": self.bankrupt,
            "stale_open_positions": sum(times[s] != at for s in self.positions),
            "oldest_open_quote_at": min((times[s] for s in self.positions), default=None),
        }


def boundaries(scope: dict[str, Any]) -> list[str]:
    start, end = parse(scope["start"]), parse(scope["end"])
    steps = int((end - start).total_seconds() / 300)
    return [
        (start + timedelta(seconds=int(steps * share) * 300)).isoformat()
        for share in (0.6, 0.8, 1.0)
    ]


def regular(at: str, market: str, scope: dict[str, Any]) -> bool:
    sessions = scope["sessions"]
    index = 1 if market == "KR" else 2
    return sessions[index]["start"] <= at < sessions[index]["end"]


def send(value: Any, target: IO[str] = sys.stdout) -> None:
    target.write(encode(value) + "\n")
    target.flush()


def emit_books(
    books: list[Book], prices: dict[int, float], times: dict[int, str], at: str, target: IO[str]
) -> None:
    batch = []
    for book in books:
        summary = book.summary(prices, times, at)
        batch.append(
            {
                "summary": summary,
                **(
                    {
                        "trades": book.trades,
                        "curve": book.curve,
                        "positions": [
                            {
                                "symbol_id": s,
                                "quantity_normalized": p.quantity,
                                "entry_price": p.entry,
                                "mark_price": prices[s],
                                "source_at": times[s],
                            }
                            for s, p in book.positions.items()
                        ],
                    }
                    if book.detailed
                    else {}
                ),
            }
        )
        if len(batch) == 500:
            send({"kind": "results", "rows": batch}, target)
            batch = []
    if batch:
        send({"kind": "results", "rows": batch}, target)


def worker_loop(source: IO[str] = sys.stdin, target: IO[str] = sys.stdout) -> None:
    books: dict[str, tuple[list[Book], list[Book]]] = {}
    prices: dict[int, float] = {}
    times: dict[int, str] = {}
    scopes: dict[str, Any] = {}
    configs: list[dict[str, Any]] = []
    cuts: dict[str, list[str]] = {}
    phases = {market: 0 for market in ("KR", "US")}
    detailed = False
    for line in source:
        request = json.loads(line)
        kind = request["kind"]
        if kind == "probe":
            checks = {}
            for path in request["paths"]:
                try:
                    with open(path, "rb") as stream:
                        stream.read(1)
                except PermissionError:
                    checks[path] = True
                else:
                    checks[path] = False
            import socket

            try:
                with socket.socket() as connection:
                    connection.connect(("127.0.0.1", 1))
            except PermissionError:
                checks["network"] = True
            except OSError:
                checks["network"] = False
            else:
                checks["network"] = False
            try:
                subprocess.run(["/usr/bin/true"], check=False, capture_output=True)
            except PermissionError:
                checks["exec"] = True
            else:
                checks["exec"] = False
            send({"kind": "ack", "isolation": checks}, target)
            continue
        if kind == "init":
            scopes, configs = request["scopes"], request["configs"]
            detailed = request.get("detailed", False)
            for market in ("KR", "US"):
                selected = [c for c in configs if market in c.get("markets", ("KR", "US"))]
                books[market] = (
                    [Book(c, market, "full", detailed) for c in selected],
                    [Book(c, market, "train", detailed) for c in selected],
                )
                cuts[market] = boundaries(scopes[market])
        elif kind == "frame":
            at, tick = request["at"], request["tick"]
            fresh = {int(s): p for s, p in request["quotes"].items()}
            if any(not math.isfinite(p) or p <= 0 for p in fresh.values()):
                raise ValueError("Invalid observed price")
            marks = {int(s): mark for s, mark in request["marks"].items()}
            for symbol, (price, observed_at) in marks.items():
                if observed_at > at or observed_at < times.get(symbol, observed_at):
                    raise ValueError("Invalid mark chronology")
                if not math.isfinite(price) or price <= 0:
                    raise ValueError("Invalid mark price")
                prices[symbol], times[symbol] = price, observed_at
            if any(prices.get(s) != price or times.get(s) != at for s, price in fresh.items()):
                raise ValueError("Fresh fills do not match current observed marks")
            for market, market_index in (("KR", 0), ("US", 1)):
                scope = scopes[market]
                if not scope["start"] <= at <= scope["end"]:
                    continue
                full, fold = books[market]
                boundary = at >= cuts[market][phases[market]]
                is_regular = regular(at, market, scope)
                for group in (full, fold):
                    for book in group:
                        cfg = book.config
                        candidates = request["ranks"][cfg["signal"]][market_index]
                        allowed = at < scope["end"] and (
                            cfg["session"] == "all" or is_regular == (cfg["session"] == "regular")
                        )
                        book.step(
                            at,
                            tick,
                            fresh,
                            prices,
                            candidates,
                            allowed and (group is full or not boundary),
                        )
                if boundary:
                    emit_books(fold, prices, times, at, target)
                    if phases[market] < 2:
                        phases[market] += 1
                        phase = ("train", "validation", "test")[phases[market]]
                        fold = [Book(b.config, market, phase, detailed) for b in fold]
                        books[market] = (full, fold)
                        if at < scope["end"]:
                            for book in fold:
                                cfg = book.config
                                allowed = cfg["session"] == "all" or is_regular == (
                                    cfg["session"] == "regular"
                                )
                                book.step(
                                    at,
                                    tick,
                                    fresh,
                                    prices,
                                    request["ranks"][cfg["signal"]][market_index],
                                    allowed,
                                )
                    else:
                        emit_books(full, prices, times, at, target)
                        books[market] = ([], [])
        else:
            raise ValueError("Unknown worker request")
        send({"kind": "ack"}, target)


class Worker:
    def __init__(self, dataset: Path, output: Path) -> None:
        from tools.replay_market_day import sandbox_profile

        script = Path(__file__).resolve()
        self.process = subprocess.Popen(
            [
                "/usr/bin/sandbox-exec",
                "-p",
                sandbox_profile(script, dataset, output),
                str(Path(sys.executable).resolve()),
                "-I",
                "-B",
                str(script),
                "--worker",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={"PATH": "/usr/bin:/bin", "LANG": "en_US.UTF-8"},
            cwd=output,
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("Worker pipes unavailable")
        self.target, self.source = self.process.stdin, self.process.stdout

    def exchange(
        self, request: dict[str, Any], db: sqlite3.Connection, details: bool = False
    ) -> dict[str, Any]:
        send(request, self.target)
        while True:
            line = self.source.readline()
            if not line:
                error = self.process.stderr.read() if self.process.stderr else ""
                raise RuntimeError("Isolated portfolio worker stopped: " + error[-2000:])
            response = json.loads(line)
            if response["kind"] == "ack":
                return response
            with db:
                for row in response["rows"]:
                    summary = row["summary"]
                    key = (summary["configuration"], summary["market"], summary["phase"])
                    if details:
                        expected = db.execute(
                            "SELECT data FROM results WHERE configuration=? "
                            "AND market=? AND phase=?",
                            key,
                        ).fetchone()
                        if expected is None or json.loads(expected[0]) != summary:
                            raise ValueError("Detailed replay changed the original result")
                        db.execute(
                            "INSERT OR REPLACE INTO details VALUES (?,?,?,?)",
                            (*key, gzip.compress(encode(row).encode(), mtime=0)),
                        )
                    else:
                        db.execute(
                            "INSERT OR REPLACE INTO results VALUES (?,?,?,?,?,?)",
                            (*key, summary["equity"], summary["return_pct"], encode(summary)),
                        )

    def close(self) -> None:
        self.target.close()
        self.process.wait()
        self.source.close()
        if self.process.stderr:
            self.process.stderr.close()
        if self.process.returncode:
            raise RuntimeError("Isolated portfolio worker exited unsuccessfully")


def seal_frames(
    db: sqlite3.Connection,
    source: sqlite3.Connection,
    dataset: sqlite3.Connection,
    specs: list[dict[str, Any]],
    ids: dict[tuple[str, str], int],
    output: Path,
) -> None:
    if meta(db, "frames_complete", False):
        return
    quotes = iter(
        dataset.execute(
            "SELECT c.at,c.market,c.symbol,c.close FROM candles c JOIN symbols s "
            "ON c.market=s.market AND c.symbol=s.symbol WHERE s.status='done' "
            "ORDER BY c.at,c.market,c.symbol"
        )
    )
    quote = next(quotes, None)
    for frame in source.execute("SELECT id,at,output_sha256,basis_gzip FROM frames ORDER BY id"):
        fresh = {}
        marks = {}
        while quote is not None and quote[0] <= frame["at"]:
            symbol = ids[(quote[1], quote[2])]
            marks[symbol] = [float(quote[3]), quote[0]]
            if quote[0] == frame["at"]:
                fresh[symbol] = float(quote[3])
            quote = next(quotes, None)
        cached = db.execute("SELECT id FROM frames WHERE id=?", (frame["id"],)).fetchone()
        if cached:
            continue
        raw = gzip.decompress(frame["basis_gzip"])
        if hashlib.sha256(raw).hexdigest() != frame["output_sha256"]:
            raise ValueError("Original causal feature seal changed")
        features = json.loads(raw)["features"]
        if any(row["quote_at"] > frame["at"] for row in features):
            raise ValueError("Future quote in causal features")
        request = {
            "kind": "frame",
            "tick": frame["id"],
            "at": frame["at"],
            "quotes": fresh,
            "marks": marks,
            "ranks": rank_frame(features, specs, ids, frame["at"]),
        }
        raw = encode(request).encode()
        with db:
            db.execute(
                "INSERT INTO frames VALUES (?,?,?,?)",
                (
                    frame["id"],
                    frame["at"],
                    hashlib.sha256(raw).hexdigest(),
                    gzip.compress(raw, compresslevel=1, mtime=0),
                ),
            )
        if frame["id"] % 6 == 0:
            progress(db, output, "sealing_causal_inputs", tick=frame["id"], at=frame["at"])
    with db:
        put(db, "frames_complete", True)


def replay(
    db: sqlite3.Connection,
    dataset: Path,
    output: Path,
    configs: list[dict[str, Any]],
    scopes: dict[str, Any],
    detailed: bool = False,
) -> None:
    worker = Worker(dataset, output)
    started = time.monotonic()
    try:
        check = worker.exchange(
            {"kind": "probe", "paths": [str(dataset), str(output / "portfolios.sqlite3")]}, db
        )["isolation"]
        if not check or not all(check.values()):
            raise RuntimeError("Portfolio worker isolation failed")
        with db:
            put(db, "isolation", check)
        worker.exchange(
            {"kind": "init", "scopes": scopes, "configs": configs, "detailed": detailed}, db
        )
        for frame in db.execute("SELECT * FROM frames ORDER BY id"):
            raw = gzip.decompress(frame["payload"])
            if hashlib.sha256(raw).hexdigest() != frame["sha256"]:
                raise ValueError("Portfolio input frame seal changed")
            worker.exchange(json.loads(raw), db, detailed)
            if frame["id"] % 3 == 0:
                progress(
                    db,
                    output,
                    "recording_winner_trades" if detailed else "simulating",
                    tick=frame["id"],
                    at=frame["at"],
                    elapsed_seconds=round(time.monotonic() - started, 1),
                    result_rows=db.execute("SELECT COUNT(*) FROM results").fetchone()[0],
                )
    finally:
        worker.close()


def row_result(db: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    config = json.loads(
        db.execute(
            "SELECT data FROM configurations WHERE id=?", (row["configuration"],)
        ).fetchone()[0]
    )
    return {"parameters": config, **json.loads(row["data"])}


def select_validation(db: sqlite3.Connection) -> list[dict[str, Any]]:
    winners = []
    for market in ("KR", "US"):
        shortlisted = set()
        groups = set()
        for row in db.execute(
            "SELECT * FROM results WHERE market=? AND phase='train' "
            "ORDER BY equity DESC,configuration",
            (market,),
        ):
            cfg = json.loads(
                db.execute(
                    "SELECT data FROM configurations WHERE id=?", (row["configuration"],)
                ).fetchone()[0]
            )
            signal = cfg["entry"]
            key = (signal["family"], signal["lookback"], signal["side"], cfg["leverage"])
            if key not in groups:
                groups.add(key)
                shortlisted.add(row["configuration"])
        for leverage in LEVERAGES:
            selected = None
            for row in db.execute(
                "SELECT * FROM results WHERE market=? AND phase='validation' "
                "ORDER BY equity DESC,configuration",
                (market,),
            ):
                if row["configuration"] not in shortlisted:
                    continue
                cfg = json.loads(
                    db.execute(
                        "SELECT data FROM configurations WHERE id=?", (row["configuration"],)
                    ).fetchone()[0]
                )
                if cfg["leverage"] == leverage:
                    selected = row
                    break
            if selected:
                row = db.execute(
                    "SELECT * FROM results WHERE configuration=? AND market=? AND phase='test'",
                    (selected["configuration"], market),
                ).fetchone()
                winners.append(
                    {
                        "market": market,
                        "leverage": leverage,
                        "validation": row_result(db, selected),
                        "test": row_result(db, row),
                    }
                )
    return winners


def export_report(db: sqlite3.Connection, output: Path) -> dict[str, Any]:
    winners = select_validation(db)
    leaders = []
    for market, leverage in product(("KR", "US"), LEVERAGES):
        top = []
        for row in db.execute(
            "SELECT * FROM results WHERE market=? AND phase='full' "
            "ORDER BY equity DESC,configuration",
            (market,),
        ):
            result = row_result(db, row)
            if result["parameters"]["leverage"] == leverage:
                top.append(result)
                if len(top) == 10:
                    break
        leaders.append({"market": market, "leverage": leverage, "rows": top})
    report = {
        "state": "ranked",
        "ai_calls": 0,
        "initial_equity_per_run": INITIAL,
        "configurations_per_market": db.execute("SELECT COUNT(*) FROM configurations").fetchone()[
            0
        ],
        "result_rows": db.execute("SELECT COUNT(*) FROM results").fetchone()[0],
        "full_day_exploratory_leaders": leaders,
        "chronological_winners": winners,
        "interpretation": "Full-day leaders are in-sample exploratory fits. Validation "
        "winners are selected from train shortlists without test returns. One trading "
        "day is not independent next-day validation. Ending equity includes marked open "
        "positions and debt; it is not forced liquidation. All fills use current exact "
        "five-minute closing quotes, with zero costs and constant FX.",
    }
    (output / "report.json").write_text(json.dumps(report, indent=2))
    fields = [
        "configuration",
        "market",
        "phase",
        "equity",
        "return_pct",
        "cash",
        "debt",
        "realized",
        "unrealized",
        "max_drawdown_pct",
        "entries",
        "closed",
        "wins",
        "open_positions",
        "stale_open_positions",
        "ever_nonpositive_equity",
        "parameters",
    ]
    with (output / "rankings.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in db.execute(
            "SELECT * FROM results ORDER BY market,phase,equity DESC,configuration"
        ):
            result = row_result(db, row)
            result["parameters"] = encode(result["parameters"])
            writer.writerow(result)
    return report


def detail_configs(report: dict[str, Any]) -> list[dict[str, Any]]:
    chosen: dict[int, dict[str, Any]] = {}
    for group in report["full_day_exploratory_leaders"]:
        for result in group["rows"]:
            cfg = result["parameters"]
            chosen.setdefault(cfg["id"], {**cfg, "markets": []})["markets"].append(group["market"])
    for group in report["chronological_winners"]:
        cfg = group["test"]["parameters"]
        chosen.setdefault(cfg["id"], {**cfg, "markets": []})["markets"].append(group["market"])
    for cfg in chosen.values():
        cfg["markets"] = sorted(set(cfg["markets"]))
    return list(chosen.values())


def run(dataset: Path, feature_cache: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (output / "search.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = sqlite3.connect(output / "portfolios.sqlite3")
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.executescript(SCHEMA)
        source = sqlite3.connect(feature_cache.as_uri() + "?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        raw = sqlite3.connect(dataset.as_uri() + "?mode=ro", uri=True)
        try:
            old_manifest = meta(source, "manifest")
            if not meta(source, "feature_complete"):
                raise ValueError("Causal feature extraction is not complete")
            if old_manifest["dataset_sha256"] != digest(dataset):
                raise ValueError("Dataset does not match the sealed feature source")
            specs = signals()
            configs = configurations(len(specs))
            configs = [{**cfg, "entry": specs[cfg["signal"]]} for cfg in configs]
            from tools.replay_market_day import sandbox_profile

            manifest = {
                "dataset": str(dataset),
                "dataset_sha256": old_manifest["dataset_sha256"],
                "feature_cache": str(feature_cache),
                "feature_cache_sha256": digest(feature_cache),
                "code_sha256": digest(Path(__file__)),
                "sandbox_code_sha256": digest(
                    Path(sys.modules[sandbox_profile.__module__].__file__ or "")
                ),
                "configs_sha256": hashlib.sha256(encode(configs).encode()).hexdigest(),
                "seed": SEED,
                "initial_equity": INITIAL,
                "profiles_per_signal": 12,
                "signal_count": len(specs),
                "configuration_count_per_market": len(configs),
                "scopes": old_manifest["scopes"],
                "costs": 0,
                "fx": "constant",
                "sample": "Fixed signal grid with two common profiles and ten seeded "
                "random portfolio profiles per signal; not an exhaustive Cartesian grid",
            }
            old = meta(db, "manifest")
            if old is not None and old != manifest:
                raise ValueError("Inputs, code, or parameters changed; use a new output directory")
            if meta(db, "progress", {}).get("stage") == "complete":
                return
            with db:
                put(db, "manifest", manifest)
                db.executemany(
                    "INSERT OR IGNORE INTO configurations VALUES (?,?)",
                    [(cfg["id"], encode(cfg)) for cfg in configs],
                )
            progress(
                db,
                output,
                "starting",
                configurations_per_market=len(configs),
                portfolios=len(configs) * 2,
            )
            symbols = list(
                raw.execute(
                    "SELECT market,symbol FROM symbols WHERE status='done' ORDER BY market,symbol"
                )
            )
            ids = {tuple(row): index for index, row in enumerate(symbols)}
            with db:
                put(db, "symbols", symbols)
            seal_frames(db, source, raw, specs, ids, output)
            if not meta(db, "simulation_complete", False):
                replay(db, dataset, output, configs, manifest["scopes"])
                count = db.execute("SELECT COUNT(*) FROM results").fetchone()[0]
                if count != len(configs) * 2 * 4:
                    raise ValueError(f"Incomplete portfolio results: {count}")
                with db:
                    put(db, "simulation_complete", True)
            progress(db, output, "ranking")
            report = export_report(db, output)
            replay(db, dataset, output, detail_configs(report), manifest["scopes"], detailed=True)
            if (
                digest(dataset) != manifest["dataset_sha256"]
                or digest(feature_cache) != manifest["feature_cache_sha256"]
            ):
                raise ValueError("Source data changed during the experiment")
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Portfolio database integrity check failed")
            report["state"] = "complete"
            (output / "report.json").write_text(json.dumps(report, indent=2))
            progress(
                db,
                output,
                "complete",
                portfolios=len(configs) * 2,
                result_rows=report["result_rows"],
                detailed_rows=db.execute("SELECT COUNT(*) FROM details").fetchone()[0],
            )
        except BaseException as error:
            progress(db, output, "failed", error_type=type(error).__name__, message=str(error))
            raise
        finally:
            raw.close()
            source.close()
            db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--features", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if args.worker:
        worker_loop()
        return
    if args.output is None:
        parser.error("--output is required")
    if args.status:
        db = sqlite3.connect(
            (args.output / "portfolios.sqlite3").resolve().as_uri() + "?mode=ro", uri=True
        )
        try:
            print(json.dumps(meta(db, "progress"), indent=2))
        finally:
            db.close()
        return
    if args.dataset is None or args.features is None:
        parser.error("--dataset and --features are required")
    run(args.dataset.resolve(), args.features.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
