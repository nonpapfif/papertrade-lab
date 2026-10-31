"""Causal five-minute forecasts and event-gated AI request planning without inference."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import socket
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any, TextIO

STEP = 300
ALGORITHMS = (
    "flat", "up_10bp", "momentum_5m", "reversion_5m", "trend_15m", "acceleration",
)
GRID_LOOKBACKS = (5, 10, 15, 30, 60)
GRID_FAMILIES = (
    "momentum", "reversal", "breakout", "mean_reversion", "transition", "volume_trend",
)
GRID_MOVES = (0.0, 0.001, 0.003, 0.005, 0.01)
GRID_VOLUMES = (0.0, 1.0, 1.5, 2.0, 3.0)
GRID_NOISES = (0.0, 1.0, 2.0, 3.0)
GRID_SCORE_READY = 150
GRID_VOLUME = 180
GRID_VOLUME_READY = 205
GRID_NOISE = 210
GRID_NOISE_READY = 230
GRID_COMMON = 235
GRID_BITS = 236
SCHEMA = """
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE ticks (
    id INTEGER PRIMARY KEY, at TEXT UNIQUE NOT NULL, observations INTEGER NOT NULL,
    candidates INTEGER NOT NULL, input_sha256 TEXT NOT NULL, predictions_sha256 TEXT NOT NULL,
    prediction_committed_at TEXT NOT NULL, evaluated_at TEXT
);
CREATE TABLE cases (
    tick_id INTEGER, market TEXT, symbol TEXT, quote_at TEXT NOT NULL, price REAL NOT NULL,
    features TEXT NOT NULL, predictions TEXT NOT NULL, events TEXT NOT NULL,
    actual_return REAL, outcome TEXT, target_quote_at TEXT,
    PRIMARY KEY (tick_id, market, symbol)
) WITHOUT ROWID;
CREATE TABLE ai_requests (
    tick_id INTEGER PRIMARY KEY, candidates INTEGER NOT NULL, input_bytes INTEGER NOT NULL,
    payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL
);
"""


def encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def parse_at(value: str) -> datetime:
    at = datetime.fromisoformat(value)
    if at.tzinfo is None:
        raise ValueError("Timestamps require a timezone")
    return at.astimezone(UTC)


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def algorithm_predictions(row: dict[str, Any]) -> dict[str, float | None]:
    r5, r15 = row["return_5m"], row["return_15m"]
    return {
        "flat": 0.0, "up_10bp": 0.001,
        "momentum_5m": r5, "reversion_5m": -r5 if r5 is not None else None,
        "trend_15m": r15 / 3 if r15 is not None else None,
        "acceleration": r5 - (r15 - r5) / 2
        if r5 is not None and r15 is not None else None,
    }


def event_conditions(row: dict[str, Any]) -> list[str]:
    r5, r15 = row["return_5m"], row["return_15m"]
    if r5 is None or r5 == 0:
        return []
    volume = row["relative_volume"]
    noise = row["prior_minute_noise"]
    events = []
    if volume is not None and volume >= 2 and (
        row["price"] > (row["prior_high"] or math.inf)
        or row["price"] < (row["prior_low"] or 0)
    ):
        events.append("volume_breakout")
    if noise is not None and abs(r5) >= max(0.005, 3 * noise * math.sqrt(5)):
        events.append("unusual_move")
    z = row["prior_price_zscore"]
    if (r15 is not None and r5 * r15 > 0 and volume is not None and volume >= 2
            and z is not None and abs(z) >= 3 and abs(r5) >= 0.005):
        events.append("trend_reversal_conflict")
    return events


def probe(paths: list[str]) -> dict[str, bool]:
    result = {}
    for index, path in enumerate(paths):
        try:
            with open(path, "rb") as stream:
                stream.read(1)
        except PermissionError:
            result[f"file_{index}_denied"] = True
        else:
            result[f"file_{index}_denied"] = False
    try:
        with socket.socket() as connection:
            connection.connect(("127.0.0.1", 1))
    except PermissionError:
        result["network_denied"] = True
    except OSError:
        result["network_denied"] = False
    else:
        result["network_denied"] = False
    try:
        subprocess.run(["/usr/bin/true"], check=False, capture_output=True)
    except PermissionError:
        result["child_exec_denied"] = True
    else:
        result["child_exec_denied"] = False
    return result


def grid_mask(row: dict[str, Any]) -> int:
    scores, volumes, noises = row["scores"], row["volumes"], row["noises"]
    if len(scores) != 30 or len(volumes) != 5 or len(noises) != 5:
        raise ValueError("Invalid grid feature dimensions")
    if any(value is not None and not math.isfinite(value)
           for value in [*scores, *volumes, *noises]):
        raise ValueError("Grid features must be finite or null")
    result = 0
    for index, value in enumerate(scores):
        if value is None:
            continue
        result |= 1 << (GRID_SCORE_READY + index)
        for threshold_index, threshold in enumerate(GRID_MOVES):
            if value > 0 if threshold == 0 else value >= threshold:
                result |= 1 << (index * 5 + threshold_index)
    for values, thresholds, offset, ready in (
        (volumes, GRID_VOLUMES, GRID_VOLUME, GRID_VOLUME_READY),
        (noises, GRID_NOISES, GRID_NOISE, GRID_NOISE_READY),
    ):
        for index, value in enumerate(values):
            if value is not None:
                result |= 1 << (ready + index)
            for threshold_index, threshold in enumerate(thresholds):
                if threshold == 0 or value is not None and value >= threshold:
                    result |= 1 << (offset + index * len(thresholds) + threshold_index)
    if all(value is not None for value in [*scores, *volumes, *noises]):
        result |= 1 << GRID_COMMON
    return result


def worker_loop(source: TextIO, target: TextIO) -> None:
    previous: dict[tuple[str, str], tuple[str, set[tuple[str, int]]]] = {}
    for line in source:
        request = json.loads(line)
        if request["kind"] == "probe":
            response: Any = probe(request["paths"])
        elif request["kind"] == "grid":
            response = [{"market": row["market"], "symbol": row["symbol"],
                         "mask": hex(grid_mask(row))} for row in request["rows"]]
        else:
            response = []
            at = request["at"]
            for row in request["rows"]:
                key = (row["market"], row["symbol"])
                r5 = row["return_5m"] or 0
                direction = 1 if r5 > 0 else -1
                active = {(event, direction) for event in event_conditions(row)}
                old_at, old = previous.get(key, (at, set()))
                if (parse_at(at) - parse_at(old_at)).total_seconds() > STEP:
                    old = set()
                new = sorted(event for event, side in active - old)
                previous[key] = (at, active)
                response.append({"market": key[0], "symbol": key[1],
                                 "predictions": algorithm_predictions(row), "events": new})
        target.write(encode(response) + "\n")
        target.flush()


def sandbox_profile(script: Path, dataset: Path, output: Path) -> str:
    binary = Path(sys.executable).resolve()
    lines = ["(version 1)", "(allow default)", "(deny network*)", "(deny file-write*)",
             "(deny process-exec)",
             f"(allow process-exec (literal {encode(str(binary))}))"]
    for path in (Path.home(), dataset.parent, output):
        lines.append(f"(deny file-read* (subpath {encode(str(path.resolve()))}))")
    lines.extend([
        f"(allow file-read* (subpath {encode(str(Path(sys.prefix).resolve()))}))",
        f"(allow file-read* (literal {encode(str(script.resolve()))}))",
    ])
    return "\n".join(lines)


class Worker:
    def __init__(self, dataset: Path, output: Path) -> None:
        script = Path(__file__).resolve()
        profile = sandbox_profile(script, dataset, output)
        self.process = subprocess.Popen(
            ["/usr/bin/sandbox-exec", "-p", profile, str(Path(sys.executable).resolve()),
             "-I", "-B", str(script), "--worker"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env={"PATH": "/usr/bin:/bin", "LANG": "en_US.UTF-8"}, cwd=output,
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("Worker pipes are unavailable")
        self.source = self.process.stdout
        self.target = self.process.stdin

    def exchange(self, request: dict[str, Any]) -> Any:
        self.target.write(encode(request) + "\n")
        self.target.flush()
        line = self.source.readline()
        if not line:
            error = self.process.stderr.read() if self.process.stderr else ""
            raise RuntimeError("Isolated worker stopped: " + error[-1000:])
        return json.loads(line)

    def close(self) -> None:
        self.target.close()
        self.process.wait()
        self.source.close()
        if self.process.stderr:
            self.process.stderr.close()
        if self.process.returncode:
            raise RuntimeError("Isolated worker exited unsuccessfully")


class History:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
        self.touched: set[tuple[str, str]] = set()

    def add(self, row: dict[str, Any]) -> None:
        key = (row["market"], row["symbol"])
        self.rows[key].append(row)
        self.touched.add(key)

    def features(self, at: datetime, scopes: dict[str, Any]) -> list[dict[str, Any]]:
        result = []
        boundaries = [at - timedelta(minutes=5 * i) for i in range(5)]
        for key in sorted(self.touched):
            market, symbol = key
            if at + timedelta(seconds=STEP) > parse_at(scopes[market]["end"]):
                continue
            rows = self.rows[key]
            cutoff = boundaries[4]
            while len(rows) > 1 and rows[1]["time"] <= cutoff:
                rows.popleft()
            last = rows[-1]
            if last["time"] > at:
                raise ValueError("Feature history contains future observations")
            recent = [r for r in rows if boundaries[1] < r["time"] <= at]
            if not recent:
                continue
            prior = [r for r in rows if cutoff < r["time"] <= boundaries[1]]
            reference5 = next((r for r in reversed(rows)
                               if r["time"] <= boundaries[1]), None)
            reference15 = next((r for r in reversed(rows)
                                if r["time"] <= boundaries[3]), None)
            volumes = [sum(r["volume"] for r in rows
                           if boundaries[i + 1] < r["time"] <= boundaries[i])
                       for i in (1, 2, 3)]
            baseline = sum(volumes) / 3
            noise = [abs(b["close"] / a["close"] - 1) for a, b in pairwise(prior)
                     if (b["time"] - a["time"]).total_seconds() == 60]
            closes = [r["close"] for r in prior]
            mean = sum(closes) / len(closes) if closes else 0
            std = math.sqrt(sum((c - mean) ** 2 for c in closes) / len(closes)) if closes else 0
            result.append({
                "market": market, "symbol": symbol, "price": last["close"],
                "quote_at": last["time"].isoformat(),
                "price_age_seconds": (at - last["time"]).total_seconds(),
                "return_5m": last["close"] / reference5["close"] - 1
                if reference5 else None,
                "return_15m": last["close"] / reference15["close"] - 1
                if reference15 else None,
                "reference_5m_at": reference5["time"].isoformat() if reference5 else None,
                "reference_15m_at": reference15["time"].isoformat() if reference15 else None,
                "minute_observations": len(recent),
                "volume": sum(r["volume"] for r in recent),
                "relative_volume": sum(r["volume"] for r in recent) / baseline
                if baseline > 0 and len(volumes) == 3 and reference15 else None,
                "prior_high": max(r["high"] for r in prior) if prior else None,
                "prior_low": min(r["low"] for r in prior) if prior else None,
                "prior_minute_noise": sum(noise) / len(noise) if noise else None,
                "prior_price_zscore": (last["close"] - mean) / std if std else None,
            })
        self.touched.clear()
        return result


def settle(
    output: sqlite3.Connection, tick_id: int, pending: list[dict[str, Any]],
    at: datetime, history: History, metrics: dict[tuple[str, str], dict[str, Any]],
) -> None:
    for case in pending:
        key = (case["market"], case["symbol"])
        last = history.rows[key][-1]
        exact = last["time"] == at and case["price_age_seconds"] == 0
        actual = last["close"] / case["price"] - 1 if exact else None
        output.execute(
            "UPDATE cases SET actual_return=?,outcome=?,target_quote_at=? "
            "WHERE tick_id=? AND market=? AND symbol=?",
            (actual, "observed" if exact else "missing_exact_endpoint",
             last["time"].isoformat(), tick_id, *key),
        )
        for algorithm, prediction in case["predictions"].items():
            meter = metrics.setdefault((key[0], algorithm), {
                "predictions": 0, "scored": 0, "missing_endpoint": 0,
                "not_ready": 0, "correct_direction": 0, "nonflat_actual": 0,
                "absolute_error": 0.0, "signed_return_sum": 0.0, "active_predictions": 0,
            })
            meter["predictions"] += 1
            if prediction is None:
                meter["not_ready"] += 1
            elif actual is None:
                meter["missing_endpoint"] += 1
            else:
                meter["scored"] += 1
                meter["absolute_error"] += abs(prediction - actual)
                if actual != 0:
                    meter["nonflat_actual"] += 1
                    meter["correct_direction"] += int((prediction > 0) == (actual > 0)
                                                       and prediction != 0)
                if prediction != 0:
                    meter["active_predictions"] += 1
                    meter["signed_return_sum"] += actual if prediction > 0 else -actual
    if pending:
        output.execute("UPDATE ticks SET evaluated_at=? WHERE id=?",
                       (datetime.now(UTC).isoformat(), tick_id))


def write_report(
    output: sqlite3.Connection, path: Path,
    metrics: dict[tuple[str, str], dict[str, Any]], gates: Counter[str],
) -> dict[str, Any]:
    results = []
    for (market, algorithm), meter in sorted(metrics.items()):
        results.append({"market": market, "algorithm": algorithm, **meter,
                        "direction_accuracy_nonflat": meter["correct_direction"]
                        / meter["nonflat_actual"] if meter["nonflat_actual"] else None,
                        "mean_absolute_error": meter["absolute_error"] / meter["scored"]
                        if meter["scored"] else None,
                        "mean_signed_return": meter["signed_return_sum"]
                        / meter["active_predictions"] if meter["active_predictions"] else None})
    calls = output.execute("SELECT COUNT(*),COALESCE(SUM(candidates),0),"
                           "COALESCE(SUM(input_bytes),0),COALESCE(MAX(input_bytes),0) "
                           "FROM ai_requests").fetchone()
    report = {"state": "complete", "ai_calls_executed": 0, "ai_requests_planned": calls[0],
              "candidate_events": calls[1], "total_ai_payload_bytes": calls[2],
              "largest_ai_payload_bytes": calls[3], "event_types": dict(gates),
              "forecast_cases": output.execute("SELECT COUNT(*) FROM cases").fetchone()[0],
              "ticks": output.execute("SELECT COUNT(*) FROM ticks").fetchone()[0],
              "outcomes": {r[0]: r[1] for r in output.execute(
                  "SELECT outcome,COUNT(*) FROM cases GROUP BY outcome")},
              "results": results,
              "interpretation": "Same-day exploratory forecasts, not validated trading returns"}
    (path / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
    return report


def replay(dataset: Path, destination: Path) -> dict[str, Any]:
    if destination.exists():
        raise FileExistsError("Use a new directory for each replay")
    destination.mkdir(parents=True, mode=0o700)
    source = sqlite3.connect(dataset.resolve().as_uri() + "?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    source.execute("PRAGMA query_only=ON")
    output = sqlite3.connect(destination / "replay.sqlite3")
    output.executescript(SCHEMA)
    worker = None
    try:
        meta = {r[0]: json.loads(r[1]) for r in source.execute("SELECT * FROM metadata")}
        if meta["state"] not in ("complete", "complete_with_errors"):
            raise ValueError("Source collection is not finished")
        scopes = meta["scopes"]
        manifest = {"dataset_sha256": hash_file(dataset),
                    "collector_date": meta["date"], "step_seconds": STEP,
                    "code_sha256": hash_file(Path(__file__)), "algorithms": list(ALGORITHMS),
                    "evaluation": "Exact observed endpoint prices only; no interpolation",
                    "source_state": meta["state"], "ai_mode": "plan_only"}
        (destination / "manifest.json").write_text(json.dumps(manifest, indent=2))
        canary = destination / "private-canary.txt"
        canary.write_text("Future outcomes must not be readable by the predictor")
        worker = Worker(dataset, destination)
        isolation = worker.exchange({"kind": "probe", "paths": [str(dataset.resolve()),
                                    str(canary.resolve()),
                                    str((destination / "replay.sqlite3").resolve())]})
        if not isolation or not all(isolation.values()):
            raise RuntimeError("Worker isolation checks did not pass: " + encode(isolation))
        manifest["isolation"] = isolation
        (destination / "manifest.json").write_text(json.dumps(manifest, indent=2))
        output.execute("INSERT INTO metadata VALUES ('manifest',?)", (encode(manifest),))
        output.commit()
        iterator = iter(source.execute(
            "SELECT c.* FROM candles c JOIN symbols s ON s.market=c.market AND s.symbol=c.symbol "
            "WHERE s.status='done' ORDER BY c.at,c.market,c.symbol"
        ))
        raw = next(iterator, None)
        history = History()
        at = min(parse_at(scope["start"]) for scope in scopes.values()) + timedelta(seconds=STEP)
        end = max(parse_at(scope["end"]) for scope in scopes.values())
        metrics: dict[tuple[str, str], dict[str, Any]] = {}
        gates: Counter[str] = Counter()
        pending: list[dict[str, Any]] = []
        tick_id = 0
        while at <= end:
            while raw is not None and parse_at(raw["at"]) <= at:
                history.add({"market": raw["market"], "symbol": raw["symbol"],
                             "time": parse_at(raw["at"]), "close": float(raw["close"]),
                             "high": float(raw["high"]), "low": float(raw["low"]),
                             "volume": float(raw["volume"])})
                raw = next(iterator, None)
            with output:
                settle(output, tick_id, pending, at, history, metrics)
            features = history.features(at, scopes)
            pending = []
            if features:
                tick_id += 1
                request = {"kind": "predict", "at": at.isoformat(), "rows": features}
                decisions = worker.exchange(request)
                keys = [(r["market"], r["symbol"]) for r in features]
                if [(r["market"], r["symbol"]) for r in decisions] != keys:
                    raise ValueError("Worker changed the prediction universe")
                candidates = []
                for feature, decision in zip(features, decisions, strict=True):
                    if set(decision["predictions"]) != set(ALGORITHMS) or any(
                        value is not None and not math.isfinite(value)
                        for value in decision["predictions"].values()
                    ):
                        raise ValueError("Invalid algorithm prediction")
                    pending.append({**feature, "predictions": decision["predictions"]})
                    if decision["events"]:
                        gates.update(decision["events"])
                        candidates.append({**feature, "predictions": decision["predictions"],
                                           "events": decision["events"]})
                with output:
                    output.execute("INSERT INTO ticks VALUES (?,?,?,?,?,?,?,NULL)", (
                        tick_id, at.isoformat(), len(features), len(candidates),
                        hashlib.sha256(encode(request).encode()).hexdigest(),
                        hashlib.sha256(encode(decisions).encode()).hexdigest(),
                        datetime.now(UTC).isoformat(),
                    ))
                    for feature, decision in zip(features, decisions, strict=True):
                        output.execute("INSERT INTO cases VALUES (?,?,?,?,?,?,?,?,NULL,NULL,NULL)", (
                            tick_id, feature["market"], feature["symbol"], feature["quote_at"],
                            feature["price"], encode(feature), encode(decision["predictions"]),
                            encode(decision["events"]),
                        ))
                    if candidates:
                        payload = encode({"at": at.isoformat(), "horizon_seconds": STEP,
                                          "candidates": candidates})
                        output.execute("INSERT INTO ai_requests VALUES (?,?,?,?,?,?)", (
                            tick_id, len(candidates), len(payload.encode()),
                            hashlib.sha256(payload.encode()).hexdigest(), payload, "planned_only",
                        ))
                if tick_id % 12 == 0:
                    print(encode({"at": at.isoformat(), "ticks": tick_id,
                                  "observations": len(features), "ai_candidates": len(candidates)}),
                          flush=True)
            at += timedelta(seconds=STEP)
        if pending:
            raise RuntimeError("Forecasts remain without a completed evaluation step")
        if output.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("Replay database integrity verification failed")
        if hash_file(dataset) != manifest["dataset_sha256"]:
            raise RuntimeError("Source dataset changed during replay")
        return write_report(output, destination, metrics, gates)
    finally:
        try:
            if worker is not None:
                worker.close()
        finally:
            output.close()
            source.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker_loop(sys.stdin, sys.stdout)
        return
    if args.dataset is None or args.output is None:
        parser.error("--dataset and --output are required")
    os.umask(0o077)
    result = replay(args.dataset.resolve(), args.output.resolve())
    print(encode({k: v for k, v in result.items() if k != "results"}), flush=True)


if __name__ == "__main__":
    main()
