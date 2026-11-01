"""Run parallel, time-bounded October 26-30 portfolio hypotheses without AI calls."""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import importlib.util
import json
import math
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DAYS = tuple(f"2026-10-{day}" for day in range(26, 31))
ROOT = Path(__file__).resolve().parents[1]
FAMILIES = ("momentum", "reversal", "breakout", "mean_reversion", "transition", "volume_trend")


def encode(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encode(value) + "\n")
    temporary.replace(path)


def metadata(db: sqlite3.Connection, key: str) -> Any:
    row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    if row is None:
        raise ValueError("Missing experiment metadata: " + key)
    return json.loads(row[0])


def readonly(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    return db


def policies() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = [
        {"id": "fixed_oct30", "mode": "fixed30"},
        {"id": "fixed_oct30_independent_days", "mode": "fixed30", "reset_daily": True},
    ]
    result.extend(
        {"id": "static_" + family, "mode": "static", "families": [family]} for family in FAMILIES
    )
    result.extend(
        [
            {"id": "daily_learning_carry_account", "mode": "daily"},
            {"id": "daily_learning_reset_account", "mode": "daily", "reset_daily": True},
            {"id": "cumulative_every_check", "mode": "cumulative", "refresh_ticks": 1},
            {"id": "cumulative_every_30min", "mode": "cumulative", "refresh_ticks": 6},
        ]
    )
    result.extend(
        {
            "id": f"rolling_{window * 5}min_refresh_{refresh * 5}min",
            "mode": "rolling",
            "window": window,
            "refresh_ticks": refresh,
        }
        for window in (3, 6, 12, 24)
        for refresh in (1, 3, 6)
    )
    result.extend(
        [
            {"id": "previous_day_only", "mode": "previous_day"},
            {"id": "all_completed_prior_days", "mode": "prior_days"},
        ]
    )
    result.extend(
        {"id": "daily_family_" + family, "mode": "daily", "families": [family]}
        for family in FAMILIES
    )
    result.extend(
        {"id": f"random_seed_{seed}", "mode": "random", "seed": seed, "refresh_ticks": 3}
        for seed in (11, 29, 47)
    )
    return result


def locations(output: Path, day: str) -> tuple[Path, Path, Path]:
    dataset = ROOT / "state" / "datasets" / day / "market.sqlite3"
    if day == "2026-10-30":
        feature = ROOT / "state/searches/2026-10-30-grid-v1"
        portfolio = ROOT / "state/portfolios/2026-10-30-diverse-v1"
    else:
        feature, portfolio = output / "features" / day, output / "portfolios" / day
    return dataset, feature, portfolio


def prepare_day(output: Path, day: str) -> None:
    from tools.search_portfolios import run as portfolio_run
    from tools.search_up_candles import run as feature_run

    if day not in DAYS:
        raise ValueError("Only the five frozen experiment dates are allowed")
    dataset, feature, portfolio = locations(output, day)
    if day != "2026-10-30":
        feature_run(dataset, feature)
        portfolio_run(dataset, feature / "search.sqlite3", portfolio)
    with readonly(portfolio / "portfolios.sqlite3") as db:
        manifest = metadata(db, "manifest")
        if not metadata(db, "simulation_complete") or not metadata(db, "frames_complete"):
            raise ValueError("Daily reference simulation is incomplete")
        if manifest["dataset_sha256"] != digest(dataset):
            raise ValueError("Daily dataset does not match the sealed cache")
        feature_hash = digest(feature / "search.sqlite3")
        if manifest["feature_cache_sha256"] != feature_hash:
            raise ValueError("Daily causal feature cache changed")
    save(
        output / "jobs" / (day + ".json"),
        {
            "state": "complete",
            "day": day,
            "portfolio_sha256": digest(portfolio / "portfolios.sqlite3"),
            "feature_sha256": feature_hash,
            "updated_at": datetime.now(UTC).isoformat(),
            "ai_calls": 0,
        },
    )


def load_worker_modules() -> Any:
    package = types.ModuleType("tools")
    package.__path__ = []  # type: ignore[attr-defined]
    sys.modules["tools"] = package
    for name in ("search_portfolios", "week_hypotheses"):
        spec = importlib.util.spec_from_file_location(
            "tools." + name, ROOT / "tools" / (name + ".py")
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("Worker source module is unavailable")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    return sys.modules["tools.week_hypotheses"]


def probe(paths: list[str]) -> dict[str, bool]:
    checks = {}
    for path in paths:
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
    return checks


def worker_loop() -> None:
    module = load_worker_modules()
    engine = None
    for line in sys.stdin:
        request = json.loads(line)
        kind = request["kind"]
        response: dict[str, Any]
        if kind == "probe":
            response = {"isolation": probe(request["paths"])}
        elif kind == "init":
            engine = module.WeekEngine(
                request["configs"],
                request["specs"],
                request["fixed_ids"],
                request["policies"],
                market=request["market"],
            )
            response = {"ready": True}
        elif engine is None:
            raise ValueError("Worker is not initialized")
        elif kind == "day_start":
            engine.day_start(request["day"], request["scopes"])
            response = {"ready": True}
        elif kind == "frame":
            engine.step(request)
            response = {"ready": True}
        elif kind == "day_end":
            response = engine.day_end()
        elif kind == "report":
            response = engine.report()
        else:
            raise ValueError("Unknown weekly worker message")
        print(encode(response), flush=True)


class IsolatedWorker:
    def __init__(self, output: Path, result: Path, forbidden: list[Path], deadline: float) -> None:
        from tools.replay_market_day import sandbox_profile

        script = Path(__file__).resolve()
        profile = sandbox_profile(script, forbidden[0], output)
        for name in ("search_portfolios.py", "week_hypotheses.py"):
            profile += "\n(allow file-read* (literal " + encode(str(script.with_name(name))) + "))"
        for path in forbidden:
            profile += "\n(deny file-read* (literal " + encode(str(path.resolve())) + "))"
        self.error = (result / "worker-stderr.log").open("a")
        self.process = subprocess.Popen(
            [
                "/usr/bin/sandbox-exec",
                "-p",
                profile,
                str(Path(sys.executable).resolve()),
                "-I",
                "-B",
                str(script),
                "--worker",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.error,
            text=True,
            cwd=result,
            env={"PATH": "/usr/bin:/bin", "LANG": "en_US.UTF-8"},
            start_new_session=True,
        )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError("Weekly worker pipes unavailable")
        self.input, self.output = self.process.stdin, self.process.stdout
        self.expired = threading.Event()
        self.timer = threading.Timer(max(0, deadline - time.time()), self.expire)
        self.timer.daemon = True
        self.timer.start()

    def expire(self) -> None:
        self.expired.set()
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def exchange(self, request: dict[str, Any]) -> dict[str, Any]:
        if self.expired.is_set():
            raise TimeoutError("Seven-hour experiment deadline reached")
        self.input.write(encode(request) + "\n")
        self.input.flush()
        line = self.output.readline()
        if not line:
            raise RuntimeError("Isolated hypothesis worker stopped; inspect worker-stderr.log")
        result = json.loads(line)
        if not isinstance(result, dict):
            raise TypeError("Invalid isolated worker response")
        return result

    def close(self) -> None:
        self.timer.cancel()
        self.input.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            terminate(self.process)
        self.output.close()
        self.error.close()


def remap_frame(
    frame: dict[str, Any],
    local: list[list[str]],
    global_ids: dict[tuple[str, str], int],
    market: str,
    signals: set[int] | None = None,
) -> dict[str, Any]:
    market_index = int(market == "US")
    mapping = {
        index: global_ids[(symbol[0], symbol[1])]
        for index, symbol in enumerate(local)
        if symbol[0] == market
    }
    quotes = {
        mapping[int(key)]: value for key, value in frame["quotes"].items() if int(key) in mapping
    }
    marks = {
        mapping[int(key)]: value for key, value in frame["marks"].items() if int(key) in mapping
    }
    ranks: list[list[list[Any]]] = []
    for index, buckets in enumerate(frame["ranks"]):
        pair: list[list[Any]] = [[], []]
        if signals is None or index in signals:
            pair[market_index] = [[mapping[row[0]], *row[1:]] for row in buckets[market_index]]
        ranks.append(pair)
    return {
        "kind": "frame",
        "at": frame["at"],
        "tick": int(datetime.fromisoformat(frame["at"]).timestamp() // 300),
        "quotes": quotes,
        "marks": marks,
        "ranks": ranks,
    }


def fixed_winners(output: Path) -> dict[str, list[int]]:
    _, _, portfolio = locations(output, "2026-10-30")
    with readonly(portfolio / "portfolios.sqlite3") as db:
        result: dict[str, list[int]] = {"KR": [], "US": []}
        for market, identifiers in result.items():
            for leverage in (1, 2, 5, 10):
                row = db.execute(
                    "SELECT r.configuration FROM results r JOIN configurations c "
                    "ON c.id=r.configuration WHERE r.market=? AND r.phase='full' "
                    "AND json_extract(c.data,'$.leverage')=? ORDER BY r.equity DESC,"
                    "r.configuration LIMIT 1",
                    (market, leverage),
                ).fetchone()
                if row is None:
                    raise ValueError("Missing October 30 reference winner")
                identifiers.append(row[0])
        return result


def diagnostics(hypothesis: dict[str, Any]) -> dict[str, Any]:
    daily = hypothesis["daily"]
    trades = [trade for trade in hypothesis["trades"] if trade["realized_pnl"] is not None]
    positive = [trade for trade in trades if trade["realized_pnl"] > 0]
    gross_profit = sum(trade["realized_pnl"] for trade in positive)
    symbol_profit: dict[int, float] = {}
    for trade in positive:
        symbol = trade["symbol_id"]
        symbol_profit[symbol] = symbol_profit.get(symbol, 0) + trade["realized_pnl"]
    return {
        "profitable_days": sum(row["pnl"] > 0 for row in daily),
        "losing_days": sum(row["pnl"] < 0 for row in daily),
        "worst_daily_pnl": min((row["pnl"] for row in daily), default=None),
        "best_daily_pnl": max((row["pnl"] for row in daily), default=None),
        "largest_win_share_of_gross_realized_profit": max(
            (trade["realized_pnl"] / gross_profit for trade in positive), default=None
        ),
        "largest_symbol_share_of_gross_realized_profit": max(
            (value / gross_profit for value in symbol_profit.values()), default=None
        ),
        "selection_changes": sum(
            before["configuration"] != after["configuration"]
            for before, after in zip(hypothesis["choices"], hypothesis["choices"][1:])
        ),
    }


def evaluate(output: Path, market: str, retrospective: bool, manifest: dict[str, Any]) -> str:
    from tools.search_portfolios import configurations, signals

    deadline = datetime.fromisoformat(manifest["deadline"]).timestamp()
    verify_code(manifest)
    group = market + ("-retrospective" if retrospective else "-causal")
    result = output / "results" / group
    result.mkdir(parents=True, exist_ok=True)
    status_path = result / "status.json"
    all_specs = signals()
    configs = configurations(len(all_specs))
    fixed_ids = manifest["fixed_oct30_ids"] if retrospective else {}
    selected_signals = None
    if retrospective:
        identifiers = set(fixed_ids[market])
        configs = [config for config in configs if config["id"] in identifiers]
        selected_signals = {config["signal"] for config in configs}
    selected_policies = [
        p for p in manifest["policies"] if (p["mode"] == "fixed30") == retrospective
    ]
    ids = {tuple(value): index for index, value in enumerate(manifest["symbols"])}
    data_paths = [locations(output, day)[0] for day in DAYS]
    cache_paths = [locations(output, day)[2] / "portfolios.sqlite3" for day in DAYS]
    canary = result / "private-canary.txt"
    canary.write_text("Private scoring data is not a worker input")
    db = sqlite3.connect(result / "evaluation.sqlite3")
    db.execute(
        "CREATE TABLE IF NOT EXISTS sealed_inputs ("
        "sequence INTEGER PRIMARY KEY,day TEXT,at TEXT,sha256 TEXT,source_sha256 TEXT)"
    )
    db.execute("CREATE TABLE IF NOT EXISTS day_results (day TEXT PRIMARY KEY,payload_gzip BLOB)")
    db.commit()
    worker = IsolatedWorker(
        output, result, [*data_paths, *cache_paths, result / "evaluation.sqlite3", canary], deadline
    )
    last_at = ""
    previous_quotes: dict[int, float] = {}
    sequence = 0
    try:
        checks = worker.exchange(
            {
                "kind": "probe",
                "paths": [
                    str(p)
                    for p in [*data_paths, *cache_paths, result / "evaluation.sqlite3", canary]
                ],
            }
        )
        if not checks["isolation"] or not all(checks["isolation"].values()):
            raise RuntimeError("Weekly worker isolation checks did not all succeed")
        save(result / "isolation.json", checks)
        worker.exchange(
            {
                "kind": "init",
                "market": market,
                "configs": configs,
                "specs": all_specs,
                "fixed_ids": fixed_ids,
                "policies": selected_policies,
            }
        )
        for day, path in zip(DAYS, cache_paths, strict=True):
            sealed = json.loads((output / "jobs" / (day + ".json")).read_text())
            if digest(path) != sealed["portfolio_sha256"]:
                raise ValueError("Frozen portfolio cache changed: " + day)
            with readonly(path) as source:
                scope = metadata(source, "manifest")["scopes"]
                local = metadata(source, "symbols")
                worker.exchange({"kind": "day_start", "day": day, "scopes": scope})
                for row in source.execute("SELECT at,sha256,payload FROM frames ORDER BY id"):
                    if time.time() >= deadline:
                        raise TimeoutError("Seven-hour experiment deadline reached")
                    if not scope[market]["start"] <= row["at"] <= scope[market]["end"]:
                        continue
                    raw = gzip.decompress(row["payload"])
                    if hashlib.sha256(raw).hexdigest() != row["sha256"]:
                        raise ValueError("Daily causal frame digest mismatch")
                    frame = remap_frame(json.loads(raw), local, ids, market, selected_signals)
                    if frame["at"] <= last_at:
                        if frame["at"] != last_at or any(
                            previous_quotes[key] != price
                            for key, price in frame["quotes"].items()
                            if key in previous_quotes
                        ):
                            raise ValueError("Cross-day quotes disagree or move backwards")
                        continue
                    sequence += 1
                    with db:
                        db.execute(
                            "INSERT INTO sealed_inputs VALUES (?,?,?,?,?)",
                            (
                                sequence,
                                day,
                                frame["at"],
                                hashlib.sha256(encode(frame).encode()).hexdigest(),
                                row["sha256"],
                            ),
                        )
                    worker.exchange(frame)
                    last_at, previous_quotes = frame["at"], frame["quotes"]
                    if sequence % 12 == 0:
                        save(
                            status_path,
                            {
                                "state": "running",
                                "day": day,
                                "at": last_at,
                                "frames": sequence,
                                "updated_at": datetime.now(UTC).isoformat(),
                                "ai_calls": 0,
                            },
                        )
                daily = worker.exchange({"kind": "day_end"})
                if retrospective and day == "2026-10-30":
                    for hypothesis in daily["hypotheses"]:
                        if hypothesis["policy"] != "fixed_oct30_independent_days":
                            continue
                        identifier = next(
                            config["id"]
                            for config in configs
                            if config["leverage"] == hypothesis["leverage"]
                        )
                        original = source.execute(
                            "SELECT data FROM results WHERE configuration=? "
                            "AND market=? AND phase='full'",
                            (identifier, market),
                        ).fetchone()
                        expected = json.loads(original[0])
                        actual = hypothesis["summary"]
                        if not math.isclose(
                            actual["equity"], expected["equity"], rel_tol=1e-9, abs_tol=1e-6
                        ) or any(actual[key] != expected[key] for key in ("entries", "closed")):
                            raise ValueError(
                                "Frozen October 30 account failed original-result replay"
                            )
                with db:
                    db.execute(
                        "INSERT INTO day_results VALUES (?,?)",
                        (
                            day,
                            gzip.compress(encode(daily).encode(), compresslevel=1, mtime=0),
                        ),
                    )
                save(
                    status_path,
                    {
                        "state": "running",
                        "completed_day": day,
                        "frames": sequence,
                        "updated_at": datetime.now(UTC).isoformat(),
                        "ai_calls": 0,
                    },
                )
        report = worker.exchange({"kind": "report"})
        for hypothesis in report["hypotheses"]:
            hypothesis["diagnostics"] = diagnostics(hypothesis)
        (result / "report.json.gz").write_bytes(
            gzip.compress(encode(report).encode(), compresslevel=1, mtime=0)
        )
        summary = {k: v for k, v in report.items() if k != "hypotheses"}
        summary["hypotheses"] = [
            {k: v for k, v in hypothesis.items() if k not in ("trades", "curve", "choices")}
            for hypothesis in report["hypotheses"]
        ]
        save(result / "summary.json", summary)
        save(
            status_path,
            {
                "state": "complete",
                "frames": sequence,
                "ai_calls": 0,
                "updated_at": datetime.now(UTC).isoformat(),
            },
        )
        return group
    except BaseException as error:
        save(
            status_path,
            {
                "state": "deadline_reached" if time.time() >= deadline else "failed",
                "error_type": type(error).__name__,
                "message": str(error),
                "frames": sequence,
                "updated_at": datetime.now(UTC).isoformat(),
                "ai_calls": 0,
            },
        )
        raise
    finally:
        worker.close()
        db.close()


def make_manifest(output: Path, deadline: datetime, workers: int) -> dict[str, Any]:
    sources = {}
    for day in DAYS:
        path = locations(output, day)[0]
        with readonly(path) as db:
            if metadata(db, "state") not in ("complete", "complete_with_errors"):
                raise ValueError("Dataset is not complete: " + day)
            sources[day] = {
                "path": str(path),
                "sha256": digest(path),
                "coverage": [
                    list(row)
                    for row in db.execute("SELECT status,COUNT(*) FROM symbols GROUP BY status")
                ],
            }
    with readonly(locations(output, "2026-10-30")[0]) as db:
        symbols = [
            list(row)
            for row in db.execute("SELECT market,symbol FROM symbols ORDER BY market,symbol")
        ]
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "deadline": deadline.isoformat(),
        "days": DAYS,
        "excluded_days": ["2026-10-23"],
        "prepare_workers": workers,
        "sources": sources,
        "symbols": symbols,
        "policies": policies(),
        "fixed_oct30_ids": fixed_winners(output),
        "code_sha256": {
            name: digest(ROOT / "tools" / name)
            for name in (
                "run_week_experiment.py",
                "week_hypotheses.py",
                "search_portfolios.py",
                "search_up_candles.py",
                "replay_market_day.py",
            )
        },
        "initial_equity_per_market_hypothesis": 1000,
        "ai_calls": 0,
        "interpretation": "Fixed October 30 models are retrospective transfer tests. "
        "Causal policies never receive those winner IDs. Daily indicators reset; cumulative "
        "learning retains policy scores, not cross-day raw indicator histories. All five dates "
        "remain research data, not independent prospective evidence.",
        "execution": "Observed current close; zero costs; constant FX; no intrabar fills; "
        "short liabilities and debt retained; no real orders.",
        "boundary": "Separate KR/US clocks, stable symbol IDs, epoch five-minute ticks, "
        "equal-time cross-day boundary processed once after overlapping price validation.",
        "events": "Excluded until original publication-time availability is verified.",
    }


def verify_code(manifest: dict[str, Any]) -> None:
    for name, expected in manifest["code_sha256"].items():
        if digest(ROOT / "tools" / name) != expected:
            raise ValueError("Frozen experiment source changed: " + name)


def terminate(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def run(output: Path, deadline: datetime, workers: int) -> None:
    if workers < 1 or deadline.utcoffset() is None or deadline.timestamp() <= time.time():
        raise ValueError("Choose positive concurrency and a future timezone-aware deadline")
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    (output / "jobs").mkdir(exist_ok=True)
    with (output / "experiment.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest_path = output / "manifest.json"
        if manifest_path.exists():
            raise ValueError("Use a new output directory for a new hypothesis experiment")
        manifest = make_manifest(output, deadline, workers)
        save(manifest_path, manifest)
        status_path = output / "status.json"
        active: list[tuple[str, subprocess.Popen[Any], Any, Any]] = []
        pending = list(DAYS)
        completed = []
        try:
            while pending or active:
                if time.time() >= deadline.timestamp():
                    raise TimeoutError("Seven-hour experiment deadline reached")
                while pending and len(active) < workers:
                    day = pending.pop(0)
                    stdout = (output / "jobs" / (day + "-stdout.log")).open("a")
                    stderr = (output / "jobs" / (day + "-stderr.log")).open("a")
                    process = subprocess.Popen(
                        [
                            str(Path(sys.executable).resolve()),
                            "-u",
                            "-m",
                            "tools.run_week_experiment",
                            "--prepare-day",
                            day,
                            "--output",
                            str(output),
                        ],
                        cwd=ROOT,
                        stdout=stdout,
                        stderr=stderr,
                        start_new_session=True,
                    )
                    active.append((day, process, stdout, stderr))
                for job in list(active):
                    day, process, stdout, stderr = job
                    code = process.poll()
                    if code is None:
                        continue
                    stdout.close()
                    stderr.close()
                    active.remove(job)
                    if code:
                        raise RuntimeError(f"Daily preparation failed: {day}, exit {code}")
                    completed.append(day)
                save(
                    status_path,
                    {
                        "state": "preparing",
                        "completed_days": completed,
                        "running_days": [job[0] for job in active],
                        "pending_days": pending,
                        "deadline": deadline.isoformat(),
                        "pid": os.getpid(),
                        "ai_calls": 0,
                        "hypotheses": len(manifest["policies"]),
                        "updated_at": datetime.now(UTC).isoformat(),
                    },
                )
                time.sleep(2)
            save(
                status_path,
                {
                    "state": "evaluating",
                    "deadline": deadline.isoformat(),
                    "pid": os.getpid(),
                    "ai_calls": 0,
                    "updated_at": datetime.now(UTC).isoformat(),
                },
            )
            groups = []
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [
                    pool.submit(evaluate, output, market, retrospective, manifest)
                    for market in ("KR", "US")
                    for retrospective in (False, True)
                ]
                for future in as_completed(futures):
                    groups.append(future.result())
                    save(
                        status_path,
                        {
                            "state": "evaluating",
                            "completed_groups": groups,
                            "deadline": deadline.isoformat(),
                            "pid": os.getpid(),
                            "ai_calls": 0,
                            "updated_at": datetime.now(UTC).isoformat(),
                        },
                    )
            for day, source in manifest["sources"].items():
                if digest(Path(source["path"])) != source["sha256"]:
                    raise ValueError("Source dataset changed during the experiment: " + day)
                sealed = json.loads((output / "jobs" / (day + ".json")).read_text())
                _, feature, portfolio = locations(output, day)
                if digest(portfolio / "portfolios.sqlite3") != sealed["portfolio_sha256"]:
                    raise ValueError("Portfolio cache changed during evaluation: " + day)
                if digest(feature / "search.sqlite3") != sealed["feature_sha256"]:
                    raise ValueError("Feature cache changed during evaluation: " + day)
            verify_code(manifest)
            rows = []
            for path in sorted((output / "results").glob("*/summary.json")):
                for hypothesis in json.loads(path.read_text())["hypotheses"]:
                    summary = hypothesis["summary"]
                    rows.append(
                        {
                            "hypothesis": hypothesis["id"],
                            "market": hypothesis["market"],
                            "leverage": hypothesis["leverage"],
                            "classification": hypothesis["classification"],
                            "accounting": encode(hypothesis["accounting"]),
                            **hypothesis["diagnostics"],
                            **summary,
                        }
                    )
            if rows:
                with (output / "comparison.csv").open("w", newline="") as stream:
                    writer = csv.DictWriter(
                        stream, fieldnames=list(dict.fromkeys(key for row in rows for key in row))
                    )
                    writer.writeheader()
                    writer.writerows(rows)
            save(
                status_path,
                {
                    "state": "complete",
                    "groups": groups,
                    "result_rows": len(rows),
                    "deadline": deadline.isoformat(),
                    "pid": os.getpid(),
                    "ai_calls": 0,
                    "updated_at": datetime.now(UTC).isoformat(),
                },
            )
        except BaseException as error:
            save(
                status_path,
                {
                    "state": "deadline_reached"
                    if time.time() >= deadline.timestamp()
                    else "failed",
                    "error_type": type(error).__name__,
                    "message": str(error),
                    "deadline": deadline.isoformat(),
                    "pid": os.getpid(),
                    "ai_calls": 0,
                    "updated_at": datetime.now(UTC).isoformat(),
                },
            )
            raise
        finally:
            for _, process, stdout, stderr in active:
                terminate(process)
                stdout.close()
                stderr.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--prepare-day", choices=DAYS)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--deadline")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if args.worker:
        worker_loop()
    elif args.output is None:
        parser.error("--output is required")
    elif args.status:
        print((args.output / "status.json").read_text())
    elif args.prepare_day:
        prepare_day(args.output.resolve(), args.prepare_day)
    elif args.deadline:
        run(args.output.resolve(), datetime.fromisoformat(args.deadline), args.workers)
    else:
        parser.error("--deadline is required")


if __name__ == "__main__":
    main()
