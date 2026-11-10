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
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from papertrade.clock import utc_iso
from papertrade.jsonio import canonical_json as encode
from papertrade.jsonio import save_canonical_json as save
from tools.research_metadata import read_metadata
from tools.source_identity import file_digest as digest

DAYS = tuple(f"2026-10-{day}" for day in range(26, 31))
ROOT = Path(__file__).resolve().parents[1]
FAMILIES = ("momentum", "reversal", "breakout", "mean_reversion", "transition", "volume_trend")


def metadata(db: sqlite3.Connection, key: str) -> Any:
    missing = object()
    value = read_metadata(db, key, missing)
    if value is missing:
        raise ValueError("Missing experiment metadata: " + key)
    return value


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


def long_policies() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for cold in (True, False):
        prefix = "cold_" if cold else "carry_"
        common = {"reset_daily": cold, "reset_learning": cold}
        result.extend(
            {"id": prefix + "static_" + family, "mode": "static", "families": [family], **common}
            for family in FAMILIES
        )
        result.append({"id": prefix + "daily_all", "mode": "daily", **common})
        result.extend(
            {"id": prefix + "daily_" + family, "mode": "daily", "families": [family], **common}
            for family in FAMILIES
        )
        result.extend(
            {
                "id": f"{prefix}rolling_{window * 5}min_refresh_{refresh * 5}min",
                "mode": "rolling",
                "window": window,
                "refresh_ticks": refresh,
                **common,
            }
            for window in (3, 6, 12, 24)
            for refresh in (1, 3, 6)
        )
    for weighting in ("equal", "linear", "exponential"):
        for online in (False, True):
            for allocation in ("winner", "family_ensemble"):
                result.append(
                    {
                        "id": f"weighted_{weighting}_{'online' if online else 'daily'}_{allocation}",
                        "mode": "weighted_" + weighting,
                        "window": int(online),
                        "refresh_day": not online,
                        "allocation": allocation,
                    }
                )
    result.append({"id": "previous_day_only", "mode": "previous_day", "refresh_day": True})
    return result


def locations(output: Path, day: str) -> tuple[Path, Path, Path]:
    dataset = ROOT / "state" / "datasets" / day / "market.sqlite3"
    if day == "2026-10-30":
        feature = ROOT / "state/searches/2026-10-30-grid-v1"
        portfolio = ROOT / "state/portfolios/2026-10-30-diverse-v1"
    else:
        feature, portfolio = output / "features" / day, output / "portfolios" / day
    return dataset, feature, portfolio


def prepare_day(output: Path, day: str, prepared_output: Path | None = None) -> None:
    from tools.search_portfolios import run as portfolio_run
    from tools.search_up_candles import run as feature_run

    if day not in DAYS:
        raise ValueError("Only the five frozen experiment dates are allowed")
    dataset, feature, portfolio = locations(prepared_output or output, day)
    if prepared_output is None and day != "2026-10-30":
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
    portfolio_hash = digest(portfolio / "portfolios.sqlite3")
    if prepared_output is not None:
        original = json.loads((prepared_output / "jobs" / (day + ".json")).read_text())
        if (portfolio_hash, feature_hash) != (
            original["portfolio_sha256"],
            original["feature_sha256"],
        ):
            raise ValueError("Prepared cache no longer matches its original run: " + day)
    save(
        output / "jobs" / (day + ".json"),
        {
            "state": "complete",
            "day": day,
            "portfolio_sha256": portfolio_hash,
            "feature_sha256": feature_hash,
            "updated_at": utc_iso(),
            "ai_calls": 0,
        },
    )


def load_worker_modules() -> Any:
    package = types.ModuleType("tools")
    vars(package)["__path__"] = []
    sys.modules["tools"] = package
    for name in ("source_identity", "research_metadata", "search_portfolios", "week_hypotheses"):
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
                long_only=request.get("long_only", False),
                initial_equity=request.get("initial_equity", 1000.0),
                whole_shares=request.get("whole_shares", False),
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
        profile = sandbox_profile(
            script, forbidden[0], output,
            source_dependencies=("tools/research_metadata.py", "tools/search_portfolios.py",
                                 "tools/week_hypotheses.py"),
        )
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
        self.timer = None
        if math.isfinite(deadline):
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
        if self.timer is not None:
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


def current_long_features(
    source: sqlite3.Connection,
    frame: dict[str, Any],
    ids: dict[tuple[str, str], int],
    market: str,
) -> tuple[list[list[Any]], str]:
    row = source.execute(
        "SELECT output_sha256,basis_gzip FROM frames WHERE at=?", (frame["at"],)
    ).fetchone()
    if row is None:
        raise ValueError("Missing original feature frame")
    raw = gzip.decompress(row["basis_gzip"])
    if hashlib.sha256(raw).hexdigest() != row["output_sha256"]:
        raise ValueError("Original feature frame digest mismatch")
    result = []
    for feature in json.loads(raw)["features"]:
        if feature["quote_at"] > frame["at"] or any(
            stamp is not None and stamp > frame["at"] for stamp in feature["reference_times"]
        ):
            raise ValueError("Future reference in long-only features")
        if feature["market"] != market or feature["quote_at"] != frame["at"]:
            continue
        symbol = ids[(market, feature["symbol"])]
        if symbol not in frame["quotes"]:
            continue
        if feature["price"] != frame["quotes"][symbol]:
            raise ValueError("Long-only feature price does not match its current quote")
        result.append([symbol, feature["scores"], feature["volumes"], feature["noises"]])
    return result, row["output_sha256"]


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


def trade_statistics(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade["realized_pnl"] is not None]
    wins = [trade["realized_pnl"] for trade in closed if trade["realized_pnl"] > 0]
    losses = [trade["realized_pnl"] for trade in closed if trade["realized_pnl"] < 0]
    streak = longest = 0
    for trade in closed:
        streak = streak + 1 if trade["realized_pnl"] < 0 else 0
        longest = max(longest, streak)
    return {
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "flat_trades": len(closed) - len(wins) - len(losses),
        "closed_trades": len(closed),
        "win_rate_pct": len(wins) / len(closed) * 100 if closed else None,
        "win_rate_excluding_flat_pct": len(wins) / (len(wins) + len(losses)) * 100
        if wins or losses
        else None,
        "gross_realized_profit": sum(wins),
        "gross_realized_loss": -sum(losses),
        "profit_factor": sum(wins) / -sum(losses) if losses else None,
        "max_consecutive_losses": longest,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=list(dict.fromkeys(key for row in rows for key in row))
        )
        writer.writeheader()
        writer.writerows(rows)


def export_long_results(output: Path, manifest: dict[str, Any]) -> None:
    daily_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for path in sorted((output / "results").glob("*/report.json.gz")):
        with gzip.open(path, "rt") as stream:
            report = json.load(stream)
        for hypothesis in report["hypotheses"]:
            base = {
                "hypothesis": hypothesis["id"],
                "market": hypothesis["market"],
                "leverage": hypothesis["leverage"],
                "accounting": hypothesis["accounting"],
            }
            if manifest.get("whole_shares"):
                base.update(
                    starting_capital_usd=hypothesis["starting_capital_usd"],
                    price_currency="USD",
                    native_currency="KRW" if hypothesis["market"] == "KR" else "USD",
                )
            independent = hypothesis["accounting"] == "independent_daily_accounts"
            positions: dict[tuple[Any, ...], dict[str, Any]] = {}
            for trade in hypothesis["trades"]:
                key: tuple[Any, ...] = (trade["configuration"], trade["symbol_id"])
                if independent:
                    key += (trade["day"],)
                if trade["realized_pnl"] is None:
                    if trade["quantity_normalized"] <= 0 or key in positions:
                        raise ValueError("Invalid long-only entry trace")
                    positions[key] = trade
                    continue
                entry = positions.pop(key)
                if trade["quantity_normalized"] >= 0 or not math.isclose(
                    -trade["quantity_normalized"], entry["quantity_normalized"], rel_tol=1e-9
                ):
                    raise ValueError("Long-only exit does not close its original position")
                if trade["realized_pnl"] < 0:
                    failures.append(
                        {
                            **base,
                            "day": trade["day"],
                            "symbol": manifest["symbols"][trade["symbol_id"]][1],
                            "configuration": trade["configuration"],
                            "entry_at": entry["fill_at"],
                            "exit_at": trade["fill_at"],
                            "entry_price": entry["price"],
                            "exit_price": trade["price"],
                            "position_return_pct": (trade["price"] / entry["price"] - 1) * 100,
                            "realized_pnl": trade["realized_pnl"],
                            "reason": trade["reason"],
                            **(
                                {
                                    "entry_native_price": entry["native_price"],
                                    "exit_native_price": trade["native_price"],
                                    "quantity": entry["quantity_normalized"],
                                }
                                if manifest.get("whole_shares")
                                else {}
                            ),
                        }
                    )
            for row in hypothesis["daily"]:
                trades = [t for t in hypothesis["trades"] if t["day"] == row["day"]]
                daily_rows.append(
                    {
                        **base,
                        **{
                            key: row[key]
                            for key in ("day", "start_equity", "end_equity", "pnl", "return_pct")
                        },
                        **trade_statistics(trades),
                        "open_positions": row["summary"]["open_positions"],
                        "stale_open_positions": row["summary"]["stale_open_positions"],
                        "cumulative_drawdown_pct": row["summary"]["max_drawdown_pct"],
                        **(
                            {
                                "cash_usd": row["summary"]["cash"],
                                "cumulative_rejected_unaffordable": row["summary"][
                                    "rejected_unaffordable"
                                ],
                                "cumulative_zero_quantity": row["summary"]["zero_quantity"],
                            }
                            if manifest.get("whole_shares")
                            else {}
                        ),
                    }
                )
    write_csv(output / "daily-comparison.csv", daily_rows)
    write_csv(output / "losing-trades.csv", failures)


def audit_whole_account(hypothesis: dict[str, Any], capital: float) -> None:
    independent = hypothesis["accounting"] == "independent_daily_accounts"
    cash, day = capital, None
    holdings: dict[int, float] = {}
    for trade in hypothesis["trades"]:
        if independent and day != trade["day"]:
            cash, holdings = capital, {}
        day = trade["day"]
        quantity = float(trade["quantity_normalized"])
        if not math.isfinite(quantity) or quantity == 0 or not quantity.is_integer():
            raise ValueError("Whole-share trace contains a fractional or invalid quantity")
        symbol = trade["symbol_id"]
        if trade["realized_pnl"] is None:
            if quantity < 0 or symbol in holdings:
                raise ValueError("Whole-share trace contains an invalid entry")
            holdings[symbol] = quantity
        elif quantity >= 0 or holdings.pop(symbol) != -quantity:
            raise ValueError("Whole-share trace sells shares that are not held")
        cash -= quantity * trade["price"]
        if cash < -1e-8 or not math.isclose(cash, trade["cash"], rel_tol=1e-9, abs_tol=1e-8):
            raise ValueError("Whole-share trace borrows money or changes cash without a fill")
    expected_funding = capital * (len(hypothesis["daily"]) if independent else 1)
    if not math.isclose(hypothesis["summary"]["initial_equity"], expected_funding):
        raise ValueError("Whole-share account reports incorrect initial funding")


def evaluate(
    output: Path,
    market: str,
    retrospective: bool,
    manifest: dict[str, Any],
    initial_equity: float | None = None,
) -> str:
    from tools.search_portfolios import configurations, signals

    deadline = (
        datetime.fromisoformat(manifest["deadline"]).timestamp()
        if manifest["deadline"]
        else math.inf
    )
    verify_code(manifest)
    group = market + ("-retrospective" if retrospective else "-causal")
    whole_shares = manifest.get("whole_shares", False)
    capital = 1000.0 if initial_equity is None else initial_equity
    if whole_shares:
        if capital not in manifest["initial_capitals_usd"]:
            raise ValueError("Starting capital is absent from the frozen experiment")
        group += f"-usd{capital:.17g}"
    result = output / "results" / group
    result.mkdir(parents=True, exist_ok=True)
    status_path = result / "status.json"
    all_specs = signals()
    configs = configurations(len(all_specs))
    long_only = manifest.get("long_only", False)
    fixed_ids = manifest["fixed_oct30_ids"] if retrospective else {}
    selected_signals = None
    if retrospective:
        identifiers = set(fixed_ids[market])
        configs = [config for config in configs if config["id"] in identifiers]
        selected_signals = {config["signal"] for config in configs}
    if long_only:
        if retrospective:
            raise ValueError("Long-only evaluation does not accept retrospective winners")
        configs = [config for config in configs if all_specs[config["signal"]]["side"] == "long"]
        selected_signals = {config["signal"] for config in configs}
    if whole_shares:
        if not long_only:
            raise ValueError("Whole-share experiments require long-only execution")
        configs = [config for config in configs if config["leverage"] == 1]
        selected_signals = {config["signal"] for config in configs}
    selected_policies = [
        p for p in manifest["policies"] if (p["mode"] == "fixed30") == retrospective
    ]
    ids = {tuple(value): index for index, value in enumerate(manifest["symbols"])}
    prepared = Path(manifest["prepared_output"]) if manifest.get("prepared_output") else output
    data_paths = [locations(prepared, day)[0] for day in DAYS]
    cache_paths = [locations(prepared, day)[2] / "portfolios.sqlite3" for day in DAYS]
    feature_paths = [locations(prepared, day)[1] / "search.sqlite3" for day in DAYS]
    forbidden_features = feature_paths if long_only else []
    canary = result / "private-canary.txt"
    canary.write_text("Private scoring data is not a worker input")
    db = sqlite3.connect(result / "evaluation.sqlite3")
    db.execute(
        "CREATE TABLE IF NOT EXISTS sealed_inputs ("
        "sequence INTEGER PRIMARY KEY,day TEXT,at TEXT,sha256 TEXT,source_sha256 TEXT)"
    )
    db.execute("CREATE TABLE IF NOT EXISTS day_results (day TEXT PRIMARY KEY,payload_gzip BLOB)")
    if long_only:
        db.execute("ALTER TABLE sealed_inputs ADD COLUMN source_feature_sha256 TEXT")
    db.commit()
    worker = IsolatedWorker(
        output,
        result,
        [*data_paths, *cache_paths, *forbidden_features, result / "evaluation.sqlite3", canary],
        deadline,
    )
    last_at = ""
    previous_quotes: dict[int, float] = {}
    sequence = 0
    feature_source = None
    try:
        checks = worker.exchange(
            {
                "kind": "probe",
                "paths": [
                    str(p)
                    for p in [
                        *data_paths,
                        *cache_paths,
                        *forbidden_features,
                        result / "evaluation.sqlite3",
                        canary,
                    ]
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
                "long_only": long_only,
                "initial_equity": capital,
                "whole_shares": whole_shares,
            }
        )
        for day, path, feature_path in zip(DAYS, cache_paths, feature_paths, strict=True):
            sealed = json.loads((output / "jobs" / (day + ".json")).read_text())
            if digest(path) != sealed["portfolio_sha256"]:
                raise ValueError("Frozen portfolio cache changed: " + day)
            if long_only:
                if digest(feature_path) != sealed["feature_sha256"]:
                    raise ValueError("Frozen feature cache changed: " + day)
                feature_source = readonly(feature_path)
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
                    feature_hash = None
                    if feature_source is not None:
                        frame["long_features"], feature_hash = current_long_features(
                            feature_source, frame, ids, market
                        )
                    if whole_shares and market == "KR":
                        rate = manifest["krw_per_usd"]
                        frame["quotes"] = {
                            key: price / rate for key, price in frame["quotes"].items()
                        }
                        frame["marks"] = {
                            key: [mark[0] / rate, mark[1]] for key, mark in frame["marks"].items()
                        }
                    if frame["at"] <= last_at:
                        if frame["at"] != last_at or any(
                            previous_quotes[key] != price
                            for key, price in frame["quotes"].items()
                            if key in previous_quotes
                        ):
                            raise ValueError("Cross-day quotes disagree or move backwards")
                        if not long_only:
                            continue
                    sequence += 1
                    with db:
                        db.execute(
                            "INSERT INTO sealed_inputs VALUES (?,?,?,?,?,?)"
                            if long_only
                            else "INSERT INTO sealed_inputs VALUES (?,?,?,?,?)",
                            (
                                sequence,
                                day,
                                frame["at"],
                                hashlib.sha256(encode(frame).encode()).hexdigest(),
                                row["sha256"],
                            )
                            + ((feature_hash,) if long_only else ()),
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
                                "updated_at": utc_iso(),
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
                        "updated_at": utc_iso(),
                        "ai_calls": 0,
                    },
                )
            if feature_source is not None:
                feature_source.close()
                feature_source = None
                if digest(feature_path) != sealed["feature_sha256"]:
                    raise ValueError("Feature cache changed during its replay: " + day)
        report = worker.exchange({"kind": "report"})
        for hypothesis in report["hypotheses"]:
            if whole_shares:
                hypothesis["starting_capital_usd"] = capital
                rate = manifest["krw_per_usd"] if market == "KR" else 1.0
                for trade in hypothesis["trades"]:
                    trade["price_currency"] = "USD"
                    trade["native_currency"] = "KRW" if market == "KR" else "USD"
                    trade["native_price"] = trade["price"] * rate
                    trade["native_entry_price"] = trade["entry_price"] * rate
                    trade["krw_per_usd"] = manifest["krw_per_usd"] if market == "KR" else None
                audit_whole_account(hypothesis, capital)
            hypothesis["diagnostics"] = diagnostics(hypothesis)
            if long_only:
                hypothesis["diagnostics"].update(trade_statistics(hypothesis["trades"]))
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
                "updated_at": utc_iso(),
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
                "updated_at": utc_iso(),
                "ai_calls": 0,
            },
        )
        raise
    finally:
        if feature_source is not None:
            feature_source.close()
        worker.close()
        db.close()


def make_manifest(
    output: Path,
    deadline: datetime | None,
    workers: int,
    *,
    long_only: bool = False,
    prepared_output: Path | None = None,
    whole_shares: bool = False,
    capitals: tuple[float, ...] = (50.0, 100.0, 200.0, 500.0),
    krw_per_usd: float | None = None,
) -> dict[str, Any]:
    prepared = prepared_output or output
    if prepared_output is not None:
        if prepared_output.resolve() == output.resolve():
            raise ValueError("Prepared caches and new results need separate directories")
        if json.loads((prepared_output / "status.json").read_text())["state"] != "complete":
            raise ValueError("Prepared experiment has not completed")
    sources = {}
    for day in DAYS:
        path = locations(prepared, day)[0]
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
    with readonly(locations(prepared, "2026-10-30")[0]) as db:
        symbols = [
            list(row)
            for row in db.execute("SELECT market,symbol FROM symbols ORDER BY market,symbol")
        ]
    return {
        "created_at": utc_iso(),
        "deadline": deadline.isoformat() if deadline else None,
        "days": DAYS,
        "excluded_days": ["2026-10-23"],
        "prepare_workers": workers,
        "sources": sources,
        "symbols": symbols,
        "policies": long_policies() if long_only else policies(),
        "long_only": long_only,
        "whole_shares": whole_shares,
        "initial_capitals_usd": list(capitals) if whole_shares else [1000.0],
        "krw_per_usd": krw_per_usd if whole_shares else None,
        "fx_assumption": "Fixed KRW/USD conversion for all five days, not a historical daily "
        "FX series or a point-in-time currency forecast. Native whole-share quantities are "
        "priced in USD for accounting; original feature ratios are unchanged."
        if whole_shares
        else None,
        "prepared_output": str(prepared_output) if prepared_output else None,
        "fixed_oct30_ids": {} if long_only else fixed_winners(output),
        "code_sha256": {
            **{
                name: digest(ROOT / "tools" / name)
                for name in (
                    "run_week_experiment.py",
                    "week_hypotheses.py",
                    "search_portfolios.py",
                    "search_up_candles.py",
                    "replay_market_day.py",
                )
            },
            **{
                name: digest(ROOT / name)
                for name in (
                    "papertrade/clock.py",
                    "papertrade/jsonio.py",
                    "tools/research_metadata.py",
                    "tools/source_identity.py",
                )
            },
        },
        "initial_equity_per_market_hypothesis": None if whole_shares else 1000,
        "ai_calls": 0,
        "interpretation": (
            "Only long-entry candidates are evaluated; no October 30 winner IDs are used. "
            if long_only
            else "Fixed October 30 models are retrospective transfer tests. "
            "Causal policies never receive those winner IDs. "
        )
        + "Daily indicators reset; cumulative learning retains policy scores, not cross-day "
        "raw indicator histories. All five dates remain research data, not independent "
        "prospective evidence.",
        "execution": "Observed current close; zero costs; constant FX; no intrabar fills; "
        + (
            "whole-share cash-only long entries and closing sells; "
            if whole_shares
            else "long entries and closing sells only; "
            if long_only
            else "shorts allowed; "
        )
        + ("no borrowing; " if whole_shares else "debt retained; ")
        + "no real orders.",
        "boundary": "Separate KR/US clocks, stable symbol IDs, epoch five-minute ticks. "
        + (
            "Validated equal-time new-day boundaries initialize cold accounts and selection "
            "cadence; continuous accounts only mark prices without duplicate fills."
            if long_only
            else "Equal-time cross-day boundaries are processed once after "
            "overlapping price validation."
        ),
        "events": "Excluded until original publication-time availability is verified.",
        "long_only_design": {
            "independent_days": "Cold policies reset capital, selected settings and learning "
            "daily. Every daily account receives its cohort's starting capital again.",
            "weighted_days": "Equal, linear 1..n, or exponential 1,2,4,... weights over "
            "daily normalized shadow P&L. Online variants include only observed current-day P&L.",
            "allocation": "Winner selection or softmax family weights applied to new entries. "
            "Existing positions keep original exit rules and are not forcibly rebalanced.",
            "validation": "Final winners are exploratory selections from these same five days.",
        }
        if long_only
        else None,
    }


def verify_code(manifest: dict[str, Any]) -> None:
    for name, expected in manifest["code_sha256"].items():
        source = ROOT / name if "/" in name else ROOT / "tools" / name
        if digest(source) != expected:
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


def run(
    output: Path,
    deadline: datetime | None,
    workers: int,
    *,
    long_only: bool = False,
    prepared_output: Path | None = None,
    whole_shares: bool = False,
    capitals: tuple[float, ...] = (50.0, 100.0, 200.0, 500.0),
    krw_per_usd: float | None = None,
) -> None:
    if whole_shares and (
        not long_only
        or not capitals
        or len(set(capitals)) != len(capitals)
        or any(not math.isfinite(capital) or capital <= 0 for capital in capitals)
        or krw_per_usd is None
        or not math.isfinite(krw_per_usd)
        or krw_per_usd <= 0
    ):
        raise ValueError(
            "Whole-share experiments need positive distinct capitals and a fixed FX rate"
        )
    if (
        workers < 1
        or deadline is None
        and not long_only
        or deadline is not None
        and (deadline.utcoffset() is None or deadline.timestamp() <= time.time())
    ):
        raise ValueError("Choose positive concurrency and a future timezone-aware deadline")
    deadline_at = deadline.timestamp() if deadline else math.inf
    deadline_text = deadline.isoformat() if deadline else None
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    (output / "jobs").mkdir(exist_ok=True)
    with (output / "experiment.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest_path = output / "manifest.json"
        if manifest_path.exists():
            raise ValueError("Use a new output directory for a new hypothesis experiment")
        manifest = make_manifest(
            output,
            deadline,
            workers,
            long_only=long_only,
            prepared_output=prepared_output,
            whole_shares=whole_shares,
            capitals=capitals,
            krw_per_usd=krw_per_usd,
        )
        save(manifest_path, manifest)
        status_path = output / "status.json"
        active: list[tuple[str, subprocess.Popen[Any], Any, Any]] = []
        pending = list(DAYS)
        completed = []
        try:
            while pending or active:
                if time.time() >= deadline_at:
                    raise TimeoutError("Seven-hour experiment deadline reached")
                while pending and len(active) < workers:
                    day = pending.pop(0)
                    stdout = (output / "jobs" / (day + "-stdout.log")).open("a")
                    stderr = (output / "jobs" / (day + "-stderr.log")).open("a")
                    command = [
                        str(Path(sys.executable).resolve()),
                        "-u",
                        "-m",
                        "tools.run_week_experiment",
                        "--prepare-day",
                        day,
                        "--output",
                        str(output),
                    ]
                    if prepared_output is not None:
                        command.extend(["--prepared-output", str(prepared_output)])
                    process = subprocess.Popen(
                        command,
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
                        "deadline": deadline_text,
                        "pid": os.getpid(),
                        "ai_calls": 0,
                        "hypotheses": len(manifest["policies"]),
                        "updated_at": utc_iso(),
                    },
                )
                time.sleep(2)
            save(
                status_path,
                {
                    "state": "evaluating",
                    "deadline": deadline_text,
                    "pid": os.getpid(),
                    "ai_calls": 0,
                    "updated_at": utc_iso(),
                },
            )
            groups = []
            with ThreadPoolExecutor(max_workers=workers if whole_shares else 4) as pool:
                futures = [
                    pool.submit(evaluate, output, market, retrospective, manifest, capital)
                    for capital in (capitals if whole_shares else (None,))
                    for market in ("KR", "US")
                    for retrospective in ((False,) if long_only else (False, True))
                ]
                for future in as_completed(futures):
                    groups.append(future.result())
                    save(
                        status_path,
                        {
                            "state": "evaluating",
                            "completed_groups": groups,
                            "deadline": deadline_text,
                            "pid": os.getpid(),
                            "ai_calls": 0,
                            "updated_at": utc_iso(),
                        },
                    )
            for day, source in manifest["sources"].items():
                if digest(Path(source["path"])) != source["sha256"]:
                    raise ValueError("Source dataset changed during the experiment: " + day)
                sealed = json.loads((output / "jobs" / (day + ".json")).read_text())
                _, feature, portfolio = locations(prepared_output or output, day)
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
                            **(
                                {"starting_capital_usd": hypothesis["starting_capital_usd"]}
                                if whole_shares
                                else {}
                            ),
                            **hypothesis["diagnostics"],
                            **summary,
                        }
                    )
            write_csv(output / "comparison.csv", rows)
            if long_only:
                cohorts = len(capitals) if whole_shares else 4
                if len(rows) != len(manifest["policies"]) * cohorts * 2:
                    raise ValueError("Long-only account count does not match the frozen design")
                export_long_results(output, manifest)
            save(
                status_path,
                {
                    "state": "complete",
                    "groups": groups,
                    "result_rows": len(rows),
                    "deadline": deadline_text,
                    "pid": os.getpid(),
                    "ai_calls": 0,
                    "updated_at": utc_iso(),
                },
            )
        except BaseException as error:
            save(
                status_path,
                {
                    "state": "deadline_reached" if time.time() >= deadline_at else "failed",
                    "error_type": type(error).__name__,
                    "message": str(error),
                    "deadline": deadline_text,
                    "pid": os.getpid(),
                    "ai_calls": 0,
                    "updated_at": utc_iso(),
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
    parser.add_argument("--long-only", action="store_true")
    parser.add_argument("--whole-shares", action="store_true")
    parser.add_argument("--capitals", nargs="+", type=float, default=[50, 100, 200, 500])
    parser.add_argument("--krw-per-usd", type=float)
    parser.add_argument("--prepared-output", type=Path)
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
        prepare_day(
            args.output.resolve(),
            args.prepare_day,
            args.prepared_output.resolve() if args.prepared_output else None,
        )
    elif args.deadline or args.long_only or args.whole_shares:
        run(
            args.output.resolve(),
            datetime.fromisoformat(args.deadline) if args.deadline else None,
            args.workers,
            long_only=args.long_only or args.whole_shares,
            prepared_output=args.prepared_output.resolve() if args.prepared_output else None,
            whole_shares=args.whole_shares,
            capitals=tuple(args.capitals),
            krw_per_usd=args.krw_per_usd,
        )
    else:
        parser.error("--deadline is required")


if __name__ == "__main__":
    main()
