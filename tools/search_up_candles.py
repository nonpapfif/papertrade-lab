"""Search fixed upward-candle rules on a causal five-minute historical replay."""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import math
import os
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timedelta
from functools import partial
from itertools import pairwise, product
from pathlib import Path
from typing import Any

from papertrade.clock import utc_iso
from papertrade.jsonio import recorded_json as encode
from tools.replay_market_day import (
    GRID_BITS,
    GRID_COMMON,
    GRID_FAMILIES,
    GRID_LOOKBACKS,
    GRID_MOVES,
    GRID_NOISE,
    GRID_NOISE_READY,
    GRID_NOISES,
    GRID_SCORE_READY,
    GRID_VOLUME,
    GRID_VOLUME_READY,
    GRID_VOLUMES,
    STEP,
    Worker,
    parse_at,
)
from tools.research_metadata import read_metadata as get_meta
from tools.research_metadata import write_metadata
from tools.source_identity import file_digest as hash_file

set_meta = partial(write_metadata, encode=encode)

GOALS = (0.0, 0.001, 0.003, 0.005, 0.01)
SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS frames (
    id INTEGER PRIMARY KEY, at TEXT UNIQUE, first_case INTEGER NOT NULL,
    cases INTEGER NOT NULL, input_sha256 TEXT NOT NULL, output_sha256 TEXT NOT NULL,
    basis_gzip BLOB NOT NULL, committed_at TEXT NOT NULL, evaluated_at TEXT
);
CREATE TABLE IF NOT EXISTS cases (
    id INTEGER PRIMARY KEY, frame_id INTEGER NOT NULL, ordinal INTEGER NOT NULL,
    market TEXT NOT NULL, symbol TEXT NOT NULL, at TEXT NOT NULL,
    quote_at TEXT NOT NULL, price REAL NOT NULL, phase TEXT NOT NULL, session TEXT NOT NULL,
    outcome TEXT, actual_return REAL, target_quote_at TEXT
);
CREATE INDEX IF NOT EXISTS cases_frame ON cases(frame_id);
CREATE INDEX IF NOT EXISTS cases_phase ON cases(market,phase);
CREATE TABLE IF NOT EXISTS configurations (
    id TEXT PRIMARY KEY, family TEXT NOT NULL, lookback INTEGER NOT NULL, data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS finished (configuration TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS scores (
    configuration TEXT, market TEXT, goal REAL, phase TEXT, cohort TEXT,
    total INTEGER, positives INTEGER, predicted INTEGER, eligible INTEGER,
    tp INTEGER, fp INTEGER, fn INTEGER, tn INTEGER, precision REAL, recall REAL, f1 REAL,
    PRIMARY KEY(configuration,market,goal,phase,cohort)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS score_rank ON scores(market,goal,phase,cohort,f1 DESC);
"""


def progress(db: sqlite3.Connection, folder: Path, stage: str, **fields: Any) -> None:
    record = {"stage": stage, "updated_at": utc_iso(),
              "pid": os.getpid(), "ai_calls": 0, **fields}
    with db:
        set_meta(db, "progress", record)
    temporary = folder / "status.json.tmp"
    temporary.write_text(encode(record) + "\n")
    temporary.replace(folder / "status.json")
    print(encode(record), flush=True)


def configurations() -> list[dict[str, Any]]:
    result = []
    for family, lookback, move, volume, noise in product(
        range(6), range(5), range(5), range(5), range(4),
    ):
        score_index = family * 5 + lookback
        result.append({
            "id": f"f{family}-l{lookback}-m{move}-v{volume}-n{noise}",
            "family": GRID_FAMILIES[family], "lookback": GRID_LOOKBACKS[lookback],
            "move": GRID_MOVES[move], "volume": GRID_VOLUMES[volume],
            "noise": GRID_NOISES[noise],
            "prediction_bits": [score_index * 5 + move, GRID_VOLUME + lookback * 5 + volume,
                                GRID_NOISE + lookback * 4 + noise],
            "ready_bits": [GRID_SCORE_READY + score_index]
            + ([GRID_VOLUME_READY + lookback] if volume else [])
            + ([GRID_NOISE_READY + lookback] if noise else []),
        })
    result.extend({"id": key, "family": key, "lookback": 0,
                   "prediction_bits": [], "ready_bits": []}
                  for key in ("always_up", "never_up"))
    return result


def phase_at(at: datetime, scope: dict[str, Any]) -> str:
    start, end = parse_at(scope["start"]), parse_at(scope["end"])
    steps = int((end - start).total_seconds() / STEP)
    train = start + timedelta(seconds=int(steps * 0.6) * STEP)
    validation = start + timedelta(seconds=int(steps * 0.8) * STEP)
    target = at + timedelta(seconds=STEP)
    if target <= train:
        return "train"
    if at >= train and target <= validation:
        return "validation"
    if at >= validation and target <= end:
        return "test"
    raise ValueError("Forecast crosses a chronological split boundary")


def session_at(at: datetime, scope: dict[str, Any]) -> str:
    middle = at + timedelta(seconds=STEP / 2)
    for index, session in enumerate(scope.get("sessions", [])):
        if parse_at(session["start"]) <= middle < parse_at(session["end"]):
            return str(index)
    return "outside_calendar"


class GridHistory:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
        self.touched: set[tuple[str, str]] = set()
        self.first: dict[tuple[str, str], datetime] = {}

    def add(self, row: sqlite3.Row) -> None:
        key = (row["market"], row["symbol"])
        at = parse_at(row["at"])
        self.rows[key].append({"time": at, **{k: float(row[k]) for k in
                                               ("close", "high", "low", "volume")}})
        self.first.setdefault(key, at)
        self.touched.add(key)

    def features(self, at: datetime, scopes: dict[str, Any]) -> list[dict[str, Any]]:
        result = []
        five = at - timedelta(minutes=5)
        ten = at - timedelta(minutes=10)
        oldest = at - timedelta(minutes=65)
        cutoffs = [(at - timedelta(minutes=length), five - timedelta(minutes=length))
                   for length in GRID_LOOKBACKS]
        for key in sorted(self.touched):
            market, symbol = key
            if at + timedelta(seconds=STEP) > parse_at(scopes[market]["end"]):
                continue
            history = self.rows[key]
            while len(history) > 1 and history[1]["time"] <= oldest:
                history.popleft()
            rows = list(history)
            last = rows[-1]
            if last["time"] > at:
                raise ValueError("Future data entered the feature history")
            recent = [r for r in rows if five < r["time"] <= at]
            if not recent:
                continue
            price = last["close"]
            reference5 = next((r for r in reversed(rows) if r["time"] <= five), None)
            previous10 = next((r for r in reversed(rows) if r["time"] <= ten), None)
            r5 = price / reference5["close"] - 1 if reference5 else None
            recent_volume = sum(r["volume"] for r in recent)
            scores: list[float | None] = [None] * 30
            volumes: list[float | None] = []
            noises: list[float | None] = []
            reference_times: list[str | None] = []
            for index, (lookback, (cutoff, prior_start)) in enumerate(
                zip(GRID_LOOKBACKS, cutoffs, strict=True)
            ):
                reference = next((r for r in reversed(rows) if r["time"] <= cutoff), None)
                reference_times.append(reference["time"].isoformat() if reference else None)
                current = [r for r in rows if cutoff < r["time"] <= at]
                prior = [r for r in rows if prior_start < r["time"] <= five]
                complete_prior = self.first[key] <= prior_start + timedelta(minutes=1)
                baseline = sum(r["volume"] for r in prior) * 5 / lookback
                volume = recent_volume / baseline if baseline > 0 and complete_prior else None
                volumes.append(volume)
                moves = [abs(b["close"] / a["close"] - 1) for a, b in pairwise(prior)
                         if (b["time"] - a["time"]).total_seconds() == 60]
                noise = sum(moves) / len(moves) if moves and complete_prior else None
                noises.append(abs(r5) / (max(noise, 1e-12) * math.sqrt(5))
                              if r5 is not None and noise is not None else None)
                if reference is None:
                    continue
                change = price / reference["close"] - 1
                scaled = change * 5 / lookback
                mean = sum(r["close"] for r in current) / len(current) if current else price
                transition = None
                if r5 is not None:
                    if lookback > 5:
                        transition = r5 - (change - r5) / (lookback / 5 - 1)
                    elif previous10 and reference5:
                        transition = r5 - (reference5["close"] / previous10["close"] - 1)
                family_scores = (
                    scaled, -scaled,
                    price / max(r["high"] for r in prior) - 1
                    if prior and complete_prior else None,
                    (mean - price) / price * 5 / lookback,
                    transition, scaled * max(volume - 1, 0) if volume is not None else None,
                )
                for family, value in enumerate(family_scores):
                    scores[family * 5 + index] = value
            result.append({"market": market, "symbol": symbol, "price": price,
                           "quote_at": last["time"].isoformat(),
                           "price_age_seconds": (at - last["time"]).total_seconds(),
                           "reference_times": reference_times, "observations": len(recent),
                           "scores": scores, "volumes": volumes, "noises": noises})
        self.touched.clear()
        return result


def extract(db: sqlite3.Connection, dataset: Path, folder: Path) -> None:
    if get_meta(db, "feature_complete", False):
        return
    source = sqlite3.connect(dataset.as_uri() + "?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    worker = Worker(dataset, folder)
    try:
        scopes = get_meta(db, "manifest")["scopes"]
        canary = folder / "private-canary.txt"
        canary.write_text("Future labels are private to the scorer")
        check = worker.exchange({"kind": "probe", "paths": [str(dataset), str(canary),
                                                              str(folder / "search.sqlite3")]})
        if not all(check.values()):
            raise RuntimeError("Predictor isolation checks failed")
        with db:
            set_meta(db, "isolation", check)
        iterator = iter(source.execute(
            "SELECT c.* FROM candles c JOIN symbols s ON c.market=s.market AND c.symbol=s.symbol "
            "WHERE s.status='done' ORDER BY c.at,c.market,c.symbol"
        ))
        raw = next(iterator, None)
        history = GridHistory()
        at = min(parse_at(s["start"]) for s in scopes.values()) + timedelta(seconds=STEP)
        end = max(parse_at(s["end"]) for s in scopes.values())
        step = 0
        case_id = 0
        pending: list[tuple[int, dict[str, Any]]] = []
        while at <= end:
            step += 1
            while raw is not None and parse_at(raw["at"]) <= at:
                history.add(raw)
                raw = next(iterator, None)
            with db:
                for identifier, feature in pending:
                    last = history.rows[(feature["market"], feature["symbol"])][-1]
                    observed = last["time"] == at and feature["price_age_seconds"] == 0
                    actual = last["close"] / feature["price"] - 1 if observed else None
                    db.execute("UPDATE cases SET outcome=?,actual_return=?,target_quote_at=? "
                               "WHERE id=?", ("observed" if observed else "missing_endpoint",
                                              actual, last["time"].isoformat(), identifier))
                if pending:
                    db.execute("UPDATE frames SET evaluated_at=? WHERE id=?",
                               (utc_iso(), step - 1))
            features = history.features(at, scopes)
            request = {"kind": "grid", "at": at.isoformat(), "rows": features}
            input_hash = hashlib.sha256(encode(request).encode()).hexdigest()
            cached = db.execute("SELECT * FROM frames WHERE id=?", (step,)).fetchone()
            if cached:
                if cached["input_sha256"] != input_hash or cached["first_case"] != case_id:
                    raise ValueError("Cached causal inputs changed during resume")
                payload = gzip.decompress(cached["basis_gzip"])
                if hashlib.sha256(payload).hexdigest() != cached["output_sha256"]:
                    raise ValueError("Cached frame hash mismatch")
                decisions = json.loads(payload)["decisions"]
            else:
                decisions = worker.exchange(request)
            if [(d["market"], d["symbol"]) for d in decisions] != [
                (f["market"], f["symbol"]) for f in features
            ]:
                raise ValueError("Predictor changed the universe")
            pending = []
            with db:
                if not cached:
                    payload = encode({"features": features, "decisions": decisions}).encode()
                    db.execute("INSERT INTO frames VALUES (?,?,?,?,?,?,?,?,NULL)", (
                        step, at.isoformat(), case_id, len(features), input_hash,
                        hashlib.sha256(payload).hexdigest(), gzip.compress(payload, mtime=0),
                        utc_iso(),
                    ))
                for ordinal, feature in enumerate(features):
                    pending.append((case_id, feature))
                    if not cached:
                        scope = scopes[feature["market"]]
                        db.execute("INSERT INTO cases VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)", (
                            case_id, step, ordinal, feature["market"], feature["symbol"],
                            at.isoformat(), feature["quote_at"], feature["price"],
                            phase_at(at, scope), session_at(at, scope),
                        ))
                    case_id += 1
            if step % 12 == 0:
                progress(db, folder, "extracting", step=step, total_steps=int(
                    (end - min(parse_at(s["start"]) for s in scopes.values())).total_seconds()
                    / STEP), cases=case_id, simulated_at=at.isoformat())
            at += timedelta(seconds=STEP)
        if pending or db.execute("SELECT COUNT(*) FROM cases WHERE outcome IS NULL").fetchone()[0]:
            raise ValueError("Unresolved forecast endpoints remain")
        if hash_file(dataset) != get_meta(db, "manifest")["dataset_sha256"]:
            raise ValueError("Source dataset changed during extraction")
        with db:
            set_meta(db, "feature_complete", True)
        progress(db, folder, "features_complete", cases=case_id, frames=step)
    finally:
        try:
            worker.close()
        finally:
            source.close()


class Bitmaps:
    def __init__(self) -> None:
        self.columns = [bytearray() for _ in range(GRID_BITS)]
        self.groups: dict[str, bytearray] = {
            key: bytearray() for key in ("KR", "US", "train", "validation", "test", "observed")
        }
        self.targets = [bytearray() for _ in GOALS]
        self.size = 0
        self.bits: list[int] = []
        self.group_bits: dict[str, int] = {}
        self.target_bits: list[int] = []

    def add(self, mask: int, case: sqlite3.Row) -> None:
        if case["id"] != self.size or mask < 0 or mask.bit_length() > GRID_BITS:
            raise ValueError("Invalid case sequence or predicate mask")
        if self.size % 8 == 0:
            for array in [*self.columns, *self.groups.values(), *self.targets]:
                array.append(0)
        bit = 1 << (self.size % 8)
        while mask:
            low = mask & -mask
            self.columns[low.bit_length() - 1][-1] |= bit
            mask ^= low
        for key in (case["market"], case["phase"]):
            self.groups[key][-1] |= bit
        if case["outcome"] == "observed":
            self.groups["observed"][-1] |= bit
            for index, goal in enumerate(GOALS):
                actual = case["actual_return"]
                if actual > 0 if goal == 0 else actual >= goal - 1e-12:
                    self.targets[index][-1] |= bit
        self.size += 1

    def finish(self) -> None:
        self.bits = [int.from_bytes(array, "little") for array in self.columns]
        self.group_bits = {key: int.from_bytes(array, "little")
                           for key, array in self.groups.items()}
        self.target_bits = [int.from_bytes(array, "little") for array in self.targets]
        self.columns.clear()
        self.groups.clear()
        self.targets.clear()

    def prediction(self, config: dict[str, Any]) -> tuple[int, int]:
        all_bits = (1 << self.size) - 1
        if config["id"] == "always_up":
            return all_bits, all_bits
        if config["id"] == "never_up":
            return 0, all_bits
        prediction = all_bits
        ready = all_bits
        for index in config["prediction_bits"]:
            prediction &= self.bits[index]
        for index in config["ready_bits"]:
            ready &= self.bits[index]
        return prediction, ready


def load_bitmaps(db: sqlite3.Connection, folder: Path) -> Bitmaps:
    maps = Bitmaps()
    for frame in db.execute("SELECT * FROM frames ORDER BY id"):
        raw = gzip.decompress(frame["basis_gzip"])
        if hashlib.sha256(raw).hexdigest() != frame["output_sha256"]:
            raise ValueError("Sealed prediction frame hash mismatch")
        decisions = json.loads(raw)["decisions"]
        cases = db.execute("SELECT * FROM cases WHERE frame_id=? ORDER BY id",
                           (frame["id"],)).fetchall()
        if len(cases) != len(decisions):
            raise ValueError("Frame and outcome counts disagree")
        for case, decision in zip(cases, decisions, strict=True):
            maps.add(int(decision["mask"], 16), case)
        if frame["id"] % 24 == 0:
            progress(db, folder, "building_bitmaps", step=frame["id"], cases=maps.size)
    maps.finish()
    return maps


def metrics(tp: int, predicted: int, positives: int, total: int) -> tuple[Any, ...]:
    fp, fn = predicted - tp, positives - tp
    tn = total - tp - fp - fn
    precision = tp / predicted if predicted else None
    recall = tp / positives if positives else None
    f1 = 2 * tp / (predicted + positives) if predicted + positives else None
    return tp, fp, fn, tn, precision, recall, f1


def score_grid(db: sqlite3.Connection, folder: Path, maps: Bitmaps,
               configs: list[dict[str, Any]]) -> None:
    all_bits = (1 << maps.size) - 1
    groups = []
    for market, phase, cohort in product(
        ("KR", "US", "ALL"), ("train", "validation", "test", "full"), ("all", "common"),
    ):
        if market == "ALL" and phase != "full":
            continue
        mask = maps.group_bits["observed"]
        mask &= maps.group_bits[market] if market != "ALL" else all_bits
        mask &= maps.group_bits[phase] if phase != "full" else all_bits
        mask &= maps.bits[GRID_COMMON] if cohort == "common" else all_bits
        groups.append((market, phase, cohort, mask, mask.bit_count(),
                       [(mask & positive).bit_count() for positive in maps.target_bits]))
    done = {r[0] for r in db.execute("SELECT configuration FROM finished")}
    for config in configs:
        if config["id"] in done:
            continue
        predicted_mask, ready = maps.prediction(config)
        values = []
        for market, phase, cohort, group, total, positives in groups:
            selected = predicted_mask & group
            predicted, eligible = selected.bit_count(), (ready & group).bit_count()
            for goal, target, positive_count in zip(GOALS, maps.target_bits, positives, strict=True):
                tp = (selected & target).bit_count()
                values.append((config["id"], market, goal, phase, cohort,
                               total, positive_count, predicted, eligible,
                               *metrics(tp, predicted, positive_count, total)))
        with db:
            db.executemany("INSERT INTO scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)
            db.execute("INSERT INTO finished VALUES (?)", (config["id"],))
        done.add(config["id"])
        if len(done) % 25 == 0 or len(done) == len(configs):
            progress(db, folder, "scoring", completed=len(done), total=len(configs))


ORDER = "COALESCE(s.f1,-1) DESC,COALESCE(s.precision,-1) DESC,s.tp DESC,s.configuration"


def selection(db: sqlite3.Connection, market: str, goal: float) -> dict[str, Any]:
    train = db.execute(
        "SELECT s.*,c.family,c.lookback,c.data FROM scores s JOIN configurations c "
        "ON s.configuration=c.id WHERE market=? AND goal=? AND phase='train' "
        "AND cohort='common' ORDER BY " + ORDER, (market, goal),
    ).fetchall()
    shortlist = []
    seen = set()
    for row in train:
        key = (row["family"], row["lookback"])
        if key not in seen:
            seen.add(key)
            shortlist.append(row["configuration"])
    if not shortlist:
        return {"market": market, "goal": goal, "state": "no_training_samples"}
    if not train[0]["positives"]:
        return {"market": market, "goal": goal, "state": "no_training_positives"}
    marks = ",".join("?" for _ in shortlist)
    best = db.execute(
        "SELECT s.*,c.data FROM scores s JOIN configurations c ON c.id=s.configuration "
        "WHERE market=? AND goal=? AND phase='validation' AND cohort='common' "
        f"AND configuration IN ({marks}) ORDER BY " + ORDER + " LIMIT 1",
        (market, goal, *shortlist),
    ).fetchone()
    if not best or not best["positives"]:
        return {"market": market, "goal": goal, "state": "no_validation_positives"}
    tests = [dict(row) for row in db.execute(
        "SELECT * FROM scores WHERE configuration=? AND market=? AND goal=? AND phase='test'",
        (best["configuration"], market, goal),
    )]
    baselines = [dict(row) for row in db.execute(
        "SELECT * FROM scores WHERE configuration IN ('always_up','never_up') "
        "AND market=? AND goal=? AND phase='test' AND cohort='common'", (market, goal),
    )]
    validation = dict(best)
    config = json.loads(validation.pop("data"))
    return {"market": market, "goal": goal, "state": "selected_on_validation",
            "configuration": config, "train_shortlist": shortlist,
            "validation": validation,
            "test": tests, "test_baselines": baselines}


def annotate_winner(db: sqlite3.Connection, maps: Bitmaps, winner: dict[str, Any]) -> None:
    if winner["state"] != "selected_on_validation":
        return
    prediction, _ = maps.prediction(winner["configuration"])
    cohort = maps.group_bits["test"] & maps.group_bits["observed"] & maps.bits[GRID_COMMON]
    if winner["market"] != "ALL":
        cohort &= maps.group_bits[winner["market"]]
    target = maps.target_bits[GOALS.index(winner["goal"])]
    length = (maps.size + 7) // 8
    prediction_bytes = prediction.to_bytes(length, "little")
    cohort_bytes = cohort.to_bytes(length, "little")
    target_bytes = target.to_bytes(length, "little")
    examples: dict[str, list[dict[str, Any]]] = {
        "false_positive": [], "missed_up": [], "correct_up": [],
    }
    sessions: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])
    total_return = 0.0
    selected = 0
    where = "phase='test' AND outcome='observed'"
    args: tuple[Any, ...] = ()
    if winner["market"] != "ALL":
        where += " AND market=?"
        args = (winner["market"],)
    for row in db.execute("SELECT * FROM cases WHERE " + where + " ORDER BY id", args):
        index, shift = divmod(row["id"], 8)
        bit = 1 << shift
        if not bit & cohort_bytes[index]:
            continue
        predicts = bool(prediction_bytes[index] & bit)
        actual = bool(target_bytes[index] & bit)
        bucket = sessions[row["market"] + ":" + row["session"]]
        bucket[0] += int(predicts and actual)
        bucket[1] += int(predicts)
        bucket[2] += int(actual)
        bucket[3] += 1
        if predicts:
            selected += 1
            total_return += row["actual_return"]
        kind = "correct_up" if predicts and actual else "false_positive" if predicts \
            else "missed_up" if actual else None
        if kind and len(examples[kind]) < 5:
            examples[kind].append(dict(row))
    winner["chronological_examples"] = examples
    winner["test_mean_return_when_predicting_up"] = total_return / selected if selected else None
    winner["test_sessions"] = {key: dict(zip(
        ("tp", "fp", "fn", "tn", "precision", "recall", "f1"), metrics(*values), strict=True,
    )) for key, values in sessions.items()}


def report(db: sqlite3.Connection, folder: Path, maps: Bitmaps) -> dict[str, Any]:
    winners = []
    exploratory = []
    for market, goal in product(("KR", "US", "ALL"), GOALS):
        if market != "ALL":
            winner = selection(db, market, goal)
            annotate_winner(db, maps, winner)
            winners.append(winner)
        rows = db.execute(
            "SELECT s.*,c.data FROM scores s JOIN configurations c ON c.id=s.configuration "
            "WHERE market=? AND goal=? AND phase='full' AND cohort='common' ORDER BY "
            + ORDER + " LIMIT 10", (market, goal),
        ).fetchall()
        exploratory.append({"market": market, "goal": goal,
                            "rows": [{**{k: v for k, v in dict(row).items() if k != 'data'},
                                      "parameters": json.loads(row["data"])} for row in rows]})
    result = {
        "state": "complete", "ai_calls": 0, "forecast_cases": maps.size,
        "configuration_count": db.execute("SELECT COUNT(*) FROM configurations").fetchone()[0],
        "outcomes": [dict(row) for row in db.execute(
            "SELECT market,outcome,COUNT(*) AS count FROM cases GROUP BY market,outcome")],
        "winners": winners, "full_day_exploration": exploratory,
        "selection_rule": "Train selects parameters per family/lookback; validation selects "
        "the winner; test scores never select parameters. Full-day rankings are exploratory.",
        "interpretation": "Single-day internal chronological evaluation, not next-day validation",
    }
    if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise ValueError("Search database integrity verification failed")
    (folder / "report.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def run(dataset: Path, folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (folder / "search.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = sqlite3.connect(folder / "search.sqlite3")
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.executescript(SCHEMA)
        try:
            source = sqlite3.connect(dataset.as_uri() + "?mode=ro", uri=True)
            try:
                meta = {r[0]: json.loads(r[1]) for r in source.execute("SELECT * FROM metadata")}
            finally:
                source.close()
            if meta["state"] not in ("complete", "complete_with_errors"):
                raise ValueError("Historical collection is not complete")
            configs = configurations()
            manifest = {"source": str(dataset), "dataset_sha256": hash_file(dataset),
                        "search_code_sha256": hash_file(Path(__file__)),
                        "worker_code_sha256": hash_file(Path(__file__).with_name(
                            "replay_market_day.py")), "scopes": meta["scopes"],
                        "configuration_sha256": hashlib.sha256(encode(configs).encode()).hexdigest(),
                        "goals": GOALS, "step_seconds": STEP, "split": [0.6, 0.2, 0.2]}
            old = get_meta(db, "manifest")
            if old is not None and encode(old) != encode(manifest):
                raise ValueError("Source, code, or parameters changed; use a new output folder")
            with db:
                set_meta(db, "manifest", manifest)
                db.executemany("INSERT OR IGNORE INTO configurations VALUES (?,?,?,?)", [
                    (c["id"], c["family"], c["lookback"], encode(c)) for c in configs
                ])
            if get_meta(db, "progress", {}).get("stage") == "complete":
                return
            progress(db, folder, "starting", configurations=len(configs))
            extract(db, dataset, folder)
            maps = load_bitmaps(db, folder)
            score_grid(db, folder, maps, configs)
            progress(db, folder, "reporting", configurations=len(configs), cases=maps.size)
            result = report(db, folder, maps)
            progress(db, folder, "complete", configurations=len(configs),
                     cases=maps.size, winners=len(result["winners"]))
        except BaseException as error:
            progress(db, folder, "failed", error_type=type(error).__name__, message=str(error))
            raise
        finally:
            db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if args.status:
        db = sqlite3.connect((args.output / "search.sqlite3").resolve().as_uri()
                             + "?mode=ro", uri=True)
        try:
            print(json.dumps(get_meta(db, "progress"), ensure_ascii=False, indent=2))
        finally:
            db.close()
        return
    if args.dataset is None:
        parser.error("--dataset is required")
    run(args.dataset.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
