"""Independent paper workers, durable recovery, and native service definitions."""

from __future__ import annotations

import json
import os
import plistlib
import signal
import subprocess
import sys
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from .ai import parse_codex_events
from .audit import Audit
from .core import (
    SafetyError,
    Settings,
    market_date,
    now_utc,
    probable_regular,
    process_identity,
    regular_session,
    stamp,
)
from .engine import Engine
from .simulation import Simulation
from .store import Store

ROLES = ("market", "scan", "risk", "review", "supervise")


def alive(pid: int, expected: str | None = None) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return expected is None or process_identity(pid) == expected


def recover(store: Store) -> dict[str, int]:
    """Recover only dead owners. A slow live model is never a timeout failure."""
    audit = Audit(store)
    recovered = 0
    for worker in store.db.execute("SELECT * FROM workers").fetchall():
        if alive(worker["pid"], json.loads(worker["data"]).get("process_identity")):
            continue
        owner = worker["owner"]
        with store.transaction(write=True):
            jobs = store.db.execute(
                "SELECT id FROM jobs WHERE status='reviewing' AND owner=?", (owner,)
            ).fetchall()
            for job in jobs:
                store.finish_job(job[0], "interrupted", {"reason": "worker_exited"})
                recovered += 1
            for call in store.db.execute(
                "SELECT id,data FROM ai_calls WHERE status='reserved'"
            ).fetchall():
                detail = json.loads(call[1])
                if detail.get("owner") != owner:
                    continue
                child = detail.get("pid")
                if (
                    isinstance(child, int)
                    and detail.get("pid_identity")
                    and alive(child, detail["pid_identity"])
                ):
                    # The worker died; discard its approval and terminate the detached child.
                    os.killpg(child, signal.SIGTERM)
                usage = detail.get("usage")
                journal = store.path.parent / "ai-events" / f"{call[0]}.jsonl"
                if journal.exists():
                    found, _message, _actions = parse_codex_events(journal.read_text())
                    usage = found or usage
                store.finish_ai(
                    call[0], "interrupted", usage, {"recovered_at": now_utc().isoformat()}
                )
            if worker["role"] == "scan":
                store.db.execute("UPDATE ticks SET status='failed' WHERE status='started'")
            store.db.execute(
                "DELETE FROM workers WHERE role=? AND owner=?", (worker["role"], owner)
            )
        if worker["phase"] != "finished":
            audit.record("worker", "process_exited", {"role": worker["role"]}, state="restarting")
    # Migrate abandoned calls from the previous single-process runner.
    for call in store.db.execute("SELECT id,data FROM ai_calls WHERE status='reserved'").fetchall():
        detail = json.loads(call[1])
        if detail.get("owner"):
            continue
        pid = detail.get("pid")
        if not isinstance(pid, int) or not alive(pid):
            store.finish_ai(
                call[0], "interrupted", detail.get("usage"), {"recovery": "legacy_orphan"}
            )
    return {"interrupted_jobs": recovered}


def review_once(engine: Engine, owner: str) -> int:
    store = engine.store
    candidates = store.lease_jobs(owner)
    if not candidates:
        return 0
    current = []
    for item in candidates:
        try:
            at = now_utc()
            session = regular_session(
                item["market"],
                engine.calendar(item["market"], market_date(item["market"], at), at)["today"],
            )
            if session and session.entry_allowed(
                at, int(engine.settings.get("strategy", "entry_cutoff_minutes"))
            ):
                current.append(item)
            else:
                store.finish_job(
                    item["signal_key"], "expired", {"reason": "market_closed_or_cutoff"}
                )
        except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
            engine.audit.exception(
                "review_job", error, job=item["signal_key"], symbol=item["symbol"]
            )
            store.finish_job(item["signal_key"], "rejected", {"reason": type(error).__name__})
    if not current:
        return 0
    # One current signal per symbol. Compare all candidates together without a count cap.
    unique: dict[str, dict[str, Any]] = {}
    for item in current:
        previous = unique.get(item["symbol"])
        if previous:
            store.finish_job(previous["signal_key"], "superseded", {})
        unique[item["symbol"]] = item
    current = list(unique.values())
    try:
        decisions = engine.reviewer.review(current, now_utc())
        for market in {item["market"] for item in current}:
            at = now_utc()
            session = regular_session(
                market, engine.calendar(market, market_date(market, at), at)["today"]
            )
            if session:
                engine.enter_approved(
                    [item for item in current if item["market"] == market], decisions, session
                )
        for item in current:
            trade = store.db.execute(
                "SELECT 1 FROM trades WHERE event_key=?", ("entry:" + item["signal_key"],)
            ).fetchone()
            store.finish_job(
                item["signal_key"],
                "filled" if trade else "not_filled",
                decisions.get(item["symbol"], {}),
            )
        engine.audit.recovered("ai")
        return len(current)
    except Exception as error:
        for item in current:
            store.finish_job(item["signal_key"], "failed", {"reason": type(error).__name__})
        raise


def risk_once(engine: Engine) -> None:
    at = now_utc()
    for market in {position["market"] for position in engine.store.positions()}:
        try:
            if not probable_regular(market, at):
                continue
            session = regular_session(
                market, engine.calendar(market, market_date(market, at), at)["today"]
            )
            if session and session.contains(at):
                engine.inspect_positions(market, session, at)
        except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
            engine.audit.exception("risk", error, market=market)
    engine.resolve_settlements(at)


def market_once(engine: Engine) -> None:
    started = now_utc()
    for market in ("KR", "US"):
        if not probable_regular(market, started):
            continue
        try:
            calendar = engine.calendar(market, market_date(market, started), started)
            session = regular_session(market, calendar["today"])
            if session is None or not session.contains(started):
                continue
            calls = engine.client.calls
            items = engine.shortlist(market, started, engine.fx(market, started), refresh=True)
            completed = now_utc()
            engine.store.event(
                "market_check",
                {
                    "market": market,
                    "started_at": started,
                    "completed_at": completed,
                    "duration_seconds": (completed - started).total_seconds(),
                    "eligible": len(items),
                    "api_calls": engine.client.calls - calls,
                    "ai_invoked": False,
                },
            )
            engine.audit.recovered("market_check", market=market)
        except Exception as error:
            engine.audit.exception("market_check", error, market=market)
            raise


def supervise(store: Store) -> None:
    recover(store)
    audit = Audit(store)
    for worker in store.db.execute("SELECT * FROM workers").fetchall():
        if now_utc() - stamp(worker["seen_at"]) > timedelta(seconds=120):
            audit.record(
                "worker",
                "heartbeat_stale",
                {"role": worker["role"], "phase": worker["phase"]},
                state="needs_attention",
            )
        else:
            audit.recovered("worker", role=worker["role"])
    present = {row[0] for row in store.db.execute("SELECT role FROM workers")}
    for role in ROLES:
        key = "missing_worker:" + role
        if role in present:
            store.db.execute("DELETE FROM cache WHERE key=?", (key,))
            continue
        missing = store.cache(key)
        if missing is None:
            store.put_cache(key, True, now_utc())
        elif now_utc() - missing[0] > timedelta(seconds=30):
            audit.record("worker", "service_missing", {"role": role}, state="needs_attention")
    day = now_utc().date().isoformat()
    backup_day = store.cache("backup_day")
    if backup_day is None or backup_day[1] != day:
        if store.db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise SafetyError("Ledger integrity failed; preserve the ledger and verified backups")
        path = store.backup()
        store.put_cache("backup_day", day, now_utc())
        store.event("verified_backup", {"path": str(path)})
    # Notify only new actionable incidents. No per-poll model calls or status spam.
    for row in store.db.execute(
        "SELECT id,component,code,state FROM incidents WHERE state IN ('waiting_external','needs_evidence','needs_attention')"
    ).fetchall():
        key = "notified:" + row[0]
        notification = store.cache(key)
        if notification and (
            notification[1] is True
            or (isinstance(notification[1], dict) and notification[1].get("delivered"))
            or now_utc() - notification[0] < timedelta(minutes=5)
        ):
            continue
        message = f"Paper trading needs attention: {row[1]} / {row[2]}"
        result = subprocess.run(
            [
                "/usr/bin/osascript",
                "-e",
                'on run argv\ndisplay notification (item 1 of argv) with title "Papertrade"\nend run',
                message,
            ],
            capture_output=True,
            check=False,
        )
        store.event(
            "incident_notification", {"incident_id": row[0], "delivered": result.returncode == 0}
        )
        store.put_cache(key, {"delivered": result.returncode == 0}, now_utc())


def run_worker(settings: Settings, store: Store, role: str) -> None:
    owner = uuid.uuid4().hex
    recover(store)
    store.role = role
    store.owner = owner
    store.process_identity = process_identity(os.getpid())
    if role == "scan":
        store.db.execute("UPDATE ticks SET status='failed' WHERE status='started'")
    engine = Engine(settings, store)
    simulation = (
        Simulation(settings, store) if settings.data.get("simulation", {}).get("enabled") else None
    )
    retry = 0.0

    def stop(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    while True:
        store.heartbeat(role, owner, "idle")
        try:
            if time.time() >= retry:
                cycle_started = time.time()
                store.heartbeat(role, owner, "working")
                if role == "market":
                    simulation.observe() if simulation else market_once(engine)
                    retry = cycle_started + 60
                elif role == "scan":
                    at = now_utc()
                    if store.claim_tick(at):
                        try:
                            simulation.propose() if simulation else engine.collect(at)
                            store.finish_tick(at, "completed")
                        except BaseException:
                            store.finish_tick(at, "failed")
                            raise
                    retry = time.time() + 15
                elif role == "risk":
                    simulation.inspect() if simulation else risk_once(engine)
                    retry = cycle_started + 60
                elif role == "review":
                    simulation.review() if simulation else review_once(engine, owner)
                    retry = time.time() + 5
                else:
                    supervise(store)
                    retry = time.time() + 15
                engine.audit.recovered("service", role=role)
        except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
            engine.audit.exception("service", error, role=role)
            retry = max(time.time() + 5, getattr(error, "retry_at", None) or 0)
        time.sleep(1)


def generate_services(settings: Settings) -> list[Path]:
    directory = settings.root / "launchd"
    directory.mkdir(exist_ok=True)
    paths = []
    for role in ROLES:
        label = "com.elsholtzia.papertrade." + role
        path = directory / (label + ".plist")
        value = {
            "Label": label,
            "ProgramArguments": [
                sys.executable,
                "-m",
                "papertrade",
                "--root",
                str(settings.root),
                "worker",
                role,
            ],
            "WorkingDirectory": str(settings.root),
            "RunAtLoad": True,
            "KeepAlive": True,
            "ThrottleInterval": 10,
            "ProcessType": "Background",
            "EnvironmentVariables": {
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "PYTHONUNBUFFERED": "1",
            },
            "StandardOutPath": str(settings.state_dir / (role + ".stdout.log")),
            "StandardErrorPath": str(settings.state_dir / (role + ".stderr.log")),
        }
        path.write_bytes(plistlib.dumps(value))
        paths.append(path)
    return paths
