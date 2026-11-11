"""Command-line entry point. No live brokerage execution exists."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

from .ai import DISABLED_FEATURES, Reviewer, sandbox_profile
from .api import ApiError, secret_file
from .audit import Audit
from .clock import now_utc
from .core import SEOUL, SafetyError, Settings, decimal
from .engine import Engine
from .jsonio import paper_json as encode
from .service import ROLES, generate_services, run_worker
from .simulation import Simulation
from .store import Store


def doctor(settings: Settings, store: Store) -> dict[str, object]:
    for name in ("TOSS_CLIENT_ID", "TOSS_CLIENT_SECRET"):
        secret_file(settings.secrets_dir / name)
    binary = Path(settings.data["ai"]["binary"]).resolve()
    status = subprocess.run(
        [str(binary), "login", "status"], check=False, capture_output=True, text=True
    )
    features = subprocess.run(
        [str(binary), "features", "list"], check=False, capture_output=True, text=True
    )
    available = {line.split()[0] for line in features.stdout.splitlines() if line.split()}
    if features.returncode or set(DISABLED_FEATURES) - available:
        raise SafetyError("Required Codex tool-disable controls are unavailable")
    profile = sandbox_profile(Path(sys.executable), [settings.secrets_dir, settings.state_dir])
    program = (
        "import os,sys; "
        "p=sys.argv[1]; "
        "\ntry:\n fd=os.open(p,os.O_RDONLY); os.close(fd)"
        "\nexcept PermissionError:\n sys.exit(0)"
        "\nelse:\n sys.exit(1)"
    )
    protection = subprocess.run(
        [
            "/usr/bin/sandbox-exec",
            "-p",
            profile,
            sys.executable,
            "-c",
            program,
            str(settings.secrets_dir / "TOSS_CLIENT_SECRET"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if protection.returncode:
        raise SafetyError("OS-level secret-read isolation did not pass")
    child_program = (
        "import subprocess,sys\n"
        "try:\n r=subprocess.run(['/usr/bin/true'],check=False,capture_output=True)"
        "\nexcept PermissionError:\n sys.exit(0)"
        "\nelse:\n sys.exit(1)"
    )
    child = subprocess.run(
        ["/usr/bin/sandbox-exec", "-p", profile, sys.executable, "-c", child_program],
        check=False,
        capture_output=True,
        text=True,
    )
    if child.returncode:
        raise SafetyError("OS-level child-process isolation did not pass")
    return {
        "credentials": "valid_local_format_and_permissions",
        "codex_login": status.returncode == 0,
        "tool_disable_controls": "available",
        "os_secret_read_protection": "passed",
        "os_child_process_protection": "passed",
        "model_invoked": False,
        "ledger": store.summary(now_utc()),
    }


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Strictly virtual USD 1000 trading lab")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in (
        "init",
        "status",
        "doctor",
        "tick",
        "usage",
        "launchd",
        "health",
        "backup",
        "consult",
    ):
        commands.add_parser(name)
    audit_parser = commands.add_parser("audit")
    audit_parser.add_argument("--open", action="store_true")
    audit_parser.add_argument("--output", type=Path)
    audit_parser.add_argument("--events", action="store_true")
    audit_parser.add_argument("--incident")
    audit_parser.add_argument("--emergency", action="store_true")
    worker = commands.add_parser("worker")
    worker.add_argument("role", choices=ROLES)
    action_parser = commands.add_parser("corporate-action")
    action_parser.add_argument("file", type=Path)
    ai_check = commands.add_parser("ai-check")
    ai_check.add_argument("--model")
    ai_check.add_argument("--effort")
    quotes = commands.add_parser("quotes")
    quotes.add_argument("symbols", nargs="+", default=["NVDA", "QQQM", "MSFT"])
    simulated = commands.add_parser("simulate")
    simulated.add_argument("symbol")
    simulated.add_argument("quantity")
    simulated.add_argument("price")
    simulated.add_argument("--market", choices=("KR", "US"), default="US")
    simulated.add_argument("--fx", default="1")
    simulated.add_argument("--reason", default="Explicitly assumed simulation experiment")
    scan = commands.add_parser("scan")
    scan.add_argument("market", choices=["KR", "US"])
    arguments = parser.parse_args()
    settings = Settings.load(arguments.root)
    settings.state_dir.mkdir(exist_ok=True, mode=0o700)
    if arguments.command == "audit" and arguments.emergency:
        events = []
        path = settings.state_dir / "emergency-audit.jsonl"
        if path.exists():
            for line_number, line in enumerate(path.read_text().splitlines(), start=1):
                try:
                    events.append(json.loads(line))
                except ValueError:
                    events.append({"line": line_number, "state": "invalid_journal_record"})
        print(encode({"emergency_events": events}))
        return 0
    role = (
        arguments.role
        if arguments.command == "worker"
        else (
            "scan"
            if arguments.command in ("tick", "scan")
            else "risk"
            if arguments.command == "corporate-action"
            else "manual"
        )
    )
    lock_path = settings.state_dir / (role + ".lock")
    with lock_path.open("a") as lock:
        if arguments.command not in ("status", "usage", "audit", "health", "backup"):
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print(encode({"status": "another_run_is_active", "model_invoked": False}))
                return 0
        database = settings.state_dir / "paper.sqlite3"
        if arguments.command != "init" and not database.exists():
            Audit(SimpleNamespace(path=database)).emergency(
                {
                    "at": now_utc(),
                    "component": "startup",
                    "code": "ledger_missing",
                    "role": role,
                    "state": "needs_attention",
                }
            )
            print(
                encode(
                    {
                        "status": "ledger_missing",
                        "recovery": "restore_verified_ledger_before_restart",
                    }
                ),
                file=sys.stderr,
            )
            return 1
        try:
            store = Store(database)
        except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
            Audit(SimpleNamespace(path=database)).emergency(
                {
                    "at": now_utc(),
                    "component": "startup",
                    "code": "ledger_unavailable",
                    "role": role,
                    "exception_type": type(error).__name__,
                    "state": "needs_attention",
                }
            )
            return 1
        try:
            engine = Engine(settings, store)
            at = now_utc()
            if arguments.command in ("init", "status"):
                result = store.summary(at)
            elif arguments.command == "consult":
                request_id = uuid.uuid4().hex
                store.put_cache("simulation_manual_request", request_id, at)
                result = {
                    "simulation_consultation_requested": request_id,
                    "ai_invoked_by_this_command": False,
                }
            elif arguments.command == "doctor":
                result = doctor(settings, store)
            elif arguments.command == "quotes":
                result = {
                    "observed_at": at.isoformat(),
                    "prices": engine.client.prices(arguments.symbols),
                    "model_invoked": False,
                    "paper_trades_created": False,
                }
            elif arguments.command == "scan":
                result = {
                    "market": arguments.market,
                    "observed_at": at.isoformat(),
                    "candidates": engine.scan(arguments.market, at),
                    "model_invoked": False,
                    "paper_trades_created": False,
                }
            elif arguments.command == "simulate":
                s = Simulation(settings, store)
                s.save_quote(
                    arguments.market,
                    {
                        "symbol": arguments.symbol,
                        "lastPrice": arguments.price,
                        "timestamp": at.isoformat(),
                        "reference_kind": "explicit_user_price_assumption",
                    },
                    decimal(arguments.fx),
                    "explicit_user_assumption",
                )
                s.fill(
                    arguments.symbol,
                    arguments.market,
                    decimal(arguments.quantity),
                    "manual-simulation:" + uuid.uuid4().hex,
                    arguments.reason,
                    {},
                    refresh=False,
                )
                result = store.summary(at)
            elif arguments.command == "ai-check":
                candidate = {
                    "symbol": "SMOKE",
                    "signal_key": "smoke:"
                    + at.astimezone(SEOUL).date().isoformat()
                    + ":"
                    + uuid.uuid4().hex,
                    "purpose": "Synthetic schema check only; not a tradable security",
                }
                result = {
                    "decisions": Reviewer(settings, store).review(
                        [candidate], at, smoke=True, model=arguments.model, effort=arguments.effort
                    ),
                    "paper_trades_created": False,
                    "ledger": store.summary(at),
                }
            elif arguments.command == "usage":
                result = Reviewer(settings, store).metadata()
                result["project_ai_limits"] = "none"
                store.event("usage_snapshot", result)
            elif arguments.command == "launchd":
                result = {
                    "generated": [str(path) for path in generate_services(settings)],
                    "activated": False,
                }
            elif arguments.command == "worker":
                run_worker(settings, store, arguments.role)
                return 0
            elif arguments.command == "audit":
                result = Audit(store).summary(arguments.open)
                if arguments.events or arguments.output or arguments.incident:
                    result["events"] = Audit(store).timeline(arguments.incident)
                if arguments.output:
                    arguments.output.write_text(encode(result) + "\n")
                    arguments.output.chmod(0o600)
            elif arguments.command == "health":
                result = Audit(store).summary(True)
                result["ledger_integrity"] = store.db.execute("PRAGMA quick_check").fetchone()[0]
                result["ledger"] = store.summary(at)
            elif arguments.command == "backup":
                result = {"verified_backup": str(store.backup())}
            elif arguments.command == "corporate-action":
                store.apply_action(json.loads(arguments.file.read_text()))
                result = store.summary(at)
            else:
                result = engine.tick(at)
            print(encode(result))
            return 0
        except KeyboardInterrupt:
            store.heartbeat(store.role, store.owner, "finished")
            store.event("worker_stopped", {"role": store.role, "reason": "operator_signal"})
            return 0
        except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
            message = (
                str(error) if isinstance(error, (ApiError, SafetyError)) else type(error).__name__
            )
            Audit(store).exception("command", error, command=arguments.command)
            print(encode({"status": "failed_closed", "reason": message}), file=sys.stderr)
            return 1
        finally:
            if store.role == "manual":
                store.db.execute(
                    "UPDATE workers SET phase='finished' WHERE role='manual' AND pid=?",
                    (os.getpid(),),
                )
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
