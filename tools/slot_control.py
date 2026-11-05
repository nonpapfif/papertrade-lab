"""Queue and inspect local commands for a scheduled shared-feed paper validation."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Self, cast

from tools.us_adaptive_ai import ENTRY_OPTIONS

SPOOL_NAME = "slot-control.sqlite3"
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


def encode(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class SlotCommand:
    id: str
    action: str
    slot_id: str
    spec: dict[str, Any] | None = None

    @property
    def command_id(self) -> str:
        return self.id

    @property
    def capital(self) -> float | None:
        return float(self.spec["capital"]) if self.spec else None

    @property
    def incumbent(self) -> dict[str, str] | None:
        return cast(dict[str, str], self.spec["incumbent"]) if self.spec else None

    @property
    def candidate(self) -> dict[str, str] | None:
        return cast(dict[str, str], self.spec["candidate"]) if self.spec else None

    def validate(self) -> None:
        if not IDENTIFIER.fullmatch(self.id) or not IDENTIFIER.fullmatch(self.slot_id):
            raise ValueError("Command and slot IDs must be 1 to 64 ASCII letters, digits, _ or -")
        if self.action == "stop" and self.spec is None:
            return
        if (
            self.action != "add"
            or self.spec is None
            or set(self.spec)
            != {
                "capital",
                "incumbent",
                "candidate",
            }
        ):
            raise ValueError("Invalid slot command")
        capital = self.spec["capital"]
        if type(capital) not in (int, float) or not math.isfinite(capital) or capital <= 0:
            raise ValueError("Capital must be positive and finite")
        for name in ("incumbent", "candidate"):
            config = self.spec[name]
            if (
                not isinstance(config, dict)
                or config.keys() != ENTRY_OPTIONS.keys()
                or any(config[key] not in choices for key, choices in ENTRY_OPTIONS.items())
            ):
                raise ValueError(f"Invalid {name} policy. Supply all supported entry options")


class SlotControl:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    @classmethod
    def open(cls, path: Path, *, initialize: bool = False) -> SlotControl:
        if initialize and not path.exists():
            path.touch(mode=0o600, exist_ok=True)
        connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=rw", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            if initialize:
                connection.executescript("""
                    BEGIN IMMEDIATE;
                    CREATE TABLE IF NOT EXISTS commands (
                        sequence INTEGER PRIMARY KEY,
                        id TEXT NOT NULL UNIQUE,
                        action TEXT NOT NULL,
                        slot_id TEXT NOT NULL,
                        spec TEXT NOT NULL,
                        submitted_at REAL NOT NULL,
                        disposition TEXT,
                        detail TEXT,
                        recorded_at REAL
                    );
                    CREATE TABLE IF NOT EXISTS state (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        slots TEXT NOT NULL,
                        closed INTEGER NOT NULL
                    );
                    INSERT OR IGNORE INTO state VALUES (1, '[]', 0);
                    COMMIT;
                """)
            if connection.execute("SELECT 1 FROM state WHERE singleton = 1").fetchone() is None:
                raise ValueError("Slot control state is missing")
        except (sqlite3.Error, ValueError):
            connection.close()
            raise
        return cls(connection)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.connection.close()

    def submit(self, command: SlotCommand, *, end_epoch: float) -> dict[str, Any]:
        command.validate()
        payload = command.action, command.slot_id, encode(command.spec)
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            row = self.connection.execute(
                "SELECT * FROM commands WHERE id = ?", (command.id,)
            ).fetchone()
            if row is not None:
                if tuple(row[key] for key in ("action", "slot_id", "spec")) != payload:
                    raise ValueError("Command ID already names a different request")
                return self._report(row)
            if (
                time.time() >= end_epoch
                or self.connection.execute(
                    "SELECT closed FROM state WHERE singleton = 1"
                ).fetchone()["closed"]
            ):
                raise ValueError("Slot command window has closed")
            self.connection.execute(
                "INSERT INTO commands (id, action, slot_id, spec, submitted_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (command.id, *payload, time.time()),
            )
            row = self.connection.execute(
                "SELECT * FROM commands WHERE id = ?", (command.id,)
            ).fetchone()
            return self._report(row)

    @staticmethod
    def _report(row: sqlite3.Row) -> dict[str, Any]:
        report = dict(row)
        report["spec"] = json.loads(report["spec"])
        if report["detail"] is not None:
            report["detail"] = json.loads(report["detail"])
        return report

    def pending(self) -> list[SlotCommand]:
        commands = [
            SlotCommand(row["id"], row["action"], row["slot_id"], json.loads(row["spec"]))
            for row in self.connection.execute(
                "SELECT * FROM commands WHERE disposition IS NULL ORDER BY sequence"
            )
        ]
        for command in commands:
            command.validate()
        return commands

    def record(self, command: SlotCommand, disposition: str, detail: dict[str, Any]) -> None:
        evidence = disposition, encode(detail)
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            row = self.connection.execute(
                "SELECT * FROM commands WHERE id = ?", (command.id,)
            ).fetchone()
            if row is None or (row["action"], row["slot_id"], row["spec"]) != (
                command.action,
                command.slot_id,
                encode(command.spec),
            ):
                raise ValueError("Command evidence does not match the queued request")
            if row["disposition"] is None:
                self.connection.execute(
                    "UPDATE commands SET disposition=?, detail=?, recorded_at=? WHERE id=?",
                    (*evidence, time.time(), command.id),
                )
            elif (row["disposition"], row["detail"]) != evidence:
                raise ValueError("Command already has different evidence")

    def snapshot(self, slots: list[dict[str, Any]]) -> None:
        with self.connection:
            self.connection.execute("UPDATE state SET slots=? WHERE singleton=1", (encode(slots),))

    def close_submissions(self) -> None:
        with self.connection:
            self.connection.execute("UPDATE state SET closed=1 WHERE singleton=1")

    def report(self) -> dict[str, Any]:
        with self.connection:
            self.connection.execute("BEGIN")
            row = self.connection.execute("SELECT * FROM state WHERE singleton=1").fetchone()
            commands = self.connection.execute("SELECT * FROM commands ORDER BY sequence")
            return {
                "slots": json.loads(row["slots"]),
                "closed": bool(row["closed"]),
                "commands": [self._report(command) for command in commands],
            }


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="Schedule output directory")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("list")
    stop = actions.add_parser("stop")
    add = actions.add_parser("add")
    for action in (stop, add):
        action.add_argument("--command-id", required=True)
        action.add_argument("--slot-id", required=True)
    add.add_argument("--capital", type=float, required=True)
    add.add_argument(
        "--incumbent", type=json.loads, required=True, help="Entry options JSON object"
    )
    add.add_argument(
        "--candidate", type=json.loads, required=True, help="Entry options JSON object"
    )
    args = parser.parse_args()

    try:
        status = json.loads((args.output / "status.json").read_text())
        schedule = status["schedule"]
        if schedule.get("dynamic_slots") is not True:
            raise ValueError("This schedule did not enable dynamic slots")
        end_epoch = schedule["end_epoch"]
        if not isinstance(end_epoch, (int, float)) or not math.isfinite(end_epoch):
            raise ValueError("Saved schedule deadline is invalid")
        accepting = status["state"] in ("waiting", "running", "restarting")
        with SlotControl.open(args.output / SPOOL_NAME) as control:
            if args.action == "list":
                result = control.report()
                result["accepting_commands"] = (
                    accepting and time.time() < end_epoch and not result["closed"]
                )
            else:
                spec = None
                if args.action == "add":
                    spec = {
                        "capital": args.capital,
                        "incumbent": args.incumbent,
                        "candidate": args.candidate,
                    }
                result = control.submit(
                    SlotCommand(args.command_id, args.action, args.slot_id, spec),
                    end_epoch=end_epoch if accepting else 0,
                )
    except (KeyError, OSError, OverflowError, sqlite3.Error, TypeError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
