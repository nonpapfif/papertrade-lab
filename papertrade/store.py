"""Transactional USD paper ledger and bounded review history."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from .core import SEOUL, ZERO, SafetyError, decimal, encode, now_utc, stamp


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.role = "manual"
        self.owner = "manual"
        self.process_identity: str | None = None
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        existing = path.exists()
        if existing and path.stat().st_size == 0:
            raise SafetyError("Existing ledger is empty; restore a verified backup")
        self.db = sqlite3.connect(path, isolation_level=None)
        if (
            existing
            and not self.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cash'"
            ).fetchone()
        ):
            self.db.close()
            raise SafetyError("Existing file is not a paper ledger")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS positions (symbol TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS cash (
                id INTEGER PRIMARY KEY, amount TEXT NOT NULL, available_at TEXT,
                market TEXT, sale_date TEXT, source TEXT UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY, event_key TEXT UNIQUE NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS signals (key TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS ai_calls (
                id INTEGER PRIMARY KEY, event_key TEXT UNIQUE NOT NULL, day TEXT NOT NULL,
                status TEXT NOT NULL, input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0, cached_tokens INTEGER NOT NULL DEFAULT 0,
                data TEXT NOT NULL DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY, fetched_at TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL, at TEXT NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY(symbol, at));
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, at TEXT NOT NULL, kind TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS ticks (
                slot TEXT PRIMARY KEY, at TEXT NOT NULL, status TEXT NOT NULL);
        """)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS incidents (
                id TEXT PRIMARY KEY,component TEXT NOT NULL,code TEXT NOT NULL,state TEXT NOT NULL,
                first_at TEXT NOT NULL,last_at TEXT NOT NULL,occurrences INTEGER NOT NULL,
                retry_at TEXT,data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rate_limits (
                name TEXT PRIMARY KEY,next_at REAL NOT NULL DEFAULT 0,interval REAL NOT NULL,
                failures INTEGER NOT NULL DEFAULT 0,data TEXT NOT NULL DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS workers (
                role TEXT PRIMARY KEY,pid INTEGER NOT NULL,owner TEXT NOT NULL,
                phase TEXT NOT NULL,seen_at TEXT NOT NULL,data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,symbol TEXT NOT NULL,market TEXT NOT NULL,status TEXT NOT NULL,
                observed_at TEXT NOT NULL,owner TEXT,attempt INTEGER NOT NULL DEFAULT 0,
                retry_at REAL NOT NULL DEFAULT 0,data TEXT NOT NULL,result TEXT);
            CREATE INDEX IF NOT EXISTS jobs_symbol_market_status_at
                ON jobs(symbol,market,status,observed_at);
            CREATE TABLE IF NOT EXISTS corporate_actions (
                event_key TEXT PRIMARY KEY,symbol TEXT NOT NULL,kind TEXT NOT NULL,
                effective_date TEXT NOT NULL,source TEXT NOT NULL,data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS simulation_quotes (
                symbol TEXT PRIMARY KEY,market TEXT NOT NULL,data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS simulation_seen (
                key TEXT PRIMARY KEY,at TEXT NOT NULL);
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(ai_calls)")}
        if "usage_known" not in columns:
            self.db.execute("ALTER TABLE ai_calls ADD COLUMN usage_known INTEGER")
            self.db.execute(
                "UPDATE ai_calls SET usage_known=1 WHERE input_tokens>0 OR output_tokens>0 OR status='completed'"
            )
        if (
            existing
            and not self.db.execute(
                "SELECT 1 FROM cash WHERE source='initial-paper-capital'"
            ).fetchone()
        ):
            self.db.close()
            raise SafetyError("Initial capital record is missing; do not recreate the ledger")
        self.db.execute(
            "INSERT OR IGNORE INTO cash(amount,available_at,source) VALUES (?,?,?)",
            ("1000", "1970-01-01T00:00:00+00:00", "initial-paper-capital"),
        )
        self.db.execute("INSERT OR IGNORE INTO meta VALUES ('strategy_version','v1')")
        with self.transaction(write=True):
            semantics = self.db.execute(
                "SELECT value FROM meta WHERE key='candle_semantics'"
            ).fetchone()
            if semantics is None or semantics[0] != "toss_close_to_start_v2":
                self.db.execute("DELETE FROM candles")
                self.db.execute("DELETE FROM cache WHERE key LIKE 'scan_pending:%'")
                self.db.execute(
                    "UPDATE jobs SET status='expired' WHERE status IN ('queued','reviewing')"
                )
                self.db.execute(
                    "INSERT OR REPLACE INTO meta VALUES ('candle_semantics','toss_close_to_start_v2')"
                )
                self.event(
                    "derived_cache_rebuilt",
                    {"reason": "minute_timestamp_is_close_time", "financial_ledger_changed": False},
                )

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def transaction(self, write: bool = False) -> Iterator[None]:
        if self.db.in_transaction:
            yield
            return
        self.db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def event(self, kind: str, data: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO events(at,kind,data) VALUES (?,?,?)",
            (now_utc().isoformat(), kind, encode(data)),
        )

    def cache(self, key: str) -> tuple[datetime, Any] | None:
        row = self.db.execute("SELECT fetched_at,data FROM cache WHERE key=?", (key,)).fetchone()
        return (stamp(row[0]), json.loads(row[1])) if row else None

    def put_cache(self, key: str, value: Any, at: datetime) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO cache VALUES (?,?,?)", (key, at.isoformat(), encode(value))
        )

    def put_candles(self, symbol: str, rows: list[dict[str, Any]]) -> None:
        with self.transaction(write=True):
            self.db.executemany(
                "INSERT OR REPLACE INTO candles VALUES (?,?,?)",
                [(symbol, stamp(row["timestamp"]).isoformat(), encode(row)) for row in rows],
            )

    def candles(self, symbol: str, since: datetime, until: datetime) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT data FROM candles WHERE symbol=? AND at>=? AND at<? ORDER BY at",
            (symbol, since.isoformat(), until.isoformat()),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def positions(self) -> list[dict[str, Any]]:
        return [json.loads(row[0]) for row in self.db.execute("SELECT data FROM positions")]

    def settled_cash(self, at: datetime) -> Decimal:
        rows = self.db.execute("SELECT amount,available_at FROM cash").fetchall()
        return sum(
            (decimal(row[0]) for row in rows if row[1] is not None and stamp(row[1]) <= at), ZERO
        )

    def total_cash(self) -> Decimal:
        return sum((decimal(row[0]) for row in self.db.execute("SELECT amount FROM cash")), ZERO)

    def equity(self) -> Decimal:
        with self.transaction():
            return self.total_cash() + sum((decimal(p["mark_usd"]) for p in self.positions()), ZERO)

    def simulate(
        self,
        symbol: str,
        market: str,
        delta: Decimal,
        price: Decimal,
        fx: Decimal,
        event_key: str,
        reason: str,
        evidence: dict[str, Any],
        plan: dict[str, Any],
        expected_revision: str | None = None,
    ) -> None:
        if (
            not all(value.is_finite() for value in (delta, price, fx))
            or not delta
            or price <= 0
            or fx <= 0
        ):
            raise SafetyError("Simulation arithmetic requires finite quantity and positive prices")
        at = now_utc()
        with self.transaction(write=True):
            if self.db.execute("SELECT 1 FROM trades WHERE event_key=?", (event_key,)).fetchone():
                return
            row = self.db.execute("SELECT data FROM positions WHERE symbol=?", (symbol,)).fetchone()
            previous = json.loads(row[0]) if row else {}
            if expected_revision is not None and previous.get("revision") != expected_revision:
                self.event(
                    "simulation_superseded",
                    {
                        "symbol": symbol,
                        "event_key": event_key,
                        "reason": "position_changed_during_review",
                    },
                )
                return
            old = decimal(previous.get("quantity", "0"))
            unit = decimal(
                previous.get(
                    "entry_unit_usd",
                    decimal(previous.get("entry_price", price)) / decimal(previous.get("fx", fx)),
                )
            )
            value = price / fx
            closing = min(abs(old), abs(delta)) if old * delta < 0 else ZERO
            pnl = closing * (value - unit) * (1 if old > 0 else -1)
            quantity = old + delta
            if old * delta >= 0 and quantity:
                entry = (abs(old) * unit + abs(delta) * value) / abs(quantity)
                entry_price = (
                    abs(old) * decimal(previous.get("entry_price", price)) + abs(delta) * price
                ) / abs(quantity)
            elif old * quantity > 0:
                entry = unit
                entry_price = decimal(previous["entry_price"])
            else:
                entry = value
                entry_price = price
            trade = {
                "symbol": symbol,
                "market": market,
                "side": "BUY" if delta > 0 else "SELL",
                "quantity": str(abs(delta)),
                "signed_quantity": str(delta),
                "closed_quantity": str(closing),
                "position_before": str(old),
                "position_after": str(quantity),
                "cash_flow_usd": str(-delta * value),
                "fill_price": str(price),
                "entry_price": previous.get("entry_price", str(entry_price))
                if closing
                else str(entry_price),
                "filled_at": at.isoformat(),
                "realized_pnl_usd": str(pnl),
                "reason": reason,
                "strategy_profile": "free-simulation-v3",
                "price_evidence": evidence,
                "simulation_assumptions": {
                    "unlimited_liquidity": True,
                    "fractional_shares": True,
                    "virtual_credit": True,
                    "fees_tax_interest_usd": "0",
                },
            }
            self.db.execute(
                "INSERT INTO cash(amount,available_at,source) VALUES (?,?,?)",
                (str(-delta * value), at.isoformat(), "simulation:" + event_key),
            )
            self.db.execute(
                "INSERT INTO trades(event_key,data) VALUES (?,?)", (event_key, encode(trade))
            )
            if quantity:
                position = {
                    **previous,
                    "symbol": symbol,
                    "market": market,
                    "quantity": str(quantity),
                    "direction": "LONG" if quantity > 0 else "SHORT",
                    "entry_price": str(entry_price),
                    "entry_unit_usd": str(entry),
                    "fx": str(fx),
                    "cost_usd": str(quantity * entry),
                    "mark_usd": str(quantity * value),
                    "marked_at": at.isoformat(),
                    "mark_quality": "simulation_reference",
                    "entered_at": previous.get("entered_at", at.isoformat())
                    if old * quantity > 0
                    else at.isoformat(),
                    "strategy_profile": "free-simulation-v3",
                    "rationale": reason,
                    "price_evidence": evidence,
                    "plan": plan,
                    "revision": event_key,
                }
                self.db.execute(
                    "INSERT OR REPLACE INTO positions VALUES (?,?)", (symbol, encode(position))
                )
            else:
                self.db.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            self.event("simulated_fill", trade)
            if closing:
                self.event(
                    "paper_trade_review",
                    {
                        "symbol": symbol,
                        "strategy_profile": "free-simulation-v3",
                        "closed_quantity": str(closing),
                        "net_pnl_usd": str(pnl),
                        "outcome": "profit" if pnl > 0 else "loss" if pnl < 0 else "flat",
                        "exit_reason": reason,
                        "ai_invoked": False,
                    },
                )

    def mark(self, symbol: str, value_usd: Decimal, at: datetime) -> None:
        if not value_usd.is_finite() or value_usd < 0:
            raise SafetyError("Invalid liquidation mark")
        with self.transaction(write=True):
            self._mark(symbol, value_usd, at)

    def _mark(self, symbol: str, value_usd: Decimal, at: datetime) -> None:
        row = self.db.execute("SELECT data FROM positions WHERE symbol=?", (symbol,)).fetchone()
        if row:
            position = json.loads(row[0])
            previous_mark = position["mark_usd"]
            position.update(
                mark_usd=str(value_usd), marked_at=at.isoformat(), mark_quality="executable_depth"
            )
            position["max_observed_mark_usd"] = str(
                max(value_usd, decimal(position.get("max_observed_mark_usd", previous_mark)))
            )
            position["min_observed_mark_usd"] = str(
                min(value_usd, decimal(position.get("min_observed_mark_usd", previous_mark)))
            )
            self.db.execute(
                "UPDATE positions SET data=? WHERE symbol=?", (encode(position), symbol)
            )

    def buy(self, position: dict[str, Any], cost: Decimal, event_key: str, at: datetime) -> None:
        if (
            not cost.is_finite()
            or cost <= 0
            or type(position["quantity"]) is not int
            or position["quantity"] <= 0
        ):
            raise SafetyError("Paper buys require positive cost and integer quantity")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.total_cash() < cost:
                raise SafetyError("Insufficient paper buying power")
            remaining = cost
            rows = self.db.execute("SELECT id,amount,available_at FROM cash ORDER BY id").fetchall()
            for row in rows:
                if remaining <= 0:
                    continue
                amount = decimal(row[1])
                used = min(amount, remaining)
                self.db.execute("UPDATE cash SET amount=? WHERE id=?", (str(amount - used), row[0]))
                remaining -= used
            trade = dict(position, side="BUY", cost_usd=str(cost), filled_at=at.isoformat())
            self.db.execute(
                "INSERT INTO trades(event_key,data) VALUES (?,?)", (event_key, encode(trade))
            )
            self.db.execute(
                "INSERT INTO positions VALUES (?,?)", (position["symbol"], encode(position))
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def sell(
        self,
        symbol: str,
        proceeds: Decimal,
        fill_price: Decimal,
        event_key: str,
        reason: str,
        at: datetime,
        sale_date: str,
    ) -> None:
        if (
            not proceeds.is_finite()
            or not fill_price.is_finite()
            or proceeds <= 0
            or fill_price <= 0
        ):
            raise SafetyError("Paper sale values must be positive")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT data FROM positions WHERE symbol=?", (symbol,)).fetchone()
            if not row:
                raise SafetyError("Cannot sell a paper position that is not owned")
            position = json.loads(row[0])
            trade = dict(
                position,
                side="SELL",
                filled_at=at.isoformat(),
                fill_price=str(fill_price),
                proceeds_usd=str(proceeds),
                reason=reason,
                realized_pnl_usd=str(proceeds - decimal(position["cost_usd"])),
            )
            self.db.execute(
                "INSERT INTO trades(event_key,data) VALUES (?,?)", (event_key, encode(trade))
            )
            self.db.execute(
                "INSERT INTO cash(amount,available_at,market,sale_date,source) VALUES (?,NULL,?,?,?)",
                (str(proceeds), position["market"], sale_date, event_key),
            )
            self.db.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            pnl = proceeds - decimal(position["cost_usd"])
            risk = decimal(position.get("planned_risk_usd", "0"))
            self.event(
                "paper_trade_review",
                {
                    "symbol": symbol,
                    "strategy_profile": position.get("strategy_profile", "v1"),
                    "entry_signal": position.get("signal_key"),
                    "exit_reason": reason,
                    "net_pnl_usd": str(pnl),
                    "risk_multiple": str(pnl / risk) if risk > 0 else None,
                    "outcome": "profit" if pnl > 0 else "loss" if pnl < 0 else "flat",
                    "max_observed_mark_usd": position.get("max_observed_mark_usd"),
                    "min_observed_mark_usd": position.get("min_observed_mark_usd"),
                    "observation_scope": "scheduled_checks_only",
                    "ai_invoked": False,
                },
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def reserve_ai(self, event_key: str, at: datetime) -> int | None:
        day = at.astimezone(SEOUL).date().isoformat()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            duplicate = self.db.execute(
                "SELECT 1 FROM ai_calls WHERE event_key=?",
                (event_key,),
            ).fetchone()
            if duplicate:
                self.db.execute("ROLLBACK")
                return None
            cursor = self.db.execute(
                "INSERT INTO ai_calls(event_key,day,status,data) VALUES (?,?,'reserved',?)",
                (event_key, day, encode({"owner": self.owner})),
            )
            self.db.execute("COMMIT")
            return int(cursor.lastrowid or 0)
        except Exception:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def finish_ai(
        self, call_id: int, status: str, usage: dict[str, Any] | None, detail: dict[str, Any]
    ) -> None:
        known = isinstance(usage, dict) and all(
            type(usage.get(key)) is int and usage[key] >= 0
            for key in ("input_tokens", "output_tokens")
        )
        if known and usage is not None:
            cached = usage.get("cached_input_tokens", 0)
            known = type(cached) is int and 0 <= cached <= usage["input_tokens"]
        valid = usage if known and usage is not None else {}
        with self.transaction(write=True):
            if not known:
                self.db.execute("INSERT OR REPLACE INTO meta VALUES ('ai_usage_unknown','1')")
            self.annotate_ai(call_id, detail)
            self.db.execute(
                "UPDATE ai_calls SET status=?,input_tokens=?,output_tokens=?,cached_tokens=?,usage_known=? WHERE id=?",
                (
                    status,
                    valid.get("input_tokens", 0),
                    valid.get("output_tokens", 0),
                    valid.get("cached_input_tokens", 0),
                    int(known),
                    call_id,
                ),
            )

    def ai_progress(self, call_id: int, **detail: Any) -> None:
        self.annotate_ai(call_id, detail)

    def annotate_ai(self, call_id: int, detail: dict[str, Any]) -> None:
        row = self.db.execute("SELECT data FROM ai_calls WHERE id=?", (call_id,)).fetchone()
        if row is None:
            raise SafetyError("Unknown AI call for usage annotation")
        data = json.loads(row[0])
        data.update(detail)
        self.db.execute("UPDATE ai_calls SET data=? WHERE id=?", (encode(data), call_id))

    def claim_signal(self, key: str, payload: dict[str, Any]) -> bool:
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO signals VALUES (?,?)", (key, encode(payload))
        )
        return cursor.rowcount == 1

    def claim_tick(self, at: datetime) -> bool:
        slot = at.replace(second=0, microsecond=0).isoformat()
        result = self.db.execute(
            "INSERT INTO ticks VALUES (?,?,'started') ON CONFLICT(slot) DO UPDATE SET "
            "at=excluded.at,status='started' WHERE ticks.status='failed'",
            (slot, at.isoformat()),
        )
        return result.rowcount == 1

    def finish_tick(self, at: datetime, status: str) -> None:
        slot = at.replace(second=0, microsecond=0).isoformat()
        self.db.execute("UPDATE ticks SET status=? WHERE slot=?", (status, slot))

    def heartbeat(self, role: str, owner: str, phase: str, **data: Any) -> None:
        data["process_identity"] = self.process_identity
        self.db.execute(
            "INSERT INTO workers VALUES (?,?,?,?,?,?) ON CONFLICT(role) DO UPDATE SET "
            "pid=excluded.pid,owner=excluded.owner,phase=excluded.phase,seen_at=excluded.seen_at,data=excluded.data",
            (role, os.getpid(), owner, phase, now_utc().isoformat(), encode(data)),
        )

    def enqueue(self, candidate: dict[str, Any]) -> bool:
        key = candidate["signal_key"]
        with self.transaction(write=True):
            self.db.execute(
                "UPDATE jobs SET status='superseded' WHERE symbol=? AND market=? AND status='queued' AND observed_at<?",
                (candidate["symbol"], candidate["market"], candidate["observed_at"]),
            )
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO jobs(id,symbol,market,status,observed_at,data) VALUES (?,?,?,'queued',?,?)",
                (
                    key,
                    candidate["symbol"],
                    candidate["market"],
                    candidate["observed_at"],
                    encode(candidate),
                ),
            )
        return cursor.rowcount == 1

    def lease_jobs(self, owner: str) -> list[dict[str, Any]]:
        import time

        with self.transaction(write=True):
            rows = self.db.execute(
                "SELECT id,data,observed_at FROM jobs WHERE status='queued' AND retry_at<=? ORDER BY observed_at",
                (time.time(),),
            ).fetchall()
            result = []
            for row in rows:
                if (now_utc() - stamp(row[2])).total_seconds() > 300:
                    self.db.execute("UPDATE jobs SET status='expired' WHERE id=?", (row[0],))
                    continue
                self.db.execute(
                    "UPDATE jobs SET status='reviewing',owner=?,attempt=attempt+1 WHERE id=?",
                    (owner, row[0]),
                )
                result.append(json.loads(row[1]))
            return result

    def finish_job(self, key: str, status: str, result: dict[str, Any]) -> None:
        self.db.execute(
            "UPDATE jobs SET status=?,result=?,owner=NULL WHERE id=?", (status, encode(result), key)
        )

    def backup(self) -> Path:
        directory = self.path.parent / "backups"
        directory.mkdir(exist_ok=True, mode=0o700)
        path = directory / (
            "ledger-"
            + now_utc().strftime("%Y%m%dT%H%M%S")
            + "-"
            + uuid.uuid4().hex[:8]
            + ".sqlite3"
        )
        destination = sqlite3.connect(path)
        try:
            self.db.backup(destination)
            if destination.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise SafetyError("Ledger backup failed integrity verification")
        finally:
            destination.close()
        path.chmod(0o600)
        return path

    def apply_action(self, action: dict[str, Any]) -> None:
        required = {"event_key", "symbol", "kind", "effective_date", "source"}
        if (
            not required <= action.keys()
            or not isinstance(action["source"], str)
            or not action["source"].startswith("https://")
        ):
            raise SafetyError("Corporate actions require an event key and verified HTTPS evidence")
        if action["kind"] not in ("split", "dividend", "settlement"):
            raise SafetyError("Unsupported corporate action")
        if action["effective_date"] > now_utc().astimezone(SEOUL).date().isoformat():
            raise SafetyError("Future corporate actions cannot be applied")
        with self.transaction(write=True):
            if self.db.execute(
                "SELECT 1 FROM corporate_actions WHERE event_key=?", (action["event_key"],)
            ).fetchone():
                return
            row = self.db.execute(
                "SELECT data FROM positions WHERE symbol=?", (action["symbol"],)
            ).fetchone()
            if not row:
                raise SafetyError("Corporate action requires an owned position")
            position = json.loads(row[0])
            if action["kind"] == "split":
                if decimal(action["old_shares"]) <= 0:
                    raise SafetyError("Split denominator must be positive")
                ratio = decimal(action["new_shares"]) / decimal(action["old_shares"])
                if ratio <= 0 or decimal(action["old_shares"]) <= 0:
                    raise SafetyError("Split quantities must be positive")
                quantity = decimal(position["quantity"]) * ratio
                position["quantity"] = (
                    int(quantity) if quantity == quantity.to_integral_value() else str(quantity)
                )
                for field in (
                    "entry_price",
                    "stop",
                    "target",
                    "risk_per_share",
                    "risk_distance",
                    "atr",
                    "breakout",
                    "support",
                ):
                    if field in position:
                        position[field] = str(decimal(position[field]) / ratio)
                anchor = position.get("action_anchor")
                if not anchor or decimal(action["adjustment_factor"]) <= 0:
                    raise SafetyError(
                        "Split requires a verified adjustment factor and reference anchor"
                    )
                anchor["factor"] = str(decimal(action["adjustment_factor"]))
            else:
                amount = decimal(action["amount_usd"])
                available = stamp(action["available_at"])
                if amount < 0:
                    raise SafetyError("Verified corporate proceeds cannot be negative")
                self.db.execute(
                    "INSERT INTO cash(amount,available_at,source) VALUES (?,?,?)",
                    (str(amount), available.isoformat(), "action:" + action["event_key"]),
                )
                if action["kind"] == "dividend" and action.get("adjustment_factor"):
                    factor = decimal(action["adjustment_factor"])
                    if factor <= 0 or not position.get("action_anchor"):
                        raise SafetyError("Dividend adjustment requires a reference anchor")
                    position["action_anchor"]["factor"] = str(factor)
                if action["kind"] == "settlement":
                    self.db.execute("DELETE FROM positions WHERE symbol=?", (action["symbol"],))
                    self.db.execute(
                        "INSERT INTO trades(event_key,data) VALUES (?,?)",
                        (
                            "action:" + action["event_key"],
                            encode(
                                dict(
                                    position,
                                    side="SETTLEMENT",
                                    proceeds_usd=str(amount),
                                    realized_pnl_usd=str(amount - decimal(position["cost_usd"])),
                                    source=action["source"],
                                )
                            ),
                        ),
                    )
            if action["kind"] != "settlement":
                self.db.execute(
                    "UPDATE positions SET data=? WHERE symbol=?",
                    (encode(position), action["symbol"]),
                )
            self.db.execute(
                "INSERT INTO corporate_actions VALUES (?,?,?,?,?,?)",
                tuple(
                    action[key]
                    for key in ("event_key", "symbol", "kind", "effective_date", "source")
                )
                + (encode(action),),
            )
            self.event("corporate_action_applied", action)

    def summary(self, at: datetime) -> dict[str, Any]:
        with self.transaction():
            return self._summary(at)

    def _summary(self, at: datetime) -> dict[str, Any]:
        simulation = self.db.execute(
            "SELECT value FROM meta WHERE key='simulation_mode'"
        ).fetchone()
        free = bool(simulation and simulation[0] == "unrestricted")
        realized = ZERO
        trades = []
        for row in self.db.execute("SELECT data FROM trades ORDER BY id DESC LIMIT 50"):
            trades.append(json.loads(row[0]))
        for row in self.db.execute("SELECT data FROM trades"):
            trade = json.loads(row[0])
            realized += decimal(trade.get("realized_pnl_usd", "0"))
        return {
            "mode": "paper_only",
            "initial_usd": "1000",
            "equity_usd": str(self.equity()),
            "buying_power_usd": str(self.total_cash()),
            "buying_power_basis": "unlimited_virtual_credit"
            if free
            else "total_unspent_cash_including_unsettled_proceeds",
            "virtual_debt_usd": str(max(-self.total_cash(), ZERO)) if free else "0",
            "simulation_assumptions": {
                "market_rules_enforced": not free,
                "new_trade_costs_zero": free,
                "price_basis": "explicit_reference_fill" if free else "executable_book",
            },
            "settled_cash_usd": str(self.settled_cash(at)),
            "unsettled_cash_usd": str(self.total_cash() - self.settled_cash(at)),
            "realized_pnl_usd": str(realized),
            "positions": [
                {
                    **position,
                    "mark_quality": position.get("mark_quality", "unverified")
                    if position.get("marked_at")
                    and at - stamp(position["marked_at"]) <= timedelta(seconds=120)
                    else "stale",
                }
                for position in self.positions()
            ],
            "trades": trades,
            "ai_usage_unknown": bool(
                self.db.execute(
                    "SELECT 1 FROM ai_calls WHERE usage_known IS NULL OR usage_known!=1",
                ).fetchone()
            ),
            "ai_blocked": bool(
                self.db.execute(
                    "SELECT 1 FROM meta WHERE key='ai_blocked'",
                ).fetchone()
            ),
            "ai_calls": [
                {
                    **{
                        key: value
                        for key, value in dict(row).items()
                        if key
                        not in (
                            "data",
                            "usage_known",
                            "input_tokens",
                            "output_tokens",
                            "cached_tokens",
                        )
                    },
                    **{
                        key: row[key] if row["usage_known"] == 1 else None
                        for key in ("input_tokens", "output_tokens", "cached_tokens")
                    },
                    "usage_known": row["usage_known"] == 1,
                    "details": json.loads(row["data"]),
                }
                for row in self.db.execute(
                    "SELECT id,event_key,day,status,input_tokens,output_tokens,cached_tokens,data,usage_known "
                    "FROM ai_calls",
                )
            ],
        }
