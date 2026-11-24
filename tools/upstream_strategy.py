"""Run the pinned day-trader-skill decision modules without the broker module."""

from __future__ import annotations

import datetime as dt
import hashlib
import subprocess
import time
from argparse import Namespace
from heapq import merge
from pathlib import Path
from types import ModuleType
from typing import Any

from papertrade.jsonio import read_json

REVISION = "3d0f4ef6986f4984b7c6bd46f7ff4f87744c3abd"
RESEARCH_REVISION = "9fa1196fffbcfd9de5146d2dd2869df84ab99838"
MODULES = {
    "scanner": "trader-scanner/scan.py",
    "entry": "trader-entry/entry.py",
    "exit": "trader-exit/exit.py",
    "budget": "trader-budget/size.py",
    "risk": "trader-risk/check.py",
    "alerts": "trader-alerts/alerts.py",
}
RANGE_MINUTES = 15
STEP = 300


def epoch(stamp: str) -> float:
    value = dt.datetime.fromisoformat(stamp)
    if value.utcoffset() is None:
        raise ValueError("Observation timestamp has no timezone")
    return value.timestamp()


def iso(at: float) -> str:
    return dt.datetime.fromtimestamp(at, dt.UTC).isoformat()


def utc(at: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(at, dt.UTC).replace(tzinfo=None)


class Upstream:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        if self._git("rev-parse", "HEAD").decode().strip() != REVISION:
            raise ValueError("Day trader checkout does not match its pinned revision")
        self.hashes: dict[str, str] = {}
        self.scanner = self._module("scanner")
        self.entry = self._module("entry")
        self.exit = self._module("exit")
        self.budget = self._module("budget")
        self.risk = self._module("risk")
        self.alerts = self._module("alerts")
        self.example = self.root / "trader-budget/config.example.json"
        self._verified("trader-budget/config.example.json")

    def _git(self, *args: str) -> bytes:
        return subprocess.run(
            ["git", "-C", str(self.root), *args],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        ).stdout

    def _verified(self, relative: str) -> bytes:
        data = (self.root / relative).read_bytes()
        if data != self._git("show", f"{REVISION}:{relative}"):
            raise ValueError(f"Pinned day trader file changed: {relative}")
        self.hashes[relative] = hashlib.sha256(data).hexdigest()
        return data

    def _module(self, name: str) -> ModuleType:
        relative = MODULES[name]
        source = self._verified(relative)
        module = ModuleType(f"pinned_day_trader_{name}")
        module.__file__ = str(self.root / relative)
        exec(compile(source, module.__file__, "exec"), module.__dict__)
        return module

    def config(self, path: Path | None, capital: float) -> dict[str, Any]:
        example = read_json(self.example)
        custom = read_json(path) if path else {}
        allowed = example.keys() - {"broker", "sec_user_agent"}
        cfg = {name: custom.get(name, example[name]) for name in allowed}
        if cfg["paper"] is not True or any(
            cfg[name] is not False
            for name in ("fractional", "allow_options", "allow_margin", "allow_short")
        ):
            raise ValueError("The paper session requires whole-share, cash-only long settings")
        cfg["budget"] = capital
        return cfg

    def cutoff(self, day: dt.date, config: dict[str, Any]) -> float:
        close = dt.datetime.combine(day, self.entry.session(day)[1])
        cutoff = self.entry.early_cutoff(
            day, self.entry.parse_hhmm(config["no_entries_after"], "no_entries_after"), close
        )
        return self.entry.eastern_to_utc(cutoff).replace(tzinfo=dt.UTC).timestamp()

    def capture(
        self,
        symbols: list[str],
        day: dt.date,
        cutoff: float,
        until: float,
        *,
        final_entry_window: bool = False,
    ) -> dict[str, Any]:
        scan = self.scanner
        stage_one: dict[str, Any] = {}
        failed: dict[str, Any] = {}
        spark_entry_window_expired: list[str] = []
        for offset in range(0, len(symbols), scan.SPARK_MAX):
            if offset and time.time() < until:
                time.sleep(scan.PAUSE)
            if time.time() >= until:
                if final_entry_window:
                    spark_entry_window_expired = sorted(symbols[offset:])
                else:
                    failed.update({symbol: "scan_window_expired" for symbol in symbols[offset:]})
                break
            data, errors = scan.fetch_spark(
                symbols[offset : offset + scan.SPARK_MAX], "1d", "5m", scan.PAUSE
            )
            stage_one.update(data)
            failed.update(errors)
        confirmed: dict[str, Any] = {}
        confirmation_entry_window_expired: set[str] = set()
        eastern = scan.to_eastern(utc(cutoff))

        def confirm(symbol: str) -> dict[str, Any]:
            if time.time() < until:
                time.sleep(scan.PAUSE)
            if time.time() >= until:
                if final_entry_window:
                    confirmation_entry_window_expired.add(symbol)
                raise scan.ScanError("Scan exceeded its decision window")
            try:
                raw = scan.yahoo_get(scan.chart_path(symbol, "1d", "5m"))
                series = scan.parse_chart(raw, symbol)
                chart = self.entry.parse_chart(raw, symbol)
                if chart["symbol"] != self.entry.yahoo_symbol(symbol):
                    raise scan.ScanError("Chart symbol mismatch")
                granularity = chart["meta"].get("dataGranularity")
                if granularity and granularity != "5m":
                    raise scan.ScanError("Chart granularity mismatch")
            except (scan.ScanError, self.entry.EntryError, ValueError, KeyError, TypeError):
                raise scan.ScanError("Yahoo confirmation unavailable") from None
            chart = {key: chart[key] for key in ("bars", "prev_close", "data_time")}
            chart["symbol"] = symbol
            confirmed[symbol] = {"series": series, "chart": chart, "received": time.time()}
            return series

        hits, counts, confirmation_fail = scan.scan_orb(
            stage_one, symbols, day, eastern, RANGE_MINUTES, 5.0, 0, confirm, scan.MAX_CONFIRM
        )
        result = {
            "cutoff": cutoff,
            "stage_one": stage_one,
            "confirmed": confirmed,
            "hits": hits,
            "counts": counts,
            "spark_failures": sorted(failed),
            "confirmation_failures": sorted(
                confirmation_fail.keys() - confirmation_entry_window_expired
            ),
        }
        if spark_entry_window_expired:
            result["spark_entry_window_expired"] = spark_entry_window_expired
        if confirmation_entry_window_expired:
            result["confirmation_entry_window_expired"] = sorted(confirmation_entry_window_expired)
        return result

    def evaluate_scan(
        self, scan: dict[str, Any], symbols: list[str], day: dt.date, config: dict[str, Any]
    ) -> list[tuple[dict[str, Any], dict[str, Any], float]]:
        source = self.scanner

        def confirmation(symbol: str) -> dict[str, Any]:
            if symbol not in scan["confirmed"]:
                raise source.ScanError("Yahoo confirmation unavailable")
            return scan["confirmed"][symbol]["series"]

        hits, counts, fails = source.scan_orb(
            scan["stage_one"],
            symbols,
            day,
            source.to_eastern(utc(scan["cutoff"])),
            RANGE_MINUTES,
            5.0,
            0,
            confirmation,
            source.MAX_CONFIRM,
        )
        recorded_failures = scan["confirmation_failures"]
        if expired := scan.get("confirmation_entry_window_expired"):
            recorded_failures = list(merge(recorded_failures, expired))
        if (hits, counts, sorted(fails)) != (
            scan["hits"],
            scan["counts"],
            recorded_failures,
        ):
            raise ValueError("Recorded scan does not reproduce with pinned source")
        entries = []
        for hit in hits:
            source_chart = scan["confirmed"][hit["symbol"]]
            seen = source_chart["received"]
            # The scan received its own chart after the fixed cut. Do not let that time
            # expose a further bar while reproducing the fixed upstream scan boundary.
            chart = {**source_chart["chart"]}
            chart["bars"] = [bar for bar in chart["bars"] if bar["t"] + STEP <= scan["cutoff"]]
            result = self.entry.evaluate(chart, utc(seen), cfg=config)
            if result["status"] != "triggered":
                continue
            bar = result["trigger"]["bar_index"]
            close = source.slot(day, bar + 1)
            trigger = source.eastern_to_utc(close).replace(tzinfo=dt.UTC).timestamp()
            entries.append((hit, self.entry.entry_doc(result, utc(seen)), trigger))
        return entries

    def plan(self, entry: dict[str, Any], at: float, config_file: Path) -> dict[str, Any]:
        orr = entry["opening_range"]
        args = Namespace(
            symbol=entry["symbol"],
            from_path=None,
            side="buy",
            entry=entry["price"],
            setup=entry["setup"],
            date=entry["date"],
            time=entry["time"],
            entry_bar=entry["bar_index"],
            interval=entry["interval"],
            or_high=orr["high"],
            or_low=orr["low"],
            or_minutes=orr["minutes"],
            stop=None,
            target=None,
            flat_by=None,
            config=str(config_file),
        )
        return self.exit.build_plan(args, utc(at))[0]

    def exit_reason(self, plan: dict[str, Any], price: float | None, at: float) -> str | None:
        rules = self.alerts.rules_from_plan(plan)
        # build_plan has already resolved the exchange's early-close offset.
        rules[-1]["before_close"] = False
        symbol = plan["symbol"]
        quotes = {symbol: {"price": price}} if price is not None else {}
        now = utc(at)
        rings, _ = self.alerts.evaluate(rules, quotes, self.alerts.market_status(now), {}, now)
        if rings:
            return rings[0]["id"].removeprefix(symbol.lower() + "-")
        return None
