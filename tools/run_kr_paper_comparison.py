"""Compare fixed rules and Astra-gated rules on shared Korean market snapshots."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from papertrade.api import SYMBOL, ApiError
from papertrade.core import SafetyError, Settings, encode
from tools.autotest_jobs import atomic_json
from tools.kr_comparison_ai import decide
from tools.live_feed import LiveFeed
from tools.probe_toss_websocket import CachedTokenClient

ROOT = Path(__file__).resolve().parents[1]
CAPITAL = Decimal(100_000)


def now() -> datetime:
    return datetime.now(UTC)


class Account:
    def __init__(self) -> None:
        self.cash = CAPITAL
        self.positions: dict[str, tuple[Decimal, Decimal]] = {}
        self.realized = Decimal(0)
        self.fills = 0
        self.pending: dict[str, tuple[dict[str, Any], datetime, str]] = {}

    def wants(self, signal: dict[str, Any]) -> bool:
        return (signal["symbol"] in self.positions) == (signal["action"] == "SELL")

    def execute(
        self,
        quotes: dict[str, tuple[Decimal, datetime]],
        observed: datetime,
        latest: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ready = []
        for symbol, (signal, decided, reason) in list(self.pending.items()):
            if latest.get(symbol) != signal or not self.wants(signal):
                del self.pending[symbol]
                continue
            if symbol in quotes and quotes[symbol][1] > decided:
                ready.append((symbol, signal, decided, reason))
                del self.pending[symbol]

        buy_count = sum(signal["action"] == "BUY" for _, signal, _, _ in ready)
        records = []
        budget = Decimal(0)
        for symbol, signal, decided, reason in sorted(
            ready, key=lambda row: row[1]["action"] == "BUY"
        ):
            price, source = quotes[symbol]
            if signal["action"] == "SELL":
                quantity, basis = self.positions.pop(symbol)
                proceeds = quantity * price
                self.cash += proceeds
                self.realized += proceeds - basis
            else:
                if not budget:
                    budget = self.cash / buy_count
                cost = min(budget, self.cash)
                if cost <= 0:
                    continue
                quantity = cost / price
                self.cash -= cost
                self.positions[symbol] = quantity, cost
            self.fills += 1
            records.append(
                {
                    "symbol": symbol,
                    "action": signal["action"],
                    "quantity": quantity,
                    "assumed_price_krw": price,
                    "source_at": source,
                    "observed_at": observed,
                    "fill_at": observed,
                    "decision_at": decided,
                    "signal": signal,
                    "reason": reason,
                }
            )
        return records

    def summary(self, prices: dict[str, tuple[Decimal, datetime]]) -> dict[str, Any]:
        positions = {}
        unrealized = Decimal(0)
        for symbol, (quantity, basis) in self.positions.items():
            price, source = prices[symbol]
            value = quantity * price
            unrealized += value - basis
            positions[symbol] = {
                "quantity": quantity,
                "cost_krw": basis,
                "mark_krw": price,
                "market_value_krw": value,
                "source_at": source,
            }
        return {
            "cash_krw": self.cash,
            "equity_krw": CAPITAL + self.realized + unrealized,
            "realized_krw": self.realized,
            "unrealized_krw": unrealized,
            "positions": positions,
            "fills": self.fills,
            "pending": sorted(self.pending),
        }


class Comparison:
    def __init__(self) -> None:
        self.accounts = {"fixed": Account(), "ai": Account()}
        self.prices: dict[str, tuple[Decimal, datetime]] = {}
        self.latest: dict[str, dict[str, Any]] = {}
        self.ai_latest: dict[str, dict[str, Any]] = {}
        self.review_queue: dict[str, dict[str, Any]] = {}
        self.watches: dict[str, tuple[Decimal, datetime]] = {}

    def observe(self, rows: list[dict[str, Any]], observed: datetime) -> list[dict[str, Any]]:
        quotes = {}
        signals = []
        for row in rows:
            if row.get("currency") != "KRW":
                continue
            try:
                symbol = row["symbol"]
                price = Decimal(str(row["lastPrice"]))
                source = datetime.fromisoformat(row["timestamp"])
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue
            if not price.is_finite() or price <= 0 or source.tzinfo is None or source > observed:
                continue
            prior = self.prices.get(symbol)
            if prior is not None and source <= prior[1]:
                continue
            quotes[symbol] = self.prices[symbol] = (price, source)
            if prior is None or prior[0] == price:
                continue
            action = "BUY" if price > prior[0] else "SELL"
            same_direction = self.latest.get(symbol, {}).get("action") == action
            watch = self.watches.get(symbol)
            if same_direction:
                if watch is None or source <= watch[1]:
                    continue
                level = watch[0]
                if not (prior[0] < level <= price or price <= level < prior[0]):
                    continue
            self.watches.pop(symbol, None)
            signal = {
                "symbol": symbol,
                "action": action,
                "trigger": "watch_crossing" if same_direction else "direction_change",
                "price_krw": str(price),
                "previous_price_krw": str(prior[0]),
                "source_at": source.isoformat(),
                "observed_at": observed.isoformat(),
            }
            if not same_direction:
                self.latest[symbol] = signal
            self.ai_latest[symbol] = signal
            signals.append(signal)
        fills: list[dict[str, Any]] = []
        for name, account in self.accounts.items():
            fills.extend(
                {"account": name, **row}
                for row in account.execute(
                    quotes, observed, self.ai_latest if name == "ai" else self.latest
                )
            )
        for signal in signals:
            symbol = signal["symbol"]
            fixed, ai = self.accounts["fixed"], self.accounts["ai"]
            if signal["trigger"] == "direction_change" and fixed.wants(signal):
                fixed.pending[symbol] = signal, observed, "fixed_direction_change"
            if ai.wants(signal):
                self.review_queue[symbol] = signal
            else:
                self.review_queue.pop(symbol, None)
        return fills

    def approve(
        self,
        signals: list[dict[str, Any]],
        approvals: dict[str, str],
        decided: datetime,
        watches: dict[str, Decimal] | None = None,
    ) -> None:
        ai = self.accounts["ai"]
        for signal in signals:
            symbol = signal["symbol"]
            if self.ai_latest.get(symbol) != signal or not ai.wants(signal):
                continue
            if symbol in approvals:
                ai.pending[symbol] = signal, decided, approvals[symbol]
            elif watches and symbol in watches:
                self.watches[symbol] = watches[symbol], decided


async def campaign(output: Path, until: datetime) -> None:
    if now() >= until:
        raise ValueError("Observation deadline has passed")
    output.mkdir(parents=True, exist_ok=False)
    settings = Settings.load(ROOT)
    client = CachedTokenClient(settings)
    comparison = Comparison()
    digest = hashlib.sha256()
    started = now()
    sweeps = calls = approvals = reviews_completed = 0
    symbols: set[str] = set()
    review: asyncio.Task[tuple[datetime, dict[str, str], dict[str, Decimal]]] | None = None
    reviewing: list[dict[str, Any]] = []
    state, error = "running", None

    async def review_signals(
        payload: dict[str, Any],
    ) -> tuple[datetime, dict[str, str], dict[str, Decimal]]:
        approved, watches = await decide(settings, payload)
        return now(), approved, watches

    def status() -> None:
        report = {
            "state": state,
            "complete": state == "complete",
            "error_type": error,
            "started_at": started.isoformat(),
            "until": until.isoformat(),
            "updated_at": now().isoformat(),
            "currency": "KRW",
            "initial_capital_krw": str(CAPITAL),
            "real_orders": False,
            "ai_calls": calls,
            "ai_approvals": approvals,
            "ai_reviews_completed": reviews_completed,
            "ai_in_flight": review is not None,
            "watch_prices_krw": {key: value[0] for key, value in comparison.watches.items()},
            "sweeps": sweeps,
            "listed_symbols": len(symbols),
            "observed_symbols": len(comparison.prices),
            "shared_input_sha256": digest.hexdigest(),
            "accounts": {
                name: account.summary(comparison.prices)
                for name, account in comparison.accounts.items()
            },
        }
        atomic_json(output / "status.json", json.loads(encode(report)))

    with (
        (output / "market.jsonl").open("x", buffering=1) as raw,
        (output / "events.jsonl").open("x", buffering=1) as events,
    ):

        def record(kind: str, data: dict[str, Any]) -> None:
            events.write(encode({"kind": kind, "recorded_at": now(), **data}) + "\n")

        feed = LiveFeed(
            settings, lambda k, data: record(k, data) if k == "http" else None, client=client
        )
        try:
            status()
            client.access_token()
            for exchange in ("KOSPI", "KOSDAQ", "KR_ETC"):
                listed = await asyncio.to_thread(client.listed, exchange)
                for row in listed:
                    symbol = row["symbol"]
                    if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
                        raise ValueError("Invalid Korean market symbol")
                    symbols.add(symbol)
            if not symbols:
                raise ValueError("No listed Korean market symbols")
            feed.symbols = sorted(symbols)
            source_files = (Path(__file__).resolve(), ROOT / "tools/kr_comparison_ai.py")
            atomic_json(
                output / "manifest.json",
                {
                    "currency": "KRW",
                    "initial_capital_krw": str(CAPITAL),
                    "until": until.isoformat(),
                    "model": "gpt-6-astra",
                    "effort": "medium",
                    "universe": feed.symbols,
                    "rules": "Directional changes propose entry or exit; Astra gates the AI account",
                    "fill": "Zero-cost fractional paper fill at a newer last trade after decision",
                    "sources": {
                        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files
                    },
                },
            )
            while now() < until:
                tick = time.monotonic()
                try:
                    rows = await asyncio.to_thread(feed.snapshot)
                except ApiError as exc:
                    if (
                        (exc.status != 429 and exc.code != "cooldown")
                        or exc.retry_at is None
                        or not math.isfinite(exc.retry_at)
                    ):
                        raise
                    delay = max(0, min(exc.retry_at - time.time(), (until - now()).total_seconds()))
                    record("market_cooldown", {"retry_at": exc.retry_at, "status": exc.status})
                    status()
                    await asyncio.sleep(delay)
                    continue
                observed = now()
                if observed >= until:
                    break
                if review is not None and review.done():
                    try:
                        decided, accepted, watches = review.result()
                    except SafetyError as exc:
                        record("ai_review_failed", {"reason": str(exc)})
                    else:
                        approvals += len(accepted)
                        reviews_completed += 1
                        comparison.approve(reviewing, accepted, decided, watches)
                        record(
                            "ai_decision",
                            {"at": decided, "approvals": accepted, "watches": watches},
                        )
                    review = None
                line = encode({"observed_at": observed, "rows": rows}) + "\n"
                raw.write(line)
                digest.update(line.encode())
                sweeps += 1
                for fill in comparison.observe(rows, observed):
                    record("fill", fill)
                if review is None:
                    ai = comparison.accounts["ai"]
                    reviewing = [
                        signal
                        for signal in comparison.review_queue.values()
                        if ai.wants(signal) and (signal["action"] == "SELL" or ai.cash > 0)
                    ]
                    comparison.review_queue.clear()
                if review is None and reviewing and now() < until:
                    payload = {
                        "signals": reviewing,
                        "currency": "KRW",
                        "observed_at": observed.isoformat(),
                        "portfolio": json.loads(
                            encode(comparison.accounts["ai"].summary(comparison.prices))
                        ),
                    }
                    record("ai_review", {"signals": reviewing, "observed_at": observed})
                    review = asyncio.create_task(review_signals(payload))
                    calls += 1
                status()
                await asyncio.sleep(
                    max(0, min(60 - (time.monotonic() - tick), (until - now()).total_seconds()))
                )
            if review is not None and review.done():
                try:
                    decided, accepted, watches = review.result()
                except SafetyError as exc:
                    record("ai_review_failed", {"reason": str(exc)})
                else:
                    if decided < until:
                        approvals += len(accepted)
                        reviews_completed += 1
                    record(
                        "ai_decision",
                        {
                            "at": decided,
                            "approvals": accepted,
                            "watches": watches,
                            "after_last_observation": True,
                        },
                    )
            state = "complete" if sweeps >= 2 and reviews_completed else "incomplete"
        except (ApiError, OSError, SafetyError, ValueError, RuntimeError, KeyError) as exc:
            state, error = "failed", type(exc).__name__
            record("failed", {"error_type": error})
            raise
        finally:
            if state == "running":
                state = "incomplete"
            if review is not None and not review.done():
                review.cancel()
                await asyncio.gather(review, return_exceptions=True)
                record("ai_review_ended_at_cutoff", {"at": now()})
            review = None
            status()
            atomic_json(
                output / "comparison.json", json.loads((output / "status.json").read_text())
            )


def main() -> None:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--until", required=True, type=datetime.fromisoformat)
    args = parser.parse_args()
    if args.until.tzinfo is None:
        parser.error("--until requires a timezone offset")
    asyncio.run(campaign(args.output.resolve(), args.until))


if __name__ == "__main__":
    main()
