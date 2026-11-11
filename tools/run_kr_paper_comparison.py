"""Compare fixed rules and Astra-gated rules on shared Korean market snapshots."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from papertrade.api import SYMBOL, ApiError
from papertrade.clock import now_utc as now
from papertrade.core import SafetyError, Settings
from papertrade.jsonio import atomic_json
from papertrade.jsonio import paper_json as encode
from tools.kr_comparison_account import CAPITAL, Account
from tools.kr_comparison_ai import SOL, decide, review_cascade
from tools.live_feed import LiveFeed
from tools.paper_market import KR
from tools.probe_toss_websocket import CachedTokenClient

ROOT = Path(__file__).resolve().parents[1]
ALLOCATION_LOOKBACK = 12




class Comparison:
    def __init__(
        self,
        allocation: Decimal | None = None,
        *,
        profile: str = "legacy",
        classification: dict[str, tuple[str, str]] | None = None,
        session_window: tuple[datetime, datetime] | None = None,
    ) -> None:
        if profile not in ("legacy", "broker-constrained"):
            raise ValueError("Unknown KR execution profile")
        self.profile = profile
        self.session_window = session_window
        self.classification = classification or {}
        self.price_history: defaultdict[str, deque[dict[str, str]]] = defaultdict(
            lambda: deque(maxlen=ALLOCATION_LOOKBACK + 2)
        )
        self.allocation = allocation
        self.accounts = {"fixed": Account(allocation), "ai": Account(allocation)}
        if profile == "broker-constrained":
            from tools.kr_whole_share_accounts import WholeShareAccount
            from tools.replay_kr_paper_costs import Ledger

            if allocation is None or session_window is None:
                raise ValueError("Constrained comparison requires allocation and session evidence")
            self.accounts = {
                name: WholeShareAccount(account, Ledger(), self.classification, {})
                for name, account in self.accounts.items()
            }
        self.moves: defaultdict[str, deque[Decimal]] = defaultdict(
            lambda: deque(maxlen=ALLOCATION_LOOKBACK)
        )
        self.prices: dict[str, tuple[Decimal, datetime]] = {}
        self.latest: dict[str, dict[str, Any]] = {}
        self.ai_latest: dict[str, dict[str, Any]] = {}
        self.review_queue: dict[str, dict[str, Any]] = {}
        self.watches: dict[str, tuple[Decimal, datetime]] = {}

    def observe(
        self, rows: list[dict[str, Any]], observed: datetime, *, warmup: bool = False
    ) -> list[dict[str, Any]]:
        quotes = {}
        signals = []
        if self.profile == "broker-constrained":
            assert self.session_window is not None
            if not self.session_window[0] <= observed < self.session_window[1]:
                self.review_queue.clear()
                for account in self.accounts.values():
                    account.pending.clear()
                    account.budgets.clear()
                return []
        for row in rows:
            if row.get("currency") != KR.currency:
                continue
            try:
                symbol = row["symbol"]
                price = Decimal(str(row["lastPrice"]))
                source = datetime.fromisoformat(row["timestamp"])
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue
            if not price.is_finite() or price <= 0 or source.tzinfo is None or source > observed:
                continue
            if self.profile == "broker-constrained":
                assert self.session_window is not None
                if (
                    symbol not in self.classification
                    or not self.session_window[0] <= source < self.session_window[1]
                    or (observed - source).total_seconds() > 90
                ):
                    continue
            prior = self.prices.get(symbol)
            if prior is not None and source <= prior[1]:
                continue
            quotes[symbol] = self.prices[symbol] = (price, source)
            if self.profile == "broker-constrained":
                self.price_history[symbol].append(
                    {
                        "price_krw": str(price),
                        "source_at": source.isoformat(),
                        "observed_at": observed.isoformat(),
                    }
                )
            if prior is None:
                continue
            strength = Decimal(0)
            if self.allocation is not None:
                history = self.moves[symbol]
                change = abs(price / prior[0] - 1)
                mean = sum(history, Decimal(0)) / len(history) if history else Decimal(0)
                if mean:
                    strength = change / mean
                history.append(change)
            if warmup or prior[0] == price:
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
            if self.allocation is not None:
                signal["strength"] = strength
            if self.profile == "broker-constrained":
                signal.update(
                    price_history=list(self.price_history[symbol]),
                    execution_profile=self.profile,
                    eligibility="verified_ordinary_common_stock",
                    session="regular_continuous",
                    strategy="direction_change_baseline",
                )
                if watch is not None and same_direction:
                    signal["watch_price_krw"] = str(watch[0])
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
                fixed.queue(signal, observed, "fixed_direction_change", self.prices)
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
        if self.profile == "broker-constrained":
            assert self.session_window is not None
            if not self.session_window[0] <= decided < self.session_window[1]:
                return
        ai = self.accounts["ai"]
        for signal in signals:
            symbol = signal["symbol"]
            if self.ai_latest.get(symbol) != signal or not ai.wants(signal):
                continue
            if symbol in approvals:
                ai.queue(signal, decided, approvals[symbol], self.prices)
            elif watches and symbol in watches:
                self.watches[symbol] = watches[symbol], decided


async def campaign(
    output: Path,
    until: datetime,
    *,
    allocation_comparison: bool = False,
    profile: str = "legacy",
) -> None:
    if profile not in ("legacy", "broker-constrained"):
        raise ValueError("Unknown KR execution profile")
    if profile == "broker-constrained":
        allocation_comparison = True
    if now() >= until:
        raise ValueError("Observation deadline has passed")
    output.mkdir(parents=True, exist_ok=False)
    settings = Settings.load(ROOT)
    client = CachedTokenClient(settings)
    comparison = Comparison()
    pairs = (
        {"5pct": Comparison(Decimal("0.05")), "10pct": Comparison(Decimal("0.10"))}
        if allocation_comparison
        else {}
    )
    pair_jobs: dict[str, asyncio.Task[None]] = {}
    pair_calls: dict[str, Counter[str]] = {key: Counter() for key in pairs}
    pair_completed: dict[str, Counter[str]] = {key: Counter() for key in pairs}
    digest = hashlib.sha256()
    started = now()
    sweeps = calls = approvals = reviews_completed = 0
    symbols: set[str] = set()
    review: (
        asyncio.Task[tuple[datetime, dict[str, str], dict[str, Decimal], dict[str, str]]] | None
    ) = None
    reviewing: list[dict[str, Any]] = []
    state, error = "running", None

    async def review_signals(
        payload: dict[str, Any],
    ) -> tuple[datetime, dict[str, str], dict[str, Decimal], dict[str, str]]:
        holds: dict[str, str] = {}
        approved, watches = await decide(settings, payload, holds=holds)
        return now(), approved, watches, holds

    def status() -> None:
        report = {
            "state": state,
            "complete": state == "complete",
            "error_type": error,
            "started_at": started.isoformat(),
            "until": until.isoformat(),
            "updated_at": now().isoformat(),
            "profile": profile,
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
        if pairs:
            report.update(
                accounts={
                    f"{key}_{name}": account.summary(pair.prices)
                    for key, pair in pairs.items()
                    for name, account in pair.accounts.items()
                },
                observed_symbols=len(next(iter(pairs.values())).prices),
                ai_calls=sum(sum(count.values()) for count in pair_calls.values()),
                ai_reviews_completed=sum(sum(count.values()) for count in pair_completed.values()),
                ai_in_flight=any(not job.done() for job in pair_jobs.values()),
                ai_by_allocation={
                    key: {"calls": pair_calls[key], "completed": pair_completed[key]}
                    for key in pairs
                },
            )
            del report["watch_prices_krw"]
            del report["ai_approvals"]
        atomic_json(output / "status.json", json.loads(encode(report)))

    with (
        (output / "market.jsonl").open("x", buffering=1) as raw,
        (output / "events.jsonl").open("x", buffering=1) as events,
    ):

        def record(kind: str, data: dict[str, Any]) -> None:
            events.write(encode({"kind": kind, "recorded_at": now(), **data}) + "\n")

        async def run_pair(key: str, signals: list[dict[str, Any]]) -> None:
            def audit(kind: str, **fields: Any) -> None:
                record(kind, {"allocation": key, **fields})

            await review_cascade(
                settings, pairs[key], signals, until, audit, pair_calls[key], pair_completed[key]
            )

        feed = LiveFeed(
            settings, lambda k, data: record(k, data) if k == "http" else None, client=client
        )
        try:
            status()
            client.access_token()
            listing_market = {}
            for exchange in ("KOSPI", "KOSDAQ", "KR_ETC"):
                listed = await asyncio.to_thread(client.listed, exchange)
                for row in listed:
                    symbol = row["symbol"]
                    if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
                        raise ValueError("Invalid Korean market symbol")
                    symbols.add(symbol)
                    listing_market[symbol] = exchange
            if not symbols:
                raise ValueError("No listed Korean market symbols")
            feed.symbols = sorted(symbols)
            profile_evidence: dict[str, Any] = {}
            source_files: tuple[Path, ...] = (
                Path(__file__).resolve(),
                ROOT / "tools/kr_comparison_ai.py",
                ROOT / "tools/kr_comparison_account.py",
                ROOT / "papertrade/clock.py",
                ROOT / "papertrade/jsonio.py",
            )
            if profile == "broker-constrained":
                from tools.kr_broker_profile import continuous_window, eligible

                calendar = await asyncio.to_thread(
                    client.calendar,
                    KR.code,
                    KR.trading_date(now()),
                )
                window = continuous_window(calendar, now())
                classification = {}
                metadata = []
                for offset in range(0, len(feed.symbols), 50):
                    metadata.extend(
                        await asyncio.to_thread(client.stocks, feed.symbols[offset : offset + 50])
                    )
                for row in metadata:
                    market = listing_market.get(row["symbol"])
                    if market in ("KOSPI", "KOSDAQ") and eligible(row):
                        classification[row["symbol"]] = (market, "STOCK")
                if not classification:
                    raise ValueError("No verified ordinary KR common stocks")
                pairs = {
                    key: Comparison(
                        pair.allocation,
                        profile=profile,
                        classification=classification,
                        session_window=window,
                    )
                    for key, pair in pairs.items()
                }
                source_files += tuple(
                    ROOT / name
                    for name in (
                        "tools/kr_whole_share_accounts.py",
                        "tools/replay_kr_paper_costs.py",
                        "tools/kr_broker_profile.py",
                        "papertrade/api.py",
                        "tools/live_feed.py",
                        "tools/paper_market.py",
                    )
                )
                profile_evidence = {
                    "profile": profile,
                    "calendar": calendar,
                    "instrument_metadata": metadata,
                    "classification": classification,
                    "quantity_policy": "whole_shares",
                    "session_policy": "regular_continuous_before_closing_auction",
                    "initial_accounts": {
                        f"{key}_{name}": account.summary({})
                        for key, pair in pairs.items()
                        for name, account in pair.accounts.items()
                    },
                }
            atomic_json(
                output / "manifest.json",
                {
                    **profile_evidence,
                    "currency": "KRW",
                    "initial_capital_krw": str(CAPITAL),
                    "until": until.isoformat(),
                    "model": "Sol first, Astra on explicit deferral" if pairs else "gpt-6-astra",
                    **(
                        {
                            "allocations": {"5pct": "0.05", "10pct": "0.10"},
                            "warmup_sweeps": ALLOCATION_LOOKBACK,
                        }
                        if pairs
                        else {}
                    ),
                    "effort": "medium",
                    "universe": feed.symbols,
                    "rules": "Directional changes propose entry or exit; Astra gates the AI account",
                    "fill": (
                        "Whole-share fee-inclusive assumption at a newer last trade; no depth proof"
                        if profile == "broker-constrained"
                        else "Zero-cost fractional paper fill at a newer last trade after decision"
                    ),
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
                        decided, accepted, watches, holds = review.result()
                    except SafetyError as exc:
                        record("ai_review_failed", {"review_id": calls, "reason": str(exc)})
                    else:
                        approvals += len(accepted)
                        reviews_completed += 1
                        comparison.approve(reviewing, accepted, decided, watches)
                        record(
                            "ai_decision",
                            {
                                "review_id": calls,
                                "at": decided,
                                "approvals": accepted,
                                "holds": holds,
                                "watches": watches,
                            },
                        )
                    review = None
                line = encode({"observed_at": observed, "rows": rows}) + "\n"
                raw.write(line)
                digest.update(line.encode())
                sweeps += 1
                if pairs:
                    for key, pair in pairs.items():
                        for fill in pair.observe(
                            rows, observed, warmup=sweeps <= ALLOCATION_LOOKBACK
                        ):
                            record("fill", {**fill, "account": f"{key}_{fill['account']}"})
                        job = pair_jobs.get(key)
                        if job is not None and not job.done():
                            continue
                        if job is not None:
                            job.result()
                            del pair_jobs[key]
                        ai = pair.accounts["ai"]
                        signals = [
                            signal
                            for signal in pair.review_queue.values()
                            if ai.wants(signal) and (signal["action"] == "SELL" or ai.cash > 0)
                        ]
                        pair.review_queue.clear()
                        if signals and now() < until:
                            pair_jobs[key] = asyncio.create_task(run_pair(key, signals))
                    status()
                    await asyncio.sleep(
                        max(0, min(60 - (time.monotonic() - tick), (until - now()).total_seconds()))
                    )
                    continue
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
                    record(
                        "ai_review",
                        {
                            "review_id": calls + 1,
                            "model": "gpt-6-astra",
                            "effort": "medium",
                            **payload,
                        },
                    )
                    review = asyncio.create_task(review_signals(payload))
                    calls += 1
                status()
                await asyncio.sleep(
                    max(0, min(60 - (time.monotonic() - tick), (until - now()).total_seconds()))
                )
            if review is not None and review.done():
                try:
                    decided, accepted, watches, holds = review.result()
                except SafetyError as exc:
                    record("ai_review_failed", {"review_id": calls, "reason": str(exc)})
                else:
                    if decided < until:
                        approvals += len(accepted)
                        reviews_completed += 1
                    record(
                        "ai_decision",
                        {
                            "review_id": calls,
                            "at": decided,
                            "approvals": accepted,
                            "holds": holds,
                            "watches": watches,
                            "after_last_observation": True,
                        },
                    )
            reviewed = (
                all(count[SOL] for count in pair_completed.values()) if pairs else reviews_completed
            )
            state = "complete" if sweeps >= 2 and reviewed else "incomplete"
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
                record("ai_review_ended_at_cutoff", {"review_id": calls, "at": now()})
            review = None
            for key, job in pair_jobs.items():
                if not job.done():
                    job.cancel()
                    await asyncio.gather(job, return_exceptions=True)
                    record(
                        "ai_cancelled_at_end",
                        {"allocation": key, "review_id": sum(pair_calls[key].values())},
                    )
                elif not job.cancelled() and (failure := job.exception()) is not None:
                    state, error = "failed", type(failure).__name__
                    record("ai_review_failed", {"allocation": key, "error_type": error})
            pair_jobs.clear()
            status()
            atomic_json(
                output / "comparison.json", json.loads((output / "status.json").read_text())
            )


def main() -> None:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--until", required=True, type=datetime.fromisoformat)
    parser.add_argument("--allocation-comparison", action="store_true")
    parser.add_argument("--profile", choices=("legacy", "broker-constrained"), default="legacy")
    args = parser.parse_args()
    if args.until.tzinfo is None:
        parser.error("--until requires a timezone offset")
    asyncio.run(
        campaign(
            args.output.resolve(),
            args.until,
            allocation_comparison=args.allocation_comparison,
            profile=args.profile,
        )
    )


if __name__ == "__main__":
    main()
