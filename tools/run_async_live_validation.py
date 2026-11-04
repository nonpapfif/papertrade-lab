"""Finite market-data-only integration campaign for the asynchronous paper engine."""

from __future__ import annotations

import argparse
import asyncio
import copy
import gzip
import hashlib
import json
import math
import os
import time
from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from papertrade.api import ApiError, private_json
from papertrade.core import SafetyError, Settings
from tools.async_layered_paper import AsyncLayeredPaper, seconds
from tools.layered_paper import FAMILIES, Feature, assign, eligible_stock, feature_from_bars
from tools.live_feed import LiveFeed
from tools.probe_toss_websocket import (
    ROOT,
    URL,
    CachedTokenClient,
    declaration,
    error_details,
    now,
    read_spec,
)

LEGACY_SOURCES = (
    "tools/run_async_live_validation.py",
    "tools/async_layered_paper.py",
    "tools/layered_paper.py",
    "tools/live_execution.py",
    "papertrade/api.py",
)
SOURCES = (
    *LEGACY_SOURCES,
    "papertrade/core.py",
    "tools/live_feed.py",
    "tools/probe_toss_websocket.py",
    "tools/search_portfolios.py",
    "tools/week_hypotheses.py",
)


US_TIMEZONE = ZoneInfo("America/New_York")
US_CAPABILITY_SNAPSHOT = {
    "source": "https://openapi.tossinvest.com/openapi-docs/latest/openapi.json",
    "retrieved": "2026-11-04",
    "version": "1.2.28",
    "sha256": "18fdc96fdf632e4869c765ed0cb169049c5310a1c48e10b40e47a71f52aa7a45",
}
US_COST_EVIDENCE = {
    "as_of": "2026-11-04",
    "status": "unverified_toss_api_fee_scenario",
    "commission": "0.1% app-disclosed rate assumed for API, no exemption applied",
    "sec": "Regulatory rate assumed passed through with modeled cent rounding and minimum",
    "taf": "Zero-rate regulatory scenario for October through December 2026",
    "settlement": "Simulated sale proceeds available immediately; broker buying power unverified",
    "fx": "Market-data conversion for reporting only; USD-funded, no simulated currency exchange",
}


def us_date(at: str) -> str:
    return datetime.fromtimestamp(seconds(at), tz=US_TIMEZONE).date().isoformat()


def us_regular_window(calendar: dict, day: str) -> tuple[float, float] | None:
    try:
        today = calendar["today"]
        regular = today["regularMarket"]
        if today["date"] != day or regular is None:
            return None
        start, end = regular["startTime"], regular["endTime"]
        start_tick, end_tick = seconds(start), seconds(end)
        if start_tick < end_tick and us_date(start) == us_date(end) == day:
            return start_tick, end_tick
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    return None


def rank_candidates(
    rows: list[dict], previous: dict[str, float], at: str, capital: float
) -> tuple[list[str], dict[str, float]]:
    fresh = {}
    for row in rows:
        try:
            price = float(row["lastPrice"])
            age = seconds(at) - seconds(row["timestamp"])
            if row["currency"] == "USD" and math.isfinite(price) and price > 0 and 0 <= age <= 90:
                fresh[row["symbol"]] = price
        except (KeyError, ValueError, TypeError):
            continue
    scored = [
        (abs(price / previous[s] - 1), s)
        for s, price in fresh.items()
        if s in previous and price <= capital and price != previous[s]
    ]
    return [s for _, s in sorted(scored, key=lambda r: (-r[0], r[1]))], fresh


def subscription_set(candidates: list[str], protected: set[str]) -> list[str]:
    if len(protected) > 50:
        raise ValueError("Protected positions exceed one connection capacity")
    return sorted(protected) + [s for s in candidates if s not in protected][: 50 - len(protected)]


def latency_summary(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "p50": ordered[int((len(ordered) - 1) * 0.5)],
        "p95": ordered[int((len(ordered) - 1) * 0.95)],
        "max": ordered[-1],
    }


class Runtime:
    def __init__(
        self,
        capital: float,
        record: Any,
        *,
        candidate_policy: str | None = None,
        profile: str = "legacy",
    ) -> None:
        if profile not in ("legacy", "broker-constrained"):
            raise ValueError("Unknown validation profile")
        self.constrained = profile == "broker-constrained"
        self.regular_window: tuple[float, float] | None = None
        if self.constrained and candidate_policy is not None:
            raise ValueError("The broker-constrained profile runs a single trend baseline")
        self.account = (
            AsyncLayeredPaper(capital, policy="trend", fees={"commission_exemption_usd": 0.0})
            if self.constrained
            else AsyncLayeredPaper(capital)
        )
        self.accounts = {"incumbent": self.account}
        if candidate_policy is not None:
            self.accounts["candidate"] = AsyncLayeredPaper(capital, policy=candidate_policy)
        self.configurations = {
            name: {
                "capital": capital,
                "policy": account.policy,
                "selection": account.selection,
                "allocation": account.allocation,
                "delay_seconds": account.delay,
                "fee_schedule": copy.deepcopy(account.context.schedule),
            }
            for name, account in self.accounts.items()
        }
        self.delivered = dict.fromkeys(self.accounts, 0)
        self.book_delivered = dict.fromkeys(self.accounts, 0)
        self.delivery_hashes = {name: hashlib.sha256() for name in self.accounts}
        self.sequence = 0
        self.record = record
        self.ids: dict[str, int] = {}
        self.features: dict[str, Feature | None] = {}
        self.candidates: list[str] = []
        self.desired: list[str] = []
        self.books: dict[str, dict] = {}
        self.fx: tuple[float, str, str | None, str | None] = (0, now(), None, None)
        self.queue: asyncio.Queue = asyncio.Queue()
        self.counts: Counter = Counter()
        self.queue_delays: list[float] = []
        self.processing_times: list[float] = []
        self.max_queue = 0
        self.symbol_delays: dict[str, list[float]] = {}

    def identifier(self, symbol: str) -> int:
        if symbol not in self.ids:
            self.ids[symbol] = len(self.ids)
        return self.ids[symbol]

    def protected(self) -> set[str]:
        ids = {
            symbol
            for account in self.accounts.values()
            for symbol in set(account.pending) | set(account.book.positions)
        }
        return {s for s, i in self.ids.items() if i in ids}

    def publish(self, kind: str, payload: Any) -> None:
        self.sequence += 1
        self.queue.put_nowait(
            (self.sequence, kind, copy.deepcopy(payload), now(), time.monotonic())
        )
        self.max_queue = max(self.max_queue, self.queue.qsize())

    def _feature_at(self, symbol: str, at: str) -> Feature | None:
        feature, book = self.features.get(symbol), self.books.get(symbol)
        tick = seconds(at)
        if (
            feature is None
            or book is None
            or book.get("currency") != "USD"
            or seconds(feature.available_at) > tick
            or not 0 <= tick - seconds(feature.bar_end) <= 90
            or not 0 <= tick - seconds(book["timestamp"]) <= 60
        ):
            return None
        if self.constrained:
            return feature
        price = (
            min(float(r["price"]) for r in book["asks"])
            + max(float(r["price"]) for r in book["bids"])
        ) / 2
        return replace(
            feature,
            price=price,
            r5=price / (feature.price / (1 + feature.r5)) - 1,
            r15=price / (feature.price / (1 + feature.r15)) - 1,
        )

    def _record_delivery(self, name: str, observation: bytes) -> None:
        self.delivered[name] += 1
        self.delivery_hashes[name].update(observation)

    async def close(self) -> None:
        for account in self.accounts.values():
            await account.close()

    async def consume(self) -> None:
        while True:
            sequence, kind, payload, received, queued = await self.queue.get()
            started = time.monotonic()
            self.queue_delays.append(started - queued)
            try:
                if kind == "selection":
                    self.candidates = payload
                elif kind == "calendar":
                    day, calendar = payload
                    self.regular_window = us_regular_window(calendar, day)
                elif kind == "feature":
                    symbol, rows = payload
                    if self.constrained:
                        window = self.regular_window
                        rows = [
                            row
                            for row in rows
                            if window is not None
                            and window[0] < seconds(row["timestamp"]) <= window[1]
                        ]
                    self.features[symbol] = feature_from_bars(
                        self.identifier(symbol),
                        [{**r, "available_at": received} for r in rows],
                        received,
                        timestamp_is_end=self.constrained,
                    )
                    self.counts[
                        "features_valid" if self.features[symbol] else "features_missing"
                    ] += 1
                elif kind == "fx":
                    try:
                        rate = float(payload["midRate"])
                    except (KeyError, TypeError, ValueError):
                        rate = 0
                    pair_matches = payload.get("baseCurrency", "USD") == "USD" and payload.get(
                        "quoteCurrency", "KRW"
                    ) == "KRW"
                    if not math.isfinite(rate) or rate <= 0 or not pair_matches:
                        self.counts["invalid_fx"] += 1
                        rate = 0
                    self.fx = (
                        rate,
                        received,
                        payload.get("validFrom"),
                        payload.get("validUntil"),
                    )
                elif kind == "book":
                    symbol, book = payload
                    self.symbol_delays.setdefault(symbol, []).append(started - queued)
                    self.counts["book_events"] += 1
                    if not isinstance(book, dict) or not isinstance(book.get("timestamp"), str):
                        self.counts["book_unavailable"] += 1
                        self.record(
                            "book_rejected",
                            reason="unavailable_timestamp",
                            sequence=sequence,
                            symbol=symbol,
                        )
                        continue
                    source_age = seconds(received) - seconds(book["timestamp"])
                    if not 0 <= source_age <= 60:
                        self.counts["invalid_source_age"] += 1
                        continue
                    at = now() if self.constrained else ""
                    if self.constrained:
                        window = self.regular_window
                        if window is None or not all(
                            window[0] <= seconds(stamp) < window[1]
                            for stamp in (book["timestamp"], received, at)
                        ):
                            self.counts["outside_regular_session"] += 1
                            continue
                        if not book["asks"] or not book["bids"]:
                            self.counts["missing_book_depth"] += 1
                            continue
                    old = self.books.get(symbol)
                    if old and seconds(old["timestamp"]) > seconds(book["timestamp"]):
                        self.counts["old_snapshot_ignored"] += 1
                        continue
                    paired = len(self.accounts) > 1
                    if (
                        (paired or self.constrained)
                        and book.get("currency") == "USD"
                        and book["asks"]
                        and book["bids"]
                        and max(float(row["price"]) for row in book["bids"])
                        > min(float(row["price"]) for row in book["asks"])
                    ):
                        self.counts["crossed_orderbook_rejected"] += 1
                        self.record(
                            "book_rejected",
                            reason="crossed_orderbook",
                            sequence=sequence,
                            symbol=symbol,
                            source_at=book["timestamp"],
                        )
                        continue
                    self.books[symbol] = book
                    f = self.features.get(symbol)
                    if f is not None and not self.constrained:
                        price = (
                            min(float(r["price"]) for r in book["asks"])
                            + max(float(r["price"]) for r in book["bids"])
                        ) / 2
                        f = replace(
                            f,
                            price=price,
                            r5=price / (f.price / (1 + f.r5)) - 1,
                            r15=price / (f.price / (1 + f.r15)) - 1,
                        )
                    if not self.constrained:
                        at = now()
                    use_prepared = paired or self.constrained
                    prepared = (
                        {s: self._feature_at(s, at) for s in self.candidates}
                        if use_prepared
                        else {}
                    )
                    if use_prepared:
                        f = self._feature_at(symbol, at)
                    fresh = f is not None and 0 <= seconds(at) - seconds(f.bar_end) <= 90
                    self.counts["fresh_features" if fresh else "unusable_features"] += 1
                    if fresh and f is not None and assign(f, self.account.policy):
                        self.counts["signal_observations"] += 1
                    shared = {
                        "id": f"live:{self.counts['book_events']}",
                        "symbol": self.identifier(symbol),
                        "received_at": at,
                        "book": book,
                        "feature": f,
                        "eligible": symbol in self.candidates,
                        "fx": self.fx[0],
                        "fx_at": self.fx[1],
                        "fx_valid_from": self.fx[2],
                        "fx_valid_until": self.fx[3],
                        "fx_provider_validity_required": self.constrained,
                    }
                    observation = (
                        json.dumps(
                            {
                                "sequence": sequence,
                                "published_at": received,
                                **shared,
                                "feature": asdict(f) if f is not None else None,
                            },
                            sort_keys=True,
                            allow_nan=False,
                        ).encode()
                        + b"\n"
                    )
                    for name, account in self.accounts.items():
                        before = len(account.intents), len(account.outcomes)
                        excluded = set(account.pending) | set(account.book.positions)
                        candidate_count = max(
                            1,
                            sum(
                                self.ids.get(x) not in excluded
                                and (
                                    candidate_feature := (
                                        prepared.get(x) if use_prepared else self.features.get(x)
                                    )
                                )
                                is not None
                                and 0 <= seconds(at) - seconds(candidate_feature.bar_end) <= 90
                                and assign(candidate_feature, account.policy) is not None
                                for x in self.candidates
                            ),
                        )
                        await account.submit({**shared, "candidate_count": candidate_count})
                        self._record_delivery(name, observation)
                        self.book_delivered[name] += 1
                        identity = {"account": name} if paired else {}
                        for i in account.intents[before[0] :]:
                            self.record(
                                "intent",
                                symbol=symbol,
                                quantity=i.quantity,
                                side=i.side,
                                decision_at=i.decision_at,
                                reserved=i.reserved,
                                **identity,
                            )
                        for result in account.outcomes[before[1] :]:
                            self.record("outcome", result=result, **identity)
                    if paired:
                        self.record(
                            "comparison_observation",
                            sequence=sequence,
                            symbol=symbol,
                            received_at=received,
                            decision_at=at,
                            input_sha256=hashlib.sha256(observation).hexdigest(),
                        )
                elif kind == "clock":
                    at = now()
                    observation = json.dumps([sequence, "clock", received, at]).encode() + b"\n"
                    for name, account in self.accounts.items():
                        await account.advance(at)
                        self._record_delivery(name, observation)
                else:
                    raise ValueError("Unknown runtime event")
                self.desired = subscription_set(self.candidates, self.protected())
            finally:
                self.processing_times.append(time.monotonic() - started)
                self.queue.task_done()

    def status(self) -> dict[str, Any]:
        result = {
            "counts": dict(self.counts),
            "desired": self.desired,
            "protected": sorted(self.protected()),
            "symbol_ids": self.ids,
            "queue_depth": self.queue.qsize(),
            "max_queue_depth": self.max_queue,
            "per_symbol_queue_delay_seconds": {
                s: latency_summary(v) for s, v in self.symbol_delays.items()
            },
            "queue_delay_seconds": latency_summary(self.queue_delays),
            "processing_seconds": latency_summary(self.processing_times),
            "paper": self.account.summary() if self.account.last_at else None,
        }
        if len(self.accounts) > 1:
            result["comparison"] = {
                "qualification": "not_assessed",
                "inputs_aligned": len(set(self.delivered.values())) == 1
                and len({h.hexdigest() for h in self.delivery_hashes.values()}) == 1,
                "configuration_unchanged": all(
                    account.policy == self.configurations[name]["policy"]
                    and account.selection == self.configurations[name]["selection"]
                    and account.allocation == self.configurations[name]["allocation"]
                    and account.delay == self.configurations[name]["delay_seconds"]
                    and account.book.initial_equity == self.configurations[name]["capital"]
                    and account.context.schedule == self.configurations[name]["fee_schedule"]
                    for name, account in self.accounts.items()
                ),
                "accounts": {
                    name: {
                        "configuration": copy.deepcopy(self.configurations[name]),
                        "observations": self.delivered[name],
                        "book_observations": self.book_delivered[name],
                        "input_sha256": self.delivery_hashes[name].hexdigest(),
                        "paper": account.summary() if account.last_at else None,
                    }
                    for name, account in self.accounts.items()
                },
            }
        return result


def write_comparison(
    output: Path, stats: dict[str, Any], runtime: Runtime
) -> dict[str, Any] | None:
    """Save descriptive comparison evidence after the shared recording files close."""
    if len(runtime.accounts) == 1 and not runtime.constrained:
        return None
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    expected = manifest.get("source_sha256", {})
    expected_sources = (
        set(SOURCES) if runtime.constrained or len(runtime.accounts) > 1 else set(LEGACY_SOURCES)
    )
    source_unchanged = set(expected) == expected_sources and all(
        (ROOT / name).is_file()
        and hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected[name]
        for name in expected_sources
    )
    hashes = {}
    for name in ("manifest.json", "market.jsonl.gz", "events.jsonl"):
        path = output / name
        if path.is_file():
            with path.open("rb") as stream:
                hashes[name] = hashlib.file_digest(stream, "sha256").hexdigest()
    runtime_status = runtime.status()
    comparison = runtime_status.get("comparison", {})
    if len(runtime.accounts) == 1:
        account = runtime.account
        frozen = runtime.configurations["incumbent"]
        configuration_unchanged = (
            account.policy == frozen["policy"]
            and account.selection == frozen["selection"]
            and account.allocation == frozen["allocation"]
            and account.delay == frozen["delay_seconds"]
            and account.book.initial_equity == frozen["capital"]
            and account.context.schedule == frozen["fee_schedule"]
        )
    else:
        configuration_unchanged = comparison.get("configuration_unchanged", False)
    freeze_matches = (
        manifest.get("comparison") == runtime.configurations
        if len(runtime.accounts) > 1
        else manifest.get("account") == runtime.configurations["incumbent"]
    )
    complete = (
        stats.get("state") == "complete"
        and source_unchanged
        and len(hashes) == 3
        and freeze_matches
        and comparison.get("inputs_aligned", True)
        and configuration_unchanged
    )
    state = stats.get("state") if stats.get("state") in ("complete", "failed") else "interrupted"
    result = {
        **stats,
        **comparison,
        "state": state if complete or state != "complete" else "incomplete",
        "result": (
            "inconclusive"
            if state == "complete" and not complete and stats.get("result") == "passed"
            else stats.get("result")
        ),
        "complete": complete,
        "ended_at": now(),
        "source_unchanged": source_unchanged,
        "configuration_unchanged": configuration_unchanged,
        "freeze_matches": freeze_matches,
        "observation_status": "observed_short_sample"
        if all(runtime.book_delivered.values())
        else "inconclusive",
        "input_files_sha256": hashes,
        "freeze": manifest.get("comparison"),
        "symbol_ids": runtime.ids,
    }
    private_json(output / "comparison.json", result)
    return result


async def wait_for_rest_cooldown(error: ApiError, deadline: float) -> None:
    if error.status in (401, 403):
        raise error
    if error.status != 429 and error.code != "cooldown":
        return
    if error.retry_at is None or not math.isfinite(error.retry_at):
        raise error
    delay = min(error.retry_at - time.time(), deadline - time.monotonic())
    if delay > 0:
        await asyncio.sleep(delay)


async def campaign(
    output: Path,
    duration: int,
    capital: float,
    *,
    candidate_policy: str | None = None,
    profile: str = "legacy",
) -> None:
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosed, WebSocketException

    os.umask(0o077)
    output.mkdir(parents=True, exist_ok=False)
    files = ExitStack()
    log = files.enter_context((output / "events.jsonl").open("x", buffering=1))
    raw = files.enter_context(
        gzip.open(output / "market.jsonl.gz", "wt")  # noqa: SIM115 - ExitStack closes on all exits.
    )

    def record(kind: str, **payload: Any) -> None:
        log.write(json.dumps({"at": now(), "kind": kind, **payload}) + "\n")

    http_audit: list[dict[str, Any]] = []
    runtime = Runtime(capital, record, candidate_policy=candidate_policy, profile=profile)
    stats: dict[str, Any] = {
        "state": "starting",
        "ai_calls": 0,
        "real_orders": False,
        "capital": capital,
        "duration_seconds": duration,
        "rest_cycles": [],
        "connections": 0,
        "acks": [],
        "faults": [],
    }
    tasks: list[asyncio.Task] = []
    token = ""
    resync = asyncio.Event()
    try:
        settings = Settings.load(ROOT)
        data = copy.deepcopy(settings.data)
        data["limits"]["minimum_request_interval"] = 0.08
        client = CachedTokenClient(Settings(ROOT, data))
        feed = LiveFeed(
            client.settings,
            lambda kind, payload: http_audit.append(payload) if kind == "http" else None,
            client=client,
        )
        token = client.access_token()
        spec = await asyncio.to_thread(read_spec)
        (output / "asyncapi.json").write_bytes(spec)
        universe = await asyncio.to_thread(feed.universe)
        started = time.monotonic()
        deadline = started + duration
        stats.update(
            state="running",
            started_at=now(),
            expected_end_epoch=time.time() + duration,
            universe_count=len(universe),
        )
        calendar_day = us_date(now()) if runtime.constrained else now()[:10]
        calendar = await asyncio.to_thread(client.calendar, "US", calendar_day)
        if runtime.constrained:
            runtime.publish("calendar", (calendar_day, calendar))
        sources = SOURCES if candidate_policy is not None or runtime.constrained else LEGACY_SOURCES
        private_json(
            output / "manifest.json",
            {
                **({"comparison": runtime.configurations} if candidate_policy is not None else {}),
                "source_sha256": {
                    p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in sources
                },
                "selection": (
                    "Absolute change between consecutive fresh minute scans; USD common shares"
                ),
                "universe_count": len(universe),
                "connection_capacity_symbols": 50,
                "calendar": calendar,
                **(
                    {
                        "profile": profile,
                        "account": runtime.configurations["incumbent"],
                        "cost_evidence": US_COST_EVIDENCE,
                        "session_policy": (
                            "Provider US regular session, open inclusive, close exclusive"
                        ),
                        "market": "US",
                        "currency": "USD",
                        "instrument": "Active ordinary common shares",
                        "quantity_mode": "whole_shares",
                        "order_model": "Simulated limit with delayed depth confirmation",
                        "capability_snapshot": US_CAPABILITY_SNAPSHOT,
                    }
                    if runtime.constrained
                    else {}
                ),
                "limitations": [
                    "One connection, market-data only; not all symbols continuously watched",
                    "No crash-resume implementation; finite isolated paper account",
                    "Single live sample does not establish profitability",
                ],
            },
        )

        async def collect() -> None:
            nonlocal calendar_day
            previous: dict[str, float] = {}
            metadata: dict[str, dict] = {}
            cycle = 0
            while time.monotonic() < deadline:
                begin = time.monotonic()
                if runtime.constrained and (day := us_date(now())) != calendar_day:
                    calendar = await asyncio.to_thread(client.calendar, "US", day)
                    raw.write(
                        json.dumps({"kind": "calendar", "date": day, "result": calendar}) + "\n"
                    )
                    runtime.publish("calendar", (day, calendar))
                    calendar_day = day
                if cycle == 2:
                    before = runtime.counts["book_events"]
                    record("rest_delay_start", seconds=5)
                    await asyncio.sleep(5)
                    stats["faults"].append(
                        {
                            "kind": "rest_delay",
                            "seconds": 5,
                            "book_events_during": runtime.counts["book_events"] - before,
                        }
                    )
                rows = await asyncio.to_thread(feed.snapshot)
                received = now()
                raw.write(
                    json.dumps({"kind": "rest_prices", "received_at": received, "rows": rows})
                    + "\n"
                )
                ranked, previous = rank_candidates(rows, previous, received, capital)
                chosen: list[str] = []
                for offset in range(0, len(ranked), 50):
                    batch = ranked[offset : offset + 50]
                    missing = [s for s in batch if s not in metadata]
                    if missing:
                        for row in await asyncio.to_thread(client.stocks, missing):
                            metadata[row["symbol"]] = row
                            raw.write(
                                json.dumps({"kind": "stock_info", "received_at": now(), "row": row})
                                + "\n"
                            )
                    chosen.extend(s for s in batch if eligible_stock(metadata.get(s, {})))
                    if len(chosen) >= 50:
                        break
                runtime.publish("selection", chosen[:50])
                fx = await asyncio.to_thread(feed.fx)
                raw.write(json.dumps({"kind": "fx", "received_at": now(), "result": fx}) + "\n")
                runtime.publish("fx", fx)
                watch = subscription_set(chosen[:50], runtime.protected())
                # Sequential REST calls share the client's rate controller off the event loop.
                for symbol in watch:
                    if time.monotonic() >= deadline:
                        break
                    try:
                        candles = await asyncio.to_thread(client.candles, symbol, now(), 90)
                        raw.write(
                            json.dumps(
                                {
                                    "kind": "candles",
                                    "received_at": now(),
                                    "symbol": symbol,
                                    "result": candles,
                                }
                            )
                            + "\n"
                        )
                        runtime.publish("feature", (symbol, candles["candles"]))
                        book = await asyncio.to_thread(client.book, symbol)
                        raw.write(
                            json.dumps(
                                {
                                    "kind": "rest_book",
                                    "received_at": now(),
                                    "symbol": symbol,
                                    "result": book,
                                }
                            )
                            + "\n"
                        )
                        runtime.publish("book", (symbol, book))
                    except ApiError as error:
                        record(
                            "rest_symbol_failure",
                            symbol=symbol,
                            status=error.status,
                            code=error.code,
                            retry_at=error.retry_at,
                        )
                        await wait_for_rest_cooldown(error, deadline)
                elapsed = time.monotonic() - begin
                stats["rest_cycles"].append(
                    {
                        "cycle": cycle,
                        "seconds": elapsed,
                        "prices": len(rows),
                        "ranked": len(ranked),
                        "selected": watch,
                    }
                )
                record("rest_cycle", **stats["rest_cycles"][-1])
                cycle += 1
                try:
                    await asyncio.wait_for(
                        resync.wait(), max(0.01, min(60 - elapsed, deadline - time.monotonic()))
                    )
                    resync.clear()
                except TimeoutError:
                    pass

        async def stream() -> None:
            injected = False
            while time.monotonic() < deadline:
                if not runtime.desired:
                    await asyncio.sleep(0.1)
                    continue
                try:
                    async with connect(
                        URL,
                        additional_headers={"Authorization": "Bearer " + token},
                        proxy=None,
                        ping_interval=20,
                        ping_timeout=20,
                        open_timeout=15,
                        close_timeout=5,
                    ) as ws:
                        stats["connections"] += 1
                        record("connected", connection=stats["connections"])
                        resync.set()
                        selected: list[str] = []
                        accepted: set[str] = set()
                        expected: set[str] = set()
                        ack_pending = False
                        declaration_at = 0.0
                        next_ping = 0.0
                        ping_at: float | None = None
                        while time.monotonic() < deadline:
                            tick = time.monotonic()
                            if not runtime.desired:
                                await ws.close(code=1000, reason="No candidates or holdings")
                                break
                            if not injected and tick - started >= duration / 2:
                                injected = True
                                stats["faults"].append(
                                    {"kind": "controlled_disconnect", "at": now()}
                                )
                                await ws.close(code=1000, reason="Controlled integration test")
                                record("controlled_disconnect")
                                break
                            if (
                                runtime.desired != selected
                                and not ack_pending
                                and tick - declaration_at >= 1
                            ):
                                selected = list(runtime.desired)
                                expected = {
                                    f"{c}:us:{s}" for c in ("trade", "orderbook") for s in selected
                                }
                                await ws.send(json.dumps(declaration("us", selected)))
                                declaration_at, ack_pending = tick, True
                            if ack_pending and tick - declaration_at > 20:
                                raise TimeoutError("Subscription acknowledgement timeout")
                            if ping_at is not None and tick - ping_at > 20:
                                raise TimeoutError("Application pong timeout")
                            if tick >= next_ping:
                                await ws.send("PING")
                                ping_at, next_ping = tick, tick + 60
                            try:
                                message = json.loads(await asyncio.wait_for(ws.recv(), 0.25))
                            except TimeoutError:
                                continue
                            kind = message.get("type")
                            if kind == "subscriptions":
                                actual = set(message.get("subscribed", []))
                                if actual != expected or message.get("rejected"):
                                    raise ValueError("Subscription rejection or mismatch")
                                accepted, ack_pending = actual, False
                                stats["acks"].append(
                                    {
                                        "at": now(),
                                        "connection": stats["connections"],
                                        "symbols": selected,
                                        "protected": sorted(runtime.protected()),
                                    }
                                )
                                record("subscription_ack", symbols=selected)
                            elif kind == "pong":
                                ping_at = None
                            elif kind == "message":
                                topic = message["topic"]
                                if topic not in accepted | expected:
                                    raise ValueError("Unexpected stream topic")
                                raw.write(
                                    json.dumps({"received_at": now(), "frame": message}) + "\n"
                                )
                                stats["market_messages"] = stats.get("market_messages", 0) + 1
                                if topic.startswith("orderbook:us:"):
                                    runtime.publish(
                                        "book", (topic.rsplit(":", 1)[1], message["data"])
                                    )
                            else:
                                raise ValueError("Unexpected provider frame")
                except (ConnectionClosed, OSError, TimeoutError) as error:
                    record("connection_failure", **error_details(error, token))
                if time.monotonic() < deadline:
                    record("reconnect_backoff", seconds=2)
                    await asyncio.sleep(2)

        consumer = asyncio.create_task(runtime.consume())
        collector = asyncio.create_task(collect())
        streamer = asyncio.create_task(stream())
        tasks = [consumer, collector, streamer]
        while time.monotonic() < deadline:
            for task in tasks:
                if task.done():
                    task.result()
                    if task is consumer:
                        raise RuntimeError("Consumer stopped")
            runtime.publish("clock", None)
            private_json(output / "status.json", {**stats, "at": now(), **runtime.status()})
            raw.flush()
            await asyncio.sleep(1)
        for producer in (collector, streamer):
            if producer.done():
                producer.result()
            producer.cancel()
        await asyncio.gather(collector, streamer, return_exceptions=True)
        if consumer.done():
            consumer.result()
        drain = asyncio.create_task(runtime.queue.join())
        done, _ = await asyncio.wait({consumer, drain}, return_when=asyncio.FIRST_COMPLETED)
        if consumer in done:
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
            consumer.result()
        await drain
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        at = now()
        for account in runtime.accounts.values():
            await account.advance(at)
        await runtime.close()
        stats.update(state="complete", at=now(), **runtime.status())
        checks = {
            "stream_observed": stats.get("market_messages", 0) > 0,
            "features_observed": runtime.counts["features_valid"] > 0,
            "rest_delay_did_not_stop_books": any(
                x["kind"] == "rest_delay" and x["book_events_during"] > 0 for x in stats["faults"]
            ),
            "reconnected": len({a["connection"] for a in stats["acks"]}) >= 2,
            "queue_p95_under_one_second": latency_summary(runtime.queue_delays).get("p95", math.inf)
            < 1,
            "subscription_replaced": len({tuple(a["symbols"]) for a in stats["acks"]}) >= 2,
            "protected_subscription_retained": all(
                set(a["protected"]) <= set(a["symbols"]) for a in stats["acks"]
            ),
            "cash_not_overreserved": all(
                account.reserved_cash <= account.book.cash + 1e-9
                for account in runtime.accounts.values()
            ),
        }
        stats.update(checks=checks, result="passed" if all(checks.values()) else "inconclusive")
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        RuntimeError,
        SafetyError,
        ApiError,
        WebSocketException,
    ) as error:
        stats.update(state="failed", at=now(), **runtime.status(), **error_details(error, token))
        record("failed", **error_details(error, token))
        raise SystemExit("Live validation failed; inspect sanitized status") from None
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runtime.close()
        if stats.get("state") not in ("complete", "failed", "interrupted"):
            stats.update(state="interrupted", ended_at=now(), **runtime.status())
        elif stats.get("state") == "complete":
            stats.setdefault("ended_at", stats.get("at", now()))
        stats.update(**runtime.status())
        private_json(output / "http-audit.json", {"requests": http_audit})
        files.close()
        comparison = write_comparison(output, stats, runtime)
        if comparison is not None:
            stats.update(
                state=comparison["state"],
                result=comparison.get("result") or stats.get("result"),
                **runtime.status(),
            )
            stats.setdefault("comparison", {}).update(
                complete=comparison["complete"],
                source_unchanged=comparison["source_unchanged"],
                freeze_matches=comparison["freeze_matches"],
            )
        private_json(output / "status.json", stats)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration", type=int, default=1200)
    parser.add_argument("--capital", type=float, default=100)
    parser.add_argument("--candidate-policy", choices=("router", *FAMILIES))
    parser.add_argument("--profile", choices=("legacy", "broker-constrained"), default="legacy")
    args = parser.parse_args()
    if args.duration < 360 or not math.isfinite(args.capital) or args.capital <= 0:
        parser.error("Use duration >= 360 and positive finite capital")
    if args.profile == "broker-constrained" and args.candidate_policy is not None:
        parser.error("The broker-constrained profile runs a single trend baseline")
    asyncio.run(
        campaign(
            args.output.resolve(),
            args.duration,
            args.capital,
            candidate_policy=args.candidate_policy,
            profile=args.profile,
        )
    )


if __name__ == "__main__":
    main()
