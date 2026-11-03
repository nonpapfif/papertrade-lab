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
from dataclasses import replace
from pathlib import Path
from typing import Any

from papertrade.api import ApiError, private_json
from papertrade.core import SafetyError, Settings
from tools.async_layered_paper import AsyncLayeredPaper, seconds
from tools.layered_paper import Feature, assign, eligible_stock, feature_from_bars
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
    def __init__(self, capital: float, record: Any) -> None:
        self.account = AsyncLayeredPaper(capital)
        self.record = record
        self.ids: dict[str, int] = {}
        self.features: dict[str, Feature | None] = {}
        self.candidates: list[str] = []
        self.desired: list[str] = []
        self.books: dict[str, dict] = {}
        self.fx: tuple[float, str] = (0, now())
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
        ids = set(self.account.pending) | set(self.account.book.positions)
        return {s for s, i in self.ids.items() if i in ids}

    def publish(self, kind: str, payload: Any) -> None:
        self.queue.put_nowait((kind, payload, now(), time.monotonic()))
        self.max_queue = max(self.max_queue, self.queue.qsize())

    async def consume(self) -> None:
        while True:
            kind, payload, received, queued = await self.queue.get()
            started = time.monotonic()
            self.queue_delays.append(started - queued)
            try:
                if kind == "selection":
                    self.candidates = payload
                elif kind == "feature":
                    symbol, rows = payload
                    self.features[symbol] = feature_from_bars(
                        self.identifier(symbol),
                        [{**r, "available_at": received} for r in rows],
                        received,
                    )
                    self.counts[
                        "features_valid" if self.features[symbol] else "features_missing"
                    ] += 1
                elif kind == "fx":
                    self.fx = (float(payload["midRate"]), received)
                elif kind == "book":
                    symbol, book = payload
                    self.symbol_delays.setdefault(symbol, []).append(started - queued)
                    self.counts["book_events"] += 1
                    source_age = seconds(received) - seconds(book["timestamp"])
                    if not 0 <= source_age <= 60:
                        self.counts["invalid_source_age"] += 1
                        continue
                    old = self.books.get(symbol)
                    if old and seconds(old["timestamp"]) > seconds(book["timestamp"]):
                        self.counts["old_snapshot_ignored"] += 1
                        continue
                    self.books[symbol] = book
                    f = self.features.get(symbol)
                    if f is not None:
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
                    at = now()
                    before = len(self.account.intents), len(self.account.outcomes)
                    fresh = f is not None and 0 <= seconds(at) - seconds(f.bar_end) <= 90
                    self.counts["fresh_features" if fresh else "unusable_features"] += 1
                    if fresh and f is not None and assign(f):
                        self.counts["signal_observations"] += 1
                    await self.account.submit(
                        {
                            "id": f"live:{self.counts['book_events']}",
                            "symbol": self.identifier(symbol),
                            "received_at": at,
                            "book": book,
                            "feature": f,
                            "eligible": symbol in self.candidates,
                            "candidate_count": max(
                                1,
                                sum(
                                    x not in self.protected()
                                    and (candidate_feature := self.features.get(x)) is not None
                                    and 0 <= seconds(at) - seconds(candidate_feature.bar_end) <= 90
                                    and assign(candidate_feature) is not None
                                    for x in self.candidates
                                ),
                            ),
                            "fx": self.fx[0],
                            "fx_at": self.fx[1],
                        }
                    )
                    for i in self.account.intents[before[0] :]:
                        self.record(
                            "intent",
                            symbol=symbol,
                            quantity=i.quantity,
                            side=i.side,
                            decision_at=i.decision_at,
                            reserved=i.reserved,
                        )
                    for result in self.account.outcomes[before[1] :]:
                        self.record("outcome", result=result)
                elif kind == "clock":
                    await self.account.advance(now())
                else:
                    raise ValueError("Unknown runtime event")
                self.desired = subscription_set(self.candidates, self.protected())
            finally:
                self.processing_times.append(time.monotonic() - started)
                self.queue.task_done()

    def status(self) -> dict[str, Any]:
        return {
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


async def campaign(output: Path, duration: int, capital: float) -> None:
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

    settings = Settings.load(ROOT)
    data = copy.deepcopy(settings.data)
    data["limits"]["minimum_request_interval"] = 0.08
    client = CachedTokenClient(Settings(ROOT, data))
    http_audit: list[dict[str, Any]] = []
    feed = LiveFeed(
        client.settings,
        lambda kind, payload: http_audit.append(payload) if kind == "http" else None,
        client=client,
    )
    runtime = Runtime(capital, record)
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
        sources = [
            "tools/run_async_live_validation.py",
            "tools/async_layered_paper.py",
            "tools/layered_paper.py",
            "tools/live_execution.py",
            "papertrade/api.py",
        ]
        private_json(
            output / "manifest.json",
            {
                "source_sha256": {
                    p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in sources
                },
                "selection": "Absolute change between consecutive fresh minute scans; USD common shares",
                "universe_count": len(universe),
                "connection_capacity_symbols": 50,
                "calendar": await asyncio.to_thread(client.calendar, "US", now()[:10]),
                "limitations": [
                    "One connection, market-data only; not all symbols continuously watched",
                    "No crash-resume implementation; finite isolated paper account",
                    "Single live sample does not establish profitability",
                ],
            },
        )

        async def collect() -> None:
            previous: dict[str, float] = {}
            metadata: dict[str, dict] = {}
            cycle = 0
            while time.monotonic() < deadline:
                begin = time.monotonic()
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
                        )
                        if error.status in (401, 403, 429):
                            raise
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
        await runtime.account.advance(now())
        await runtime.account.close()
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
            "cash_not_overreserved": runtime.account.reserved_cash
            <= runtime.account.book.cash + 1e-9,
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
        await runtime.account.close()
        private_json(output / "status.json", stats)
        private_json(output / "http-audit.json", {"requests": http_audit})
        files.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration", type=int, default=1200)
    parser.add_argument("--capital", type=float, default=100)
    args = parser.parse_args()
    if args.duration < 360 or not math.isfinite(args.capital) or args.capital <= 0:
        parser.error("Use duration >= 360 and positive finite capital")
    asyncio.run(campaign(args.output.resolve(), args.duration, args.capital))


if __name__ == "__main__":
    main()
