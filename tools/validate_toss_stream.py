"""Finite read-only subscription, load, REST and OHLCV validation campaign."""

from __future__ import annotations

import argparse
import asyncio
import copy
import gzip
import hashlib
import json
import logging
import os
import resource
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from papertrade.clock import utc_iso as now
from papertrade.core import SafetyError, Settings
from papertrade.jsonio import private_json
from tools.live_feed import LiveFeed
from tools.probe_toss_websocket import (
    ROOT,
    SPEC,
    URL,
    CachedTokenClient,
    declaration,
    error_details,
    read_spec,
    record_source_delta,
)

LIQUID = [
    "F",
    "SOFI",
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    "AMD",
    "INTC",
    "QCOM",
    "MU",
    "AMAT",
    "ARM",
    "DELL",
    "HPQ",
    "HPE",
    "CSCO",
    "ORCL",
    "CRM",
    "ADBE",
    "PLTR",
    "SNOW",
    "NET",
    "CRWD",
    "PANW",
    "UBER",
    "LYFT",
    "ABNB",
    "DASH",
    "PYPL",
    "HOOD",
    "COIN",
    "GM",
    "RIVN",
    "LCID",
    "NIO",
    "XPEV",
    "BABA",
    "JD",
    "PDD",
    "BIDU",
    "SPY",
    "QQQ",
    "AVGO",
    "TXN",
    "LRCX",
    "KLAC",
    "GOOG",
    "NOW",
]


def mature_bars(result: dict[str, Any], received: str) -> dict[str, dict[str, Any]]:
    cutoff = datetime.fromisoformat(received).timestamp() - 60
    return {
        r["timestamp"]: r
        for r in result["candles"]
        if datetime.fromisoformat(r["timestamp"]).timestamp() <= cutoff
    }


async def run(output: Path, duration: int) -> None:
    from websockets.asyncio.client import connect
    from websockets.exceptions import WebSocketException

    os.umask(0o077)
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    settings = Settings.load(ROOT)
    data = copy.deepcopy(settings.data)
    data["limits"]["minimum_request_interval"] = 0.08
    client = CachedTokenClient(Settings(ROOT, data))
    token = ""
    started = time.monotonic()
    state: dict[str, Any] = {
        "state": "preflight",
        "at": now(),
        "started_at": now(),
        "pid": os.getpid(),
        "ai_calls": 0,
        "real_orders": False,
        "scope": "T2/T3 read-only data tests; no strategy profitability claim",
        "duration_seconds": duration,
        "acks": [],
        "rest_cycles": [],
        "clock_deltas": {},
        "messages": {},
        "bar_revisions": 0,
    }
    private_json(output / "status.json", state)
    events = (output / "events.jsonl").open("x", buffering=1)

    def record(kind: str, **payload: Any) -> None:
        events.write(
            json.dumps(
                {"at": now(), "elapsed": time.monotonic() - started, "kind": kind, **payload}
            )
            + "\n"
        )

    audit: list[dict[str, Any]] = []
    client.audit = audit.append
    rest_task: asyncio.Task[None] | None = None
    try:
        token = client.access_token()
        spec = await asyncio.to_thread(read_spec)
        (output / "asyncapi.json").write_bytes(spec)

        def audit_record(kind: str, payload: dict[str, Any]) -> None:
            if kind == "http":
                audit.append(payload)

        feed = LiveFeed(client.settings, audit_record, client=client)
        universe = await asyncio.to_thread(feed.universe)
        symbols = [s for s in LIQUID if s in universe][:50]
        if len(symbols) != 50:
            raise ValueError("Fifty listed validation symbols are required")
        core = symbols[:2]
        private_json(
            output / "manifest.json",
            {
                "symbols": symbols,
                "universe_count": len(universe),
                "spec_url": SPEC,
                "spec_sha256": hashlib.sha256(spec).hexdigest(),
                "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "calendar": await asyncio.to_thread(client.calendar, "US", now()[:10]),
                "bar_end_assumption": "timestamp plus 60s; maturity candidate, not provider finality guarantee",
                "stages": [
                    "2 symbols",
                    "replace candidate retaining core",
                    "10 symbols",
                    "50 symbols",
                    "reconnect 50",
                ],
                "core": core,
                "ai_calls": 0,
                "real_orders": False,
            },
        )
        deadline = time.monotonic() + duration
        state["expected_end_epoch"] = time.time() + duration + 30
        counts: Counter[str] = Counter()
        previous_bars: dict[str, dict[str, dict[str, Any]]] = {}

        def collect_rest(cycle: int) -> dict[str, Any]:
            begin = time.monotonic()
            prices = feed.snapshot()
            candles = {}
            for symbol in core:
                result = client.candles(symbol, now(), 90)
                candles[symbol] = {"received_at": now(), "result": result}
            path = output / f"rest-{cycle:03d}.json.gz"
            with gzip.open(path, "wt") as stream:
                json.dump({"received_at": now(), "prices": prices, "candles": candles}, stream)
            return {
                "cycle": cycle,
                "elapsed_seconds": time.monotonic() - begin,
                "prices": len(prices),
                "candles": candles,
            }

        async def rest_loop() -> None:
            cycle = 0
            while time.monotonic() < deadline:
                begin = time.monotonic()
                result = await asyncio.to_thread(collect_rest, cycle)
                for symbol, payload in result.pop("candles").items():
                    current = mature_bars(payload["result"], payload["received_at"])
                    old = previous_bars.get(symbol, {})
                    revised = [t for t in old.keys() & current.keys() if old[t] != current[t]]
                    state["bar_revisions"] += len(revised)
                    previous_bars[symbol] = current
                    record(
                        "ohlcv",
                        symbol=symbol,
                        mature_candidates=len(current),
                        revised_timestamps=revised,
                        newest=max(current, default=None),
                    )
                state["rest_cycles"].append(result)
                record("rest_cycle", **result)
                cycle += 1
                await asyncio.sleep(
                    max(0, min(60 - (time.monotonic() - begin), deadline - time.monotonic()))
                )

        rest_task = asyncio.create_task(rest_loop())
        with gzip.open(output / "core-frames.jsonl.gz", "wt") as frames:
            for connection in range(2):
                boundary = deadline - duration / 3 if connection == 0 else deadline
                async with connect(
                    URL,
                    additional_headers={"Authorization": "Bearer " + token},
                    proxy=None,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=15,
                    close_timeout=5,
                ) as ws:
                    record("connected", connection=connection)
                    sent_stage = -1
                    next_ping, next_status = 0.0, 0.0
                    pending: float | None = None
                    seen_topics = {f"{c}:us:{s}" for c in ("trade", "orderbook") for s in symbols}
                    while time.monotonic() < boundary:
                        tick = time.monotonic()
                        if rest_task.done():
                            rest_task.result()
                        fraction = 1 - (deadline - tick) / duration
                        stage = (
                            0
                            if fraction < 1 / 6
                            else 1
                            if fraction < 1 / 3
                            else 2
                            if fraction < 1 / 2
                            else 3
                        )
                        selected = (
                            symbols[:2]
                            if stage == 0
                            else [symbols[0], symbols[2]]
                            if stage == 1
                            else symbols[:10]
                            if stage == 2
                            else symbols
                        )
                        if stage != sent_stage:
                            await ws.send(json.dumps(declaration("us", selected)))
                            record(
                                "declaration", connection=connection, stage=stage, symbols=selected
                            )
                            sent_stage = stage
                        if pending is not None and tick - pending > 20:
                            raise TimeoutError("Application keepalive deadline")
                        if tick >= next_ping:
                            await ws.send("PING")
                            pending = tick
                            next_ping = tick + 60
                            record("ping", connection=connection)
                        if tick >= next_status:
                            state.update(
                                state="running",
                                at=now(),
                                stage=stage,
                                connection=connection,
                                messages=dict(counts),
                                cpu_seconds=time.process_time(),
                                max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                            )
                            private_json(output / "status.json", state)
                            frames.flush()
                            next_status = tick + 10
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1)
                        except TimeoutError:
                            continue
                        message = json.loads(raw)
                        kind = message.get("type")
                        received = now()
                        if kind == "subscriptions":
                            accepted = set(message.get("subscribed", []))
                            desired = {
                                f"{c}:us:{s}" for c in ("trade", "orderbook") for s in selected
                            }
                            state["acks"].append(
                                {
                                    "connection": connection,
                                    "stage": stage,
                                    "accepted": len(accepted),
                                    "matched": accepted == desired,
                                    "rejected_count": len(message.get("rejected", [])),
                                }
                            )
                            if accepted != desired or message.get("rejected"):
                                raise ValueError("Unexpected subscription acknowledgement")
                            record("ack", **state["acks"][-1])
                        elif kind == "pong":
                            record(
                                "pong",
                                connection=connection,
                                rtt_seconds=time.monotonic() - pending
                                if pending is not None
                                else None,
                                transport_rtt_seconds=ws.latency,
                            )
                            pending = None
                        elif kind == "message":
                            topic = message.get("topic")
                            if topic not in seen_topics:
                                raise ValueError("Unexpected market topic")
                            counts[topic] += 1
                            record_source_delta(
                                state["clock_deltas"], message["data"].get("timestamp"), received
                            )
                            if topic.rsplit(":", 1)[-1] in core:
                                frames.write(
                                    json.dumps(
                                        {
                                            "received_at": received,
                                            "connection": connection,
                                            "frame": message,
                                        }
                                    )
                                    + "\n"
                                )
                        else:
                            record(
                                "provider_frame",
                                frame_type=kind,
                                code=message.get("error", {}).get("code"),
                            )
                            raise ValueError("Unexpected provider frame")
                record("closed", connection=connection, close_code=ws.close_code)
                if connection == 0:
                    await asyncio.sleep(1)
        await rest_task
        checks = {
            "subscription_stages": len(state["acks"]) >= 5
            and all(a["matched"] for a in state["acks"]),
            "rest_scan_under_one_minute": bool(state["rest_cycles"])
            and all(r["elapsed_seconds"] < 60 and r["prices"] > 0 for r in state["rest_cycles"]),
            "ohlcv_observed": all(previous_bars.get(symbol) for symbol in core),
            "core_market_data_observed": all(
                counts.get(f"{channel}:us:{symbol}", 0)
                for symbol in core
                for channel in ("trade", "orderbook")
            ),
        }
        state.update(
            state="complete",
            at=now(),
            messages=dict(counts),
            result="passed" if all(checks.values()) else "inconclusive",
            checks=checks,
            note="Stream delivery is lossy; OHLCV stability does not prove immutability",
        )
        private_json(output / "http-audit.json", {"requests": audit})
    except (OSError, ValueError, KeyError, TypeError, SafetyError, WebSocketException) as error:
        state.update(state="failed", at=now(), **error_details(error, token))
        record("failed", **error_details(error, token))
        raise SystemExit("Stream validation failed; inspect sanitized artifacts") from None
    finally:
        if rest_task is not None and not rest_task.done():
            rest_task.cancel()
            try:
                await rest_task
            except asyncio.CancelledError:
                pass
        private_json(output / "status.json", state)
        events.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=1800)
    args = parser.parse_args()
    if args.duration < 360:
        parser.error("Use at least six minutes for the six campaign periods")
    asyncio.run(run(args.output.resolve(), args.duration))


if __name__ == "__main__":
    main()
