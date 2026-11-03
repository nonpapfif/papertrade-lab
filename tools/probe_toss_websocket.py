"""Run a finite market-data-only WebSocket probe with a cached token.

Run with: uv run --extra websocket-probe python -m tools.probe_toss_websocket
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import time
import urllib.request
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from papertrade.api import SYMBOL, MarketClient, private_json, secret_file
from papertrade.core import SafetyError, Settings

ROOT = Path(__file__).resolve().parents[1]
URL = "wss://openapi-ws.tossinvest.com/ws/v1"
SPEC = "https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json"


def now() -> str:
    return datetime.now(UTC).isoformat()


def read_spec() -> bytes:
    with urllib.request.urlopen(SPEC, timeout=15) as response:
        return response.read()


class CachedTokenClient(MarketClient):
    """Never issue, invalidate, or refresh a credential during this probe."""

    def access_token(self) -> str:
        payload = json.loads(secret_file(self.settings.secrets_dir / "TOSS_ACCESS_TOKEN.json"))
        token = payload.get("access_token")
        if not isinstance(token, str) or not token or payload.get("expires_at", 0) <= time.time():
            raise SafetyError("A valid existing cached token is required")
        return token

    def _invalidate_token(self) -> None:
        raise SafetyError("Cached token rejected; probe will not refresh credentials")


def declaration(market: str, symbols: list[str]) -> list[dict[str, Any]]:
    if market not in ("kr", "us") or not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("Choose unique market-data symbols")
    if len(symbols) * 2 > 100 or any(not SYMBOL.fullmatch(s) for s in symbols):
        raise ValueError("Invalid symbols or provider subscription capacity exceeded")
    return [{"type": f"{channel}:{market}", "codes": symbols} for channel in ("trade", "orderbook")]


def inspect_frame(frame: dict[str, Any], expected: set[str]) -> tuple[str, dict[str, Any]]:
    kind = frame.get("type")
    if kind == "message":
        topic = frame.get("topic")
        if topic not in expected or not isinstance(frame.get("data"), dict):
            raise ValueError("Unexpected market-data topic or payload")
        return str(topic), {"type": kind, "topic": topic, "data": frame["data"]}
    if kind == "subscriptions":
        subscribed = frame.get("subscribed", [])
        if not isinstance(subscribed, list) or any(s not in expected for s in subscribed):
            raise ValueError("Unexpected subscription acknowledgement")
        rejected = frame.get("rejected", [])
        if not isinstance(rejected, list):
            raise ValueError("Invalid subscription rejection list")
        return "subscriptions", {
            "type": kind,
            "subscribed": subscribed,
            "rejected_count": len(rejected),
        }
    if kind == "pong":
        return "pong", {"type": kind}
    if kind == "error":
        return "error", {"type": kind, "code": frame.get("error", {}).get("code")}
    raise ValueError("Unexpected WebSocket frame type")


async def run(output: Path, market: str, symbols: list[str], duration: int) -> None:
    from websockets.asyncio.client import connect
    from websockets.exceptions import WebSocketException

    os.umask(0o077)
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    started = datetime.now(UTC)
    deadline = time.monotonic() + duration
    state: dict[str, Any] = {
        "state": "preflight",
        "started_at": started.isoformat(),
        "at": now(),
        "expected_end": (started + timedelta(seconds=duration + 45)).isoformat(),
        "pid": os.getpid(),
        "ai_calls": 0,
        "real_orders": False,
        "connections_completed": 0,
        "counts": {},
        "segments": [],
        "market": market,
        "symbols": symbols,
    }
    private_json(output / "status.json", state)
    try:
        client = CachedTokenClient(Settings.load(ROOT))
        token = client.access_token()
        request = declaration(market, symbols)
        expected = {
            f"{channel}:{market}:{s}" for channel in ("trade", "orderbook") for s in symbols
        }
        raw = await asyncio.to_thread(read_spec)
        spec = json.loads(raw)
        (output / "asyncapi.json").write_bytes(raw)
        private_json(
            output / "manifest.json",
            {
                "started_at": started.isoformat(),
                "duration_seconds": duration,
                "declaration": request,
                "spec_sha256": hashlib.sha256(raw).hexdigest(),
                "spec_version": spec["info"]["version"],
                "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "calendar": client.calendar(
                    market.upper(), started.astimezone().date().isoformat()
                ),
                "stocks": client.stocks(symbols),
                "prices": client.prices(symbols),
                "credential_mode": "existing_cache_only_no_issuance_or_invalidation",
                "real_orders": False,
                "ai_calls": 0,
                "scope": "T0, T1 and one controlled reconnect; not strategy or fill validation",
            },
        )
        counts: Counter[str] = Counter()
        with (output / "frames.jsonl").open("x") as stream:
            for segment in range(2):
                segment_end = min(deadline, time.monotonic() + duration / 2)
                detail: dict[str, Any] = {
                    "segment": segment,
                    "connected_at": now(),
                    "counts": {},
                    "subscribed": [],
                }
                state["segments"].append(detail)
                segment_counts: Counter[str] = Counter()
                async with connect(
                    URL,
                    additional_headers={"Authorization": "Bearer " + token},
                    proxy=None,
                    ping_interval=None,
                    open_timeout=15,
                    close_timeout=5,
                ) as ws:
                    await ws.send(json.dumps(request))
                    next_ping = time.monotonic()
                    pending_pong: float | None = None
                    while time.monotonic() < segment_end:
                        tick = time.monotonic()
                        if pending_pong is not None and tick - pending_pong > 20:
                            raise TimeoutError("Pong deadline exceeded")
                        if tick >= next_ping:
                            await ws.send("PING")
                            pending_pong = tick
                            next_ping = tick + 60
                        try:
                            raw_frame = await asyncio.wait_for(
                                ws.recv(), timeout=min(1, max(0.01, segment_end - time.monotonic()))
                            )
                        except TimeoutError:
                            continue
                        frame = json.loads(raw_frame)
                        key, safe = inspect_frame(frame, expected)
                        counts[key] += 1
                        segment_counts[key] += 1
                        if key == "pong":
                            pending_pong = None
                        elif key == "subscriptions":
                            detail["subscribed"] = safe["subscribed"]
                            if safe["rejected_count"] or set(safe["subscribed"]) != expected:
                                raise ValueError("Market subscriptions were not fully accepted")
                        elif key == "error":
                            raise ValueError("Provider returned a WebSocket error frame")
                        stream.write(
                            json.dumps({"received_at": now(), "segment": segment, "frame": safe})
                            + "\n"
                        )
                        detail["counts"] = dict(segment_counts)
                        if key == "pong" or key == "subscriptions":
                            stream.flush()
                            state.update(state="running", at=now(), counts=dict(counts))
                            private_json(output / "status.json", state)
                    stream.flush()
                detail["closed_at"] = now()
                state["connections_completed"] += 1
                if segment == 0:
                    state.update(state="controlled_disconnect", at=now(), counts=dict(counts))
                    private_json(output / "status.json", state)
                    await asyncio.sleep(1)
        all_seen = all(
            all(part["counts"].get(t, 0) for t in expected) for part in state["segments"]
        )
        state.update(
            state="complete",
            at=now(),
            counts=dict(counts),
            result="passed" if all_seen else "inconclusive",
            market_messages_seen_on_every_topic_per_connection=all_seen,
            keepalive_passed=all(p["counts"].get("pong", 0) >= 2 for p in state["segments"]),
            normal_close_completed=True,
        )
        if not state["keepalive_passed"]:
            state["result"] = "inconclusive"
        private_json(output / "status.json", state)
    except (OSError, ValueError, KeyError, TypeError, SafetyError, WebSocketException) as error:
        state.update(state="failed", at=now(), error_type=type(error).__name__)
        private_json(output / "status.json", state)
        raise SystemExit("Probe failed; see sanitized status.json") from None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--market", choices=("kr", "us"), default="kr")
    parser.add_argument("--symbols", nargs="+", default=["005930", "000660"])
    parser.add_argument("--duration", type=int, default=600)
    args = parser.parse_args()
    if args.duration < 240 or not math.isfinite(args.duration):
        parser.error("Use at least 240 seconds to exercise keepalive on both connections")
    declaration(args.market, args.symbols)
    asyncio.run(run(args.output.resolve(), args.market, args.symbols, args.duration))


if __name__ == "__main__":
    main()
