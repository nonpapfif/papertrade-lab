"""Run a finite market-data-only WebSocket probe with a cached token.

Run with: uv run --extra websocket-probe python -m tools.probe_toss_websocket
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import re
import time
import urllib.request
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from papertrade.api import SYMBOL, ApiError, MarketClient, secret_file
from papertrade.clock import now_utc
from papertrade.clock import utc_iso as now
from papertrade.core import SafetyError, Settings
from papertrade.jsonio import private_json

ROOT = Path(__file__).resolve().parents[1]
URL = "wss://openapi-ws.tossinvest.com/ws/v1"
SPEC = "https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json"
KR_SYMBOL = re.compile(r"[A-Za-z0-9]{6}")


def read_spec() -> bytes:
    with urllib.request.urlopen(SPEC, timeout=15) as response:
        return response.read()


def error_details(error: BaseException, token: str) -> dict[str, Any]:
    details: dict[str, Any] = {"error_type": type(error).__name__}
    if isinstance(error, ApiError):
        details.update(
            api_status=error.status,
            api_code=error.code
            if error.code in ("cooldown", "transport_error", "invalid_response", "authentication_failed")
            else "other",
            api_retry_at=error.retry_at,
        )
    frames = []
    trace = error.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        frames.append(
            {
                "file": Path(code.co_filename).name,
                "function": code.co_name,
                "line": trace.tb_lineno,
            }
        )
        trace = trace.tb_next
    if frames:
        details["failure_frames"] = frames
    for name in ("rcvd", "sent"):
        close = getattr(error, name, None)
        if close is not None:
            reason = str(close.reason)
            if token:
                reason = reason.replace(token, "[redacted]")
            reason = re.sub(r"Bearer\s+\S+", "Bearer [redacted]", reason, flags=re.IGNORECASE)
            details[name] = {"code": int(close.code), "reason": reason[:256]}
    details["rcvd_then_sent"] = getattr(error, "rcvd_then_sent", None)
    response = getattr(error, "response", None)
    if response is not None:
        details["http_status"] = getattr(response, "status_code", None)
    if error.__cause__ is not None:
        details["cause_type"] = type(error.__cause__).__name__
    return details


def record_source_delta(stats: dict[str, Any], source: Any, received: str) -> None:
    try:
        stamp = datetime.fromisoformat(source)
        if stamp.tzinfo is None:
            raise ValueError("Missing source timezone")
        delta = (datetime.fromisoformat(received) - stamp).total_seconds()
    except (ValueError, TypeError):
        stats["invalid_timestamp_count"] = stats.get("invalid_timestamp_count", 0) + 1
        return
    stats["count"] = stats.get("count", 0) + 1
    stats["negative_count"] = stats.get("negative_count", 0) + int(delta < 0)
    stats["min_seconds"] = min(stats.get("min_seconds", delta), delta)
    stats["max_seconds"] = max(stats.get("max_seconds", delta), delta)
    stats["sum_seconds"] = stats.get("sum_seconds", 0) + delta
    stats["interpretation"] = "raw_clock_difference_plus_delivery_delay_not_network_latency"


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
    pattern = KR_SYMBOL if market == "kr" else SYMBOL
    if len(symbols) * 2 > 100 or any(not pattern.fullmatch(s) for s in symbols):
        raise ValueError("Invalid symbols or provider subscription capacity exceeded")
    return [{"type": f"{channel}:{market}", "codes": symbols} for channel in ("trade", "orderbook")]


def inspect_frame(frame: dict[str, Any], expected: set[str]) -> tuple[str, dict[str, Any]]:
    if not isinstance(frame, dict):
        raise TypeError("WebSocket frame must be an object")
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
        error = frame.get("error", {})
        if not isinstance(error, dict):
            raise ValueError("Provider error must be an object")
        return "error", {"type": kind, "code": error.get("code")}
    raise ValueError("Unexpected WebSocket frame type")


async def run(output: Path, market: str, symbols: list[str], duration: int) -> None:
    from websockets.asyncio.client import connect
    from websockets.exceptions import WebSocketException

    os.umask(0o077)
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    started = now_utc()
    monotonic_start = time.monotonic()
    deadline = monotonic_start + duration
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
    token = ""
    counts: Counter[str] = Counter()
    events = (output / "events.jsonl").open("x", buffering=1)

    def event(kind: str, **data: Any) -> None:
        events.write(
            json.dumps(
                {
                    "at": now(),
                    "monotonic_elapsed": time.monotonic() - monotonic_start,
                    "kind": kind,
                    **data,
                }
            )
            + "\n"
        )

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
                "transport_ping_interval_seconds": 20,
                "application_ping_interval_seconds": 60,
            },
        )
        with (output / "frames.jsonl").open("x") as stream:
            for segment in range(2):
                segment_end = min(deadline, time.monotonic() + duration / 2)
                detail: dict[str, Any] = {
                    "segment": segment,
                    "connected_at": now(),
                    "counts": {},
                    "subscribed": [],
                    "source_clock_deltas": {},
                }
                state["segments"].append(detail)
                segment_counts: Counter[str] = Counter()
                async with connect(
                    URL,
                    additional_headers={"Authorization": "Bearer " + token},
                    proxy=None,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=15,
                    close_timeout=5,
                ) as ws:
                    await ws.send(json.dumps(request))
                    event("connected_and_declared", segment=segment)
                    pending_pong: float | None = None

                    async def heartbeat(
                        end: float = segment_end,
                        number: int = segment,
                        metrics: dict[str, Any] = detail,
                    ) -> None:
                        nonlocal pending_pong
                        while time.monotonic() < end:
                            tick = time.monotonic()
                            if pending_pong is not None:
                                event("application_pong_timeout", segment=number)
                                await ws.close(code=1011, reason="Application pong deadline")
                                raise TimeoutError("Pong deadline exceeded")
                            await ws.send("PING")
                            pending_pong = tick
                            metrics["pings_sent"] = metrics.get("pings_sent", 0) + 1
                            event("ping_sent", segment=number)
                            await asyncio.sleep(20)
                            if pending_pong is not None:
                                continue
                            await asyncio.sleep(40)

                    heartbeat_task = asyncio.create_task(heartbeat())
                    last_status = time.monotonic()
                    try:
                        while time.monotonic() < segment_end:
                            if heartbeat_task.done():
                                heartbeat_task.result()
                            tick = time.monotonic()
                            if tick - last_status >= 10:
                                state.update(state="running", at=now(), counts=dict(counts))
                                detail["wall_clock_step_seconds"] = (
                                    now_utc() - started
                                ).total_seconds() - (tick - monotonic_start)
                                private_json(output / "status.json", state)
                                stream.flush()
                                last_status = tick
                            try:
                                raw_frame = await asyncio.wait_for(
                                    ws.recv(),
                                    timeout=min(1, max(0.01, segment_end - time.monotonic())),
                                )
                            except TimeoutError:
                                continue
                            frame = json.loads(raw_frame)
                            key, safe = inspect_frame(frame, expected)
                            counts[key] += 1
                            segment_counts[key] += 1
                            if key == "pong":
                                event(
                                    "pong_received",
                                    segment=segment,
                                    application_rtt_seconds=(time.monotonic() - pending_pong)
                                    if pending_pong is not None
                                    else None,
                                    transport_rtt_seconds=ws.latency,
                                )
                                pending_pong = None
                            elif key == "subscriptions":
                                detail["subscribed"] = safe["subscribed"]
                                if safe["rejected_count"] or set(safe["subscribed"]) != expected:
                                    raise ValueError("Market subscriptions were not fully accepted")
                            elif key == "error":
                                event("provider_error", segment=segment, code=safe.get("code"))
                                raise ValueError("Provider returned a WebSocket error frame")
                            received = now()
                            detail["last_frame_at"] = received
                            if key not in ("pong", "subscriptions"):
                                record_source_delta(
                                    detail["source_clock_deltas"],
                                    safe["data"].get("timestamp"),
                                    received,
                                )
                            stream.write(
                                json.dumps(
                                    {"received_at": received, "segment": segment, "frame": safe}
                                )
                                + "\n"
                            )
                            detail["counts"] = dict(segment_counts)
                            if key == "pong" or key == "subscriptions":
                                stream.flush()
                                state.update(state="running", at=now(), counts=dict(counts))
                                private_json(output / "status.json", state)
                    finally:
                        heartbeat_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await heartbeat_task
                        event(
                            "connection_exiting",
                            segment=segment,
                            close_code=ws.close_code,
                            transport_rtt_seconds=ws.latency,
                        )
                    stream.flush()
                detail["closed_at"] = now()
                event("normal_close", segment=segment, close_code=ws.close_code)
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
        details = error_details(error, token)
        event("failed", **details)
        state.update(state="failed", at=now(), counts=dict(counts), **details)
        private_json(output / "status.json", state)
        raise SystemExit("Probe failed; see sanitized status.json") from None
    finally:
        events.close()


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
