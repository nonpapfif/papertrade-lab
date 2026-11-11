"""Receive Toss trade and orderbook frames for a finite paper campaign."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any, Protocol, TextIO

from papertrade.api import ApiError
from tools.probe_toss_websocket import URL, declaration, error_details

if TYPE_CHECKING:
    from tools.market_data_gateway import MarketDataGateway
    from tools.runtime_audit import RuntimeAudit


class StreamTarget(Protocol):
    @property
    def desired(self) -> list[str]: ...

    def protected(self) -> set[str]: ...

    def publish(self, kind: str, payload: Any, *, received: str | None = None) -> None: ...


def websocket_retry_at(headers: list[str], now_epoch: float) -> float:
    retry_at = now_epoch + 2
    for header in headers:
        value = header.strip()
        try:
            if value.isascii() and value.isdecimal():
                candidate = now_epoch + float(value)
            else:
                stamp = parsedate_to_datetime(value)
                if stamp.utcoffset() is None:
                    continue
                candidate = stamp.timestamp()
        except (ValueError, TypeError, OSError, OverflowError):
            continue
        if math.isfinite(candidate):
            retry_at = max(retry_at, candidate)
    return retry_at


async def receive_market_stream(
    target: StreamTarget,
    gateway: MarketDataGateway,
    *,
    market: str,
    token: str,
    deadline: float,
    started: float,
    duration: float,
    adaptive: bool,
    raw: TextIO,
    stats: dict[str, Any],
    record: Callable[..., None],
    resync: asyncio.Event,
    audit: RuntimeAudit,
    clock: Callable[[], str],
) -> None:
    """Maintain the connection and forward valid orderbooks until the observation cutoff."""
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosed, InvalidStatus

    stream_market = market.lower()

    def rejected_frame(frame: str | bytes, received: str, code: str) -> ValueError:
        data = (
            frame.encode("utf-8", errors="surrogatepass")
            if isinstance(frame, str)
            else frame
        )
        raw.write(
            json.dumps(
                {
                    "kind": "websocket_frame_failure",
                    "received_at": received,
                    "frame_type": "text" if isinstance(frame, str) else "binary",
                    "byte_length": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "code": code,
                }
            )
            + "\n"
        )
        raw.flush()
        error = ValueError(f"Invalid WebSocket frame: {code}")
        audit.report_exception(
            "provider_adapter", code, error, {"market": market},
        )
        return error

    injected = False
    while time.monotonic() < deadline:
        if not target.desired:
            await asyncio.sleep(0.1)
            continue
        transport = True
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
                transport = False
                stats["connections"] += 1
                record("connected", connection=stats["connections"])
                for code in ("websocket_disconnected", "websocket_handshake_failed"):
                    audit.record(
                        "provider_adapter", code, {"market": market}, state="resolved",
                    )
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
                    if not target.desired:
                        transport = True
                        await ws.close(code=1000, reason="No candidates or holdings")
                        break
                    if not adaptive and not injected and tick - started >= duration / 2:
                        injected = True
                        stats["faults"].append(
                            {"kind": "controlled_disconnect", "at": clock()}
                        )
                        transport = True
                        await ws.close(code=1000, reason="Controlled integration test")
                        transport = False
                        record("controlled_disconnect")
                        break
                    if (
                        target.desired != selected
                        and not ack_pending
                        and tick - declaration_at >= 1
                    ):
                        selected = list(target.desired)
                        expected = {
                            f"{c}:{stream_market}:{s}"
                            for c in ("trade", "orderbook")
                            for s in selected
                        }
                        subscription = json.dumps(declaration(stream_market, selected))
                        transport = True
                        await ws.send(subscription)
                        declaration_at, ack_pending = tick, True
                    transport = True
                    if ack_pending and tick - declaration_at > 20:
                        raise TimeoutError("Subscription acknowledgement timeout")
                    if ping_at is not None and tick - ping_at > 20:
                        raise TimeoutError("Application pong timeout")
                    if tick >= next_ping:
                        await ws.send("PING")
                        ping_at, next_ping = tick, tick + 60
                    try:
                        wire = await asyncio.wait_for(ws.recv(), 0.25)
                    except TimeoutError:
                        transport = False
                        continue
                    transport = False
                    received = clock()
                    try:
                        message = json.loads(wire)
                    except (ValueError, RecursionError):
                        raise rejected_frame(wire, received, "invalid_json") from None
                    if not isinstance(message, dict):
                        raise rejected_frame(wire, received, "non_object")
                    kind = message.get("type")
                    if kind == "subscriptions":
                        subscribed = message.get("subscribed", [])
                        if not isinstance(subscribed, list) or any(
                            not isinstance(topic, str) for topic in subscribed
                        ):
                            raise rejected_frame(wire, received, "invalid_subscription")
                        actual = set(subscribed)
                        if actual != expected or message.get("rejected"):
                            raise rejected_frame(wire, received, "subscription_mismatch")
                        accepted, ack_pending = actual, False
                        stats["acks"].append(
                            {
                                "at": clock(),
                                "connection": stats["connections"],
                                "symbols": selected,
                                "protected": sorted(target.protected()),
                            }
                        )
                        record("subscription_ack", symbols=selected)
                    elif kind == "pong":
                        ping_at = None
                    elif kind == "message":
                        # A rejected market message leaves the subscription state intact.
                        topic = message.get("topic")
                        if not isinstance(topic, str) or topic not in accepted | expected:
                            rejected_frame(wire, received, "unexpected_topic")
                            continue
                        frame: dict[str, Any] = {"received_at": received, "frame": message}
                        stats["market_messages"] = stats.get("market_messages", 0) + 1
                        if topic.startswith(f"orderbook:{stream_market}:"):
                            symbol = topic.rsplit(":", 1)[1]
                            data = message.get("data")
                            if not isinstance(data, dict):
                                rejected_frame(wire, received, "invalid_orderbook_data")
                                continue
                            try:
                                observation = gateway.ingest_book(
                                    market, symbol, data,
                                    received_time=received,
                                )
                            except ApiError as error:
                                record("stream_symbol_failure", symbol=symbol, code=error.code)
                            else:
                                frame["published_sequence"] = observation.published_sequence
                                frame["source_identity"] = observation.source_identity
                                target.publish(
                                    "book", (symbol, observation.payload), received=received
                                )
                        raw.write(json.dumps(frame) + "\n")
                    else:
                        raise rejected_frame(wire, received, "unknown_type")
                transport = True
        except InvalidStatus as error:
            status = error.response.status_code
            audit.report_exception(
                "provider_adapter", "websocket_handshake_failed", error,
                {"market": market, "http_status": status},
                severity="warning" if status in (429, 503) else "error",
            )
            if not adaptive or status not in (429, 503):
                raise
            record("connection_failure", **error_details(error, token))
            raise ApiError(
                "WebSocket handshake temporarily unavailable",
                status=status,
                code="cooldown",
                retry_at=websocket_retry_at(
                    error.response.headers.get_all("Retry-After"), time.time()
                ),
            ) from error
        except (ConnectionClosed, OSError) as error:
            # Storage, publication and evidence failures retain their local identity.
            if not transport:
                raise
            audit.report_exception(
                "provider_adapter", "websocket_disconnected", error,
                {"market": market}, severity="warning",
            )
            record("connection_failure", **error_details(error, token))
        if time.monotonic() < deadline:
            record("reconnect_backoff", seconds=2)
            await asyncio.sleep(2)
