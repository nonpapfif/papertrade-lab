"""Read-only Codex metadata and local model routing without model inference."""

from __future__ import annotations

import json
import os
import selectors
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from typing import Any

from .clock import now_utc
from .core import SEOUL, SafetyError, decimal
from .jsonio import paper_json as encode


def quota_report(payload: dict[str, Any]) -> dict[str, Any]:
    buckets = payload.get("rateLimitsByLimitId")
    if not buckets:
        legacy = payload.get("rateLimits")
        buckets = {legacy.get("limitId") or "codex": legacy} if legacy else {}
    windows = []
    for key, bucket in buckets.items():
        if not isinstance(bucket, dict):
            continue
        for name in ("primary", "secondary"):
            window = bucket.get(name)
            if not isinstance(window, dict) or not window:
                continue
            used = window.get("usedPercent")
            reset = window.get("resetsAt")
            minutes = window.get("windowDurationMins")
            used = used if isinstance(used, (int, float)) and not isinstance(used, bool) else None
            try:
                reset_at = (
                    datetime.fromtimestamp(reset, UTC) if isinstance(reset, (int, float)) else None
                )
            except (ValueError, OverflowError, OSError):
                reset_at = None
            windows.append(
                {
                    "bucket": key,
                    "window": name,
                    "used_percent": used,
                    "remaining_percent": max(0, min(100, 100 - used)) if used is not None else None,
                    "window_minutes": minutes,
                    "resets_at": reset_at.isoformat() if reset_at else None,
                    "resets_at_seoul": reset_at.astimezone(SEOUL).isoformat() if reset_at else None,
                }
            )
    return {
        "status": "available" if windows else "unavailable",
        "observed_at": now_utc().isoformat(),
        "scope": "shared_codex_account",
        "exact_remaining_tokens": None,
        "ordinary_usage_allowed": payload.get("ordinaryUsageAllowed"),
        "windows": windows,
    }


def read_metadata(command: list[str], environment: dict[str, str]) -> dict[str, Any]:
    """Only initialize, read quota, and list models on an isolated stdio server."""
    with tempfile.TemporaryDirectory(prefix="papertrade-metadata-") as temporary:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            cwd=temporary,
        )
        if process.stdin is None or process.stdout is None:
            process.kill()
            process.wait()
            raise SafetyError("Codex metadata pipes are unavailable")
        source, target = process.stdout, process.stdin
        selector = selectors.DefaultSelector()
        selector.register(source, selectors.EVENT_READ)
        buffer = b""

        def send(value: dict[str, Any]) -> None:
            target.write((encode(value) + "\n").encode())
            target.flush()

        def request(identifier: int, method: str, params: dict[str, Any]) -> dict[str, Any]:
            nonlocal buffer
            if method not in ("initialize", "account/rateLimits/read", "model/list"):
                raise SafetyError("Only read-only Codex metadata requests are permitted")
            send({"id": identifier, "method": method, "params": params})
            deadline = time.monotonic() + 20
            while True:
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    response = json.loads(line)
                    if response.get("id") == identifier:
                        if "error" in response:
                            raise SafetyError("Codex metadata request rejected: " + method)
                        if not isinstance(response.get("result"), dict):
                            raise SafetyError("Invalid Codex metadata response")
                        return response["result"]
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise SafetyError("Codex metadata read timed out; quota is unavailable")
                chunk = os.read(source.fileno(), 65536)
                if not chunk:
                    raise SafetyError("Codex metadata connection ended")
                buffer += chunk

        try:
            request(
                0,
                "initialize",
                {
                    "clientInfo": {
                        "name": "papertrade_lab",
                        "title": "Papertrade Lab",
                        "version": "0.1.0",
                    }
                },
            )
            send({"method": "initialized", "params": {}})
            try:
                quota = quota_report(request(1, "account/rateLimits/read", {}))
            except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
                quota = {
                    "status": "unavailable",
                    "observed_at": now_utc().isoformat(),
                    "exact_remaining_tokens": None,
                    "reason": type(error).__name__,
                    "windows": [],
                }
            models = []
            seen_cursors = set()
            cursor = None
            identifier = 2
            while True:
                params: dict[str, Any] = {"limit": 100, "includeHidden": False}
                if cursor:
                    params["cursor"] = cursor
                result = request(identifier, "model/list", params)
                models.extend(result["data"])
                cursor = result.get("nextCursor")
                if not cursor:
                    break
                if cursor in seen_cursors:
                    raise SafetyError("Repeated model catalog cursor")
                seen_cursors.add(cursor)
                identifier += 1
            return {
                "quota": quota,
                "models": [
                    {
                        "model": item["model"],
                        "display_name": item.get("displayName"),
                        "description": item.get("description"),
                        "default_effort": item.get("defaultReasoningEffort"),
                        "efforts": [
                            row["reasoningEffort"] for row in item["supportedReasoningEfforts"]
                        ],
                    }
                    for item in models
                ],
                "model_invoked": False,
            }
        finally:
            selector.close()
            target.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            source.close()


def select_model(
    candidates: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    smoke: bool,
    policy: str,
    saved: dict[str, Any],
    model: str | None = None,
    effort: str | None = None,
) -> dict[str, str]:
    if model:
        reason = "explicit_test_override"
    elif policy == "current":
        model = saved.get("model")
        effort = effort or saved.get("model_reasoning_effort")
        reason = "existing_user_configuration"
    elif smoke:
        model, effort, reason = "gpt-6-luna", effort or "low", "synthetic_connection_test"
    else:
        volatile = any(
            decimal(row["atr"]) / decimal(row["entry_price"]) >= decimal("0.03")
            for row in candidates
        )
        returns = sorted(decimal(row["relative_return"]) for row in candidates)
        close_choices = len(returns) > 1 and returns[-1] - returns[-2] < decimal("0.002")
        if volatile or close_choices:
            model, effort = "gpt-6-astra", effort or "high"
            reason = "high_volatility" if volatile else "closely_ranked_candidates"
        else:
            model, effort, reason = "gpt-6.1-sol", effort or "medium", "standard_candidate_review"
    entry = next((row for row in catalog if row["model"] == model), None)
    if entry is None:
        raise SafetyError("Selected model is absent from the current Codex catalog")
    effort = effort or entry["default_effort"]
    if effort not in entry["efforts"]:
        raise SafetyError("Selected reasoning effort is absent from the current Codex catalog")
    return {"model": str(model), "effort": str(effort), "reason": reason}
