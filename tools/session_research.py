"""Publish optional TradingAgents research independently of a paper ledger.

The feed supplies point-in-time snapshots and material signal identities. ``submit``
only persists/queues work; fixed workers perform inference. Replay consumes recorded
publication times and never runs a model. One instance owns each output directory.
"""

from __future__ import annotations

import asyncio
import bisect
import hashlib
import json
import os
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from papertrade.jsonio import atomic_json, canonical_json, read_json

_REVISION = "9fa1196fffbcfd9de5146d2dd2869df84ab99838"
_ROLES = (
    "market-analyst",
    "sentiment-analyst",
    "news-analyst",
    "fundamentals-analyst",
    "bull-researcher",
    "bear-researcher",
    "research-manager",
    "trader",
    "aggressive-analyst",
    "conservative-analyst",
    "neutral-analyst",
    "portfolio-manager",
)
_RATINGS = {"Buy", "Overweight", "Hold", "Underweight", "Sell", "REVIEW"}
_ENTRY = {"Buy", "Overweight"}
_FAILURE_REASONS = frozenset(
    {
        "cli_unavailable",
        "child_nonzero",
        "timeout",
        "invalid_response",
        "prohibited_action",
    }
)


def _epoch(value: str) -> float:
    stamp = datetime.fromisoformat(value)
    if stamp.utcoffset() is None:
        raise ValueError("Research timestamps need a timezone offset")
    return stamp.timestamp()


def _iso(value: float) -> str:
    return datetime.fromtimestamp(value, UTC).isoformat()


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class Candidate:
    """A qualifying snapshot; signal identifies a material event, never a timer tick."""

    snapshot: dict[str, Any]
    inputs: dict[str, Any] | None = None
    signal: Any = None


class ResearchService(Protocol):
    async def analyze(self, snapshot: dict[str, Any], inputs: Any = None) -> Any: ...

    async def aclose(self) -> None: ...


def toss_service(output_dir: Path, **settings: Any) -> ResearchService:
    """Load the optional installed service; construction does not make model calls."""
    from papertrade.research.service import CodexSettings
    from papertrade.research.service import ResearchService as GraphResearchService

    return GraphResearchService(output_dir, CodexSettings(**settings))


def _candidate(candidate: Candidate, observed_at: str) -> tuple[str, dict[str, Any]]:
    # Detach mutable feed buffers before enqueueing or crossing a thread boundary.
    data = json.loads(
        canonical_json(
            {
                "snapshot": candidate.snapshot,
                "inputs": candidate.inputs,
                "signal": candidate.signal,
            }
        )
    )
    snapshot = data["snapshot"]
    market, symbol = snapshot.get("market"), snapshot.get("symbol")
    if (
        market not in {"KR", "US"}
        or not isinstance(symbol, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.\-]{0,31}", symbol)
    ):
        raise ValueError("Research candidate requires a supported market and symbol")
    cutoff = _epoch(snapshot["as_of"])
    if cutoff > _epoch(observed_at):
        raise ValueError("Research evidence cannot be newer than its observation")

    def validate_times(value: Any) -> None:
        if isinstance(value, dict):
            for name, item in value.items():
                if name.lower() in {
                    "authorization",
                    "authentication",
                    "headers",
                    "access_token",
                    "refresh_token",
                    "client_secret",
                    "api_key",
                    "credentials",
                }:
                    raise ValueError(
                        "Credentials and authentication fields are not research evidence"
                    )
                if name in {"timestamp", "received_at", "published_at", "as_of"}:
                    if _epoch(item) > cutoff:
                        raise ValueError("Research candidate contains future evidence")
                else:
                    validate_times(item)
        elif isinstance(value, list):
            for item in value:
                validate_times(item)

    validate_times(snapshot)
    validate_times(data["inputs"])
    evidence = {"snapshot": snapshot, "inputs": data["inputs"]}
    if candidate.signal is not None:
        identity = {"market": market, "symbol": symbol, "signal": data["signal"]}
    else:
        normalized = json.loads(canonical_json(evidence))
        normalized["snapshot"].pop("as_of")
        for name, value in normalized["snapshot"].items():
            if name.endswith("provenance") and isinstance(value, dict):
                value.pop("received_at", None)
        identity = normalized
    return _digest(identity), {
        **data,
        "observed_at": observed_at,
        "evidence_sha256": _digest(evidence),
    }


def _verify_bundle(directory: Path, request: dict[str, Any]) -> dict[str, Any]:
    manifest = read_json(directory / "manifest.json")
    evidence = read_json(directory / "evidence.json")
    snapshot = evidence["snapshot"]
    inputs = {
        name: (request["inputs"] or {}).get(name, [])
        for name in ("news", "sentiment", "fundamentals")
    }
    statuses = manifest.get("reports", {})
    if (
        not re.fullmatch(r"[a-f0-9]{64}", directory.name)
        or manifest.get("run_id") != directory.name
        or manifest.get("status") != "completed"
        or "error_type" in manifest
        or manifest.get("tradingagents_revision") != _REVISION
        or manifest.get("rating") not in _RATINGS
        or not isinstance(manifest.get("model"), str)
        or not manifest["model"].strip()
        or "Portfolio Manager" not in manifest.get("executed_nodes", [])
        or snapshot != request["snapshot"]
        or evidence.get("inputs") != inputs
        or set(statuses) != set(_ROLES)
    ):
        raise ValueError("Research bundle provenance does not match the submitted evidence")
    skipped = {
        "sentiment-analyst": "sentiment",
        "news-analyst": "news",
        "fundamentals-analyst": "fundamentals",
    }
    hashes = {}
    for filename in (
        "manifest.json",
        "evidence.json",
        "report.md",
        *(f"{role}.md" for role in _ROLES),
    ):
        raw = (directory / filename).read_bytes()
        if not raw.strip():
            raise ValueError("Research bundle contains an empty report")
        hashes[filename] = hashlib.sha256(raw).hexdigest()
    for role, status in statuses.items():
        expected = "skipped" if role in skipped and not inputs[skipped[role]] else "completed"
        if status != expected:
            raise ValueError("Research bundle has incomplete role reports")
    return {
        "bundle": str(directory.resolve()),
        "run_id": directory.name,
        "rating": manifest["rating"],
        "files_sha256": hashes,
        "tradingagents_revision": manifest["tradingagents_revision"],
        "model": manifest["model"],
        "roles": statuses,
    }


def _partial_bundle(directory: Path, request: dict[str, Any]) -> dict[str, Any]:
    manifest = read_json(directory / "manifest.json")
    evidence = read_json(directory / "evidence.json")
    inputs = {
        name: (request["inputs"] or {}).get(name, [])
        for name in ("news", "sentiment", "fundamentals")
    }
    statuses = manifest.get("reports", {})
    if (
        manifest.get("status") != "failed"
        or evidence["snapshot"] != request["snapshot"]
        or evidence.get("inputs") != inputs
        or manifest.get("tradingagents_revision") != _REVISION
        or set(statuses) != set(_ROLES)
        or set(statuses.values()) - {"completed", "skipped", "failed", "not_run"}
    ):
        raise ValueError("Partial report provenance does not match research request")
    hashes = {}
    for name in ("manifest.json", "evidence.json", "report.md", *(f"{role}.md" for role in _ROLES)):
        hashes[name] = hashlib.sha256((directory / name).read_bytes()).hexdigest()
    return {"bundle": str(directory.resolve()), "roles": statuses, "files_sha256": hashes}


def _partial_reports(error: Exception, request: dict[str, Any], root: Path) -> list[dict[str, Any]]:
    reports = []
    for note in getattr(error, "__notes__", ()):
        prefix = "Partial role reports: "
        if not note.startswith(prefix):
            continue
        directory = Path(note[len(prefix) :])
        report = _partial_bundle(directory, request)
        report["bundle"] = os.path.relpath(directory.resolve(), root.resolve())
        reports.append(report)
    return reports


class ResearchReplay:
    """Read persisted per-symbol publications once; never infer historical availability."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = Path(output_dir)
        self.rows = [
            read_json(path) for path in sorted((self.output_dir / "attempts").glob("*.json"))
        ]
        for row in self.rows:
            for partial in row.get("partial_reports", []):
                verified_partial = _partial_bundle(self.output_dir / partial["bundle"], row)
                verified_partial["bundle"] = partial["bundle"]
                if verified_partial != partial:
                    raise ValueError("Published partial research bundle changed")
            if "report" not in row:
                continue
            if _epoch(row["published_at"]) < max(
                _epoch(row["observed_at"]), _epoch(row["snapshot"]["as_of"])
            ):
                raise ValueError("Research publication was backdated")
            if row["status"] == "completed" and _epoch(row["published_at"]) >= _epoch(
                row["expires_at"]
            ):
                raise ValueError("Research completion was published after its horizon")
            verified = _verify_bundle(self.output_dir / row["report"]["bundle"], row)
            verified["bundle"] = row["report"]["bundle"]
            if verified != row["report"]:
                raise ValueError("Published research bundle changed")
        self.rows.sort(key=lambda row: row.get("publication_index", 0))
        self._reindex_selection()

    def _reindex_selection(self) -> None:
        self._selection_rows: dict[str, list[tuple[float, float, str, str]]] = {}
        self._selection_events: dict[str, list[float]] = {}
        self._selection_cache: dict[tuple[str, int], frozenset[str]] = {}
        for row in self.rows:
            if row["status"] != "completed":
                continue
            market = row["snapshot"]["market"]
            published, expires = _epoch(row["published_at"]), _epoch(row["expires_at"])
            self._selection_rows.setdefault(market, []).append(
                (published, expires, row["snapshot"]["symbol"], row["report"]["rating"]),
            )
            self._selection_events.setdefault(market, []).extend((published, expires))
        for market, events in self._selection_events.items():
            self._selection_events[market] = sorted(set(events))

    def publications(self) -> list[dict[str, Any]]:
        ordered = sorted(
            self.rows,
            key=lambda row: (
                row.get("publication_index", 0),
                row.get("submission_index", 0),
                row["key"],
            ),
        )
        return json.loads(canonical_json(ordered))

    def at(self, tick: float, market: str = "US") -> frozenset[str]:
        interval = bisect.bisect_right(self._selection_events.get(market, []), tick)
        key = (market, interval)
        cached = self._selection_cache.get(key)
        if cached is not None:
            return cached
        latest: dict[str, tuple[float, str]] = {}
        for published, expires, symbol, rating in self._selection_rows.get(market, []):
            if published <= tick:
                latest[symbol] = (expires, rating)
        admitted = frozenset(
            symbol
            for symbol, (expires, rating) in latest.items()
            if rating in _ENTRY and tick < expires
        )
        self._selection_cache[key] = admitted
        return admitted


class SessionResearch(ResearchReplay):
    """A feed-side queue with durable deduplication and bounded graph concurrency.

    ``signal`` is an explicit qualifying event identity. Reusing it suppresses another
    model call even if a rolling snapshot changes. Omit it to deduplicate by evidence.
    Failed and interrupted attempts remain recorded; a new material signal permits
    another attempt. Publication is the actual completion clock, never ``as_of``.
    """

    def __init__(
        self,
        output_dir: Path,
        service: ResearchService,
        *,
        expires_at: str,
        max_concurrency: int = 2,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("Research concurrency must be positive")
        super().__init__(output_dir)
        self.service = service
        self.expires_at = expires_at
        _epoch(expires_at)
        self.clock = clock
        self._workers_count = max_concurrency
        self._workers: list[asyncio.Task[None]] = []
        self._queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._queued: set[tuple[str, str]] = set()
        self._pending: dict[tuple[str, str], dict[str, Any]] = {}
        self._inflight: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
        self._lock = asyncio.Lock()
        self._closed = False
        self._seen = {row["key"] for row in self.rows}
        self._submission_index = max(
            (row.get("submission_index", 0) for row in self.rows), default=0
        )
        self._publication_index = max(
            (row.get("publication_index", 0) for row in self.rows), default=0
        )
        self._last_publication_epoch = max(
            (_epoch(row["published_at"]) for row in self.rows if "published_at" in row), default=0
        )
        self._latest: dict[tuple[str, str], dict[str, Any]] = {}
        for row in sorted(self.rows, key=lambda item: item.get("submission_index", 0)):
            if row["status"] != "superseded":
                self._latest[(row["snapshot"]["market"], row["snapshot"]["symbol"])] = row
        self.output_dir.mkdir(parents=True, exist_ok=True)
        (self.output_dir / "attempts").mkdir(exist_ok=True)
        # Unknown in-flight requests must never silently be replayed after restart.
        for row in self.rows:
            if row["status"] == "pending":
                row.update(status="interrupted", error_type="InterruptedResearch")
                atomic_json(self.output_dir / "attempts" / f"{row['key']}.json", row)

    async def submit(self, candidates: Iterable[Candidate], observed_at: str) -> None:
        """Persist and enqueue qualifying events without awaiting model inference."""
        if self._closed:
            raise RuntimeError("Research adapter is closed")
        if self.clock() >= _epoch(self.expires_at):
            return
        if _epoch(observed_at) > self.clock():
            raise ValueError("Observed time is ahead of the research publication clock")
        for candidate in candidates:
            key, request = _candidate(candidate, observed_at)
            async with self._lock:
                if self._closed:
                    raise RuntimeError("Research adapter is closed")
                if key in self._seen:
                    continue
                identity = (request["snapshot"]["market"], request["snapshot"]["symbol"])
                previous = self._latest.get(identity)
                stale = previous is not None and _epoch(request["snapshot"]["as_of"]) < _epoch(
                    previous["snapshot"]["as_of"]
                )
                row = {
                    **request,
                    "key": key,
                    "status": "superseded" if stale else "pending",
                    "expires_at": self.expires_at,
                    "submission_index": self._submission_index + 1,
                }
                await asyncio.to_thread(
                    atomic_json,
                    self.output_dir / "attempts" / f"{key}.json",
                    row,
                )
                self._submission_index += 1
                self._seen.add(key)
                self.rows.append(row)
                if stale:
                    continue
                queued = self._pending.get(identity)
                if queued is not None and queued["status"] == "pending":
                    superseded = {**queued, "status": "superseded"}
                    await asyncio.to_thread(
                        atomic_json,
                        self.output_dir / "attempts" / f"{queued['key']}.json",
                        superseded,
                    )
                    queued.update(superseded)
                self._latest[identity] = row
                self._pending[identity] = row
                if identity not in self._queued:
                    self._queued.add(identity)
                    self._queue.put_nowait(identity)
                if not self._workers:
                    self._workers = [
                        asyncio.create_task(self._worker()) for _ in range(self._workers_count)
                    ]

    async def invalidate(self, market: str, symbol: str) -> None:
        """Discard pending generations on a family loss; keep historic publications.

        An already-running service call may finish, but its result cannot be admitted.
        The optional service owns model execution; this method never starts a call.
        """
        async with self._lock:
            identity = (market, symbol)
            targets = list(self._inflight.get(identity, {}).values())
            queued = self._pending.get(identity)
            if queued is not None:
                targets.append(queued)
            for row in targets:
                if row["status"] != "pending":
                    continue
                invalidated = {**row, "status": "invalidated", "invalidated_at": _iso(self.clock())}
                atomic_json(self.output_dir / "attempts" / f"{row['key']}.json", invalidated)
                row.update(invalidated)

    async def _worker(self) -> None:
        while True:
            identity = await self._queue.get()
            row = None
            try:
                async with self._lock:
                    self._queued.discard(identity)
                    row = self._pending.pop(identity, None)
                    if row is None:
                        continue
                    self._inflight.setdefault(identity, {})[row["key"]] = row
                update: dict[str, Any]
                try:
                    if row["status"] != "pending":
                        continue
                    if self._latest[identity]["key"] != row["key"]:
                        await self._finish(row, {"status": "superseded"})
                        continue
                    if self.clock() >= _epoch(row["expires_at"]):
                        await self._finish(row, {"status": "expired"})
                        continue
                    result = await self.service.analyze(row["snapshot"], row["inputs"])
                    report = await asyncio.to_thread(_verify_bundle, Path(result.directory), row)
                    report["bundle"] = os.path.relpath(
                        Path(result.directory).resolve(),
                        self.output_dir.resolve(),
                    )
                    update = {"status": "completed", "report": report}
                except Exception as error:  # noqa: BLE001 - Persist each service failure.
                    update = {"status": "failed", "error_type": type(error).__name__}
                    reason = getattr(error, "reason", None)
                    if isinstance(reason, str) and reason in _FAILURE_REASONS:
                        update["failure_reason"] = reason
                    exit_code = getattr(error, "exit_code", None)
                    if (
                        reason == "child_nonzero"
                        and type(exit_code) is int
                        and -255 <= exit_code <= 255
                    ):
                        update["exit_code"] = exit_code
                    try:
                        update["partial_reports"] = await asyncio.to_thread(
                            _partial_reports,
                            error,
                            row,
                            self.output_dir,
                        )
                        update["failed_roles"] = sorted(
                            {
                                role
                                for report in update["partial_reports"]
                                for role, status in report["roles"].items()
                                if status == "failed"
                            }
                        )
                    except (OSError, ValueError, KeyError, TypeError) as partial_error:
                        update["partial_report_error_type"] = type(partial_error).__name__
                if self._latest[identity]["key"] != row["key"]:
                    update = {**update, "status": "superseded"}
                await self._finish(row, update)
            finally:
                if row is not None:
                    active = self._inflight[identity]
                    active.pop(row["key"], None)
                    if not active:
                        self._inflight.pop(identity)
                self._queue.task_done()

    async def _finish(self, row: dict[str, Any], update: dict[str, Any]) -> None:
        async with self._lock:
            identity = (row["snapshot"]["market"], row["snapshot"]["symbol"])
            if row["status"] == "interrupted":
                update = {**update, "status": "interrupted"}
                update.pop("report", None)
            elif row["status"] == "invalidated":
                update = {**update, "status": "invalidated"}
            elif self._latest[identity]["key"] != row["key"]:
                update = {**update, "status": "superseded"}
            published = self.clock()
            if published >= _epoch(row["expires_at"]) and row["status"] not in {
                "invalidated",
                "interrupted",
            }:
                update = {**update, "status": "expired"}
            previous = self._last_publication_epoch
            if published < max(
                _epoch(row["observed_at"]), _epoch(row["snapshot"]["as_of"]), previous
            ):
                update = {"status": "failed", "error_type": "PublicationClockError"}
            finished = {
                **row,
                **update,
                "published_at": _iso(published),
                "publication_index": self._publication_index + 1,
            }
            # This infrequent commit cannot yield between publication time and index update.
            # Feed consumers on this loop therefore observe the same causal cut as replay.
            atomic_json(self.output_dir / "attempts" / f"{row['key']}.json", finished)
            row.update(finished)
            self._publication_index += 1
            self._last_publication_epoch = max(previous, published)
            self.rows.sort(key=lambda item: item.get("publication_index", 0))
            if row["status"] == "completed":
                self._reindex_selection()

    def selection_at(self, tick: float, market: str = "US") -> frozenset[str]:
        return self.at(tick, market)

    def metadata(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        return {
            "adapter": "tradingagents_research_publications",
            "expires_at": self.expires_at,
            "max_concurrency": self._workers_count,
            "attempts": counts,
            "errors": [
                {
                    "key": row["key"],
                    "symbol": row["snapshot"]["symbol"],
                    "error_type": row["error_type"],
                    **{
                        name: row[name]
                        for name in ("failure_reason", "exit_code", "failed_roles")
                        if name in row
                    },
                }
                for row in self.rows
                if "error_type" in row
            ],
        }

    async def drain(self) -> None:
        joined = asyncio.create_task(self._queue.join())
        try:
            done, _ = await asyncio.wait(
                [joined, *self._workers], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
        finally:
            if not joined.done():
                joined.cancel()
                await asyncio.gather(joined, return_exceptions=True)

    async def aclose(self) -> None:
        """Stop new work, retain interrupted role evidence, and join the closed service."""
        async with self._lock:
            self._closed = True
            for row in self.rows:
                if row["status"] == "pending":
                    interrupted = {
                        **row,
                        "status": "interrupted",
                        "error_type": "InterruptedResearch",
                    }
                    atomic_json(self.output_dir / "attempts" / f"{row['key']}.json", interrupted)
                    row.update(interrupted)
        try:
            # Closing the service stops its active child. Let workers collect its partial reports.
            await self.service.aclose()
            await self.drain()
        finally:
            for worker in self._workers:
                worker.cancel()
            worker_results = await asyncio.gather(*self._workers, return_exceptions=True)
        for result in worker_results:
            if isinstance(result, Exception):
                raise result
