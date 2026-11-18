"""Run or replay one provider-calendar session with shared data and independent paper accounts."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import importlib.util
import math
import os
import sqlite3
import sys
import time
import traceback
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from papertrade.api import ApiError, MarketClient
from papertrade.core import SafetyError, Settings
from papertrade.jsonio import atomic_json, canonical_json, read_json
from tools.autotest_jobs import ROOT, poll_job, start_job
from tools.paper_campaign.evidence import BOOK_COVERAGE_SECONDS, MAX_BOOK_GAP_SECONDS
from tools.paper_market import KR, US
from tools.session_accounts import SessionAccounts
from tools.session_feed import JournalFeed, feed_event
from tools.session_projection import ProjectionClock
from tools.session_research import Candidate, ResearchReplay, SessionResearch, toss_service
from tools.source_identity import command, tree_digest


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat()


class Comparison:
    """Project committed market observations into independent accounts and research requests."""

    def __init__(
        self,
        plan: dict[str, Any],
        journal: Path,
        research: ResearchReplay,
    ) -> None:
        self.market = US if plan["market"] == "US" else KR
        start, end = self.market.session(plan["calendar"], plan["day"])
        self.start, self.end = start.timestamp(), end.timestamp()
        self.feed = JournalFeed(journal, self.market.code, self.end, plan["capital"])
        self.accounts = SessionAccounts(
            market=self.market.code,
            capital=plan["capital"],
            opened_at=start.isoformat(),
            closes_at=end.isoformat(),
            selection_at=lambda tick: research.at(tick, self.market.code),
        )
        self.research = research
        self.signals: dict[str, str] = {}
        self.bars: dict[str, list[dict[str, Any]]] = {}
        self.last_time = 0.0
        self.last_book = self.start
        self.max_gap = 0.0
        self.covered_until = self.start
        self.covered_seconds = 0.0
        self.scan_end = self.start
        self.scan_last = self.start
        self.scan_gap = 0.0
        self.scan_seconds = 0.0
        self.feature_cut = min(self.end, self.start + 16 * 60)
        self.feature_fresh: dict[str, float] = {}
        self.feature_observed = 0.0
        self.feature_possible = 0.0
        self.recoveries = 0
        self.calendar_verified = False

    def feature_interval(self, until: float) -> tuple[float, float]:
        stop = min(until, self.end)
        duration = max(0.0, stop - self.feature_cut)
        covered = sum(
            max(0.0, min(stop, self.feature_fresh.get(symbol, 0)) - self.feature_cut)
            for symbol in self.accounts.eligible
        )
        return covered, duration * len(self.accounts.eligible)

    async def apply(self, entry: dict[str, Any], *, live: bool, available_at: float = 0.0) -> None:
        kind = entry["kind"]
        if kind == "calendar":
            day, evidence = entry["payload"]
            window = self.market.session(evidence, day)
            if tuple(t.timestamp() for t in window) != (self.start, self.end):
                raise ValueError("Feed session window differs from frozen provider calendar")
            self.calendar_verified = True
        event = feed_event(entry, max(self.last_time, available_at))
        if event is None:
            return
        at = event["received_at"]
        tick = datetime.fromisoformat(at).timestamp()
        self.last_time = tick
        if tick >= self.start and kind in ("book", "feature") and not self.calendar_verified:
            raise ValueError("Market evidence preceded verified calendar")
        if kind == "recovery_gap":
            self.recoveries += 1
        if kind in ("feature", "selection"):
            observed, possible = self.feature_interval(tick)
            self.feature_observed += observed
            self.feature_possible += possible
            self.feature_cut = max(self.feature_cut, min(tick, self.end))
        accepted = self.accounts.counts["accepted_books"]
        await self.accounts.consume(event)
        if kind == "book" and self.accounts.counts["accepted_books"] > accepted:
            through = min(self.end, tick + BOOK_COVERAGE_SECONDS)
            self.covered_seconds += max(0.0, through - max(self.covered_until, tick))
            self.covered_until = max(self.covered_until, through)
            self.max_gap = max(self.max_gap, tick - self.last_book)
            self.last_book = tick
        if kind == "feature":
            feature = self.accounts.features.get(event["symbol"])
            self.feature_fresh[event["symbol"]] = (
                datetime.fromisoformat(feature.bar_end).timestamp() + 90 if feature else 0.0
            )
        if kind == "selection":
            if self.start <= tick < self.end:
                through = min(self.end, tick + BOOK_COVERAGE_SECONDS)
                self.scan_seconds += max(0.0, through - max(self.scan_end, tick))
                self.scan_end = max(self.scan_end, through)
                self.scan_gap = max(self.scan_gap, tick - self.scan_last)
                self.scan_last = tick
            for symbol in self.signals.keys() - self.accounts.eligible:
                self.signals.pop(symbol)
                if live and isinstance(self.research, SessionResearch):
                    await self.research.invalidate(self.market.code, symbol)
            self.bars = {s: bars for s, bars in self.bars.items() if s in self.accounts.eligible}
        if kind not in ("feature", "book") or not self.start <= tick < self.end:
            return
        symbol = event["symbol"]
        if kind == "feature" and symbol in self.accounts.eligible:
            self.bars[symbol] = [
                row
                for row in event["result"]["candles"]
                if self.start < datetime.fromisoformat(row["timestamp"]).timestamp() <= tick
            ]
        family = self.accounts.signal_family(symbol, tick) if symbol in self.bars else None
        if self.signals.get(symbol) == family:
            return
        if not family:
            self.signals.pop(symbol, None)
            if live and isinstance(self.research, SessionResearch):
                await self.research.invalidate(self.market.code, symbol)
            return
        self.signals[symbol] = family
        if live and isinstance(self.research, SessionResearch) and 0 <= time.time() - tick <= 90:
            snapshot = {
                "market": self.market.code,
                "symbol": symbol,
                "as_of": at,
                "candles": self.bars[symbol],
                "quote": self.accounts.latest[symbol],
                "provenance": {
                    "source": "Toss committed market data",
                    "journal_index": entry["index"],
                },
            }
            await self.research.submit(
                [
                    Candidate(
                        snapshot,
                        signal={"strategy": family, "event": entry["index"]},
                    )
                ],
                at,
            )

    async def settle(self, state: str) -> None:
        if state == "complete":
            # Make quiet symbols expire their remaining reservations at the session boundary.
            await self.accounts.close_session()

    def report(self, state: str, at: float) -> dict[str, Any]:
        coverage = self.covered_seconds / (self.end - self.start)
        gap = max(self.max_gap, min(self.end, at) - self.last_book)
        if state == "complete" and (
            self.feed.state != "complete"
            or not self.feed.caught_up
            or not self.calendar_verified
            or coverage < 0.8
            or gap > MAX_BOOK_GAP_SECONDS
        ):
            state = "incomplete"
        result = self.accounts.summary(at=iso(max(at, self.last_time)), stream_state=state)
        result.update(
            book_coverage_fraction=coverage,
            max_book_gap_seconds=gap,
            recovery_gaps=self.recoveries,
            journal_index=self.feed.index,
            journal_hash=self.feed.chain,
            last_usable_book_at=iso(self.last_book) if self.covered_seconds else None,
        )
        research_counts: dict[str, int] = {}
        for row in self.research.rows:
            status = row["status"]
            research_counts[status] = research_counts.get(status, 0) + 1
        result["research_attempts"] = research_counts
        scan_fraction = self.scan_seconds / (self.end - self.start)
        scan_gap = max(self.scan_gap, min(self.end, at) - self.scan_last)
        observed, possible = self.feature_interval(at)
        possible += self.feature_possible
        feature_fraction = (observed + self.feature_observed) / possible if possible else None
        result.update(
            scan_coverage_fraction=scan_fraction,
            max_scan_gap_seconds=scan_gap,
            eligible_symbol_feature_coverage=feature_fraction,
        )
        inputs_qualified = (
            scan_fraction >= 0.8
            and scan_gap <= MAX_BOOK_GAP_SECONDS
            and feature_fraction is not None
            and feature_fraction >= 0.8
        )
        result["strategy_inputs_qualified"] = inputs_qualified
        if (
            not inputs_qualified
            or not research_counts.get("completed")
            or any(research_counts.get(k) for k in ("failed", "interrupted", "pending", "expired"))
        ):
            result.update(ranking=[], ranking_eligible=False, evaluation_status="inconclusive")
        if state != "running":
            result["research_sha256"] = hashlib.sha256(
                canonical_json(self.research.publications()).encode()
            ).hexdigest()

        return result


def session_plan(market: str, day: str, capital: float, calendar: dict[str, Any]) -> dict[str, Any]:
    if market not in ("US", "KR") or not math.isfinite(capital) or capital <= 0:
        raise ValueError("Invalid market or initial capital")
    start, end = (US if market == "US" else KR).session(calendar, day)
    return {
        "market": market,
        "day": day,
        "capital": capital,
        "calendar": calendar,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


async def replay(session: Path, out: Path) -> int:
    if out.exists():
        raise ValueError("Replay output must be new")
    plan = read_json(session / "session.json")
    if plan["source_digest"] != tree_digest(ROOT):
        raise ValueError("Replay requires the session source checkout")
    original = read_json(session / "comparison.json")
    schedule = session / "schedule"
    saved = read_json(session / "capture.json")
    if saved["state"] not in {"complete", "incomplete", "failed", "expired", "blocked"}:
        raise ValueError("Replay requires a terminal capture")
    result = Comparison(plan, schedule / "journal.sqlite3", ResearchReplay(session / "research"))
    clock = ProjectionClock(session / "projection.bin", replay=True)
    try:
        while result.feed.path.is_file() and not result.feed.caught_up:
            for entry in await asyncio.to_thread(result.feed.read):
                await result.apply(entry, live=False, available_at=clock.cut(entry["index"], 0.0))
        state = saved["state"]
        at = result.end if state == "complete" else result.last_time or result.start
        await result.settle(state)
        report = result.report(state, at)
        report["projection_sha256"] = clock.seal()
        if canonical_json(report) != canonical_json(original):
            raise ValueError("Replay differs from the sealed comparison or research evidence")
        out.mkdir(mode=0o700, parents=True)
        atomic_json(out / "comparison.json", report)
    finally:
        clock.close()
        await result.accounts.close()
    return 0


async def run(args: argparse.Namespace, out: Path) -> int:
    plan_file = out / "session.json"
    final = out / "comparison.json"
    digest = tree_digest(ROOT)
    plan = read_json(plan_file) if plan_file.is_file() else None
    if plan is not None:
        if (plan["market"], plan["day"], plan["capital"], plan["parallelism"]) != (
            args.market,
            args.day.isoformat(),
            args.capital,
            args.research_workers,
        ):
            raise ValueError("Saved session contract differs from invocation")
        if plan["source_digest"] != digest:
            raise ValueError("Session source changed; use the pinned checkout")
        if final.is_file():
            return int(read_json(final)["stream_state"] != "complete")
    if any(importlib.util.find_spec(name) is None for name in ("tradingagents", "websockets")):
        raise ImportError("Install the research and websocket-probe extras before starting")
    settings = Settings.load(ROOT)
    if not settings.get("ai", "enabled") or not settings.get("simulation", "enabled"):
        raise ValueError("Research sessions require enabled AI and paper simulation")
    if plan is None:
        client = MarketClient(settings)
        calendar = await asyncio.to_thread(client.calendar, args.market, args.day.isoformat())
        plan = session_plan(args.market, args.day.isoformat(), args.capital, calendar)
        if datetime.fromisoformat(plan["end"]).timestamp() <= time.time():
            raise ValueError("The requested session has ended")
        plan.update(
            source_commit=command(["git", "rev-parse", "HEAD"], ROOT),
            source_digest=digest,
            parallelism=args.research_workers,
        )
        atomic_json(plan_file, plan)

    schedule, job = out / "schedule", out / "scheduler"
    receipts = out / "projection.bin"
    started = (job / "start.claim").exists()
    if not receipts.exists() and (started or (out / "status.json").exists()):
        raise ValueError("Missing projection clock for an existing session")
    protection = out / "protected.json"
    if not protection.exists():
        if started:
            raise ValueError("Existing session is missing its protected-position record")
        atomic_json(protection, {"market": args.market, "symbols": []})
    service = toss_service(
        out / "research" / "reports",
        executable=settings.get("ai", "binary"),
        protected_paths=(
            settings.secrets_dir,
            out,
        ),
    )
    research = SessionResearch(
        out / "research",
        service,
        max_concurrency=args.research_workers,
        expires_at=plan["end"],
    )
    comparison = Comparison(plan, schedule / "journal.sqlite3", research)
    clock = ProjectionClock(receipts, replay=False)
    research_closed = False
    try:
        if not (job / "start.claim").exists():
            await asyncio.to_thread(
                start_job,
                job,
                [
                    sys.executable,
                    "-m",
                    "tools.schedule_async_live_validation",
                    "--output",
                    str(schedule),
                    "--start-at",
                    plan["start"],
                    "--end-at",
                    plan["end"],
                    "--market",
                    args.market,
                    "--capital",
                    str(args.capital),
                    "--source-commit",
                    plan["source_commit"],
                    "--source-digest",
                    plan["source_digest"],
                    "--recover",
                    "--dynamic-slots",
                    "--research-protection",
                    str(protection),
                ],
                ROOT,
            )
        held = set(read_json(protection)["symbols"])
        last_status = 0.0
        terminal = None
        last_poll = 0.0
        while True:
            now = time.monotonic()
            if terminal is None and now >= last_poll + 2:
                terminal = await asyncio.to_thread(poll_job, job)
                last_poll = now
            rows = []
            if comparison.feed.path.is_file():
                rows = await asyncio.to_thread(comparison.feed.read, initializing=terminal is None)
                for entry in rows:
                    await comparison.apply(
                        entry,
                        live=terminal is None,
                        available_at=clock.cut(entry["index"], time.time()),
                    )
                if rows:
                    await asyncio.to_thread(clock.sync)
            if comparison.feed.caught_up:
                new_held = comparison.accounts.protected()
                if new_held != held:
                    await asyncio.to_thread(
                        atomic_json,
                        protection,
                        {
                            "market": args.market,
                            "symbols": sorted(new_held),
                        },
                    )
                    held = new_held
            if terminal is not None and not rows:
                status_file = schedule / "status.json"
                state = read_json(status_file)["state"] if status_file.exists() else "failed"
                if state not in {"complete", "failed", "incomplete", "expired", "blocked"}:
                    state = "failed"
                if terminal["returncode"] and state == "complete":
                    state = "failed"
                break
            if now - last_status >= 10:
                await asyncio.to_thread(
                    atomic_json, out / "status.json", comparison.report("running", time.time())
                )
                last_status = now
            if not rows or comparison.feed.caught_up:
                await asyncio.sleep(0.2 if time.time() >= comparison.start else 2)
        await research.aclose()
        research_closed = True
        at = comparison.end if state == "complete" else comparison.last_time or comparison.start
        await comparison.settle(state)
        report = comparison.report(state, at)
        report["projection_sha256"] = clock.seal()
        atomic_json(out / "capture.json", {"state": state})
        atomic_json(final, report)
        atomic_json(out / "status.json", report)
        return int(report["stream_state"] != "complete")
    finally:
        clock.close()
        if not research_closed:
            await research.aclose()
        await comparison.accounts.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    live = commands.add_parser("run", help="Start or reconcile one finite paper session")
    live.add_argument("--market", choices=("US", "KR"), required=True)
    live.add_argument("--day", type=date.fromisoformat, required=True)
    live.add_argument("--capital", type=float)
    live.add_argument("--research-workers", type=int, default=2)
    live.add_argument("--output", required=True, type=Path)
    recorded = commands.add_parser(
        "replay", help="Rebuild a recorded session without network or AI"
    )
    recorded.add_argument("--session", required=True, type=Path)
    recorded.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    out = args.output.resolve()
    try:
        if args.mode == "replay":
            return asyncio.run(replay(args.session.resolve(), out))
        if args.capital is None:
            args.capital = 1000 if args.market == "US" else 1_000_000
        if not math.isfinite(args.capital) or args.capital <= 0 or args.research_workers < 1:
            raise ValueError("Capital and research concurrency must be positive")
        out.mkdir(parents=True, exist_ok=True)
        with (out / "session.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                return asyncio.run(run(args, out))
            except Exception as error:
                frames = traceback.extract_tb(error.__traceback__)
                origin = next(
                    (f for f in reversed(frames) if Path(f.filename).is_relative_to(ROOT)), None
                )
                incident: dict[str, Any] = {
                    "error_type": type(error).__name__,
                    "at": iso(time.time()),
                }
                if origin:
                    incident.update(
                        module=Path(origin.filename).name, function=origin.name, line=origin.lineno
                    )
                if isinstance(error, ApiError):
                    incident.update(api_code=error.code, api_status=error.status)
                atomic_json(out / "error.json", incident)
                raise
    except (
        OSError,
        ValueError,
        KeyError,
        ImportError,
        RuntimeError,
        sqlite3.Error,
        ApiError,
        SafetyError,
    ) as error:
        parser.exit(
            1, f"Paper session failed ({type(error).__name__}). See the private session evidence.\n"
        )


if __name__ == "__main__":
    raise SystemExit(main())
