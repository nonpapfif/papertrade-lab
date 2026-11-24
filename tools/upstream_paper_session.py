"""Capture one US paper session sourced from two pinned upstream projects."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import math
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from papertrade.api import SYMBOL, ApiError, MarketClient
from papertrade.core import SafetyError, Settings
from papertrade.jsonio import atomic_json, canonical_json, read_json
from tools.adaptive_journal import AdaptiveJournal, JournalError, read_status
from tools.paper_market import US
from tools.session_research import Candidate, ResearchReplay, ResearchService, SessionResearch
from tools.source_identity import tree_digest
from tools.upstream_paper_accounts import (
    FEES,
    PROFILES,
    REWARD_RISK_PROFILES,
    Comparison,
    reward_risk_profiles,
)
from tools.upstream_strategy import (
    RANGE_MINUTES,
    RESEARCH_REVISION,
    REVISION,
    STEP,
    Upstream,
    epoch,
    iso,
)

ROOT = Path(__file__).resolve().parents[1]
POLL_SECONDS = 5
STATUS_SECONDS = 30


def _plan(
    args: argparse.Namespace, source: Upstream, at: float
) -> tuple[dict[str, Any], dict[str, Any]]:
    config = source.config(args.config, args.capital)
    calendar = read_json(args.calendar)
    day = args.day.isoformat()
    start, end = US.session(calendar, day)
    if not source.entry.trading_day(args.day)[0] or source.entry.session(args.day) != (
        start.astimezone(US.timezone).time(),
        end.astimezone(US.timezone).time(),
    ):
        raise ValueError("Toss regular session does not match the pinned source calendar")
    watchlist = read_json(args.watchlist)
    created = epoch(watchlist["created_at"])
    previous_day = calendar["previousBusinessDay"]["date"]
    if (
        watchlist.get("created_by") != "trader-scanner"
        or watchlist.get("mode") != "daily"
        or watchlist.get("source") != "yahoo"
        or watchlist.get("offline") is not None
        or watchlist.get("as_of") != f"{previous_day} close"
        or not created <= min(at, start.timestamp())
        or US.trading_date(dt.datetime.fromtimestamp(created, dt.UTC)) != day
    ):
        raise ValueError("Expected a native premarket daily watchlist through the prior close")
    symbols = [hit["symbol"] for hit in watchlist["hits"]]
    if not all(isinstance(symbol, str) and SYMBOL.fullmatch(symbol) for symbol in symbols):
        raise ValueError("A watchlist symbol does not match the provider symbol contract")
    if len(symbols) != len(set(symbols)):
        raise ValueError("The watchlist repeats a symbol")
    premarket = {key: watchlist[key] for key in ("universe", "scanned", "failed")}
    if any(type(count) is not int or count < 0 for count in premarket.values()):
        raise ValueError("Invalid native premarket scan counts")
    premarket["hits"] = len(symbols)
    entry_cutoff = source.cutoff(args.day, config)
    if not start.timestamp() < entry_cutoff < end.timestamp():
        raise ValueError("Native entry cutoff is outside the Toss regular session")
    comparison: dict[str, Any]
    pair = args.reward_risk_pair
    if args.compare_reward_risk or pair is not None:
        comparison = {
            "min_reward_risk_by_profile": (
                REWARD_RISK_PROFILES.copy() if pair is None else reward_risk_profiles(*pair)
            ),
            "research_policy": "disabled",
        }
    else:
        comparison = {
            "tradingagents_revision": RESEARCH_REVISION,
            "research_workers": args.research_workers,
            "has_research_history_directory": args.history_dir is not None,
            "research_policy": (
                "native_daily_hit once, Buy or Overweight published by the ORB bar close"
            ),
        }
    return (
        {
            "schema": 1,
            "mode": "paper_only",
            "day": day,
            "market": "US",
            "currency": "USD",
            "created_at": iso(at),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "entry_cutoff": iso(entry_cutoff),
            "calendar": calendar,
            "config": config,
            "symbols": symbols,
            "premarket_scan": premarket,
            "watchlist_sha256": hashlib.sha256(canonical_json(watchlist).encode()).hexdigest(),
            "day_trader_root": str(source.root),
            "day_trader_revision": REVISION,
            "day_trader_files_sha256": source.hashes,
            "runner_tree_sha256": tree_digest(ROOT),
            **comparison,
            "strategy": {
                "source": "Yahoo 5-minute ORB within the pinned daily watchlist",
                "opening_range_minutes": RANGE_MINUTES,
                "min_price": 5.0,
                "min_volume": 0,
                "max_confirm": source.scanner.MAX_CONFIRM,
                "long_stop": "native OR midpoint",
                "target": "native 2R",
            },
            "execution": {
                "data": "Toss GET /api/v1/orderbook and GET /api/v1/prices",
                "fill": "A separate, strictly later USD book at top displayed whole-share depth",
                "buy_remainder": "Expire unfilled quantity after the first eligible later snapshot",
                "sell_remainder": "Continue at later distinct eligible bid snapshots",
                "fees": {
                    key: FEES[key]
                    for key in (
                        "commission_rate",
                        "commission_exemption_usd",
                        "sec_sell_rate",
                        "sec_minimum_usd",
                        "taf_rate",
                        "max_quote_age_seconds",
                    )
                },
                "cost_evidence": (
                    "Unverified Toss API fee scenario; USD funded; "
                    "proceeds immediately reusable in paper"
                ),
            },
        },
        watchlist,
    )


def _candidate(
    hit: dict[str, Any],
    watchlist: dict[str, Any],
    day: str,
    history_dir: Path | None,
    at: float,
) -> Candidate:
    symbol = hit["symbol"]
    snapshot = {
        "market": "US",
        "symbol": symbol,
        "as_of": iso(at),
        "last": hit["price"],
        "daily_scan_hit": hit,
        "source": "day-trader-skill Yahoo daily scan",
        "scanner_as_of": watchlist["as_of"],
        "daily_bar_status": "unavailable",
    }
    if history_dir is not None:
        history = history_dir / f"{symbol}.json"
        try:
            data = read_json(history)
            received = epoch(data["received_at"])
            bars = data["bars"]
            dates = [dt.date.fromisoformat(row["date"]).isoformat() for row in bars]
            if not (
                data["symbol"] == symbol
                and type(data["source"]) is str
                and data["source"]
                and received <= at
                and dates
                and dates == sorted(set(dates))
                and dates[-1] < day
                and all(
                    math.isfinite(float(row[key])) and float(row[key]) > 0
                    for row in bars
                    for key in ("open", "high", "low", "close")
                )
                and all(
                    math.isfinite(float(row["volume"])) and float(row["volume"]) >= 0
                    for row in bars
                )
            ):
                raise ValueError("Invalid completed daily history")
        except FileNotFoundError:
            pass
        except (OSError, ValueError, TypeError, KeyError, OverflowError):
            snapshot["daily_bar_status"] = "rejected"
        else:
            snapshot.update(
                daily_bar_status="completed_prior_dates",
                daily_bars=[
                    {name: row[name] for name in ("date", "open", "high", "low", "close", "volume")}
                    for row in bars
                ],
                daily_closes=[{"date": row["date"], "close": row["close"]} for row in bars],
                daily_bar_provenance={"source": data["source"], "received_at": data["received_at"]},
            )
    return Candidate(
        snapshot, signal={"kind": "native_daily_hit", "day": day, "setups": hit["setups"]}
    )


class ResearchGuard:
    """Pause the shared queue when the child is unavailable across requests."""

    def __init__(self, service: ResearchService) -> None:
        self.service = service
        self.failure = asyncio.Event()
        self.closed = asyncio.Event()
        self.reason = ""
        self.last_exit: int | None = None

    async def analyze(self, snapshot: dict[str, Any], inputs: Any = None) -> Any:
        if self.failure.is_set():
            await self.closed.wait()
            raise RuntimeError("Research provider is disabled")
        try:
            result = await self.service.analyze(snapshot, inputs)
        except Exception as error:  # propagate original worker evidence
            reason = getattr(error, "reason", None)
            exit_code = getattr(error, "exit_code", None)
            if reason == "cli_unavailable" or (
                reason == "child_nonzero" and type(exit_code) is int and exit_code == self.last_exit
            ):
                self.reason = (
                    "cli_unavailable" if reason == "cli_unavailable" else "repeated_child_failure"
                )
                self.failure.set()
            self.last_exit = exit_code if reason == "child_nonzero" else None
            raise
        self.last_exit = None
        return result

    async def aclose(self) -> None:
        self.closed.set()
        await self.service.aclose()


def _login_usable(executable: str) -> bool:
    try:
        result = subprocess.run(
            [executable, "login", "status"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


class Capture:
    def __init__(
        self,
        comparison: Comparison,
        journal: AdaptiveJournal,
        output: Path,
        watchlist: dict[str, Any],
    ) -> None:
        self.comparison, self.journal, self.output = comparison, journal, output
        self.watchlist = watchlist
        self.research: SessionResearch | None = None
        self.research_stop = asyncio.Event()
        self.research_status = "starting" if comparison.research is not None else "disabled"
        self.next_status = 0.0
        self.pending_research_invalidations: set[str] = set()
        self.expired_symbols = asyncio.Event()
        self.cutoff = epoch(comparison.plan["entry_cutoff"])

    def event(self, kind: str, payload: Any = None) -> dict[str, Any]:
        event = {"kind": kind, "at": iso(time.time()), "payload": payload}
        result = self.comparison.process(event)
        completed_at = time.time()
        self.comparison.complete_decisions(result, completed_at)
        self.journal.append({**event, "completed_at": iso(completed_at), "outputs": result})
        if self.comparison.research is not None:
            if kind == "scan":
                self.pending_research_invalidations.update(
                    signal["symbol"] for signal in result["public"]["new_signals"]
                )
                self.expired_symbols.set()
            if self.comparison.accounts[PROFILES[1]].entry_limit_reached():
                self.research_stop.set()
        return result

    def status(self, state: str, *, force: bool = False) -> dict[str, Any]:
        now = time.time()
        stats = {
            **self.comparison.summary(),
            **self.journal.evidence(),
            "state": state,
            "status_written_at": iso(now),
            "research_state": self.research_status,
            "research": self.research.metadata() if self.research is not None else {},
        }
        if force or now >= self.next_status:
            atomic_json(self.output / "status.json", stats)
            self.next_status = now + STATUS_SECONDS
        return stats

    async def research_worker(self, args: argparse.Namespace, settings: Settings) -> None:
        def unavailable(reason: str) -> None:
            self.research_status = "unavailable"
            self.event("research_unavailable", {"code": reason})

        if time.time() >= self.cutoff:
            self.research_status = "outside_native_entry_window"
            return
        executable = str(args.codex or settings.get("ai", "binary"))
        if not settings.get("ai", "enabled") or not await asyncio.to_thread(
            _login_usable, executable
        ):
            unavailable("codex_login_or_binary_unavailable")
            return
        try:
            from papertrade.research.upstream_session import native_service

            guard = ResearchGuard(
                native_service(
                    self.output / "research" / "reports",
                    executable=executable,
                    protected_paths=(settings.secrets_dir, self.output),
                )
            )
        except (ImportError, OSError, ValueError, RuntimeError) as error:
            unavailable("native_graph_" + type(error).__name__)
            return
        research = SessionResearch(
            self.output / "research",
            guard,
            expires_at=iso(self.comparison.entry_end),
            max_concurrency=args.research_workers,
        )
        self.research = research
        self.comparison.research = research
        self.research_status = "native_graph_ready"
        try:
            attempted = {row["snapshot"]["symbol"] for row in research.rows}
            remaining = iter(
                hit for hit in self.watchlist["hits"] if hit["symbol"] not in attempted
            )
            exhausted = False
            while (
                not self.research_stop.is_set()
                and not guard.failure.is_set()
                and time.time() < self.cutoff
            ):
                self.expired_symbols.clear()
                invalidated = sorted(self.pending_research_invalidations)
                self.pending_research_invalidations.difference_update(invalidated)
                for symbol in invalidated:
                    await research.invalidate("US", symbol)
                pending = sum(row["status"] == "pending" for row in research.rows)
                for _ in range(args.research_workers - pending):
                    if (
                        self.research_stop.is_set()
                        or guard.failure.is_set()
                        or time.time() >= self.cutoff
                    ):
                        break
                    hit = next(remaining, None)
                    while hit is not None and hit["symbol"] in self.comparison.seen:
                        hit = next(remaining, None)
                    if hit is None:
                        exhausted = True
                        break
                    at = time.time()
                    candidate = _candidate(
                        hit, self.watchlist, self.comparison.plan["day"], args.history_dir, at
                    )
                    await research.submit([candidate], iso(at))
                if exhausted and not any(row["status"] == "pending" for row in research.rows):
                    break
                signals = [
                    asyncio.create_task(event.wait())
                    for event in (
                        self.expired_symbols,
                        self.research_stop,
                        guard.failure,
                    )
                ]
                try:
                    await asyncio.wait(
                        signals,
                        timeout=min(1, max(0, self.cutoff - time.time())),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    for signal in signals:
                        signal.cancel()
                    await asyncio.gather(*signals, return_exceptions=True)
            if guard.failure.is_set():
                unavailable(guard.reason)
            elif exhausted:
                self.research_status = "native_daily_candidates_exhausted"
            elif time.time() >= self.cutoff:
                self.research_status = "stopped_after_native_entry_window"
            else:
                self.research_status = "stopped_after_native_entry_limit_or_session_stop"
        finally:
            await research.aclose()

    async def scans(self) -> None:
        comp = self.comparison
        first = comp.start + (RANGE_MINUTES + 5) * 60
        next_cut = max(first, float(int(time.time()) // STEP * STEP))
        while next_cut <= self.cutoff:
            await asyncio.sleep(max(0, next_cut + 2 - time.time()))
            now = time.time()
            if now >= comp.entry_end:
                return
            cut = min(self.cutoff, max(next_cut, float(int(now) // STEP * STEP)))
            until = min(cut + STEP, comp.entry_end)
            data = await asyncio.to_thread(
                comp.source.capture,
                comp.plan["symbols"],
                comp.day,
                cut,
                until,
                final_entry_window=until == comp.entry_end,
            )
            if time.time() < comp.end:
                self.event("scan", data)
            next_cut = cut + STEP

    async def books(self, client: MarketClient) -> None:
        comp = self.comparison
        expected = {"symbol", "currency", "timestamp", "lastPrice"}
        suspended_until = 0.0

        def get_prices(symbols: list[str]) -> tuple[list[dict[str, Any]], float]:
            rows = client.prices(symbols)
            return rows, time.time()

        def get_book(symbol: str) -> tuple[dict[str, Any], float]:
            result = client.book(symbol)
            return result, time.time()

        while time.time() < comp.end:
            if time.time() < suspended_until:
                await asyncio.sleep(min(POLL_SECONDS, suspended_until - time.time()))
                continue
            started = time.monotonic()
            try:
                held = comp.held_symbols()
                if held:
                    rows, received = await asyncio.to_thread(get_prices, held)
                    self.event(
                        "trades",
                        {
                            "received": received,
                            "rows": [
                                {key: row[key] for key in row.keys() & expected}
                                for row in rows
                                if row.get("symbol") in held
                            ],
                        },
                    )
                for symbol in comp.active_symbols():
                    if time.time() >= comp.end:
                        return
                    try:
                        book, received = await asyncio.to_thread(get_book, symbol)
                    except ApiError as error:
                        if error.status in {401, 403, 429} or (
                            error.retry_at and error.retry_at - time.time() > POLL_SECONDS
                        ):
                            raise
                        self.event(
                            "market_data_error",
                            {
                                "symbol": symbol,
                                "code": error.code,
                                "http_status": error.status,
                            },
                        )
                    else:
                        fields = {"currency", "timestamp", "asks", "bids"}
                        public_book = {key: book[key] for key in book.keys() & fields}
                        self.event("book", [symbol, public_book, received])
            except ApiError as error:
                self.event("market_data_error", {"code": error.code})
                suspended_until = max(time.time() + POLL_SECONDS, error.retry_at or 0)
            await asyncio.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))

    async def run(self, args: argparse.Namespace, settings: Settings) -> None:
        comp = self.comparison
        research_task = (
            asyncio.create_task(self.research_worker(args, settings))
            if comp.research is not None
            else None
        )
        market_tasks: list[asyncio.Task[None]] = []
        try:
            self.event("capture_started")
            self.status("waiting_for_regular_session", force=True)
            while time.time() < comp.start:
                if research_task is not None and research_task.done():
                    research_task.result()
                self.status("waiting_for_regular_session")
                await asyncio.sleep(min(STATUS_SECONDS, max(0, comp.start - time.time())))
            market_tasks += [
                asyncio.create_task(self.scans()),
                asyncio.create_task(self.books(MarketClient(settings))),
            ]
            active = market_tasks if research_task is None else [research_task, *market_tasks]
            while time.time() < comp.end:
                for task in active:
                    if task.done():
                        task.result()
                self.event("clock")
                if time.time() >= self.cutoff:
                    self.research_stop.set()
                self.status("capturing")
                await asyncio.sleep(min(POLL_SECONDS, max(0, comp.end - time.time())))
            self.event("close")
        finally:
            self.research_stop.set()
            for task in market_tasks:
                task.cancel()
            await asyncio.gather(*market_tasks, return_exceptions=True)
            if research_task is not None:
                await research_task


def _replay(comparison: Comparison, journal: AdaptiveJournal) -> None:
    for event in journal.events():
        actual = comparison.process(event)
        comparison.complete_decisions(actual, epoch(event["completed_at"]))
        if actual != event["outputs"]:
            raise JournalError(f"Recorded account decisions differ at event {event['index']}")


def _summary(comparison: Comparison, journal: AdaptiveJournal) -> dict[str, Any]:
    observed = comparison.stats
    publications = comparison.research.publications() if comparison.research is not None else []
    premarket = comparison.plan["premarket_scan"]
    limitations = []
    if premarket["failed"]:
        limitations.append("some_native_premarket_scan_requests_failed")
    if observed["research_unavailable"]:
        limitations.append("research_unavailable_during_capture")
    if comparison.plan["symbols"] and not observed["scanner_scanned"]:
        limitations.append("no_usable_native_scan_inputs")
    if observed["scanner_spark_failures"] or observed["scanner_confirmation_failures"]:
        limitations.append("some_native_scan_requests_failed")
    if (
        comparison.live_since
        and comparison.live_since >= comparison.start + (RANGE_MINUTES + 5) * 60
    ):
        limitations.append("capture_started_after_first_possible_source_signal")
    if observed["capture_started"] > 1:
        limitations.append("capture_was_restarted")
    if observed["market_data_error"]:
        limitations.append("recorded_market_data_incidents")
    if observed["research_gate_declined"] and not any(
        row["status"] == "completed" for row in publications
    ):
        limitations.append("no_completed_research_for_observed_entry_signals")
    return {
        **comparison.summary(),
        **journal.evidence(),
        "state": "closed_incomplete" if limitations else "closed",
        "comparison_quality": "incomplete" if limitations else "observed",
        "limitations": limitations,
        "premarket_scan": premarket,
        "research_publications_sha256": hashlib.sha256(
            canonical_json(publications).encode()
        ).hexdigest(),
        "research_attempts": len(publications),
    }


async def _run(args: argparse.Namespace) -> int:
    source = Upstream(args.source)
    settings = Settings.load(ROOT)
    output = args.output.resolve()
    plan, watchlist = _plan(args, source, time.time())
    if not settings.get("simulation", "enabled"):
        raise ValueError("The repository paper simulation setting must be enabled")
    if not args.resume and time.time() >= epoch(plan["end"]):
        raise ValueError("The requested live-data session has ended")
    if args.resume:
        frozen = read_json(output / "plan.json")
        if {**plan, "created_at": frozen["created_at"]} != frozen:
            raise ValueError("Resume arguments or runner files differ from the frozen contract")
        plan = frozen
        if read_json(output / "watchlist.json") != watchlist:
            raise ValueError("The frozen watchlist changed")
        if read_json(output / "source-config.json") != plan["config"]:
            raise ValueError("The frozen strategy config changed")
    else:
        output.mkdir(parents=True, mode=0o700, exist_ok=False)
        atomic_json(output / "plan.json", plan)
        atomic_json(output / "watchlist.json", watchlist)
        atomic_json(output / "source-config.json", plan["config"])
    replay = None if "min_reward_risk_by_profile" in plan else ResearchReplay(output / "research")
    comparison = Comparison(source, plan, output / "source-config.json", replay)
    with AdaptiveJournal.open(output / "journal.sqlite3", plan, resume=args.resume) as journal:
        if args.resume:
            _replay(comparison, journal)
        capture = Capture(comparison, journal, output, watchlist)
        if not comparison.stats["close"]:
            if time.time() >= comparison.end:
                capture.event("market_data_error", {"code": "process_not_running_at_close"})
                capture.event("close")
            else:
                try:
                    await capture.run(args, settings)
                except (Exception, asyncio.CancelledError) as error:
                    capture.event("market_data_error", {"code": "session_" + type(error).__name__})
                    capture.status("interrupted", force=True)
                    raise
        summary = _summary(comparison, journal)
        final = output / "comparison.json"
        if final.exists():
            if read_json(final) != summary:
                raise JournalError("Final comparison does not match the stored close cut")
        else:
            atomic_json(final, summary)
        journal.complete()
        capture.status(summary["state"], force=True)
    print(canonical_json(summary))
    return 0


def _verify(args: argparse.Namespace) -> int:
    output = args.session.resolve()
    plan = read_json(output / "plan.json")
    source = Upstream(args.source or Path(plan["day_trader_root"]))
    if (
        plan["runner_tree_sha256"] != tree_digest(ROOT)
        or plan["day_trader_files_sha256"] != source.hashes
    ):
        raise ValueError("The runner or upstream scripts differ from the frozen source")
    config = output / "source-config.json"
    watchlist = read_json(output / "watchlist.json")
    watchlist_hash = hashlib.sha256(canonical_json(watchlist).encode()).hexdigest()
    if read_json(config) != plan["config"] or watchlist_hash != plan["watchlist_sha256"]:
        raise ValueError("The archived config or watchlist changed")
    replay = None if "min_reward_risk_by_profile" in plan else ResearchReplay(output / "research")
    comparison = Comparison(source, plan, config, replay)
    journal_path = output / "journal.sqlite3"
    status = read_status(journal_path)
    if status["identity"] != plan:
        raise ValueError("The archived contract differs from the journal identity")
    with AdaptiveJournal.read_completed(journal_path, status["hash"]) as journal:
        _replay(comparison, journal)
        if _summary(comparison, journal) != read_json(output / "comparison.json"):
            raise JournalError("Offline reconstruction differs from the sealed comparison")
    print(
        canonical_json({"verified": True, "records": status["records"], "sha256": status["hash"]})
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    run = modes.add_parser("run")
    run.add_argument("--day", type=dt.date.fromisoformat, required=True)
    run.add_argument("--source", type=Path, required=True)
    run.add_argument("--calendar", type=Path, required=True)
    run.add_argument("--watchlist", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--config", type=Path)
    run.add_argument("--capital", type=float, default=1000)
    run.add_argument("--codex")
    run.add_argument("--research-workers", type=int, default=2)
    run.add_argument("--history-dir", type=Path)
    reward_risk = run.add_mutually_exclusive_group()
    reward_risk.add_argument(
        "--compare-reward-risk", action="store_true",
        help="Compare minimum reward:risk of 1 and 2 without TradingAgents research",
    )
    reward_risk.add_argument(
        "--reward-risk-pair", nargs=2, type=float, metavar=("INCUMBENT", "CANDIDATE"),
        help="Run the specified minimum reward:risk pair without TradingAgents research",
    )
    run.add_argument("--resume", action="store_true")
    verify = modes.add_parser("verify")
    verify.add_argument("--session", type=Path, required=True)
    verify.add_argument("--source", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.mode == "verify":
            return _verify(args)
        if not math.isfinite(args.capital) or args.capital <= 0 or args.research_workers < 1:
            raise ValueError("Capital and research worker count must be positive")
        return asyncio.run(_run(args))
    except (
        ValueError,
        TypeError,
        KeyError,
        JournalError,
        OSError,
        SafetyError,
        subprocess.SubprocessError,
    ) as error:
        print(f"Upstream paper session stopped: {type(error).__name__}: {error}", file=sys.stderr)
    except KeyboardInterrupt:
        print("Upstream paper session interrupted", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
