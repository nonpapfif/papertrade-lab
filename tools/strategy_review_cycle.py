"""Run one evidence-driven, paired paper strategy review on a dated market session."""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from datetime import UTC, date, datetime
from datetime import time as wall_time
from pathlib import Path
from typing import Any

from papertrade.api import ApiError, MarketClient
from papertrade.core import SafetyError, Settings
from papertrade.jsonio import atomic_json, read_json
from tools.paper_campaign.evidence import compact_rows
from tools.paper_market import KR, US
from tools.source_identity import command, file_digest, tree_digest

ROOT = Path(__file__).resolve().parents[1]
SLOT = "rebound-entry-cost"
US_ACCOUNTS = ("source_min_reward_risk_1", "source_min_reward_risk_2")


def review_loss(market: str, evidence: Path) -> dict[str, Any]:
    report = read_json(evidence)
    if market == "KR":
        if report["strict_snapshot_attribution_reconciled"] is not True:
            raise ValueError("KR loss reconstruction did not reconcile")
        accounts = report["accounts"]["candidate"]
        alternatives = accounts["alternatives"]
        return {
            "day": "2026-11-09",
            "finding": "Rebound entries can spend more than the net proceeds at their target.",
            "original_mark_pnl": accounts["baseline_mark_pnl_krw"],
            "equal_allocation_mark_pnl": alternatives["allocation_frozen_equal"]["mark_pnl_krw"],
            "candidate_mark_pnl": alternatives["entry_rebound_net_target_equal"]["mark_pnl_krw"],
            "candidate_stale_marks": alternatives["entry_rebound_net_target_equal"][
                "stale_open_positions"
            ],
        }
    accounts = report["accounts"]["source_only"]
    if (
        report["state"] != "closed_incomplete"
        or accounts["counts"]["exit_confirmation:filled"] != 3
    ):
        raise ValueError("Expected the archived US Day Trader loss review")
    if accounts["net_pnl_usd"] != "-21.19102":
        raise ValueError("US loss review does not match the archived session")
    journal = evidence.with_name("journal.sqlite3").resolve()
    with closing(sqlite3.connect(journal.as_uri() + "?mode=ro", uri=True)) as db:
        if db.execute("SELECT state, hash FROM header").fetchone() != (
            "complete", report["chain_sha256"],
        ):
            raise ValueError("US comparison does not match the sealed loss journal")
        events = {}
        for event_id, body in db.execute("SELECT id, body FROM events WHERE id IN (297, 7016)"):
            for trade in json.loads(body)["outputs"]["accounts"]["source_only"]:
                if trade.get("symbol") == "ACCO" and trade.get("status") == "filled":
                    events[event_id] = trade
    entry, exit_fill = events[297], events[7016]
    sizing = entry["sizing"]
    if (
        entry["kind"] != "entry_confirmation" or sizing["reward_risk"] != 1.0
        or exit_fill["kind"] != "exit_confirmation" or exit_fill["reason"] != "plan-stop"
    ):
        raise ValueError("US source trades do not support the opening-range hypothesis")
    return {
        "day": sizing["date"],
        "finding": "ACCO filled after reward/risk fell from 2:1 to 1:1 and exited at the stop.",
        "journal": str(journal),
        "journal_chain": report["chain_sha256"],
        "entry_event": 297,
        "exit_event": 7016,
        "symbol": "ACCO",
        "planned_entry": sizing["entry_plan"],
        "filled_entry": entry["price"],
        "target": sizing["target"],
        "stop": sizing["stop"],
        "trade_net_pnl": exit_fill["realized_usd"],
        "account_net_pnl": accounts["net_pnl_usd"],
    }


def decide(
    incumbent: float | None, candidate: float | None, *, complete: bool, traded: bool
) -> dict[str, Any]:
    if not complete or None in (incumbent, candidate):
        return {"decision": "repair_evidence", "reason": "Incomplete or unmarkable comparison"}
    assert incumbent is not None and candidate is not None
    if not math.isfinite(incumbent) or not math.isfinite(candidate):
        raise ValueError("Comparison must have finite returns")
    delta = candidate - incumbent
    if not traded or delta == 0:
        return {
            "decision": "retest",
            "reason": "No observed improvement or deterioration to evaluate",
            "candidate_minus_incumbent_pct": delta,
        }
    return {
        "decision": "retest" if delta > 0 else "revise",
        "reason": (
            "Positive first sample; compare the frozen candidate on another unseen session"
            if delta > 0 else
            "Candidate underperformed; review the individual trades before scheduling it again"
        ),
        "candidate_minus_incumbent_pct": delta,
    }


def run_kr(output: Path, plan: dict[str, Any]) -> tuple[int, Path]:
    schedule = output / "schedule"
    pair = plan["strategies"]
    path = output / "campaign-plan.json"
    atomic_json(path, {
        "initial_slots": [{"id": "add-" + SLOT, "action": "add", "slot_id": SLOT, "spec": {
            "capital": plan["capital"], **pair,
        }}],
        "seeds": {},
    })
    result = subprocess.run([
        sys.executable, "-m", "tools.schedule_async_live_validation",
        "--output", str(schedule), "--market", "KR",
        "--start-at", plan["start"], "--end-at", plan["end"], "--capital", str(plan["capital"]),
        "--source-digest", plan["source_digest"], "--source-commit", plan["source_commit"],
        "--recover", "--dynamic-slots", "--campaign-plan", str(path),
    ], cwd=ROOT, check=False)
    status = schedule / "status.json"
    restarts = read_json(status).get("restarts", []) if status.is_file() else []
    worker = schedule / "attempts" / str(len(restarts) + 1) if restarts else schedule
    return result.returncode, worker / "run" / "status.json"


def wait_until(at: datetime) -> None:
    while (remaining := (at - datetime.now(UTC)).total_seconds()) > 0:
        time.sleep(min(remaining, 60))


def run_us(output: Path, plan: dict[str, Any], source: Path) -> tuple[int, Path]:
    scanner = source / "trader-scanner" / "scan.py"
    universe = output / "universe.txt"
    watchlist = output / "watchlist.json"
    subprocess.run(
        [sys.executable, str(scanner), "universe", "--out", str(universe)],
        cwd=output, check=True,
    )
    scan_day = datetime.combine(date.fromisoformat(plan["day"]), wall_time.min, US.timezone)
    wait_until(scan_day)
    scan = subprocess.run(
        [sys.executable, str(scanner), "daily", "--universe", str(universe),
         "--out", str(watchlist), "--quiet"],
        cwd=output, check=False,
    )
    if not (
        scan.returncode == 1 and watchlist.is_file() and read_json(watchlist).get("hits") == []
    ):
        scan.check_returncode()
    start = datetime.fromisoformat(plan["start"])
    if datetime.now(UTC) >= start:
        raise RuntimeError("US premarket scan did not finish before the comparison window")
    wait_until(start)
    if tree_digest(ROOT) != plan["source_digest"]:
        raise RuntimeError("Approved repository source changed before the US session")
    session = output / "session"
    result = subprocess.run([
        sys.executable, "-m", "tools.upstream_paper_session", "run",
        "--compare-reward-risk", "--day", plan["day"], "--source", str(source),
        "--calendar", str(output / "calendar.json"), "--watchlist", str(watchlist),
        "--output", str(session), "--capital", str(plan["capital"]),
    ], cwd=ROOT, check=False)
    if result.returncode == 0:
        result = subprocess.run([
            sys.executable, "-m", "tools.upstream_paper_session", "verify",
            "--session", str(session), "--source", str(source),
        ], cwd=ROOT, check=False)
    return result.returncode, session / "comparison.json"


def evaluate(market: str, report: dict[str, Any], ok: bool) -> dict[str, Any]:
    if market == "KR":
        slots = report.get("shared_slots", [])
        pair = slots[0].get("comparison", {}).get("accounts", {}) if len(slots) == 1 else {}
        shared_input = pair.get("incumbent", {}).get("input_sha256")
        accounts = compact_rows(report, "KRW").get(SLOT, {})
        a, b = (accounts.get(key, {}) for key in ("incumbent", "candidate"))
        complete = (
            ok and report.get("shared_feed", {}).get("complete") is True
            and bool(shared_input) and shared_input == pair.get("candidate", {}).get("input_sha256")
            and a.get("initial_equity") == b.get("initial_equity")
            and all(row.get("comparison_eligible") is True for row in (a, b))
        )
        key = "fee_inclusive_return_pct"
        traded = any(row.get("closed", 0) > 0 for row in (a, b))
    else:
        accounts = report.get("accounts", {})
        a, b = (accounts.get(key, {}) for key in US_ACCOUNTS)
        complete = (
            ok and report.get("state") == "closed"
            and report.get("comparison_quality") == "observed"
            and a.get("seed_usd") == b.get("seed_usd")
        )
        for row in (a, b):
            if row.get("net_pnl_usd") is not None:
                row["return_pct"] = float(row["net_pnl_usd"]) / float(row["seed_usd"]) * 100
        key = "return_pct"
        traded = any(row.get("counts", {}).get("exit_confirmation:filled", 0) for row in (a, b))
    return {
        **decide(a.get(key), b.get(key), complete=complete, traded=traded),
        "incumbent": a,
        "candidate": b,
    }


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", required=True, choices=("KR", "US"))
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source", type=Path, help="Pinned Day Trader checkout for US")
    parser.add_argument("--capital", type=float)
    parser.add_argument("--previous-decision", type=Path, help="Link a retest to a prior decision")
    args = parser.parse_args()
    if args.market == "US" and args.source is None:
        parser.error("--source is required for the US comparison")
    default_capital = 1_000_000 if args.market == "KR" else 1000
    capital = args.capital if args.capital is not None else default_capital
    if not math.isfinite(capital) or capital <= 0:
        parser.error("Capital must be positive and finite")
    loss = review_loss(args.market, args.evidence)
    if args.day.isoformat() <= loss["day"]:
        parser.error("The comparison day must follow the archived loss")
    previous = None
    if args.previous_decision is not None:
        previous = read_json(args.previous_decision)
        if (
            previous["market"] != args.market or previous["day"] >= args.day.isoformat()
            or previous["decision"] not in {"retest", "repair_evidence"}
        ):
            parser.error("Previous decision does not allow another comparison of this candidate")
        prior_path = args.previous_decision.with_name("plan.json")
        if file_digest(prior_path) != previous["plan_sha256"]:
            parser.error("Previous decision does not match its frozen plan")
        prior = read_json(prior_path)
        if prior["capital"] != capital or prior["evidence_sha256"] != file_digest(args.evidence):
            parser.error("The retest must use the same capital and loss evidence")
    market = KR if args.market == "KR" else US
    day = args.day.isoformat()
    try:
        calendar = MarketClient(Settings.load(ROOT)).calendar(market.code, day)
        start, end = market.session(calendar, day)
    except (ApiError, SafetyError, OSError, ValueError, KeyError) as error:
        parser.error(f"Cannot establish the requested market window: {error}")
    if datetime.now(UTC) >= start:
        parser.error("The comparison must be frozen before the session opens")
    source = args.source.resolve() if args.source is not None else None
    if source is not None and args.market == "US":
        pinned = "3d0f4ef6986f4984b7c6bd46f7ff4f87744c3abd"
        if (
            command(["git", "rev-parse", "HEAD"], source) != pinned
            or command(["git", "status", "--porcelain", "--untracked-files=no"], source)
        ):
            parser.error("Expected the clean pinned Day Trader source used by the loss review")
    out = args.output.resolve()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    with (out / "run.lock").open("x") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        strategies = (
            {name: {"policy": policy, "selection": "all", "allocation": "equal"}
             for name, policy in (("incumbent", "rebound"), ("candidate", "rebound_net_target"))}
            if args.market == "KR" else
            {"incumbent": {"min_reward_risk": 1.0}, "candidate": {"min_reward_risk": 2.0}}
        )
        plan = {
            "market": market.code, "day": day, "start": start.isoformat(), "end": end.isoformat(),
            "capital": capital, "strategies": strategies, "loss_review": loss,
            "previous_decision": None if args.previous_decision is None else {
                "path": str(args.previous_decision.resolve()),
                "sha256": file_digest(args.previous_decision),
            },
            "evidence": str(args.evidence.resolve()), "evidence_sha256": file_digest(args.evidence),
            "day_trader_source": str(source) if source is not None else None,
            "source_commit": command(["git", "rev-parse", "HEAD"], ROOT),
            "source_digest": tree_digest(ROOT),
            "decision_rule": "Missing evidence: repair. Nonnegative delta: retest. "
            "Negative qualified delta: review and revise. A single session cannot promote.",
        }
        plan_path = out / "plan.json"
        atomic_json(plan_path, plan)
        atomic_json(out / "calendar.json", calendar)
        try:
            if args.market == "KR":
                exit_code, result_path = run_kr(out, plan)
            else:
                assert source is not None
                exit_code, result_path = run_us(out, plan, source)
            result = evaluate(
                args.market, read_json(result_path) if result_path.is_file() else {}, exit_code == 0
            )
            result.update(result_path=str(result_path), runner_exit=exit_code)
            if result_path.is_file():
                result["result_sha256"] = file_digest(result_path)
        except (
            ApiError, SafetyError, OSError, ValueError, KeyError, RuntimeError,
            subprocess.SubprocessError,
        ) as error:
            exit_code = 1
            result = {"decision": "repair_evidence", "reason": str(error)}
        atomic_json(out / "decision.json", {
            "market": market.code, "day": day, "plan_sha256": file_digest(plan_path), **result,
        })
        return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
