"""Review a terminal paper comparison and freeze the next market-session plan."""

from __future__ import annotations

import argparse
import fcntl
import math
from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from shutil import copyfile
from typing import Any

from papertrade.jsonio import atomic_json, read_json
from tools.adaptive_journal import AdaptiveJournal, JournalError
from tools.paper_campaign.evidence import compact_rows
from tools.paper_market import KR, US
from tools.source_identity import file_digest
from tools.upstream_paper_accounts import reward_risk_profiles

SLOT = "rebound-entry-cost"
ROLES = ("incumbent", "candidate")
MAX_RETEST_LOSS_PCT = 5.0


def _artifact(path: Path, root: Path) -> dict[str, str]:
    return {"path": str(path.relative_to(root)), "sha256": file_digest(path)}


def _result_path(run: Path, market: str) -> Path:
    if market == "US":
        return run / "session" / "comparison.json"
    schedule = run / "schedule"
    status = read_json(schedule / "status.json")
    restarts = status.get("restarts", [])
    if restarts:
        schedule = schedule / "attempts" / str(len(restarts) + 1)
    return schedule / "run" / "status.json"


def _money(value: Any) -> Decimal:
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("Invalid realized or marked amount")
    return number


def _reward_risk(sizing: dict[str, Any]) -> Decimal:
    entry = _money(sizing["entry"])
    stop = _money(sizing["stop"])
    if entry <= stop:
        raise ValueError("US fill has an invalid reward/risk basis")
    return (_money(sizing["target"]) - entry) / (entry - stop)


def _us_review(
    journal_path: Path, plan: dict[str, Any], report: dict[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    thresholds = {role: plan["strategies"][role]["min_reward_risk"] for role in ROLES}
    profiles = reward_risk_profiles(*[thresholds[role] for role in ROLES])
    names = dict(zip(ROLES, profiles, strict=True))
    symbols: dict[str, dict[str, Any]] = {role: {} for role in ROLES}
    blocked: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with AdaptiveJournal.read_completed(journal_path, report["chain_sha256"]) as journal:
        identity = journal.identity
        if (
            identity["day"] != plan["day"]
            or identity["min_reward_risk_by_profile"] != profiles
            or journal.evidence()
            != {key: report[key] for key in ("records", "cursor", "chain_sha256")}
        ):
            raise ValueError("US journal does not match the frozen comparison")
        for event in journal.events():
            for role, profile in names.items():
                for trade in event["outputs"].get("accounts", {}).get(profile, []):
                    kind, status = trade["kind"], trade["status"]
                    sizing = trade.get("sizing", {})
                    fill = (
                        kind in {"entry_confirmation", "exit_confirmation"} and status == "filled"
                    )
                    refusal = sizing.get("refusal")
                    threshold_refusal = (
                        role == "candidate"
                        and kind in {"entry_decision", "entry_confirmation"}
                        and status in {"blocked", "cancelled"}
                        and isinstance(refusal, str)
                        and refusal.startswith("reward:risk ")
                        and "is under your minimum 2," in refusal
                    )
                    if not fill and not threshold_refusal:
                        continue
                    symbol = trade["symbol"]
                    row = {
                        "event": event["index"],
                        "completed_at": event.get("completed_at"),
                        **{
                            key: trade[key]
                            for key in (
                                "kind",
                                "status",
                                "quantity",
                                "price",
                                "reason",
                                "realized_usd",
                                "remaining",
                                "quote_source",
                                "quote_received",
                            )
                            if key in trade
                        },
                    }
                    if sizing:
                        row["sizing"] = {
                            key: sizing[key]
                            for key in (
                                "entry_plan",
                                "entry",
                                "target",
                                "stop",
                                "reward_risk",
                                "refusal",
                            )
                            if key in sizing
                        }
                    if threshold_refusal:
                        blocked[symbol].append(row)
                        continue
                    fills = symbols[role].setdefault(symbol, {"entries": [], "exits": []})
                    fills["entries" if kind == "entry_confirmation" else "exits"].append(row)
    accounts = {
        role: {**report["accounts"][profile], "symbols": symbols[role]}
        for role, profile in names.items()
    }
    for account in accounts.values():
        realized = []
        entry_count = exit_count = closed_positions = 0
        for details in account["symbols"].values():
            entry_count += len(details["entries"])
            exit_count += len(details["exits"])
            closed_positions += sum(fill["remaining"] == 0 for fill in details["exits"])
            amounts = [_money(fill["realized_usd"]) for fill in details["exits"]]
            realized.extend(amounts)
            details["realized_usd"] = str(sum(amounts, Decimal(0)))
            details["loss_fills"] = [
                fill["event"]
                for fill, amount in zip(details["exits"], amounts, strict=True)
                if amount < 0
            ]
        account["closed_positions"] = closed_positions
        counts = account["counts"]
        if (
            entry_count != counts.get("entry_confirmation:filled", 0)
            or exit_count != counts.get("exit_confirmation:filled", 0)
            or abs(sum(realized, Decimal(0)) - _money(account["realized_usd"])) > Decimal("1e-12")
        ):
            raise ValueError("US journal fills do not reconcile with terminal account metrics")
    supported = []
    for symbol, fills in symbols["incumbent"].items():
        position: dict[str, Any] | None = None
        exits: list[dict[str, Any]] = []
        for fill in sorted(fills["entries"] + fills["exits"], key=lambda row: row["event"]):
            if fill["kind"] == "entry_confirmation":
                position, exits = fill, []
                continue
            exits.append(fill)
            if position is None or fill["remaining"] != 0:
                continue
            sizing = position["sizing"]
            profit = sum((_money(row["realized_usd"]) for row in exits), Decimal(0))
            if profit > 0 and Decimal("1.5") <= _reward_risk(sizing) < 2:
                for refusal in blocked.get(symbol, ()):
                    if refusal["event"] == position["event"] and all(
                        refusal["sizing"][key] == sizing[key]
                        for key in ("entry_plan", "entry", "target", "stop")
                    ):
                        supported.append(
                            {
                                "symbol": symbol,
                                "entry_event": position["event"],
                                "refusal_event": refusal["event"],
                                "candidate_refusal": refusal,
                                "exit_events": [exit_fill["event"] for exit_fill in exits],
                                "observed_net_realized_usd": str(profit),
                                "observed_reward_risk": str(_reward_risk(sizing)),
                            }
                        )
            position = None
    return accounts, supported


def _kr_review(
    report: dict[str, Any], journal_path: Path
) -> tuple[dict[str, Any], list[str], bool]:
    accounts = compact_rows(report, "KRW").get(SLOT, {})
    slot: dict[str, Any] = next(
        (row for row in report.get("shared_slots", []) if row["slot_id"] == SLOT), {}
    )
    details = slot.get("comparison", {}).get("accounts", {})
    limitations = [
        (
            "Terminal KR return uses a midpoint mark for open positions. "
            "Bid liquidation equity is an estimate. The comparison records paper fills."
        )
    ]
    for name, metrics in accounts.items():
        row = details[name]
        paper = row.get("paper") or {}
        metrics.update(
            {
                "input_sha256": row.get("input_sha256"),
                "mark_basis": paper.get("account", {}).get("mark_basis"),
                "positions": paper.get("positions", {}),
                "fill_outcomes": [
                    outcome
                    for outcome in paper.get("outcomes", ())
                    if outcome.get("status") in ("filled", "partial")
                ],
                "net_target_blocked_entries": paper.get("net_target_blocked_entries", []),
                "spread_guard_counts": paper.get("spread_guard_counts", {}),
                "exit_reason_counts": paper.get("exit_reason_counts", {}),
                "realized_fills": [],
            }
        )
    chain = report.get("journal", {}).get("chain_sha256")
    if not chain or not journal_path.is_file():
        limitations.append("Per-fill realized amounts are unavailable from the KR journal.")
        return accounts, limitations, False
    with AdaptiveJournal.read_completed(journal_path, chain) as journal:
        if journal.evidence() != report["journal"]:
            raise ValueError("KR journal does not match the terminal account metrics")
        for event in journal.events():
            attached = event.get("outputs", {}).get(SLOT, {}).get("account_results", {})
            final = event.get("account_results", {}).get(SLOT, {})
            for name, metrics in accounts.items():
                cut = attached.get(name) or final.get(name) or {}
                for trade in cut.get("trades", ()):
                    if trade.get("realized_pnl") is not None:
                        metrics["realized_fills"].append({"event": event["index"], **trade})
    for metrics in accounts.values():
        fills = metrics["realized_fills"]
        total = metrics.get("closed_net_realized")
        if total is None or not math.isclose(
            sum(fill["realized_pnl"] for fill in fills), total, abs_tol=1e-7
        ):
            limitations.append(
                "The KR journal's available fill details do not cover all realized P&L."
            )
            return accounts, limitations, False
    return accounts, limitations, True


def _recommend(
    market: str, strategies: dict[str, Any], review: dict[str, Any], complete: bool
) -> tuple[str, str]:
    if not complete:
        return "repair_evidence", "Repair the incomplete or unmarkable paired comparison."
    key = "closed" if market == "KR" else "closed_positions"
    closed = any(review["accounts"][role][key] for role in ROLES)
    delta = review["candidate_minus_incumbent_pct"]
    if delta is None or not closed or delta == 0:
        return (
            "retest",
            (
                "Repeat the frozen pair on the next unseen market session to collect evidence. "
                "The qualified comparison provided no closed-fill performance difference."
            ),
        )
    if delta > 0:
        cand = review["accounts"]["candidate"]
        cand_return = (
            cand.get("fee_inclusive_return_pct")
            if market == "KR"
            else (
                None
                if cand.get("net_pnl_usd") is None or not cand.get("seed_usd")
                else float(_money(cand["net_pnl_usd"]) / _money(cand["seed_usd"]) * 100)
            )
        )
        if cand_return is not None and cand_return <= -MAX_RETEST_LOSS_PCT:
            return (
                "hold_for_new_hypothesis",
                (
                    f"Candidate outperformed incumbent by {delta:.2f} pct, "
                    f"but closed with a {cand_return:.2f}% net return "
                    f"(below the -{MAX_RETEST_LOSS_PCT:.1f}% auto-retest floor). "
                    "Hold for a new hypothesis instead of repeating the losing strategy."
                ),
            )
        return "retest", "Repeat the frozen pair on the next unseen market session."
    if (
        market == "US"
        and strategies["candidate"] == {"min_reward_risk": 2.0}
        and review["supported_threshold_observations"]
    ):
        strategies["candidate"] = {"min_reward_risk": 1.5}
        return "revise", (
            "Test a 1.5 minimum against the unchanged incumbent. The journal contains "
            "a profitable completed incumbent position whose fill was refused by the "
            "2.0 candidate at the same event. A future fill and return are unverified."
        )
    return "hold_for_new_hypothesis", "Candidate underperformed. Review a new testable hypothesis."


def _review(run: Path, plan: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    market = plan["market"]
    review: dict[str, Any] = {
        "market": market,
        "day": plan["day"],
        "accounts": {},
        "previous_artifacts": {},
        "candidate_minus_incumbent_pct": None,
        "supported_threshold_observations": [],
        "quality": "incomplete",
        "limitations": [],
    }
    artifacts = review["previous_artifacts"]
    try:
        decision_path = run / "decision.json"
        plan_path = run / "plan.json"
        artifacts["plan"] = _artifact(plan_path, run)
        artifacts["calendar"] = _artifact(run / "calendar.json", run)
        artifacts["decision"] = _artifact(decision_path, run)
        decision = read_json(decision_path)
        if decision["plan_sha256"] != artifacts["plan"]["sha256"] or (
            decision["market"],
            decision["day"],
        ) != (market, plan["day"]):
            raise ValueError("Terminal decision does not match its frozen market plan")
        result_path = _result_path(run, market)
        artifacts["result"] = _artifact(result_path, run)
        if decision["result_sha256"] != artifacts["result"]["sha256"]:
            raise ValueError("Terminal result digest does not match its decision")
        report = read_json(result_path)
        review["terminal_state"] = report["state"]
        journal = (
            result_path.with_name("journal.sqlite3")
            if market == "US"
            else run / "schedule" / "journal.sqlite3"
        )
        if journal.is_file():
            artifacts["journal"] = _artifact(journal, run)
        if market == "KR":
            accounts, limitations, journal_complete = _kr_review(report, journal)
            review.update(accounts=accounts, limitations=limitations)
            a, b = (accounts[role] for role in ROLES)
            complete = (
                journal_complete
                and report.get("state") == "complete"
                and report["shared_feed"]["complete"] is True
                and a["initial_equity"] == b["initial_equity"] == plan["capital"]
                and bool(a["input_sha256"])
                and a["input_sha256"] == b["input_sha256"]
                and a["comparison_eligible"]
                and b["comparison_eligible"]
                and len(report["shared_slots"]) == 1
            )
            marks = [row["fee_inclusive_return_pct"] for row in (a, b)]
        else:
            review["limitations"] = report.get("limitations", []).copy()
            profiles = reward_risk_profiles(
                plan["strategies"]["incumbent"]["min_reward_risk"],
                plan["strategies"]["candidate"]["min_reward_risk"],
            )
            review["accounts"] = {
                role: report["accounts"].get(profile, {})
                for role, profile in zip(ROLES, profiles, strict=True)
            }
            accounts, supported = _us_review(journal, plan, report)
            review.update(accounts=accounts, supported_threshold_observations=supported)
            a, b = (accounts[role] for role in ROLES)
            complete = (
                report["state"] == "closed"
                and report["comparison_quality"] == "observed"
                and _money(a["seed_usd"]) == _money(b["seed_usd"]) == _money(plan["capital"])
            )
            marks = [
                None
                if row["net_pnl_usd"] is None
                else float(_money(row["net_pnl_usd"]) / _money(row["seed_usd"]) * 100)
                for row in (a, b)
            ]
        complete = (
            complete
            and decision["runner_exit"] == 0
            and decision["decision"] in {"retest", "revise"}
            and all(value is not None and math.isfinite(value) for value in marks)
        )
        if complete:
            assert marks[0] is not None and marks[1] is not None
            review["candidate_minus_incumbent_pct"] = marks[1] - marks[0]
            review["quality"] = "qualified"
        return review, complete
    except (OSError, ValueError, TypeError, KeyError, JournalError, InvalidOperation) as error:
        review["limitations"].append(str(error))
        return review, False


def _phase(output: Path, phase: str) -> None:
    path = output / "lifecycle.json"
    state = read_json(path) if path.is_file() else {}
    at = datetime.now(UTC).isoformat()
    state.setdefault("phases", {})[phase] = at
    state.update(phase=phase, updated_at=at)
    atomic_json(path, state)


def _next_window(
    run: Path, plan: dict[str, Any], next_day: str, calendar: dict[str, Any] | None
) -> tuple[datetime, datetime, dict[str, Any]]:
    market = KR if plan["market"] == "KR" else US
    prior_calendar = read_json(run / "calendar.json")
    next_business_day = prior_calendar["nextBusinessDay"]
    if (
        plan["day"] != prior_calendar["today"]["date"]
        or date.fromisoformat(next_day) <= date.fromisoformat(plan["day"])
        or next_business_day["date"] != next_day
    ):
        raise ValueError("Next date does not match the prior market calendar's next business day")
    selected = calendar if calendar is not None else {"today": next_business_day}
    start, end = market.session(selected, next_day)
    return start, end, selected


def write_handoff(
    run: Path,
    next_day: str,
    next_start: datetime,
    *,
    output_dir: Path | None = None,
    calendar: dict[str, Any] | None = None,
) -> bool:
    """Write the post-session review and return whether the frozen plan can run."""
    output = run if output_dir is None else output_dir
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (output / "handoff.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        next_path = output / "next_plan.json"
        plan = read_json(run / "plan.json")
        if next_path.exists():
            existing = read_json(next_path)
            if (
                existing["day"] != next_day
                or existing["market"] != plan["market"]
                or datetime.fromisoformat(existing["start"]) != next_start
                or existing["previous_plan"]["sha256"] != file_digest(run / "plan.json")
            ):
                raise ValueError("Next-session plan already frozen for different inputs")
            if existing["executable"] is True:
                load_next_plan(next_path, plan["market"], next_day)
                _phase(output, "next_plan_ready")
                return True
            _phase(output, "blocked")
            return False
        _phase(output, "reviewing")
        review, complete = _review(run, plan)
        _phase(output, "reconsidering")
        end = None
        calendar_link = None
        try:
            expected_start, end, selected_calendar = _next_window(run, plan, next_day, calendar)
            if expected_start != next_start:
                raise ValueError("Next-session opening differs from the market calendar")
            calendar_path = output / "next-calendar.json"
            atomic_json(calendar_path, selected_calendar)
            calendar_link = _artifact(calendar_path, output)
        except (OSError, ValueError, KeyError, TypeError) as error:
            review["limitations"].append(str(error))
            complete = False
        strategies = plan["strategies"].copy()
        action, reason = _recommend(plan["market"], strategies, review, complete)
        if calendar_link is not None and datetime.now(UTC) >= next_start:
            elapsed = "The next verified market session has already opened."
            review["limitations"].append(elapsed)
            if action in {"retest", "revise"}:
                review.update(reviewed_action=action, reviewed_reason=reason)
                action, reason = "hold_elapsed", elapsed
        executable = action in {"retest", "revise"}
        review.update(action=action, reason=reason, executable=executable)
        review_path = output / "review.json"
        atomic_json(review_path, review)
        for name in ("plan", "decision"):
            original = run / f"{name}.json"
            if original.is_file():
                archive = output / f"prior-{name}.json"
                copyfile(original, archive)
                observed = review["previous_artifacts"].get(name)
                if observed and file_digest(archive) != observed["sha256"]:
                    raise ValueError(f"Prior {name} changed during review")
        prior_decision = output / "prior-decision.json"
        atomic_json(
            next_path,
            {
                "market": plan["market"],
                "day": next_day,
                "start": next_start.isoformat(),
                "end": end.isoformat() if end else None,
                "action": action,
                "reason": reason,
                "executable": executable,
                "capital": plan["capital"],
                "strategies": strategies,
                "evidence": plan["evidence"],
                "evidence_sha256": plan["evidence_sha256"],
                "previous_day": plan["day"],
                "previous_plan": _artifact(output / "prior-plan.json", output),
                "previous_decision": _artifact(prior_decision, output)
                if prior_decision.is_file()
                else None,
                "review": _artifact(review_path, output),
                "calendar": calendar_link,
                "previous_artifacts": review["previous_artifacts"],
            },
        )
        _phase(output, "next_plan_ready" if executable else "blocked")
        return executable


def _linked(path: Path, link: dict[str, str], name: str) -> Path:
    if link["path"] != name:
        raise ValueError("Frozen next-session plan contains an unexpected artifact path")
    target = path.with_name(name)
    if file_digest(target) != link["sha256"]:
        raise ValueError(f"Frozen next-session plan does not match {name}")
    return target


def load_next_plan(path: Path, market: str, day: str) -> dict[str, Any]:
    plan = read_json(path)
    if (plan["market"], plan["day"]) != (market, day) or plan["executable"] is not True:
        raise ValueError("Next-session plan is blocked or intended for another market/date")
    if plan["action"] not in {"retest", "revise"}:
        raise ValueError("Next-session plan has an unsupported action")
    prior_path = _linked(path, plan["previous_plan"], "prior-plan.json")
    decision_path = _linked(path, plan["previous_decision"], "prior-decision.json")
    review_path = _linked(path, plan["review"], "review.json")
    calendar_path = _linked(path, plan["calendar"], "next-calendar.json")
    prior, decision, review = map(read_json, (prior_path, decision_path, review_path))
    if (
        not plan["previous_day"] == prior["day"] == decision["day"] == review["day"] < day
        or not prior["market"] == decision["market"] == review["market"] == market
        or plan["capital"] != prior["capital"]
        or plan["evidence_sha256"] != prior["evidence_sha256"]
        or plan["evidence"] != prior["evidence"]
        or decision["plan_sha256"] != plan["previous_plan"]["sha256"]
        or decision["result_sha256"] != plan["previous_artifacts"]["result"]["sha256"]
        or plan["previous_artifacts"] != review["previous_artifacts"]
        or review["quality"] != "qualified"
        or review["executable"] is not True
        or review["action"] != plan["action"]
    ):
        raise ValueError("Next-session plan does not reconcile with its evidence")
    strategies = prior["strategies"].copy()
    action, _ = _recommend(market, strategies, review, True)
    if plan["action"] != action or plan["strategies"] != strategies:
        raise ValueError("Next-session strategy settings do not match the reviewed decision")
    start, end = (datetime.fromisoformat(plan[field]) for field in ("start", "end"))
    venue = KR if market == "KR" else US
    if (start, end) != venue.session(read_json(calendar_path), day):
        raise ValueError("Next-session plan has an invalid market window")
    plan["previous_decision"]["path"] = str(decision_path.resolve())
    return plan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument(
        "--calendar", type=Path, required=True, help="Recorded next-session calendar"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        plan = read_json(args.run / "plan.json")
        calendar = read_json(args.calendar)
        day = calendar["today"]["date"]
        start, _, _ = _next_window(args.run, plan, day, calendar)
        executable = write_handoff(args.run, day, start, output_dir=args.output, calendar=calendar)
    except (OSError, ValueError, TypeError, KeyError) as error:
        parser.error(str(error))
    print(args.output / "next_plan.json")
    return 0 if executable else 1


if __name__ == "__main__":
    raise SystemExit(main())
