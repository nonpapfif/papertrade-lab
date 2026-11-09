"""Select review metrics without exposing raw market, environment or credential files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from tools.adaptive_journal import AdaptiveJournal
from tools.paper_campaign.policy import numeric


def account_eligible(account: dict[str, Any]) -> bool:
    if "comparison_eligible" in account:
        return account["comparison_eligible"] is True
    # Legacy checkpoints without open positions need no held quote.
    return type(account.get("open_positions")) is int and account["open_positions"] == 0


def _health_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    return stamp if stamp.utcoffset() is not None else None


def _live_quote_exclusion(account: dict[str, Any], as_of: datetime) -> str | None:
    cut = _health_time(account.get("quote_health_as_of"))
    if cut is None:
        return "missing_quote_health"
    if cut > as_of:
        return "future_quote_health"
    positions = account.get("open_positions")
    if type(positions) is not int or positions < 0:
        return "missing_quote_health"
    if positions == 0:
        return None
    expiry = _health_time(account.get("comparison_valid_until"))
    if expiry is None or expiry < cut:
        return "missing_quote_health"
    return "stale_quote_health" if as_of > expiry else None


def compact_rows(
    status: dict[str, Any], currency: str, *, as_of: datetime | None = None
) -> dict[str, Any]:
    slots = {}
    for row in status.get("shared_slots", []):
        accounts = {}
        for name, details in row.get("comparison", {}).get("accounts", {}).items():
            paper = details.get("paper") or {}
            account = paper.get("account", {})
            eligible = account_eligible(account)
            exclusions = account.get("comparison_exclusions", [])
            reason = (
                _live_quote_exclusion(account, as_of) if as_of is not None and eligible else None
            )
            if reason is not None or not eligible and not exclusions:
                exclusions = [
                    {
                        "symbol": None,
                        "reason": reason or "missing_quote_health",
                        "source_at": None,
                        "age_seconds": None,
                    }
                ]
            accounts[name] = {
                "equity": numeric(account.get("equity")),
                "initial_equity": numeric(account.get("initial_equity")),
                "fee_inclusive_return_pct": numeric(account.get("return_pct")),
                "estimated_bid_liquidation_equity": numeric(
                    account.get(f"bid_liquidation_equity_estimate_{currency.lower()}")
                ),
                "fees": numeric(account.get(f"fees_{currency.lower()}")),
                "closed_net_realized": numeric(
                    account.get(f"closed_net_realized_{currency.lower()}")
                ),
                "fills": sum(
                    outcome.get("status") in ("filled", "partial")
                    for outcome in paper.get("outcomes", ())
                ),
                "closed": account.get("closed", 0),
                "entries": account.get("entries", 0),
                "max_drawdown_pct": numeric(account.get("max_drawdown_pct")),
                "open_positions": account.get("open_positions"),
                "stale_open_positions": account.get("stale_open_positions"),
                "comparison_eligible": eligible and reason is None,
                "comparison_exclusions": exclusions,
                "quote_health_as_of": account.get("quote_health_as_of"),
                "comparison_valid_until": account.get("comparison_valid_until"),
                "pending_intents": paper.get("pending_intents"),
                "observations": details.get("observations", 0),
                "book_observations": details.get("book_observations", 0),
            }
        slots[row["slot_id"]] = accounts
    return slots


def review_metrics(accounts: dict[str, Any], *, comparable_pair: bool = True) -> dict[str, Any]:
    return {
        name: (
            account
            if comparable_pair and account_eligible(account)
            else {
                key: value
                for key, value in account.items()
                if key
                in {
                    "open_positions",
                    "stale_open_positions",
                    "pending_intents",
                    "comparison_eligible",
                    "comparison_exclusions",
                    "quote_health_as_of",
                    "comparison_valid_until",
                    "observations",
                    "book_observations",
                }
            }
        )
        for name, account in accounts.items()
    }


def aggregate(history: list[dict[str, Any]]) -> dict[str, Any]:
    statistics: dict[str, Any] = {}
    for session in history:
        for slot, accounts in session.get("metrics", {}).items():
            if not {"incumbent", "candidate"} <= accounts.keys():
                continue
            stats = statistics.setdefault(
                slot,
                {
                    "qualified_sessions": 0,
                    "qualified_closed_trades": 0,
                    "qualified_positive_candidate_sessions": 0,
                    "qualified_candidate_return_pct_total": 0.0,
                    "latest": {},
                },
            )
            prior = stats["latest"]
            current = accounts["candidate"]
            prior_candidate = prior.get("candidate", {})
            end_eligible = all(account_eligible(account) for account in accounts.values())
            baseline_eligible = all(account_eligible(account) for account in prior.values())
            stats["comparison_eligible"] = end_eligible
            stats["return_baseline_eligible"] = baseline_eligible
            stats["has_exposure"] = any(
                account["open_positions"] != 0 or account["pending_intents"] != 0
                for account in accounts.values()
            )
            partial = slot in session.get("new_branches", []) or slot in session.get(
                "partial_slots", []
            )
            if session.get("qualified") and not partial and end_eligible and baseline_eligible:
                stats["qualified_sessions"] += 1
                stats["qualified_closed_trades"] += max(
                    0, current["closed"] - prior_candidate.get("closed", 0)
                )
                starting_equity = prior_candidate.get("equity", current["initial_equity"])
                ending_equity = current["equity"]
                if starting_equity and ending_equity is not None:
                    delta = (ending_equity / starting_equity - 1) * 100
                    stats["qualified_positive_candidate_sessions"] += int(delta > 0)
                    stats["qualified_candidate_return_pct_total"] += delta
            stats["latest"] = accounts
    return statistics


MAX_BOOK_GAP_SECONDS = 180
BOOK_COVERAGE_SECONDS = 60


def observed_coverage(path: Path, chain: str, start: float, end: float) -> dict[str, Any]:
    """Measure intervals containing accepted books across every worker attempt."""
    previous = start
    covered_until = start
    observed = 0
    received = 0
    total_coverage = 0.0
    largest_gap = 0.0
    with AdaptiveJournal.read_completed(path, chain) as journal:
        for event in journal.events():
            if event["kind"] != "book":
                continue
            arrived = datetime.fromisoformat(event["received"]).timestamp()
            if arrived < start or arrived >= end:
                continue
            received += 1
            if not any(
                output.get("accepted_book") is True for output in event.get("outputs", {}).values()
            ):
                continue
            observed += 1
            largest_gap = max(largest_gap, arrived - previous)
            previous = max(previous, arrived)
            through = min(end, arrived + BOOK_COVERAGE_SECONDS)
            total_coverage += max(0.0, through - max(covered_until, arrived))
            covered_until = max(covered_until, through)
    return {
        "fraction": total_coverage / (end - start),
        "received_books": received,
        "accepted_books": observed,
        "maximum_gap_seconds": max(largest_gap, end - previous),
        "book_coverage_seconds": BOOK_COVERAGE_SECONDS,
        "qualification_maximum_gap_seconds": MAX_BOOK_GAP_SECONDS,
    }
