"""Recurring market-data-only campaign state transitions."""

from __future__ import annotations

import sqlite3
from contextlib import nullcontext, suppress
from datetime import datetime
from pathlib import Path
from typing import Any

from papertrade.jsonio import atomic_json, read_json
from tools.paper_campaign.calendar import MarketCalendar, Window
from tools.paper_campaign.evidence import (
    MAX_BOOK_GAP_SECONDS,
    account_eligible,
    aggregate,
    compact_rows,
    observed_coverage,
    review_metrics,
)
from tools.paper_campaign.policy import (
    BASELINE,
    CAPITALS,
    MIN_CLOSED_TRADES,
    MIN_QUALIFIED_SESSIONS,
    POLICY_VERSION,
    Admission,
    fingerprint,
    next_plan,
)
from tools.paper_campaign.review import AstraReview
from tools.paper_campaign.runner import SessionRunner
from tools.paper_market import PaperMarket
from tools.runtime_audit import RuntimeAudit, RuntimeAuditError
from tools.slot_control import SlotCommand

MIN_SESSION_COVERAGE = 0.8


def _review_history(statistics: dict[str, Any]) -> dict[str, Any]:
    return {
        slot: {
            **stats,
            "latest": review_metrics(
                stats["latest"],
                comparable_pair=stats["comparison_eligible"] and stats["return_baseline_eligible"],
            ),
        }
        for slot, stats in statistics.items()
    }


class Campaign:
    def __init__(
        self,
        output: Path,
        calendar: MarketCalendar,
        runner: SessionRunner,
        review: AstraReview,
        admissions: dict[str, Admission],
        not_before: dict[str, datetime] | None = None,
        *,
        audit: RuntimeAudit | None = None,
    ) -> None:
        self.output = output
        self.calendar = calendar
        self.runner = runner
        self.review = review
        self.admissions = admissions
        self.not_before = not_before or {}
        self.audit = audit

    def tick(self, market: PaperMarket, now: datetime) -> None:
        if self.audit is None:
            self._tick(market, now)
            return
        context = {"market": market.code}
        try:
            with self.audit.operation("campaign", "tick_failed", context):
                if not self.audit.healthy:
                    raise RuntimeAuditError("Campaign audit storage is unhealthy")
                self.audit.heartbeat("campaign", worker_id=market.code, status="active", timeout=120)
                try:
                    self._tick(market, now)
                finally:
                    if self.audit.healthy:
                        with suppress(RuntimeAuditError):
                            self.audit.heartbeat(
                                "campaign", worker_id=market.code, status="waiting"
                            )
        except (OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error) as error:
            try:
                self._handoff(
                    self.output / market.code,
                    f"campaign_{type(error).__name__}",
                    market.code,
                    now.date().isoformat(),
                )
            except (OSError, RuntimeError, ValueError, TypeError) as handoff_error:
                error.add_note(f"Engineering handoff failed: {type(handoff_error).__name__}")
            raise

    def _tick(self, market: PaperMarket, now: datetime) -> None:
        admission = self.admissions[market.code]
        earliest = self.not_before.get(market.code)
        approved_start = earliest.isoformat() if earliest is not None else None
        directory = self.output / market.code
        path = directory / "state.json"
        state = (
            read_json(path)
            if path.exists()
            else {
                "policy": POLICY_VERSION,
                "capacity_digest": admission.evidence_digest,
                "source_commit": self.runner.source_commit,
                "source_digest": self.runner.source_digest,
                "approved_slots": admission.initial(),
                "not_before": approved_start,
                "configs": {},
                "seeds": {},
                "history": [],
                "active": None,
            }
        )
        if (
            state["policy"] != POLICY_VERSION
            or state["capacity_digest"] != admission.evidence_digest
            or state["source_commit"] != self.runner.source_commit
            or state["source_digest"] != self.runner.source_digest
            or state.get("not_before") != approved_start
        ):
            raise ValueError("Campaign source, capacity evidence or evaluation policy changed")
        if state.get("blocked"):
            return
        active = state["active"]
        if active is None:
            if earliest is not None and now < earliest:
                return
            window = self.calendar.window(market, now)
            if (
                window is None
                or now >= window.end
                or any(row["day"] == window.day for row in state["history"])
            ):
                return
            active = self._plan(market, window, state, admission)
            state["active"] = active
            atomic_json(path, state)
        session_dir = directory / active["day"]
        if "evidence" not in active:
            try:
                with (
                    self.audit.operation(
                        "campaign_runner", "session_poll_failed",
                        {"market": market.code, "event_id": active["day"]}, timeout=30,
                    ) if self.audit is not None else nullcontext()
                ):
                    outcome = self.runner.poll(session_dir, active)
            except RuntimeAuditError:
                raise
            except (OSError, RuntimeError, ValueError, TypeError, sqlite3.Error) as error:
                self._block(path, state, session_dir, f"session_{type(error).__name__}")
                return
            if outcome is None:
                self._intraday(path, state, session_dir, market, admission, now)
                return
            if outcome["state"] == "expired":
                state["history"].append({"day": active["day"], "state": "expired"})
                state["active"] = None
                atomic_json(path, state)
                return
            if outcome["state"] != "complete":
                if self.audit is not None:
                    self.audit.record(
                        "campaign_runner", "session_unsuccessful",
                        {"market": market.code, "event_id": active["day"]},
                    )
                self._block(path, state, session_dir, outcome["reason"])
                return
            try:
                audit_scope = (
                    self.audit.operation(
                        "evidence", "terminal_evidence_invalid",
                        {"market": market.code, "event_id": active["day"]},
                    ) if self.audit is not None else nullcontext()
                )
                with audit_scope:
                    self._finish(active, outcome, market, state, audit=self.audit)
            except RuntimeAuditError:
                raise
            except (KeyError, RuntimeError, TypeError, ValueError):
                self._block(path, state, session_dir, "invalid_terminal_evidence")
                return
            atomic_json(path, state)
        self._close_review(path, state, session_dir, market, admission, now)

    @staticmethod
    def _plan(
        market: PaperMarket, window: Window, state: dict[str, Any], admission: Admission
    ) -> dict[str, Any]:
        capital = CAPITALS[market.code]
        slots = [
            {
                "id": f"add-{slot}",
                "action": "add",
                "slot_id": slot,
                "spec": state["configs"][slot]
                if slot in state["configs"]
                else {
                    "capital": capital,
                    "incumbent": BASELINE,
                    "candidate": admission.candidates[slot],
                },
            }
            for slot in state["approved_slots"]
        ]
        return {
            "day": window.day,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "market": market.code,
            "capital": capital,
            "slots": slots,
            "seeds": {
                slot: state["seeds"][slot]
                for slot in state["approved_slots"]
                if slot in state["seeds"]
            },
            "intraday": {"last_fills": {}, "decisions": [], "pending": None},
        }

    def _intraday(
        self,
        path: Path,
        state: dict[str, Any],
        output: Path,
        market: PaperMarket,
        admission: Admission,
        now: datetime,
    ) -> None:
        active = state["active"]
        intraday = active["intraday"]
        if now >= datetime.fromisoformat(active["end"]):
            return
        try:
            live = self.runner.live(output)
        except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error) as error:
            if self.audit is not None:
                self.audit.record(
                    "campaign", "session_status_unavailable",
                    {"market": market.code, "event_id": active["day"]}, error=error,
                    severity="warning",
                )
            return
        if live is None:
            return
        if self.audit is not None:
            self.audit.record(
                "campaign", "session_status_unavailable",
                {"market": market.code, "event_id": active["day"]}, state="resolved",
            )
        summary = live["summary"]
        sequence = summary.get("committed_sequence")
        running = [row["slot_id"] for row in live["slots"] if row["state"] == "active"]
        used = {row["slot_id"] for row in live["slots"]}
        occupied = set(running) | {
            item["slot_id"]
            for item in live["commands"]
            if item["disposition"] is None and item["action"] in ("add", "branch")
        }
        if type(sequence) is not int or not running:
            return
        targets = {
            f"fork-{target}": config
            for target, config in admission.candidates.items()
            if target not in state.get("retired_slots", [])
            and f"fork-{target}" not in used
            and f"fork-{target}" not in state["seeds"]
        }
        metrics = compact_rows(summary, market.currency, as_of=now)
        live_configs = {
            row["slot_id"]: {
                name: {key: details["configuration"][key] for key in BASELINE}
                for name, details in row["comparison"]["accounts"].items()
            }
            for row in summary.get("shared_slots", [])
            if row["state"] == "active"
        }
        fills = {
            f"{slot}.{account}": row["fills"]
            for slot, accounts in metrics.items()
            for account, row in accounts.items()
        }
        eligible_sources = {
            slot: sorted(name for name, account in accounts.items() if account_eligible(account))
            for slot, accounts in metrics.items()
            if slot in running
        }
        pending = intraday["pending"]
        if pending is None:
            statistics = aggregate(state["history"])
            parkable = [
                slot
                for slot in running
                if slot != admission.initial()[0]
                and statistics.get(slot, {}).get("qualified_sessions", 0) >= MIN_QUALIFIED_SESSIONS
                and statistics[slot]["qualified_closed_trades"] >= MIN_CLOSED_TRADES
                and statistics[slot]["comparison_eligible"]
                and statistics[slot]["return_baseline_eligible"]
                and all(
                    row["open_positions"] == row["pending_intents"] == 0 and account_eligible(row)
                    for row in metrics.get(slot, {}).values()
                )
                and len(metrics.get(slot, {})) == 2
            ]
            starting_fills = {
                f"{slot}.{account}": row["fills"]
                for slot, stats in statistics.items()
                for account, row in stats["latest"].items()
            }
            for row in summary.get("shared_slots", []):
                if inherited := (row.get("lineage") or {}).get("source_metrics"):
                    for name in ("incumbent", "candidate"):
                        starting_fills.setdefault(f"{row['slot_id']}.{name}", inherited["fills"])
            old = {**starting_fills, **intraday["last_fills"]}
            if not any(
                fills[f"{slot}.{name}"] > old.get(f"{slot}.{name}", 0)
                for slot, names in eligible_sources.items()
                for name in names
            ):
                return
            pending = {
                "kind": "intraday",
                "market": market.code,
                "currency": market.currency,
                "day": active["day"],
                "sequence": sequence,
                "next_slots": running,
                "eligible_targets": targets,
                "eligible_sources": eligible_sources,
                "metrics": {slot: review_metrics(accounts) for slot, accounts in metrics.items()},
                "source_configurations": live_configs,
                "market_messages": summary.get("market_messages", 0),
                "quality_counts": summary.get("counts", {}),
                "history": _review_history(statistics),
                "parkable": parkable,
                "available_lanes": max(0, admission.max_slots - len(occupied)),
            }
            intraday.update(pending=pending, last_fills=fills)
            atomic_json(path, state)
        decision = self.review.poll(output / "reviews", pending)
        if decision is None:
            return
        target = decision.get("branch_target")
        source = decision.get("branch_source")
        account = decision.get("branch_account")
        park = decision.get("park_slot")
        commands = {item["id"]: item for item in live["commands"]}
        branch_id = f"branch-{target}"
        park_id = f"park-for-{target}"
        existing_branch = commands.get(branch_id)
        existing_park = commands.get(park_id)
        cutoff = datetime.fromisoformat(active["end"]).timestamp()
        if existing_branch:
            decision["branch_dispatched"] = True
        elif (
            decision["quality"] == "sufficient"
            and decision.get("next_slots") == pending["next_slots"]
            and isinstance(source, str)
            and source in running
            and account in ("incumbent", "candidate")
            and account in pending.get("eligible_sources", {}).get(source, [])
            and account in eligible_sources.get(source, [])
            and target in targets
            and target in pending["eligible_targets"]
            and targets[target] != live_configs.get(source, {}).get(account)
            and (park == "" or park != source and park in pending["parkable"])
        ):
            try:
                if park and existing_park is None and len(occupied) >= admission.max_slots:
                    self.runner.branch(output, SlotCommand(park_id, "park", park), cutoff)
                    return
                if existing_park and existing_park["disposition"] is None:
                    return
                if existing_park:
                    decision["park_disposition"] = existing_park["disposition"]
                if len(occupied) < admission.max_slots:
                    request = SlotCommand(
                        branch_id,
                        "branch",
                        target,
                        {
                            "source_slot": source,
                            "source_account": account,
                            "candidate": targets[target],
                            "evidence_sequence": pending["sequence"],
                        },
                    )
                    self.runner.branch(output, request, cutoff)
                    decision["branch_dispatched"] = True
            except (OSError, ValueError, sqlite3.Error) as error:
                if self.audit is not None:
                    self.audit.record(
                        "campaign", "branch_dispatch_failed",
                        {"market": market.code, "request_id": branch_id}, error=error,
                    )
                decision["branch_dispatched"] = False
        intraday["decisions"].append(decision)
        intraday["pending"] = None
        atomic_json(path, state)

    @staticmethod
    def _finish(
        active: dict[str, Any],
        outcome: dict[str, Any],
        market: PaperMarket,
        state: dict[str, Any],
        *,
        audit: RuntimeAudit | None = None,
    ) -> None:
        summary = outcome["summary"]
        checks = summary["checks"]
        rows = summary["shared_slots"]
        command_results = {item["id"]: item["disposition"] for item in outcome["commands"]}
        if any(command_results.get(slot["id"]) != "applied" for slot in active["slots"]):
            raise ValueError("An admitted slot failed to start")
        chain = summary["journal"]["chain_sha256"]
        if (
            summary.get("real_orders") is not False
            or summary.get("ai_calls") != 0
            or summary.get("market") != market.code
        ):
            raise ValueError("Frozen market-data-only session was not verified")
        metrics = compact_rows(summary, market.currency)
        if len(rows) != len(metrics) or any(
            row["state"] not in ("active", "parked") for row in rows
        ):
            raise ValueError("A recurring slot did not reach a reusable session close")
        for row in rows:
            slot = row["slot_id"]
            state["seeds"][slot] = {
                "journal": outcome["journal"],
                "slot_id": slot,
                "chain_sha256": chain,
            }
            configuration = row["comparison"]["accounts"]
            state["configs"][slot] = {
                "capital": active["capital"],
                **{
                    account: {key: configuration[account]["configuration"][key] for key in BASELINE}
                    for account in ("incumbent", "candidate")
                },
            }
        coverage = observed_coverage(
            Path(outcome["journal"]),
            chain,
            datetime.fromisoformat(active["start"]).timestamp(),
            datetime.fromisoformat(active["end"]).timestamp(),
            audit=audit,
            context={"market": market.code, "event_id": active["day"]},
        )
        required = (
            "stream_observed",
            "features_observed",
            "queue_p95_under_one_second",
            "protected_subscription_retained",
            "cash_not_overreserved",
            "matched_inputs",
        )
        initial_slots = [item["slot_id"] for item in active["slots"]]
        active["evidence"] = {
            "kind": "close",
            "day": active["day"],
            "market": market.code,
            "currency": market.currency,
            "state": "complete",
            "coverage": coverage,
            "checks": {key: checks.get(key) is True for key in required},
            "qualified": coverage["fraction"] >= MIN_SESSION_COVERAGE
            and coverage["maximum_gap_seconds"] <= MAX_BOOK_GAP_SECONDS
            and all(checks.get(key) is True for key in required),
            "new_branches": sorted(metrics.keys() - set(initial_slots)),
            "partial_slots": sorted(row["slot_id"] for row in rows if row["state"] == "parked"),
            "branch_cuts": {
                row["slot_id"]: {
                    key: value
                    for key, value in row["lineage"].items()
                    if key
                    in {"source_slot", "source_account", "through_sequence", "source_metrics"}
                }
                for row in rows
                if row["slot_id"] not in initial_slots and row.get("lineage")
            },
            "journal_chain": chain,
            "market_messages": summary.get("market_messages", 0),
            "metrics": metrics,
            "next_slots": sorted(row["slot_id"] for row in rows if row["state"] == "active"),
            "slot_command_dispositions": command_results,
        }

    def _close_review(
        self,
        path: Path,
        state: dict[str, Any],
        output: Path,
        market: PaperMarket,
        admission: Admission,
        now: datetime,
    ) -> None:
        active = state["active"]
        close = active["evidence"]
        stats = aggregate([*state["history"], close])
        retired = set(state.get("retired_slots", []))
        candidates = {
            **admission.candidates,
            **{slot: config["candidate"] for slot, config in state["configs"].items()},
        }
        performance_exclusions = {
            slot
            for slot, row in stats.items()
            if not row["comparison_eligible"] or not row["return_baseline_eligible"]
        }
        running = close["next_slots"]
        parked = [slot for slot in close["partial_slots"] if slot not in retired]
        previous = list(dict.fromkeys([*running, *parked]))
        required_carryover = {
            slot for slot in previous if stats.get(slot, {}).get("has_exposure", True)
        }
        evidence = {
            **close,
            "metrics": {
                slot: review_metrics(accounts, comparable_pair=slot not in performance_exclusions)
                for slot, accounts in close["metrics"].items()
            },
            "branch_cuts": {
                slot: {
                    key: value
                    for key, value in cut.items()
                    if key != "source_metrics"
                    or (slot not in performance_exclusions and account_eligible(value))
                }
                for slot, cut in close.get("branch_cuts", {}).items()
            },
            "eligible": {
                slot: config
                for slot, config in candidates.items()
                if slot not in retired and slot not in performance_exclusions
            },
            "performance_exclusions": sorted(performance_exclusions),
            "required_carryover": sorted(required_carryover),
            "retired_slots": sorted(retired),
            "max_slots": admission.max_slots,
            "history": _review_history(stats),
        }
        result = self.review.poll(output / "reviews", evidence)
        if result is None:
            next_window = self.calendar.window(market, now)
            if next_window is None or next_window.day == active["day"] or now < next_window.start:
                return
            result = {"quality": "degraded", "reason": "review_pending_at_next_market_open"}
        approved = previous[: admission.max_slots]
        retired_now: list[str] = []
        usable = close["qualified"] and bool(close["metrics"].keys() - performance_exclusions)
        if result["quality"] == "sufficient" and usable:
            try:
                if any(
                    result[key]
                    for key in ("branch_source", "branch_target", "branch_account", "park_slot")
                ):
                    raise ValueError("Close review requested an intraday branch")
                approved = next_plan(
                    result, previous, admission, stats, set(state["seeds"]), retired
                )
                retired_now = sorted(set(previous) - set(approved))
            except (KeyError, TypeError, ValueError) as error:
                if self.audit is not None:
                    self.audit.record(
                        "strategic_review", "unsafe_plan_rejected",
                        {"market": market.code, "event_id": active["day"]}, error=error,
                    )
                result.update(quality="degraded", disposition="unsafe_plan_rejected")
        elif not close["qualified"]:
            result["disposition"] = "degraded_market_evidence"
        elif not usable:
            result["disposition"] = "degraded_comparison_evidence"
        state["approved_slots"] = approved
        state["retired_slots"] = sorted(retired | set(retired_now))
        state["history"].append(
            {
                **close,
                "review": result,
                "approved_slots": approved,
                "retired_slots": retired_now,
                "intraday": active["intraday"],
            }
        )
        state["active"] = None
        atomic_json(path, state)

    def _block(self, path: Path, state: dict[str, Any], output: Path, reason: str) -> None:
        active = state["active"]
        handoff = self._handoff(output, reason, path.parent.name, active["day"])
        state["blocked"] = handoff
        atomic_json(path, state)

    def _handoff(self, output: Path, reason: str, market: str, day: str) -> dict[str, Any]:
        path = output / "engineering_handoff.json"
        identity = fingerprint({
            "market": market, "session": day, "reason": reason,
            "source_commit": self.runner.source_commit,
            "source_digest": self.runner.source_digest,
        })
        if path.exists():
            saved = read_json(path)
            if saved.get("task_id") == identity:
                return saved
        handoff = {
            "state": "blocked",
            "task_id": identity,
            "reason": reason,
            "source_commit": self.runner.source_commit,
            "source_digest": self.runner.source_digest,
            "market": market,
            "session": day,
            "evidence": {
                "scheduler": str(output / "scheduler"),
                "schedule": str(output / "schedule" / "status.json"),
                "journal": str(output / "schedule" / "journal.sqlite3"),
            },
            "tests": ["tests.test_paper_campaign", "tests.test_async_live_validation"],
            "required": "Reconcile the worker exit and committed accounts before any restart.",
        }
        with (
            self.audit.operation(
                "engineering_handoff", "write_failed",
                {"market": market, "request_id": identity}, timeout=10,
            ) if self.audit is not None and self.audit.healthy else nullcontext()
        ):
            atomic_json(path, handoff)
        if self.audit is not None and self.audit.healthy:
            self.audit.record(
                "engineering_handoff", "campaign_blocked",
                {"market": market, "request_id": identity},
            )
        return handoff
