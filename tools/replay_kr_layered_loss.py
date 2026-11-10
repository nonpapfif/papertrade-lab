"""Reproduce a sealed KR shared-slot run and test changes on its captured quote tape.

Run from the frozen source tree with PYTHONPATH set to that tree. The journal
and prior status are read-only. Recorded adaptive decisions are reused.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator
from contextlib import closing
from pathlib import Path
from statistics import median
from typing import Any, cast

from papertrade.jsonio import journal_json as encode
from tools.adaptive_journal import AdaptiveJournal, JournalError, _header
from tools.async_layered_paper import AsyncLayeredPaper, seconds
from tools.layered_paper import Feature, assign
from tools.paper_market import KR
from tools.run_async_live_validation import Runtime, journal_identity, subscription_set
from tools.shared_feed_slots import SharedFeedSlots
from tools.slot_control import SlotControl

EXPERIMENTS = {
    "incumbent": ("control", "selection_spread_23bp", "zero_cost_resized"),
    "candidate": (
        "control",
        "selection_spread_23bp",
        "entry_rebound_net_target",
        "entry_rebound_net_target_equal",
        "exit_rebound_executable_target",
        "allocation_frozen_equal",
        "zero_cost_resized",
    ),
}


class ReplayJournal(AdaptiveJournal):
    def events(self) -> Iterator[dict[str, Any]]:
        for entry in super().events():
            if entry["index"] % 100_000 == 1:
                print(
                    f"Verified replay through journal record {entry['index'] - 1}", file=sys.stderr
                )
            yield entry


def quote_bounds(book: dict[str, Any]) -> tuple[float, float]:
    return (
        max(float(row["price"]) for row in book["bids"] if float(row["volume"]) >= 1),
        min(float(row["price"]) for row in book["asks"] if float(row["volume"]) >= 1),
    )


class Experiment(AsyncLayeredPaper):
    def __init__(
        self,
        config: dict[str, Any],
        variant: str,
        eligible: Callable[[str, str, str], list[int]],
    ) -> None:
        fees = config["fee_schedule"].copy()
        if variant == "zero_cost_resized":
            fees.update(commission_rate=0.0, stock_sale_tax_rate=0.0)
        super().__init__(
            config["capital"],
            delay=config["delay_seconds"],
            policy=config["policy"],
            selection=config["selection"],
            allocation=config["allocation"],
            fees=fees,
            market=KR,
        )
        self.variant = variant
        self.eligible = eligible
        self.vetoed_bars: set[tuple[int, str, str]] = set()
        self.excluded_at_dispatch: set[int] = set()

    def _process(self, event: dict[str, Any]) -> None:
        # Runtime computes its candidate count before the account expires pending intents.
        self.excluded_at_dispatch = self.pending.keys() | self.book.positions.keys()
        super()._process(event)

    def _intent(self, event: dict, f: Feature, strategy: str, side: str) -> None:
        bid, ask = quote_bounds(event["book"])
        veto = False
        if side == "BUY":
            if self.variant == "selection_spread_23bp":
                veto = (ask - bid) / ((ask + bid) / 2) > 0.0023
            elif self.variant.startswith("entry_rebound_net_target") and strategy == "rebound":
                limit = KR.limit_price(ask, "BUY")
                # KR charges scale with quantity, and a BUY fill cannot exceed its limit.
                target = f.mean
                target_net = target - self.book.fees(target, 1, "SELL")["total_krw"]
                veto = target_net <= self.book.entry_cost(1, limit)
        elif self.variant == "exit_rebound_executable_target" and strategy == "rebound":
            _, entry = self.owners[event["symbol"]]
            veto = f.r5 >= -0.003 and bid < entry.mean
        if veto:
            self.vetoed_bars.add((event["symbol"], side, f.bar_end))
            return
        if side == "BUY" and self.allocation == "equal":
            candidates = self.eligible(self.policy, self.selection, event["received_at"])
            count = max(1, len(set(candidates) - self.excluded_at_dispatch))
            event = {**event, "candidate_count": count}
        super()._intent(event, f, strategy, side)


class LossRuntime(Runtime):
    """Observe verified baseline deliveries and feed independent alternative accounts."""

    def __init__(self, capital: float, record: Any, identity: dict[str, Any]) -> None:
        super().__init__(
            capital,
            record,
            profile=identity["profile"],
            adaptive=True,
            end_epoch=identity["end_epoch"],
            market=KR,
        )
        self.experiments: dict[str, dict[str, Experiment]] = {}
        self.confirmations: dict[str, list[tuple[float, float]]] = defaultdict(list)
        self.candidates_at_book: dict[tuple[str, str, str], list[int]] = {}
        self.observation: bytes | None = None

    def eligible_at(self, policy: str, selection: str, at: str) -> list[int]:
        key = policy, selection, at
        if key not in self.candidates_at_book:
            self.candidates_at_book[key] = [
                self.ids[symbol]
                for symbol in self.candidates
                if symbol in self.entry_watch
                and (f := self._feature_at(symbol, at)) is not None
                and 0 <= seconds(at) - seconds(f.bar_end) <= 90
                and assign(f, policy) is not None
                and (selection != "active" or abs(f.r5) >= 0.001 or abs(f.r15) >= 0.003)
            ]
        return self.candidates_at_book[key]

    def _record_delivery(self, name: str, observation: bytes) -> None:
        super()._record_delivery(name, observation)
        if name == "incumbent":
            self.observation = observation

    async def _consume_event(
        self,
        entry: dict[str, Any],
        queue_delay: float,
        *,
        saved: dict[str, Any] | None = None,
        replaying: bool = False,
        feature_cut: dict[tuple[Any, ...], Feature | None] | None = None,
    ) -> None:
        if not replaying and self.review_settings is not None:
            raise JournalError("Offline replay cannot run live reviews")
        if not self.experiments:
            self.experiments = {
                name: {
                    variant: Experiment(self.configurations[name], variant, self.eligible_at)
                    for variant in variants
                }
                for name, variants in EXPERIMENTS.items()
            }
        before = {name: len(account.book.trades) for name, account in self.accounts.items()}
        self.observation = None
        await super()._consume_event(
            entry, queue_delay, saved=saved, replaying=replaying, feature_cut=feature_cut
        )
        if entry["kind"] == "policy_review":
            for name, variants in self.experiments.items():
                baseline = self.accounts[name]
                for variant, account in variants.items():
                    account.policy = baseline.policy
                    account.selection = baseline.selection
                    account.allocation = (
                        "equal"
                        if variant in ("allocation_frozen_equal", "entry_rebound_net_target_equal")
                        else baseline.allocation
                    )
                    account.entry_policy_version = baseline.entry_policy_version
        elif entry["kind"] == "clock" and self.observation is not None:
            for variants in self.experiments.values():
                for account in variants.values():
                    await account.advance(entry["at"])
        elif entry["kind"] == "book" and self.observation is not None:
            event = json.loads(self.observation)
            for name, account in self.accounts.items():
                self.confirmations[name].extend(
                    quote_bounds(event["book"]) for _ in account.book.trades[before[name] :]
                )
            if event["feature"] is not None:
                event["feature"] = Feature(**event["feature"])
            self.candidates_at_book.clear()
            for variants in self.experiments.values():
                for account in variants.values():
                    # The original dispatch awaits each event. Serial application preserves order.
                    account._process(event)


def account_attribution(
    account: AsyncLayeredPaper,
    confirmations: list[tuple[float, float]],
    symbols: dict[int, str],
    listings: dict[str, str],
) -> dict[str, Any]:
    intents = {intent.id: intent for intent in account.intents}
    fills = [outcome for outcome in account.outcomes if outcome["status"] in ("filled", "partial")]
    rows = []
    open_entries: dict[int, dict[str, Any]] = {}
    last_exits: dict[int, dict[str, Any]] = {}
    by_symbol: dict[int, Counter[str]] = defaultdict(Counter)
    durations: list[float] = []
    for trade, outcome, (bid, ask) in zip(account.book.trades, fills, confirmations, strict=True):
        intent = intents[outcome["intent_id"]]
        symbol = intent.symbol
        quantity = trade["quantity_normalized"]
        midpoint = (ask + bid) / 2
        spread = abs(quantity) * (ask - bid) / 2
        depth = quantity * (trade["price"] - (ask if quantity > 0 else bid))
        total = by_symbol[symbol]
        total.update(
            {
                "fills": 1,
                "quoted_half_spread_krw": spread,
                "execution_vs_touch_krw": depth,
                "fees_krw": trade["fee_krw"],
            }
        )
        decision_bid, decision_ask = quote_bounds(intent.initial)
        row = {
            **trade,
            "intent_id": intent.id,
            "symbol": symbols[symbol],
            "name": listings.get(symbols[symbol]),
            "strategy": intent.strategy,
            "policy_version": intent.policy_version,
            "decision_at": intent.decision_at,
            "feature_bar_end": intent.feature.bar_end,
            "feature_available_at": intent.feature.available_at,
            "feature_price": intent.feature.price,
            "feature_mean": intent.feature.mean,
            "feature_r5": intent.feature.r5,
            "decision_bid": decision_bid,
            "decision_ask": decision_ask,
            "fill_bid": bid,
            "fill_ask": ask,
            "fill_midpoint": midpoint,
            "quoted_half_spread_krw": spread,
            "execution_vs_touch_krw": depth,
        }
        if quantity > 0:
            prior = last_exits.get(symbol)
            row["same_feature_reentry"] = (
                prior is not None
                and prior["feature_bar_end"] == intent.feature.bar_end
                and prior["feature_available_at"] == intent.feature.available_at
            )
            row["rebound_fill_at_or_above_entry_mean"] = (
                intent.strategy == "rebound" and trade["price"] >= intent.feature.mean
            )
            total["entries"] += 1
            total["same_feature_reentries"] += int(row["same_feature_reentry"])
            total["rebound_entries_at_or_above_mean"] += int(
                row["rebound_fill_at_or_above_entry_mean"]
            )
            open_entries[symbol] = row
        else:
            entry = open_entries[symbol]
            held = seconds(trade["fill_at"]) - seconds(entry["fill_at"])
            row["entry_intent_id"] = entry["intent_id"]
            row["held_seconds"] = held
            row["same_feature_exit"] = (
                entry["feature_bar_end"] == intent.feature.bar_end
                and entry["feature_available_at"] == intent.feature.available_at
            )
            if intent.strategy == "rebound":
                row["rebound_momentum_invalidated"] = intent.feature.r5 < -0.003
                row["rebound_feature_reached_entry_mean"] = (
                    intent.feature.price >= entry["feature_mean"]
                )
                row["rebound_decision_bid_reached_entry_mean"] = (
                    decision_bid >= entry["feature_mean"]
                )
            total["exit_fills"] += 1
            total["same_feature_exit_fills"] += int(row["same_feature_exit"])
            total["closed_gross_krw"] += trade["gross_realized_pnl"]
            total["closed_net_krw"] += trade["realized_pnl"]
            if entry["rebound_fill_at_or_above_entry_mean"]:
                total["above_mean_entry_closed_net_krw"] += trade["realized_pnl"]
            if trade["position_closed"]:
                durations.append(held)
                last_exits[symbol] = row
                del open_entries[symbol]
        rows.append(row)

    paper = account.summary()
    for key, position in paper["positions"].items():
        total = by_symbol[int(key)]
        total["open_mark_pnl_krw"] = position["market_value_krw"] - position["entry_cost_krw"]
    for total in by_symbol.values():
        total["net_mark_pnl_krw"] = (
            total["closed_gross_krw"] + total["open_mark_pnl_krw"] - total["fees_krw"]
        )
    summed: Counter[str] = Counter()
    for total in by_symbol.values():
        summed.update(total)
    net = summed["net_mark_pnl_krw"]
    explicit = summed["fees_krw"]
    ledger = paper["account"]
    expected = {
        "mark P&L": (net, ledger["equity"] - ledger["initial_equity"]),
        "fees": (explicit, ledger["fees_krw"]),
        "closed net P&L": (summed["closed_net_krw"], ledger["closed_net_realized_krw"]),
    }
    for measure, (attributed, recorded) in expected.items():
        if not math.isclose(attributed, recorded, rel_tol=0, abs_tol=1e-7):
            raise JournalError(f"Fill attribution does not reconcile with replayed {measure}")
    return {
        "totals": dict(summed),
        "fixed_fill_mark_pnl_without_fees_krw": net + explicit,
        "fixed_fill_mark_pnl_at_midpoints_without_fees_krw": (
            net + explicit + summed["quoted_half_spread_krw"] + summed["execution_vs_touch_krw"]
        ),
        "median_completed_position_seconds": median(durations) if durations else None,
        "by_symbol": [
            {"symbol": symbols[key], "name": listings.get(symbols[key]), **total}
            for key, total in sorted(by_symbol.items(), key=lambda row: row[1]["net_mark_pnl_krw"])
        ],
        "fills": rows,
    }


def stable_status(status: dict[str, Any]) -> dict[str, Any]:
    timing = {
        "queue_delay_seconds",
        "processing_seconds",
        "per_symbol_queue_delay_seconds",
        "max_queue_depth",
    }
    return {
        **{key: status[key] for key in ("ai_calls", "counts", "desired", "protected")},
        "shared_slots": [
            {key: value for key, value in slot.items() if key not in timing}
            for slot in status["shared_slots"]
        ],
    }


def compare_status(replayed: dict[str, Any], recorded: dict[str, Any]) -> None:
    # Persisted status has string object keys, including adaptive owner symbol IDs.
    expected = stable_status(recorded)
    observed = json.loads(encode(stable_status(replayed)))
    mismatched = [key for key in expected if encode(observed[key]) != encode(expected[key])]
    if mismatched:
        details = [
            f"shared_slots[{index}].{key}"
            for index, (live, replay) in enumerate(
                zip(expected["shared_slots"], observed["shared_slots"], strict=True)
            )
            for key in live.keys() | replay.keys()
            if key not in live or key not in replay or encode(live[key]) != encode(replay[key])
        ]
        raise JournalError(
            "Replayed stable status differs from completed status: "
            + ", ".join(mismatched + sorted(details))
        )


def listing_names(path: Path) -> dict[str, str]:
    names: dict[str, str] = {}
    with gzip.open(path, "rt") as stream:
        for line in stream:
            entry = json.loads(line)
            if entry["kind"] != "listings":
                break
            names.update((row["symbol"], row["name"]) for row in entry["rows"])
    return names


def ledger_snapshot(account: AsyncLayeredPaper) -> dict[str, Any]:
    return {
        "summary": account.summary(),
        "intents": account.intents,
        "trades": account.book.trades,
        "decisions": account.decisions,
        "owners": account.owners,
        "owner_policy_versions": account.owner_policy_versions,
        "pending": account.pending,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("x") as stream:
        stream.write(encode(payload) + "\n")


async def replay(capture: Path, output: Path) -> None:
    tool_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    status = json.loads((capture / "run/status.json").read_text())
    if status["state"] != "complete" or len(status["shared_slots"]) != 1:
        raise JournalError("Expected one completed KR paper slot")
    uri = (capture / "journal.sqlite3").resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        db.execute("BEGIN")
        raw, state, _, _, chain = _header(db)
        if state != "complete" or chain != status["journal"]["chain_sha256"]:
            raise JournalError("Journal and completed status do not have the same anchor")
        identity = json.loads(raw)
        expected = journal_identity(
            identity["capital"],
            identity["end_epoch"],
            KR,
            slot_control=Path(identity["slot_control"]),
        )
        if identity != expected:
            raise JournalError("Run replay from the matching frozen source tree")
        journal = ReplayJournal(db, identity)
        if journal.evidence() != status["journal"]:
            raise JournalError("Journal evidence differs from completed status")
        slots = SharedFeedSlots(
            lambda capital, record: LossRuntime(capital, record, identity),
            subscription_set,
            lambda *_args, **_kwargs: None,
            journal,
            cast(SlotControl, None),
        )
        try:
            await slots.restore()
            replayed = slots.status()
            slot = next(iter(slots.slots.values()))
            runtime = cast(LossRuntime, slot.runtime)
            if slot.stopped_at is None:
                raise JournalError("Completed replay has no final slot stop")
            names = listing_names(capture / "run/market.jsonl.gz")
            symbols = {value: key for key, value in runtime.ids.items()}
            baseline = {
                name: ledger_snapshot(account) for name, account in runtime.accounts.items()
            }
            shadows: dict[str, dict[str, Any]] = {}
            for name, variants in runtime.experiments.items():
                shadows[name] = {}
                for variant, alternative in variants.items():
                    await alternative.cancel_pending(slot.stopped_at)
                    shadows[name][variant] = {
                        **ledger_snapshot(alternative),
                        "vetoed_symbol_side_bars": len(alternative.vetoed_bars),
                    }
            snapshot = output.with_suffix(".unverified.json")
            write_json(
                snapshot,
                {
                    "verified": False,
                    "journal": journal.evidence(),
                    "tool_sha256": tool_sha256,
                    "replayed_status": replayed,
                    "baseline": baseline,
                    "shadows": shadows,
                    "confirmation_touches": dict(runtime.confirmations),
                    "symbol_ids": runtime.ids,
                    "listing_names": names,
                },
            )
            if "committed_sequence" in status and status["committed_sequence"] != slots.consumed:
                raise JournalError("Replayed committed sequence differs from completed status")
            compare_status(replayed, status)
            report: dict[str, Any] = {
                "baseline_checkpoint_chain_verified": True,
                "baseline_stable_status_matched": True,
                "shadow_control_matches_baseline": {},
                "tool_sha256": tool_sha256,
                "journal": journal.evidence(),
                "counts": replayed["counts"],
                "committed_sequence": slots.consumed,
                "counterfactual_scope": (
                    "Fixed observed tape and baseline watch, with recorded policies and clocks. "
                    "Coverage for holdings unique to alternatives can be stale or absent. "
                    "Recorded fees and quote fills are assumptions."
                ),
                "accounts": {},
            }
            for name, account in runtime.accounts.items():
                attribution = account_attribution(
                    account, runtime.confirmations[name], symbols, names
                )
                paper = baseline[name]["summary"]
                alternatives = {}
                for variant, candidate in shadows[name].items():
                    result = candidate["summary"]
                    if variant == "control":
                        mismatched = [
                            key
                            for key, value in baseline[name].items()
                            if encode(candidate[key]) != encode(value)
                        ]
                        if mismatched:
                            raise JournalError(
                                f"{name} no-op control diverged: {', '.join(mismatched)}"
                            )
                        report["shadow_control_matches_baseline"][name] = True
                        continue
                    alternatives[variant] = {
                        "vetoed_symbol_side_bars": candidate["vetoed_symbol_side_bars"],
                        "account": result["account"],
                        "positions": result["positions"],
                        "intents": result["intent_count"],
                    }
                report["accounts"][name] = {
                    "account": paper["account"],
                    "positions": paper["positions"],
                    "attribution": attribution,
                    "alternatives": alternatives,
                }
            write_json(output, report)
        finally:
            await slots.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(replay(args.capture, args.output))


if __name__ == "__main__":
    main()
