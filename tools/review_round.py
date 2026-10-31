"""Reconcile a read-only paper-ledger snapshot and attribute entry-lot outcomes."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

ZERO = Decimal(0)
TOLERANCE = Decimal("0.000000001")


def dec(value: Any) -> Decimal:
    return Decimal(str(value))


def unpack(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, **json.loads(row["data"])} for row in rows]


def strategy_group(text: str) -> str:
    text = text.lower()
    if any(word in text for word in ("flip", "reversal", "rebound", "failed", "opposite")):
        return "reversal_or_rebound"
    if any(word in text for word in ("scale", "extend", "add to", "increase", "addition")):
        return "scaling_continuation"
    if any(word in text for word in ("breakout", "momentum", "impulse", "continuation")):
        return "continuation"
    return "other_directional"


def summarize(values: list[Decimal]) -> dict[str, Any]:
    wins = [value for value in values if value > TOLERANCE]
    losses = [value for value in values if value < -TOLERANCE]
    return {
        "count": len(values),
        "wins": len(wins),
        "losses": len(losses),
        "flat": len(values) - len(wins) - len(losses),
        "realized": sum(values, ZERO),
        "gross_wins": sum(wins, ZERO),
        "gross_losses": sum(losses, ZERO),
        "average_win": sum(wins, ZERO) / len(wins) if wins else None,
        "average_loss": sum(losses, ZERO) / len(losses) if losses else None,
    }


def review(snapshot: dict[str, Any]) -> dict[str, Any]:
    trades = sorted(unpack(snapshot["trades"]), key=lambda row: row["id"])
    calls = {
        row["id"]: {**json.loads(row["data"]), **{k: v for k, v in row.items() if k != "data"}}
        for row in snapshot["ai_calls"]
    }
    positions = {row["symbol"]: row for row in unpack(snapshot["positions"])}
    quotes = {row["symbol"]: row for row in unpack(snapshot["quotes"])}
    legacy_calls = {
        key: call for call in calls.values() for key in call["event_key"].split("|")
        if key.startswith("momentum-")
    }
    discovery_at = next(
        (row["filled_at"] for row in trades if row["event_key"].startswith("simulation-ai:75:")),
        snapshot["cutoff_utc"],
    )
    lots: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    active: dict[str, dict[str, Any]] = {}
    previous: dict[str, dict[str, Any]] = {}
    records = []
    cash = Decimal(1000)
    realized = ZERO
    max_pnl_error = ZERO
    max_gross_cost = ZERO
    plans = [row for row in snapshot["events"] if row["kind"] == "simulation_plan"]
    plan_index = 0

    for trade in trades:
        while plan_index < len(plans) and plans[plan_index]["at"] <= trade["filled_at"]:
            change = json.loads(plans[plan_index]["data"])
            if change["symbol"] in active:
                active[change["symbol"]]["plan"] = change["plan"]
                active[change["symbol"]]["plan_history"].append(plans[plan_index]["id"])
            plan_index += 1
        symbol = trade["symbol"]
        free = "signed_quantity" in trade
        quantity = dec(trade["quantity"])
        delta = dec(trade["signed_quantity"]) if free else quantity * (
            1 if trade["side"] == "BUY" else -1
        )
        price = dec(trade.get("fill_price", trade.get("entry_price")))
        cash_flow = dec(trade["cash_flow_usd"]) if free else (
            -dec(trade["cost_usd"]) if delta > 0 else dec(trade["proceeds_usd"])
        )
        cash += cash_flow
        unit_value = abs(cash_flow / delta)
        pnl = dec(trade.get("realized_pnl_usd", 0))
        realized += pnl
        episode = active.get(symbol)
        before = episode["quantity"] if episode else ZERO
        if free and abs(before - dec(trade["position_before"])) > TOLERANCE:
            raise ValueError(f"Position-before mismatch at trade {trade['id']}")
        closing = min(abs(before), abs(delta)) if before * delta < 0 else ZERO
        opening = abs(delta) - closing
        call_id = None
        decision = None
        key = trade["event_key"]
        if key.startswith("simulation-ai:"):
            _, raw_call, raw_index = key.split(":")
            call_id = int(raw_call)
            decision = calls[call_id]["decisions"][int(raw_index)]
        elif key.startswith("simulation-exit:simulation-ai:"):
            raw_call, raw_index = key.removeprefix("simulation-exit:simulation-ai:").split(":")
            call_id = int(raw_call)
            decision = calls[call_id]["decisions"][int(raw_index)]
        elif trade.get("signal_key") in legacy_calls:
            call = legacy_calls[trade["signal_key"]]
            call_id = call["id"]
            decision = next((item for item in call.get("decisions", [])
                if item.get("symbol") == symbol), None)
        strategy = (
            decision.get("strategy", trade.get("strategy_profile", ""))
            if decision else trade.get("strategy_profile", "")
        )
        reason = trade.get("reason", "") or trade.get("rationale", "")
        phase = "legacy_v2" if not free else (
            "discovery_open" if trade["filled_at"] >= discovery_at else "free_before_discovery"
        )
        evidence = trade.get("price_evidence", {})
        source_time = evidence.get("source_timestamp")
        source_age = (
            (datetime.fromisoformat(trade["filled_at"]) - datetime.fromisoformat(source_time))
            .total_seconds()
            if source_time else None
        )
        candidate_row = snapshot["job_evidence"].get(f"{call_id}:{symbol}")
        candidate = json.loads(candidate_row["data"]) if candidate_row else {}
        reference = candidate.get("reference", {})
        record = {
            "id": trade["id"], "event_key": key, "symbol": symbol,
            "name": snapshot["names"].get(symbol, symbol), "market": trade["market"],
            "phase": phase, "side": trade["side"], "quantity": quantity,
            "signed_quantity": delta, "before": before, "after": before + delta,
            "price": price, "cash_flow": cash_flow, "cash_after": cash,
            "book_entry_price": dec(trade.get("entry_price", price)),
            "realized": pnl, "filled_at": trade["filled_at"], "call_id": call_id,
            "strategy": strategy, "strategy_group": strategy_group(strategy),
            "proposed_stop": decision.get("stop", trade.get("stop")) if decision else trade.get("stop"),
            "proposed_target": decision.get("target", trade.get("target")) if decision else trade.get("target"),
            "reason_original": reason, "decision_original": decision,
            "source_timestamp": source_time, "source_age_seconds": source_age,
            "candidate_return": candidate.get("relative_return"),
            "candidate_volume_ratio": candidate.get("volume_ratio", trade.get("volume_ratio")),
            "breakout": candidate.get("breakout", trade.get("breakout")),
            "support": candidate.get("support", trade.get("support")),
            "candidate_timestamp": candidate.get("observed_at"),
            "candidate_ranking": reference.get("ranking_evidence", {}),
            "closed_quantity": closing, "opened_quantity": opening,
            "closed_episode": None, "opened_episode": None,
            "opening_lot_id": None, "stop_gap_usd": ZERO,
            "active_stop": episode.get("plan", {}).get("stop") if episode else None,
            "active_target": episode.get("plan", {}).get("target") if episode else None,
        }
        if closing and episode:
            record["closed_episode"] = episode["id"]
            expected_pnl = ZERO
            for lot_id in episode["lots"]:
                lot = lots[lot_id]
                allocation = lot["remaining"] * closing / abs(before)
                allocated_pnl = allocation * episode["direction"] * (unit_value - lot["unit"])
                lot["remaining"] -= allocation
                lot["realized"] += allocated_pnl
                expected_pnl += allocated_pnl
            max_pnl_error = max(max_pnl_error, abs(pnl - expected_pnl))
            if abs(pnl - expected_pnl) > TOLERANCE:
                raise ValueError(f"Realized P&L mismatch at trade {trade['id']}")
            episode["realized"] += pnl
            episode["closed_quantity"] += closing
            episode["close_trade_ids"].append(trade["id"])
            episode["last_exit_price"] = price
            episode["last_exit_reason"] = reason
            episode["quantity"] = before - closing * episode["direction"]
            if reason == "optional_stop" and episode.get("plan", {}).get("stop") is not None:
                stop = dec(episode["plan"]["stop"])
                fx = price / unit_value
                record["stop_gap_usd"] = closing * (stop - price) * episode["direction"] / fx
            if not episode["quantity"]:
                episode["closed_at"] = trade["filled_at"]
                previous[symbol] = episode
                del active[symbol]
        if opening:
            is_add = bool(before * delta > 0)
            if not is_add:
                prev = previous.get(symbol)
                episode = {
                    "id": f"E{len(episodes) + 1:04d}", "symbol": symbol,
                    "name": record["name"], "market": trade["market"], "phase": phase,
                    "direction": 1 if delta > 0 else -1, "quantity": ZERO,
                    "opened_at": trade["filled_at"], "closed_at": None,
                    "lots": [], "open_trade_ids": [], "close_trade_ids": [],
                    "realized": ZERO, "unrealized": ZERO, "closed_quantity": ZERO,
                    "opened_quantity": ZERO, "entry_notional": ZERO, "additions": 0,
                    "entry_price": price, "strategy": strategy,
                    "strategy_group": strategy_group(strategy), "opening_reason": reason,
                    "reverse_entry": bool(closing or (prev and prev["direction"] * delta < 0)),
                    "direct_flip": bool(closing),
                    "previous_episode": prev["id"] if prev else None,
                    "plan": {}, "plan_history": [],
                }
                episodes.append(episode)
                active[symbol] = episode
            if episode is None:
                raise ValueError("Opening episode is missing")
            lot_id = len(lots)
            lot = {
                "trade_id": trade["id"], "episode_id": episode["id"],
                "quantity": opening, "remaining": opening, "unit": unit_value,
                "price": price, "realized": ZERO, "unrealized": ZERO, "is_add": is_add,
                "direction": episode["direction"],
            }
            lots.append(lot)
            episode["lots"].append(lot_id)
            episode["open_trade_ids"].append(trade["id"])
            episode["opened_quantity"] += opening
            episode["entry_notional"] += opening * unit_value
            episode["quantity"] += opening * episode["direction"]
            episode["additions"] += int(is_add)
            if decision:
                episode["plan"] = {"stop": decision.get("stop"), "target": decision.get("target")}
            elif not free:
                episode["plan"] = {"stop": trade.get("stop"), "target": trade.get("target")}
            record["opened_episode"] = episode["id"]
            record["opening_lot_id"] = lot_id
        record["action"] = (
            "reverse" if closing and opening else "increase" if opening and before * delta > 0
            else "open" if opening else "close" if closing == abs(before) else "partial_close"
        )
        record["gross_open_cost"] = sum(
            sum((lots[i]["unit"] * lots[i]["remaining"] for i in item["lots"]), ZERO)
            for item in active.values()
        )
        max_gross_cost = max(max_gross_cost, record["gross_open_cost"])
        record["open_positions_after"] = len(active)
        records.append(record)

    for symbol, episode in active.items():
        position = positions[symbol]
        if abs(episode["quantity"] - dec(position["quantity"])) > TOLERANCE:
            raise ValueError(f"Final quantity mismatch for {symbol}")
        mark_unit = dec(position["mark_usd"]) / episode["quantity"]
        for lot_id in episode["lots"]:
            lot = lots[lot_id]
            lot["unrealized"] = lot["remaining"] * episode["direction"] * (mark_unit - lot["unit"])
            episode["unrealized"] += lot["unrealized"]
        episode["source_timestamp"] = quotes.get(symbol, {}).get("source_timestamp")
    if set(active) != set(positions):
        raise ValueError("Final position symbols do not match")
    for episode in episodes:
        episode["total_pnl"] = episode["realized"] + episode["unrealized"]
        episode["outcome"] = (
            "open" if episode["closed_at"] is None else "profit"
            if episode["realized"] > TOLERANCE else "loss"
            if episode["realized"] < -TOLERANCE else "flat"
        )
    for record in records:
        lot_id = record["opening_lot_id"]
        lot = lots[lot_id] if lot_id is not None else None
        record["entry_attributed_realized"] = lot["realized"] if lot else None
        record["entry_attributed_unrealized"] = lot["unrealized"] if lot else None
        record["entry_remaining_quantity"] = lot["remaining"] if lot else None
    marked = sum((dec(p["mark_usd"]) for p in positions.values()), ZERO)
    unrealized = sum((lot["unrealized"] for lot in lots), ZERO)
    source_cash = sum((dec(row["amount"]) for row in snapshot["cash"]), ZERO)
    checks = {
        "trade_count": len(records), "source_trade_count": len(snapshot["trades"]),
        "unique_trade_ids": len({row["id"] for row in records}),
        "cash_difference": cash - source_cash,
        "equity_difference": cash + marked - (Decimal(1000) + realized + unrealized),
        "entry_attribution_difference": sum((lot["realized"] for lot in lots), ZERO) - realized,
        "maximum_fill_pnl_difference": max_pnl_error,
        "open_episode_count": len(active), "source_position_count": len(positions),
    }
    for key in ("cash_difference", "equity_difference", "entry_attribution_difference"):
        if abs(checks[key]) > TOLERANCE:
            raise ValueError(f"Reconciliation failed: {key}={checks[key]}")
    phases: dict[str, list[Decimal]] = defaultdict(list)
    symbols: dict[str, dict[str, Any]] = {}
    for row in records:
        if row["closed_quantity"]:
            phases[row["phase"]].append(row["realized"])
        item = symbols.setdefault(row["symbol"], {
            "symbol": row["symbol"], "name": row["name"], "fills": 0,
            "realized": ZERO, "unrealized": ZERO, "episodes": 0,
        })
        item["fills"] += 1
        item["realized"] += row["realized"]
    for episode in episodes:
        item = symbols[episode["symbol"]]
        item["unrealized"] += episode["unrealized"]
        item["episodes"] += 1
    for item in symbols.values():
        item["total_pnl"] = item["realized"] + item["unrealized"]
    executed = {row["event_key"] for row in records}
    superseded = {json.loads(row["data"]).get("event_key") for row in snapshot["events"]
        if row["kind"] == "simulation_superseded"}
    unexecuted = []
    for call in calls.values():
        for index, decision in enumerate(call.get("decisions", [])):
            if not isinstance(decision, dict) or decision.get("action") not in ("BUY", "SELL"):
                continue
            key = f"simulation-ai:{call['id']}:{index}"
            if "strategy" not in decision:
                candidates = set(call["event_key"].split("|"))
                if any(row["symbol"] == decision["symbol"] and row["side"] == "BUY"
                       and row["event_key"].removeprefix("entry:") in candidates
                       for row in records):
                    continue
            if key not in executed:
                unexecuted.append({"event_key": key, "call_id": call["id"],
                    "decision": decision, "position_changed": key in superseded})
    summary = {
        "cutoff_utc": snapshot["cutoff_utc"], "trade_count": len(records),
        "episode_count": len(episodes), "open_positions": len(active),
        "cash": cash, "virtual_debt": max(-cash, ZERO), "equity": cash + marked,
        "realized": realized, "unrealized": unrealized,
        "gross_exposure": sum((abs(dec(p["mark_usd"])) for p in positions.values()), ZERO),
        "net_exposure": marked, "max_gross_open_cost": max_gross_cost,
        "closed_fills": summarize([row["realized"] for row in records if row["closed_quantity"]]),
        "closed_episodes": summarize([e["realized"] for e in episodes if e["closed_at"]]),
        "phases": {key: summarize(values) for key, values in phases.items()},
        "stop_gap_usd": sum((row["stop_gap_usd"] for row in records), ZERO),
        "ai_requests": len(calls),
        "known_tokens": sum(call["input_tokens"] + call["output_tokens"] for call in calls.values()),
        "unknown_usage_calls": sum(call.get("usage_known") != 1 for call in calls.values()),
        "unexecuted_decisions": len(unexecuted),
    }
    return {"summary": summary, "trades": records, "episodes": episodes,
        "lots": lots, "symbols": list(symbols.values()), "unexecuted": unexecuted,
        "checks": checks}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = review(json.loads(args.snapshot.read_text()))
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps({"summary": result["summary"], "checks": result["checks"]}, default=str))


if __name__ == "__main__":
    main()
