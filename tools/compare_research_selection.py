"""Replay a US selection comparison using completed TossTradingLab research bundles."""

from __future__ import annotations

import argparse
import asyncio
import bisect
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from papertrade.jsonio import private_json, read_json
from tools.async_layered_paper import AsyncLayeredPaper
from tools.layered_paper import eligible_stock
from tools.replay_layered_stream import parse, read_recording, replay_case
from tools.source_identity import file_digest

_ENTRY_RATINGS = frozenset(("Buy", "Overweight"))
_RATINGS = _ENTRY_RATINGS | {"Hold", "Underweight", "Sell", "REVIEW"}


def _timestamp(value: Any) -> float:
    if not isinstance(value, str):
        raise TypeError("Expected an ISO timestamp with a UTC offset")
    stamp = datetime.fromisoformat(value)
    if stamp.utcoffset() is None:
        raise ValueError("Expected an ISO timestamp with a UTC offset")
    return stamp.timestamp()


@dataclass(frozen=True)
class Selection:
    published_at: float
    expires_at: float
    admitted: frozenset[int]
    reports: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class ResearchSchedule:
    starts: tuple[float, ...]
    snapshots: tuple[Selection, ...]

    def at(self, tick: float) -> frozenset[int]:
        index = bisect.bisect_right(self.starts, tick) - 1
        if index < 0 or tick >= self.snapshots[index].expires_at:
            return frozenset()
        return self.snapshots[index].admitted


def _bundle(path: Path) -> tuple[str, str, str, float, dict[str, str]]:
    manifest_raw = (path / "manifest.json").read_bytes()
    evidence_raw = (path / "evidence.json").read_bytes()
    manifest = json.loads(manifest_raw)
    snapshot = json.loads(evidence_raw)["snapshot"]
    run_id = manifest.get("run_id")
    rating = manifest.get("rating")
    if (
        run_id != path.name
        or not re.fullmatch(r"[a-f0-9]{64}", path.name)
        or manifest.get("status", "completed") != "completed"
        or "error_type" in manifest
        or manifest["reports"].get("portfolio-manager") != "completed"
        or "Portfolio Manager" not in manifest["executed_nodes"]
        or not isinstance(rating, str)
        or rating not in _RATINGS
    ):
        raise ValueError(f"Unusable research bundle: {path.name}")
    return snapshot["market"], snapshot["symbol"], rating, _timestamp(snapshot["as_of"]), {
        "run_id": run_id,
        "rating": rating,
        "symbol": snapshot["symbol"],
        "as_of": snapshot["as_of"],
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "evidence_sha256": hashlib.sha256(evidence_raw).hexdigest(),
    }


def load_schedule(path: Path, ids: dict[str, int]) -> ResearchSchedule:
    """Read complete selection snapshots; the newest usable snapshot replaces the older one."""
    rows = read_json(path)["snapshots"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("At least one research selection snapshot is required")
    snapshots: list[Selection] = []
    for row in rows:
        published = _timestamp(row["published_at"])
        expires = _timestamp(row["expires_at"])
        if published >= expires or (snapshots and published <= snapshots[-1].published_at):
            raise ValueError("Snapshots must be ordered and have a positive lifetime")
        bundles = row["bundles"]
        if not isinstance(bundles, list) or not bundles:
            raise ValueError("Each snapshot needs a nonempty list of research bundles")
        admitted: set[int] = set()
        seen: set[str] = set()
        provenance = []
        for name in bundles:
            market, symbol, rating, cutoff, report = _bundle(path.parent / name)
            if market != "US" or symbol not in ids or symbol in seen or cutoff > published:
                raise ValueError("Wrong market, duplicate/unobserved symbol, or future evidence")
            seen.add(symbol)
            provenance.append(report)
            if rating in _ENTRY_RATINGS:
                admitted.add(ids[symbol])
        snapshots.append(Selection(published, expires, frozenset(admitted), tuple(provenance)))
    return ResearchSchedule(tuple(row.published_at for row in snapshots), tuple(snapshots))


async def compare(
    capture: Path, freeze_path: Path, fx_path: Path, selection_path: Path,
    *, capital: float, strategy: str, compare_strategy: str | None = None,
) -> dict[str, Any]:
    freeze = read_json(freeze_path)
    ids = {symbol: index for index, symbol in enumerate(freeze["symbols"])}
    if len(ids) != len(freeze["symbols"]) or not ids:
        raise ValueError("The capture must identify a nonempty, distinct symbol universe")
    eligible = {
        ids[row["symbol"]]
        for row in freeze["stock_info"]
        if row["currency"] == "USD" and eligible_stock(row)
    }
    if not eligible:
        raise ValueError("The capture has no eligible US stocks")
    # Load once. Each independent account receives the same ordered input.
    books, history = read_recording(capture, ids)
    if not any(books.values()):
        raise ValueError("The capture has no orderbooks")
    fx = read_json(fx_path)["observations"]
    fx_times = [parse(row["received_at"]) for row in fx]
    start = _timestamp(freeze.get("evaluation_start", freeze["frozen_at"]))
    cut = max(parse(row["received_at"]) for rows in books.values() for row in rows)
    if cut < start:
        raise ValueError("The capture does not reach the comparison start")
    schedule = load_schedule(selection_path, ids)
    times = {symbol: [ts for ts, _ in rows] for symbol, rows in history.items()}
    config = {
        "policy": strategy, "selection": "all", "allocation": "equal",
        "cadence": 0, "delay": 1, "depth": 1.0,
    }
    fees = {"commission_exemption_usd": 0.0}
    scenarios = [("existing_selection", strategy, None), ("research_selection", strategy, schedule)]
    if compare_strategy is not None:
        scenarios.append(("research_alternative_strategy", compare_strategy, schedule))
    accounts = {}
    for name, policy, gate in scenarios:
        account = await replay_case(
            capital, {**config, "policy": policy}, books, history, times, fx, fx_times,
            eligible, start, selection_at=gate.at if gate is not None else None, fees=fees,
        )
        accounts[name] = {
            "strategy": policy,
            "uses_research_selection": gate is not None,
            "fee_schedule": account.context.schedule,
            "summary": account.summary(datetime.fromtimestamp(cut, UTC).isoformat()),
        }
    return {
        "mode": "offline_counterfactual_research_availability",
        "market": "US",
        "capital_per_account_usd": capital,
        "start_epoch": start,
        "observed_end_epoch": cut,
        "common_book_count": sum(len(rows) for rows in books.values()),
        "symbol_ids": ids,
        "eligible_symbols": sorted(symbol for symbol, idx in ids.items() if idx in eligible),
        "fixed_execution": config,
        "selection_input_sha256": file_digest(selection_path),
        "freeze_sha256": file_digest(freeze_path),
        "fx_history_sha256": file_digest(fx_path),
        "capture_sha256": {
            path.name: file_digest(path)
            for path in (capture / "core-frames.jsonl.gz", *sorted(capture.glob("rest-*.json.gz")))
        },
        "selection": [
            {"published_epoch": row.published_at, "expires_epoch": row.expires_at,
             "reports": row.reports}
            for row in schedule.snapshots
        ],
        "accounts": accounts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--freeze", type=Path, required=True)
    parser.add_argument("--fx-history", type=Path, required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True, help="New output directory")
    parser.add_argument("--capital", type=float, default=1000.0)
    parser.add_argument("--strategy", choices=AsyncLayeredPaper.entry_policies, default="trend")
    parser.add_argument("--compare-strategy", choices=AsyncLayeredPaper.entry_policies)
    args = parser.parse_args()
    if args.compare_strategy == args.strategy:
        parser.error("The alternative strategy must differ from the fixed strategy")
    try:
        result = asyncio.run(compare(
            args.capture, args.freeze, args.fx_history, args.selection, capital=args.capital,
            strategy=args.strategy, compare_strategy=args.compare_strategy,
        ))
        args.out.mkdir(parents=True, exist_ok=False)
        private_json(args.out / "comparison.json", result)
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as error:
        parser.error(str(error))
    print(args.out / "comparison.json")


if __name__ == "__main__":
    main()
