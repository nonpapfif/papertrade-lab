"""Evaluate frozen layered hypotheses on a received-order Toss stream recording."""

from __future__ import annotations

import argparse
import bisect
import gzip
import hashlib
import json
import time
from dataclasses import asdict, replace
from datetime import UTC, datetime
from itertools import product
from pathlib import Path
from typing import Any

from papertrade.api import ApiError, private_json
from papertrade.core import SafetyError, Settings
from tools.layered_paper import FAMILIES, LayeredPaper, eligible_stock, feature_from_bars
from tools.probe_toss_websocket import ROOT, CachedTokenClient, now

SOURCES = (
    "tools/layered_paper.py",
    "tools/replay_layered_stream.py",
    "tools/live_execution.py",
    "tools/search_portfolios.py",
    "papertrade/core.py",
    "papertrade/api.py",
)


def parse(at: str) -> float:
    return datetime.fromisoformat(at).timestamp()


def variations() -> list[dict[str, Any]]:
    baseline = {
        "policy": "router",
        "selection": "all",
        "allocation": "equal",
        "cadence": 0,
        "delay": 1,
        "depth": 1.0,
    }
    result = [baseline]
    for name, choices in (
        ("policy", FAMILIES),
        ("selection", ("active",)),
        ("allocation", ("priority",)),
        ("cadence", (60, 300)),
        ("delay", (3, 5)),
        ("depth", (0.25, 0.1)),
    ):
        result.extend({**baseline, name: value} for value in choices)
    return result


def read_recording(capture: Path, ids: dict[str, int]) -> tuple[dict, dict]:
    books: dict[int, list[dict[str, Any]]] = {s: [] for s in ids.values()}
    features: dict[int, list[tuple[float, Any]]] = {s: [] for s in ids.values()}
    with gzip.open(capture / "core-frames.jsonl.gz", "rt") as stream:
        for line in stream:
            event = json.loads(line)
            f = event["frame"]
            if f["topic"].startswith("orderbook:"):
                symbol = ids[f["topic"].rsplit(":", 1)[1]]
                books[symbol].append({"received_at": event["received_at"], **f["data"]})
    for path in sorted(capture.glob("rest-*.json.gz")):
        with gzip.open(path, "rt") as stream:
            data = json.load(stream)
        for symbol, payload in data["candles"].items():
            at = payload["received_at"]
            rows = [{**row, "available_at": at} for row in payload["result"]["candles"]]
            f = feature_from_bars(ids[symbol], rows, at)
            features[ids[symbol]].append((parse(at), f))
    return books, features


def evaluate(capture: Path, output: Path, freeze: dict[str, Any]) -> None:
    ids = {symbol: i for i, symbol in enumerate(freeze["symbols"])}
    eligible = {ids[row["symbol"]] for row in freeze["stock_info"] if eligible_stock(row)}
    books, history = read_recording(capture, ids)
    received = {s: [parse(x["received_at"]) for x in rows] for s, rows in books.items()}
    feature_times = {s: [r[0] for r in rows] for s, rows in history.items()}
    coverage = {
        symbol: {
            "snapshots": len(history[i]),
            "usable_snapshots": sum(f is not None for _, f in history[i]),
            "observed_features": [asdict(f) for _, f in history[i] if f is not None],
        }
        for symbol, i in ids.items()
    }
    private_json(output / "feature-coverage.json", coverage)
    decisions = sorted(
        {
            t
            for rows in received.values()
            for t in rows
            if t >= parse(freeze.get("evaluation_start", freeze["frozen_at"]))
        }
    )
    fx_history = json.loads((output / "fx-history.json").read_text())["observations"]
    fx_times = [parse(r["received_at"]) for r in fx_history]
    results = []
    for capital, config in product((50, 100, 200, 500), variations()):
        account = LayeredPaper(
            capital,
            policy=config["policy"],
            selection=config["selection"],
            allocation=config["allocation"],
        )
        ready = -1.0
        slot = None
        for tick in decisions:
            if tick < ready:
                continue
            cadence = config["cadence"]
            current_slot = int(tick // cadence) if cadence else tick
            if current_slot == slot:
                continue
            initial, confirmation, fs, prices = {}, {}, {}, {}
            arrivals, sources = {}, {}
            for symbol, rows in books.items():
                i = bisect.bisect_right(received[symbol], tick) - 1
                j = bisect.bisect_left(received[symbol], tick + config["delay"])
                if i < 0 or j >= len(rows):
                    continue
                decision_book, confirmed = rows[i], rows[j]
                if tick - received[symbol][i] > 60 or received[symbol][j] - tick > 30:
                    continue
                initial[symbol] = decision_book
                confirmation[symbol] = {
                    **confirmed,
                    **{
                        side: [
                            {**r, "volume": str(int(float(r["volume"]) * config["depth"]))}
                            for r in confirmed[side]
                        ]
                        for side in ("asks", "bids")
                    },
                }
                arrivals[symbol] = confirmed["received_at"]
                sources[symbol] = decision_book["timestamp"]
                best_ask = min(float(r["price"]) for r in decision_book["asks"])
                best_bid = max(float(r["price"]) for r in decision_book["bids"])
                prices[symbol] = (best_bid + best_ask) / 2
                index = bisect.bisect_right(feature_times[symbol], tick) - 1
                if index >= 0 and history[symbol][index][1] is not None:
                    f = history[symbol][index][1]
                    fs[symbol] = replace(
                        f,
                        price=prices[symbol],
                        r5=prices[symbol] / (f.price / (1 + f.r5)) - 1,
                        r15=prices[symbol] / (f.price / (1 + f.r15)) - 1,
                    )
            if not confirmation:
                continue
            decision_at = datetime.fromtimestamp(tick, UTC).isoformat()
            at = max(arrivals.values(), key=parse)
            fx_index = bisect.bisect_right(fx_times, tick) - 1
            fx = fx_history[fx_index] if fx_index >= 0 else None
            frame = {
                "at": at,
                "decision_at": decision_at,
                "tick": int(tick * 1000),
                "quotes": prices,
                "marks": {s: [p, sources[s]] for s, p in prices.items()},
                "source_times": sources,
                "decision_orderbooks": initial,
                "orderbooks": confirmation,
                "confirmation_received_at": arrivals,
                "fx": float(fx["result"]["midRate"]) if fx else 0,
                "fx_at": fx["received_at"] if fx else at,
            }
            account.step(str(tick), frame, fs, eligible)
            ready = parse(at)
            slot = current_slot
        result = account.summary() if account.last_at else {"account": None}
        results.append({"capital": capital, "configuration": config, **result})
        private_json(
            output / "progress.json",
            {
                "state": "running",
                "at": now(),
                "completed": len(results),
                "total": 4 * len(variations()),
                "ai_calls": 0,
            },
        )
    total_entries = sum(row["account"]["entries"] for row in results if row.get("account"))
    private_json(
        output / "results.json",
        {
            "state": "complete",
            "at": now(),
            "ai_calls": 0,
            "real_orders": False,
            "freeze": freeze,
            "decisions_available": len(decisions),
            "evaluation_status": "inconclusive" if total_entries == 0 else "observed_short_sample",
            "total_entries_including_counterfactual_duplicates": total_entries,
            "feature_coverage": {
                s: {k: v for k, v in item.items() if k != "observed_features"}
                for s, item in coverage.items()
            },
            "results": results,
            "limitations": [
                "Two recorded US symbols in one short session; not generalization proof",
                "OHLCV maturity assumes timestamp denotes minute start",
                "Depth fractions and delay are sensitivity assumptions, not calibrated fill probabilities",
                "Midpoint marks are estimates, not liquidation proceeds",
                "Tax uses observed trade-time FX estimates, not statutory settlement-date FX",
                "Source clocks are not corrected; future/stale quote rules remain enforced",
            ],
        },
    )
    private_json(
        output / "progress.json",
        {
            "state": "complete",
            "at": now(),
            "completed": len(results),
            "total": len(results),
            "ai_calls": 0,
            "evaluation_status": "inconclusive" if total_entries == 0 else "observed_short_sample",
        },
    )


def run(capture: Path, output: Path, wait_seconds: int) -> None:
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    manifest = json.loads((capture / "manifest.json").read_text())
    client = CachedTokenClient(Settings.load(ROOT))
    symbols = manifest["core"]
    freeze = {
        "frozen_at": now(),
        "symbols": symbols,
        "stock_info": client.stocks(symbols),
        "source_sha256": {
            name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES
        },
        "configuration": variations(),
        "sample_role": "Frozen pre-evaluation hypotheses",
    }
    private_json(output / "freeze.json", freeze)
    fx_history: list[dict[str, Any]] = []
    private_json(output / "fx-history.json", {"observations": fx_history})
    deadline = time.monotonic() + wait_seconds
    while True:
        state = json.loads((capture / "status.json").read_text())
        if state["state"] == "complete":
            break
        if state["state"] == "failed":
            raise RuntimeError("Capture failed; evaluation not started")
        if time.monotonic() >= deadline:
            raise TimeoutError("Capture completion deadline exceeded")
        fx = client.get("/api/v1/exchange-rate", baseCurrency="USD", quoteCurrency="KRW")
        fx_history.append({"received_at": now(), "result": fx})
        private_json(output / "fx-history.json", {"observations": fx_history})
        private_json(
            output / "progress.json",
            {
                "state": "waiting_for_capture",
                "at": now(),
                "ai_calls": 0,
                "frozen_at": freeze["frozen_at"],
            },
        )
        time.sleep(30)
    for name, digest in freeze["source_sha256"].items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError("Frozen implementation changed before evaluation")
    evaluate(capture, output, freeze)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wait-seconds", type=int, default=2400)
    args = parser.parse_args()
    destination = args.output.resolve()
    if destination.exists():
        parser.error("Choose a new output directory")
    try:
        run(args.capture.resolve(), destination, args.wait_seconds)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, SafetyError, ApiError) as error:
        if destination.exists():
            private_json(
                destination / "progress.json",
                {"state": "failed", "at": now(), "error_type": type(error).__name__, "ai_calls": 0},
            )
        raise SystemExit("Layered replay failed; inspect sanitized progress.json") from None


if __name__ == "__main__":
    main()
