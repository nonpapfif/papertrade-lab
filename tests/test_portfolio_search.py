"""Verify portfolio cashflows, causal ranks, observed fills, and isolated replay results."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tools.search_portfolios import (
    SCHEMA,
    Book,
    Worker,
    boundaries,
    configurations,
    detail_configs,
    digest,
    encode,
    export_report,
    rank_frame,
    run,
    select_validation,
    signals,
    worker_loop,
)

START = datetime(2026, 10, 30, tzinfo=UTC)


def config(**changes: Any) -> dict[str, Any]:
    return {**configurations(1)[0], **changes}


def at(minute: int) -> str:
    return (START + timedelta(minutes=minute)).isoformat()


def scopes() -> dict[str, Any]:
    return {
        m: {
            "start": at(0),
            "end": at(100),
            "sessions": [
                {"start": at(0), "end": at(10)},
                {"start": at(10), "end": at(20)},
                {"start": at(20), "end": at(100)},
            ],
        }
        for m in ("KR", "US")
    }


class PortfolioTests(unittest.TestCase):
    def test_custom_capital_whole_shares_round_down_and_reconcile_without_debt(self) -> None:
        book = Book(config(hold=1), "US", "whole", True, initial_equity=50, whole_shares=True)
        book.step(at(5), 1, {0: 9}, {0: 9}, [[0, 1, 1]], True)
        self.assertEqual(book.cash, 5)
        self.assertEqual(book.positions[0].quantity, 5)
        book.step(at(10), 2, {0: 10}, {0: 10}, [], False)
        result = book.summary({0: 10}, {0: at(10)}, at(10))
        self.assertEqual(result["initial_equity"], 50)
        self.assertEqual(result["equity"], 55)
        self.assertAlmostEqual(result["return_pct"], 10)
        self.assertEqual(result["debt"], 0)
        for quantity in (-1, 0, 1.5, 100):
            with self.subTest(quantity=quantity), self.assertRaises(ValueError):
                book.fill(1, quantity, 10, at(15), 3, "entry")

    def test_whole_zero_rounding_skips_to_lower_rank_without_borrowing_or_cash_refill(self) -> None:
        book = Book(
            config(slots=2, hold=0), "US", "whole", True, initial_equity=50, whole_shares=True
        )
        prices: dict[int, float] = {0: 30, 1: 40, 2: 10, 3: 5}
        book.step(at(5), 1, prices, prices, [[s, 1, 1] for s in prices], True)
        self.assertEqual(set(book.positions), {2, 3})
        self.assertEqual([p.quantity for p in book.positions.values()], [2, 5])
        self.assertEqual(book.cash, 5)
        self.assertEqual(book.equity, 50)
        self.assertEqual(book.zero_quantity, 2)
        self.assertTrue(all(float(t["quantity_normalized"]).is_integer() for t in book.trades))

    def test_whole_fixed_sizing_uses_custom_initial_capital_and_preserves_cash_dust(self) -> None:
        book = Book(
            config(compound=False, hold=1),
            "US",
            "whole",
            True,
            initial_equity=50,
            whole_shares=True,
        )
        book.step(at(5), 1, {0: 10}, {0: 10}, [[0, 1, 1]], True)
        book.step(at(10), 2, {0: 20, 1: 10}, {0: 20, 1: 10}, [[1, 1, 1]], True)
        self.assertEqual(book.equity, 100)
        self.assertEqual(book.positions[1].quantity, 5)
        self.assertEqual(book.cash, 50)
        self.assertEqual(book.affordable_quantity(0.3, 0.1), 2)

    def test_fixed_grid_and_sample_ids_are_unique_and_reproducible(self) -> None:
        specs = signals()
        configs = configurations(len(specs))
        self.assertEqual(len(specs), 5400)
        self.assertEqual(len(configs), 64800)
        self.assertEqual(len({c["id"] for c in configs}), len(configs))
        self.assertEqual(configurations(10), configurations(10))
        self.assertEqual({c["leverage"] for c in configs}, {1, 2, 5, 10})
        self.assertEqual({s["side"] for s in specs}, {"long", "short", "both"})

    def test_long_borrowing_is_debt_and_not_profit(self) -> None:
        b = Book(config(leverage=5, hold=0), "US", "full")
        b.step(at(5), 1, {0: 100}, {0: 100}, [[0, 1, 0.02]], True)
        first = b.summary({0: 100}, {0: at(5)}, at(5))
        self.assertEqual((first["equity"], first["debt"]), (1000, 4000))
        b.step(at(10), 2, {0: 90}, {0: 90}, [], False)
        self.assertEqual(b.equity, 500)
        self.assertEqual(b.summary({0: 90}, {0: at(10)}, at(10))["unrealized"], -500)

    def test_short_proceeds_offset_short_liability(self) -> None:
        b = Book(config(hold=1), "US", "full", detailed=True)
        b.step(at(5), 1, {0: 100}, {0: 100}, [[0, -1, 0.02]], True)
        self.assertEqual((b.cash, b.equity), (2000, 1000))
        b.step(at(10), 2, {0: 80}, {0: 80}, [], False)
        summary = b.summary({0: 80}, {0: at(10)}, at(10))
        self.assertEqual((summary["equity"], summary["realized"]), (1200, 200))
        self.assertEqual(b.trades[-1]["realized_pnl"], 200)

    def test_gapped_stop_fills_at_observed_price_not_target(self) -> None:
        b = Book(config(hold=0, stop=0.01), "US", "full", detailed=True)
        b.step(at(5), 1, {0: 100}, {0: 100}, [[0, 1, 0.02]], True)
        b.step(at(10), 2, {}, {0: 100}, [], False)
        self.assertEqual(b.closed, 0)
        b.step(at(15), 3, {0: 80}, {0: 80}, [], False)
        self.assertEqual(b.trades[-1]["price"], 80)
        self.assertEqual(b.equity, 800)

    def test_nonpositive_equity_is_preserved_without_new_funding(self) -> None:
        b = Book(config(hold=0, leverage=10), "US", "full")
        b.step(at(5), 1, {0: 100}, {0: 100}, [[0, 1, 0.02]], True)
        b.step(at(10), 2, {0: 1}, {0: 1}, [], False)
        summary = b.summary({0: 1}, {0: at(10)}, at(10))
        self.assertEqual(summary["equity"], -8900)
        self.assertTrue(summary["ever_nonpositive_equity"])
        self.assertEqual(summary["debt"], 9000)
        self.assertGreater(summary["max_drawdown_pct"], 100)

    def test_fixed_notional_can_continue_with_negative_equity(self) -> None:
        b = Book(config(hold=1, leverage=10, compound=False), "US", "full")
        b.step(at(5), 1, {0: 100}, {0: 100}, [[0, 1, 0.02]], True)
        b.step(at(10), 2, {0: 1, 1: 50}, {0: 1, 1: 50}, [[1, 1, 0.02]], True)
        self.assertLess(b.equity, 0)
        self.assertIn(1, b.positions)
        self.assertEqual(b.opened, 2)

    def test_score_weights_spend_the_declared_slot_budget(self) -> None:
        b = Book(config(slots=3, sizing="score", hold=0), "US", "full")
        prices = {0: 100.0, 1: 100.0}
        b.step(at(5), 1, prices, prices, [[0, 1, 1], [1, 1, 3]], True)
        self.assertAlmostEqual(b.positions[1].quantity, 3 * b.positions[0].quantity)
        self.assertAlmostEqual(b.cash, 1000 / 3)
        self.assertAlmostEqual(b.equity, 1000)

    def test_cooldown_blocks_exact_number_of_checks_and_expires(self) -> None:
        b = Book(config(cooldown=3), "US", "full")
        for tick in range(1, 5):
            b.step(at(tick * 5), tick, {0: 100}, {0: 100}, [[0, 1, 1]], True)
        self.assertEqual(b.opened, 1)
        b.step(at(25), 5, {0: 100}, {0: 100}, [[0, 1, 1]], True)
        self.assertEqual(b.opened, 2)
        self.assertEqual(b.cooldowns, {})

    def test_stale_marks_are_disclosed_and_not_closed_at_end(self) -> None:
        b = Book(config(hold=0), "US", "full")
        b.step(at(5), 1, {0: 100}, {0: 100}, [[0, 1, 1]], True)
        result = b.summary({0: 110}, {0: at(10)}, at(15))
        self.assertEqual(result["equity"], 1100)
        self.assertEqual(result["open_positions"], 1)
        self.assertEqual(result["stale_open_positions"], 1)
        self.assertEqual(b.closed, 0)

    def test_ranks_use_only_current_quotes_and_apply_signed_rules(self) -> None:
        specs = [
            {"column": 0, "side": side, "volume": 1.5, "noise": 2, "move": 0.01, "ranking": ranking}
            for side in ("long", "short", "both")
            for ranking in ("strongest", "nearest_threshold")
        ]
        rows = []
        for symbol, score, timestamp in (
            ("A", 0.03, at(5)),
            ("B", -0.02, at(5)),
            ("C", 0.01, at(5)),
            ("OLD", 1, at(0)),
        ):
            rows.append(
                {
                    "market": "US",
                    "symbol": symbol,
                    "quote_at": timestamp,
                    "scores": [score] + [None] * 29,
                    "volumes": [2] * 5,
                    "noises": [2] * 5,
                }
            )
        ids = {("US", str(r["symbol"])): i for i, r in enumerate(rows)}
        ranked = rank_frame(rows, specs, ids, at(5))
        self.assertEqual([r[0] for r in ranked[0][1]], [0, 2])
        self.assertEqual([r[0] for r in ranked[1][1]], [2, 0])
        self.assertEqual(ranked[2][1], [[1, -1, 0.02]])
        self.assertEqual([r[0] for r in ranked[4][1]], [0, 1, 2])

    def requests(self, future_jump: bool = False) -> list[dict[str, Any]]:
        cfg = config(hold=1)
        requests = [{"kind": "init", "configs": [cfg], "scopes": scopes(), "detailed": True}]
        for minute in range(5, 101, 5):
            price = 100 + minute + (1000 if future_jump and minute > 80 else 0)
            requests.append(
                {
                    "kind": "frame",
                    "tick": minute // 5,
                    "at": at(minute),
                    "quotes": {"0": price},
                    "marks": {"0": [price, at(minute)]},
                    "ranks": [[[], [[0, 1, 1]]]],
                }
            )
        return requests

    def test_worker_prefix_is_unchanged_by_future_prices(self) -> None:
        prefixes = []
        for jump in (False, True):
            target = io.StringIO()
            worker_loop(io.StringIO("\n".join(json.dumps(r) for r in self.requests(jump))), target)
            rows = [json.loads(line) for line in target.getvalue().splitlines()]
            results = [r for msg in rows if msg["kind"] == "results" for r in msg["rows"]]
            prefixes.append(
                [
                    r
                    for r in results
                    if r["summary"]["market"] == "US"
                    and r["summary"]["phase"] in ("train", "validation")
                ]
            )
            self.assertEqual(len(results), 8)
        self.assertEqual(prefixes[0], prefixes[1])
        self.assertEqual(boundaries(scopes()["US"]), [at(60), at(80), at(100)])

    def test_latest_minute_mark_does_not_allow_a_stale_fill(self) -> None:
        requests = self.requests()
        requests[0]["configs"][0].update(hold=0, target=0.01)
        requests[2]["quotes"] = {}
        requests[2]["marks"] = {"0": [210, at(9)]}
        requests[2]["ranks"] = [[[], []]]
        target = io.StringIO()
        worker_loop(io.StringIO("\n".join(json.dumps(r) for r in requests)), target)
        results = [
            r for line in target.getvalue().splitlines() for r in json.loads(line).get("rows", [])
        ]
        train = next(
            r
            for r in results
            if r["summary"]["market"] == "US" and r["summary"]["phase"] == "train"
        )
        point = next(p for p in train["curve"] if p["at"] == at(10))
        self.assertEqual(point["equity"], 2000)
        self.assertEqual(point["open_positions"], 1)
        self.assertFalse(any(t["fill_at"] == at(10) for t in train["trades"]))

    def test_future_mark_is_rejected(self) -> None:
        requests = self.requests()[:2]
        requests[1]["marks"]["0"][1] = at(6)
        with self.assertRaisesRegex(ValueError, "mark chronology"):
            worker_loop(io.StringIO("\n".join(json.dumps(r) for r in requests)), io.StringIO())

    def test_test_returns_cannot_change_selection_between_two_strategies(self) -> None:
        with sqlite3.connect(":memory:") as db:
            db.row_factory = sqlite3.Row
            db.executescript(SCHEMA)
            for identifier, family in ((0, "momentum"), (1, "reversal")):
                cfg = {
                    **config(id=identifier),
                    "entry": {"family": family, "lookback": 5, "side": "long"},
                }
                db.execute("INSERT INTO configurations VALUES (?,?)", (identifier, encode(cfg)))
                for phase in ("train", "validation", "test"):
                    equity = 1500 - identifier * 100 if phase != "test" else 100 + identifier * 9999
                    result = {
                        "configuration": identifier,
                        "market": "US",
                        "phase": phase,
                        "equity": equity,
                        "return_pct": equity / 10 - 100,
                    }
                    db.execute(
                        "INSERT INTO results VALUES (?,?,?,?,?,?)",
                        (identifier, "US", phase, equity, result["return_pct"], encode(result)),
                    )
            before = select_validation(db)
            db.execute("UPDATE results SET equity=-equity WHERE phase='test'")
            after = select_validation(db)
            self.assertEqual(before[0]["test"]["configuration"], 0)
            self.assertEqual(after[0]["test"]["configuration"], 0)

    def test_complete_run_and_restart_preserve_source_and_results(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset, cache, output = (
                root / "market.sqlite3",
                root / "features.sqlite3",
                root / "out",
            )
            raw = sqlite3.connect(dataset)
            raw.executescript("""
                CREATE TABLE symbols(market TEXT,symbol TEXT,status TEXT);
                CREATE TABLE candles(market TEXT,symbol TEXT,at TEXT,close TEXT);
                INSERT INTO symbols VALUES ('US','TEST','done');
            """)
            raw.executemany(
                "INSERT INTO candles VALUES (?,?,?,?)",
                [("US", "TEST", at(minute), str(100 + minute)) for minute in range(5, 101, 5)],
            )
            raw.commit()
            raw.close()
            source = sqlite3.connect(cache)
            source.executescript("""
                CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
                CREATE TABLE frames(id INTEGER PRIMARY KEY,at TEXT,output_sha256 TEXT,
                                    basis_gzip BLOB);
            """)
            source.executemany(
                "INSERT INTO metadata VALUES (?,?)",
                [
                    ("manifest", encode({"dataset_sha256": digest(dataset), "scopes": scopes()})),
                    ("feature_complete", "true"),
                ],
            )
            for minute in range(5, 101, 5):
                payload = encode(
                    {
                        "features": [
                            {
                                "market": "US",
                                "symbol": "TEST",
                                "quote_at": at(minute),
                                "scores": [0.01] * 30,
                                "volumes": [2] * 5,
                                "noises": [2] * 5,
                            }
                        ]
                    }
                ).encode()
                source.execute(
                    "INSERT INTO frames VALUES (?,?,?,?)",
                    (
                        minute // 5,
                        at(minute),
                        hashlib.sha256(payload).hexdigest(),
                        gzip.compress(payload),
                    ),
                )
            source.commit()
            source.close()
            original = (digest(dataset), digest(cache))
            with (
                patch("tools.search_portfolios.signals", return_value=signals()[:1]),
                redirect_stdout(io.StringIO()),
            ):
                run(dataset, cache, output)
                run(dataset, cache, output)
            self.assertEqual(original, (digest(dataset), digest(cache)))
            report = json.loads((output / "report.json").read_text())
            self.assertEqual(report["state"], "complete")
            self.assertEqual(report["result_rows"], 12 * 2 * 4)
            self.assertEqual(json.loads((output / "status.json").read_text())["stage"], "complete")
            with sqlite3.connect(output / "portfolios.sqlite3") as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM frames").fetchone()[0], 20)
                self.assertGreater(db.execute("SELECT COUNT(*) FROM details").fetchone()[0], 0)

    def test_actual_sandbox_reports_and_detailed_replay_reconciles(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "private.sqlite3"
            dataset.write_text("private future data")
            db = sqlite3.connect(root / "portfolios.sqlite3")
            db.row_factory = sqlite3.Row
            db.executescript(SCHEMA)
            cfg = {**config(), "entry": signals()[0]}
            db.execute("INSERT INTO configurations VALUES (?,?)", (0, json.dumps(cfg)))
            for detailed in (False, True):
                worker = Worker(dataset, root)
                try:
                    checks = worker.exchange({"kind": "probe", "paths": [str(dataset)]}, db)
                    self.assertTrue(all(checks["isolation"].values()))
                    for request in self.requests():
                        request["detailed"] = detailed
                        worker.exchange(request, db, details=detailed)
                finally:
                    worker.close()
            self.assertEqual(db.execute("SELECT COUNT(*) FROM results").fetchone()[0], 8)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM details").fetchone()[0], 8)
            report = export_report(db, root)
            self.assertEqual(len(report["chronological_winners"]), 2)
            self.assertEqual(len(detail_configs(report)), 1)
            self.assertTrue((root / "rankings.csv").exists())
            before = [r["test"]["configuration"] for r in report["chronological_winners"]]
            db.execute("UPDATE results SET equity=999999 WHERE phase='test'")
            with patch("tools.search_portfolios.LEVERAGES", (1,)):
                after = export_report(db, root)
            self.assertEqual(
                before, [r["test"]["configuration"] for r in after["chronological_winners"]]
            )
            db.close()


if __name__ == "__main__":
    unittest.main()
