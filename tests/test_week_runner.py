"""Weekly input isolation, frozen hypotheses, and cross-day symbol mapping."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import tempfile
import time
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from tools.run_week_experiment import (
    DAYS,
    ROOT,
    IsolatedWorker,
    audit_whole_account,
    current_long_features,
    digest,
    encode,
    evaluate,
    export_long_results,
    long_policies,
    policies,
    prepare_day,
    remap_frame,
    run,
    trade_statistics,
)
from tools.search_portfolios import configurations, signals


class WeekRunnerTests(unittest.TestCase):
    def test_whole_account_audit_rejects_fractional_fills_and_unfunded_cash(self) -> None:
        hypothesis = {
            "accounting": "continuous_account",
            "daily": [{}],
            "summary": {"initial_equity": 50},
            "trades": [
                {
                    "day": DAYS[0],
                    "symbol_id": 0,
                    "realized_pnl": None,
                    "quantity_normalized": 5,
                    "price": 10,
                    "cash": 0,
                },
                {
                    "day": DAYS[0],
                    "symbol_id": 0,
                    "realized_pnl": -10,
                    "quantity_normalized": -5,
                    "price": 8,
                    "cash": 40,
                },
            ],
        }
        audit_whole_account(hypothesis, 50)
        hypothesis["trades"][0]["quantity_normalized"] = 4.5
        with self.assertRaisesRegex(ValueError, "fractional"):
            audit_whole_account(hypothesis, 50)
        hypothesis["trades"][0].update(quantity_normalized=6, cash=-10)
        with self.assertRaisesRegex(ValueError, "borrows"):
            audit_whole_account(hypothesis, 50)

    def test_whole_run_rejects_invalid_capitals_or_fx_before_creating_files(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "new"
            for capitals, fx in [
                ((50, 50), 1300),
                ((-1,), 1300),
                ((50,), None),
                ((50,), float("nan")),
            ]:
                with self.subTest(capitals=capitals, fx=fx), self.assertRaises(ValueError):
                    run(
                        output,
                        None,
                        4,
                        long_only=True,
                        whole_shares=True,
                        capitals=capitals,
                        krw_per_usd=fx,
                    )
                self.assertFalse(output.exists())

    def test_long_design_freezes_cold_carry_weighted_and_ensemble_comparisons(self) -> None:
        candidates = long_policies()
        self.assertEqual(len(candidates), 63)
        self.assertEqual(len({policy["id"] for policy in candidates}), 63)
        self.assertEqual(sum(bool(p.get("reset_learning")) for p in candidates), 25)
        self.assertEqual(sum(p.get("allocation") == "family_ensemble" for p in candidates), 6)
        self.assertFalse(any(p["mode"] == "fixed30" for p in candidates))

    def test_trade_metrics_include_flat_trades_and_keep_loss_streaks(self) -> None:
        result = trade_statistics([{"realized_pnl": value} for value in (None, 12, -3, -1, 0)])
        self.assertEqual(result["win_rate_pct"], 25)
        self.assertAlmostEqual(result["win_rate_excluding_flat_pct"], 100 / 3)
        self.assertEqual(result["profit_factor"], 3)
        self.assertEqual(result["max_consecutive_losses"], 2)
        self.assertIsNone(trade_statistics([])["win_rate_pct"])

    def test_full_feature_projection_excludes_outcomes_stale_rows_and_other_markets(self) -> None:
        with sqlite3.connect(":memory:") as db:
            db.row_factory = sqlite3.Row
            db.execute("CREATE TABLE frames(at TEXT,output_sha256 TEXT,basis_gzip BLOB)")
            at = "2026-10-26T00:05:00+00:00"
            feature = {
                "market": "KR",
                "symbol": "A",
                "quote_at": at,
                "price": 10,
                "reference_times": [None] * 5,
                "scores": [0.1] * 30,
                "volumes": [2] * 5,
                "noises": [3] * 5,
            }
            raw = encode(
                {
                    "features": [feature, {**feature, "market": "US"}],
                    "decisions": ["Must never be sent"],
                }
            ).encode()
            db.execute(
                "INSERT INTO frames VALUES (?,?,?)",
                (at, hashlib.sha256(raw).hexdigest(), gzip.compress(raw)),
            )
            rows, seal = current_long_features(
                db, {"at": at, "quotes": {7: 10}}, {("KR", "A"): 7}, "KR"
            )
            self.assertEqual(rows, [[7, [0.1] * 30, [2] * 5, [3] * 5]])
            self.assertEqual(seal, hashlib.sha256(raw).hexdigest())
            with self.assertRaisesRegex(ValueError, "current quote"):
                current_long_features(db, {"at": at, "quotes": {7: 11}}, {("KR", "A"): 7}, "KR")
            bad = {**feature, "reference_times": ["2026-10-26T00:10:00+00:00"]}
            raw = encode({"features": [bad]}).encode()
            db.execute(
                "UPDATE frames SET output_sha256=?,basis_gzip=?",
                (hashlib.sha256(raw).hexdigest(), gzip.compress(raw)),
            )
            with self.assertRaisesRegex(ValueError, "Future reference"):
                current_long_features(db, {"at": at, "quotes": {7: 10}}, {("KR", "A"): 7}, "KR")

    def test_prepared_cache_reuse_checks_original_seals_without_running_search(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            output, prepared = root / "new", root / "old"
            for base in (output, prepared):
                (base / "jobs").mkdir(parents=True)
            dataset, feature = root / "data.sqlite3", root / "search.sqlite3"
            dataset.write_text("Original prices")
            feature.write_text("Original causal features")
            portfolio = root / "portfolios.sqlite3"
            with sqlite3.connect(portfolio) as db:
                db.execute("CREATE TABLE metadata(key TEXT,value TEXT)")
                db.executemany(
                    "INSERT INTO metadata VALUES (?,?)",
                    [
                        ("simulation_complete", "true"),
                        ("frames_complete", "true"),
                        (
                            "manifest",
                            encode(
                                {
                                    "dataset_sha256": digest(dataset),
                                    "feature_cache_sha256": digest(feature),
                                }
                            ),
                        ),
                    ],
                )
            seal = {"feature_sha256": digest(feature), "portfolio_sha256": digest(portfolio)}
            (prepared / "jobs/2026-10-26.json").write_text(encode(seal))
            with (
                patch("tools.run_week_experiment.locations", return_value=(dataset, root, root)),
                patch("tools.search_up_candles.run") as features,
                patch("tools.search_portfolios.run") as portfolios,
            ):
                prepare_day(output, "2026-10-26", prepared)
                features.assert_not_called()
                portfolios.assert_not_called()
                feature.write_text("Changed causal features")
                with self.assertRaisesRegex(ValueError, "changed"):
                    prepare_day(output, "2026-10-26", prepared)

    def test_experiment_dates_exclude_october_23_and_freeze_35_unique_policies(self) -> None:
        self.assertEqual(DAYS, tuple(f"2026-10-{day}" for day in range(26, 31)))
        candidates = policies()
        self.assertEqual(len(candidates), 35)
        self.assertEqual(len({policy["id"] for policy in candidates}), 35)
        self.assertEqual(sum(policy["mode"] == "fixed30" for policy in candidates), 2)
        self.assertEqual(sum(policy["mode"] == "rolling" for policy in candidates), 12)

    def test_daily_symbol_mapping_preserves_identity_and_filters_other_markets(self) -> None:
        raw = {
            "at": "2026-10-27T00:00:00+00:00",
            "quotes": {"0": 20, "1": 100},
            "marks": {
                "0": [20, "2026-10-27T00:00:00+00:00"],
                "1": [100, "2026-10-27T00:00:00+00:00"],
            },
            "ranks": [[[[0, 1, 0.1]], [[1, -1, 0.2]]]],
        }
        ids = {("KR", "A"): 100, ("US", "B"): 200}
        actual = remap_frame(raw, [["KR", "A"], ["US", "B"]], ids, "KR")
        self.assertEqual(actual["quotes"], {100: 20})
        self.assertEqual(actual["ranks"], [[[[100, 1, 0.1]], []]])
        self.assertEqual(actual["tick"], int(datetime(2026, 10, 27, tzinfo=UTC).timestamp() // 300))
        self.assertEqual(
            remap_frame(raw, [["KR", "A"], ["US", "B"]], ids, "KR", set())["ranks"], [[[], []]]
        )

    def test_real_worker_denies_private_files_and_replays_five_causal_days(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            result = output / "results"
            result.mkdir()
            source = output / "source.sqlite3"
            source.write_text("Private future prices")
            result_db = result / "evaluation.sqlite3"
            result_db.write_text("Private future outcomes")
            home_canary = Path.home() / ".codex/memories/MEMORY.md"
            worker = IsolatedWorker(
                output, result, [source, result_db, home_canary], time.time() + 90
            )
            try:
                checks = worker.exchange(
                    {"kind": "probe", "paths": [str(source), str(result_db), str(home_canary)]}
                )
                self.assertTrue(all(checks["isolation"].values()))
                worker.exchange(
                    {
                        "kind": "init",
                        "market": "KR",
                        "configs": configurations(1)[:2],
                        "specs": signals()[:1],
                        "fixed_ids": {},
                        "policies": [
                            {"id": "daily", "mode": "daily"},
                            {"id": "prior", "mode": "prior_days"},
                        ],
                    }
                )
                for index, day in enumerate(DAYS):
                    start = day + "T00:00:00+00:00"
                    end = day + "T00:15:00+00:00"
                    scope = {
                        "KR": {
                            "start": start,
                            "end": end,
                            "sessions": [
                                {"start": start, "end": end},
                                {"start": start, "end": end},
                            ],
                        }
                    }
                    worker.exchange({"kind": "day_start", "day": day, "scopes": scope})
                    for minute, price in ((5, 100 + index), (10, 102 + index), (15, 101 + index)):
                        at = day + f"T00:{minute:02d}:00+00:00"
                        worker.exchange(
                            {
                                "kind": "frame",
                                "at": at,
                                "tick": int(datetime.fromisoformat(at).timestamp() // 300),
                                "quotes": {0: price},
                                "marks": {0: [price, at]},
                                "ranks": [[[[0, 1, 0.1]], []]],
                            }
                        )
                    daily = worker.exchange({"kind": "day_end"})
                    self.assertEqual(daily["day"], day)
                report = worker.exchange({"kind": "report"})
                self.assertEqual(report["days"], list(DAYS))
                self.assertEqual(report["ai_calls"], 0)
                self.assertTrue(all(len(row["daily"]) == 5 for row in report["hypotheses"]))
                self.assertTrue(
                    all(row["classification"] == "causal_replay" for row in report["hypotheses"])
                )
            finally:
                worker.close()

    def test_controller_seals_five_days_and_exports_real_isolated_worker_results(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            (output / "jobs").mkdir()
            paths = {}
            for index, day in enumerate(DAYS):
                base = output / day
                base.mkdir()
                dataset = base / "source.sqlite3"
                dataset.write_text("Private future data")
                scope = {
                    "KR": {
                        "start": day + "T00:00:00+00:00",
                        "end": day + "T00:15:00+00:00",
                        "sessions": [
                            {"start": day + "T00:00:00+00:00", "end": day + "T00:15:00+00:00"}
                        ]
                        * 2,
                    }
                }
                with sqlite3.connect(base / "portfolios.sqlite3") as db:
                    db.executescript(
                        "CREATE TABLE metadata(key TEXT,value TEXT);"
                        "CREATE TABLE frames(id INTEGER,at TEXT,sha256 TEXT,payload BLOB);"
                    )
                    db.executemany(
                        "INSERT INTO metadata VALUES (?,?)",
                        [
                            ("manifest", encode({"scopes": scope})),
                            ("symbols", encode([["KR", "A"]])),
                        ],
                    )
                    for ordinal, minute in enumerate((5, 10, 15)):
                        at = day + f"T00:{minute:02d}:00+00:00"
                        price = 100 + index + ordinal
                        frame = {
                            "at": at,
                            "tick": ordinal,
                            "quotes": {0: price},
                            "marks": {0: [price, at]},
                            "ranks": [[[[0, 1, 0.01]], []]],
                        }
                        raw = encode(frame).encode()
                        db.execute(
                            "INSERT INTO frames VALUES (?,?,?,?)",
                            (ordinal, at, hashlib.sha256(raw).hexdigest(), gzip.compress(raw)),
                        )
                (output / "jobs" / (day + ".json")).write_text(
                    encode({"portfolio_sha256": digest(base / "portfolios.sqlite3")})
                )
                paths[day] = dataset, base, base
            manifest = {
                "deadline": datetime.fromtimestamp(time.time() + 90, UTC).isoformat(),
                "symbols": [["KR", "A"]],
                "policies": [
                    {"id": "daily", "mode": "daily"},
                    {"id": "reset", "mode": "daily", "reset_daily": True},
                ],
                "code_sha256": {
                    name: digest(ROOT / "tools" / name)
                    for name in ("run_week_experiment.py", "week_hypotheses.py")
                },
            }
            with (
                patch(
                    "tools.run_week_experiment.locations", side_effect=lambda out, day: paths[day]
                ),
                patch("tools.search_portfolios.configurations", return_value=configurations(1)[:2]),
                patch("tools.search_portfolios.signals", return_value=signals()[:1]),
            ):
                self.assertEqual(evaluate(output, "KR", False, manifest), "KR-causal")
            result = output / "results/KR-causal"
            self.assertEqual(json.loads((result / "status.json").read_text())["state"], "complete")
            summary = json.loads((result / "summary.json").read_text())
            self.assertEqual(len(summary["hypotheses"]), 2)
            self.assertEqual(summary["hypotheses"][1]["summary"]["initial_equity"], 5000)
            with sqlite3.connect(result / "evaluation.sqlite3") as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM sealed_inputs").fetchone()[0], 15)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM day_results").fetchone()[0], 5)

    def test_long_controller_streams_full_features_and_exports_daily_losses(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            (output / "jobs").mkdir()
            paths = {}
            for day in DAYS:
                base = output / day
                base.mkdir()
                dataset = base / "source.sqlite3"
                dataset.write_text("Private future prices")
                scope = {
                    "KR": {
                        "start": day + "T00:00:00+00:00",
                        "end": day + "T00:15:00+00:00",
                        "sessions": [
                            {"start": day + "T00:00:00+00:00", "end": day + "T00:15:00+00:00"}
                        ]
                        * 2,
                    }
                }
                with (
                    sqlite3.connect(base / "portfolios.sqlite3") as db,
                    sqlite3.connect(base / "search.sqlite3") as features,
                ):
                    db.executescript(
                        "CREATE TABLE metadata(key TEXT,value TEXT);"
                        "CREATE TABLE frames(id INTEGER,at TEXT,sha256 TEXT,payload BLOB);"
                    )
                    features.execute(
                        "CREATE TABLE frames(at TEXT,output_sha256 TEXT,basis_gzip BLOB)"
                    )
                    db.executemany(
                        "INSERT INTO metadata VALUES (?,?)",
                        [
                            ("manifest", encode({"scopes": scope})),
                            ("symbols", encode([["KR", "A"]])),
                        ],
                    )
                    for ordinal, price in enumerate((100, 80, 70), 1):
                        at = day + f"T00:{ordinal * 5:02d}:00+00:00"
                        raw = encode(
                            {
                                "at": at,
                                "quotes": {0: price},
                                "marks": {0: [price, at]},
                                "ranks": [[[[0, 1, 0.01]], []]],
                            }
                        ).encode()
                        db.execute(
                            "INSERT INTO frames VALUES (?,?,?,?)",
                            (ordinal, at, hashlib.sha256(raw).hexdigest(), gzip.compress(raw)),
                        )
                        raw = encode(
                            {
                                "features": [
                                    {
                                        "market": "KR",
                                        "symbol": "A",
                                        "quote_at": at,
                                        "price": price,
                                        "reference_times": [None] * 5,
                                        "scores": [0.01] * 30,
                                        "volumes": [2] * 5,
                                        "noises": [3] * 5,
                                    }
                                ],
                                "decisions": ["Forbidden outcome data"],
                            }
                        ).encode()
                        features.execute(
                            "INSERT INTO frames VALUES (?,?,?)",
                            (at, hashlib.sha256(raw).hexdigest(), gzip.compress(raw)),
                        )
                (output / "jobs" / (day + ".json")).write_text(
                    encode(
                        {
                            "portfolio_sha256": digest(base / "portfolios.sqlite3"),
                            "feature_sha256": digest(base / "search.sqlite3"),
                        }
                    )
                )
                paths[day] = dataset, base, base
            manifest = {
                "deadline": None,
                "long_only": True,
                "symbols": [["KR", "A"]],
                "policies": [
                    {"id": "cold", "mode": "daily", "reset_daily": True, "reset_learning": True},
                    {
                        "id": "ensemble",
                        "mode": "weighted_equal",
                        "window": 1,
                        "allocation": "family_ensemble",
                    },
                ],
                "code_sha256": {},
            }
            with (
                patch(
                    "tools.run_week_experiment.locations", side_effect=lambda out, day: paths[day]
                ),
                patch("tools.search_portfolios.configurations", return_value=configurations(1)[:1]),
                patch("tools.search_portfolios.signals", return_value=signals()[:1]),
            ):
                evaluate(output, "KR", False, manifest)
            export_long_results(output, manifest)
            import csv

            with (output / "daily-comparison.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 10)
            cold = [row for row in rows if row["hypothesis"] == "cold"]
            self.assertTrue(all(float(row["start_equity"]) == 1000 for row in cold))
            self.assertTrue(all(int(row["losing_trades"]) == 2 for row in cold))
            with (output / "losing-trades.csv").open() as stream:
                losses = list(csv.DictReader(stream))
            self.assertTrue(losses)
            self.assertTrue(all(float(row["position_return_pct"]) < 0 for row in losses))
            with sqlite3.connect(output / "results/KR-causal/evaluation.sqlite3") as db:
                self.assertEqual(
                    db.execute(
                        "SELECT COUNT(*) FROM sealed_inputs WHERE source_feature_sha256 IS NOT NULL"
                    ).fetchone()[0],
                    15,
                )

            whole = output / "whole"
            (whole / "jobs").mkdir(parents=True)
            for seal in (output / "jobs").glob("*.json"):
                (whole / "jobs" / seal.name).write_bytes(seal.read_bytes())
            whole_manifest = {
                **manifest,
                "whole_shares": True,
                "initial_capitals_usd": [50],
                "krw_per_usd": 10,
            }
            with (
                patch(
                    "tools.run_week_experiment.locations", side_effect=lambda out, day: paths[day]
                ),
                patch("tools.search_portfolios.configurations", return_value=configurations(1)[:1]),
                patch("tools.search_portfolios.signals", return_value=signals()[:1]),
            ):
                self.assertEqual(
                    evaluate(whole, "KR", False, whole_manifest, 50), "KR-causal-usd50"
                )
            export_long_results(whole, whole_manifest)
            with gzip.open(whole / "results/KR-causal-usd50/report.json.gz", "rt") as stream:
                report = json.load(stream)
            for hypothesis in report["hypotheses"]:
                self.assertEqual(hypothesis["starting_capital_usd"], 50)
                self.assertTrue(
                    all(float(t["quantity_normalized"]).is_integer() for t in hypothesis["trades"])
                )
                self.assertTrue(all(t["cash"] >= -1e-8 for t in hypothesis["trades"]))
                self.assertTrue(
                    all(t["native_price"] == t["price"] * 10 for t in hypothesis["trades"])
                )
            self.assertEqual(report["hypotheses"][0]["summary"]["initial_equity"], 250)
            with (whole / "daily-comparison.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 10)
            self.assertTrue(all(float(row["starting_capital_usd"]) == 50 for row in rows))


if __name__ == "__main__":
    unittest.main()
