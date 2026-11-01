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
    digest,
    encode,
    evaluate,
    policies,
    remap_frame,
)
from tools.search_portfolios import configurations, signals


class WeekRunnerTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
