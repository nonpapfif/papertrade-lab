"""Run native session and replay paths against real offline journals and report bundles."""

import argparse
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

from papertrade.jsonio import atomic_json, read_json
from tests.test_session_research import Service
from tools.adaptive_journal import AdaptiveJournal, JournalError
from tools.research_session import Comparison, replay, run, session_plan
from tools.session_projection import ProjectionClock
from tools.session_research import Candidate, SessionResearch


class ResearchSessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def plan(self, market: str) -> dict[str, Any]:
        hour = 14 if market == "US" else 0
        minute = 30 if market == "US" else 0
        start = datetime(2026, 11, 17, hour, minute, tzinfo=UTC)
        end = start + timedelta(minutes=20)
        regular = {"startTime": start.isoformat(), "endTime": end.isoformat()}
        today: dict[str, Any] = {"date": "2026-11-17"}
        if market == "US":
            today["regularMarket"] = regular
        else:
            regular.update(
                singlePriceAuctionStartTime=end.isoformat(),
                endTime=(end + timedelta(minutes=10)).isoformat(),
            )
            today["integrated"] = {"regularMarket": regular}
        plan = session_plan(
            market, today["date"], 100 if market == "US" else 100_000, {"today": today}
        )
        return {
            **plan,
            "source_commit": "offline-fixture",
            "source_digest": "fixture-source",
            "parallelism": 1,
        }

    def bars(self, plan: dict[str, Any], at: datetime) -> list[dict[str, str]]:
        unit, base = (0.01, 10) if plan["market"] == "US" else (1, 1000)
        return [
            {
                "timestamp": (at - timedelta(minutes=15 - index)).isoformat(),
                "closePrice": str(round(base + index * unit, 2)),
                "highPrice": str(round(base + index * unit, 2)),
            }
            for index in range(16)
        ]

    def book(self, plan: dict[str, Any], at: datetime, *, falling: bool = False) -> dict[str, Any]:
        if plan["market"] == "US":
            bid, ask = (9.84, 9.86) if falling else (10.14, 10.16)
        else:
            bid, ask = (984, 986) if falling else (1014, 1016)
        return {
            "timestamp": at.isoformat(),
            "currency": "USD" if plan["market"] == "US" else "KRW",
            "asks": [{"price": str(ask), "volume": "1000"}],
            "bids": [{"price": str(bid), "volume": "1000"}],
        }

    def entry(self, kind: str, at: datetime, payload: Any = None) -> dict[str, Any]:
        return {"kind": kind, "received": at.isoformat(), "at": at.isoformat(), "payload": payload}

    def rows(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        start = datetime.fromisoformat(plan["start"])
        symbol = "A" if plan["market"] == "US" else "005930"
        rows = [
            self.entry("calendar", start, (plan["day"], plan["calendar"])),
            self.entry("selection", start, [symbol]),
        ]
        for minute in range(20):
            at = start + timedelta(minutes=minute)
            if minute:
                rows.append(self.entry("selection", at, [symbol]))
            if minute >= 16:
                rows.append(self.entry("feature", at, (symbol, self.bars(plan, at))))
            if plan["market"] == "US":
                rows.append(
                    self.entry(
                        "fx",
                        at,
                        {
                            "midRate": "1400",
                            "validFrom": (at - timedelta(seconds=60)).isoformat(),
                            "validUntil": (at + timedelta(seconds=60)).isoformat(),
                        },
                    )
                )
            rows.append(self.entry("book", at, (symbol, self.book(plan, at))))
            if minute in (16, 17):
                later = at + timedelta(seconds=2)
                rows.append(self.entry("book", later, (symbol, self.book(plan, later))))
        end = datetime.fromisoformat(plan["end"])
        rows.append(
            self.entry(
                "book",
                end - timedelta(seconds=2),
                (symbol, self.book(plan, end - timedelta(seconds=2))),
            )
        )
        rows.append(self.entry("final_advance", end))
        return rows

    def journal(
        self, root: Path, plan: dict[str, Any], rows: list[dict[str, Any]], *, complete: bool = True
    ) -> Path:
        root.mkdir(parents=True, exist_ok=True)
        path = root / "journal.sqlite3"
        identity = {
            "profile": "broker-constrained",
            "market": plan["market"],
            "capital": plan["capital"],
            "end_epoch": datetime.fromisoformat(plan["end"]).timestamp(),
        }
        with AdaptiveJournal.open(path, identity, resume=False) as journal:
            for row in rows:
                journal.append(row)
            if complete:
                journal.complete()
        return path

    async def provision(
        self,
        market: str,
        *,
        state: str = "complete",
        missing: str | None = None,
    ) -> tuple[Path, dict[str, Any]]:
        root = self.root / f"{market}-{state}{'-' + missing if missing else ''}"
        root.mkdir()
        plan = self.plan(market)
        atomic_json(root / "session.json", plan)
        rows = self.rows(plan)
        if missing == "eligible_feature":
            for row in rows:
                if row["kind"] == "selection":
                    row["payload"].append("B")
        elif missing is not None:
            first = next(row for row in rows if row["kind"] == missing)
            rows = [row for row in rows if row["kind"] != missing or row is first]
        if state != "complete":
            rows = rows[:-2]
        self.journal(root / "schedule", plan, rows, complete=state == "complete")
        atomic_json(root / "schedule" / "status.json", {"state": state})
        start = datetime.fromisoformat(plan["start"])
        observed = start + timedelta(minutes=16)
        symbol = "A" if market == "US" else "005930"
        service = Service(root / "research" / "reports")
        research = SessionResearch(
            root / "research",
            service,
            expires_at=plan["end"],
            clock=lambda: observed.timestamp() + 0.5,
        )
        try:
            await research.submit(
                [
                    Candidate(
                        {
                            "market": market,
                            "symbol": symbol,
                            "as_of": observed.isoformat(),
                            "candles": self.bars(plan, observed),
                        },
                        signal={"strategy": "trend", "event": "offline-initial-signal"},
                    )
                ],
                observed.isoformat(),
            )
            await research.drain()
        finally:
            await research.aclose()
        return root, plan

    async def native(
        self,
        root: Path,
        plan: dict[str, Any],
        *,
        returncode: int = 0,
        backlog_at: datetime | None = None,
    ) -> dict[str, Any]:
        args = argparse.Namespace(
            market=plan["market"],
            capital=plan["capital"],
            day=datetime.fromisoformat(plan["start"]).date(),
            research_workers=plan["parallelism"],
        )
        service = Service(root / "research" / "reports")
        with sqlite3.connect(root / "schedule" / "journal.sqlite3") as db:
            events = [
                json.loads(row[0]) for row in db.execute("SELECT body FROM events ORDER BY id")
            ]
        arrivals = {
            entry["index"]: datetime.fromisoformat(entry["received"]).timestamp()
            for entry in events
        }
        real_cut = ProjectionClock.cut

        def cut(clock: ProjectionClock, index: int, now: float) -> float:
            fixture_time = backlog_at.timestamp() if backlog_at is not None else arrivals[index]
            return real_cut(clock, index, fixture_time)

        with ExitStack() as stack:
            stack.enter_context(
                patch("tools.research_session.tree_digest", return_value="fixture-source")
            )
            stack.enter_context(
                patch(
                    "tools.research_session.Settings.load",
                    return_value=SimpleNamespace(
                        secrets_dir=self.root / "secrets",
                        get=lambda section, key: "/offline/codex" if key == "binary" else True,
                    ),
                )
            )
            stack.enter_context(
                patch("tools.research_session.importlib.util.find_spec", return_value=object())
            )
            stack.enter_context(patch("tools.research_session.toss_service", return_value=service))
            launch = stack.enter_context(patch("tools.research_session.start_job"))
            stack.enter_context(
                patch("tools.research_session.poll_job", return_value={"returncode": returncode})
            )
            stack.enter_context(patch("tools.research_session.asyncio.sleep", new=AsyncMock()))
            stack.enter_context(patch("tools.research_session.ProjectionClock.cut", new=cut))
            result = await run(args, root)
        report = read_json(root / "comparison.json")
        self.assertEqual(result, int(report["stream_state"] != "complete"))
        launch.assert_called_once()
        command = launch.call_args.args[1]
        self.assertIn("--recover", command)
        self.assertIn("--dynamic-slots", command)
        self.assertIn("--research-protection", command)
        self.assertEqual(service.calls, [])
        self.assertTrue(service.closed)
        return report

    async def rebuild(self, root: Path, output: Path) -> None:
        with (
            patch("tools.research_session.tree_digest", return_value="fixture-source"),
            patch(
                "tools.research_session.toss_service",
                side_effect=AssertionError("Replay invoked research"),
            ),
            patch(
                "tools.research_session.start_job",
                side_effect=AssertionError("Replay launched a worker"),
            ),
        ):
            self.assertEqual(await replay(root, output), 0)

    async def test_fresh_session_fetches_one_calendar_and_seals_the_contract(self) -> None:
        root, plan = await self.provision("US")
        (root / "session.json").unlink()
        before_open = datetime.fromisoformat(plan["start"]).timestamp() - 60
        with (
            patch("tools.research_session.MarketClient") as broker,
            patch("tools.research_session.command", return_value="offline-fixture"),
            patch(
                "tools.research_session.time",
                SimpleNamespace(monotonic=time.monotonic, time=lambda: before_open),
            ),
        ):
            broker.return_value.calendar.return_value = plan["calendar"]
            report = await self.native(root, plan)
        broker.return_value.calendar.assert_called_once_with("US", plan["day"])
        self.assertEqual(read_json(root / "session.json"), plan)
        self.assertTrue(report["session_complete"])
        await self.rebuild(root, self.root / "reconstructed-fresh")

    async def test_missing_dependency_is_detected_before_broker_access(self) -> None:
        args = argparse.Namespace(
            market="US", day=datetime.now(UTC).date(), capital=100, research_workers=1
        )
        with (
            patch("tools.research_session.importlib.util.find_spec", return_value=None),
            patch("tools.research_session.MarketClient") as broker,
            self.assertRaises(ImportError),
        ):
            await run(args, self.root)
        broker.assert_not_called()

    async def test_native_kr_us_session_replay_preserves_equal_shared_inputs_and_real_accounts(
        self,
    ) -> None:
        for market in ("US", "KR"):
            with self.subTest(market=market):
                root, plan = await self.provision(market)
                report = await self.native(root, plan)
                self.assertEqual(report["stream_state"], "complete")
                self.assertTrue(report["ranking_eligible"])
                self.assertEqual(report["book_coverage_fraction"], 1)
                self.assertTrue(report["strategy_inputs_qualified"])
                self.assertEqual(report["scan_coverage_fraction"], 1)
                self.assertEqual(report["max_scan_gap_seconds"], 60)
                self.assertEqual(report["eligible_symbol_feature_coverage"], 1)
                baseline = report["accounts"]["existing_selection"]["summary"]
                research = report["accounts"]["research_selection"]["summary"]
                rebound = report["accounts"]["research_alternative_strategy"]["summary"]
                self.assertEqual(baseline["account"]["entries"], 1)
                self.assertEqual(research["account"]["entries"], 1)
                self.assertEqual(rebound["account"]["entries"], 0)
                self.assertLess(
                    baseline["intents"][0]["decision_at"], research["intents"][0]["decision_at"]
                )
                self.assertEqual(baseline["pending_intents"], 0)
                self.assertEqual(baseline["positions_as_of"], plan["end"])
                self.assertEqual(report["research_attempts"], {"completed": 1})
                self.assertTrue(read_json(root / "protected.json")["symbols"])
                output = root / "replayed"
                await self.rebuild(root, output)
                self.assertEqual(read_json(output / "comparison.json"), report)

    async def test_partial_native_session_retains_positions_without_ranking_and_replays(
        self,
    ) -> None:
        root, plan = await self.provision("US", state="failed")
        report = await self.native(root, plan, returncode=1)
        self.assertEqual(report["stream_state"], "failed")
        self.assertFalse(report["ranking_eligible"])
        self.assertEqual(report["ranking"], [])
        account = report["accounts"]["existing_selection"]["summary"]
        self.assertTrue(account["positions"])
        self.assertLess(account["account"]["cash"], plan["capital"])
        await self.rebuild(root, root / "replayed")

    async def test_complete_scheduler_cannot_seal_an_active_capture_as_complete(self) -> None:
        root, plan = await self.provision("US")
        with sqlite3.connect(root / "schedule" / "journal.sqlite3") as db:
            db.execute("UPDATE header SET state='active'")
        report = await self.native(root, plan)
        self.assertEqual(report["stream_state"], "incomplete")
        self.assertFalse(report["session_complete"])
        self.assertEqual(report["ranking"], [])
        await self.rebuild(root, root / "replayed")

    async def test_late_backlog_cannot_fill_a_new_intent_from_already_received_quotes(self) -> None:
        root, plan = await self.provision("US")
        report = await self.native(
            root, plan, backlog_at=datetime.fromisoformat(plan["end"]) - timedelta(seconds=2)
        )
        account = report["accounts"]["existing_selection"]["summary"]
        self.assertGreater(account["intent_count"], 0)
        self.assertEqual(account["account"]["entries"], 0)
        self.assertEqual(account["account"]["cash"], plan["capital"])
        self.assertEqual(account["pending_intents"], 0)
        self.assertEqual(report["ranking"], [])
        self.assertLess(report["book_coverage_fraction"], 0.8)
        await self.rebuild(root, root / "replayed")

    async def test_unqualified_scans_or_features_keep_complete_capture_but_suppress_ranking(
        self,
    ) -> None:
        for missing in ("selection", "feature", "eligible_feature"):
            with self.subTest(missing=missing):
                root, plan = await self.provision("US", missing=missing)
                report = await self.native(root, plan)
                self.assertEqual(report["stream_state"], "complete")
                self.assertTrue(report["session_complete"])
                self.assertEqual(report["book_coverage_fraction"], 1)
                self.assertFalse(report["ranking_eligible"])
                self.assertFalse(report["strategy_inputs_qualified"])
                self.assertEqual(report["ranking"], [])
                self.assertEqual(report["evaluation_status"], "inconclusive")
                if missing == "selection":
                    self.assertEqual(report["scan_coverage_fraction"], 0.05)
                    self.assertGreater(report["max_scan_gap_seconds"], 180)
                    self.assertEqual(report["eligible_symbol_feature_coverage"], 1)
                else:
                    self.assertEqual(report["scan_coverage_fraction"], 1)
                    self.assertEqual(report["max_scan_gap_seconds"], 60)
                    self.assertEqual(
                        report["eligible_symbol_feature_coverage"],
                        0.5 if missing == "eligible_feature" else 0.375,
                    )
                baseline = report["accounts"]["existing_selection"]["summary"]
                research = report["accounts"]["research_selection"]["summary"]
                self.assertEqual(baseline["account"]["entries"], 1)
                self.assertEqual(research["account"]["entries"], 1)
                self.assertTrue(baseline["comparison_eligible"])
                self.assertEqual(read_json(root / "capture.json"), {"state": "complete"})
                await self.rebuild(root, root / "replayed")

    async def test_replay_rejects_tampered_journal_sealed_comparison_and_projection(self) -> None:
        for target in (
            "journal",
            "comparison",
            "projection",
            "missing_projection",
            "partial_projection",
        ):
            with self.subTest(target=target):
                root, plan = await self.provision("US")
                await self.native(root, plan)
                if target == "journal":
                    with sqlite3.connect(root / "schedule" / "journal.sqlite3") as db:
                        row = json.loads(
                            db.execute("SELECT body FROM events WHERE id=2").fetchone()[0]
                        )
                        row["payload"] = ["ALTERED"]
                        db.execute("UPDATE events SET body=? WHERE id=2", (json.dumps(row),))
                    expected: type[Exception] = JournalError
                elif target == "comparison":
                    report = read_json(root / "comparison.json")
                    report["accounts"]["existing_selection"]["summary"]["account"]["cash"] = (
                        1_000_000
                    )
                    atomic_json(root / "comparison.json", report)
                    expected = ValueError
                elif target == "missing_projection":
                    (root / "projection.bin").unlink()
                    expected = FileNotFoundError
                else:
                    path = root / "projection.bin"
                    raw = bytearray(path.read_bytes())
                    if target == "projection":
                        raw[0] ^= 1
                    else:
                        raw.pop()
                    path.write_bytes(raw)
                    expected = ValueError
                with (
                    patch("tools.research_session.tree_digest", return_value="fixture-source"),
                    self.assertRaises(expected),
                ):
                    await replay(root, root / "rejected")
                self.assertFalse((root / "rejected").exists())
                # The next subtest needs a fresh independent session directory.
                root.rename(root.with_name(f"{root.name}-{target}"))

    async def test_actual_quote_family_loss_and_return_trigger_research_without_timer_calls(
        self,
    ) -> None:
        root = self.root / "transitions"
        plan = self.plan("US")
        at = datetime.fromisoformat(plan["start"]) + timedelta(minutes=16)
        rows = [
            self.entry("calendar", at, (plan["day"], plan["calendar"])),
            self.entry("selection", at, ["A"]),
            self.entry("feature", at, ("A", self.bars(plan, at))),
            self.entry("book", at, ("A", self.book(plan, at))),
        ]
        for seconds, kind, falling in (
            (2, "book", False),
            (3, "clock", False),
            (4, "book", True),
            (5, "book", False),
            (6, "book", False),
        ):
            tick = at + timedelta(seconds=seconds)
            rows.append(
                self.entry(
                    kind,
                    tick,
                    ("A", self.book(plan, tick, falling=falling)) if kind == "book" else None,
                )
            )
        path = self.journal(root, plan, rows)
        clock = [at.timestamp()]
        service = Service(root / "research" / "reports")
        research = SessionResearch(
            root / "research", service, expires_at=plan["end"], clock=lambda: clock[0]
        )
        comparison = Comparison(plan, path, research)
        try:
            for entry in comparison.feed.read():
                clock[0] = datetime.fromisoformat(entry["received"]).timestamp()
                with patch("tools.research_session.time.time", side_effect=lambda: clock[0]):
                    await comparison.apply(entry, live=True)
                await research.drain()
            self.assertEqual(service.calls, ["A", "A"])
            self.assertEqual([row["status"] for row in research.rows], ["completed", "completed"])
            self.assertNotEqual(
                research.rows[0]["signal"]["event"], research.rows[1]["signal"]["event"]
            )
        finally:
            await research.aclose()
            await comparison.accounts.close()


if __name__ == "__main__":
    unittest.main()
