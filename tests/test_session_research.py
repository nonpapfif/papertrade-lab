"""Verify causal research publication, bounded work, failures and recovery offline."""

import asyncio
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from papertrade.jsonio import atomic_json
from tools.session_research import (
    _REVISION,
    _ROLES,
    Candidate,
    ResearchReplay,
    SessionResearch,
    _iso,
)


def candidate(symbol: str, signal: Any = None, *, cutoff: float = 100) -> Candidate:
    return Candidate(
        {
            "market": "US",
            "symbol": symbol,
            "as_of": _iso(cutoff),
            "candles": [{"timestamp": _iso(90), "closePrice": "10"}],
            "candle_provenance": {"source": "recorded", "received_at": _iso(95)},
        },
        signal=signal,
    )


class Service:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls: list[str] = []
        self.fail: set[str] = set()
        self.ratings: dict[str, str] = {}
        self.gate: asyncio.Event | None = None
        self.active = 0
        self.maximum = 0
        self.closed = False

    async def analyze(self, snapshot: dict[str, Any], inputs: Any = None) -> Any:
        symbol = snapshot["symbol"]
        self.calls.append(symbol)
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            if self.gate is not None:
                await self.gate.wait()
            await asyncio.sleep(0)
            if symbol in self.fail:
                raise ValueError("Synthetic role failure")
            # Concurrent fixtures must have distinct identities as real service bundles do.
            directory = (
                self.root
                / hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
            )
            directory.mkdir(parents=True, exist_ok=True)
            statuses = {
                role: (
                    "skipped"
                    if role
                    in {
                        "sentiment-analyst",
                        "news-analyst",
                        "fundamentals-analyst",
                    }
                    else "completed"
                )
                for role in _ROLES
            }
            atomic_json(
                directory / "manifest.json",
                {
                    "run_id": directory.name,
                    "status": "completed",
                    "reports": statuses,
                    "tradingagents_revision": _REVISION,
                    "model": "offline-fixture",
                    "executed_nodes": ["Portfolio Manager"],
                    "rating": self.ratings.get(symbol, "Buy"),
                },
            )
            atomic_json(
                directory / "evidence.json",
                {
                    "snapshot": snapshot,
                    "inputs": {
                        name: (inputs or {}).get(name, [])
                        for name in ("news", "sentiment", "fundamentals")
                    },
                },
            )
            for name in ("report", *_ROLES):
                (directory / f"{name}.md").write_text("Offline role evidence")
            return SimpleNamespace(directory=directory)
        finally:
            self.active -= 1

    async def aclose(self) -> None:
        self.closed = True
        if self.gate is not None:
            self.gate.set()


class ResearchTests(unittest.IsolatedAsyncioTestCase):
    async def test_publication_is_completion_not_evidence_time_and_restart_keeps_dedup(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            clock = [110.0]
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: clock[0]
            )
            service.gate = asyncio.Event()
            await adapter.submit([candidate("F")], _iso(105))
            self.assertFalse(adapter.selection_at(110))
            clock[0] = 120
            service.gate.set()
            await adapter.drain()
            self.assertFalse(adapter.selection_at(119))
            self.assertEqual(adapter.selection_at(120), {"F"})
            self.assertFalse(adapter.selection_at(200))
            await adapter.aclose()
            replay = ResearchReplay(root / "state")
            self.assertEqual(replay.at(120), {"F"})
            restarted = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: clock[0]
            )
            await restarted.submit([candidate("F", cutoff=115)], _iso(115))
            await restarted.drain()
            self.assertEqual(service.calls, ["F"])
            await restarted.aclose()

    async def test_new_symbols_and_signals_work_without_fixed_cohort_and_failures_isolate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            service.fail.add("FAIL")
            service.gate = asyncio.Event()
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), max_concurrency=2, clock=lambda: 120
            )
            await adapter.submit(
                [candidate(s, "entry") for s in ("A", "B", "C", "FAIL")], _iso(105)
            )
            await asyncio.sleep(0)
            service.gate.set()
            await adapter.drain()
            self.assertEqual(adapter.selection_at(120), {"A", "B", "C"})
            self.assertEqual(service.maximum, 2)
            await adapter.submit(
                [candidate("D", "entry"), candidate("A", "entry", cutoff=110)], _iso(115)
            )
            await adapter.drain()
            self.assertEqual(service.calls.count("A"), 1)
            self.assertEqual(adapter.selection_at(120), {"A", "B", "C", "D"})
            service.fail.add("A")
            await adapter.submit([candidate("A", "new-entry", cutoff=115)], _iso(115))
            await adapter.drain()
            self.assertIn("A", adapter.selection_at(120))
            self.assertEqual(adapter.metadata()["attempts"], {"completed": 4, "failed": 2})
            await adapter.aclose()

    async def test_close_cancels_pending_and_restart_does_not_repeat_unknown_calls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            service.gate = asyncio.Event()
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), max_concurrency=1, clock=lambda: 120
            )
            await adapter.submit([candidate("A"), candidate("B")], _iso(105))
            await asyncio.sleep(0)
            await asyncio.wait_for(adapter.aclose(), 1)
            self.assertEqual(service.calls, ["A"])
            self.assertTrue(service.closed)
            self.assertEqual(adapter.metadata()["attempts"], {"interrupted": 2})
            restarted = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: 120
            )
            await restarted.submit([candidate("A"), candidate("B")], _iso(105))
            await restarted.drain()
            self.assertEqual(service.calls, ["A"])
            await restarted.aclose()

    async def test_future_evidence_and_tampered_bundles_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: 120
            )
            future = candidate("A")
            future.snapshot["candles"][0]["timestamp"] = _iso(101)
            with self.assertRaisesRegex(ValueError, "future evidence"):
                await adapter.submit([future], _iso(105))
            await adapter.submit([candidate("A")], _iso(105))
            await adapter.drain()
            row = adapter.publications()[0]
            (adapter.output_dir / row["report"]["bundle"] / "market-analyst.md").write_text(
                "Changed"
            )
            with self.assertRaisesRegex(ValueError, "changed"):
                ResearchReplay(root / "state")
            await adapter.aclose()

    async def test_family_loss_invalidates_active_and_queued_generations_without_repeating(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            service.gate = asyncio.Event()
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), max_concurrency=1, clock=lambda: 120
            )
            await adapter.submit([candidate("A", "entry"), candidate("B", "entry")], _iso(105))
            await asyncio.sleep(0)
            await adapter.invalidate("US", "A")
            await adapter.invalidate("US", "B")
            service.gate.set()
            await adapter.drain()
            self.assertEqual(service.calls, ["A"])
            self.assertFalse(adapter.selection_at(120))
            self.assertEqual(adapter.metadata()["attempts"], {"invalidated": 2})
            await adapter.submit([candidate("A", "entry"), candidate("B", "entry")], _iso(105))
            await adapter.drain()
            self.assertEqual(service.calls, ["A"])
            await adapter.submit([candidate("A", "new-entry", cutoff=115)], _iso(115))
            await adapter.drain()
            await adapter.invalidate("US", "A")
            self.assertEqual(adapter.selection_at(120), {"A"})
            self.assertEqual(service.calls, ["A", "A"])
            await adapter.aclose()

    async def test_graph_finishing_after_review_horizon_is_expired_without_admission(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            service.gate = asyncio.Event()
            clock = [120.0]
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: clock[0]
            )
            await adapter.submit([candidate("A")], _iso(105))
            await asyncio.sleep(0)
            clock[0] = 200
            service.gate.set()
            await adapter.drain()
            row = adapter.publications()[0]
            self.assertEqual(row["status"], "expired")
            self.assertIn("report", row)
            self.assertEqual(adapter.metadata()["attempts"], {"expired": 1})
            self.assertFalse(adapter.selection_at(200))
            self.assertFalse(ResearchReplay(root / "state").at(200))
            await adapter.aclose()

    async def test_publication_commit_does_not_yield_before_live_index_update(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            adapter = SessionResearch(
                root / "state", Service(root / "reports"), expires_at=_iso(200), clock=lambda: 120
            )
            loop = asyncio.get_running_loop()
            observed = []

            def persist(path: Path, data: dict[str, Any]) -> None:
                atomic_json(path, data)
                if data["status"] == "completed":
                    loop.call_soon_threadsafe(lambda: observed.append(adapter.selection_at(120)))

            with patch("tools.session_research.atomic_json", side_effect=persist):
                await adapter.submit([candidate("A")], _iso(105))
                await adapter.drain()
            self.assertEqual(observed, [frozenset({"A"})])
            self.assertEqual(ResearchReplay(root / "state").at(120), observed[0])
            await adapter.aclose()

    async def test_thousand_same_symbol_generations_keep_one_queued_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            service.gate = asyncio.Event()
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), max_concurrency=1, clock=lambda: 120
            )
            await adapter.submit([candidate("BLOCK", "active")], _iso(105))
            await asyncio.sleep(0)
            await adapter.submit(
                (candidate("A", f"transition-{index}") for index in range(1000)), _iso(105)
            )
            self.assertEqual(adapter._queue.qsize(), 1)
            self.assertEqual(len(adapter._pending), 1)
            self.assertEqual(service.calls, ["BLOCK"])
            rows = adapter.publications()
            self.assertEqual(len(rows), 1001)
            self.assertEqual(sum(row["status"] == "superseded" for row in rows), 999)
            self.assertEqual(len(list((root / "state/attempts").glob("*.json"))), 1001)
            self.assertEqual(sorted(row["submission_index"] for row in rows), list(range(1, 1002)))
            await adapter.invalidate("US", "A")
            await adapter.submit([candidate("A", "latest")], _iso(105))
            self.assertEqual(adapter._queue.qsize(), 1)
            service.gate.set()
            await adapter.drain()
            self.assertEqual(service.calls, ["BLOCK", "A"])
            self.assertEqual(adapter.selection_at(120), {"BLOCK", "A"})
            final = adapter.publications()
            self.assertEqual(sum(row["status"] == "invalidated" for row in final), 1)
            self.assertEqual(sum(row["status"] == "superseded" for row in final), 999)
            self.assertEqual(sum(row["status"] == "completed" for row in final), 2)
            await adapter.aclose()

    async def test_superseded_pending_and_inflight_signals_do_not_replace_newer_result(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            service.gate = asyncio.Event()
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), max_concurrency=1, clock=lambda: 120
            )
            await adapter.submit([candidate("A", "first")], _iso(105))
            await asyncio.sleep(0)
            await adapter.submit(
                [candidate("A", "middle", cutoff=110), candidate("A", "newest", cutoff=115)],
                _iso(115),
            )
            service.gate.set()
            await adapter.drain()
            self.assertEqual(service.calls, ["A", "A"])
            self.assertEqual(adapter.metadata()["attempts"], {"superseded": 2, "completed": 1})
            await adapter.submit([candidate("A", "outdated", cutoff=100)], _iso(115))
            await adapter.drain()
            self.assertEqual(service.calls, ["A", "A"])
            self.assertEqual(adapter.selection_at(120), {"A"})
            await adapter.aclose()

    async def test_copy_replays_relative_bundles_and_same_interval_queries_use_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            adapter = SessionResearch(
                source / "state",
                Service(source / "reports"),
                expires_at=_iso(200),
                clock=lambda: 120,
            )
            await adapter.submit([candidate("A")], _iso(105))
            await adapter.drain()
            await adapter.aclose()
            original_publications = adapter.publications()
            shutil.copytree(source, root / "copy")
            shutil.rmtree(source)
            replay = ResearchReplay(root / "copy/state")
            self.assertEqual(replay.publications(), original_publications)
            live_hash = hashlib.sha256(json.dumps(original_publications, sort_keys=True).encode())
            replay_hash = hashlib.sha256(json.dumps(replay.publications(), sort_keys=True).encode())
            self.assertEqual(live_hash.hexdigest(), replay_hash.hexdigest())
            self.assertFalse(Path(original_publications[0]["report"]["bundle"]).is_absolute())
            self.assertEqual(replay.at(120), {"A"})
            with patch("tools.session_research._epoch", side_effect=AssertionError("rescanned")):
                self.assertEqual(replay.at(121), {"A"})
                self.assertEqual(replay.at(199), {"A"})
                self.assertFalse(replay.at(200))

    async def test_failed_service_partial_role_reports_are_retained_and_do_not_admit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)

            class PartialService(Service):
                async def analyze(self, snapshot: dict[str, Any], inputs: Any = None) -> Any:
                    result = await super().analyze(snapshot, inputs)
                    manifest_path = result.directory / "manifest.json"
                    manifest = json.loads(manifest_path.read_text())
                    manifest.update(status="failed", rating=None, error_type="ValueError")
                    manifest["reports"]["portfolio-manager"] = "not_run"
                    atomic_json(manifest_path, manifest)
                    error = ValueError("Synthetic portfolio failure")
                    error.add_note(f"Partial role reports: {result.directory}")
                    raise error

            adapter = SessionResearch(
                root / "state",
                PartialService(root / "reports"),
                expires_at=_iso(200),
                clock=lambda: 120,
            )
            await adapter.submit([candidate("A")], _iso(105))
            await adapter.drain()
            self.assertFalse(adapter.selection_at(120))
            row = adapter.publications()[0]
            self.assertEqual(row["partial_reports"][0]["roles"]["market-analyst"], "completed")
            self.assertEqual(row["partial_reports"][0]["roles"]["portfolio-manager"], "not_run")
            self.assertTrue(row["partial_reports"][0]["files_sha256"])
            await adapter.aclose()

    async def test_unclean_pending_restart_is_interrupted_and_expiry_stops_new_calls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = Service(root / "reports")
            state = root / "state"
            (state / "attempts").mkdir(parents=True)
            atomic_json(
                state / "attempts" / "unknown.json",
                {
                    "key": "unknown",
                    "status": "pending",
                    "snapshot": candidate("A").snapshot,
                    "observed_at": _iso(105),
                    "inputs": None,
                    "expires_at": _iso(200),
                },
            )
            adapter = SessionResearch(state, service, expires_at=_iso(200), clock=lambda: 200)
            self.assertEqual(adapter.metadata()["attempts"], {"interrupted": 1})
            await adapter.submit([candidate("B")], _iso(105))
            self.assertEqual(service.calls, [])
            await adapter.aclose()
