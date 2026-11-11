"""Exercise the finite worker's handoff against real journals and fake provider calls."""

from __future__ import annotations

import ast
import asyncio
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from papertrade.clock import utc_iso
from papertrade.jsonio import atomic_json, read_json
from tests.test_shared_feed_slots import add
from tools.adaptive_journal import AdaptiveJournal, JournalError, read_status
from tools.run_async_live_validation import ROOT, DeadlineMarketClient, campaign, drain_queue
from tools.session_update import (
    CUT_EVENT,
    ECONOMIC_DEFINITIONS,
    ECONOMIC_FILES,
    HOLD_EVENT,
    SOURCE_EVENT,
    held_inputs,
    load_ready,
    validate_target,
)
from tools.shared_feed_slots import SharedFeedSlots
from tools.slot_control import SlotControl

RUNNER = "tools.run_async_live_validation"


class WorkerUpdateTests(unittest.IsolatedAsyncioTestCase):
    def test_target_accepts_audit_changes_but_rejects_changes_to_frozen_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp)
            for name in (*ECONOMIC_FILES, *ECONOMIC_DEFINITIONS):
                path = target / name
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / name, path)
            for filename, owner, method in (
                ("tools/run_async_live_validation.py", "Runtime", "consume"),
                ("tools/shared_feed_slots.py", "SharedFeedSlots", "_apply"),
            ):
                file = target / filename
                source = file.read_text()
                owner_node = next(
                    node for node in ast.parse(source).body
                    if isinstance(node, ast.ClassDef) and node.name == owner
                )
                definition = next(
                    node for node in owner_node.body
                    if isinstance(node, ast.AsyncFunctionDef) and node.name == method
                )
                lines = source.splitlines(keepends=True)
                lines.insert(definition.body[0].lineno - 1, (
                    "        if self.audit is not None:\n"
                    '            self.audit.idle_workers(("ordered_dispatch",), progress=0)\n'
                ))
                file.write_text("".join(lines))
            request = {
                "target_root": str(target),
                "target_commit": "c" * 40,
                "target_digest": "d" * 64,
                "validation_report": str(target / "validation.json"),
            }
            with (
                patch("tools.session_update.validate_source"),
                patch("tools.session_update.command", return_value=str(ROOT / ".git")),
                patch("tools.session_update.require_report") as require_report,
            ):
                validate_target(request, source_root=ROOT)
                require_report.assert_called_once()
                require_report.reset_mock()
                engine = target / "tools/async_layered_paper.py"
                source = engine.read_text()
                self.assertIn("if proceeds > entry_cost:", source)
                engine.write_text(source.replace(
                    "if proceeds > entry_cost:", "if proceeds >= entry_cost:",
                ))
                with self.assertRaisesRegex(JournalError, "frozen economic behavior"):
                    validate_target(request, source_root=ROOT)
                require_report.assert_not_called()

    async def test_old_queue_drains_and_partial_candidate_replay_applies_held_inputs_once(
        self,
    ) -> None:
        try:
            import websockets.asyncio.client  # noqa: F401 - Optional provider dependency.
        except ModuleNotFoundError:
            self.skipTest("Optional websockets dependency is unavailable")
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name).resolve()
        update = root / "attempt-1" / "update"
        journal = root / "journal.sqlite3"
        spool = root / "controls.sqlite3"
        cutoff = time.time() + 45
        request = {
            "schema": 1,
            "from_commit": "a" * 40,
            "from_digest": "b" * 64,
            "target_root": str(ROOT),
            "target_commit": "c" * 40,
            "target_digest": "d" * 64,
            "validation_report": str(root / "validation.json"),
        }
        atomic_json(update / "request.json", request)
        with SlotControl.open(spool, initialize=True):
            pass

        holding = asyncio.Event()
        consume = SharedFeedSlots.consume
        begin_hold = SharedFeedSlots.begin_hold
        apply_held = SharedFeedSlots.apply_held

        async def parked_consumer(slots: SharedFeedSlots) -> None:
            await holding.wait()
            await consume(slots)

        def publish_during_drain(
            slots: SharedFeedSlots, held: AdaptiveJournal, request: dict[str, Any],
        ) -> None:
            self.assertGreater(slots.queue.qsize(), 0)
            begin_hold(slots, held, request)
            slots.control.submit(add("during-hold", 100), end_epoch=cutoff)
            slots.poll_controls()
            slots.publish("selection", ["HELD"])
            slots.publish("clock", None)
            holding.set()

        async def run(
            output: str, *, resume: bool = False, handoff: Path | None = None,
            capital: float = 100,
        ) -> None:
            await campaign(
                root / output,
                45,
                capital,
                profile="broker-constrained",
                adaptive=True,
                end_epoch=cutoff,
                journal_path=journal,
                slot_control=spool,
                campaign_seeds={},
                update_dir=update if not resume else None,
                resume=resume,
                resume_update=handoff,
            )

        with (
            patch(f"{RUNNER}.validate_source"),
            patch(f"{RUNNER}.validate_target"),
            patch.object(DeadlineMarketClient, "access_token", return_value="fixture-token"),
            patch.object(DeadlineMarketClient, "calendar", return_value={}),
            patch.object(DeadlineMarketClient, "recovery_token") as recovery_token,
            patch(f"{RUNNER}.LiveFeed") as feed,
            patch(f"{RUNNER}.read_spec", return_value=b"{}"),
        ):
            feed.return_value.universe.return_value = []
            feed.return_value.fx.return_value = (0, utc_iso(), None, None)
            with (
                patch.object(SharedFeedSlots, "consume", new=parked_consumer),
                patch.object(SharedFeedSlots, "begin_hold", new=publish_during_drain),
            ):
                await run("old")
            ready = load_ready(update)
            original = read_status(journal)
            self.assertEqual(read_json(root / "old" / "status.json")["state"], "handoff_ready")
            self.assertEqual(original["state"], "active")
            self.assertEqual(original["hash"], ready["journal"]["chain_sha256"])
            with SlotControl.open(spool) as control:
                self.assertEqual([command.slot_id for command in control.pending()], ["during-hold"])
                self.assertFalse(control.report()["closed"])

            with self.assertRaises(JournalError):
                await run("mismatched", resume=True, handoff=update, capital=101)
            self.assertEqual(read_json(root / "mismatched" / "status.json")["state"], "failed")
            with self.assertRaisesRegex(SystemExit, "Live validation failed"):
                await run("bypass", resume=True)
            self.assertEqual(read_status(journal), original)

            inputs = list(held_inputs(update, ready))
            self.assertEqual([event["kind"] for event in inputs], ["selection", "clock"])
            self.assertEqual(inputs[0]["payload"], ["HELD"])

            async def fail_after_first(slots: SharedFeedSlots, entry: dict[str, Any]) -> None:
                if entry["sequence"] == inputs[1]["sequence"]:
                    raise OSError("Synthetic failure between durable held cuts")
                await apply_held(slots, entry)

            with (
                patch.object(SharedFeedSlots, "apply_held", new=fail_after_first),
                self.assertRaisesRegex(SystemExit, "Live validation failed"),
            ):
                await run("partial", resume=True, handoff=update)
            recovery_token.assert_not_called()
            self.assertEqual(read_status(journal)["records"], original["records"] + 2)

            def check_before_provider() -> str:
                with AdaptiveJournal.read_active(journal) as current:
                    self.assertEqual(current.identity, original["identity"])
                    records = list(current.events())
                self.assertEqual(
                    [record["kind"] for record in records if record["kind"].startswith("session_")],
                    [HOLD_EVENT, CUT_EVENT, SOURCE_EVENT],
                )
                self.assertEqual(
                    [
                        event["sequence"]
                        for event in records
                        if "sequence" in event and event["kind"] != CUT_EVENT
                    ],
                    list(range(1, ready["sequence"] + len(inputs) + 1)),
                )
                self.assertNotIn("recovery_gap", [event["kind"] for event in records])
                for actual, expected in zip(records[-len(inputs):], inputs, strict=True):
                    self.assertEqual({key: actual[key] for key in expected}, expected)
                raise OSError("Stop fixture before provider access")

            recovery_token.side_effect = check_before_provider
            with self.assertRaisesRegex(SystemExit, "Live validation failed"):
                await run("candidate", resume=True, handoff=update)
            self.assertEqual(read_status(journal)["records"], original["records"] + 3)
            with self.assertRaisesRegex(SystemExit, "Live validation failed"):
                await run("candidate-retry", resume=True, handoff=update)
            self.assertEqual(read_status(journal)["records"], original["records"] + 3)
            self.assertEqual(recovery_token.call_count, 2)

    async def test_drain_fails_if_consumer_exits_with_uncommitted_items(self) -> None:
        queue: asyncio.Queue[int] = asyncio.Queue()
        queue.put_nowait(1)

        async def fail() -> None:
            raise JournalError("Synthetic consumer failure")

        consumer = asyncio.create_task(fail())
        with self.assertRaisesRegex(JournalError, "Synthetic consumer failure"):
            await drain_queue(queue, consumer, deadline=time.monotonic() + 2)
        self.assertEqual(queue.qsize(), 1)
