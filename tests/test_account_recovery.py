"""Verify that one shared journal commit controls all process-backed accounts."""

import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tests.test_shared_feed_slots import BASELINE, START, add, make_runtime
from tools.adaptive_journal import AdaptiveJournal, JournalError
from tools.layered_paper import feature_from_bars
from tools.run_async_live_validation import subscription_set
from tools.runtime_audit import RuntimeAudit, RuntimeAuditError
from tools.shared_feed_slots import SharedFeedSlots
from tools.slot_control import SlotCommand, SlotControl


class AccountRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.control = self.enterContext(
            SlotControl.open(self.root / "slots.sqlite3", initialize=True),
        )
        self.audit = RuntimeAudit(self.root / "audit.sqlite3", "synthetic")
        self.journal = self.enterContext(
            AdaptiveJournal.open(self.root / "journal.sqlite3", {"test": "accounts"}, resume=False),
        )

    def slots(self, journal: AdaptiveJournal, *, concurrent: bool = True) -> SharedFeedSlots:
        slots = SharedFeedSlots(
            make_runtime,
            subscription_set,
            lambda *_a, **_k: None,
            journal,
            self.control,
            concurrent_accounts=concurrent,
            audit=self.audit if concurrent else None,
        )
        self.addAsyncCleanup(slots.close)
        return slots

    async def feed(self, slots: SharedFeedSlots, kind: str, payload: Any, at: int = 0) -> None:
        stamp = (START + timedelta(seconds=at)).isoformat()
        slots.sequence += 1
        await slots._apply(
            {
                "sequence": slots.sequence,
                "kind": kind,
                "payload": payload,
                "received": stamp,
                "at": stamp,
            },
            0,
        )

    async def bars(
        self,
        slots: SharedFeedSlots,
        symbol: str,
        at: int = 0,
        *,
        step: float = 0.05,
    ) -> None:
        await self.feed(
            slots,
            "feature",
            (
                symbol,
                [
                    {
                        "timestamp": (
                            START + timedelta(seconds=at, minutes=index - 15)
                        ).isoformat(),
                        "closePrice": 9 + step * index,
                        "highPrice": 9 + step * index,
                    }
                    for index in range(16)
                ],
            ),
            at,
        )

    async def seed(self, slots: SharedFeedSlots, *names: str) -> None:
        day = START.date().isoformat()
        await self.feed(
            slots,
            "calendar",
            (
                day,
                {
                    "today": {
                        "date": day,
                        "regularMarket": {
                            "startTime": (START - timedelta(minutes=30)).isoformat(),
                            "endTime": (START + timedelta(hours=1)).isoformat(),
                        },
                    }
                },
            ),
        )
        await self.feed(slots, "selection", ["A", "B"])
        for name in names:
            await self.command(slots, add(name, 100))
        with patch(
            "tools.run_async_live_validation.feature_from_bars",
            wraps=feature_from_bars,
        ) as feature:
            await self.bars(slots, "A")
            self.assertEqual(feature.call_count, 1 if slots.concurrent_accounts else len(names))
        await self.bars(slots, "B")

    async def command(self, slots: SharedFeedSlots, command: SlotCommand, at: int = 0) -> None:
        self.control.submit(command, end_epoch=time.time() + 3600)
        await self.feed(slots, "slot_command", asdict(command), at)

    async def book(
        self, slots: SharedFeedSlots, at: int, symbol: str = "A", price: float = 10
    ) -> None:
        await self.feed(
            slots,
            "book",
            (
                symbol,
                {
                    "timestamp": (START + timedelta(seconds=at)).isoformat(),
                    "currency": "USD",
                    "asks": [{"price": str(price), "volume": "100"}],
                    "bids": [{"price": str(price - 0.01), "volume": "100"}],
                },
            ),
            at,
        )

    async def test_failed_journal_commit_discards_every_staged_account_then_replays(self) -> None:
        first = self.slots(self.journal)
        await self.seed(first, "first", "second")
        await self.book(first, 1)
        before = self.journal.evidence()
        checksums = {name: slot.runtime._checkpoint() for name, slot in first.slots.items()}
        with (
            patch.object(
                self.journal, "append", side_effect=JournalError("synthetic commit failure")
            ),
            self.assertRaises(JournalError),
        ):
            await self.book(first, 3)
        self.assertEqual(self.journal.evidence(), before)
        self.assertEqual(first.consumed, before["records"])
        for slot in first.slots.values():
            for account in slot.runtime.accounts.values():
                self.assertFalse(account.book.trades)
                self.assertTrue(account.pending)
        self.assertEqual(
            {name: slot.runtime._checkpoint() for name, slot in first.slots.items()},
            checksums,
        )
        await first.close()

        resumed = self.slots(self.journal)
        await resumed.restore()
        await self.book(resumed, 3)
        for slot in resumed.slots.values():
            for account in slot.runtime.accounts.values():
                self.assertEqual(len(account.book.trades), 1)
                self.assertEqual(len(account.outcomes), 1)
                self.assertGreater(account.book.fee_totals["total_usd"], 0)
        self.assertEqual(self.journal.evidence()["records"], before["records"] + 1)

    async def test_audit_storage_failure_blocks_prepared_economic_commit(self) -> None:
        slots = self.slots(self.journal)
        await self.seed(slots, "source")
        await self.book(slots, 1)
        before = self.journal.evidence()
        runtime = slots.slots["source"].runtime
        checkpoint = runtime._checkpoint()
        with closing(sqlite3.connect(self.audit.path)) as db, db:
            db.execute("""
                CREATE TRIGGER reject_journal_heartbeat
                BEFORE INSERT ON runtime_workers
                WHEN NEW.component = 'journal' AND NEW.status = 'active'
                BEGIN SELECT RAISE(ABORT, 'fixture audit write failure'); END
            """)
        with self.assertRaises(RuntimeAuditError):
            await self.book(slots, 3)
        self.assertFalse(self.audit.healthy)
        self.assertEqual(self.journal.evidence(), before)
        self.assertEqual(runtime._checkpoint(), checkpoint)
        self.assertTrue(runtime.account.pending)
        self.assertFalse(runtime.account.book.trades)
        self.assertTrue(self.audit.path.with_suffix(".sqlite3.emergency.jsonl").exists())

    async def test_worker_killed_after_durable_commit_before_ack_then_exact_fork(self) -> None:
        initial = self.slots(self.journal)
        await self.seed(initial, "source")
        await self.book(initial, 1)
        runtime = initial.slots["source"].runtime
        executor = runtime.account_executor
        assert executor is not None
        process = executor._workers["candidate"].process
        prior_pid = process.pid
        append = self.journal.append

        def commit_then_kill(entry: dict[str, Any]) -> None:
            append(entry)
            process.kill()
            process.wait(timeout=10)
            self.assertIsNotNone(process.poll())

        with patch.object(self.journal, "append", side_effect=commit_then_kill):
            await self.book(initial, 3)
        entry = list(self.journal.events())[-1]
        output = entry["outputs"]["source"]["account_results"]["candidate"]
        self.assertEqual(output["accepted_mark"]["received_at"], entry["received"])
        self.assertEqual(output["accepted_mark"]["source_at"], entry["payload"][1]["timestamp"])
        self.assertEqual(len(output["outcomes"]), 1)

        await self.bars(initial, "A", 4, step=-0.05)
        await self.book(initial, 5, price=8)
        await self.book(initial, 7, price=8)
        self.assertNotEqual(executor.pids["candidate"], prior_pid)
        await self.book(initial, 8, "B", 2)
        source = runtime.accounts["candidate"]
        self.assertLess(source.book.closed_net, 0)
        self.assertGreater(source.reserved_cash, 0)

        cut = initial.consumed
        await self.command(
            initial,
            SlotCommand(
                "fork",
                "branch",
                "branch",
                {
                    "source_slot": "source",
                    "source_account": "candidate",
                    "candidate": {**BASELINE, "policy": "rebound"},
                },
            ),
            9,
        )
        branch = initial.slots["branch"]
        assert branch.lineage is not None
        self.assertEqual(branch.lineage["through_sequence"], cut)
        for account in branch.runtime.accounts.values():
            self.assertEqual(account.summary(), source.summary())
            self.assertEqual(account.pending, source.pending)
        await self.book(initial, 10, "B", 2)
        for account in branch.runtime.accounts.values():
            self.assertEqual(account.owner_policy_versions, {runtime.ids["B"]: 0})
        await initial.close()
        self.assertFalse(any(issue.code == "worker_dead" for issue in self.audit.scan_health()))

        recovered = self.slots(self.journal)
        await recovered.restore()
        for name, slot in initial.slots.items():
            actual = recovered.slots[name].runtime
            self.assertEqual(actual._checkpoint(), slot.runtime._checkpoint())
            for account_name, expected in slot.runtime.accounts.items():
                self.assertEqual(actual.accounts[account_name].summary(), expected.summary())
