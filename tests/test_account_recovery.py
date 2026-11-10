"""Verify that one shared journal commit controls all process-backed accounts."""

import copy
import sqlite3
import tempfile
import time
import unittest
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Any, Literal
from unittest.mock import patch

from tests.fixtures.warning_account import WarningAccount
from tests.test_async_layered_paper import event
from tests.test_shared_feed_slots import BASELINE, START, add, make_runtime
from tools.account_executor import (
    AccountAction,
    AccountExecutor,
    AccountProtocolError,
    AccountWorkerError,
)
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

    async def test_child_audit_failure_blocks_economic_commit(self) -> None:
        account = WarningAccount(100, policy="trend")
        self.addAsyncCleanup(account.close)
        executor = AccountExecutor({"candidate": account}, audit=self.audit, timeout=5)
        self.addCleanup(executor.close)
        await executor.apply(1, {"candidate": AccountAction("book", event())})
        executor.commit()
        prior = account.summary(), account.pending.copy()
        with closing(sqlite3.connect(self.audit.path)) as db, db:
            db.execute("""
                CREATE TRIGGER reject_all_incidents BEFORE INSERT ON runtime_incidents
                BEGIN SELECT RAISE(ABORT, 'fixture incident storage failure'); END
            """)

        actions = {"candidate": AccountAction("book", event(at=2))}
        with self.assertRaisesRegex(AccountWorkerError, "audit_storage_failed.*RuntimeAuditError"):
            await executor.apply(2, actions)
        self.assertEqual(executor._committed, {"candidate": (1, 1)})
        self.assertEqual(executor.pids, {})
        self.assertEqual((account.summary(), account.pending), prior)
        self.assertFalse(self.audit.healthy)
        with self.assertRaisesRegex(AccountWorkerError, "fresh journal replay"):
            await executor.apply(2, actions)

    async def test_child_audit_failure_cannot_enter_durable_journal(self) -> None:
        slots = self.slots(self.journal)
        await self.seed(slots, "source")
        runtime = slots.slots["source"].runtime
        runtime.accounts["candidate"].__class__ = WarningAccount
        await self.book(slots, 1)
        before = self.journal.evidence()
        checkpoint = runtime._checkpoint()
        executor = runtime.account_executor
        assert executor is not None
        committed = executor._committed.copy()
        self.assertEqual(len(committed), 2)
        with closing(sqlite3.connect(self.audit.path)) as db, db:
            db.execute("""
                CREATE TRIGGER reject_warning BEFORE INSERT ON runtime_incidents
                WHEN NEW.code = 'python_warning'
                BEGIN SELECT RAISE(ABORT, 'fixture child audit failure'); END
            """)

        with self.assertRaisesRegex(AccountWorkerError, "audit_storage_failed.*candidate"):
            await self.book(slots, 3)
        self.assertTrue(self.audit.healthy)
        self.assertEqual(self.journal.evidence(), before)
        self.assertEqual(slots.consumed, before["records"])
        self.assertEqual(executor._committed, committed)
        self.assertEqual(executor.pids, {})
        self.assertEqual(runtime._checkpoint(), checkpoint)
        for account in runtime.accounts.values():
            self.assertTrue(account.pending)
            self.assertFalse(account.book.trades)
        emergency = self.audit.path.with_suffix(".sqlite3.emergency.jsonl")
        evidence = emergency.read_bytes()
        self.assertIn(b'"code": "audit_storage_failed"', evidence)
        with self.assertRaisesRegex(JournalError, "fresh journal replay"):
            await self.book(slots, 3)
        await slots.close()

        with closing(sqlite3.connect(self.audit.path)) as db, db:
            db.execute("DROP TRIGGER reject_warning")
        resumed = self.slots(self.journal)
        await resumed.restore()
        self.assertEqual(resumed.consumed, before["records"])
        self.assertEqual(resumed.slots["source"].runtime._checkpoint(), checkpoint)
        self.assertEqual(self.journal.evidence(), before)
        await self.book(resumed, 3)
        for account in resumed.slots["source"].runtime.accounts.values():
            self.assertEqual(len(account.book.trades), 1)
        self.assertEqual(self.journal.evidence()["records"], before["records"] + 1)
        self.assertEqual(emergency.read_bytes(), evidence)

    async def test_healthy_child_warning_is_audited_and_commits(self) -> None:
        account = WarningAccount(100, policy="trend")
        self.addAsyncCleanup(account.close)
        executor = AccountExecutor({"candidate": account}, audit=self.audit, timeout=5)
        self.addCleanup(executor.close)
        for sequence in (1, 2):
            await executor.apply(
                sequence, {"candidate": AccountAction("book", event(at=2 * (sequence - 1)))}
            )
            executor.assert_committable()
            executor.commit()
        with closing(sqlite3.connect(self.audit.path)) as db:
            rows = db.execute("""
                SELECT severity, occurrences FROM runtime_incidents WHERE code = 'python_warning'
            """).fetchall()
        self.assertEqual(rows, [("warning", 1)])
        self.assertEqual(len(account.book.trades), 1)

    def reject_thread_diagnostic(self) -> None:
        with closing(sqlite3.connect(self.audit.path)) as db, db:
            db.execute("""
                CREATE TRIGGER reject_thread_diagnostic BEFORE INSERT ON runtime_incidents
                WHEN NEW.code = 'thread_diagnostic'
                BEGIN SELECT RAISE(ABORT, 'fixture thread audit failure'); END
            """)

    async def test_audit_failure_before_publication_guard_blocks_append(self) -> None:
        slots = self.slots(self.journal)
        await self.seed(slots, "source")
        await self.book(slots, 1)
        runtime = slots.slots["source"].runtime
        before = self.journal.evidence()
        checkpoint = runtime._checkpoint()
        self.reject_thread_diagnostic()
        at_guard, fault_done = Event(), Event()
        guard = self.audit.publication_guard

        def fail_audit() -> None:
            if at_guard.wait(3):
                self.audit.report_exception("probe", "thread_diagnostic", RuntimeError("fixture"))
                fault_done.set()

        @contextmanager
        def fail_before_guard() -> Iterator[None]:
            at_guard.set()
            self.assertTrue(fault_done.wait(3), "audit fault did not complete before guard")
            self.assertFalse(self.audit.healthy)
            with guard():
                yield

        thread = Thread(target=fail_audit, daemon=True)
        thread.start()
        try:
            with (
                patch.object(self.audit, "publication_guard", side_effect=fail_before_guard),
                patch.object(self.journal, "append", wraps=self.journal.append) as append,
                self.assertRaisesRegex(RuntimeAuditError, "storage is unhealthy"),
            ):
                await self.book(slots, 3)
            append.assert_not_called()
        finally:
            at_guard.set()
            thread.join(4)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.journal.evidence(), before)
        self.assertEqual(slots.consumed, before["records"])
        self.assertEqual(runtime._checkpoint(), checkpoint)

    async def test_audit_failure_after_durable_append_blocks_ack_and_replays_once(self) -> None:
        slots = self.slots(self.journal)
        await self.seed(slots, "source")
        await self.book(slots, 1)
        runtime = slots.slots["source"].runtime
        executor = runtime.account_executor
        assert executor is not None
        committed = executor._committed.copy()
        before = self.journal.evidence()
        self.reject_thread_diagnostic()
        at_append, lock_checked, durable, fault_done = Event(), Event(), Event(), Event()
        acquired_early = []
        failed_before_durable = []
        append = self.journal.append
        record_process_evidence = slots._record_process_evidence

        def fail_audit() -> None:
            if at_append.wait(3):
                acquired = self.audit._publication_lock.acquire(blocking=False)
                acquired_early.append(acquired)
                if acquired:
                    self.audit._publication_lock.release()
                lock_checked.set()
                self.audit.report_exception("probe", "thread_diagnostic", RuntimeError("fixture"))
                failed_before_durable.append(not durable.is_set())
                fault_done.set()

        def append_under_contention(entry: dict[str, Any]) -> None:
            at_append.set()
            self.assertTrue(lock_checked.wait(3), "competing audit thread did not reach the lock")
            self.assertEqual(acquired_early, [False])
            append(entry)
            durable.set()

        def wait_for_fault_before_ack() -> None:
            self.assertTrue(durable.is_set())
            self.assertTrue(fault_done.wait(3), "audit fault did not complete after append")
            self.assertFalse(self.audit.healthy)
            record_process_evidence()

        thread = Thread(target=fail_audit, daemon=True)
        thread.start()
        try:
            with (
                patch.object(self.journal, "append", side_effect=append_under_contention),
                patch.object(slots, "_record_process_evidence", side_effect=wait_for_fault_before_ack),
                patch.object(runtime, "acknowledge_accounts", wraps=runtime.acknowledge_accounts)
                as ack,
                self.assertRaisesRegex(RuntimeAuditError, "storage is unhealthy") as raised,
            ):
                await self.book(slots, 3)
            ack.assert_not_called()
        finally:
            at_append.set()
            thread.join(4)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failed_before_durable, [False])
        after = self.journal.evidence()
        self.assertEqual(after["records"], before["records"] + 1)
        self.assertEqual(slots.consumed, after["records"])
        self.assertEqual(executor._committed, committed)
        self.assertIn("is durable and requires replay", " ".join(raised.exception.__notes__))
        checkpoint = runtime._checkpoint()
        with self.assertRaisesRegex(JournalError, "fresh journal replay"):
            await self.book(slots, 4)
        await slots.close()

        with closing(sqlite3.connect(self.audit.path)) as db, db:
            db.execute("DROP TRIGGER reject_thread_diagnostic")
        self.audit = RuntimeAudit(self.audit.path, self.audit.run_id)
        resumed = self.slots(self.journal)
        await resumed.restore()
        self.assertEqual(resumed.slots["source"].runtime._checkpoint(), checkpoint)
        self.assertEqual(self.journal.evidence(), after)
        await self.book(resumed, 4)
        for account in resumed.slots["source"].runtime.accounts.values():
            self.assertEqual(len(account.book.trades), 1)

    async def test_corrupt_worker_mark_cannot_poison_replay(self) -> None:
        slots = self.slots(self.journal)
        await self.seed(slots, "source")
        await self.book(slots, 1)
        runtime = slots.slots["source"].runtime
        executor = runtime.account_executor
        assert executor is not None
        committed = executor._committed.copy()
        before = self.journal.evidence()
        checkpoint = runtime._checkpoint()
        exchange = executor._exchange

        def corrupt(*args: Any) -> dict[str, Any]:
            replies = exchange(*args)
            result = replies["candidate"]
            replies["candidate"] = replace(
                result, accepted_mark={**result.accepted_mark, "equity": 999999999}
            )
            return replies

        with (
            patch.object(executor, "_exchange", side_effect=corrupt),
            self.assertRaisesRegex(AccountProtocolError, "Account mark mismatch: candidate"),
        ):
            await self.book(slots, 3)
        self.assertEqual(self.journal.evidence(), before)
        self.assertEqual(slots.consumed, before["records"])
        self.assertEqual(executor._committed, committed)
        self.assertEqual(executor.pids, {})
        self.assertEqual(runtime._checkpoint(), checkpoint)
        with closing(sqlite3.connect(self.audit.path)) as db:
            rows = db.execute("""
                SELECT state FROM runtime_incidents WHERE code = 'result_rejected'
            """).fetchall()
        self.assertEqual(rows, [("open",), ("open",)])
        await slots.close()

        resumed = self.slots(self.journal)
        await resumed.restore()
        self.assertEqual(resumed.slots["source"].runtime._checkpoint(), checkpoint)
        self.assertEqual(self.journal.evidence(), before)
        await self.book(resumed, 3)
        self.assertEqual(self.journal.evidence()["records"], before["records"] + 1)
        for account in resumed.slots["source"].runtime.accounts.values():
            self.assertEqual(len(account.book.trades), 1)

    async def assert_invalid_patch_leaves_replayable_cut(
        self, variant: Literal["clock_fee", "clock_quote", "book_symbol", "book_history"]
    ) -> None:
        clock = variant in ("clock_fee", "clock_quote")
        slots = self.slots(self.journal)
        await self.seed(slots, "source")
        if clock:
            await self.book(slots, 1)
        else:
            await self.feed(slots, "clock", None)
        runtime = slots.slots["source"].runtime
        executor = runtime.account_executor
        assert executor is not None
        committed = executor._committed.copy()
        before, checkpoint = self.journal.evidence(), runtime._checkpoint()
        exchange = executor._exchange

        def corrupt(*args: Any) -> dict[str, Any]:
            replies = exchange(*args)
            result = replies["candidate"]
            if variant == "clock_fee":
                self.assertIsNone(result.accepted_mark)
                fees = {**result.patch.book["fee_totals"], "total_usd": float("nan")}
                book = {**result.patch.book, "fee_totals": fees}
                replies["candidate"] = replace(result, patch=replace(result.patch, book=book))
            elif variant == "clock_quote":
                self.assertIsNone(result.accepted_mark)
                pending = result.patch.account["pending"].copy()
                intent = pending[0]
                initial = copy.deepcopy(intent.initial)
                initial["asks"][0]["price"] = float("nan")
                pending[0] = replace(intent, initial=initial)
                account = {**result.patch.account, "pending": pending}
                replies["candidate"] = replace(result, patch=replace(result.patch, account=account))
            elif variant == "book_symbol":
                self.assertIsNotNone(result.accepted_mark)
                self.assertEqual(result.protected_symbols, frozenset({0}))
                replies["candidate"] = replace(result, protected_symbols=frozenset({False}))
            else:
                self.assertIsNotNone(result.accepted_mark)
                (intent,) = result.patch.account_history["intents"]
                initial = copy.deepcopy(intent.initial)
                initial["bids"][0]["volume"] = "inf"
                history = {
                    **result.patch.account_history,
                    "intents": [replace(intent, initial=initial)],
                }
                replies["candidate"] = replace(
                    result, patch=replace(result.patch, account_history=history)
                )
            return replies

        with (
            patch.object(executor, "_exchange", side_effect=corrupt),
            patch("tools.account_executor._Patch.apply", autospec=True) as apply_patch,
            self.assertRaises(AccountProtocolError),
        ):
            if clock:
                await self.feed(slots, "clock", None, 3)
            else:
                await self.book(slots, 1)
        apply_patch.assert_not_called()
        self.assertEqual(self.journal.evidence(), before)
        self.assertEqual(slots.consumed, before["records"])
        self.assertEqual(runtime._checkpoint(), checkpoint)
        self.assertEqual(executor._committed, committed)
        self.assertEqual(executor.pids, {})
        with closing(sqlite3.connect(self.audit.path)) as db:
            incidents = db.execute("""
                SELECT state FROM runtime_incidents WHERE code = 'result_rejected'
            """).fetchall()
        self.assertEqual(incidents, [("open",), ("open",)])
        await slots.close()

        resumed = self.slots(self.journal)
        await resumed.restore()
        self.assertEqual(resumed.slots["source"].runtime._checkpoint(), checkpoint)
        self.assertEqual(self.journal.evidence(), before)
        if clock:
            await self.feed(resumed, "clock", None, 3)
        else:
            await self.book(resumed, 1)
        await self.book(resumed, 3)
        self.assertEqual(self.journal.evidence()["records"], before["records"] + 2)
        for account in resumed.slots["source"].runtime.accounts.values():
            self.assertEqual(len(account.book.trades), 1)

    async def test_markless_clock_with_nonfinite_fee_cannot_poison_journal(self) -> None:
        await self.assert_invalid_patch_leaves_replayable_cut("clock_fee")

    async def test_markless_clock_with_nonfinite_pending_quote_cannot_poison_journal(self) -> None:
        await self.assert_invalid_patch_leaves_replayable_cut("clock_quote")

    async def test_book_with_boolean_protection_cannot_poison_journal(self) -> None:
        await self.assert_invalid_patch_leaves_replayable_cut("book_symbol")

    async def test_nonfinite_new_intent_history_quote_cannot_poison_journal(self) -> None:
        await self.assert_invalid_patch_leaves_replayable_cut("book_history")

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
