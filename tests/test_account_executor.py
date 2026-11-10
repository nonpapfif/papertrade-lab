"""Exercise the process boundary with the existing fee-aware execution engine."""

import asyncio
import os
import signal
import subprocess
import unittest
from dataclasses import replace
from functools import partial
from multiprocessing.connection import Connection
from typing import Any
from unittest.mock import patch

from tests.test_async_layered_paper import event, stamp
from tools import account_executor as workers
from tools.account_executor import (
    AccountAction,
    AccountExecutor,
    AccountProtocolError,
    AccountWorkerError,
)
from tools.async_layered_paper import AsyncLayeredPaper
from tools.paper_market import KR, US


class AccountExecutorTests(unittest.IsolatedAsyncioTestCase):
    def executor(self, **accounts: AsyncLayeredPaper) -> AccountExecutor:
        runner = AccountExecutor(accounts, timeout=10)
        self.addCleanup(runner.close)
        for account in accounts.values():
            self.addAsyncCleanup(account.close)
        return runner

    async def test_serial_equivalence_and_independent_exact_fork(self) -> None:
        mirror = AsyncLayeredPaper(100, policy="trend", fees={"commission_exemption_usd": 0})
        reference = mirror.fork()
        self.addAsyncCleanup(reference.close)
        # A restart may start with account queues and tasks from a serial replay.
        await mirror.submit(event())
        await reference.submit(event())
        runner = self.executor(first=mirror)
        initial_pids: dict[str, int | None] = {}
        for sequence, row in enumerate(
            (
                event(at=2),
                event(at=4, price=8, feature=replace(event()["feature"], r15=-0.02)),
                event(at=6, price=8),
                event(symbol=1, at=8),
            ),
            1,
        ):
            result = await runner.apply(
                sequence, {"first": AccountAction("book", row, stamp(sequence))}
            )
            runner.commit()
            self.assertTrue(result["first"].accepted_book)
            self.assertEqual(
                result["first"].accepted_mark,
                {
                    "symbol": row["symbol"],
                    "event_id": row["id"],
                    "source_at": row["book"]["timestamp"],
                    "received_at": stamp(sequence),
                    "decision_at": row["received_at"],
                    "currency": "USD",
                    "midpoint": mirror.prices[row["symbol"]],
                    "equity": mirror.book.equity,
                    "realized": mirror.book.realized,
                    "fees": mirror.book.fee_totals,
                },
            )
            await reference.submit(row)
            self.assertEqual(mirror.summary(), reference.summary())
            self.assertEqual(mirror.__getstate__()["pending"], reference.pending)
            self.assertEqual(mirror.owner_policy_versions, reference.owner_policy_versions)
            if not initial_pids:
                initial_pids = runner.pids
            self.assertEqual(runner.pids, initial_pids)

        self.assertLess(mirror.book.realized, 0)
        self.assertGreater(mirror.reserved_cash, 0)
        child = mirror.fork()
        self.assertEqual(child.summary(), mirror.summary())
        candidate = self.executor(child=child)
        child.entry_policy_version += 1
        await candidate.apply(5, {"child": AccountAction("book", event(symbol=1, at=10))})
        candidate.commit()
        self.assertEqual(child.owner_policy_versions, {1: 0})
        self.assertFalse(mirror.book.positions)
        self.assertTrue(mirror.pending)
        self.assertEqual(child.book.closed_net, mirror.book.closed_net)

    async def test_worker_starts_without_parent_environment(self) -> None:
        runner = self.executor(a=AsyncLayeredPaper(100))
        with (
            patch.dict(os.environ, {"TOSS_CLIENT_SECRET": "synthetic-fixture"}),
            patch.object(workers.subprocess, "Popen", wraps=subprocess.Popen) as spawn,
        ):
            result = await runner.apply(1, {"a": AccountAction("book", event())})
            runner.commit()
        self.assertNotEqual(result["a"].pid, os.getpid())
        self.assertEqual(spawn.call_args.kwargs["env"], {})

    async def test_actual_cpu_intervals_overlap_in_distinct_account_processes(self) -> None:
        runner = self.executor(a=AsyncLayeredPaper(100), b=AsyncLayeredPaper(200))
        row = event()
        for side in ("asks", "bids"):
            row["book"][side] = [dict(row["book"][side][0]) for _ in range(35_000)]
        results = await runner.apply(1, {name: AccountAction("book", row) for name in ("a", "b")})
        runner.commit()
        a, b = results["a"], results["b"]
        self.assertNotEqual(a.pid, b.pid)
        self.assertNotIn(os.getpid(), {a.pid, b.pid})
        self.assertGreater(min(a.cpu_ns, b.cpu_ns), 0)
        self.assertLess(max(a.started_ns, b.started_ns), min(a.finished_ns, b.finished_ns))
        self.assertNotEqual(runner.accounts["a"].reserved_cash, runner.accounts["b"].reserved_cash)

    async def test_cut_order_and_malformed_result_cannot_stage_mirrors(self) -> None:
        account = AsyncLayeredPaper(100)
        runner = self.executor(a=account)
        action = {"a": AccountAction("book", event())}
        original = runner._exchange

        def malformed(*args: Any) -> dict[str, Any]:
            results = original(*args)
            results["a"] = replace(results["a"], command_sequence=4)
            return results

        with (
            patch.object(runner, "_exchange", side_effect=malformed),
            self.assertRaises(AccountProtocolError),
        ):
            await runner.apply(1, action)
        self.assertFalse(account.intents)
        self.assertFalse(runner.pids)
        await runner.apply(2, action)
        with self.assertRaises(AccountProtocolError):
            await runner.apply(3, action)
        runner.commit()
        for cut in (1, 2):
            with self.assertRaises(AccountProtocolError):
                await runner.apply(cut, action)
        self.assertEqual(len(account.intents), 1)

    async def test_missing_worker_command_and_closed_parent_pipe_exit(self) -> None:
        runner = self.executor(a=AsyncLayeredPaper(100))
        action = AccountAction("advance", stamp(0))
        commands = {"a": workers._Command("a", 1, 2, action, ("router", "all", "equal", 0))}
        boot = runner._start(commands)
        boot["a"].send(workers._Bootstrap(runner.accounts["a"], "a", (0, 0), None, 10))
        self.assertEqual(workers._receive(boot, 10), runner.pids)
        worker = runner._workers["a"]
        worker.pipe.send(commands["a"])
        fault = workers._receive({"a": worker.pipe}, 10)["a"]
        self.assertEqual(fault.code, "command_rejected")
        worker.process.wait(timeout=10)
        self.assertIsNotNone(worker.process.poll())
        runner.close()

        boot = runner._start(commands)
        boot["a"].send(workers._Bootstrap(runner.accounts["a"], "a", (0, 0), None, 10))
        workers._receive(boot, 10)
        worker = runner._workers["a"]
        worker.pipe.close()
        worker.process.wait(timeout=10)
        self.assertIsNotNone(worker.process.poll())

    async def test_process_death_after_partial_result_discards_uncommitted_batch(self) -> None:
        runner = self.executor(a=AsyncLayeredPaper(100), b=AsyncLayeredPaper(100))
        await runner.apply(1, {name: AccountAction("book", event()) for name in ("a", "b")})
        runner.commit()
        previous = runner.pids
        second = runner._workers["b"]
        assert second.process.pid is not None
        os.kill(second.process.pid, signal.SIGSTOP)
        original = workers._receive

        def partial(
            pipes: dict[str, Any],
            timeout: float,
            interrupted: Connection | None = None,
        ) -> dict[str, Any]:
            if set(pipes) != {"a", "b"}:
                return original(pipes, timeout, interrupted)
            result = original({"a": pipes["a"]}, timeout, interrupted)
            self.assertEqual(len(result["a"].outcomes), 1)
            second.process.kill()
            return {**result, **original({"b": pipes["b"]}, timeout, interrupted)}

        confirm = {name: AccountAction("book", event(at=2)) for name in ("a", "b")}
        with (
            patch.object(workers, "_receive", side_effect=partial),
            self.assertRaises(AccountWorkerError),
        ):
            await runner.apply(2, confirm)
        for account in runner.accounts.values():
            self.assertTrue(account.pending)
            self.assertFalse(account.book.trades)
            self.assertFalse(account.outcomes)
        await runner.apply(2, confirm)
        runner.commit()
        self.assertNotEqual(runner.pids, previous)
        for account in runner.accounts.values():
            self.assertEqual(len(account.book.trades), 1)
            self.assertEqual(len(account.outcomes), 1)

    async def test_cancelling_inflight_apply_drains_ipc_and_keeps_committed_state(self) -> None:
        account = AsyncLayeredPaper(100)
        runner = self.executor(a=account)
        await runner.apply(1, {"a": AccountAction("book", event())})
        runner.commit()
        process = runner._workers["a"].process
        os.kill(process.pid, signal.SIGSTOP)
        loop = asyncio.get_running_loop()
        reading = asyncio.Event()
        receive = workers._receive

        def notified(
            pipes: dict[str, Any],
            timeout: float,
            interrupted: Connection | None = None,
        ) -> dict[str, Any]:
            if pipes:
                loop.call_soon_threadsafe(reading.set)
            return receive(pipes, timeout, interrupted)

        action = {"a": AccountAction("book", event(at=2))}
        with patch.object(workers, "_receive", side_effect=notified):
            task = asyncio.create_task(runner.apply(2, action))
            await asyncio.wait_for(reading.wait(), 10)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)

        self.assertIsNotNone(process.poll())
        self.assertFalse(runner.pids)
        self.assertTrue(account.pending)
        self.assertFalse(account.book.trades)
        await runner.apply(2, action)
        runner.commit()
        self.assertEqual(len(account.book.trades), 1)

    async def test_rejected_book_does_not_claim_mark_acceptance(self) -> None:
        runner = self.executor(a=AsyncLayeredPaper(30_000, market=KR))
        book = event(price=3000)
        book["book"]["currency"] = KR.currency
        book["book"]["bids"][0]["price"] = "2995"
        first = await runner.apply(1, {"a": AccountAction("book", book)})
        runner.commit()
        self.assertTrue(first["a"].accepted_book)
        self.assertEqual(runner.accounts["a"].context.market, KR)
        duplicate = await runner.apply(2, {"a": AccountAction("book", book)})
        runner.commit()
        self.assertFalse(duplicate["a"].accepted_book)
        same_quote = {**book, "id": "duplicate-source", "received_at": stamp(1)}
        unchanged = await runner.apply(3, {"a": AccountAction("book", same_quote)})
        runner.commit()
        self.assertFalse(unchanged["a"].accepted_book)
        rejected_book = event(at=2, price=3000)
        rejected_book["book"]["currency"] = KR.currency
        rejected_book["book"]["asks"] = []
        rejected = await runner.apply(4, {"a": AccountAction("book", rejected_book)})
        runner.commit()
        self.assertFalse(rejected["a"].accepted_book)
        self.assertEqual(runner.accounts["a"]._books, {})
        self.assertIs(US, AsyncLayeredPaper(100).fork().context.market)

    async def test_malformed_and_inconsistent_worker_marks_are_rejected(self) -> None:
        account = AsyncLayeredPaper(100, policy="trend")
        runner = self.executor(a=account)
        await runner.apply(1, {"a": AccountAction("book", event())})
        runner.commit()
        exchange = runner._exchange
        actions = {"a": AccountAction("book", event(at=2), stamp(3))}
        prior = account.summary()
        replacements = {
            "symbol": 42,
            "event_id": "other",
            "source_at": stamp(3),
            "received_at": stamp(2),
            "decision_at": stamp(3),
            "currency": "KRW",
            "midpoint": 42.0,
            "equity": 999999999,
            "realized": 5.0,
            "fees": {"total_usd": 42.0},
        }

        def corrupt(field: str, value: Any, *args: Any) -> dict[str, Any]:
            replies = exchange(*args)
            result = replies["a"]
            mark = result.accepted_mark.copy()
            if field == "mark":
                mark = value
            elif field == "missing":
                del mark[value]
            elif field == "fee_value":
                mark["fees"] = {**mark["fees"], "total_usd": value}
            elif field == "fee_key":
                mark["fees"] = {**mark["fees"], value: 0.0}
            else:
                mark[field] = value
            replies["a"] = replace(result, accepted_mark=mark)
            return replies

        for field, value in (
            *replacements.items(),
            ("mark", None),
            ("mark", []),
            ("missing", "equity"),
            ("unexpected", 1),
            ("equity", "100.0"),
            ("equity", float("nan")),
            ("realized", -0.0),
            ("symbol", False),
            ("midpoint", float("inf")),
            ("fee_value", float("-inf")),
            ("fee_value", 42.0),
            ("fee_key", "unknown_usd"),
        ):
            with self.subTest(field=field, value=value):
                with (
                    patch.object(runner, "_exchange", side_effect=partial(corrupt, field, value)),
                    self.assertRaisesRegex(AccountProtocolError, "Account mark mismatch"),
                ):
                    await runner.apply(2, actions)
                self.assertEqual(runner._committed, {"a": (1, 1)})
                self.assertEqual(runner.pids, {})
                self.assertEqual(account.summary(), prior)
        result = await runner.apply(2, actions)
        self.assertTrue(result["a"].accepted_book)
        runner.commit()
