"""Process-isolated paper accounts with a journal-controlled commit boundary."""

from __future__ import annotations

import asyncio
import copy
import math
import os
import subprocess
import sys
import time
from collections.abc import Mapping
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass, fields
from multiprocessing.connection import Connection, Pipe, wait
from pathlib import Path
from typing import Any, Literal, cast

from tools.async_layered_paper import AsyncLayeredPaper, Intent
from tools.runtime_audit import RuntimeAudit

MODULE = "account_evaluation"
EXECUTE = "execute"
ACCOUNT_HISTORY = ("intents", "outcomes", "decisions")
BOOK_HISTORY = ("trades", "curve", "order_attempts")
SYMBOL_FIELDS = ("prices", "times", "_source_times", "_receipts", "_books")
ACCOUNT_EXCLUDED = {
    *ACCOUNT_HISTORY,
    *SYMBOL_FIELDS,
    "book",
    "context",
    "_processed",
    "_lock",
    "_turn",
    "_queues",
    "_workers",
    "policy",
    "selection",
    "allocation",
    "entry_policy_version",
}
BOOK_EXCLUDED = {*BOOK_HISTORY, "context", "position_net_usd"}


class AccountWorkerError(RuntimeError):
    """A worker failed before returning a usable account result."""


class AccountProtocolError(AccountWorkerError):
    """An account command or result crossed the wrong observation boundary."""


@dataclass(frozen=True)
class AccountAction:
    kind: Literal["book", "advance", "cancel"]
    payload: dict[str, Any] | str
    published_at: str | None = None
    marker: bool = False


@dataclass(frozen=True)
class _Command:
    account_id: str
    sequence: int
    command_sequence: int
    action: AccountAction
    options: tuple[str, str, str, int]


@dataclass(frozen=True)
class _Patch:
    account: dict[str, Any]
    book: dict[str, Any]
    context: dict[str, Any]
    symbol: int | None
    symbol_values: dict[str, Any]
    event_id: str | None
    processed: bool
    account_history: dict[str, list[Any]]
    book_history: dict[str, list[Any]]

    @classmethod
    def capture(
        cls,
        account: AsyncLayeredPaper,
        action: AccountAction,
        account_offsets: dict[str, int],
        book_offsets: dict[str, int],
    ) -> _Patch:
        payload = action.payload
        symbol = payload["symbol"] if isinstance(payload, dict) else None
        event_id = payload["id"] if isinstance(payload, dict) else None
        return cls(
            {key: value for key, value in vars(account).items() if key not in ACCOUNT_EXCLUDED},
            {key: getattr(account.book, key) for key in _book_fields(account)},
            {key: value for key, value in vars(account.context).items() if key != "market"},
            symbol,
            {
                key: getattr(account, key)[symbol]
                for key in SYMBOL_FIELDS
                if symbol in getattr(account, key)
            },
            event_id,
            event_id in account._processed,
            {key: getattr(account, key)[offset:] for key, offset in account_offsets.items()},
            {key: getattr(account.book, key)[offset:] for key, offset in book_offsets.items()},
        )

    def check(
        self,
        account: AsyncLayeredPaper,
        action: AccountAction,
    ) -> None:
        payload = action.payload
        if (
            set(self.account) != set(vars(account)) - ACCOUNT_EXCLUDED
            or set(self.book) != _book_fields(account)
            or set(self.context) != set(vars(account.context)) - {"market"}
            or set(self.account_history) != set(ACCOUNT_HISTORY)
            or set(self.book_history) != set(BOOK_HISTORY)
            or not all(isinstance(value, list) for value in self.account_history.values())
            or not all(isinstance(value, list) for value in self.book_history.values())
            or set(self.symbol_values) - set(SYMBOL_FIELDS)
            or self.symbol != (payload["symbol"] if isinstance(payload, dict) else None)
            or self.event_id != (payload["id"] if isinstance(payload, dict) else None)
            or type(self.processed) is not bool
            or not math.isfinite(self.book["cash"])
            or not math.isfinite(self.book["realized"])
            or not isinstance(self.account["pending"], dict)
            or not isinstance(self.book["positions"], dict)
            or not all(isinstance(row, Intent) for row in self.account_history["intents"])
            or not all(isinstance(row, dict) for row in self.account_history["outcomes"])
        ):
            raise AccountProtocolError("Malformed account state patch")

    def apply(self, account: AsyncLayeredPaper) -> None:
        vars(account).update(self.account)
        for key, value in self.book.items():
            setattr(account.book, key, value)
        account.book.position_net_usd = account.book.position_net
        vars(account.context).update(self.context)
        if self.symbol is not None:
            for key in SYMBOL_FIELDS:
                mapping = getattr(account, key)
                if key in self.symbol_values:
                    mapping[self.symbol] = self.symbol_values[key]
                else:
                    mapping.pop(self.symbol, None)
        if self.event_id is not None:
            if self.processed:
                account._processed.add(self.event_id)
            else:
                account._processed.discard(self.event_id)
        for key, rows in self.account_history.items():
            getattr(account, key).extend(rows)
        for key, rows in self.book_history.items():
            getattr(account.book, key).extend(rows)


@dataclass(frozen=True)
class AccountResult:
    account_id: str
    sequence: int
    command_sequence: int
    pid: int
    started_ns: int
    finished_ns: int
    cpu_ns: int
    accepted_mark: dict[str, Any] | None
    protected_symbols: frozenset[int]
    patch: _Patch

    @property
    def accepted_book(self) -> bool:
        return self.accepted_mark is not None

    @property
    def outcomes(self) -> list[dict[str, Any]]:
        return self.patch.account_history["outcomes"]

    @property
    def trades(self) -> list[dict[str, Any]]:
        return self.patch.book_history["trades"]


@dataclass(frozen=True)
class _Ack:
    command_sequence: int


@dataclass(frozen=True)
class _Fault:
    account_id: str
    code: str
    exception_type: str


@dataclass(frozen=True)
class _Bootstrap:
    account: AsyncLayeredPaper
    account_id: str
    committed: tuple[int, int]
    audit_location: tuple[Path, str] | None
    timeout: float


@dataclass
class _Worker:
    process: subprocess.Popen[bytes]
    pipe: Connection


def _book_fields(account: AsyncLayeredPaper) -> set[str]:
    return (
        set(vars(account.book)) | {field.name for field in fields(account.book)}
    ) - BOOK_EXCLUDED


def _offsets(target: object, fields: tuple[str, ...]) -> dict[str, int]:
    return {key: len(getattr(target, key)) for key in fields}


async def _evaluate(account: AsyncLayeredPaper, command: _Command) -> AccountResult:
    action = command.action
    account.policy, account.selection, account.allocation, account.entry_policy_version = (
        command.options
    )
    account_offsets = _offsets(account, ACCOUNT_HISTORY)
    book_offsets = _offsets(account.book, BOOK_HISTORY)
    started_ns, cpu_start = time.perf_counter_ns(), time.process_time_ns()
    payload = action.payload
    accepted_mark = None
    if action.kind == "book" and isinstance(payload, dict):
        symbol = payload["symbol"]
        previous = account._books.get(symbol)
        await account.submit(payload)
        book = account._books.get(symbol)
        if book is not None and book is not previous:
            accepted_mark = {
                "symbol": symbol,
                "event_id": payload["id"],
                "source_at": book["timestamp"],
                "received_at": action.published_at or payload["received_at"],
                "decision_at": payload["received_at"],
                "currency": book["currency"],
                "midpoint": account.prices[symbol],
                "equity": account.book.equity,
                "realized": account.book.realized,
                "fees": account.book.fee_totals.copy(),
            }
    elif action.kind == "advance" and isinstance(payload, str):
        await account.advance(payload)
    elif action.kind == "cancel" and isinstance(payload, str):
        await account.cancel_pending(payload)
    else:
        raise AccountProtocolError("Malformed account action")
    patch = _Patch.capture(account, action, account_offsets, book_offsets)
    cpu_ns = time.process_time_ns() - cpu_start
    return AccountResult(
        command.account_id,
        command.sequence,
        command.command_sequence,
        os.getpid(),
        started_ns,
        time.perf_counter_ns(),
        cpu_ns,
        accepted_mark,
        frozenset(account.pending.keys() | account.book.positions.keys()),
        patch,
    )


async def _serve(
    pipe: Connection,
    account: AsyncLayeredPaper,
    account_id: str,
    committed: tuple[int, int],
    audit_location: tuple[Path, str] | None,
    timeout: float,
) -> None:
    audit = RuntimeAudit(*audit_location) if audit_location else None
    last_command, last_cut = committed
    pid = os.getpid()
    context: dict[str, str | int] = {"account_id": account_id}
    diagnostics = ExitStack()
    if audit:
        diagnostics.enter_context(audit.capture_runtime_failures(MODULE))
    try:
        if audit:
            audit.heartbeat(
                MODULE, worker_id=account_id, status="idle", progress=last_command, pid=pid
            )
        pipe.send(pid)
        while True:
            command = pipe.recv()
            if command is None:
                if audit:
                    audit.heartbeat(MODULE, worker_id=account_id, progress=last_command)
                return
            if (
                not isinstance(command, _Command)
                or command.account_id != account_id
                or command.command_sequence != last_command + 1
                or command.sequence < last_cut
                or (command.sequence == last_cut and not command.action.marker)
            ):
                raise AccountProtocolError("Missing, duplicate, or unordered account command")
            context["sequence"] = command.sequence
            if audit:
                audit.heartbeat(
                    MODULE,
                    worker_id=account_id,
                    status="active",
                    timeout=timeout,
                    progress=last_command,
                    pid=pid,
                )
            if pipe.recv() != EXECUTE:
                raise AccountProtocolError("Expected an execution release after dispatch")
            result = await _evaluate(account, command)
            if audit:
                audit.heartbeat(
                    MODULE,
                    worker_id=account_id,
                    status="waiting",
                    timeout=timeout,
                    progress=command.command_sequence,
                    pid=pid,
                )
            pipe.send(result)
            ack = pipe.recv()
            if not isinstance(ack, _Ack) or ack.command_sequence != command.command_sequence:
                raise AccountProtocolError("Account result was not acknowledged in order")
            last_command, last_cut = command.command_sequence, command.sequence
            if audit:
                audit.heartbeat(
                    MODULE,
                    worker_id=account_id,
                    status="idle",
                    progress=last_command,
                    pid=pid,
                )
    except (BrokenPipeError, EOFError):
        return
    except Exception as error:  # noqa: BLE001 - Isolate every child failure at the process boundary.
        code = "command_rejected" if isinstance(error, AccountProtocolError) else "execution_failed"
        if audit:
            audit.report_exception(MODULE, code, error, context)
        pipe.send(_Fault(account_id, code, type(error).__name__))
    finally:
        try:
            with audit.cleanup(MODULE, "account_cleanup_failed") if audit else nullcontext():
                await account.close()
        finally:
            diagnostics.close()


def _worker_main(fd: int) -> None:
    with Connection(fd) as pipe:
        bootstrap = pipe.recv()
        if not isinstance(bootstrap, _Bootstrap):
            raise AccountProtocolError("Malformed account worker initialization")
        asyncio.run(
            _serve(
                pipe,
                bootstrap.account,
                bootstrap.account_id,
                bootstrap.committed,
                bootstrap.audit_location,
                bootstrap.timeout,
            )
        )


def _receive(
    pipes: Mapping[str, Connection],
    timeout: float,
    interrupted: Connection | None = None,
) -> dict[str, Any]:
    names = {pipe: name for name, pipe in pipes.items()}
    result = {}
    end = time.monotonic() + timeout
    while names:
        watched = [*names, interrupted] if interrupted is not None else list(names)
        ready = wait(watched, timeout=max(0.0, end - time.monotonic()))
        if interrupted is not None and interrupted in ready:
            raise AccountWorkerError("Account response wait interrupted")
        if not ready:
            raise AccountWorkerError(f"Account response timed out: {', '.join(names.values())}")
        for ready_pipe in ready:
            pipe = cast(Connection, ready_pipe)
            name = names.pop(pipe)
            try:
                result[name] = pipe.recv()
            except (EOFError, OSError) as error:
                raise AccountWorkerError(f"Account worker disconnected: {name}") from error
    return result


class AccountExecutor:
    """Keep a committed mirror for existing status, exact forks and worker restart."""

    def __init__(
        self,
        accounts: Mapping[str, AsyncLayeredPaper],
        *,
        account_prefix: str = "",
        audit: RuntimeAudit | None = None,
        timeout: float = 30,
    ) -> None:
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Account worker timeout must be finite and positive")
        self.accounts = dict(accounts)
        self.audit = audit
        self.timeout = timeout
        self._prefix = account_prefix
        self._workers: dict[str, _Worker] = {}
        self._committed = dict.fromkeys(accounts, (0, 0))
        self._results: dict[str, AccountResult] = {}
        self._rollback: dict[str, tuple[_Patch, dict[str, int], dict[str, int]]] = {}
        self._busy = False
        self._incidents: set[tuple[str, str]] = set()

    def _identity(self, name: str) -> str:
        return f"{self._prefix}/{name}" if self._prefix else name

    @property
    def pids(self) -> dict[str, int | None]:
        return {name: worker.process.pid for name, worker in self._workers.items()}

    def assert_committable(self) -> None:
        if self.audit and not self.audit.healthy:
            raise AccountWorkerError("Account audit storage is unhealthy")

    def _start(self, names: Mapping[str, _Command]) -> dict[str, Connection]:
        booting = {}
        for name in names:
            current = self._workers.get(name)
            if current is not None and current.process.poll() is not None:
                self._discard(name)
                self._record("worker_disconnected", name)
                current = None
            if current is None:
                parent, child = Pipe()
                fd = child.fileno()
                try:
                    process = subprocess.Popen(
                        [
                            sys.executable,
                            "-B",
                            "-S",
                            "-c",
                            (
                                "import sys\n"
                                "from tools.account_executor import _worker_main\n"
                                "_worker_main(int(sys.argv[1]))"
                            ),
                            str(fd),
                        ],
                        cwd=Path(__file__).resolve().parent.parent,
                        env={},
                        stdin=subprocess.DEVNULL,
                        pass_fds=(fd,),
                    )
                except BaseException:
                    parent.close()
                    raise
                finally:
                    child.close()
                self._workers[name] = _Worker(process, parent)
                booting[name] = parent
        return booting

    def _exchange(
        self,
        commands: dict[str, _Command],
        booting: dict[str, Connection],
        interrupted: Connection,
    ) -> dict[str, Any]:
        location = (self.audit.path.resolve(), self.audit.run_id) if self.audit else None
        for name, pipe in booting.items():
            pipe.send(
                _Bootstrap(
                    self.accounts[name],
                    self._identity(name),
                    self._committed[name],
                    location,
                    self.timeout,
                )
            )
        for name, pid in _receive(booting, self.timeout, interrupted).items():
            if pid != self._workers[name].process.pid:
                raise AccountProtocolError(f"Account worker startup identity mismatch: {name}")
        pipes = {name: self._workers[name].pipe for name in commands}
        for name, command in commands.items():
            try:
                pipes[name].send(command)
            except (BrokenPipeError, EOFError, OSError) as error:
                raise AccountWorkerError(f"Account dispatch failed: {name}") from error
        for pipe in pipes.values():
            pipe.send(EXECUTE)
        return _receive(pipes, self.timeout, interrupted)

    def _validate(self, name: str, command: _Command, result: object) -> AccountResult:
        if isinstance(result, _Fault):
            if result.account_id != command.account_id:
                raise AccountProtocolError("Account fault identity mismatch")
            raise AccountWorkerError(
                f"Account worker {result.code}: {name} ({result.exception_type})"
            )
        if (
            not isinstance(result, AccountResult)
            or (result.account_id, result.sequence, result.command_sequence)
            != (
                command.account_id,
                command.sequence,
                command.command_sequence,
            )
            or result.pid != self._workers[name].process.pid
            or not 0 < result.cpu_ns <= result.finished_ns - result.started_ns
            or not isinstance(result.patch, _Patch)
            or not isinstance(result.protected_symbols, frozenset)
        ):
            raise AccountProtocolError(f"Malformed or unordered account result: {name}")
        try:
            result.patch.check(self.accounts[name], command.action)
            expected_protected = (
                result.patch.account["pending"].keys() | result.patch.book["positions"].keys()
            )
            if result.protected_symbols != expected_protected:
                raise AccountProtocolError(f"Account protection mismatch: {name}")
            if result.accepted_mark is not None:
                book = result.patch.symbol_values["_books"]
                if (
                    command.action.kind != "book"
                    or result.accepted_mark["source_at"] != book["timestamp"]
                    or result.accepted_mark["symbol"] != result.patch.symbol
                    or result.accepted_mark["event_id"] != result.patch.event_id
                ):
                    raise AccountProtocolError(f"Account mark provenance mismatch: {name}")
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise AccountProtocolError(f"Malformed account result: {name}") from error
        return result

    async def apply(
        self,
        sequence: int,
        actions: Mapping[str, AccountAction],
    ) -> dict[str, AccountResult]:
        if self._busy or self._results:
            raise AccountProtocolError(
                "Account results must be committed or aborted before dispatch"
            )
        if type(sequence) is not int or sequence < 0 or actions.keys() - self.accounts.keys():
            raise AccountProtocolError("Invalid account dispatch identity")
        self.assert_committable()
        commands = {}
        for name, action in actions.items():
            last_command, last_cut = self._committed[name]
            if sequence < last_cut or (sequence == last_cut and not action.marker):
                raise AccountProtocolError(f"Duplicate or unordered account observation: {name}")
            account = self.accounts[name]
            commands[name] = _Command(
                self._identity(name),
                sequence,
                last_command + 1,
                copy.deepcopy(action),
                (
                    account.policy,
                    account.selection,
                    account.allocation,
                    account.entry_policy_version,
                ),
            )
        interrupted, interrupt = Pipe(duplex=False)
        self._busy = True
        exchange: asyncio.Task[dict[str, Any]] | None = None
        try:
            booting = self._start(commands)
            exchange = asyncio.create_task(
                asyncio.to_thread(self._exchange, commands, booting, interrupted)
            )
            results = await asyncio.wait_for(asyncio.shield(exchange), self.timeout * 2)
            if results.keys() != commands.keys():
                raise AccountProtocolError("Account result batch is incomplete")
            checked = {
                name: self._validate(name, commands[name], result)
                for name, result in results.items()
            }
            for name, result in checked.items():
                account = self.accounts[name]
                account_offsets = _offsets(account, ACCOUNT_HISTORY)
                book_offsets = _offsets(account.book, BOOK_HISTORY)
                self._rollback[name] = (
                    _Patch.capture(account, actions[name], account_offsets, book_offsets),
                    account_offsets,
                    book_offsets,
                )
                result.patch.apply(account)
            self._results = checked
            return checked
        except BaseException as error:
            if exchange is not None and not exchange.done():
                # Closing a watched worker pipe does not reliably wake poll on macOS.
                interrupt.send_bytes(b"")
            self.abort()
            if exchange is not None:
                await asyncio.gather(exchange, return_exceptions=True)
            if not isinstance(error, asyncio.CancelledError):
                code = (
                    "result_rejected"
                    if isinstance(error, AccountProtocolError)
                    else "dispatch_failed"
                )
                for name in commands:
                    self._record(code, name, sequence, error)
            raise
        finally:
            interrupt.close()
            interrupted.close()
            self._busy = False

    def _record(
        self,
        code: str,
        name: str,
        sequence: int | None = None,
        error: BaseException | None = None,
        *,
        resolved: bool = False,
    ) -> None:
        if self.audit and (not resolved or (name, code) in self._incidents):
            context: dict[str, str | int] = {"account_id": self._identity(name)}
            if sequence is not None:
                context["sequence"] = sequence
            self.audit.record(
                MODULE,
                code,
                context,
                error=error,
                state="resolved" if resolved else "open",
                severity="warning" if code in ("worker_disconnected", "ack_failed") else "error",
            )
            if resolved:
                self._incidents.discard((name, code))
            else:
                self._incidents.add((name, code))

    def commit(self) -> None:
        """Acknowledge a result only after its journal transaction is durable."""
        results, self._results = self._results, {}
        self._rollback.clear()
        for name, result in results.items():
            self._committed[name] = (result.command_sequence, result.sequence)
        for name, result in results.items():
            try:
                self._workers[name].pipe.send(_Ack(result.command_sequence))
            except (BrokenPipeError, EOFError, OSError) as error:
                self._discard(name)
                self._record("ack_failed", name, result.sequence, error)
            else:
                for code in (
                    "dispatch_failed",
                    "result_rejected",
                    "worker_disconnected",
                    "ack_failed",
                ):
                    self._record(code, name, result.sequence, resolved=True)

    def abort(self) -> None:
        """Undo speculative mirrors and discard workers whose state lacks a commit."""
        for name, (patch, account_offsets, book_offsets) in self._rollback.items():
            account = self.accounts[name]
            for key, length in account_offsets.items():
                del getattr(account, key)[length:]
            for key, length in book_offsets.items():
                del getattr(account.book, key)[length:]
            patch.apply(account)
        self._rollback.clear()
        self._results.clear()
        for name in tuple(self._workers):
            self._discard(name)

    def _discard(self, name: str, *, graceful: bool = False) -> None:
        worker = self._workers.pop(name)
        worker.pipe.close()
        if worker.process.poll() is not None:
            return
        if not graceful:
            worker.process.terminate()
        try:
            worker.process.wait(timeout=0.25)
        except subprocess.TimeoutExpired:
            worker.process.kill()
            try:
                worker.process.wait(timeout=1)
            except subprocess.TimeoutExpired as error:
                raise AccountWorkerError(f"Account worker did not exit: {name}") from error

    def close(self) -> None:
        if self._busy or self._results:
            self.abort()
            return
        for name, worker in list(self._workers.items()):
            try:
                worker.pipe.send(None)
            except (BrokenPipeError, EOFError, OSError):
                pass
            self._discard(name, graceful=True)
