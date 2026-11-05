"""Route one finite market feed to independently recoverable adaptive paper pairs."""

from __future__ import annotations

import asyncio
import copy
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from papertrade.core import Settings
from tools.adaptive_journal import AdaptiveJournal, JournalError, encode
from tools.async_layered_paper import ProcessingClock
from tools.probe_toss_websocket import now
from tools.slot_control import SlotCommand, SlotControl

if TYPE_CHECKING:
    from tools.run_async_live_validation import Runtime


@dataclass
class Slot:
    runtime: Runtime
    started_sequence: int
    started_at: str
    state: str = "active"
    stopped_sequence: int | None = None
    stopped_at: str | None = None


class SharedFeedSlots:
    def __init__(
        self,
        make_runtime: Callable[[float, Callable[..., None]], Runtime],
        subscribe: Callable[[list[str], set[str]], list[str]],
        record: Callable[..., None],
        journal: AdaptiveJournal,
        control: SlotControl,
    ) -> None:
        self.make_runtime = make_runtime
        self.subscribe = subscribe
        self.record = record
        self.journal = journal
        self.control = control
        self.review_settings: Settings | None = None
        self.slots: dict[str, Slot] = {}
        self.commands: dict[str, tuple[SlotCommand, str, dict[str, Any]]] = {}
        self.queued_commands: set[str] = set()
        self.queue: asyncio.Queue[tuple[dict[str, Any], float]] = asyncio.Queue()
        self.sequence = 0
        self.consumed = 0
        self.processing_clock = ProcessingClock()
        self.counts: Counter[str] = Counter()
        self.committing = asyncio.Lock()
        self.max_queue = 0
        self.queue_delays: list[float] = []
        self.desired: list[str] = []
        self.candidates: list[str] = []
        self.seed: dict[str, dict[str, Any]] = {}
        self.feature_seed: dict[str, dict[str, Any]] = {}

    def active(self) -> list[tuple[str, Slot]]:
        return [(key, slot) for key, slot in self.slots.items() if slot.state == "active"]

    @property
    def capital(self) -> float:
        return max(
            (slot.runtime.account.book.initial_equity for _, slot in self.active()), default=0.0
        )

    def protected(self) -> set[str]:
        return {symbol for _, slot in self.active() for symbol in slot.runtime.protected()}

    def publish(self, kind: str, payload: Any) -> None:
        self.sequence += 1
        self.queue.put_nowait(
            (
                {
                    "sequence": self.sequence,
                    "kind": kind,
                    "payload": copy.deepcopy(payload),
                    "received": now(),
                },
                time.monotonic(),
            )
        )
        self.max_queue = max(self.max_queue, self.queue.qsize())

    def poll_controls(self) -> None:
        for command in self.control.pending():
            prior = self.commands.get(command.id)
            if prior is not None:
                if prior[0] != command:
                    raise JournalError("Control command differs from the committed command")
                self.control.record(*prior)
            elif command.id not in self.queued_commands:
                self.publish("slot_command", asdict(command))
                self.queued_commands.add(command.id)

    async def consume(self) -> None:
        while True:
            entry, queued = await self.queue.get()
            delay = time.monotonic() - queued
            self.queue_delays.append(delay)
            try:
                async with self.committing:
                    await self._apply(entry, delay)
            finally:
                self.queue.task_done()

    def _record_review(self, slot_id: str, model: str, evidence_sequence: int) -> None:
        self.journal.append(
            {
                "kind": "slot_review_call",
                "slot_id": slot_id,
                "model": model,
                "evidence_sequence": evidence_sequence,
            }
        )

    async def _add(self, command: SlotCommand, entry: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        if command.slot_id in self.slots:
            return "rejected", {"reason": "slot_id_already_used"}
        command.validate()
        spec = command.spec
        assert spec is not None
        capital = spec["capital"]

        slot_id = command.slot_id

        def record(kind: str, **payload: Any) -> None:
            self.record(kind, slot_id=slot_id, **payload)

        runtime = self.make_runtime(float(capital), record)
        runtime.fx = (0, entry["received"], None, None)
        runtime.shared_review_call = lambda model, sequence: self._record_review(
            slot_id, model, sequence
        )
        runtime.shared_review_publish = lambda result: self.publish(
            "slot_review", (slot_id, result)
        )
        for name, account in runtime.accounts.items():
            for key, value in spec[name].items():
                setattr(account, key, value)
            runtime.configurations[name].update(spec[name])
        for event in sorted(
            (*self.seed.values(), *self.feature_seed.values()), key=lambda row: row["sequence"]
        ):
            await runtime._consume_event(copy.deepcopy(event), 0)
        runtime.review_settings = self.review_settings
        self.slots[slot_id] = Slot(runtime, entry["sequence"], entry["at"])
        return "applied", {"state": "active", "sequence": entry["sequence"]}

    async def _stop(self, slot: Slot, entry: dict[str, Any], state: str) -> None:
        slot.state = state
        slot.stopped_sequence = entry["sequence"]
        slot.stopped_at = entry["at"]
        await slot.runtime.close()
        for account in slot.runtime.accounts.values():
            await account.cancel_pending(entry["at"])

    async def _command(self, entry: dict[str, Any]) -> tuple[SlotCommand, str, dict[str, Any]]:
        command = SlotCommand(**entry["payload"])
        if command.id in self.commands:
            raise JournalError("Duplicate committed slot command")
        if command.action == "add":
            disposition, detail = await self._add(command, entry)
        elif command.action == "stop":
            slot = self.slots.get(command.slot_id)
            if slot is None or slot.state != "active":
                disposition, detail = "rejected", {"reason": "slot_not_active"}
            else:
                await self._stop(slot, entry, "stopped")
                disposition, detail = "applied", {"state": "stopped", "sequence": entry["sequence"]}
        else:
            raise JournalError("Unknown slot command")
        result = command, disposition, detail
        self.commands[command.id] = result
        return result

    async def _apply(
        self, entry: dict[str, Any], delay: float, *, saved: dict[str, Any] | None = None
    ) -> None:
        sequence, kind, payload = entry["sequence"], entry["kind"], entry["payload"]
        if sequence != self.consumed + 1:
            raise JournalError("Shared observation sequence mismatch")
        self.processing_clock.stamp(entry, entry.get("at") or now(), self.record)
        command_result = None
        outputs: dict[str, Any] = {}
        if kind == "slot_command":
            command_result = await self._command(entry)
            entry["disposition"], entry["detail"] = command_result[1:]
        elif kind == "final_advance":
            for _, slot in self.active():
                await self._stop(slot, entry, "completed")
        else:
            if kind in ("calendar", "selection", "fx"):
                self.seed[kind] = entry.copy()
            if kind == "selection":
                self.candidates = payload
                retained = set(payload) | self.protected()
                self.feature_seed = {
                    symbol: row for symbol, row in self.feature_seed.items() if symbol in retained
                }
            elif kind == "feature":
                self.feature_seed[payload[0]] = entry.copy()
            event_kind, target_id, event_payload = kind, None, payload
            if kind == "slot_review":
                target_id, event_payload = payload
                event_kind = "policy_review"
            elif kind == "recovery_gap":
                event_kind = "clock"
            watch = set(self.desired)
            for slot_id, slot in self.active():
                if target_id is not None and slot_id != target_id:
                    continue
                runtime = slot.runtime
                runtime.shared_watch = watch
                runtime.sequence = sequence
                event = {**entry, "kind": event_kind, "payload": event_payload}
                recorded = saved["outputs"][slot_id] if saved is not None else None
                books_before = runtime.book_delivered["incumbent"]
                outcomes_before = sum(
                    len(account.outcomes) for account in runtime.accounts.values()
                )
                await runtime._consume_event(event, delay, replaying=saved is not None)
                if recorded is not None:
                    runtime.reviewed_fills = recorded["reviewed_fills"]
                    runtime.reviewed_families = set(recorded["reviewed_families"])
                    runtime.allocation_blocked = recorded["allocation_blocked"]
                outputs[slot_id] = {
                    "progress": (
                        runtime.book_delivered["incumbent"] > books_before
                        or sum(len(account.outcomes) for account in runtime.accounts.values())
                        > outcomes_before
                        or bool(event.get("applied"))
                    ),
                    "reviewed_fills": runtime.reviewed_fills,
                    "reviewed_families": sorted(runtime.reviewed_families),
                    "allocation_blocked": runtime.allocation_blocked,
                }
        self.desired = self.subscribe(self.candidates, self.protected()) if self.active() else []
        self.counts[kind] += 1
        self.consumed = sequence
        entry["outputs"] = outputs
        entry["checkpoints"] = {
            slot_id: slot.runtime._checkpoint() for slot_id, slot in self.slots.items()
        }
        if saved is not None:
            if encode(entry) != encode(
                {key: value for key, value in saved.items() if key != "index"}
            ):
                raise JournalError("Shared feed replay diverged from committed slot results")
            return
        self.journal.append(entry)
        if command_result is not None:
            self.control.record(*command_result)
            self.queued_commands.discard(command_result[0].id)
            self.record(
                "slot_command",
                command_id=command_result[0].id,
                slot_id=command_result[0].slot_id,
                action=command_result[0].action,
                disposition=command_result[1],
                **command_result[2],
            )
        for _, slot in self.active():
            slot.runtime.launch_review()

    async def restore(self) -> None:
        original_record = self.record
        self.record = lambda *_args, **_kwargs: None
        try:
            for saved in self.journal.events():
                if saved["kind"] == "slot_review_call":
                    slot = self.slots[saved["slot_id"]]
                    if slot.state != "active" or saved["evidence_sequence"] > self.consumed:
                        raise JournalError("Review call has no committed active-slot evidence")
                    slot.runtime.review_calls[saved["model"]] += 1
                    continue
                entry = {
                    key: saved[key] for key in ("sequence", "kind", "payload", "received", "at")
                }
                if "wall_at" in saved:
                    entry["wall_at"] = saved["wall_at"]
                await self._apply(entry, 0, saved=saved)
            self.sequence = self.consumed
        finally:
            self.record = original_record

    async def marker(self, kind: str, at: str) -> None:
        self.sequence += 1
        await self._apply(
            {"sequence": self.sequence, "kind": kind, "payload": None, "received": at, "at": at},
            0,
        )

    def enable_reviews(self, settings: Settings) -> None:
        self.review_settings = settings
        for _, slot in self.active():
            slot.runtime.review_settings = settings

    def check_reviews(self) -> None:
        for _, slot in self.active():
            task = slot.runtime.review_task
            if task is not None and task.done():
                task.result()

    async def quiesce_reviews(self) -> None:
        self.review_settings = None
        async with self.committing:
            tasks = []
            for _, slot in self.active():
                runtime = slot.runtime
                runtime.review_settings = None
                if runtime.review_task is not None:
                    tasks.append(runtime.review_task)
                    runtime.review_task.cancel()
                    runtime.review_task = None
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self) -> None:
        for slot in self.slots.values():
            await slot.runtime.close()

    def status(self) -> dict[str, Any]:
        rows = [
            {
                "slot_id": slot_id,
                "state": slot.state,
                "started_sequence": slot.started_sequence,
                "started_at": slot.started_at,
                "stopped_sequence": slot.stopped_sequence,
                "stopped_at": slot.stopped_at,
                **slot.runtime.status(),
            }
            for slot_id, slot in self.slots.items()
        ]
        return {
            "shared_slots": rows,
            "ai_calls": sum(row["ai_calls"] for row in rows),
            "counts": dict(self.counts),
            "desired": self.desired,
            "protected": sorted(self.protected()),
            "queue_depth": self.queue.qsize(),
            "max_queue_depth": self.max_queue,
        }
