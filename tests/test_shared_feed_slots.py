"""Shared-feed lifecycle and recovery checks using synthetic provider observations."""

import asyncio
import tempfile
import time
import unittest
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

from papertrade.core import Settings
from tools.adaptive_journal import AdaptiveJournal, read_status
from tools.run_async_live_validation import Runtime, subscription_set
from tools.shared_feed_slots import SharedFeedSlots
from tools.slot_control import SlotCommand, SlotControl

START = datetime(2026, 11, 4, 15, tzinfo=UTC)
BASELINE = {"policy": "trend", "selection": "all", "allocation": "equal"}


def add(slot_id: str, capital: float, command_id: str | None = None) -> SlotCommand:
    return SlotCommand(
        command_id or slot_id,
        "add",
        slot_id,
        {"capital": capital, "incumbent": BASELINE, "candidate": BASELINE},
    )


def make_runtime(capital: float, record: Any) -> Runtime:
    return Runtime(
        capital,
        record,
        profile="broker-constrained",
        adaptive=True,
        end_epoch=(START + timedelta(hours=1)).timestamp(),
    )


class SharedFeedSlotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.spool = root / "control.sqlite"
        self.path = root / "journal.sqlite"
        self.identity = {"experiment": "synthetic_shared_feed"}
        self.controls = self.enterContext(SlotControl.open(self.spool, initialize=True))
        self.journal = self.enterContext(
            AdaptiveJournal.open(self.path, self.identity, resume=False)
        )
        self.slots = self.coordinator(self.journal)
        self.addAsyncCleanup(self.slots.close)

    def coordinator(self, journal: AdaptiveJournal) -> SharedFeedSlots:
        return SharedFeedSlots(
            make_runtime, subscription_set, lambda *_args, **_kwargs: None, journal, self.controls
        )

    async def feed(self, kind: str, payload: Any, offset: int = 0) -> None:
        at = (START + timedelta(seconds=offset)).isoformat()
        self.slots.sequence += 1
        await self.slots._apply(
            {
                "sequence": self.slots.sequence,
                "kind": kind,
                "payload": payload,
                "received": at,
                "at": at,
            },
            0,
        )

    async def command(self, command: SlotCommand, offset: int = 0) -> None:
        self.controls.submit(command, end_epoch=time.time() + 60)
        await self.feed("slot_command", asdict(command), offset)

    async def seed(self) -> None:
        day = START.date().isoformat()
        await self.feed(
            "calendar",
            (
                day,
                {
                    "today": {
                        "date": day,
                        "regularMarket": {
                            "startTime": (START - timedelta(minutes=30)).isoformat(),
                            "endTime": (START + timedelta(hours=2)).isoformat(),
                        },
                    }
                },
            ),
        )
        await self.feed("selection", ["A", "B"])
        for symbol in ("A", "B"):
            await self.feed(
                "feature",
                (
                    symbol,
                    [
                        {
                            "timestamp": (START - timedelta(minutes=15 - i)).isoformat(),
                            "closePrice": 9 + i * 0.05,
                            "highPrice": 9 + i * 0.05,
                        }
                        for i in range(16)
                    ],
                ),
            )

    async def book(self, symbol: str, offset: int, price: float = 10.0) -> None:
        await self.feed(
            "book",
            (
                symbol,
                {
                    "timestamp": (START + timedelta(seconds=offset)).isoformat(),
                    "currency": "USD",
                    "asks": [{"price": str(price), "volume": "100"}],
                    "bids": [{"price": str(price - 0.01), "volume": "100"}],
                },
            ),
            offset,
        )

    async def test_clock_rollback_skips_book_and_preserves_shared_replay(self) -> None:
        await self.seed()
        await self.command(add("small", 100))
        await self.command(add("large", 200))
        await self.book("A", 2)
        await self.feed("clock", None, 0)
        await self.book("A", 1)
        await self.book("A", 3)
        restored = self.coordinator(self.journal)
        self.addAsyncCleanup(restored.close)
        await restored.restore()
        for slot_id, slot in restored.slots.items():
            self.assertEqual(slot.runtime.book_delivered, {"incumbent": 2, "candidate": 2})
            self.assertEqual(
                slot.runtime._checkpoint(), self.slots.slots[slot_id].runtime._checkpoint()
            )

    async def test_add_stop_replay_keeps_independent_ledgers_and_last_marks(self) -> None:
        await self.seed()
        self.assertEqual(self.slots.desired, [])
        await self.command(add("small", 100))
        await self.book("A", 1)
        await self.command(add("large", 200), 2)
        self.assertEqual(self.slots.capital, 200)
        self.assertEqual(self.slots.slots["large"].runtime.account.intents, [])
        await self.book("A", 3)
        await self.book("A", 5)
        await self.book("B", 6, price=2)
        small = self.slots.slots["small"].runtime
        large = self.slots.slots["large"].runtime
        self.assertIn("A", self.slots.protected())
        self.assertGreater(small.account.reserved_cash, 0)
        self.assertGreater(large.account.reserved_cash, 0)
        small_mark = small.account.prices.copy()
        await self.command(SlotCommand("stop", "stop", "small"), 7)
        self.assertEqual(small.account.reserved_cash, 0)
        self.assertEqual(small.account.outcomes[-1]["status"], "slot_stopped")
        self.assertTrue(small.account.book.positions)
        self.assertEqual(small.account.prices, small_mark)
        self.assertTrue(small.status()["comparison"]["inputs_aligned"])
        self.assertTrue(large.status()["comparison"]["inputs_aligned"])
        stopped = small._checkpoint()
        await self.book("B", 8, price=2)
        self.assertEqual(small._checkpoint(), stopped)
        self.assertNotEqual(small._checkpoint(), large._checkpoint())
        await self.command(add("small", 500, "reuse"), 9)
        self.assertEqual(self.controls.report()["commands"][-1]["disposition"], "rejected")
        self.assertEqual(self.slots.capital, 200)

        recovered = self.coordinator(self.journal)
        self.addAsyncCleanup(recovered.close)
        await recovered.restore()
        self.assertEqual(recovered.slots["small"].state, "stopped")
        self.assertEqual(recovered.slots["small"].runtime._checkpoint(), stopped)
        self.assertEqual(recovered.slots["large"].runtime._checkpoint(), large._checkpoint())
        cursor = read_status(self.path)["cursor"]
        await recovered.marker("recovery_gap", (START + timedelta(seconds=10)).isoformat())
        self.assertEqual(read_status(self.path)["cursor"], cursor)
        self.assertEqual(recovered.slots["small"].runtime._checkpoint(), stopped)

    async def test_recovery_acknowledges_already_committed_command(self) -> None:
        command = add("one", 100)
        self.controls.submit(command, end_epoch=time.time() + 60)
        with (
            patch.object(self.controls, "record", side_effect=OSError("lost projection")),
            self.assertRaises(OSError),
        ):
            await self.feed("slot_command", asdict(command))
        self.assertEqual(self.controls.pending(), [command])

        recovered = self.coordinator(self.journal)
        self.addAsyncCleanup(recovered.close)
        await recovered.restore()
        recovered.poll_controls()
        self.assertTrue(recovered.queue.empty())
        self.assertEqual(self.controls.pending(), [])
        self.assertEqual(recovered.slots["one"].state, "active")
        self.assertEqual(read_status(self.path)["cursor"], 1)

    async def test_review_is_slot_scoped_and_committed_before_external_call(self) -> None:
        self.slots.enable_reviews(Settings(Path("."), {}))
        await self.seed()
        await self.command(add("one", 100))
        await self.command(add("two", 150))
        reviewed: list[float] = []

        async def propose(_settings: Any, payload: dict[str, Any], _model: str) -> dict[str, Any]:
            reviewed.append(payload["accounts"]["candidate"]["cash_usd"])
            evidence = payload["evidence_sequence"]
            self.assertTrue(any(row.get("sequence") == evidence for row in self.journal.events()))
            return {"action": "change", "reason": "synthetic", **BASELINE, "allocation": "priority"}

        with (
            patch("tools.run_async_live_validation.propose", side_effect=propose),
            patch("tools.run_async_live_validation.time.time", return_value=START.timestamp()),
        ):
            await self.book("A", 1)
            await asyncio.gather(
                *(
                    task
                    for _, slot in self.slots.active()
                    if (task := slot.runtime.review_task) is not None
                )
            )
            self.assertCountEqual(reviewed, [100, 150])
            review_entry, _ = self.slots.queue.get_nowait()
            # A result for slot one leaves the other candidate untouched until its own result.
            self.assertEqual(review_entry["payload"][0], "one")
            review_entry["at"] = (START + timedelta(seconds=2)).isoformat()
            await self.slots._apply(review_entry, 0)
            one = self.slots.slots["one"].runtime
            two = self.slots.slots["two"].runtime
            self.assertEqual(one.accounts["candidate"].allocation, "priority")
            self.assertEqual(two.accounts["candidate"].allocation, "equal")

        recovered = self.coordinator(self.journal)
        self.addAsyncCleanup(recovered.close)
        await recovered.restore()
        self.assertEqual(
            recovered.slots["one"].runtime.accounts["candidate"].allocation, "priority"
        )
        self.assertEqual(recovered.slots["two"].runtime.accounts["candidate"].allocation, "equal")
        self.assertEqual(recovered.status()["ai_calls"], 2)
        self.assertIsNone(recovered.slots["two"].runtime.review_task)
        self.slots = recovered
        recovered.enable_reviews(Settings(Path("."), {}))
        with patch("tools.run_async_live_validation.propose", new_callable=AsyncMock) as request:
            await self.book("A", 1)
            await asyncio.sleep(0)
        request.assert_not_called()

    async def test_partial_fanout_is_discarded_on_recovery(self) -> None:
        await self.seed()
        await self.command(add("one", 100))
        await self.command(add("two", 150))
        one = self.slots.slots["one"].runtime
        two = self.slots.slots["two"].runtime
        checkpoints = one._checkpoint(), two._checkpoint()
        before = read_status(self.path)
        with (
            patch.object(two, "_consume_event", side_effect=OSError("second slot failed")),
            self.assertRaises(OSError),
        ):
            await self.book("A", 1)
        self.assertNotEqual(one._checkpoint(), checkpoints[0])
        self.assertEqual(read_status(self.path), before)

        recovered = self.coordinator(self.journal)
        self.addAsyncCleanup(recovered.close)
        await recovered.restore()
        self.assertEqual(
            tuple(slot.runtime._checkpoint() for _, slot in recovered.active()), checkpoints
        )

    async def test_cutoff_cancels_in_flight_review_and_keeps_terminal_evidence(self) -> None:
        self.slots.enable_reviews(Settings(Path("."), {}))
        await self.seed()
        await self.command(add("one", 100))
        began = asyncio.Event()

        async def propose(*_args: Any) -> Any:
            began.set()
            await asyncio.Future()

        with (
            patch("tools.run_async_live_validation.propose", side_effect=propose),
            patch("tools.run_async_live_validation.time.time", return_value=START.timestamp()),
        ):
            await self.book("A", 1)
            await asyncio.wait_for(began.wait(), 1)
            await self.slots.quiesce_reviews()
            self.slots.check_reviews()
            await self.slots.marker("final_advance", (START + timedelta(seconds=2)).isoformat())
        self.assertEqual(self.slots.slots["one"].state, "completed")
        self.assertEqual(self.slots.slots["one"].runtime.account.reserved_cash, 0)
        self.assertEqual(self.slots.status()["ai_calls"], 1)
