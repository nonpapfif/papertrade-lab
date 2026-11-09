"""Shared-feed lifecycle and recovery checks using synthetic provider observations."""

import asyncio
import json
import tempfile
import time
import unittest
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, call, patch

from papertrade.api import ApiError, MarketClient
from papertrade.core import Settings
from tools.adaptive_journal import AdaptiveJournal, read_status
from tools.async_layered_paper import NET_TARGET_BLOCKED
from tools.layered_paper import REBOUND_NET_TARGET
from tools.run_async_live_validation import (
    Runtime,
    fetch_rest_book,
    recover_held_quotes,
    subscription_set,
    wait_for_rest_cooldown,
)
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
            await self.bars(symbol)

    async def bars(
        self,
        symbol: str,
        price: float = 9,
        step: float = 0.05,
        offset: int = 0,
        start: datetime = START,
    ) -> None:
        await self.feed(
            "feature",
            (
                symbol,
                [
                    {
                        "timestamp": (start - timedelta(minutes=15 - i)).isoformat(),
                        "closePrice": price + i * step,
                        "highPrice": price + i * step,
                    }
                    for i in range(16)
                ],
            ),
            offset,
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

    async def test_net_target_candidate_replays_entry_block_and_owner(self) -> None:
        await self.seed()
        await self.feed(
            "feature",
            (
                "A",
                [
                    {
                        "timestamp": (START - timedelta(minutes=15 - i)).isoformat(),
                        "closePrice": price,
                        "highPrice": price,
                    }
                    for i, price in enumerate([11.0] * 10 + [9.0] * 5 + [10.0])
                ],
            ),
        )
        await self.command(
            SlotCommand(
                "test-net-target",
                "add",
                "comparison",
                {
                    "capital": 100,
                    "incumbent": BASELINE,
                    "candidate": {**BASELINE, "policy": REBOUND_NET_TARGET},
                },
            ),
        )
        await self.book("A", 1, price=10.35)
        runtime = self.slots.slots["comparison"].runtime
        candidate = runtime.accounts["candidate"]
        self.assertEqual(candidate.decisions[-1]["blocked_reason"], NET_TARGET_BLOCKED)
        self.assertEqual(candidate.pending, {})
        await self.book("A", 2, price=10.1)
        await self.book("A", 4, price=10.1)
        self.assertEqual(candidate.owners[runtime.ids["A"]][0], REBOUND_NET_TARGET)

        restored = self.coordinator(self.journal)
        self.addAsyncCleanup(restored.close)
        await restored.restore()
        restored_runtime = restored.slots["comparison"].runtime
        self.assertEqual(restored_runtime._checkpoint(), runtime._checkpoint())
        self.assertEqual(restored_runtime.accounts["candidate"].summary(), candidate.summary())

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

    async def test_branch_inherits_holdings_reservations_and_replays_once(self) -> None:
        await self.seed()
        await self.command(add("source", 100))
        await self.book("A", 1)
        await self.book("A", 3)
        await self.book("B", 4, price=2)
        source = self.slots.slots["source"].runtime.accounts["candidate"]
        self.assertTrue(source.book.positions)
        self.assertGreater(source.reserved_cash, 0)
        branch = SlotCommand(
            "cut",
            "branch",
            "comparison",
            {
                "source_slot": "source",
                "source_account": "candidate",
                "candidate": {**BASELINE, "policy": "rebound"},
            },
        )
        self.controls.submit(branch, end_epoch=time.time() + 60)
        cut = self.slots.consumed
        with (
            patch.object(self.controls, "record", side_effect=OSError("lost projection")),
            self.assertRaises(OSError),
        ):
            await self.feed("slot_command", asdict(branch), 5)
        copied = self.slots.slots["comparison"].runtime
        for account in copied.accounts.values():
            self.assertEqual(account.summary(), source.summary())
            self.assertEqual(account.book.trades, source.book.trades)
            self.assertIsNot(account.book.trades, source.book.trades)
            self.assertIsNot(account.context, source.context)
            self.assertEqual(account.pending, source.pending)
            self.assertIsNot(account.pending, source.pending)
            self.assertEqual(account.owner_policy_versions, source.owner_policy_versions)
            self.assertEqual(account._workers, {})
        self.assertEqual(copied.account.policy, "trend")
        self.assertEqual(copied.accounts["candidate"].policy, "rebound")
        self.assertEqual(copied.accounts["candidate"].entry_policy_version, 1)
        lineage = self.slots.slots["comparison"].lineage
        assert lineage is not None
        self.assertEqual(lineage["through_sequence"], cut)

        recovered = self.coordinator(self.journal)
        self.addAsyncCleanup(recovered.close)
        await recovered.restore()
        recovered.poll_controls()
        self.assertTrue(recovered.queue.empty())
        self.assertEqual(self.controls.pending(), [])
        self.assertEqual(recovered.slots["comparison"].runtime._checkpoint(), copied._checkpoint())
        recovered.enable_reviews(Settings(Path("."), {}))
        self.assertIsNone(recovered.slots["comparison"].runtime.review_settings)
        await recovered.quiesce_reviews()

        self.slots = recovered
        await self.feed("selection", [], 6)
        self.assertCountEqual(recovered.desired, ["A", "B"])
        await self.book("B", 7, price=2)
        for account in recovered.slots["comparison"].runtime.accounts.values():
            self.assertEqual(account.owner_policy_versions[1], 0)
            self.assertEqual(account.reserved_cash, 0)
        self.assertGreater(source.reserved_cash, 0)
        await self.feed("selection", ["C"], 8)
        await self.bars("C", 0.4, 0.005, 8)
        await self.book("C", 9, price=0.5)
        comparison = recovered.slots["comparison"].runtime
        symbol = comparison.ids["C"]
        self.assertIn(symbol, comparison.account.pending)
        self.assertNotIn(symbol, comparison.accounts["candidate"].pending)

    async def test_stale_source_branch_rejects_at_cut_and_replays_the_rejection(self) -> None:
        await self.seed()
        source = add("source", 100)
        assert source.spec is not None
        source.spec["incumbent"] = {**BASELINE, "policy": "rebound"}
        await self.command(source)
        await self.book("A", 1)
        await self.book("A", 3)
        runtime = self.slots.slots["source"].runtime
        self.assertFalse(runtime.account.book.positions)
        self.assertTrue(runtime.accounts["candidate"].book.positions)
        self.assertTrue(runtime.accounts["candidate"].quote_health()["comparison_eligible"])

        branch = SlotCommand(
            "cut",
            "branch",
            "comparison",
            {
                "source_slot": "source",
                "source_account": "candidate",
                "candidate": {**BASELINE, "policy": "rebound"},
            },
        )
        await self.command(branch, 64)
        _, disposition, detail = self.slots.commands["cut"]
        self.assertEqual(disposition, "rejected")
        self.assertEqual(detail["reason"], "source_account_quote_ineligible")
        self.assertEqual(detail["comparison_exclusions"][0]["age_seconds"], 61)
        self.assertNotIn("comparison", self.slots.slots)
        self.assertTrue(
            runtime.account.quote_health(detail["quote_health_as_of"])["comparison_eligible"]
        )

        recovered = self.coordinator(self.journal)
        self.addAsyncCleanup(recovered.close)
        await recovered.restore()
        self.assertEqual(recovered.commands["cut"], self.slots.commands["cut"])

    async def test_shared_held_recovery_deduplicates_and_replays_source_time(self) -> None:
        await self.seed()
        for slot_id in ("one", "two"):
            await self.command(add(slot_id, 100))
        await self.book("A", 1)
        await self.book("A", 3)
        accounts = [
            account for _, slot in self.slots.active() for account in slot.runtime.accounts.values()
        ]
        self.assertTrue(all(account.book.positions for account in accounts))
        offset = 64
        attempts: dict[str, float] = {}
        raw = StringIO()
        old_book = {
            "timestamp": (START + timedelta(seconds=3)).isoformat(),
            "currency": "USD",
            "asks": [{"price": "10", "volume": "100"}],
            "bids": [{"price": "9.99", "volume": "100"}],
        }
        fresh_book = {**old_book, "timestamp": (START + timedelta(seconds=76)).isoformat()}
        client = Mock(spec=MarketClient)
        client.book.side_effect = [old_book, ApiError("Rate limit", status=429, retry_at=102), fresh_book]

        async def failure(symbol: str, error: ApiError) -> None:
            self.assertEqual(symbol, "A")
            await wait_for_rest_cooldown(error, 100)

        async def fetch(symbol: str) -> None:
            await fetch_rest_book(client, symbol, raw, self.slots.publish, failure, repair=True)
            if not self.slots.queue.empty():
                entry, _ = self.slots.queue.get_nowait()
                entry["at"] = (START + timedelta(seconds=offset)).isoformat()
                await self.slots._apply(entry, 0)
                self.slots.queue.task_done()

        with (
            patch("tools.run_async_live_validation.now") as now,
            patch("tools.shared_feed_slots.now", side_effect=lambda: now()),
            patch("tools.run_async_live_validation.time") as clock,
            patch("tools.run_async_live_validation.asyncio.sleep") as sleep,
        ):
            now.side_effect = lambda: (START + timedelta(seconds=offset)).isoformat()
            clock.time.return_value = 100
            clock.monotonic.side_effect = lambda: float(offset)
            await recover_held_quotes(self.slots, fetch, attempts, 100)
            client.book.assert_called_once_with("A")
            self.assertEqual(self.slots.stale_held_symbols(now()), {"A"})
            self.assertEqual(
                [account.times[0] for account in accounts],
                [(START + timedelta(seconds=3)).isoformat()] * 4,
            )
            await recover_held_quotes(self.slots, fetch, attempts, 100)
            client.book.assert_called_once_with("A")

            offset = 70
            await recover_held_quotes(self.slots, fetch, attempts, 100)
            sleep.assert_awaited_once_with(2)
            self.assertEqual(self.slots.stale_held_symbols(now()), {"A"})
            self.assertEqual(len(raw.getvalue().splitlines()), 1)

            offset = 76
            await recover_held_quotes(self.slots, fetch, attempts, 100)
            self.assertEqual(self.slots.stale_held_symbols(now()), set())
            self.assertTrue(all(account.times[0] == now() for account in accounts))
            await recover_held_quotes(self.slots, fetch, attempts, 100)
            self.assertEqual(client.book.call_args_list, [call("A")] * 3)

        rows = [json.loads(line) for line in raw.getvalue().splitlines()]
        self.assertEqual([row["result"] for row in rows], [old_book, fresh_book])
        self.assertTrue(all(row["purpose"] == "held_quote_recovery" for row in rows))

        recovered = self.coordinator(self.journal)
        self.addAsyncCleanup(recovered.close)
        await recovered.restore()
        for slot_id, slot in self.slots.active():
            restored = recovered.slots[slot_id].runtime
            self.assertEqual(
                [account.summary() for account in restored.accounts.values()],
                [account.summary() for account in slot.runtime.accounts.values()],
            )

    async def test_invalid_branch_options_do_not_enter_command_spool(self) -> None:
        invalid = SlotCommand(
            "invalid",
            "branch",
            "comparison",
            {"source_slot": "source", "source_account": "candidate", "candidate": {}},
        )
        with self.assertRaises(ValueError):
            self.controls.submit(invalid, end_epoch=time.time() + 60)
        self.assertEqual(self.controls.report()["commands"], [])
        self.assertEqual(self.slots.slots, {})

    async def test_sealed_session_replays_full_accounts_into_next_session(self) -> None:
        root = self.path.parent
        prior_path = root / "prior.sqlite"
        prior_identity = {
            **self.identity,
            "campaign_seeds": {},
            "end_epoch": (START + timedelta(hours=1)).timestamp(),
        }
        prior_journal = self.enterContext(
            AdaptiveJournal.open(prior_path, prior_identity, resume=False)
        )
        self.slots = SharedFeedSlots(
            make_runtime,
            subscription_set,
            lambda *_args, **_kwargs: None,
            prior_journal,
            self.controls,
            session_seeds={},
        )
        self.addAsyncCleanup(self.slots.close)
        await self.seed()
        await self.command(add("one", 100))
        await self.book("A", 1)
        await self.book("A", 3)
        await self.book("B", 4, price=2)
        await self.book("B", 6, price=2)
        await self.bars("A", 9, -0.05, 7)
        await self.book("A", 8, price=7)
        await self.book("A", 10, price=7)
        await self.bars("A", 7, 0.05, 11)
        await self.book("A", 12, price=7)
        await self.command(SlotCommand("reject-park", "park", "one"), 12)
        self.assertEqual(
            self.controls.report()["commands"][-1]["detail"]["reason"], "slot_has_exposure"
        )
        await self.command(add("flat", 120), 12)
        await self.command(SlotCommand("park-flat", "park", "flat"), 12)
        self.assertEqual(self.slots.slots["flat"].state, "parked")
        flat = self.slots.slots["flat"].runtime
        await self.slots.marker("session_close", (START + timedelta(seconds=13)).isoformat())
        prior_journal.complete()
        prior = self.slots.slots["one"].runtime
        self.assertTrue(prior.account.book.positions)
        self.assertLess(prior.account.book.realized, 0)
        self.assertGreater(prior.account.reserved_cash, 0)

        seed = {
            "journal": str(prior_path),
            "slot_id": "one",
            "chain_sha256": prior_journal.evidence()["chain_sha256"],
        }
        seeds = {"one": seed, "flat": {**seed, "slot_id": "flat"}}
        next_end = (START + timedelta(days=1, hours=1)).timestamp()
        identity = {
            **prior_identity,
            "campaign_seeds": seeds,
            "end_epoch": next_end,
        }

        def runtime_at(capital: float, record: Any, end: float) -> Runtime:
            return Runtime(
                capital,
                record,
                profile="broker-constrained",
                adaptive=True,
                end_epoch=end,
            )

        self.controls = self.enterContext(
            SlotControl.open(root / "next-control.sqlite", initialize=True)
        )
        journal = self.enterContext(
            AdaptiveJournal.open(root / "next.sqlite", identity, resume=False)
        )

        def next_slots(next_seeds: dict[str, Any]) -> SharedFeedSlots:
            return SharedFeedSlots(
                lambda capital, record: runtime_at(capital, record, next_end),
                subscription_set,
                lambda *_args, **_kwargs: None,
                journal,
                self.controls,
                session_seeds=next_seeds,
            )

        wrong_source = next_slots({"one": {**seed, "chain_sha256": "0" * 64}})
        with self.assertRaisesRegex(RuntimeError, "different hash"):
            await wrong_source.load_predecessors(runtime_at)
        self.slots = next_slots(seeds)
        self.addAsyncCleanup(self.slots.close)
        await self.slots.load_predecessors(runtime_at)
        self.slots.enable_reviews(Settings(Path("."), {}))
        await self.command(add("one", 999, "wrong-capital"), offset=86_400)
        self.assertNotIn("one", self.slots.slots)
        await self.command(add("one", 100), offset=86_400)
        await self.command(add("flat", 120), offset=86_400)
        resumed = self.slots.slots["one"].runtime
        self.assertEqual(resumed._checkpoint(), prior._checkpoint())
        self.assertEqual(self.slots.slots["flat"].runtime._checkpoint(), flat._checkpoint())
        self.assertEqual(self.slots.slots["flat"].state, "active")
        self.assertIsNone(resumed.review_settings)
        self.assertCountEqual(self.slots.protected(), ["A", "B"])
        await self.feed("clock", None, 86_401)
        self.assertEqual(resumed.account.reserved_cash, 0)
        self.assertEqual(resumed.account.outcomes[-1]["status"], "expired")
        self.assertGreater(prior.account.reserved_cash, 0)
        await self.book("A", 86_402, price=7)
        self.assertEqual(resumed.counts["outside_regular_session"], 1)
        self.assertEqual(resumed.account.reserved_cash, 0)
        next_start = START + timedelta(days=1)
        day = next_start.date().isoformat()
        await self.feed(
            "calendar",
            (
                day,
                {
                    "today": {
                        "date": day,
                        "regularMarket": {
                            "startTime": (next_start - timedelta(minutes=30)).isoformat(),
                            "endTime": (next_start + timedelta(hours=2)).isoformat(),
                        },
                    }
                },
            ),
            86_403,
        )
        await self.feed("selection", ["A", "B"], 86_403)
        await self.bars("A", 7, 0.05, 86_403, next_start)
        await self.book("A", 86_404, price=7)
        self.assertGreater(resumed.account.reserved_cash, 0)
        await self.book("A", 86_406, price=7)
        self.assertIn(resumed.ids["A"], resumed.account.book.positions)

        restored = next_slots(seeds)
        self.addAsyncCleanup(restored.close)
        await restored.load_predecessors(runtime_at)
        await restored.restore()
        self.assertEqual(restored.slots["one"].runtime._checkpoint(), resumed._checkpoint())

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
