"""Offline crash and replay tests for the adaptive paired paper worker."""

import asyncio
import importlib.util
import json
import os
import signal
import sqlite3
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import PropertyMock, patch

from papertrade.core import Settings
from tools.adaptive_journal import AdaptiveJournal, JournalError, read_status
from tools.layered_paper import Feature
from tools.paper_market import KR
from tools.run_async_live_validation import Runtime, campaign

BASE = datetime(2026, 11, 5, 15, tzinfo=UTC)
DEADLINE = (BASE + timedelta(hours=1)).timestamp()
IDENTITY = {"capital": 100, "end_epoch": DEADLINE, "source_sha256": {"fixture": "same"}}


def stamp(offset: int) -> str:
    return (BASE + timedelta(seconds=offset)).isoformat()


def runtime() -> Runtime:
    return Runtime(
        100,
        lambda *_args, **_kwargs: None,
        profile="broker-constrained",
        adaptive=True,
        end_epoch=DEADLINE,
    )


async def inputs(run: Runtime, after: int, through: int) -> None:
    calendar = {
        "today": {
            "date": "2026-11-05",
            "regularMarket": {"startTime": stamp(-1800), "endTime": stamp(1800)},
        },
    }
    bars = [
        {
            "timestamp": stamp((i - 15) * 60),
            "closePrice": 9.7 + i * 0.02,
            "highPrice": 9.7 + i * 0.02,
        }
        for i in range(16)
    ]
    review = (
        1,
        0,
        {
            "action": "change",
            "policy": "rebound",
            "selection": "all",
            "allocation": "equal",
            "reason": "Test a change after a partial fill",
        },
    )

    def book(offset: int) -> tuple[str, dict]:
        return (
            "A",
            {
                "timestamp": stamp(offset),
                "currency": "USD",
                "asks": [{"price": "10", "volume": "1"}],
                "bids": [{"price": "9.99", "volume": "1"}],
            },
        )

    observations = (
        (0, "calendar", ("2026-11-05", calendar)),
        (0, "selection", ["A"]),
        (0, "feature", ("A", bars)),
        (1, "book", book(1)),
        (3, "book", book(3)),
        (4, "policy_review", review),
        (5, "book", book(5)),
    )
    worker = asyncio.create_task(run.consume())
    try:
        for offset, kind, payload in observations[after:through]:
            with patch("tools.run_async_live_validation.now", return_value=stamp(offset)):
                run.publish(kind, payload)
                await asyncio.wait_for(run.queue.join(), 3)
            if worker.done():
                worker.result()
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def crash(path: Path, through: int) -> None:
    run = runtime()
    with AdaptiveJournal.open(path, IDENTITY, resume=False) as journal:
        run.journal = journal
        journal.append({"kind": "initial", "fx_at": run.fx[1]})
        if through >= 0:
            await inputs(run, 0, through)
        else:
            await inputs(run, 0, 4)
            if through == -1:

                async def interrupt_candidate(_event: dict) -> None:
                    os.kill(os.getpid(), signal.SIGKILL)

                with patch.object(run.accounts["candidate"], "submit", interrupt_candidate):
                    await inputs(run, 4, 5)
            else:
                journal.db.execute("BEGIN")
                journal.db.execute("DELETE FROM events")
        os.kill(os.getpid(), signal.SIGKILL)


async def kill_worker(path: Path, through: int) -> int:
    command = (
        "import asyncio, sys; from pathlib import Path; "
        "from tests.test_adaptive_journal import crash; "
        "asyncio.run(crash(Path(sys.argv[1]), int(sys.argv[2])))"
    )
    child = await asyncio.create_subprocess_exec(
        sys.executable, "-c", command, str(path), str(through)
    )
    return await asyncio.wait_for(child.wait(), 10)


class AdaptiveJournalTests(unittest.IsolatedAsyncioTestCase):
    async def test_review_requires_new_affordable_rule_or_allocation_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "journal.db"
            run, restored = runtime(), runtime()
            run.review_settings = Settings.load(Path.cwd())
            run.shared_watch = {"A", "B"}
            candidate = run.accounts["candidate"]
            at = stamp(0)
            features = {
                name: Feature(index, 10, at, at, 0.01, 0.02, 10, 9, 0.8)
                for index, name in enumerate(("A", "B"))
            }
            for name in features:
                run.books[name] = {"asks": [{"price": "10", "volume": "100"}]}

            run._consider_review(1, at, {"A": features["A"]})
            self.assertIsNotNone(run.review_payload)
            run.review_payload = None
            candidate.book.trades.append({"side": "SELL"})
            run._consider_review(2, at, features)
            self.assertIsNone(run.review_payload)
            self.assertEqual(run.reviewed_fills["candidate"], 0)

            candidate.book.cash = 15
            run._consider_review(3, at, features)
            payload = run.review_payload
            assert payload is not None
            self.assertTrue(payload["allocation_became_blocked"])
            self.assertEqual(payload["accounts"]["candidate"]["new_fills"], [{"side": "SELL"}])
            run.review_payload = None
            run._consider_review(4, at, features)
            self.assertIsNone(run.review_payload)

            candidate.book.cash = 100
            candidate.book.trades.clear()
            run.reviewed_fills["candidate"] = 0
            with AdaptiveJournal.open(path, IDENTITY, resume=False) as journal:
                run.journal = journal
                journal.append({"kind": "initial", "fx_at": run.fx[1]})
                await run.mark_advance("recovery_gap", at)
            with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                await restored.restore(journal)
                self.assertEqual(restored.reviewed_families, {"trend"})
                self.assertTrue(restored.allocation_blocked)

            run._consider_review(5, at, features)
            self.assertFalse(run.allocation_blocked)
            self.assertIsNone(run.review_payload)
            candidate.book.cash = 15
            run._consider_review(6, at, features)
            self.assertIsNotNone(run.review_payload)

    async def test_processing_clock_rollback_preserves_pair_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "journal.db"
            run, recovered = runtime(), runtime()
            self.addAsyncCleanup(run.close)
            self.addAsyncCleanup(recovered.close)
            with AdaptiveJournal.open(path, IDENTITY, resume=False) as journal:
                run.journal = journal
                journal.append({"kind": "initial", "fx_at": run.fx[1]})
                await inputs(run, 0, 4)
                worker = asyncio.create_task(run.consume())
                try:
                    for offset in (0, 2):
                        with patch("tools.run_async_live_validation.now", return_value=stamp(offset)):
                            run.publish("clock", None)
                            await asyncio.wait_for(run.queue.join(), 3)
                finally:
                    worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)
                self.assertTrue(worker.cancelled())
                await inputs(run, 3, 5)
                self.assertEqual(run.book_delivered, {"incumbent": 2, "candidate": 2})
                self.assertEqual(run.counts["books_skipped_clock_regression"], 1)
                events = list(journal.events())
                self.assertEqual(
                    [(row["kind"], row["wall_at"], row["at"]) for row in events if "wall_at" in row],
                    [("clock", stamp(0), stamp(1)), ("book", stamp(1), stamp(2))],
                )
            with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                await recovered.restore(journal)
                self.assertEqual(recovered._checkpoint(), run._checkpoint())
                await recovered.mark_advance("recovery_gap", stamp(2))
                self.assertEqual(recovered.account.last_at, stamp(3))
                self.assertEqual(list(journal.events())[-1]["wall_at"], stamp(2))

    async def test_sigkill_replays_reservation_partial_fill_and_policy_change(self) -> None:
        for through, restart_at in ((4, 2), (5, 3), (6, 4)):
            with (
                self.subTest(committed_observations=through),
                tempfile.TemporaryDirectory() as temp,
            ):
                path = Path(temp) / "journal.db"
                self.assertEqual(await kill_worker(path, through), -signal.SIGKILL)

                clean = runtime()
                recovered = runtime()
                repeated = runtime()
                try:
                    # Align the initial FX timestamp so the account input hashes can be compared.
                    with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                        first = next(journal.events())
                        clean.fx = (0, first["fx_at"], None, None)
                        with (
                            patch(
                                "tools.run_async_live_validation.now",
                                side_effect=AssertionError("Replay used the wall clock"),
                            ),
                            patch(
                                "tools.run_async_live_validation.propose",
                                side_effect=AssertionError("Replay called AI"),
                            ),
                        ):
                            await recovered.restore(journal)
                        await inputs(clean, 0, through)
                        self.assertEqual(clean._checkpoint(), recovered._checkpoint())
                        a, b = recovered.accounts.values()
                        if through == 4:
                            self.assertEqual(a.pending, b.pending)
                            self.assertGreater(a.reserved_cash, 0)
                        else:
                            self.assertEqual(a.book.positions[0].quantity, 1)
                            self.assertEqual(b.owner_policy_versions[0], 0)
                            self.assertEqual(b.entry_policy_version, int(through == 6))

                        previous = read_status(path)
                        await recovered.mark_advance("recovery_gap", stamp(restart_at))
                        self.assertEqual(read_status(path)["cursor"], previous["cursor"])
                        self.assertNotEqual(read_status(path)["hash"], previous["hash"])
                        # Apply the same gap clock on the uninterrupted account without persisting it.
                        await clean._advance_marker(
                            {
                                "kind": "recovery_gap",
                                "sequence": clean.sequence,
                                "payload": {"last_at": clean.account.last_at},
                                "received": stamp(restart_at),
                                "at": stamp(restart_at),
                            }
                        )
                        await inputs(clean, through, 7)
                        await inputs(recovered, through, 7)
                        self.assertEqual(clean._checkpoint(), recovered._checkpoint())
                        self.assertEqual(clean.policy_history, recovered.policy_history)
                        self.assertEqual(clean.ids, recovered.ids)
                        self.assertEqual(clean.sequence, recovered.sequence)
                    with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                        await repeated.restore(journal)
                        self.assertEqual(repeated._checkpoint(), recovered._checkpoint())
                        self.assertEqual(
                            repeated.status()["comparison"], recovered.status()["comparison"]
                        )
                        self.assertEqual(repeated.policy_history, recovered.policy_history)
                        journal.complete()
                    with (
                        self.assertRaises(JournalError),
                        AdaptiveJournal.open(path, IDENTITY, resume=True),
                    ):
                        self.fail("Completed journal reopened")
                finally:
                    for run in (clean, recovered, repeated):
                        await run.close()

    async def test_restart_expiry_does_not_create_a_fill_or_repeat_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "journal.db"
            original, restored, repeated = runtime(), runtime(), runtime()
            try:
                with AdaptiveJournal.open(path, IDENTITY, resume=False) as journal:
                    original.journal = journal
                    journal.append({"kind": "initial", "fx_at": original.fx[1]})
                    await inputs(original, 0, 4)
                with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                    await restored.restore(journal)
                    await restored.mark_advance("recovery_gap", stamp(32))
                with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                    await repeated.restore(journal)
                    await repeated.mark_advance("recovery_gap", stamp(33))
                    for account in repeated.accounts.values():
                        self.assertEqual(
                            account.outcomes,
                            [
                                {
                                    "intent_id": account.intents[0].id,
                                    "at": stamp(32),
                                    "status": "expired",
                                }
                            ],
                        )
                        self.assertFalse(account.book.trades)
                        self.assertFalse(account.pending)
            finally:
                for run in (original, restored, repeated):
                    await run.close()

    async def test_crash_before_fanout_or_database_commit_restores_prior_record(self) -> None:
        for stage in (-1, -2):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / "journal.db"
                self.assertEqual(await kill_worker(path, stage), -signal.SIGKILL)
                self.assertEqual(read_status(path)["cursor"], 1)
                restored = runtime()
                try:
                    with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                        await restored.restore(journal)
                        self.assertEqual(restored.sequence, 4)
                        for account in restored.accounts.values():
                            self.assertTrue(account.pending)
                            self.assertFalse(account.book.trades)
                        await inputs(restored, 4, 5)
                        for account in restored.accounts.values():
                            self.assertEqual(len(account.book.trades), 1)
                finally:
                    await restored.close()

    async def test_interrupted_review_is_counted_and_same_signal_does_not_restart_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "journal.db"
            original, restored = runtime(), runtime()
            review_entered = asyncio.Event()

            async def review(*_args: object) -> None:
                review_entered.set()
                await asyncio.Future()

            try:
                with AdaptiveJournal.open(path, IDENTITY, resume=False) as journal:
                    original.journal = journal
                    original.review_settings = cast(Settings, object())
                    journal.append({"kind": "initial", "fx_at": original.fx[1]})
                    with (
                        patch("tools.run_async_live_validation.propose", side_effect=review),
                        patch(
                            "tools.run_async_live_validation.time.time",
                            return_value=BASE.timestamp(),
                        ),
                    ):
                        await inputs(original, 0, 4)
                        await asyncio.wait_for(review_entered.wait(), 3)
                        await original.close()
                with AdaptiveJournal.open(path, IDENTITY, resume=True) as journal:
                    restored.review_settings = cast(Settings, object())
                    with patch("tools.run_async_live_validation.propose") as propose:
                        await restored.restore(journal)
                        self.assertEqual(sum(restored.review_calls.values()), 1)
                        # A matching observation at the original decision time has the same
                        # signal but no confirming quote or fill to justify another review.
                        worker = asyncio.create_task(restored.consume())
                        try:
                            with patch(
                                "tools.run_async_live_validation.now", return_value=stamp(1)
                            ):
                                restored.publish("book", ("A", restored.books["A"]))
                                await asyncio.wait_for(restored.queue.join(), 3)
                            propose.assert_not_called()
                            self.assertEqual(sum(restored.review_calls.values()), 1)
                        finally:
                            worker.cancel()
                            await asyncio.gather(worker, return_exceptions=True)
            finally:
                await original.close()
                await restored.close()

    @unittest.skipUnless(importlib.util.find_spec("websockets"), "Optional WebSocket dependency")
    async def test_expired_cached_token_refreshes_only_after_journal_replay(self) -> None:
        runner = "tools.run_async_live_validation"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            secrets = root / "secrets"
            secrets.mkdir(mode=0o700)
            for name, value in {
                "TOSS_CLIENT_ID": "fixture-client",
                "TOSS_CLIENT_SECRET": "fixture-secret",
                "TOSS_ACCESS_TOKEN.json": json.dumps(
                    {"access_token": "expired", "expires_at": BASE.timestamp() - 1}
                ),
            }.items():
                path = secrets / name
                path.write_text(value)
                path.chmod(0o600)
            output = root / "initial"
            journal_path = root / "journal.db"

            def oauth(method: str, path: str, **_kwargs: object) -> dict:
                self.assertEqual((method, path), ("POST", "/oauth2/token"))
                event_log = (root / "resumed" / "events.jsonl").read_text()
                self.assertIn('"kind": "recovery_gap"', event_log)
                return {"access_token": "fixture-renewed", "expires_in": 3600}

            async def run(output: Path, resume: bool) -> None:
                with self.assertRaisesRegex(SystemExit, "Live validation failed"):
                    await campaign(
                        output,
                        1200,
                        100,
                        profile="broker-constrained",
                        adaptive=True,
                        end_epoch=DEADLINE,
                        journal_path=journal_path,
                        resume=resume,
                        market=KR,
                    )

            with (
                patch.object(
                    Settings, "secrets_dir", new_callable=PropertyMock, return_value=secrets
                ),
                patch(f"{runner}.time.time", return_value=BASE.timestamp()),
                patch(f"{runner}.now", return_value=stamp(0)),
                patch(f"{runner}.MarketClient._request", side_effect=oauth) as request,
                patch(f"{runner}.read_spec", side_effect=OSError("stop before market I/O")) as spec,
            ):
                await run(output, resume=False)
                request.assert_not_called()
                spec.assert_not_called()
                self.assertEqual(
                    json.loads((output / "status.json").read_text())["error_type"], "SafetyError"
                )

                await run(root / "resumed", resume=True)
                request.assert_called_once()
                spec.assert_called_once_with()
                self.assertEqual(
                    json.loads((secrets / "TOSS_ACCESS_TOKEN.json").read_text())["access_token"],
                    "fixture-renewed",
                )

    async def test_expired_recovery_does_not_open_journal_or_load_market_settings(self) -> None:
        with (
            patch("tools.run_async_live_validation.time.time", return_value=DEADLINE),
            patch("tools.run_async_live_validation.AdaptiveJournal.open") as journal_open,
            patch("tools.run_async_live_validation.Settings.load") as settings_load,
            self.assertRaisesRegex(ValueError, "cutoff reached before journal"),
        ):
            await campaign(
                Path("unused"),
                1200,
                100,
                profile="broker-constrained",
                adaptive=True,
                end_epoch=DEADLINE,
                journal_path=Path("unused.db"),
                resume=True,
            )
        journal_open.assert_not_called()
        settings_load.assert_not_called()

    def test_corruption_identity_and_exclusive_owner(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "journal.db"
            with AdaptiveJournal.open(path, IDENTITY, resume=False) as journal:
                journal.append({"kind": "initial", "fx_at": stamp(0)})
                with (
                    self.assertRaises(JournalError),
                    AdaptiveJournal.open(path, IDENTITY, resume=True),
                ):
                    self.fail("Concurrent owner acquired journal")
            with (
                self.assertRaises(JournalError),
                AdaptiveJournal.open(path, {**IDENTITY, "capital": 99}, resume=True),
            ):
                self.fail("Incompatible settings acquired journal")
            with sqlite3.connect(path) as db:
                db.execute("DELETE FROM events")
            with self.assertRaises(JournalError):
                read_status(path)
