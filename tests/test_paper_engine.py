import ast
import sqlite3
import tempfile
import threading
import unittest
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import patch

from paper_engine.account import advance, reserved_cash, reserved_shares
from paper_engine.contracts import (
    AccountInput,
    AccountResult,
    AccountState,
    Branch,
    Commit,
    Cut,
    Intent,
    IntentStatus,
    Level,
    MarketRules,
    PendingIntent,
    Quote,
    Side,
)
from paper_engine.journal import Conflict, Journal
from paper_engine.session import Session

D = Decimal
RULES = MarketRules("US", "USD", D(".01"), D(".02"), 200, confirmation_delay=5, intent_ttl=100)


def proposal(
    intent_id: str, side: Side, quantity: str, limit: str, owner: str = "trend@1", symbol: str = "A"
) -> Intent:
    return Intent(intent_id, symbol, owner, side, D(quantity), D(limit))


def book(
    quote_id: str, at: int, bid: str, ask: str, quantity: str, *, symbol: str = "A",
) -> Quote:
    return Quote(quote_id, symbol, "recording", at, at, (
        Level(D(bid), D(quantity)),
    ), (Level(D(ask), D(quantity)),))


def state(account_id: str = "source") -> AccountState:
    return AccountState(account_id, "trend@1", D(100), D(100))


def for_account(commit: Commit, account_id: str = "source") -> AccountState:
    return next(value for value in commit.checkpoint.accounts if value.account_id == account_id)


class AccountTests(unittest.TestCase):
    def test_hand_calculated_partial_fills_and_reservations(self) -> None:
        initial = state()
        submitted = advance(initial, RULES, 10, (book("before", 10, "8", "9", "20"),), AccountInput(
            "source", (proposal("buy", Side.BUY, "5", "10"),),
        ))
        self.assertEqual(submitted.fills, ())
        self.assertEqual(reserved_cash(submitted.state), D("50.50"))
        early = advance(submitted.state, RULES, 14, (book("early", 14, "8", "9", "20"),), AccountInput(
            "source",
        ))
        self.assertEqual(early.intents[0].reason, "quote_time")
        received_late = replace(book("old", 10, "8", "9", "20"), available_at=15)
        unchanged = advance(early.state, RULES, 15, (received_late,), AccountInput("source"))
        self.assertEqual(unchanged.state, submitted.state)

        partial = advance(
            unchanged.state, RULES, 16, (book("partial", 16, "8", "9", "2.9"),),
            AccountInput("source"),
        )
        self.assertEqual(partial.intents[0].status, IntentStatus.PARTIAL)
        self.assertEqual(partial.fills[0].fill.quantity, D(2))
        self.assertEqual(partial.state.cash, D("81.82"))
        self.assertEqual(reserved_cash(partial.state), D("30.30"))
        self.assertEqual(partial.state.positions[0].cost_basis, D("18.18"))

        rejected = advance(partial.state, RULES, 17, (book("zero", 17, "10", "11", "10"),), AccountInput(
            "source", (proposal("unfunded", Side.BUY, "6", "10", symbol="B"),),
        ))
        self.assertEqual(rejected.intents[0].reason, "limit_price")
        self.assertEqual(rejected.intents[1].reason, "insufficient_cash")
        self.assertEqual(rejected.state, partial.state)

        done = advance(partial.state, RULES, 18, (book("rest", 18, "9", "10", "3"),), AccountInput(
            "source", (proposal("sell", Side.SELL, "4", "9"), proposal("oversell", Side.SELL, "2", "9")),
        ))
        self.assertEqual(done.state.cash, D("51.52"))
        self.assertEqual(done.state.positions[0].cost_basis, D("48.48"))
        self.assertEqual(reserved_shares(done.state, "A"), D(4))
        self.assertEqual(done.intents[-1].reason, "insufficient_shares")
        sold = advance(
            done.state, RULES, 23, (book("sell-depth", 23, "9", "10", "2"),),
            AccountInput("source"),
        )
        self.assertEqual(sold.state.positions[0].quantity, D(3))
        self.assertEqual(sold.state.positions[0].cost_basis, D("29.088"))
        self.assertEqual(sold.state.positions[0].owner, "trend@1")
        self.assertEqual(sold.state.cash, D("69.16"))
        self.assertEqual(sold.state.fees_paid, D(".84"))
        self.assertEqual(sold.state.realized_pnl, D("-1.752"))
        self.assertEqual(reserved_shares(sold.state, "A"), D(2))
        expired = advance(sold.state, RULES, 118, (), AccountInput("source"))
        self.assertEqual(expired.intents[0].status, IntentStatus.EXPIRED)
        self.assertEqual(expired.state.pending, ())
        self.assertEqual(expired.state.realized_pnl, sold.state.realized_pnl)

    def test_multi_level_notional_is_exact_even_when_average_repeats(self) -> None:
        submitted = advance(state(), RULES, 0, (), AccountInput(
            "source", (proposal("buy", Side.BUY, "3", "10"),),
        ))
        quote = replace(book("depth", 5, "8", "9", "2"), asks=(
            Level(D(10), D(1)), Level(D(9), D(2)),
        ))
        done = advance(submitted.state, RULES, 5, (quote,), AccountInput("source"))
        self.assertEqual(done.state.cash, D("71.72"))
        self.assertEqual(done.state.positions[0].cost_basis, D("28.28"))
        self.assertEqual(done.fills[0].fill.notional, D(28))

    def test_one_attempt_preserves_reservation_until_usable_confirmation(self) -> None:
        rules = replace(RULES, single_attempt=True)
        submitted = advance(state(), rules, 10, (), AccountInput(
            "source", (proposal("buy", Side.BUY, "5", "10"),),
        ))
        held = advance(
            submitted.state, rules, 16, (book("crossed", 16, "10", "9", "2"),),
            AccountInput("source"),
        )
        self.assertEqual(held.state, submitted.state)
        expired = advance(held.state, rules, 110, (book("late", 110, "8", "9", "2"),), AccountInput(
            "source",
        ))
        self.assertEqual(expired.fills, ())
        self.assertEqual(expired.intents[-1].status, IntentStatus.EXPIRED)
        partial = advance(
            held.state, rules, 17, (book("confirmation", 17, "8", "9", "2"),),
            AccountInput("source"),
        )
        self.assertEqual(partial.state.pending, ())
        self.assertEqual(partial.state.positions[0].quantity, D(2))

    def test_cancellation_precedes_quote_matching(self) -> None:
        inputs = AccountInput("source", (proposal("buy", Side.BUY, "1", "10"),))
        submitted = advance(state(), RULES, 10, (), inputs)
        cancelled = advance(
            submitted.state, RULES, 16, (book("depth", 16, "8", "9", "2"),),
            AccountInput("source", cancellations=("buy",)),
        )
        self.assertEqual(cancelled.fills, ())
        self.assertEqual(cancelled.state, state())

    def test_kr_and_us_whole_share_inputs(self) -> None:
        kr = MarketRules("KR", "KRW", D(".00015"), D(".00215"), 200)
        self.assertEqual(kr.currency, "KRW")
        with self.assertRaisesRegex(ValueError, "whole shares"):
            proposal("fractional", Side.BUY, "0.5", "10")


class JournalTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "journal.sqlite3"
        self.session = Session.create(self.path, RULES, (state(),))
        self.addCleanup(self.session.close)

    def open(self) -> Session:
        session = Session.open(self.path)
        self.addCleanup(session.close)
        return session

    def submit(self) -> Cut:
        cut = Cut("submit", 0, 10, accounts=(AccountInput(
            "source", (proposal("buy", Side.BUY, "5", "10"),),
        ),))
        self.session.apply(cut)
        return cut

    def test_independent_liquidity_and_exact_branch_state_after_reopen(self) -> None:
        self.submit()
        initial_fill = self.session.apply(Cut("first", 1, 15, (book("initial", 15, "8", "9", "2"),)))
        branch = Branch("compare", "source", "incumbent", "candidate", "breakout@2")
        fork = self.session.apply(Cut("fork", 2, 16, branches=(branch,)))
        parent = for_account(initial_fill)
        self.assertEqual(for_account(fork), parent)
        self.assertEqual(for_account(fork, "incumbent"), replace(parent, account_id="incumbent"))
        self.assertEqual(for_account(fork, "candidate"), replace(
            parent, account_id="candidate", entry_strategy="breakout@2",
        ))
        reopened = self.open()
        self.assertEqual(fork, reopened.published)
        self.assertEqual(initial_fill.checkpoint, reopened.checkpoint(2))

        fill = self.session.apply(Cut(
            "shared", 3, 21, (book("shared-depth", 21, "9", "10", "3"),),
            (
                AccountInput("candidate", (
                    proposal("wrong-owner", Side.BUY, "1", "10", "breakout@2"),
                    proposal("new-owner", Side.BUY, "1", "10", "breakout@2", "B"),
                )),
            ),
        ))
        self.assertEqual([r.fills[0].fill.quantity for r in fill.results], [D(3)] * 3)
        self.assertEqual(for_account(fill, "candidate").positions[0].owner, "trend@1")
        self.assertEqual(fill.results[-1].intents[-2].reason, "strategy_owner")
        self.assertEqual(for_account(fill, "candidate").pending[0].intent.owner, "breakout@2")
        sell = proposal("sell", Side.SELL, "3", "8")
        self.session.apply(Cut("sell", 4, 22, accounts=(AccountInput("source", (sell,)),)))
        loss = self.session.apply(Cut("loss", 5, 27, (book("loss-quote", 27, "8", "9", "1"),)))
        self.assertLess(for_account(loss).realized_pnl, D(0))
        loss_fork = self.session.apply(Cut(
            "loss-fork", 6, 28, branches=(Branch("later", "source", "a2", "b2", "other@1"),),
        ))
        self.assertEqual(for_account(loss_fork, "b2"), replace(
            for_account(loss), account_id="b2", entry_strategy="other@1",
        ))
        self.assertEqual(reserved_shares(for_account(loss_fork, "b2"), "A"), D(2))
        self.assertEqual(loss_fork, self.open().published)

    def test_two_intents_share_depth_only_within_the_same_account(self) -> None:
        branch = Branch("compare", "source", "incumbent", "candidate", "candidate@1")
        self.session.apply(Cut("fork", 0, 10, accounts=(
            AccountInput("source", (
                proposal("first", Side.BUY, "2", "10"), proposal("second", Side.BUY, "2", "10"),
            )),
            AccountInput("candidate", (proposal("entry", Side.BUY, "3", "10", "candidate@1"),)),
        ), branches=(branch,)))
        filled = self.session.apply(Cut("fill", 1, 15, (book("shared-depth", 15, "9", "10", "3"),)))
        self.assertEqual([f.fill.quantity for f in filled.results[0].fills], [D(2), D(1)])
        self.assertEqual(filled.results[2].fills[0].fill.quantity, D(3))

    def test_duplicate_cut_intent_quote_and_branch_do_not_write_twice(self) -> None:
        submit = self.submit()
        first = self.session.published
        quote = book("depth", 15, "8", "9", "5")
        second = self.session.apply(Cut("fill", 1, 15, (quote,)))
        reopened = self.open()
        self.assertEqual(first, reopened.apply(submit))
        self.assertEqual(reopened.published, second)
        with self.assertRaisesRegex(Conflict, "different input"):
            reopened.apply(replace(submit, at=11))
        third = reopened.apply(Cut("duplicate-intent", 2, 16, accounts=submit.accounts))
        self.assertEqual(third.results[0].intents[0].reason, "duplicate_intent")
        self.assertEqual(for_account(third), for_account(second))
        with self.assertRaisesRegex(Conflict, "Duplicate"):
            reopened.apply(Cut("duplicate-quote", 3, 16, (quote,)))
        branch = Branch("branch", "source", "incumbent", "candidate", "breakout@2")
        fourth = reopened.apply(Cut("fork", 3, 17, branches=(branch,)))
        with self.assertRaisesRegex(Conflict, "Duplicate"):
            reopened.apply(Cut(
                "other-fork", 4, 18,
                branches=(replace(branch, incumbent_id="other-a", candidate_id="other-b"),),
            ))
        self.assertEqual(fourth, self.open().published)

    def test_bad_book_does_not_hide_valid_input_with_equal_or_lower_timestamp(self) -> None:
        self.submit()
        bad = self.session.apply(Cut("invalid", 1, 20, (book("crossed", 20, "10", "9", "2"),)))
        self.assertEqual(bad.checkpoint.quote_watermarks, ())
        reopened = self.open()
        valid = reopened.apply(Cut("valid", 2, 20, (
            replace(book("valid-1", 19, "8", "9", "2"), available_at=20),
            replace(book("valid-2", 19, "8", "9", "2"), available_at=20),
        )))
        self.assertEqual(len(valid.results[0].fills), 2)
        self.assertEqual(for_account(valid).positions[0].quantity, D(4))

    def test_incomplete_account_calculation_never_commits(self) -> None:
        advance_account = advance

        def worker(
            value: AccountState, rules: MarketRules, at: int,
            quotes: tuple[Quote, ...], inputs: AccountInput, used_intent_ids: frozenset[str],
        ) -> AccountResult:
            if value.account_id == "candidate":
                raise RuntimeError("worker lost")
            return advance_account(value, rules, at, quotes, inputs, used_intent_ids)

        branch = Branch("compare", "source", "incumbent", "candidate", "candidate@1")
        with patch("paper_engine.session.advance", side_effect=worker), self.assertRaisesRegex(
            RuntimeError, "worker lost",
        ):
            self.session.apply(Cut("failed", 0, 10, branches=(branch,)))
        self.assertEqual(self.open().published.checkpoint.sequence, 0)

    def test_rollback_and_lost_ack_on_a_real_reopened_database(self) -> None:
        self.submit()
        fill = Cut("fill", 1, 15, (book("depth", 15, "8", "9", "5"),))

        def fail_after(fn: Callable[..., Commit]) -> Callable[..., Commit]:
            def raise_after(*args: object) -> Commit:
                fn(*args)
                raise RuntimeError("injected failure")

            return raise_after

        with patch.object(Journal, "_write", fail_after(Journal._write)), self.assertRaisesRegex(
            RuntimeError, "injected failure",
        ):
            self.session.apply(fill)
        self.assertEqual(self.open().published.checkpoint.sequence, 1)
        self.assertEqual(self.session.published.checkpoint.sequence, 1)
        with patch.object(Journal, "commit", fail_after(Journal.commit)), self.assertRaisesRegex(
            RuntimeError, "injected failure",
        ):
            self.session.apply(fill)
        self.assertEqual(self.session.published.checkpoint.sequence, 1)
        reopened = self.open()
        self.assertEqual(reopened.published.checkpoint.sequence, 2)
        self.assertEqual(reopened.published, self.session.apply(fill))
        self.assertEqual(reopened.published, reopened.apply(fill))
        self.assertEqual(for_account(reopened.published).cash, D("54.55"))
        self.assertEqual(len(reopened.published.results[0].fills), 1)

    def test_concurrent_stale_writers_can_only_commit_one_predecessor(self) -> None:
        self.submit()
        barrier = threading.Barrier(2, timeout=5)
        commit = Journal.commit
        outcomes: list[str] = []

        def synchronize(
            journal: Journal, cut: Cut, previous: Commit, results: tuple[AccountResult, ...],
            quotes: tuple[Quote, ...],
        ) -> Commit:
            barrier.wait()
            return commit(journal, cut, previous, results, quotes)

        def write(writer_id: str) -> None:
            session = Session.open(self.path)
            try:
                session.apply(Cut(writer_id, 1, 15, (book(writer_id, 15, "8", "9", "5"),)))
                outcomes.append("committed")
            except Conflict:
                outcomes.append("stale")
            finally:
                session.close()

        writers = [threading.Thread(target=write, args=(str(index),)) for index in range(2)]
        with patch.object(Journal, "commit", synchronize):
            for writer in writers:
                writer.start()
            for writer in writers:
                writer.join(10)
        self.assertCountEqual(outcomes, ["committed", "stale"])
        latest = self.open().published
        self.assertEqual(latest.checkpoint.sequence, 2)
        self.assertEqual(len(latest.results[0].fills), 1)

    def test_duplicate_requests_leave_valid_siblings_eligible(self) -> None:
        self.submit()
        branch = Branch("compare", "source", "incumbent", "candidate", "candidate@1")
        self.session.apply(Cut("fork", 1, 11, branches=(branch,)))
        buy = proposal("candidate-buy", Side.BUY, "1", "10", "candidate@1", "B")
        cut = Cut("duplicate", 2, 15, quotes=(book("confirm", 15, "8", "9", "5"),), accounts=(
            AccountInput("source", (proposal("sibling", Side.BUY, "2", "10", symbol="B"),)),
            AccountInput("candidate", (buy, buy), cancellations=("buy", "buy")),
        ))
        committed = self.session.apply(cut)
        self.assertEqual(committed.results[-1].fills[0].fill.intent_id, "buy")
        self.assertEqual(
            [result.reason for result in committed.results[-1].intents
             if result.status == IntentStatus.REJECTED],
            ["duplicate_cancellation"] * 2 + ["duplicate_intent"] * 2,
        )
        self.assertEqual(for_account(committed).pending[0].intent.intent_id, "sibling")
        self.assertEqual(committed, self.open().published)

    def test_under_reserved_genesis_cannot_be_created_or_reopened(self) -> None:
        path = self.path.with_name("under-reserved.sqlite3")
        request = proposal("under-reserved", Side.BUY, "10", "10")
        initial = replace(state(), pending=(PendingIntent(request, D(10), 0, D(0)),))
        with self.assertRaisesRegex(ValueError, "Pending cash reservation"):
            Session.create(path, RULES, (initial,))
        self.assertFalse(path.exists())

        journal = Journal(path)
        try:
            journal.initialize(RULES, (initial,))
        finally:
            journal.close()
        with self.assertRaisesRegex(ValueError, "Pending cash reservation"):
            Session.open(path)
        with self.assertRaisesRegex(ValueError, "Pending cash reservation"):
            advance(initial, RULES, 0, (), AccountInput("source"))

    def test_invalid_nested_input_or_genesis_never_persists(self) -> None:
        with self.assertRaisesRegex(ValueError, "nonempty strings"):
            proposal(cast(str, 123), Side.BUY, "1", "10")
        with self.assertRaisesRegex(ValueError, "nonempty strings"):
            AccountInput("source", cancellations=(cast(str, 123),))
        with self.assertRaisesRegex(ValueError, "Invalid journal value"):
            self.session.apply(Cut("malformed", 0, 1, accounts=(AccountInput(
                "source", (cast(Intent, 123),),
            ),)))
        invalid_path = self.path.with_name("invalid.sqlite3")
        with self.assertRaisesRegex(ValueError, "Invalid journal value"):
            Session.create(invalid_path, replace(RULES, single_attempt=cast(bool, 123)), (state(),))
        self.assertFalse(invalid_path.exists())
        self.assertEqual(self.open().published.checkpoint.sequence, 0)

    def test_recovery_checks_identity_indexes(self) -> None:
        self.submit()
        with sqlite3.connect(self.path) as db:
            db.execute("DELETE FROM intent_ids")
        with self.assertRaisesRegex(ValueError, "identity indexes differ"):
            self.open()

    def test_recovery_checks_cut_hashes(self) -> None:
        self.submit()
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE cuts SET results = '[]'")
        with self.assertRaisesRegex(ValueError, "Invalid journal chain"):
            self.open()


class ArchitectureTests(unittest.TestCase):
    def test_core_imports_only_declared_contracts_and_standard_library(self) -> None:
        package = Path(__file__).resolve().parents[1] / "paper_engine"
        internal = {
            "__init__": set(),
            "contracts": set(),
            "execution": {"contracts"},
            "account": {"contracts", "execution"},
            "codec": {"contracts"},
            "journal": {"codec", "contracts"},
            "session": {"account", "codec", "contracts", "execution", "journal"},
        }
        standard_library: dict[str, set[str]] = {
            "__init__": set(),
            "contracts": {"dataclasses", "decimal", "enum"},
            "execution": {"decimal"},
            "account": {"collections", "dataclasses", "decimal"},
            "codec": {"dataclasses", "decimal", "enum", "json", "typing"},
            "journal": {"collections.abc", "contextlib", "hashlib", "json", "pathlib", "sqlite3"},
            "session": {"dataclasses", "pathlib", "typing"},
        }
        self.assertEqual({path.stem for path in package.glob("*.py")}, internal.keys())
        for module, dependencies in internal.items():
            tree = ast.parse((package / f"{module}.py").read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for item in node.names:
                        self.assertIn(item.name, standard_library[module])
                if isinstance(node, ast.ImportFrom):
                    name = node.module or ""
                    if node.level:
                        self.assertEqual(node.level, 1)
                        self.assertIn(name, dependencies)
                    else:
                        self.assertIn(name, standard_library[module])
