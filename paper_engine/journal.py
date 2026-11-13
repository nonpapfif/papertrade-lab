"""SQLite ownership of durable cuts, idempotency and optimistic writer checks."""

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .codec import decode, encode
from .contracts import (
    VERSION,
    AccountInput,
    AccountResult,
    AccountState,
    Checkpoint,
    Commit,
    Cut,
    IntentStatus,
    MarketRules,
    Quote,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    version INTEGER NOT NULL,
    rules TEXT NOT NULL,
    genesis TEXT NOT NULL,
    digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cuts (
    sequence INTEGER PRIMARY KEY,
    cut_id TEXT NOT NULL UNIQUE,
    input TEXT NOT NULL,
    accepted_quotes TEXT NOT NULL,
    results TEXT NOT NULL,
    digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quote_ids (quote_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS intent_ids (
    account_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    PRIMARY KEY (account_id, intent_id)
);
CREATE TABLE IF NOT EXISTS branch_ids (command_id TEXT PRIMARY KEY);
"""


class Conflict(RuntimeError):
    """A durable ID or expected predecessor conflicts with the request."""


def _pack(items: tuple[str, ...]) -> str:
    return json.dumps(items, separators=(",", ":"))


def _digest(*items: str) -> str:
    return hashlib.sha256(_pack(items).encode()).hexdigest()


def _intent_ids(
    cut: Cut, accounts: tuple[AccountState, ...], results: tuple[AccountResult, ...]
) -> Iterator[tuple[str, str]]:
    for result in results:
        for outcome in result.intents:
            if outcome.status == IntentStatus.ACCEPTED:
                yield result.state.account_id, outcome.intent_id
    parents = {state.account_id: state for state in accounts}
    for branch in cut.branches:
        for child in (branch.incumbent_id, branch.candidate_id):
            for pending in parents[branch.parent_id].pending:
                yield child, pending.intent.intent_id


def _watermarks(
    current: tuple[tuple[str, int], ...], quotes: tuple[Quote, ...]
) -> tuple[tuple[str, int], ...]:
    watermarks = dict(current)
    for quote in quotes:
        watermarks[quote.symbol] = max(quote.observed_at, watermarks.get(quote.symbol, -1))
    return tuple(sorted(watermarks.items()))


class Journal:
    """A durable market journal. Open separate connections for separate writers."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path, isolation_level=None)
        try:
            self._db.execute("PRAGMA journal_mode = WAL")
            self._db.execute("PRAGMA synchronous = FULL")
            self._db.executescript(_SCHEMA)
        except sqlite3.Error:
            self.close()
            raise

    def close(self) -> None:
        self._db.close()

    @contextmanager
    def _transaction(self, *, write: bool = False) -> Iterator[None]:
        self._db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
            self._db.commit()
        except BaseException:
            if self._db.in_transaction:
                self._db.rollback()
            raise

    def initialize(self, rules: MarketRules, states: tuple[AccountState, ...]) -> None:
        raw_rules = encode(rules)
        raw_genesis = _pack(tuple(encode(state) for state in states))
        digest = _digest(str(VERSION), raw_rules, raw_genesis)
        with self._transaction(write=True):
            row = self._db.execute("SELECT digest FROM metadata").fetchone()
            if row is not None:
                if row[0] != digest:
                    raise Conflict("Journal already has different initial state or rules")
                return
            self._db.execute(
                "INSERT INTO metadata VALUES (1, ?, ?, ?, ?)",
                (VERSION, raw_rules, raw_genesis, digest),
            )
            self._db.executemany(
                "INSERT INTO intent_ids VALUES (?, ?)",
                (
                    (state.account_id, pending.intent.intent_id)
                    for state in states for pending in state.pending
                ),
            )

    def _genesis(self) -> tuple[MarketRules, Commit]:
        row = self._db.execute("SELECT version, rules, genesis, digest FROM metadata").fetchone()
        if row is None:
            raise ValueError("Journal has no initial state")
        version, rules, genesis, digest = row
        if version != VERSION or _digest(str(version), rules, genesis) != digest:
            raise ValueError("Invalid journal version or initial state digest")
        states = tuple(decode(raw, AccountState) for raw in json.loads(genesis))
        return decode(rules, MarketRules), Commit(digest, Checkpoint(0, 0, states), ())

    def _rows(self, sequence: int | None = None) -> sqlite3.Cursor:
        if sequence is None:
            return self._db.execute(
                "SELECT sequence, cut_id, input, accepted_quotes, results, digest "
                "FROM cuts ORDER BY sequence"
            )
        return self._db.execute(
            "SELECT sequence, cut_id, input, accepted_quotes, results, digest FROM cuts "
            "WHERE sequence <= ? ORDER BY sequence", (sequence,),
        )

    def _read(
        self,
        sequence: int | None = None,
        *,
        after: Commit | None = None,
        verify_indexes: bool = False,
    ) -> tuple[MarketRules, Commit]:
        rules, commit = self._genesis()
        genesis_digest = commit.digest
        quote_ids: set[tuple[str, ...]] = set()
        branch_ids: set[tuple[str, ...]] = set()
        intent_ids: set[tuple[str, ...]] = {
            (state.account_id, pending.intent.intent_id)
            for state in commit.checkpoint.accounts for pending in state.pending
        }
        rows = self._rows(sequence)
        if after is not None:
            commit = after
            rows.close()
            anchor = self._db.execute(
                "SELECT digest FROM cuts WHERE sequence = ?", (commit.checkpoint.sequence,),
            ).fetchone() if commit.checkpoint.sequence else (genesis_digest,)
            if anchor != (commit.digest,):
                raise ValueError("Published journal anchor changed")
            rows = self._db.execute(
                "SELECT sequence, cut_id, input, accepted_quotes, results, digest FROM cuts "
                "WHERE sequence > ? ORDER BY sequence", (commit.checkpoint.sequence,),
            )

        for number, cut_id, raw_cut, raw_quotes, raw_results, digest in rows:
            expected = commit.checkpoint.sequence + 1
            if number != expected or _digest(commit.digest, raw_cut, raw_quotes, raw_results) != digest:
                raise ValueError(f"Invalid journal chain at cut {number}")
            cut = decode(raw_cut, Cut)
            if cut.cut_id != cut_id or cut.expected_sequence != number - 1:
                raise ValueError(f"Invalid journal identity at cut {number}")
            results = tuple(decode(raw, AccountResult) for raw in json.loads(raw_results))
            if verify_indexes:
                new_quotes = [(quote.quote_id,) for quote in cut.quotes]
                new_branches = [(branch.command_id,) for branch in cut.branches]
                new_intents = list(_intent_ids(cut, commit.checkpoint.accounts, results))
                for known, incoming in (
                    (quote_ids, new_quotes), (branch_ids, new_branches), (intent_ids, new_intents),
                ):
                    if len(incoming) != len(set(incoming)) or known.intersection(incoming):
                        raise ValueError(f"Duplicate journal identity at cut {number}")
                    known.update(incoming)
            accepted_ids = set(json.loads(raw_quotes))
            quotes = tuple(quote for quote in cut.quotes if quote.quote_id in accepted_ids)
            if len(quotes) != len(accepted_ids):
                raise ValueError(f"Invalid journal quote references at cut {number}")
            checkpoint = Checkpoint(
                number, cut.at, tuple(result.state for result in results),
                _watermarks(commit.checkpoint.quote_watermarks, quotes),
            )
            commit = Commit(digest, checkpoint, results)
        if verify_indexes and (
            quote_ids != set(self._db.execute("SELECT quote_id FROM quote_ids"))
            or branch_ids != set(self._db.execute("SELECT command_id FROM branch_ids"))
            or intent_ids != set(self._db.execute("SELECT account_id, intent_id FROM intent_ids"))
        ):
            raise ValueError("Journal identity indexes differ from recorded cuts")
        if sequence is not None and commit.checkpoint.sequence != sequence:
            raise ValueError(f"Unknown cut sequence {sequence}")
        return rules, commit

    def recover(self) -> tuple[MarketRules, Commit]:
        """Verify the recorded chain and restore the latest committed economic state."""
        with self._transaction():
            return self._read(verify_indexes=True)

    def checkpoint(self, sequence: int) -> Checkpoint:
        with self._transaction():
            return self._read(sequence)[1].checkpoint

    def refresh(self, after: Commit) -> Commit:
        with self._transaction():
            return self._read(after=after)[1]

    def used_proposal_ids(self, inputs: AccountInput) -> frozenset[str]:
        return frozenset(
            proposal.intent_id
            for proposal in inputs.proposals
            if self._db.execute(
                "SELECT 1 FROM intent_ids WHERE account_id = ? AND intent_id = ?",
                (inputs.account_id, proposal.intent_id),
            ).fetchone() is not None
        )

    def receipt(self, cut: Cut) -> Commit | None:
        with self._transaction():
            return self._receipt(cut)

    def _receipt(self, cut: Cut) -> Commit | None:
        row = self._db.execute(
            "SELECT sequence, input FROM cuts WHERE cut_id = ?", (cut.cut_id,),
        ).fetchone()
        if row is None:
            return None
        sequence, raw_cut = row
        if raw_cut != encode(cut):
            raise Conflict("Cut ID already has different input")
        return self._read(sequence)[1]

    def _write(
        self,
        cut: Cut,
        previous: Commit,
        results: tuple[AccountResult, ...],
        quotes: tuple[Quote, ...],
    ) -> Commit:
        sequence = previous.checkpoint.sequence + 1
        raw_cut = encode(cut)
        raw_quotes = _pack(tuple(quote.quote_id for quote in quotes))
        raw_results = _pack(tuple(encode(result) for result in results))
        digest = _digest(previous.digest, raw_cut, raw_quotes, raw_results)
        self._db.execute(
            "INSERT INTO cuts VALUES (?, ?, ?, ?, ?, ?)",
            (sequence, cut.cut_id, raw_cut, raw_quotes, raw_results, digest),
        )
        self._db.executemany(
            "INSERT INTO quote_ids VALUES (?)", ((quote.quote_id,) for quote in cut.quotes),
        )
        self._db.executemany(
            "INSERT INTO branch_ids VALUES (?)", ((branch.command_id,) for branch in cut.branches),
        )
        self._db.executemany(
            "INSERT INTO intent_ids VALUES (?, ?)", _intent_ids(cut, previous.checkpoint.accounts, results),
        )
        return Commit(
            digest,
            Checkpoint(
                sequence, cut.at, tuple(result.state for result in results),
                _watermarks(previous.checkpoint.quote_watermarks, quotes),
            ),
            results,
        )

    def commit(
        self,
        cut: Cut,
        previous: Commit,
        results: tuple[AccountResult, ...],
        quotes: tuple[Quote, ...],
    ) -> Commit:
        try:
            with self._transaction(write=True):
                receipt = self._receipt(cut)
                if receipt is not None:
                    return receipt
                head = self._db.execute(
                    "SELECT sequence, digest FROM cuts ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                if head is None:
                    head = (0, self._genesis()[1].digest)
                if head != (cut.expected_sequence, previous.digest) or (
                    cut.expected_sequence != previous.checkpoint.sequence
                ):
                    raise Conflict("Journal predecessor changed")
                return self._write(cut, previous, results, quotes)
        except sqlite3.IntegrityError as error:
            raise Conflict("Duplicate journal identity") from error
