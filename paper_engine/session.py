"""Coordinate every account in a market cut before publishing a durable commit."""

from dataclasses import replace
from pathlib import Path
from typing import Self

from .account import advance, validate_account
from .codec import validate
from .contracts import (
    AccountInput,
    AccountState,
    Checkpoint,
    Commit,
    Cut,
    MarketRules,
    Quote,
    require_time,
)
from .execution import book_issue
from .journal import Conflict, Journal


def _usable_quotes(
    cut: Cut, current: tuple[tuple[str, int], ...], rules: MarketRules
) -> tuple[Quote, ...]:
    watermarks = dict(current)
    usable: list[Quote] = []
    prior_availability = -1
    for quote in cut.quotes:
        if not prior_availability <= quote.available_at <= cut.at:
            raise ValueError("Quotes must be available in cut order")
        prior_availability = quote.available_at
        if (
            quote.observed_at >= watermarks.get(quote.symbol, -1)
            and cut.at - quote.observed_at <= rules.max_quote_age
            and not book_issue(quote)
        ):
            usable.append(quote)
            watermarks[quote.symbol] = quote.observed_at
    return tuple(usable)


class Session:
    """The only writer of committed account state from paper execution."""

    def __init__(self, journal: Journal) -> None:
        self._journal = journal
        self.rules, self._published = journal.recover()
        for state in self._published.checkpoint.accounts:
            validate_account(state, self.rules)

    @classmethod
    def create(
        cls, path: Path, rules: MarketRules, accounts: tuple[AccountState, ...]
    ) -> Self:
        if not accounts or len({state.account_id for state in accounts}) != len(accounts):
            raise ValueError("Session requires unique initial accounts")
        validate(rules)
        for state in accounts:
            validate(state)
            validate_account(state, rules)
        journal = Journal(path)
        try:
            journal.initialize(rules, accounts)
            return cls(journal)
        except BaseException:
            journal.close()
            raise

    @classmethod
    def open(cls, path: Path) -> Self:
        if not path.is_file():
            raise FileNotFoundError(path)
        journal = Journal(path)
        try:
            return cls(journal)
        except BaseException:
            journal.close()
            raise

    @property
    def published(self) -> Commit:
        return self._published

    def checkpoint(self, sequence: int) -> Checkpoint:
        require_time(sequence)
        return self._journal.checkpoint(sequence)

    def close(self) -> None:
        self._journal.close()

    def apply(self, cut: Cut) -> Commit:
        validate(cut)
        previous = self._journal.refresh(self._published)
        self._published = previous
        checkpoint = previous.checkpoint
        if cut.expected_sequence != checkpoint.sequence:
            receipt = self._journal.receipt(cut)
            if receipt is not None:
                return receipt
            raise Conflict("Cut must name the current journal predecessor")
        if cut.at < checkpoint.at:
            raise ValueError("Cut time moved backwards")

        states = {state.account_id: state for state in checkpoint.accounts}
        for branch in cut.branches:
            parent = next(
                (state for state in checkpoint.accounts if state.account_id == branch.parent_id),
                None,
            )
            if parent is None:
                raise ValueError("Branch requires a parent at the exact predecessor cut")
            if (
                branch.incumbent_id == branch.candidate_id
                or branch.incumbent_id in states
                or branch.candidate_id in states
            ):
                raise ValueError("Branch requires distinct unused account IDs and a strategy")
            states[branch.incumbent_id] = replace(parent, account_id=branch.incumbent_id)
            states[branch.candidate_id] = replace(
                parent, account_id=branch.candidate_id, entry_strategy=branch.candidate_strategy,
            )

        inputs = {entry.account_id: entry for entry in cut.accounts}
        if len(inputs) != len(cut.accounts) or not inputs.keys() <= states.keys():
            raise ValueError("Cut has duplicate or unknown account input")
        quotes = _usable_quotes(cut, checkpoint.quote_watermarks, self.rules)
        results = []
        for account_id, state in states.items():
            entry = inputs.get(account_id, AccountInput(account_id))
            used_intent_ids = self._journal.used_proposal_ids(entry)
            results.append(advance(state, self.rules, cut.at, quotes, entry, used_intent_ids))
        committed = self._journal.commit(cut, previous, tuple(results), quotes)
        self._published = committed
        return committed
