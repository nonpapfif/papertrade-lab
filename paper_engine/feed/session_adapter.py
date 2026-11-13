"""Transfer one ordered market observation at a time into the session."""

from ..contracts import Commit, Cut, require_time
from ..session import Session
from .books import Books


def commit_next(books: Books, session: Session, *, at: int) -> Commit | None:
    require_time(at)
    market = session.rules.market
    quote = books.next_quote(market)
    if quote is None:
        return None
    cut_id = "feed:" + quote.quote_id
    existing = session.quote_receipt(cut_id, quote)
    if existing is not None:
        books.acknowledge(market, quote)
        return existing
    previous = session.published.checkpoint
    commit = session.apply(Cut(cut_id, previous.sequence, at, (quote,)))
    books.acknowledge(market, quote)
    return commit


def protected_symbols(session: Session) -> frozenset[str]:
    symbols: set[str] = set()
    for account in session.published.checkpoint.accounts:
        symbols.update(position.symbol for position in account.positions)
        symbols.update(pending.intent.symbol for pending in account.pending)
    return frozenset(symbols)
