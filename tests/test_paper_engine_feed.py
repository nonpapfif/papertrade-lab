import asyncio
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

from paper_engine.contracts import (
    AccountInput,
    AccountState,
    Cut,
    Intent,
    Level,
    MarketRules,
    Quote,
    Side,
)
from paper_engine.feed.books import BookResult, Books, Rejected
from paper_engine.feed.session_adapter import commit_next, protected_symbols
from paper_engine.feed.toss import StreamState, TossBooks, TossStream, normalize_book
from paper_engine.journal import Conflict
from paper_engine.session import Session
from papertrade.api import ApiError, MarketClient

BASE = 1_760_000_000_000


def payload(at: int, market: str = "US") -> dict[str, object]:
    return {
        "timestamp": datetime.fromtimestamp(at / 1000, UTC).isoformat(),
        "currency": {"KR": "KRW", "US": "USD"}[market],
        "bids": [{"price": 9.95, "volume": 7}],
        "asks": [{"price": "10.20", "volume": "4"}],
    }


def frame(at: int, symbol: str = "AAPL", market: str = "US") -> str:
    return json.dumps(
        {
            "type": "message",
            "topic": f"orderbook:{market.lower()}:{symbol}",
            "data": payload(at, market),
        }
    )


def ack(*symbols: str, rejected: tuple[str, ...] = ()) -> str:
    return json.dumps(
        {
            "type": "subscriptions",
            "subscribed": [f"orderbook:us:{symbol}" for symbol in symbols],
            "rejected": rejected,
        }
    )


class Provider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.release = asyncio.Event()
        self.reply: BookResult = None

    async def book(self, market: str, symbol: str) -> BookResult:
        self.calls.append((market, symbol))
        await self.release.wait()
        return self.reply


class StreamTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "paper.sqlite"
        rules = MarketRules("US", "USD", Decimal(0), Decimal(0), 1000, confirmation_delay=0)
        self.session = Session.create(
            self.path,
            rules,
            (
                AccountState("first", "baseline", Decimal(100), Decimal(100)),
                AccountState("second", "candidate", Decimal(100), Decimal(100)),
            ),
        )
        self.addCleanup(self.session.close)
        self.now = BASE
        self.books = Books(Provider(), lambda: self.now, {"US": 1000})
        self.stream = TossStream("US", self.books)
        self.generation = self.stream.begin(("AAPL",)).generation

    def accepted(self, at: int, received: int) -> Quote:
        self.now = received
        accepted = self.stream.receive(self.generation, frame(at), received)
        assert isinstance(accepted, Quote)
        return accepted

    def committed_ids(self) -> list[tuple[int, str]]:
        with sqlite3.connect(self.path) as db:
            return [
                (sequence, quote_id)
                for sequence, raw in db.execute(
                    "SELECT sequence, accepted_quotes FROM cuts ORDER BY sequence"
                )
                for quote_id in json.loads(raw)
            ]

    def test_bad_frames_do_not_disconnect_or_advance_session(self) -> None:
        self.assertEqual(
            self.stream.receive(self.generation, ack("AAPL"), BASE),
            StreamState(self.generation, "trusted"),
        )
        inputs = tuple(
            AccountInput(
                account, (Intent(account, "AAPL", strategy, Side.BUY, Decimal(3), Decimal(11)),)
            )
            for account, strategy in (("first", "baseline"), ("second", "candidate"))
        )
        self.session.apply(Cut("orders", 0, BASE, accounts=inputs))
        first = self.accepted(BASE + 1, BASE + 3)
        self.assertEqual(protected_symbols(self.session), {"AAPL"})
        commit = commit_next(self.books, self.session, at=self.now)
        assert commit is not None
        self.assertEqual(commit.checkpoint.sequence, 2)
        self.assertEqual([result.fills[0].fill.quantity for result in commit.results], [3, 3])
        errors: tuple[object, ...] = (
            None,
            [],
            {},
            {"code": "auth-revoked"},
            {"code": 1, "message": "bad"},
        )
        for error in errors:
            invalid_error = json.dumps({"type": "error", "error": error})
            self.assertEqual(
                self.stream.receive(self.generation, invalid_error, self.now),
                Rejected("invalid_provider_error"),
            )
            self.assertIsNone(commit_next(self.books, self.session, at=self.now))
        for wire in (
            "{",
            b"\xff",
            "[]",
            '{"type":"bad"}',
            frame(BASE + 2, "MSFT"),
            '{"type":"message","topic":"orderbook:us:AAPL","data":[]}',
        ):
            self.assertIsInstance(self.stream.receive(self.generation, wire, self.now), Rejected)
            self.assertIsNone(commit_next(self.books, self.session, at=self.now))
        self.assertIs(first, self.accepted(BASE + 1, BASE + 4))
        self.assertIsNone(commit_next(self.books, self.session, at=self.now))
        second = self.accepted(BASE + 2, BASE + 6)
        self.assertIsNotNone(commit_next(self.books, self.session, at=self.now))
        self.assertEqual(self.committed_ids(), [(2, first.quote_id), (3, second.quote_id)])
        self.assertEqual(self.session.published.checkpoint.at, BASE + 6)

    def test_old_queued_quote_does_not_fill_at_later_processing_time(self) -> None:
        self.stream.receive(self.generation, ack("AAPL"), BASE)
        intent = Intent("pending", "AAPL", "baseline", Side.BUY, Decimal(1), Decimal(11))
        self.session.apply(Cut("orders", 0, BASE, accounts=(AccountInput("first", (intent,)),)))
        quote = self.accepted(BASE + 1, BASE + 3)
        self.now = BASE + 2000
        commit = commit_next(self.books, self.session, at=self.now)
        assert commit is not None
        self.assertEqual(commit.checkpoint.sequence, 2)
        self.assertEqual(commit.checkpoint.at, self.now)
        self.assertEqual(commit.results[0].fills, ())
        self.assertEqual(commit.results[0].state.pending[0].intent, intent)
        self.assertEqual(self.committed_ids(), [])
        self.assertIsNone(self.books.next_quote("US"))
        self.assertEqual(quote.available_at, BASE + 3)

    def test_invalid_ack_requires_close_and_new_generation(self) -> None:
        self.assertEqual(
            self.stream.receive(self.generation, frame(BASE), BASE),
            Rejected("untrusted_generation"),
        )
        for bad_ack in (
            ack("AAPL", rejected=("MSFT",)),
            ack("AAPL", "AAPL"),
            '{"type":"subscriptions","subscribed":[],"rejected":{}}',
            '{"type":"subscriptions","subscribed":["orderbook:us:AAPL"]}',
            '{"type":"error","error":{"code":"wrong-format","message":"Invalid declaration"}}',
        ):
            self.assertEqual(
                self.stream.receive(self.generation, bad_ack, BASE),
                StreamState(self.generation, "resync_required"),
            )
            self.assertEqual(
                self.stream.receive(self.generation, ack("AAPL"), BASE),
                Rejected("untrusted_generation"),
            )
            self.assertEqual(
                self.stream.receive(self.generation, frame(BASE), BASE),
                Rejected("untrusted_generation"),
            )
            self.assertIsNone(self.books.latest("US", "AAPL", 1000))
            with self.assertRaises(ValueError):
                self.stream.begin(("AAPL",))
            self.stream.disconnect(self.generation)
            self.generation = self.stream.begin(("AAPL",)).generation
        self.assertEqual(
            self.stream.disconnect(self.generation - 1), StreamState(self.generation, "pending_ack")
        )
        self.assertEqual(
            self.stream.receive(self.generation - 1, ack("AAPL"), BASE),
            Rejected("inactive_generation"),
        )
        self.assertEqual(
            self.stream.receive(self.generation, ack("AAPL"), BASE),
            StreamState(self.generation, "trusted"),
        )
        self.accepted(BASE, BASE)
        self.assertIsNotNone(commit_next(self.books, self.session, at=self.now))

    def test_unusable_depth_cannot_hide_older_usable_quote(self) -> None:
        self.stream.receive(self.generation, ack("AAPL"), BASE)
        self.now = BASE + 3
        for code, bids in (
            ("locked_or_crossed_book", [{"price": 11, "volume": 10}]),
            ("missing_depth", []),
        ):
            bad_book = payload(BASE + 2)
            bad_book["bids"] = bids
            wire = json.dumps({"type": "message", "topic": "orderbook:us:AAPL", "data": bad_book})
            self.assertEqual(self.stream.receive(self.generation, wire, self.now), Rejected(code))
            self.assertIsNone(self.books.latest("US", "AAPL", 1000))
            self.assertIsNone(self.books.next_quote("US"))
        older = self.accepted(BASE + 1, BASE + 4)
        self.assertIsNotNone(commit_next(self.books, self.session, at=self.now))
        self.assertEqual(self.committed_ids(), [(1, older.quote_id)])
        bad = '{"type":"error","error":{"code":"auth-revoked","message":"Token revoked"}}'
        self.assertEqual(
            self.stream.receive(self.generation, bad, self.now),
            StreamState(self.generation, "resync_required"),
        )
        self.assertEqual(
            self.stream.receive(self.generation, frame(self.now), self.now),
            Rejected("untrusted_generation"),
        )

    def test_reopened_session_acks_only_matching_observation(self) -> None:
        self.stream.receive(self.generation, ack("AAPL"), BASE)
        quote = self.accepted(BASE, BASE + 1)
        original = commit_next(self.books, self.session, at=self.now)
        reopened = Session.open(self.path)
        self.addCleanup(reopened.close)
        self.now = BASE + 4
        duplicate = replace(quote, available_at=self.now)
        books = Books(Provider(), lambda: self.now, {"US": 1000})
        books.observe("US", duplicate)
        self.assertEqual(commit_next(books, reopened, at=self.now), original)
        self.assertIsNone(books.next_quote("US"))
        self.assertEqual(self.committed_ids(), [(1, quote.quote_id)])
        conflicting = Books(Provider(), lambda: self.now, {"US": 1000})
        changed = replace(duplicate, asks=(Level(Decimal("10.50"), Decimal(4)),))
        conflicting.observe("US", changed)
        with self.assertRaises(Conflict):
            commit_next(conflicting, reopened, at=self.now)
        self.assertIs(conflicting.next_quote("US"), changed)


class SharedRequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_protected_requests_precede_candidates_and_share_waiters(self) -> None:
        provider = Provider()
        books = Books(provider, lambda: BASE + 30, {"KR": 1000, "US": 1000})
        active = asyncio.create_task(books.request("US", "ACTIVE", max_age=50))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        tasks = [
            asyncio.create_task(books.request(market, symbol, max_age=50, protected=priority))
            for market, symbol, priority in (
                ("US", "CANDIDATE", False),
                ("KR", "005930", False),
                ("KR", "005930", True),
                ("KR", "005930", True),
            )
        ]
        await asyncio.sleep(0)
        tasks[-1].cancel()
        await asyncio.gather(tasks[-1], return_exceptions=True)
        provider.release.set()
        await asyncio.gather(active, *tasks[:-1])
        self.assertEqual(
            provider.calls,
            [
                ("US", "ACTIVE"),
                ("KR", "005930"),
                ("US", "CANDIDATE"),
            ],
        )

    async def test_coalesced_results_respect_each_waiters_freshness(self) -> None:
        provider = Provider()
        books = Books(provider, lambda: BASE + 30, {"US": 100})
        provider.reply = normalize_book("US", "AAPL", payload(BASE), BASE + 30)
        loose = asyncio.create_task(books.request("US", "AAPL", max_age=50))
        strict = asyncio.create_task(books.request("US", "AAPL", max_age=10))
        await asyncio.sleep(0)
        provider.release.set()
        self.assertEqual(
            await asyncio.gather(loose, strict), [provider.reply, Rejected("stale_quote")]
        )
        self.assertEqual(await books.request("US", "AAPL", max_age=50), provider.reply)
        self.assertEqual(provider.calls, [("US", "AAPL")])
        self.assertEqual(books.next_quote("US"), provider.reply)

    async def test_real_json_numbers_rest_cooldown_and_stream_identity(self) -> None:
        now = BASE + 1
        client = Mock(spec=MarketClient, store=None)
        client.book.side_effect = [
            ApiError("HTTP 429", status=429, code="rate_limit", retry_at=(BASE + 2) / 1000),
            json.loads(json.dumps(payload(BASE))),
        ]
        books = Books(TossBooks(client, lambda: now), lambda: now, {"KR": 1000, "US": 1000})
        self.assertEqual(
            await books.request("KR", "005930", max_age=1000), Rejected("rate_limit", BASE + 2)
        )
        self.assertEqual(
            await books.request("US", "AAPL", max_age=1000), Rejected("cooldown", BASE + 2)
        )
        self.assertEqual(client.book.call_count, 1)
        now = BASE + 2
        rest = await books.request("US", "AAPL", max_age=1000)
        assert isinstance(rest, Quote)
        self.assertEqual(rest.bids[0].price, Decimal("9.95"))
        stream = TossStream("US", books)
        generation = stream.begin(("AAPL",)).generation
        stream.receive(generation, ack("AAPL"), now)
        now += 1
        self.assertIs(stream.receive(generation, frame(BASE), now), rest)
        self.assertIs(books.next_quote("US"), rest)
        books.acknowledge("US", rest)
        self.assertIsNone(books.next_quote("US"))
        self.assertEqual(client.book.call_count, 2)

    async def test_future_stale_and_older_observations_do_not_replace_cache(self) -> None:
        now = BASE + 10
        books = Books(Provider(), lambda: now, {"US": 100})
        current = normalize_book("US", "AAPL", payload(BASE), now)
        assert isinstance(current, Quote)
        self.assertIs(books.observe("US", current), current)
        self.assertEqual(
            books.observe("US", replace(current, available_at=now + 1)), Rejected("future_quote")
        )
        self.assertEqual(
            books.observe("US", replace(current, observed_at=BASE - 1)), Rejected("older_quote")
        )
        self.assertIs(books.latest("US", "AAPL", 10), current)
        now = BASE + 101
        self.assertIsNone(books.latest("US", "AAPL", 1000))
        self.assertEqual(books.observe("US", current), Rejected("stale_quote"))
