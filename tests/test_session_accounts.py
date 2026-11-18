"""Focused offline tests for incremental public-feed comparison accounts."""

import copy
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

from tools.session_accounts import SessionAccounts


class SessionAccountTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.open = datetime(2026, 11, 17, 14, 30, tzinfo=UTC)
        self.at = self.open + timedelta(minutes=20)
        self.end = self.open + timedelta(hours=6, minutes=30)
        self.events: list[dict[str, Any]] = []
        self.session = self.make_session()

    def make_session(self, **kwargs: Any) -> SessionAccounts:
        session = SessionAccounts(
            market="US",
            capital=100,
            opened_at=self.open.isoformat(),
            closes_at=self.end.isoformat(),
            **kwargs,
        )
        self.addAsyncCleanup(session.close)
        return session

    def event(self, kind: str, at: datetime | None = None, **kwargs: Any) -> dict[str, Any]:
        row = {
            "id": str(len(self.events) + 1),
            "sequence": len(self.events) + 1,
            "received_at": (at or self.at).isoformat(),
            "kind": kind,
            **kwargs,
        }
        self.events.append(copy.deepcopy(row))
        return row

    async def publish(self, kind: str, at: datetime | None = None, **kwargs: Any) -> None:
        await self.session.consume(self.event(kind, at, **kwargs))

    def candles(self, at: datetime, *, falling: bool = False) -> dict[str, Any]:
        return {
            "candles": [
                {
                    "timestamp": (at - timedelta(minutes=15 - index)).isoformat(),
                    "closePrice": str(10 + (-index if falling else index) / 100),
                    "highPrice": str(10 + (-index if falling else index) / 100),
                }
                for index in range(16)
            ]
        }

    def book(self, at: datetime, *, price: float = 10.15, volume: int = 100) -> dict[str, Any]:
        return {
            "timestamp": at.isoformat(),
            "currency": "USD",
            "asks": [{"price": str(round(price + 0.01, 2)), "volume": str(volume)}],
            "bids": [{"price": str(round(price - 0.01, 2)), "volume": str(volume)}],
        }

    async def prime(self, *, rating: str = "Buy") -> None:
        await self.publish("selection", symbols=["A"])
        await self.publish("candles", symbol="A", result=self.candles(self.at))
        await self.publish("fx", result={"midRate": "1400"})
        await self.publish(
            "research",
            ratings={"A": rating},
            published_at=self.at.isoformat(),
            expires_at=self.end.isoformat(),
        )

    async def fill(self, *, volume: int = 100) -> None:
        await self.publish("orderbook", symbol="A", book=self.book(self.at))
        later = self.at + timedelta(seconds=2)
        await self.publish("orderbook", later, symbol="A", book=self.book(later, volume=volume))

    async def test_shared_publications_independent_cash_and_partial_depth(self) -> None:
        await self.prime()
        await self.fill(volume=2)
        baseline, research, rebound = self.session.accounts.values()
        self.assertEqual(baseline.book.positions[0].quantity, 2)
        self.assertEqual(research.book.positions[0].quantity, 2)
        self.assertIsNot(baseline.book.positions[0], research.book.positions[0])
        self.assertEqual(baseline.book.cash, research.book.cash)
        self.assertLess(baseline.book.cash, 100)
        self.assertEqual(rebound.book.cash, 100)
        self.assertFalse(rebound.book.positions)
        self.assertEqual(baseline.outcomes[0]["status"], "partial")
        request = baseline.intents[0].order_request
        assert request is not None
        self.assertEqual(request["quantity"], "9")
        self.assertEqual(self.session.protected(), {"A"})
        tick = self.at.timestamp() + 2
        self.assertEqual(self.session.signal_family("A", tick), "trend")
        self.assertIsNone(self.session.signal_family("A", tick + 91))

    async def test_research_only_gates_new_entries_and_selection_keeps_exits(self) -> None:
        await self.prime()
        await self.fill()
        later = self.at + timedelta(seconds=3)
        await self.publish(
            "research",
            later,
            ratings={"A": "Sell"},
            published_at=later.isoformat(),
            expires_at=self.end.isoformat(),
        )
        await self.publish("selection", later, symbols=[])
        self.assertEqual(self.session.protected(), {"A"})
        self.assertIn(0, self.session.accounts["research_selection"].book.positions)
        await self.publish("candles", later, symbol="A", result=self.candles(later, falling=True))
        await self.publish("orderbook", later, symbol="A", book=self.book(later, price=9.85))
        confirm = later + timedelta(seconds=2)
        await self.publish("orderbook", confirm, symbol="A", book=self.book(confirm, price=9.85))
        for name in ("existing_selection", "research_selection"):
            account = self.session.accounts[name]
            self.assertFalse(account.book.positions)
            self.assertLess(account.book.realized, 0)
            self.assertEqual(account.intents[-1].side, "SELL")
        self.assertEqual(self.session.protected(), set())

    async def test_unpublished_and_expired_ratings_cannot_open_research_account(self) -> None:
        await self.prime(rating="Hold")
        await self.fill()
        self.assertTrue(self.session.accounts["existing_selection"].book.positions)
        self.assertFalse(self.session.accounts["research_selection"].intents)
        bad = self.event(
            "research",
            self.at + timedelta(seconds=3),
            ratings={"A": "Buy"},
            published_at=(self.at + timedelta(seconds=5)).isoformat(),
            expires_at=self.end.isoformat(),
        )
        with self.assertRaisesRegex(ValueError, "published"):
            await self.session.consume(bad)
        published, expiry = self.at + timedelta(seconds=3), self.at + timedelta(seconds=4)
        await self.publish(
            "research",
            published,
            ratings={"A": "Buy"},
            published_at=published.isoformat(),
            expires_at=expiry.isoformat(),
        )
        await self.publish("orderbook", expiry, symbol="A", book=self.book(expiry))
        self.assertFalse(self.session.accounts["research_selection"].intents)

    async def test_research_rebound_has_its_own_fixed_strategy(self) -> None:
        await self.prime()
        result = self.candles(self.at)
        prices = [12 - index * 0.3 for index in range(11)] + [
            9 + index * 0.02 for index in range(1, 6)
        ]
        for row, price in zip(result["candles"], prices, strict=True):
            row["closePrice"] = row["highPrice"] = str(price)
        await self.publish("candles", symbol="A", result=result)
        for offset in (0, 2):
            at = self.at + timedelta(seconds=offset)
            await self.publish("orderbook", at, symbol="A", book=self.book(at, price=9.1))
        baseline, trend, rebound = self.session.accounts.values()
        self.assertFalse(baseline.intents)
        self.assertFalse(trend.intents)
        self.assertTrue(rebound.book.positions)
        self.assertEqual(rebound.owners[0][0], "rebound")
        self.assertEqual(self.session.signal_family("A", self.at.timestamp() + 2), "rebound")

    async def test_ordered_replay_duplicates_and_conflicts(self) -> None:
        await self.prime()
        await self.fill(volume=2)
        cut = (self.at + timedelta(seconds=2)).isoformat()
        expected = self.session.summary(at=cut)
        self.assertFalse(await self.session.consume(self.events[0]))
        self.assertEqual(self.session.summary(at=cut), expected)
        restored = self.make_session()
        for event in self.events:
            await restored.consume(event)
        self.assertEqual(restored.summary(at=cut), expected)
        altered = {**self.events[0], "symbols": ["B"]}
        with self.assertRaisesRegex(ValueError, "different content"):
            await restored.consume(altered)
        backwards = {**self.events[-1], "id": "other", "sequence": 1}
        with self.assertRaisesRegex(ValueError, "sequence"):
            await restored.consume(backwards)

    async def test_final_marks_use_calendar_close_and_incomplete_is_never_ranked(self) -> None:
        await self.prime()
        await self.fill()
        partial = self.session.summary(at=self.end.isoformat(), stream_state="interrupted")
        self.assertFalse(partial["session_complete"])
        self.assertFalse(partial["ranking_eligible"])
        self.assertEqual(partial["ranking"], [])
        final = self.session.summary(at=self.end.isoformat(), stream_state="complete")
        marked = final["accounts"]["existing_selection"]["summary"]
        self.assertTrue(final["session_complete"])
        self.assertFalse(final["ranking_eligible"])
        self.assertEqual(marked["positions_as_of"], self.end.isoformat())
        self.assertEqual(marked["positions"]["0"]["mark_at"], self.events[-1]["book"]["timestamp"])
        self.assertGreater(marked["positions"]["0"]["mark_age_seconds"], 60)
        await self.publish("clock", self.end + timedelta(seconds=90))
        grace = self.session.summary(
            at=(self.end + timedelta(seconds=90)).isoformat(),
            stream_state="complete",
        )
        self.assertEqual(grace["reported_as_of"], self.end.isoformat())

    async def test_empty_account_summary_and_clock_expiry(self) -> None:
        summary = self.session.summary(at=self.at.isoformat())
        self.assertEqual(
            summary["accounts"]["existing_selection"]["summary"]["account"]["cash"], 100
        )
        await self.prime()
        await self.publish("orderbook", symbol="A", book=self.book(self.at))
        self.assertTrue(self.session.accounts["existing_selection"].pending)
        await self.publish("clock", self.at + timedelta(seconds=31))
        self.assertFalse(self.session.accounts["existing_selection"].pending)
        self.assertEqual(self.session.accounts["existing_selection"].reserved_cash, 0)

    async def test_session_close_releases_young_day_intent_once(self) -> None:
        self.at = self.end - timedelta(seconds=2)
        await self.prime()
        await self.publish("orderbook", symbol="A", book=self.book(self.at))
        account = self.session.accounts["existing_selection"]
        self.assertTrue(account.pending)
        self.assertGreater(account.reserved_cash, 0)
        await self.session.close_session()
        self.assertFalse(account.pending)
        self.assertEqual(account.reserved_cash, 0)
        self.assertEqual(account.last_at, self.end.isoformat())
        count = len(account.outcomes)
        await self.session.close_session()
        self.assertEqual(len(account.outcomes), count)
        self.assertFalse(account.book.positions)
        self.assertIsNone(self.session.signal_family("A", self.at.timestamp()))

    async def test_us_zero_and_subshare_depth_cannot_cross_or_distort_midpoint(self) -> None:
        await self.prime()
        for offset in (0, 2):
            at = self.at + timedelta(seconds=offset)
            book = self.book(at)
            book["bids"].append({"price": "999", "volume": "0"})
            book["bids"].append({"price": "998", "volume": "0.5"})
            book["asks"].append({"price": "0", "volume": "0"})
            await self.publish("orderbook", at, symbol="A", book=book)
        account = self.session.accounts["existing_selection"]
        self.assertTrue(account.book.positions)
        self.assertEqual(account.prices[0], 10.15)
        self.assertEqual(self.session.latest["A"]["bids"], self.book(self.at)["bids"])
        self.assertEqual(self.session.counts["accepted_books"], 2)
        self.assertEqual(self.session.counts["unusable_books"], 0)

    async def test_complete_healthy_marks_allow_only_descriptive_ranking(self) -> None:
        self.at = self.end - timedelta(seconds=4)
        await self.prime()
        await self.fill()
        before_close = self.session.summary(
            at=(self.at + timedelta(seconds=2)).isoformat(),
            stream_state="complete",
        )
        self.assertFalse(before_close["session_complete"])
        self.assertEqual(before_close["ranking"], [])
        closed = self.session.summary(at=self.end.isoformat(), stream_state="complete")
        self.assertTrue(closed["ranking_eligible"])
        self.assertEqual(len(closed["ranking"]), 3)
        self.assertEqual(closed["evaluation_status"], "observed_short_sample")

    async def test_invalid_latest_book_excludes_held_mark_and_old_source_cannot_restore_it(
        self,
    ) -> None:
        self.at = self.end - timedelta(seconds=6)
        await self.prime()
        await self.fill()
        later = self.at + timedelta(seconds=3)
        invalid = self.book(later)
        invalid["bids"][0]["price"] = "20"
        await self.publish("orderbook", later, symbol="A", book=invalid)
        await self.publish("orderbook", later, symbol="A", book=self.book(self.at))
        report = self.session.summary(at=self.end.isoformat(), stream_state="complete")
        self.assertFalse(report["ranking_eligible"])
        marked = report["accounts"]["existing_selection"]["summary"]
        self.assertEqual(
            marked["comparison_exclusions"][-1]["reason"], "latest_publication_quote_unusable"
        )
        self.assertIsNone(marked["account"]["bid_liquidation_equity_estimate_usd"])
        self.assertEqual(report["publication_counts"]["regressed_books"], 1)

    async def test_external_research_gate_is_queried_at_publication_time(self) -> None:
        self.session = self.make_session(
            selection_at=lambda tick: (
                frozenset({"A"}) if tick >= self.at.timestamp() + 1 else frozenset()
            )
        )
        await self.publish("selection", symbols=["A"])
        await self.publish("candles", symbol="A", result=self.candles(self.at))
        await self.publish("orderbook", symbol="A", book=self.book(self.at))
        self.assertFalse(self.session.accounts["research_selection"].intents)
        later = self.at + timedelta(seconds=2)
        await self.publish("orderbook", later, symbol="A", book=self.book(later))
        self.assertEqual(
            self.session.accounts["research_selection"].intents[0].decision_at, later.isoformat()
        )

    async def test_reporting_fx_preserves_original_receipt_and_requires_provider_validity(
        self,
    ) -> None:
        for label in ("valid", "missing", "expired", "stale_receipt"):
            with self.subTest(label=label):
                self.session = self.make_session()
                await self.prime()
                result = {"midRate": "1400"}
                if label != "missing":
                    result.update(
                        {
                            "validFrom": (self.at - timedelta(hours=1)).isoformat(),
                            "validUntil": (
                                self.at + timedelta(seconds=1 if label == "expired" else 60)
                            ).isoformat(),
                        }
                    )
                receipt = self.at - timedelta(seconds=70) if label == "stale_receipt" else self.at
                await self.publish("fx", result=result, source_received_at=receipt.isoformat())
                await self.fill()
                basis = self.session.accounts["existing_selection"].book.entry_cost_krw[0]
                if label == "valid":
                    self.assertIsNotNone(basis)
                else:
                    self.assertIsNone(basis)
                self.assertEqual(self.session.fx["fx_at"], receipt.isoformat())
                self.assertTrue(self.session.fx["fx_provider_validity_required"])

    async def test_queued_quote_cannot_satisfy_delay_from_later_processing_time(self) -> None:
        await self.prime()
        await self.publish("orderbook", symbol="A", book=self.book(self.at))
        processed = self.at + timedelta(seconds=2)
        source = self.at + timedelta(milliseconds=500)
        receipt = self.at + timedelta(milliseconds=750)
        await self.publish(
            "orderbook",
            processed,
            symbol="A",
            book=self.book(source),
            source_received_at=receipt.isoformat(),
        )
        account = self.session.accounts["existing_selection"]
        self.assertFalse(account.book.positions)
        self.assertTrue(account.pending)
        source = self.at + timedelta(seconds=3)
        processed = self.at + timedelta(seconds=4)
        await self.publish(
            "orderbook",
            processed,
            symbol="A",
            book=self.book(source),
            source_received_at=source.isoformat(),
        )
        self.assertTrue(account.book.positions)
        self.assertEqual(account.intents[0].decision_at, self.at.isoformat())
        self.assertEqual(account.context.frame["at"], processed.isoformat())
        self.assertEqual(account.context.frame["confirmation_received_at"][0], source.isoformat())

    async def test_source_receipt_before_open_cannot_become_session_evidence(self) -> None:
        await self.prime()
        await self.publish(
            "orderbook",
            symbol="A",
            book=self.book(self.at),
            source_received_at=(self.open - timedelta(seconds=1)).isoformat(),
        )
        self.assertFalse(self.session.accounts["existing_selection"].intents)
        self.assertEqual(self.session.counts["accepted_books"], 0)

    async def test_kr_native_whole_shares_fees_and_session_boundary(self) -> None:
        session = SessionAccounts(
            market="KR",
            capital=100_000,
            opened_at=self.open.isoformat(),
            closes_at=self.end.isoformat(),
        )
        self.addAsyncCleanup(session.close)
        self.session = session
        await self.publish("selection", symbols=["005930"])
        result = self.candles(self.at)
        for index, row in enumerate(result["candles"]):
            row["closePrice"] = row["highPrice"] = str(1000 + index)
        await self.publish("candles", symbol="005930", result=result)
        for offset in (0, 2):
            at = self.at + timedelta(seconds=offset)
            book = {
                "timestamp": at.isoformat(),
                "currency": "KRW",
                "asks": [{"price": "1016", "volume": "100"}, {"price": "0", "volume": "0"}],
                "bids": [{"price": "1014", "volume": "100"}],
            }
            await self.publish("orderbook", at, symbol="005930", book=book)
        account = session.accounts["existing_selection"]
        self.assertEqual(account.book.positions[0].quantity, 98)
        self.assertAlmostEqual(account.book.cash, 100_000 - 98 * 1016 * 1.00015)
        await self.publish(
            "orderbook",
            self.end,
            symbol="005930",
            book={
                **book,
                "timestamp": self.end.isoformat(),
            },
        )
        self.assertEqual(len(account.intents), 1)
        self.assertEqual(session.counts["accepted_books"], 2)
        self.assertEqual(session.summary(at=self.end.isoformat())["currency"], "KRW")


if __name__ == "__main__":
    unittest.main()
