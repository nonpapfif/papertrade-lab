"""Offline integration coverage for the KR shared paper runner."""

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from papertrade.api import MarketClient
from papertrade.core import Settings
from tools.adaptive_journal import AdaptiveJournal, JournalError
from tools.live_feed import LiveFeed
from tools.paper_market import KR, US
from tools.probe_toss_websocket import declaration
from tools.run_async_live_validation import (
    Runtime,
    journal_identity,
    rank_candidates,
    write_comparison,
)

BASE = datetime.fromisoformat("2026-11-05T15:19:00+09:00")
SYMBOL = "005930"


def stamp(offset: int) -> str:
    return (BASE + timedelta(seconds=offset)).isoformat()


def quote(
    offset: int, bid: str = "990", currency: str = "KRW", volume: str = "1"
) -> tuple[str, dict[str, Any]]:
    return SYMBOL, {
        "timestamp": stamp(offset),
        "currency": currency,
        "asks": [{"price": "0", "volume": "0"}, {"price": "1000", "volume": volume}],
        "bids": [{"price": bid, "volume": volume}],
    }


class KrFeedTests(unittest.TestCase):
    def test_native_universe_and_stream_declaration(self) -> None:
        client = Mock(spec=MarketClient)
        client.listed.side_effect = [[{"symbol": "005930"}], [{"symbol": "1234A0"}], []]
        feed = LiveFeed(Settings(Path("/tmp"), {}), Mock(), client=client, market=KR)
        self.assertEqual(feed.universe(), ["005930", "1234A0"])
        self.assertEqual(
            [c.args[0] for c in client.listed.call_args_list], ["KOSPI", "KOSDAQ", "KR_ETC"]
        )
        self.assertEqual(feed.listing_exchanges["005930"], {"KOSPI"})
        self.assertEqual(
            declaration("kr", ["005930", "1234A0"]),
            [
                {"type": "trade:kr", "codes": ["005930", "1234A0"]},
                {"type": "orderbook:kr", "codes": ["005930", "1234A0"]},
            ],
        )
        with self.assertRaises(ValueError):
            declaration("kr", ["AAPL"])

    def test_native_rank_rejects_wrong_currency_and_keeps_leading_zeroes(self) -> None:
        rows = [
            {"symbol": "005930", "currency": "KRW", "lastPrice": "1200", "timestamp": stamp(0)},
            {"symbol": "000660", "currency": "USD", "lastPrice": "100", "timestamp": stamp(0)},
        ]
        ranked, fresh = rank_candidates(
            rows, {"005930": 1000, "000660": 20}, stamp(0), 1500, KR.currency
        )
        self.assertEqual(ranked, ["005930"])
        self.assertEqual(fresh, {"005930": 1200})


class KrRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_fanout_policy_and_market_bound_recovery(self) -> None:
        end_epoch = (BASE + timedelta(minutes=3)).timestamp()
        capital = 10_000

        def runtime() -> Runtime:
            return Runtime(
                capital,
                Mock(),
                profile="broker-constrained",
                adaptive=True,
                end_epoch=end_epoch,
                market=KR,
            )

        run, recovered = runtime(), runtime()
        self.addAsyncCleanup(run.close)
        self.addAsyncCleanup(recovered.close)
        calendar = {
            "today": {
                "date": "2026-11-05",
                "integrated": {
                    "regularMarket": {
                        "startTime": "2026-11-05T09:00:00+09:00",
                        "singlePriceAuctionStartTime": stamp(60),
                        "endTime": stamp(660),
                    }
                },
            }
        }
        bars = [
            {
                "timestamp": stamp((i - 15) * 60),
                "closePrice": 970 + i * 2,
                "highPrice": 970 + i * 2,
                "volume": 0 if i == 10 else 1,
            }
            for i in range(17)  # Includes the unfinished next bar.
        ]
        review = (
            1,
            0,
            {
                "action": "change",
                "reason": "Try the recovery rule",
                "policy": "rebound",
                "selection": "all",
                "allocation": "equal",
            },
        )
        observations = (
            (0, "calendar", ("2026-11-05", calendar)),
            (0, "selection", [SYMBOL]),
            (0, "feature", (SYMBOL, bars)),
            (1, "book", quote(1)),
            (2, "book", quote(2, bid="1001")),
            (3, "book", quote(1)),  # Older than the uncertain quote.
            (4, "book", quote(4, currency="USD")),
            (5, "book", quote(5)),
            (6, "policy_review", review),
            (7, "book", quote(7, bid="1001")),
            (8, "book", quote(8)),
            (9, "book", quote(9, volume="0")),
            (60, "book", quote(60)),  # Closing auction boundary.
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "journal.sqlite3"
            identity = journal_identity(capital, end_epoch, KR)
            with AdaptiveJournal.open(path, identity, resume=False) as journal:
                run.journal = journal
                journal.append({"kind": "initial", "fx_at": run.fx[1]})
                for sequence, (offset, kind, payload) in enumerate(observations, start=1):
                    run.sequence = sequence
                    entry = {
                        "sequence": sequence,
                        "kind": kind,
                        "payload": payload,
                        "received": stamp(offset),
                    }
                    with patch("tools.run_async_live_validation.now", return_value=stamp(offset)):
                        await run._consume_event(entry, 0)
                    if offset in (7, 9):
                        self.assertIsNone(run._feature_at(SYMBOL, stamp(offset)))
                        for account in run.accounts.values():
                            report = account.summary()["account"]
                            self.assertIsNone(report["bid_liquidation_equity_estimate_krw"])
                            self.assertEqual(report["bid_liquidation_missing_symbols"], [0])
                    run._commit(entry)

            incumbent, candidate = run.accounts.values()
            self.assertEqual(run.book_delivered, {"incumbent": 6, "candidate": 6})
            self.assertEqual(run.counts["integrated_cross_uncertain"], 2)
            self.assertEqual(run.counts["missing_book_depth"], 1)
            self.assertEqual(run.counts["old_snapshot_ignored"], 1)
            self.assertEqual(run.counts["outside_regular_session"], 1)
            self.assertEqual(run.counts["book_currency_mismatch"], 1)
            feature = run.features[SYMBOL]
            assert feature is not None
            self.assertEqual(datetime.fromisoformat(feature.bar_end), BASE)
            self.assertEqual(feature.observations, 16)
            draft = incumbent.intents[0].order_request
            assert draft is not None
            self.assertEqual(
                (draft["symbol"], draft["quantity"], draft["price"]), (SYMBOL, "9", "1000")
            )
            self.assertEqual(incumbent.book.positions[0].quantity, 1)
            self.assertAlmostEqual(incumbent.book.cash, 8999.85)
            self.assertEqual(incumbent.book.cash, candidate.book.cash)
            self.assertEqual(candidate.policy, "rebound")
            self.assertEqual(candidate.owner_policy_versions[0], 0)
            self.assertEqual(candidate.entry_policy_version, 1)
            self.assertTrue(run.status()["comparison"]["inputs_aligned"])
            self.assertEqual(incumbent.summary()["account"]["accounting_currency"], "KRW")
            self.assertIn("fees_krw", incumbent.summary()["account"])
            self.assertNotIn("fees_usd", incumbent.summary()["account"])

            with (
                AdaptiveJournal.open(path, identity, resume=True) as journal,
                patch(
                    "tools.run_async_live_validation.now",
                    side_effect=AssertionError("Recovery must use the saved event clock"),
                ),
            ):
                await recovered.restore(journal)
                self.assertEqual(run._checkpoint(), recovered._checkpoint())
                report = recovered.account.summary()["account"]
                self.assertIsNone(report["bid_liquidation_equity_estimate_krw"])

            output = Path(temp) / "run"
            output.mkdir()
            (output / "events.jsonl").touch()
            (output / "market.jsonl.gz").touch()
            manifest = {**identity, "comparison": run.configurations}
            manifest_path = output / "manifest.json"
            manifest_path.write_text(json.dumps(manifest))
            summary = write_comparison(output, {"state": "complete"}, run)
            assert summary is not None
            self.assertTrue(summary["complete"])
            manifest_path.write_text(json.dumps({**manifest, "currency": "USD"}))
            summary = write_comparison(output, {"state": "complete"}, run)
            assert summary is not None
            self.assertFalse(summary["complete"])
            self.assertFalse(summary["freeze_matches"])

            with (
                self.assertRaises(JournalError),
                AdaptiveJournal.open(path, journal_identity(capital, end_epoch, US), resume=True),
            ):
                self.fail("A US campaign cannot resume a KR journal")

    def test_legacy_kr_profile_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "broker-constrained"):
            Runtime(10_000, Mock(), market=KR)
