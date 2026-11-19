from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from papertrade.jsonio import atomic_json
from tools.adaptive_journal import AdaptiveJournal, read_status
from tools.session_research import ResearchReplay
from tools.upstream_paper_accounts import PROFILES, Account, Comparison, Quote, Signal
from tools.upstream_paper_session import _replay, _summary
from tools.upstream_strategy import Upstream, epoch, iso

SOURCE = os.environ.get("PAPERTRADE_DAY_TRADER_SOURCE")
OPEN = epoch("2026-10-19T09:30:00-04:00")
TRIGGER = epoch("2026-10-19T10:15:00-04:00")
CLOSE = epoch("2026-10-19T16:00:00-04:00")


class Rated(ResearchReplay):
    def __init__(self, directory: Path, published: float) -> None:
        super().__init__(directory)
        self.published = published

    def at(self, tick: float, market: str = "US") -> frozenset[str]:
        return frozenset(("TSLA",)) if self.published <= tick else frozenset()


@unittest.skipUnless(SOURCE, "Set PAPERTRADE_DAY_TRADER_SOURCE to the pinned source checkout")
class UpstreamPaperTests(unittest.TestCase):
    source: Upstream

    @classmethod
    def setUpClass(cls) -> None:
        assert SOURCE is not None
        cls.source = Upstream(Path(SOURCE))

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.out = Path(directory.name)
        self.config = self.source.config(None, 1000)
        self.config_file = self.out / "config.json"
        atomic_json(self.config_file, self.config)
        self.plan = {
            "config": self.config,
            "day": "2026-10-19",
            "created_at": iso(OPEN - 60),
            "start": iso(OPEN),
            "end": iso(CLOSE),
            "entry_cutoff": iso(CLOSE - 30 * 60),
            "symbols": ["TSLA"],
            "premarket_scan": {"universe": 1, "scanned": 1, "failed": 0, "hits": 1},
        }
        fixture = self.source.root / "tests/fixtures/trader-entry/TSLA-2026-10-19-5m.json"
        self.raw = fixture.read_text()

    def comparison(self, published: float = TRIGGER - 20) -> Comparison:
        return Comparison(self.source, self.plan, self.config_file, Rated(self.out, published))

    def scan(
        self,
        cutoff: float = TRIGGER,
        *,
        symbols: tuple[str, ...] = ("TSLA",),
        available: bool = True,
    ) -> dict[str, Any]:
        scanner = self.source.scanner
        series = scanner.parse_chart(self.raw, "TSLA")
        chart = self.source.entry.parse_chart(self.raw, "TSLA")
        chart["data_time"] = int(cutoff + 1)
        stage_one = {symbol: series for symbol in symbols} if available else {}
        confirmed = {
            symbol: {"series": series, "chart": {**chart, "symbol": symbol}, "received": cutoff + 1}
            for symbol in symbols
        }
        hits, counts, failed = scanner.scan_orb(
            stage_one,
            list(symbols),
            date(2026, 10, 19),
            scanner.ts_to_eastern(cutoff),
            15,
            5.0,
            0,
            lambda symbol: confirmed[symbol]["series"],
            scanner.MAX_CONFIRM,
        )
        return {
            "cutoff": cutoff,
            "stage_one": stage_one,
            "confirmed": confirmed,
            "hits": hits,
            "counts": counts,
            "spark_failures": [] if available else list(symbols),
            "confirmation_failures": sorted(failed),
        }

    @staticmethod
    def book(
        source_at: float,
        received: float,
        *,
        bid: float = 364.00,
        ask: float = 364.18,
        depth: int = 50,
    ) -> list[Any]:
        return [
            "TSLA",
            {
                "currency": "USD",
                "timestamp": iso(source_at),
                "bids": [{"price": str(bid), "volume": str(depth)}],
                "asks": [{"price": str(ask), "volume": str(depth)}],
            },
            received,
        ]

    def test_original_signals_and_strictly_later_depth_with_offline_replay(self) -> None:
        comp = self.comparison()
        log_path = self.out / "journal.sqlite3"
        with AdaptiveJournal.open(log_path, self.plan, resume=False) as journal:

            def apply(
                kind: str, at: float, payload: Any = None, *, delay: float = 0
            ) -> dict[str, Any]:
                event = {"kind": kind, "at": iso(at), "payload": payload}
                outputs = comp.process(event)
                comp.complete_decisions(outputs, at + delay)
                journal.append({**event, "completed_at": iso(at + delay), "outputs": outputs})
                return outputs

            apply("capture_started", OPEN - 10)
            scan = apply("scan", TRIGGER + 2, self.scan())
            entry = scan["public"]["new_signals"][0]
            self.assertEqual(
                (entry["entry"]["price"], entry["plan"]["stop"], entry["plan"]["target"]),
                (364.18, 358.32, 375.90),
            )
            decision = apply("book", TRIGGER + 3, self.book(TRIGGER + 3, TRIGGER + 3), delay=2)
            for profile in PROFILES:
                order = decision["accounts"][profile][0]
                self.assertEqual(order["order"]["qty"], "2")
                self.assertEqual(order["intent_at"], iso(TRIGGER + 5))
            # This arrives after approval, but its source is from during the risk computation.
            during = apply("book", TRIGGER + 6, self.book(TRIGGER + 4, TRIGGER + 6))
            self.assertTrue(all(not actions for actions in during["accounts"].values()))
            entry_fill = apply("book", TRIGGER + 8, self.book(TRIGGER + 7, TRIGGER + 8, depth=1))
            self.assertEqual(entry_fill["accounts"][PROFILES[0]][0]["quantity"], 1)
            self.assertEqual(entry_fill["accounts"][PROFILES[0]][0]["unfilled"], 1)
            self.assertIsNot(
                comp.accounts[PROFILES[0]].positions["TSLA"],
                comp.accounts[PROFILES[1]].positions["TSLA"],
            )
            cheap_bid = apply(
                "book", TRIGGER + 11, self.book(TRIGGER + 10, TRIGGER + 11, bid=357, ask=359)
            )
            self.assertTrue(all(not actions for actions in cheap_bid["accounts"].values()))
            drops = {
                "received": TRIGGER + 12,
                "rows": [
                    {
                        "symbol": "TSLA",
                        "currency": "USD",
                        "timestamp": iso(TRIGGER + 11),
                        "lastPrice": "358.32",
                    }
                ],
            }
            sell = apply("trades", TRIGGER + 12, drops, delay=2)
            self.assertEqual(sell["accounts"][PROFILES[0]][0]["reason"], "plan-stop")
            prematurely = apply(
                "book", TRIGGER + 15, self.book(TRIGGER + 13, TRIGGER + 15, bid=356, ask=358)
            )
            self.assertTrue(all(not actions for actions in prematurely["accounts"].values()))
            sell_fill = apply(
                "book", TRIGGER + 17, self.book(TRIGGER + 16, TRIGGER + 17, bid=355, ask=358)
            )
            for profile in PROFILES:
                fill = sell_fill["accounts"][profile][0]
                self.assertEqual(
                    (fill["quantity"], fill["price"], fill["status"]), (1, 355, "filled")
                )
            apply("close", CLOSE)
            sealed = _summary(comp, journal)
            self.assertEqual(sealed["accounts"][PROFILES[0]], sealed["accounts"][PROFILES[1]])
            self.assertLess(float(sealed["accounts"][PROFILES[0]]["realized_usd"]), -9)
            journal.complete()
        chain = read_status(log_path)["hash"]
        replay = self.comparison()
        with AdaptiveJournal.read_completed(log_path, chain) as journal:
            _replay(replay, journal)
            self.assertEqual(sealed, _summary(replay, journal))

    def test_late_rating_and_expired_source_bar_never_open_position(self) -> None:
        comp = self.comparison(published=TRIGGER + 1)
        comp.process({"kind": "capture_started", "at": iso(OPEN - 1)})
        signal = comp.process({"kind": "scan", "at": iso(TRIGGER + 2), "payload": self.scan()})
        self.assertEqual(
            signal["accounts"][PROFILES[1]][0]["reason"], "no_favorable_publication_at_trigger"
        )
        late = comp.process(
            {
                "kind": "book",
                "at": iso(TRIGGER + 301),
                "payload": self.book(TRIGGER + 300, TRIGGER + 301),
            }
        )
        self.assertEqual(late["accounts"][PROFILES[0]][0]["status"], "expired")
        self.assertTrue(all(not account.positions for account in comp.accounts.values()))
        comp = self.comparison()
        comp.process({"kind": "capture_started", "at": iso(OPEN - 1)})
        delayed = comp.process(
            {"kind": "scan", "at": iso(TRIGGER + 302), "payload": self.scan(TRIGGER + 300)}
        )
        self.assertEqual(
            delayed["accounts"][PROFILES[0]][0]["reason"], "source_trigger_older_than_current_bar"
        )

    def test_contemporaneous_entry_books_follow_native_orb_order(self) -> None:
        self.plan["symbols"] = ["TSLA", "AAPL"]
        comp = self.comparison()
        comp.process({"kind": "capture_started", "at": iso(OPEN - 1)})
        comp.process(
            {"kind": "scan", "at": iso(TRIGGER + 2), "payload": self.scan(symbols=("TSLA", "AAPL"))}
        )
        self.assertEqual(comp.active_symbols(), ["AAPL", "TSLA"])

    def test_missing_market_input_differs_from_observed_no_signal(self) -> None:
        cut = OPEN + 20 * 60
        for available in (False, True):
            comp = self.comparison()
            comp.process({"kind": "capture_started", "at": iso(OPEN - 1)})
            comp.process(
                {
                    "kind": "scan",
                    "at": iso(cut + 2),
                    "payload": self.scan(
                        cut,
                        available=available,
                    ),
                }
            )
            comp.process({"kind": "close", "at": iso(CLOSE)})
            with AdaptiveJournal.open(
                self.out / f"quality-{available}.sqlite3",
                self.plan,
                resume=False,
            ) as journal:
                summary = _summary(comp, journal)
            self.assertEqual(
                summary["comparison_quality"], "observed" if available else "incomplete"
            )
            self.assertFalse(summary["counts"].get("native_entry_signals"))
            if not available:
                self.assertIn("no_usable_native_scan_inputs", summary["limitations"])

    def test_premarket_and_research_failures_remain_visible_with_no_signal(self) -> None:
        premarket = {"universe": 2, "scanned": 1, "failed": 1, "hits": 1}
        self.plan["premarket_scan"] = premarket
        comp = self.comparison()
        comp.process({"kind": "capture_started", "at": iso(OPEN - 1)})
        comp.process(
            {
                "kind": "research_unavailable",
                "at": iso(OPEN),
                "payload": {"code": "codex_login_or_binary_unavailable"},
            }
        )
        cut = OPEN + 20 * 60
        comp.process({"kind": "scan", "at": iso(cut + 2), "payload": self.scan(cut)})
        comp.process({"kind": "close", "at": iso(CLOSE)})
        with AdaptiveJournal.open(self.out / "failed.sqlite3", self.plan, resume=False) as journal:
            summary = _summary(comp, journal)
        self.assertEqual(summary["premarket_scan"], premarket)
        self.assertEqual(summary["comparison_quality"], "incomplete")
        self.assertEqual(
            summary["limitations"],
            [
                "some_native_premarket_scan_requests_failed",
                "research_unavailable_during_capture",
            ],
        )

    def test_subcent_buy_preserves_cash_and_native_cash_buffer(self) -> None:
        for buffer, capital in ((0, 10.01), (10, 1000)):
            for initial_ask, affordable, status in (
                (10.004, "10.01", "blocked"),
                (10, "10.01", "cancelled"),
                (10.004, "10.014004", "filled"),
            ):
                with self.subTest(buffer=buffer, initial_ask=initial_ask, status=status):
                    config = {
                        **self.config,
                        "cash_buffer_pct": buffer,
                        "budget": capital,
                        "max_position_pct": 100,
                    }
                    account = Account(self.source, config, "2026-10-19")
                    cash_buffer = Decimal("100" if buffer else "0")
                    starting_cash = cash_buffer + Decimal(affordable)
                    account.cash = starting_cash
                    account.candidates["TSLA"] = Signal(
                        TRIGGER,
                        TRIGGER,
                        {
                            "symbol": "TSLA",
                            "date": "2026-10-19",
                            "entry": 10,
                            "stop": 9,
                            "target": 12,
                        },
                        TRIGGER + 300,
                    )
                    initial = Quote(TRIGGER + 1, TRIGGER + 1, TRIGGER + 1, 10, 1, initial_ask, 1)
                    decision = account.observe_book("TSLA", initial, {"TSLA": initial})[0]
                    if status == "blocked":
                        self.assertEqual(decision["status"], status)
                    else:
                        self.assertEqual(decision["status"], "approved")
                        self.assertEqual(decision["reserved_usd"], affordable)
                        later = Quote(TRIGGER + 2, TRIGGER + 2, TRIGGER + 2, 10, 1, 10.004, 1)
                        confirmation = account.observe_book("TSLA", later, {"TSLA": later})[0]
                        self.assertEqual(confirmation["status"], status)
                    self.assertEqual(
                        account.cash, cash_buffer if status == "filled" else starting_cash
                    )


if __name__ == "__main__":
    unittest.main()
