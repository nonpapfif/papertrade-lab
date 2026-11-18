"""Incremental, replayable three-account comparison on one ordered public feed."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from collections import Counter
from collections.abc import Callable
from collections.abc import Set as AbstractSet
from dataclasses import replace
from typing import Any

from tools.async_layered_paper import AsyncLayeredPaper, seconds
from tools.kr_broker_profile import eligible as eligible_kr_stock
from tools.layered_paper import Feature, assign, eligible_stock, feature_from_bars
from tools.paper_market import KR, US

ACCOUNT_POLICIES = {
    "existing_selection": "trend",
    "research_selection": "trend",
    "research_alternative_strategy": "rebound",
}
ENTRY_RATINGS = frozenset(("Buy", "Overweight"))
RATINGS = ENTRY_RATINGS | {"Hold", "Underweight", "Sell", "REVIEW"}


class SessionAccounts:
    """Consume committed publications once; restore by replaying them on a fresh instance.

    Each event has id, sequence, received_at and kind. Receipt time is the ordered
    publication clock; source_received_at retains the actual receipt clock and
    book.timestamp retains the provider's observation clock.
    Selection events carry a collector-verified eligible symbols list. Optional
    stock_info events allow callers to feed eligibility evidence directly instead.
    Candle timestamps denote provider minute ends. Research changes entries only.
    Session bounds must be obtained from PaperMarket.session and frozen by the caller.
    """

    def __init__(
        self,
        *,
        market: str,
        capital: float,
        opened_at: str,
        closes_at: str,
        selection_at: Callable[[float], AbstractSet[str]] | None = None,
    ) -> None:
        if market not in ("US", "KR"):
            raise ValueError("Only verified KR and US whole-share profiles are supported")
        self.market = US if market == "US" else KR
        self.opened_at, self.closes_at = opened_at, closes_at
        self.open_epoch, self.close_epoch = seconds(opened_at), seconds(closes_at)
        if self.open_epoch >= self.close_epoch:
            raise ValueError("A positive continuous-session window is required")
        self.capital = capital
        self.selection_at = selection_at
        self.accounts = {
            name: AsyncLayeredPaper(
                capital,
                policy=policy,
                market=self.market,
                fees={"commission_exemption_usd": 0.0} if market == "US" else None,
            )
            for name, policy in ACCOUNT_POLICIES.items()
        }
        self.ids: dict[str, int] = {}
        self.eligible: set[str] = set()
        self.features: dict[str, Feature | None] = {}
        self.latest: dict[str, dict[str, Any]] = {}
        self.source_times: dict[str, float] = {}
        self.invalid_books: set[str] = set()
        self.fx: dict[str, Any] = {"fx_provider_validity_required": self.market is US}
        self.ratings: dict[str, str] = {}
        self.research_published = math.inf
        self.research_expires = -math.inf
        self.research_provenance: list[Any] = []
        self.last_sequence = 0
        self.last_received_at: str | None = None
        self.counts: Counter[str] = Counter()
        self._seen: dict[str, str] = {}
        self._input_digest = hashlib.sha256()
        self._session_closed = False

    def identifier(self, symbol: str) -> int:
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("A nonempty provider symbol is required")
        return self.ids.setdefault(symbol, len(self.ids))

    def protected(self) -> set[str]:
        """Retain every held or pending symbol when the selection universe changes."""
        protected = {
            symbol
            for account in self.accounts.values()
            for symbol in (*account.book.positions, *account.pending)
        }
        return {symbol for symbol, identifier in self.ids.items() if identifier in protected}

    def signal_family(self, symbol: str, tick: float) -> str | None:
        """Return a fresh eligible trend/rebound signal from the observed book midpoint."""
        if (
            self._session_closed
            or symbol not in self.eligible
            or not self.open_epoch <= tick < self.close_epoch
        ):
            return None
        feature = self._feature(symbol, tick)
        if feature is None:
            return None
        return next((family for family in ("trend", "rebound") if assign(feature, family)), None)

    def _feature(self, symbol: str, tick: float) -> Feature | None:
        feature, book = self.features.get(symbol), self.latest.get(symbol)
        if feature is None or book is None:
            return None
        if seconds(feature.available_at) > tick:
            return None
        if not 0 <= tick - seconds(book["timestamp"]) <= 60:
            return None
        if not 0 <= tick - seconds(feature.bar_end) <= 90:
            return None
        price = (
            min(float(row["price"]) for row in book["asks"])
            + max(float(row["price"]) for row in book["bids"])
        ) / 2
        return replace(
            feature,
            price=price,
            r5=price / (feature.price / (1 + feature.r5)) - 1,
            r15=price / (feature.price / (1 + feature.r15)) - 1,
        )

    def _admitted(self, tick: float) -> set[str]:
        if self.selection_at is not None:
            return self.eligible.intersection(self.selection_at(tick))
        if not self.research_published <= tick < self.research_expires:
            return set()
        return self.eligible.intersection(
            symbol for symbol, rating in self.ratings.items() if rating in ENTRY_RATINGS
        )

    def _usable_book(self, book: dict[str, Any], tick: float) -> bool:
        if book.get("currency") != self.market.currency:
            return False
        if not self.open_epoch <= seconds(book["timestamp"]) < self.close_epoch:
            return False
        if not 0 <= tick - seconds(book["timestamp"]) <= 60:
            return False
        levels = {
            side: [(float(row["price"]), float(row["volume"])) for row in book[side]]
            for side in ("bids", "asks")
        }
        return (
            all(levels.values())
            and all(
                math.isfinite(price)
                and price > 0
                and math.isfinite(volume)
                and volume >= 0
                and self.market.valid_quote_level(price, volume)
                for rows in levels.values()
                for price, volume in rows
            )
            and any(volume >= 1 for _, volume in levels["bids"])
            and any(volume >= 1 for _, volume in levels["asks"])
            and max(price for price, volume in levels["bids"] if volume >= 1)
            <= min(price for price, volume in levels["asks"] if volume >= 1)
        )

    async def consume(self, event: dict[str, Any]) -> bool:
        """Apply one immutable publication; identical duplicate IDs are harmless."""
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
        event_id = event["id"]
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("A nonempty string event ID is required")
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        if event_id in self._seen:
            if self._seen[event_id] != digest:
                raise ValueError("An event ID was reused with different content")
            return False
        sequence, at, kind = event["sequence"], event["received_at"], event["kind"]
        tick = seconds(at)
        source_received_at = event.get("source_received_at", at)
        if seconds(source_received_at) > tick:
            raise ValueError("A publication cannot precede its source receipt")
        if (
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence <= self.last_sequence
        ):
            raise ValueError("Publication sequence must increase")
        if self.last_received_at is not None and tick < seconds(self.last_received_at):
            raise ValueError("Publication receipt time cannot regress")
        if kind == "selection":
            symbols = event["symbols"]
            if not isinstance(symbols, list):
                raise ValueError("Selection requires a verified eligible symbol list")
            for symbol in symbols:
                self.identifier(symbol)
            self.eligible = set(symbols)
        elif kind == "stock_info":
            row = event["row"]
            symbol = row["symbol"]
            self.identifier(symbol)
            eligible = (
                eligible_stock(row)
                if self.market is US
                else (
                    eligible_kr_stock(row)
                    and event.get("listing_exchange", row.get("exchange")) in ("KOSPI", "KOSDAQ")
                )
            )
            if eligible:
                self.eligible.add(symbol)
            else:
                self.eligible.discard(symbol)
        elif kind == "candles":
            symbol = event["symbol"]
            rows = [
                {**row, "available_at": source_received_at}
                for row in event["result"]["candles"]
                if self.open_epoch < seconds(row["timestamp"]) <= self.close_epoch
            ]
            self.features[symbol] = feature_from_bars(
                self.identifier(symbol),
                rows,
                source_received_at,
                timestamp_is_end=True,
            )
        elif kind == "fx":
            result = event["result"]
            rate = float(result["midRate"])
            if (
                not math.isfinite(rate)
                or rate <= 0
                or result.get("baseCurrency", "USD") != "USD"
                or result.get("quoteCurrency", "KRW") != "KRW"
            ):
                raise ValueError("Expected a positive USD/KRW reporting rate")
            self.fx = {
                "fx": rate,
                "fx_at": source_received_at,
                "fx_valid_from": result.get("validFrom"),
                "fx_valid_until": result.get("validUntil"),
                "fx_provider_validity_required": self.market is US,
            }
        elif kind == "research":
            published, expires = seconds(event["published_at"]), seconds(event["expires_at"])
            ratings = event["ratings"]
            if (
                not isinstance(ratings, dict)
                or any(rating not in RATINGS for rating in ratings.values())
                or published > tick
                or expires <= tick
            ):
                raise ValueError("Research requires published, unexpired known ratings")
            self.ratings = copy.deepcopy(ratings)
            self.research_published, self.research_expires = tick, expires
            self.research_provenance = copy.deepcopy(event.get("provenance", []))
        elif kind == "orderbook":
            await self._book(event, tick)
        elif kind != "clock":
            raise ValueError("Unsupported session event kind")
        # Every publication advances expiry, including a quiet symbol's reservations.
        for account in self.accounts.values():
            await account.advance(self.closes_at if tick >= self.close_epoch else at)
        self.last_sequence, self.last_received_at = sequence, at
        self._seen[event_id] = digest
        self._input_digest.update(encoded.encode() + b"\n")
        self.counts[kind] += 1
        return True

    async def _book(self, event: dict[str, Any], tick: float) -> None:
        symbol, at, book = event["symbol"], event["received_at"], event["book"]
        identifier = self.identifier(symbol)
        received_at = event.get("source_received_at", at)
        received = seconds(received_at)
        if self._session_closed or not all(
            self.open_epoch <= stamp < self.close_epoch for stamp in (tick, received)
        ):
            self.counts["outside_session"] += 1
            return
        book = {
            **book,
            **{
                side: [
                    row
                    for row in book[side]
                    if float(row["volume"]) != 0
                    and (self.market is KR or not 0 < float(row["volume"]) < 1)
                ]
                for side in ("bids", "asks")
            },
        }
        source = seconds(book["timestamp"])
        if source < self.source_times.get(symbol, -math.inf):
            self.counts["regressed_books"] += 1
            return
        if source > received or not self._usable_book(book, tick):
            self.latest.pop(symbol, None)
            self.invalid_books.add(symbol)
            if 0 <= tick - source <= 60:
                self.source_times[symbol] = source
            self.counts["unusable_books"] += 1
            return
        self.source_times[symbol] = source
        self.invalid_books.discard(symbol)
        self.latest[symbol] = copy.deepcopy(book)
        current = self._feature(symbol, tick)
        admitted = self._admitted(tick)
        features: dict[str, Feature | None] = {symbol: current}
        for name, account in self.accounts.items():
            selected = self.eligible if name == "existing_selection" else admitted
            candidate_count = 1
            if (
                current is not None
                and symbol in selected
                and identifier not in account.pending
                and identifier not in account.book.positions
                and assign(current, account.policy)
            ):
                candidate_count = 0
                for candidate in selected:
                    candidate_id = self.ids[candidate]
                    if candidate_id in account.pending or candidate_id in account.book.positions:
                        continue
                    if candidate not in features:
                        features[candidate] = self._feature(candidate, tick)
                    feature = features[candidate]
                    if feature is not None and assign(feature, account.policy):
                        candidate_count += 1
            await account.submit(
                {
                    "id": event["id"],
                    "symbol": identifier,
                    "provider_symbol": symbol,
                    "received_at": at,
                    "source_received_at": received_at,
                    "book": book,
                    "feature": current,
                    "eligible": symbol in selected,
                    "entries": True,
                    "candidate_count": max(1, candidate_count),
                    **self.fx,
                }
            )
        self.counts["accepted_books"] += 1

    def summary(self, *, at: str, stream_state: str = "running") -> dict[str, Any]:
        """Mark at the explicit reporting cut; never call a last quote the close."""
        cutoff = seconds(at)
        if (
            self.last_received_at is not None
            and cutoff < seconds(self.last_received_at)
            and cutoff < self.close_epoch
        ):
            raise ValueError("Cannot report before the committed publication cut")
        if cutoff >= self.close_epoch:
            at = self.closes_at
        rows: dict[str, dict[str, Any]] = {
            name: {
                "strategy": account.policy,
                "uses_research_selection": name != "existing_selection",
                "fee_schedule": copy.deepcopy(account.context.schedule),
                "summary": account.summary(at),
            }
            for name, account in self.accounts.items()
        }
        for name, account in self.accounts.items():
            exclusions = [
                {"symbol": self.ids[symbol], "reason": "latest_publication_quote_unusable"}
                for symbol in sorted(self.invalid_books)
                if self.ids[symbol] in account.book.positions
            ]
            if exclusions:
                row = rows[name]["summary"]
                row["comparison_eligible"] = row["account"]["comparison_eligible"] = False
                row["comparison_valid_until"] = row["account"]["comparison_valid_until"] = None
                row["comparison_exclusions"].extend(exclusions)
                row["account"]["comparison_exclusions"] = copy.deepcopy(
                    row["comparison_exclusions"]
                )
                row["account"][
                    f"bid_liquidation_equity_estimate_{self.market.currency.lower()}"
                ] = None
                if self.market is US:
                    row["account"]["after_tax_bid_liquidation_estimate_usd"] = None
        complete = stream_state == "complete" and cutoff >= self.close_epoch
        healthy = all(row["summary"]["comparison_eligible"] for row in rows.values())
        traded = any(account.book.opened for account in self.accounts.values())
        ranking_eligible = complete and healthy and traded
        return {
            "mode": "online_shared_publication_comparison",
            "market": self.market.code,
            "currency": self.market.currency,
            "capital_per_account": self.capital,
            "calendar_open": self.opened_at,
            "calendar_close": self.closes_at,
            "reported_as_of": at,
            "last_publication_at": self.last_received_at,
            "last_sequence": self.last_sequence,
            "stream_state": stream_state,
            "session_complete": complete,
            "ranking_eligible": ranking_eligible,
            "evaluation_status": "observed_short_sample" if ranking_eligible else "inconclusive",
            "ranking": sorted(
                rows, key=lambda name: rows[name]["summary"]["account"]["return_pct"], reverse=True
            )
            if ranking_eligible
            else [],
            "symbol_ids": dict(self.ids),
            "eligible_symbols": sorted(self.eligible),
            "protected_symbols": sorted(self.protected()),
            "publication_counts": dict(self.counts),
            "common_input_sha256": self._input_digest.hexdigest(),
            "research_provenance": copy.deepcopy(self.research_provenance),
            "accounts": rows,
            "real_orders": False,
            "limitations": [
                "Whole-share, native-currency paper funding; no broker account or order access",
                "Fees and depth-confirmation fills are labelled scenarios, not broker verification",
                "Midpoint equity is a mark, not executed liquidation or a closing auction price",
                "Short samples and descriptive rankings do not establish strategy promotion",
                "USD/KRW rates support reporting only; missing or stale FX stays unresolved",
            ],
        }

    async def close(self) -> None:
        for account in self.accounts.values():
            await account.close()

    async def close_session(self) -> None:
        """Release all DAY-intent reservations at the frozen close without liquidation."""
        if self._session_closed:
            return
        for account in self.accounts.values():
            await account.cancel_pending(self.closes_at)
        self._session_closed = True
