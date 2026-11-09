"""Finite market-data-only integration campaign for the asynchronous paper engine."""

from __future__ import annotations

import argparse
import asyncio
import copy
import gzip
import hashlib
import json
import math
import os
import signal
import time
from collections import Counter
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import asdict, replace
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from papertrade.api import ApiError, MarketClient, private_json
from papertrade.core import SafetyError, Settings
from tools.adaptive_journal import AdaptiveJournal, JournalError, digest, encode
from tools.async_layered_paper import AsyncLayeredPaper, ProcessingClock, seconds
from tools.kr_broker_profile import eligible as eligible_kr_stock
from tools.layered_paper import FAMILIES, Feature, assign, eligible_stock, feature_from_bars
from tools.live_feed import LiveFeed
from tools.paper_market import KR, US, PaperMarket
from tools.probe_toss_websocket import (
    KR_SYMBOL,
    ROOT,
    URL,
    CachedTokenClient,
    declaration,
    error_details,
    now,
    read_spec,
)
from tools.shared_feed_slots import SharedFeedSlots, validate_session_seeds
from tools.slot_control import SlotControl
from tools.us_adaptive_ai import ASTRA, ENTRY_OPTIONS, SOL, propose

LEGACY_SOURCES = (
    "tools/run_async_live_validation.py",
    "tools/async_layered_paper.py",
    "tools/layered_paper.py",
    "tools/live_execution.py",
    "papertrade/api.py",
    "tools/paper_market.py",
    "tools/toss_order_contract.py",
)
SOURCES = (
    *LEGACY_SOURCES,
    "papertrade/core.py",
    "tools/live_feed.py",
    "tools/probe_toss_websocket.py",
    "tools/search_portfolios.py",
    "tools/week_hypotheses.py",
)

ADAPTIVE_SOURCES = (
    *SOURCES,
    "tools/us_adaptive_ai.py",
    "tools/adaptive_journal.py",
    "tools/kr_comparison_ai.py",
    "papertrade/ai.py",
)
SLOT_SOURCES = (
    "tools/shared_feed_slots.py",
    "tools/slot_control.py",
)
KR_SOURCES = ("tools/kr_broker_profile.py",)

MARKET_CAPABILITY_SNAPSHOT = {
    "source": "https://openapi.tossinvest.com/openapi-docs/latest/openapi.json",
    "retrieved": "2026-11-04",
    "version": "1.2.28",
    "sha256": "18fdc96fdf632e4869c765ed0cb169049c5310a1c48e10b40e47a71f52aa7a45",
}
KR_COST_EVIDENCE = {
    "as_of": "2026-11-04",
    "status": "unverified_toss_api_fee_scenario",
    "commission": "0.015% commission assumed on each side, unrounded",
    "sale_tax": "0.20% ordinary KOSPI/KOSDAQ stock sale tax, unrounded",
    "settlement": "Simulated sale proceeds available immediately; broker buying power unverified",
}
MARKETS = {"US": US, "KR": KR}
US_COST_EVIDENCE = {
    "as_of": "2026-11-04",
    "status": "unverified_toss_api_fee_scenario",
    "commission": "0.1% app-disclosed rate assumed for API, no exemption applied",
    "sec": "Regulatory rate assumed passed through with modeled cent rounding and minimum",
    "taf": "Zero-rate regulatory scenario for October through December 2026",
    "settlement": "Simulated sale proceeds available immediately; broker buying power unverified",
    "fx": "Market-data conversion for reporting only; USD-funded, no simulated currency exchange",
}


def us_date(at: str) -> str:
    return US.trading_date(datetime.fromisoformat(at))


def us_regular_window(
    calendar: dict, day: str, market: PaperMarket = US
) -> tuple[float, float] | None:
    try:
        start, end = market.session(calendar, day)
        return start.timestamp(), end.timestamp()
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def rank_candidates(
    rows: list[dict], previous: dict[str, float], at: str, capital: float, currency: str = "USD"
) -> tuple[list[str], dict[str, float]]:
    fresh = {}
    for row in rows:
        try:
            price = float(row["lastPrice"])
            age = seconds(at) - seconds(row["timestamp"])
            if (
                row["currency"] == currency
                and math.isfinite(price)
                and price > 0
                and 0 <= age <= 90
            ):
                fresh[row["symbol"]] = price
        except (KeyError, ValueError, TypeError):
            continue
    scored = [
        (abs(price / previous[s] - 1), s)
        for s, price in fresh.items()
        if s in previous and price <= capital and price != previous[s]
    ]
    return [s for _, s in sorted(scored, key=lambda r: (-r[0], r[1]))], fresh


def subscription_set(candidates: list[str], protected: set[str]) -> list[str]:
    if len(protected) > 50:
        raise ValueError("Protected positions exceed one connection capacity")
    return sorted(protected) + [s for s in candidates if s not in protected][: 50 - len(protected)]


def latency_summary(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "p50": ordered[int((len(ordered) - 1) * 0.5)],
        "p95": ordered[int((len(ordered) - 1) * 0.95)],
        "max": ordered[-1],
    }


class CampaignCutoff(Exception):
    """The absolute adaptive deadline prevented another REST request."""


class DeadlineMarketClient(CachedTokenClient):
    def __init__(
        self, settings: Settings, end_epoch: float, *, recurring: bool = False
    ) -> None:
        super().__init__(settings)
        self.end_epoch = end_epoch
        self.recurring = recurring

    def access_token(self) -> str:
        return self.recovery_token() if self.recurring else super().access_token()

    def _invalidate_token(self) -> None:
        if self.recurring:
            MarketClient._invalidate_token(self)
        else:
            super()._invalidate_token()

    def _reserve(self, group: str) -> None:
        if time.time() >= self.end_epoch:
            raise CampaignCutoff
        super()._reserve(group)
        if time.time() >= self.end_epoch:
            raise CampaignCutoff

    def recovery_token(self) -> str:
        if time.time() >= self.end_epoch:
            raise CampaignCutoff
        token = MarketClient.access_token(self)
        if time.time() >= self.end_epoch:
            raise CampaignCutoff
        return token


class Runtime:
    def __init__(
        self,
        capital: float,
        record: Any,
        *,
        candidate_policy: str | None = None,
        profile: str = "legacy",
        adaptive: bool = False,
        end_epoch: float | None = None,
        market: PaperMarket = US,
    ) -> None:
        if profile not in ("legacy", "broker-constrained"):
            raise ValueError("Unknown validation profile")
        self.constrained = profile == "broker-constrained"
        if market is KR and not self.constrained:
            raise ValueError("KR requires the broker-constrained profile")
        self.market = market
        self.adaptive = adaptive
        if adaptive and (not self.constrained or candidate_policy is not None or end_epoch is None):
            raise ValueError("Adaptive experiment requires a constrained profile and fixed end")
        self.end_epoch = end_epoch
        self.journal: AdaptiveJournal | None = None
        self.review_settings: Settings | None = None
        self.shared_watch: set[str] | None = None
        self.shared_review_call: Callable[[str, int], None] | None = None
        self.shared_review_publish: Callable[[Any], None] | None = None
        self.review_payload: dict[str, Any] | None = None
        self.review_task: asyncio.Task | None = None
        self.review_calls: Counter = Counter()
        self.reviewed_families: set[str] = set()
        self.allocation_blocked = False
        self.reviewed_fills = dict.fromkeys(("incumbent", "candidate"), 0)
        self.policy_history: list[dict[str, Any]] = []
        self.regular_window: tuple[float, float] | None = None
        if self.constrained and candidate_policy is not None:
            raise ValueError("The broker-constrained profile runs a single trend baseline")
        constrained_fees = {"commission_exemption_usd": 0.0} if market is US else None
        self.account = (
            AsyncLayeredPaper(capital, policy="trend", fees=constrained_fees, market=market)
            if self.constrained
            else AsyncLayeredPaper(capital)
        )
        self.accounts = {"incumbent": self.account}
        if adaptive:
            self.accounts["candidate"] = AsyncLayeredPaper(
                capital, policy="trend", fees=constrained_fees, market=market
            )
        elif candidate_policy is not None:
            self.accounts["candidate"] = AsyncLayeredPaper(capital, policy=candidate_policy)
        self.configurations = {
            name: {
                "capital": capital,
                "policy": account.policy,
                "selection": account.selection,
                "allocation": account.allocation,
                "delay_seconds": account.delay,
                "fee_schedule": copy.deepcopy(account.context.schedule),
            }
            for name, account in self.accounts.items()
        }
        self.delivered = dict.fromkeys(self.accounts, 0)
        self.book_delivered = dict.fromkeys(self.accounts, 0)
        self.delivery_hashes = {name: hashlib.sha256() for name in self.accounts}
        self.sequence = 0
        self.processing_clock = ProcessingClock()
        self.record = record
        self.ids: dict[str, int] = {}
        self.features: dict[str, Feature | None] = {}
        self.candidates: list[str] = []
        self.desired: list[str] = []
        self.books: dict[str, dict] = {}
        self.unactionable_books: set[str] = set()
        self.fx: tuple[float, str, str | None, str | None] = (0, now(), None, None)
        self.queue: asyncio.Queue = asyncio.Queue()
        self.counts: Counter = Counter()
        self.queue_delays: list[float] = []
        self.processing_times: list[float] = []
        self.max_queue = 0
        self.symbol_delays: dict[str, list[float]] = {}

    def identifier(self, symbol: str) -> int:
        if symbol not in self.ids:
            self.ids[symbol] = len(self.ids)
        return self.ids[symbol]

    def protected(self) -> set[str]:
        ids = {
            symbol
            for account in self.accounts.values()
            for symbol in set(account.pending) | set(account.book.positions)
        }
        return {s for s, i in self.ids.items() if i in ids}

    def inherit_accounts(self, source: Runtime, account_names: dict[str, str]) -> None:
        """Initialize an empty runtime from a committed observation cut."""
        for field in (
            "ids",
            "features",
            "candidates",
            "desired",
            "books",
            "unactionable_books",
            "fx",
            "counts",
            "processing_clock",
            "regular_window",
        ):
            setattr(self, field, copy.deepcopy(getattr(source, field)))
        self.accounts = {
            name: source.accounts[prior].fork() for name, prior in account_names.items()
        }
        self.account = self.accounts["incumbent"]
        self.configurations = {
            name: copy.deepcopy(source.configurations[prior])
            for name, prior in account_names.items()
        }
        self.delivered = {name: source.delivered[prior] for name, prior in account_names.items()}
        self.book_delivered = {
            name: source.book_delivered[prior] for name, prior in account_names.items()
        }
        self.delivery_hashes = {
            name: source.delivery_hashes[prior].copy() for name, prior in account_names.items()
        }

    def publish(self, kind: str, payload: Any) -> None:
        self.sequence += 1
        self.queue.put_nowait(
            (self.sequence, kind, copy.deepcopy(payload), now(), time.monotonic())
        )
        self.max_queue = max(self.max_queue, self.queue.qsize())

    def _feature_at(self, symbol: str, at: str) -> Feature | None:
        feature, book = self.features.get(symbol), self.books.get(symbol)
        tick = seconds(at)
        if (
            feature is None
            or book is None
            or book.get("currency") != self.market.currency
            or symbol in self.unactionable_books
            or seconds(feature.available_at) > tick
            or not 0 <= tick - seconds(feature.bar_end) <= 90
            or not 0 <= tick - seconds(book["timestamp"]) <= 60
        ):
            return None
        if self.constrained:
            return feature
        price = (
            min(float(r["price"]) for r in book["asks"])
            + max(float(r["price"]) for r in book["bids"])
        ) / 2
        return replace(
            feature,
            price=price,
            r5=price / (feature.price / (1 + feature.r5)) - 1,
            r15=price / (feature.price / (1 + feature.r15)) - 1,
        )

    def _record_delivery(self, name: str, observation: bytes) -> None:
        self.delivered[name] += 1
        self.delivery_hashes[name].update(observation)

    async def close(self) -> None:
        if self.review_task is not None:
            self.review_task.cancel()
            await asyncio.gather(self.review_task, return_exceptions=True)
            self.review_task = None
        for account in self.accounts.values():
            await account.close()

    def _entry_configuration(self) -> dict[str, str]:
        account = self.accounts["candidate"]
        return {key: getattr(account, key) for key in ENTRY_OPTIONS}

    def _consider_review(self, sequence: int, at: str, prepared: dict[str, Feature | None]) -> None:
        if (
            self.review_settings is None
            or self.end_epoch is None
            or seconds(at) >= self.end_epoch
            or self.review_task is not None
            or self.review_payload is not None
        ):
            return
        candidate = self.accounts["candidate"]
        excluded = set(candidate.pending) | set(candidate.book.positions)
        observed = {
            symbol: feature
            for symbol, feature in prepared.items()
            if feature is not None and symbol in self.entry_watch
        }
        eligible = {
            symbol: feature
            for symbol, feature in observed.items()
            if feature.symbol not in excluded
            and (
                candidate.selection != "active"
                or abs(feature.r5) >= 0.001
                or abs(feature.r15) >= 0.003
            )
        }
        one_share_costs = {}
        for symbol in eligible:
            asks = [
                float(row["price"])
                for row in self.books[symbol]["asks"]
                if float(row["volume"]) >= 1
            ]
            if asks:
                limit = self.market.limit_price(min(asks), "BUY")
                if limit > 0:
                    one_share_costs[symbol] = candidate.book.entry_cost(1, limit)

        opportunities: list[dict[str, Any]] = []
        allocation_became_blocked = False
        cash = candidate.available_cash
        for family in ENTRY_OPTIONS["policy"]:
            signals = {
                symbol: feature
                for symbol, feature in eligible.items()
                if assign(feature, family) is not None
            }
            if not signals:
                continue
            budget = cash / len(signals) if candidate.allocation == "equal" else cash
            affordable = [
                symbol for symbol in signals if one_share_costs.get(symbol, math.inf) <= budget
            ]
            if family == candidate.policy and candidate.allocation == "equal":
                if affordable:
                    self.allocation_blocked = False
                elif any(one_share_costs.get(symbol, math.inf) <= cash for symbol in signals):
                    allocation_became_blocked = not self.allocation_blocked
                    self.allocation_blocked = True
            if affordable and family not in self.reviewed_families:
                self.reviewed_families.add(family)
                opportunities.extend(
                    {
                        "symbol": symbol,
                        "family": family,
                        "observed_at": at,
                        "feature": asdict(signals[symbol]),
                    }
                    for symbol in affordable
                )
        if not opportunities and not allocation_became_blocked:
            return
        trades = {
            name: copy.deepcopy(account.book.trades[self.reviewed_fills[name] :])
            for name, account in self.accounts.items()
        }
        for name, fills in trades.items():
            self.reviewed_fills[name] += len(fills)
        currency = self.market.currency.lower()
        payload = {
            "market": self.market.code,
            "currency": self.market.currency,
            "evidence_sequence": sequence,
            "evidence_at": at,
            "base_version": candidate.entry_policy_version,
            "current": self._entry_configuration(),
            "new_rule_opportunities": opportunities,
            "allocation_became_blocked": allocation_became_blocked,
            "completed_bar_features": {
                symbol: {
                    **asdict(feature),
                    "unowned_and_unreserved": feature.symbol not in excluded,
                }
                for symbol, feature in observed.items()
            },
            "orderbooks": {symbol: self.books[symbol] for symbol in observed},
            "accounts": {
                name: {
                    f"cash_{currency}": account.book.cash,
                    f"reserved_cash_{currency}": account.reserved_cash,
                    f"realized_{currency}": account.book.realized,
                    "new_fills": trades[name],
                    "positions": {
                        symbol: {
                            "quantity": position.quantity,
                            f"entry_price_{currency}": position.entry,
                            f"last_mark_{currency}": account.prices[symbol],
                            "mark_at": account.times[symbol],
                            f"entry_fee_{currency}": account.book.entry_fees[symbol],
                            "strategy": account.owners[symbol][0],
                            "policy_version": account.owner_policy_versions[symbol],
                        }
                        for symbol, position in account.book.positions.items()
                    },
                    "pending": [
                        {
                            "symbol_id": intent.symbol,
                            "side": intent.side,
                            "quantity": intent.quantity,
                            f"limit_{currency}": intent.limit,
                            "decision_at": intent.decision_at,
                            "policy_version": intent.policy_version,
                        }
                        for intent in account.pending.values()
                    ],
                }
                for name, account in self.accounts.items()
            },
            "symbol_ids": self.ids.copy(),
            "cost_scenario": candidate.context.schedule.copy(),
        }
        self.review_payload = payload

    @property
    def entry_watch(self) -> list[str] | set[str]:
        return self.desired if self.shared_watch is None else self.shared_watch

    def launch_review(self) -> None:
        payload, self.review_payload = self.review_payload, None
        if payload is not None:
            assert self.review_settings is not None
            self.review_task = asyncio.create_task(self._review(self.review_settings, payload))

    def _publish_review(self, payload: Any) -> None:
        if self.shared_review_publish is None:
            self.publish("policy_review", payload)
        else:
            self.shared_review_publish(payload)

    async def _review(self, settings: Settings, payload: dict[str, Any]) -> None:
        model = SOL
        try:
            while True:
                if (
                    self.shared_review_call is not None
                    and self.end_epoch is not None
                    and time.time() >= self.end_epoch
                ):
                    break
                if self.journal is not None:
                    self.journal.append({"kind": "review_call", "model": model})
                if self.shared_review_call is not None:
                    self.shared_review_call(model, payload["evidence_sequence"])
                self.review_calls[model] += 1
                review_id = sum(self.review_calls.values())
                self.record(
                    "ai_review", review_id=review_id, model=model, effort="medium", **payload
                )
                response = await propose(settings, payload, model)
                after_cutoff = self.end_epoch is None or time.time() >= self.end_epoch
                self.record(
                    "ai_decision",
                    review_id=review_id,
                    model=model,
                    response=response,
                    after_cutoff=after_cutoff,
                )
                if after_cutoff:
                    break
                if response["action"] != "defer":
                    self._publish_review((review_id, payload["base_version"], response))
                    return
                payload = {**payload, "sol_question": response["reason"]}
                model = ASTRA
        except (SafetyError, OSError) as error:
            self.record(
                "ai_review_failed",
                model=model,
                error=type(error).__name__,
                **({"reason": str(error)} if isinstance(error, SafetyError) else {}),
            )
        except asyncio.CancelledError:
            self.record("ai_review_cancelled", model=model)
            raise
        self._publish_review(None)

    def _apply_review(
        self, sequence: int, received: str, result: Any, at: str | None = None
    ) -> None:
        self.review_task = None
        if result is None:
            return
        review_id, base_version, response = result
        candidate = self.accounts["candidate"]
        at = now() if at is None else at
        if (
            self.end_epoch is None
            or seconds(at) >= self.end_epoch
            or self.regular_window is None
            or not self.regular_window[0] <= seconds(at) < self.regular_window[1]
            or candidate.entry_policy_version != base_version
        ):
            self.record("ai_application_skipped", review_id=review_id, reason="cutoff_or_stale")
            return
        if response["action"] != "change":
            return
        config = {key: response[key] for key in ENTRY_OPTIONS}
        for key, value in config.items():
            setattr(candidate, key, value)
        candidate.entry_policy_version += 1
        self.reviewed_families.clear()
        self.allocation_blocked = False
        applied = {
            "sequence": sequence,
            "published_at": received,
            "applied_at": at,
            "review_id": review_id,
            "version": candidate.entry_policy_version,
            "configuration": config,
            "reason": response["reason"],
        }
        self.policy_history.append(applied)
        self.record("ai_policy_applied", **applied)

    async def consume(self) -> None:
        while True:
            sequence, kind, payload, received, queued = await self.queue.get()
            started = time.monotonic()
            self.queue_delays.append(started - queued)
            try:
                entry = {
                    "sequence": sequence,
                    "kind": kind,
                    "payload": payload,
                    "received": received,
                }
                await self._consume_event(entry, started - queued)
                if self.journal is not None:
                    self._commit(entry)
                self.launch_review()
            except (ValueError, TypeError, KeyError, RuntimeError, OverflowError) as error:
                self.record(
                    "runtime_event_failed",
                    sequence=sequence,
                    event_kind=kind,
                    published_at=received,
                    symbol=payload[0] if kind == "book" else None,
                    **error_details(error, ""),
                )
                raise
            finally:
                self.processing_times.append(time.monotonic() - started)
                self.queue.task_done()

    async def _consume_event(
        self,
        entry: dict[str, Any],
        queue_delay: float,
        *,
        saved: dict[str, Any] | None = None,
        replaying: bool = False,
    ) -> None:
        sequence, kind, payload, received = (
            entry["sequence"],
            entry["kind"],
            entry["payload"],
            entry["received"],
        )
        if kind == "selection":
            self.candidates = payload
        elif kind == "calendar":
            day, calendar = payload
            self.regular_window = us_regular_window(calendar, day, self.market)
        elif kind == "feature":
            symbol, rows = payload
            if self.constrained:
                window = self.regular_window
                rows = [
                    row
                    for row in rows
                    if window is not None and window[0] < seconds(row["timestamp"]) <= window[1]
                ]
            self.features[symbol] = feature_from_bars(
                self.identifier(symbol),
                [{**r, "available_at": received} for r in rows],
                received,
                timestamp_is_end=self.constrained,
            )
            self.counts["features_valid" if self.features[symbol] else "features_missing"] += 1
        elif kind == "fx":
            try:
                rate = float(payload["midRate"])
            except (KeyError, TypeError, ValueError):
                rate = 0
            pair_matches = (
                payload.get("baseCurrency", "USD") == "USD"
                and payload.get("quoteCurrency", "KRW") == "KRW"
            )
            if not math.isfinite(rate) or rate <= 0 or not pair_matches:
                self.counts["invalid_fx"] += 1
                rate = 0
            self.fx = (
                rate,
                received,
                payload.get("validFrom"),
                payload.get("validUntil"),
            )
        elif kind == "policy_review":
            if self.adaptive:
                at = self.processing_clock.stamp(entry, entry.get("at") or now(), self.record)
                if "wall_at" in entry:
                    return
                prior_version_count = len(self.policy_history)
                self._apply_review(sequence, received, payload, at)
                entry["applied"] = self.policy_history[prior_version_count:]
            else:
                self._apply_review(sequence, received, payload)
        elif kind == "book":
            symbol, book = payload
            if saved is None and not replaying:
                self.symbol_delays.setdefault(symbol, []).append(queue_delay)
            self.counts["book_events"] += 1
            if not isinstance(book, dict) or not isinstance(book.get("timestamp"), str):
                self.counts["book_unavailable"] += 1
                self.record(
                    "book_rejected",
                    reason="unavailable_timestamp",
                    sequence=sequence,
                    symbol=symbol,
                )
                return
            source_age = seconds(received) - seconds(book["timestamp"])
            if not 0 <= source_age <= 60:
                self.counts["invalid_source_age"] += 1
                return
            at = self.processing_clock.stamp(entry, entry.get("at") or now(), self.record)
            if "wall_at" in entry:
                self.counts["books_skipped_clock_regression"] += 1
                return
            if self.adaptive and self.end_epoch is not None and seconds(at) >= self.end_epoch:
                self.counts["after_experiment_cutoff"] += 1
                return
            if self.constrained:
                if self.market is KR and book.get("currency") != KR.currency:
                    self.counts["book_currency_mismatch"] += 1
                    return
                window = self.regular_window
                if window is None or not all(
                    window[0] <= seconds(stamp) < window[1]
                    for stamp in (book["timestamp"], received, at)
                ):
                    self.counts["outside_regular_session"] += 1
                    return
                if self.market is US and (not book["asks"] or not book["bids"]):
                    self.counts["missing_book_depth"] += 1
                    return
            old = self.books.get(symbol)
            if old and seconds(old["timestamp"]) > seconds(book["timestamp"]):
                self.counts["old_snapshot_ignored"] += 1
                return
            paired = len(self.accounts) > 1
            if self.market is KR:
                self.unactionable_books.discard(symbol)
                # The provider can pad a depth side with zero-volume placeholders.
                bids = [row for row in book["bids"] if float(row["volume"]) != 0]
                asks = [row for row in book["asks"] if float(row["volume"]) != 0]
                reason = None
                if not asks or not bids:
                    reason = "missing_book_depth"
                elif not all(
                    KR.valid_quote_level(float(row["price"]), float(row["volume"]))
                    for row in bids + asks
                ):
                    reason = "invalid_book_depth"
                elif max(float(row["price"]) for row in bids) > min(
                    float(row["price"]) for row in asks
                ):
                    # Integrated KRX/NXT may legitimately cross.
                    reason = "integrated_cross_uncertain"
                if reason is not None:
                    self.unactionable_books.add(symbol)
                    self.counts[reason] += 1
                    self.record(
                        "book_unactionable",
                        reason=reason,
                        sequence=sequence,
                        symbol=symbol,
                        source_at=book["timestamp"],
                    )
            elif (
                (paired or self.constrained)
                and book.get("currency") == self.market.currency
                and book["asks"]
                and book["bids"]
                and max(float(row["price"]) for row in book["bids"])
                > min(float(row["price"]) for row in book["asks"])
            ):
                self.counts["crossed_orderbook_rejected"] += 1
                self.record(
                    "book_rejected",
                    reason="crossed_orderbook",
                    sequence=sequence,
                    symbol=symbol,
                    source_at=book["timestamp"],
                )
                return
            self.books[symbol] = book
            f = self.features.get(symbol)
            if f is not None and not self.constrained:
                price = (
                    min(float(r["price"]) for r in book["asks"])
                    + max(float(r["price"]) for r in book["bids"])
                ) / 2
                f = replace(
                    f,
                    price=price,
                    r5=price / (f.price / (1 + f.r5)) - 1,
                    r15=price / (f.price / (1 + f.r15)) - 1,
                )
            use_prepared = paired or self.constrained
            prepared = {s: self._feature_at(s, at) for s in self.candidates} if use_prepared else {}
            if use_prepared:
                f = self._feature_at(symbol, at)
            fresh = f is not None and 0 <= seconds(at) - seconds(f.bar_end) <= 90
            self.counts["fresh_features" if fresh else "unusable_features"] += 1
            if fresh and f is not None and assign(f, self.account.policy):
                self.counts["signal_observations"] += 1
            shared = {
                "id": f"live:{self.counts['book_events']}",
                "symbol": self.identifier(symbol),
                "received_at": at,
                "book": book,
                "feature": f,
                "eligible": symbol in self.candidates
                and (not self.adaptive or symbol in self.entry_watch),
                "fx": self.fx[0],
                "fx_at": self.fx[1],
                "fx_valid_from": self.fx[2],
                "fx_valid_until": self.fx[3],
                "fx_provider_validity_required": self.constrained,
            }
            if self.constrained:
                shared["provider_symbol"] = symbol
            observation = (
                json.dumps(
                    {
                        "sequence": sequence,
                        "published_at": received,
                        **shared,
                        "feature": asdict(f) if f is not None else None,
                    },
                    sort_keys=True,
                    allow_nan=False,
                ).encode()
                + b"\n"
            )
            if self.adaptive and saved is None and not replaying:
                self._consider_review(sequence, at, prepared)
            for name, account in self.accounts.items():
                before = len(account.intents), len(account.outcomes)
                excluded = set(account.pending) | set(account.book.positions)
                candidate_count = max(
                    1,
                    sum(
                        self.ids.get(x) not in excluded
                        and (
                            candidate_feature := (
                                prepared.get(x) if use_prepared else self.features.get(x)
                            )
                        )
                        is not None
                        and 0 <= seconds(at) - seconds(candidate_feature.bar_end) <= 90
                        and assign(candidate_feature, account.policy) is not None
                        and (
                            not self.adaptive
                            or (
                                x in self.entry_watch
                                and (
                                    account.selection != "active"
                                    or abs(candidate_feature.r5) >= 0.001
                                    or abs(candidate_feature.r15) >= 0.003
                                )
                            )
                        )
                        for x in self.candidates
                    ),
                )
                submission = {**shared, "candidate_count": candidate_count}
                if self.journal is not None or saved is not None:
                    encoded = {**submission, "feature": asdict(f) if f is not None else None}
                    entry.setdefault("submissions", {})[name] = encoded
                    if saved is not None:
                        recorded = saved["submissions"][name]
                        if encode(encoded) != encode(recorded):
                            raise JournalError("Journal submission does not match observations")
                        feature = recorded["feature"]
                        submission = {
                            **recorded,
                            "feature": Feature(**feature) if feature else None,
                        }
                await account.submit(submission)
                self._record_delivery(name, observation)
                self.book_delivered[name] += 1
                identity = {"account": name} if paired else {}
                for i in account.intents[before[0] :]:
                    self.record(
                        "intent",
                        symbol=symbol,
                        quantity=i.quantity,
                        side=i.side,
                        decision_at=i.decision_at,
                        reserved=i.reserved,
                        **({"order_request": i.order_request} if i.order_request else {}),
                        **(
                            {
                                "intent_id": i.id,
                                "policy_version": i.policy_version,
                                "strategy": i.strategy,
                                "sequence": sequence,
                            }
                            if self.adaptive
                            else {}
                        ),
                        **identity,
                    )
                for result in account.outcomes[before[1] :]:
                    self.record("outcome", result=result, **identity)
            if self.adaptive:
                entry["observation"] = observation.decode()
            if paired:
                self.record(
                    "comparison_observation",
                    sequence=sequence,
                    symbol=symbol,
                    received_at=received,
                    decision_at=at,
                    input_sha256=hashlib.sha256(observation).hexdigest(),
                )
        elif kind == "clock":
            at = self.processing_clock.stamp(entry, entry.get("at") or now(), self.record)
            observation = json.dumps([sequence, "clock", received, at]).encode() + b"\n"
            expired = await self._advance(at, observation)
            if self.journal is not None or saved is not None:
                entry["observation"] = observation.decode()
                entry["expired"] = expired
        else:
            raise ValueError("Unknown runtime event")
        self.desired = subscription_set(self.candidates, self.protected())

    async def _advance(self, at: str, observation: bytes) -> dict[str, list[dict[str, Any]]]:
        expired = {}
        for name, account in self.accounts.items():
            before = len(account.outcomes)
            await account.advance(at)
            if len(account.outcomes) > before:
                expired[name] = account.outcomes[before:]
            self._record_delivery(name, observation)
        return expired

    def _checkpoint(self) -> str:
        return digest(
            encode(
                {
                    name: {
                        "cash": account.book.cash,
                        "realized": account.book.realized,
                        "fees": account.book.fee_totals,
                        "entry_fees": account.book.entry_fees,
                        "position_marks": {
                            s: (account.prices[s], account.times[s]) for s in account.book.positions
                        },
                        "positions": {s: asdict(p) for s, p in account.book.positions.items()},
                        "owners": account.owners,
                        "owner_versions": account.owner_policy_versions,
                        "pending": {s: asdict(i) for s, i in account.pending.items()},
                        "intent_count": len(account.intents),
                        "outcome_count": len(account.outcomes),
                        "trade_count": len(account.book.trades),
                        "last_intent": asdict(account.intents[-1]) if account.intents else None,
                        "last_outcome": account.outcomes[-1] if account.outcomes else None,
                        "last_trade": account.book.trades[-1] if account.book.trades else None,
                        "last_at": account.last_at,
                        "policy_version": account.entry_policy_version,
                        "input_hash": self.delivery_hashes[name].hexdigest(),
                    }
                    for name, account in self.accounts.items()
                }
            )
        )

    def _commit(self, entry: dict[str, Any]) -> None:
        assert self.journal is not None
        self.journal.append(
            {
                **entry,
                "checkpoint": self._checkpoint(),
                "reviewed_fills": self.reviewed_fills,
                "reviewed_families": sorted(self.reviewed_families),
                "allocation_blocked": self.allocation_blocked,
            }
        )

    async def restore(self, journal: AdaptiveJournal) -> None:
        original_record = self.record
        self.record = lambda *_args, **_kwargs: None
        try:
            for saved in journal.events():
                kind = saved["kind"]
                if kind == "initial":
                    if saved["index"] != 1:
                        raise JournalError("Unexpected journal initialization record")
                    self.fx = (0, saved["fx_at"], None, None)
                    continue
                if saved["index"] == 1:
                    raise JournalError("Journal has no initialization record")
                if kind == "review_call":
                    self.review_calls[saved["model"]] += 1
                    continue
                entry = {key: saved[key] for key in ("sequence", "kind", "payload", "received")}
                for key in ("at", "wall_at"):
                    if key in saved:
                        entry[key] = saved[key]
                if kind in ("recovery_gap", "final_advance"):
                    await self._advance_marker(entry)
                else:
                    if entry["sequence"] != self.sequence + 1:
                        raise JournalError("Journal observation sequence mismatch")
                    self.sequence = entry["sequence"]
                    await self._consume_event(entry, 0, saved=saved)
                if (
                    encode(entry) != encode({key: saved[key] for key in entry})
                    or saved["checkpoint"] != self._checkpoint()
                ):
                    raise JournalError("Journal replay diverged from committed account results")
                self.reviewed_fills = saved["reviewed_fills"]
                self.reviewed_families = set(saved["reviewed_families"])
                self.allocation_blocked = saved["allocation_blocked"]
        finally:
            self.record = original_record
        self.journal = journal

    async def _advance_marker(self, entry: dict[str, Any]) -> None:
        at = self.processing_clock.stamp(entry, entry["at"], self.record)
        expired = await self._advance(at, (encode(entry) + "\n").encode())
        entry["gap_expired"] = expired
        self.desired = subscription_set(self.candidates, self.protected())

    async def mark_advance(self, kind: str, at: str) -> None:
        last = self.account.last_at
        entry = {
            "kind": kind,
            "sequence": self.sequence,
            "payload": {"last_at": last},
            "received": at,
            "at": at,
        }
        await self._advance_marker(entry)
        self._commit(entry)
        self.record(kind, sequence=self.sequence, previous_at=last, at=at)

    def status(self) -> dict[str, Any]:
        result = {
            "counts": dict(self.counts),
            "desired": self.desired,
            "protected": sorted(self.protected()),
            "symbol_ids": self.ids,
            "queue_depth": self.queue.qsize(),
            "max_queue_depth": self.max_queue,
            "per_symbol_queue_delay_seconds": {
                s: latency_summary(v) for s, v in self.symbol_delays.items()
            },
            "queue_delay_seconds": latency_summary(self.queue_delays),
            "processing_seconds": latency_summary(self.processing_times),
            "paper": self.account.summary() if self.account.last_at else None,
        }
        if len(self.accounts) > 1:
            result["comparison"] = {
                "qualification": "not_assessed",
                "inputs_aligned": len(set(self.delivered.values())) == 1
                and len({h.hexdigest() for h in self.delivery_hashes.values()}) == 1,
                "configuration_unchanged": all(
                    account.policy == self.configurations[name]["policy"]
                    and account.selection == self.configurations[name]["selection"]
                    and account.allocation == self.configurations[name]["allocation"]
                    and account.delay == self.configurations[name]["delay_seconds"]
                    and account.book.initial_equity == self.configurations[name]["capital"]
                    and account.context.schedule == self.configurations[name]["fee_schedule"]
                    for name, account in self.accounts.items()
                ),
                "accounts": {
                    name: {
                        "configuration": copy.deepcopy(self.configurations[name]),
                        "observations": self.delivered[name],
                        "book_observations": self.book_delivered[name],
                        "input_sha256": self.delivery_hashes[name].hexdigest(),
                        "paper": account.summary() if account.last_at else None,
                    }
                    for name, account in self.accounts.items()
                },
            }
        if self.adaptive:
            candidate = self.accounts["candidate"]
            result["ai_calls"] = sum(self.review_calls.values())
            result["adaptive"] = {
                "model_calls": dict(self.review_calls),
                "policy_version": candidate.entry_policy_version,
                "current": self._entry_configuration(),
                "policy_history": self.policy_history,
                "position_policy_versions": candidate.owner_policy_versions.copy(),
                "execution_contract_preserved": all(
                    account.delay == self.configurations[name]["delay_seconds"]
                    and account.book.initial_equity == self.configurations[name]["capital"]
                    and account.context.schedule == self.configurations[name]["fee_schedule"]
                    for name, account in self.accounts.items()
                )
                and all(
                    getattr(self.account, key) == self.configurations["incumbent"][key]
                    for key in ENTRY_OPTIONS
                ),
            }
        return result


def write_comparison(
    output: Path, stats: dict[str, Any], runtime: Runtime
) -> dict[str, Any] | None:
    """Save descriptive comparison evidence after the shared recording files close."""
    if len(runtime.accounts) == 1 and not runtime.constrained:
        return None
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    expected = manifest.get("source_sha256", {})
    sources: tuple[str, ...] = (
        SOURCES if runtime.constrained or len(runtime.accounts) > 1 else LEGACY_SOURCES
    )
    if runtime.adaptive:
        sources = ADAPTIVE_SOURCES
    if runtime.market is KR:
        sources += KR_SOURCES
    expected_sources = set(sources)
    source_unchanged = set(expected) == expected_sources and all(
        (ROOT / name).is_file()
        and hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected[name]
        for name in expected_sources
    )
    hashes = {}
    for name in ("manifest.json", "market.jsonl.gz", "events.jsonl"):
        path = output / name
        if path.is_file():
            with path.open("rb") as stream:
                hashes[name] = hashlib.file_digest(stream, "sha256").hexdigest()
    runtime_status = runtime.status()
    comparison = runtime_status.get("comparison", {})
    if len(runtime.accounts) == 1:
        account = runtime.account
        frozen = runtime.configurations["incumbent"]
        configuration_unchanged = (
            account.policy == frozen["policy"]
            and account.selection == frozen["selection"]
            and account.allocation == frozen["allocation"]
            and account.delay == frozen["delay_seconds"]
            and account.book.initial_equity == frozen["capital"]
            and account.context.schedule == frozen["fee_schedule"]
        )
    else:
        configuration_unchanged = comparison.get("configuration_unchanged", False)
    freeze_matches = (
        manifest.get("comparison") == runtime.configurations
        if len(runtime.accounts) > 1
        else manifest.get("account") == runtime.configurations["incumbent"]
    )
    if runtime.market is KR:
        freeze_matches = (
            freeze_matches
            and manifest.get("market") == KR.code
            and manifest.get("currency") == KR.currency
        )
    policy_trace_valid = True
    if runtime.adaptive:
        adaptive_status = runtime_status["adaptive"]
        history = adaptive_status["policy_history"]
        baseline = {key: runtime.configurations["candidate"][key] for key in ENTRY_OPTIONS}
        policy_trace_valid = (
            adaptive_status["execution_contract_preserved"]
            and adaptive_status["policy_version"] == len(history)
            and runtime.configurations["incumbent"] == runtime.configurations["candidate"]
            and adaptive_status["current"]
            == (history[-1]["configuration"] if history else baseline)
            and all(
                row["version"] == index + 1
                and (index == 0 or row["sequence"] > history[index - 1]["sequence"])
                and (index == 0 or row["applied_at"] >= history[index - 1]["applied_at"])
                and all(
                    row["configuration"][key] in values for key, values in ENTRY_OPTIONS.items()
                )
                for index, row in enumerate(history)
            )
        )
    complete = (
        stats.get("state") == "complete"
        and source_unchanged
        and len(hashes) == 3
        and freeze_matches
        and comparison.get("inputs_aligned", True)
        and (policy_trace_valid if runtime.adaptive else configuration_unchanged)
    )
    state = stats.get("state") if stats.get("state") in ("complete", "failed") else "interrupted"
    result = {
        **stats,
        **comparison,
        "state": state if complete or state != "complete" else "incomplete",
        "result": (
            "inconclusive"
            if state == "complete" and not complete and stats.get("result") == "passed"
            else stats.get("result")
        ),
        "complete": complete,
        "ended_at": now(),
        "source_unchanged": source_unchanged,
        "configuration_unchanged": configuration_unchanged,
        "freeze_matches": freeze_matches,
        **({"policy_trace_valid": policy_trace_valid} if runtime.adaptive else {}),
        "observation_status": "observed_short_sample"
        if all(runtime.book_delivered.values())
        else "inconclusive",
        "input_files_sha256": hashes,
        "freeze": manifest.get("comparison"),
        "symbol_ids": runtime.ids,
    }
    private_json(output / "comparison.json", result)
    return result


async def wait_for_rest_cooldown(error: ApiError, deadline: float) -> None:
    if error.status in (401, 403):
        raise error
    if error.status != 429 and error.code != "cooldown":
        return
    if error.retry_at is None or not math.isfinite(error.retry_at):
        raise error
    delay = min(error.retry_at - time.time(), deadline - time.monotonic())
    if delay > 0:
        await asyncio.sleep(delay)


def websocket_retry_at(headers: list[str], now_epoch: float) -> float:
    retry_at = now_epoch + 2
    for header in headers:
        value = header.strip()
        try:
            if value.isascii() and value.isdecimal():
                candidate = now_epoch + float(value)
            else:
                stamp = parsedate_to_datetime(value)
                if stamp.utcoffset() is None:
                    continue
                candidate = stamp.timestamp()
        except (ValueError, TypeError, OSError, OverflowError):
            continue
        if math.isfinite(candidate):
            retry_at = max(retry_at, candidate)
    return retry_at


def journal_identity(
    capital: float,
    end_epoch: float,
    market: PaperMarket = US,
    *,
    slot_control: Path | None = None,
    campaign_seeds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sources: tuple[str, ...] = ADAPTIVE_SOURCES
    if market is KR:
        sources += KR_SOURCES
    if slot_control is not None:
        sources += SLOT_SOURCES
    return {
        "profile": "broker-constrained",
        "experiment": "adaptive_exploration" if slot_control is None else "shared_feed_slots",
        **({"slot_control": str(slot_control.resolve())} if slot_control is not None else {}),
        **({"campaign_seeds": campaign_seeds} if campaign_seeds is not None else {}),
        "market": market.code,
        "currency": market.currency,
        "capital": float(capital),
        "end_epoch": end_epoch,
        "source_sha256": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in sources
        },
        "settings_sha256": hashlib.sha256((ROOT / "settings.toml").read_bytes()).hexdigest(),
    }


async def campaign(
    output: Path,
    duration: int,
    capital: float,
    *,
    candidate_policy: str | None = None,
    profile: str = "legacy",
    adaptive: bool = False,
    end_epoch: float | None = None,
    journal_path: Path | None = None,
    slot_control: Path | None = None,
    resume: bool = False,
    market: PaperMarket = US,
    campaign_seeds: dict[str, Any] | None = None,
) -> None:
    if market is KR and profile != "broker-constrained":
        raise ValueError("KR requires the broker-constrained profile")
    if slot_control is not None and journal_path is None:
        raise ValueError("Shared slots require a durable campaign journal")
    if campaign_seeds is not None:
        if slot_control is None:
            raise ValueError("Campaign seeds require a shared-slot journal")
        validate_session_seeds(campaign_seeds)
    if resume and journal_path is None:
        raise ValueError("Recovery requires an existing journal")
    if journal_path is None:
        await _campaign(
            output,
            duration,
            capital,
            candidate_policy=candidate_policy,
            profile=profile,
            adaptive=adaptive,
            end_epoch=end_epoch,
            market=market,
        )
        return
    if not adaptive or profile != "broker-constrained" or end_epoch is None:
        raise ValueError("Recovery journals require the constrained adaptive profile")
    if not math.isfinite(end_epoch) or time.time() >= end_epoch:
        raise ValueError("Campaign cutoff reached before journal open")
    os.umask(0o077)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    identity = journal_identity(
        capital, end_epoch, market, slot_control=slot_control, campaign_seeds=campaign_seeds
    )
    with ExitStack() as contexts:
        journal = contexts.enter_context(
            AdaptiveJournal.open(journal_path, identity, resume=resume)
        )
        control = contexts.enter_context(SlotControl.open(slot_control)) if slot_control else None
        await _campaign(
            output,
            duration,
            capital,
            candidate_policy=candidate_policy,
            profile=profile,
            adaptive=adaptive,
            end_epoch=end_epoch,
            journal=journal,
            control=control,
            resume=resume,
            market=market,
            campaign_seeds=campaign_seeds,
        )


async def _campaign(
    output: Path,
    duration: int,
    capital: float,
    *,
    candidate_policy: str | None = None,
    profile: str = "legacy",
    adaptive: bool = False,
    end_epoch: float | None = None,
    journal: AdaptiveJournal | None = None,
    control: SlotControl | None = None,
    resume: bool = False,
    market: PaperMarket = US,
    campaign_seeds: dict[str, Any] | None = None,
) -> None:
    if adaptive and (
        profile != "broker-constrained"
        or candidate_policy is not None
        or end_epoch is None
        or not math.isfinite(end_epoch)
        or time.time() >= end_epoch
    ):
        raise ValueError("Adaptive campaign requires a constrained profile and future cutoff")
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

    os.umask(0o077)
    output.mkdir(parents=True, exist_ok=False)
    files = ExitStack()
    log = files.enter_context((output / "events.jsonl").open("x", buffering=1))
    raw = files.enter_context(
        gzip.open(output / "market.jsonl.gz", "wt")  # noqa: SIM115 - ExitStack closes on all exits.
    )

    def record(kind: str, **payload: Any) -> None:
        log.write(json.dumps({"at": now(), "kind": kind, **payload}) + "\n")
        if kind == "slot_command" and payload["disposition"] == "applied":
            resync.set()

    http_audit: list[dict[str, Any]] = []

    def record_feed(kind: str, payload: dict[str, Any]) -> None:
        if kind == "http":
            http_audit.append(payload)
        elif market is KR and kind in ("listings", "prices"):
            raw.write(json.dumps({"kind": kind, "received_at": now(), **payload}) + "\n")

    def make_runtime(
        slot_capital: float, slot_record: Any, slot_end_epoch: float | None = end_epoch
    ) -> Runtime:
        return Runtime(
            slot_capital,
            slot_record,
            candidate_policy=candidate_policy,
            profile=profile,
            adaptive=adaptive,
            end_epoch=slot_end_epoch,
            market=market,
        )

    slots = None
    runtime: Runtime | SharedFeedSlots
    if control is not None:
        assert journal is not None
        slots = SharedFeedSlots(
            make_runtime, subscription_set, record, journal, control, session_seeds=campaign_seeds
        )
        runtime = slots
    else:
        runtime = make_runtime(capital, record)
    constrained = profile == "broker-constrained"

    stats: dict[str, Any] = {
        "state": "starting",
        "ai_calls": 0,
        "real_orders": False,
        **({"capital": capital} if slots is None else {"experiment": "shared_feed_slots"}),
        **({"market": market.code, "currency": market.currency} if constrained else {}),
        "duration_seconds": duration,
        "rest_cycles": [],
        "connections": 0,
        "acks": [],
        "faults": [],
    }
    tasks: list[asyncio.Task] = []
    token = ""
    resync = asyncio.Event()
    try:
        if slots is not None:
            await slots.load_predecessors(make_runtime)
            if resume:
                await slots.restore()
                stats["recovered_from"] = slots.journal.evidence()
                recovered_at = now()
                if end_epoch is None or seconds(recovered_at) >= end_epoch:
                    raise CampaignCutoff("Campaign cutoff reached during replay")
                await slots.marker("recovery_gap", recovered_at)
        elif journal is not None:
            assert isinstance(runtime, Runtime)
            if resume:
                await runtime.restore(journal)
                if journal.evidence()["records"] == 0:
                    journal.append({"kind": "initial", "fx_at": runtime.fx[1]})
                stats["recovered_from"] = journal.evidence()
                recovered_at = now()
                if end_epoch is None or seconds(recovered_at) >= end_epoch:
                    raise CampaignCutoff("Campaign cutoff reached during replay")
                await runtime.mark_advance("recovery_gap", recovered_at)
            else:
                runtime.journal = journal
                journal.append({"kind": "initial", "fx_at": runtime.fx[1]})
        settings = Settings.load(ROOT)
        if journal is not None and (
            end_epoch is None
            or journal_identity(
                capital,
                end_epoch,
                market,
                slot_control=Path(journal.identity["slot_control"]) if slots is not None else None,
                campaign_seeds=campaign_seeds,
            )
            != journal.identity
        ):
            raise JournalError("Campaign source or settings changed during initialization")
        if slots is not None:
            slots.enable_reviews(settings)
        elif adaptive:
            assert isinstance(runtime, Runtime)
            runtime.review_settings = settings
        data = copy.deepcopy(settings.data)
        data["limits"]["minimum_request_interval"] = 0.08
        market_settings = Settings(ROOT, data)
        client = (
            DeadlineMarketClient(market_settings, end_epoch, recurring=campaign_seeds is not None)
            if adaptive and end_epoch is not None
            else CachedTokenClient(market_settings)
        )
        feed = LiveFeed(
            client.settings,
            record_feed,
            client=client,
            market=market,
        )
        token = (
            client.recovery_token()
            if resume and isinstance(client, DeadlineMarketClient)
            else client.access_token()
        )
        spec = await asyncio.to_thread(read_spec)
        (output / "asyncapi.json").write_bytes(spec)
        universe = await asyncio.to_thread(feed.universe)
        started = time.monotonic()
        remaining = (end_epoch - time.time()) if end_epoch is not None else duration
        if remaining <= 0:
            raise ValueError("Campaign cutoff reached during initialization")
        deadline = started + remaining
        stats.update(
            state="running",
            duration_seconds=remaining,
            started_at=now(),
            expected_end_epoch=end_epoch if end_epoch is not None else time.time() + duration,
            universe_count=len(universe),
        )
        calendar_day = (
            market.trading_date(datetime.fromisoformat(now())) if constrained else now()[:10]
        )
        calendar = await asyncio.to_thread(client.calendar, market.code, calendar_day)
        if constrained:
            runtime.publish("calendar", (calendar_day, calendar))
        sources: tuple[str, ...] = (
            SOURCES if candidate_policy is not None or constrained else LEGACY_SOURCES
        )
        if adaptive:
            sources = ADAPTIVE_SOURCES
        if market is KR:
            sources += KR_SOURCES
        stream_market = market.code.lower()
        private_json(
            output / "manifest.json",
            {
                **(
                    {"comparison": runtime.configurations}
                    if isinstance(runtime, Runtime) and len(runtime.accounts) > 1
                    else {}
                ),
                **(
                    {
                        "experiment": (
                            "shared_feed_slots" if slots is not None else "adaptive_exploration"
                        ),
                        "controlled_faults": False,
                        "end_epoch": end_epoch,
                        "entry_options": ENTRY_OPTIONS,
                        "ai_models": [SOL, ASTRA],
                        "ai_reasoning_effort": "medium",
                        "durable_journal": journal is not None,
                        "resumed": resume,
                    }
                    if adaptive
                    else {}
                ),
                "source_sha256": (
                    journal.identity["source_sha256"]
                    if journal is not None
                    else {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in sources}
                ),
                "selection": (
                    "Absolute change between consecutive fresh minute scans; "
                    f"{market.currency} common shares"
                ),
                "universe_count": len(universe),
                "connection_capacity_symbols": 50,
                "calendar": calendar,
                **(
                    {
                        "profile": profile,
                        **(
                            {"account": runtime.configurations["incumbent"]}
                            if isinstance(runtime, Runtime)
                            else {}
                        ),
                        "cost_evidence": KR_COST_EVIDENCE if market is KR else US_COST_EVIDENCE,
                        "session_policy": (
                            "Provider integrated KR regular session, open inclusive, "
                            "closing auction start exclusive"
                            if market is KR
                            else "Provider US regular session, open inclusive, close exclusive"
                        ),
                        "market": market.code,
                        "currency": market.currency,
                        "instrument": (
                            "Verified active ordinary KOSPI/KOSDAQ common shares"
                            if market is KR
                            else "Active ordinary common shares"
                        ),
                        "quantity_mode": "whole_shares",
                        "order_model": "Simulated limit with delayed depth confirmation",
                        "capability_snapshot": MARKET_CAPABILITY_SNAPSHOT,
                    }
                    if constrained
                    else {}
                ),
                "limitations": [
                    "One connection, market-data only; not all symbols continuously watched",
                    (
                        "Recovery replays committed inputs and skips missed market time"
                        if journal is not None
                        else "No recovery journal configured; finite isolated paper account"
                    ),
                    "Single live sample does not establish profitability",
                    *(
                        [
                            "Attempt artifacts cover current acquisition only; accounts include replay"
                        ]
                        if resume
                        else []
                    ),
                ],
            },
        )

        async def collect() -> None:
            nonlocal calendar_day
            previous: dict[str, float] = {}
            metadata: dict[str, dict] = {}
            cycle = 0
            while time.monotonic() < deadline:
                begin = time.monotonic()
                day = market.trading_date(datetime.fromisoformat(now()))
                if constrained and day != calendar_day:
                    calendar = await asyncio.to_thread(client.calendar, market.code, day)
                    raw.write(
                        json.dumps({"kind": "calendar", "date": day, "result": calendar}) + "\n"
                    )
                    runtime.publish("calendar", (day, calendar))
                    calendar_day = day
                    if market is KR:
                        previous.clear()
                        metadata.clear()
                        await asyncio.to_thread(feed.universe)
                if not adaptive and cycle == 2:
                    before = runtime.counts["book_events"]
                    record("rest_delay_start", seconds=5)
                    await asyncio.sleep(5)
                    stats["faults"].append(
                        {
                            "kind": "rest_delay",
                            "seconds": 5,
                            "book_events_during": runtime.counts["book_events"] - before,
                        }
                    )
                rows = await asyncio.to_thread(feed.snapshot)
                received = now()
                raw.write(
                    json.dumps({"kind": "rest_prices", "received_at": received, "rows": rows})
                    + "\n"
                )
                ranked, previous = rank_candidates(
                    rows,
                    previous,
                    received,
                    slots.capital if slots is not None else capital,
                    market.currency,
                )
                if market is KR:
                    ranked = [
                        symbol
                        for symbol in ranked
                        if KR_SYMBOL.fullmatch(symbol)
                        and feed.listing_exchanges[symbol] in ({"KOSPI"}, {"KOSDAQ"})
                    ]
                stock_is_eligible = eligible_kr_stock if market is KR else eligible_stock
                chosen: list[str] = []
                for offset in range(0, len(ranked), 50):
                    batch = ranked[offset : offset + 50]
                    missing = [s for s in batch if s not in metadata]
                    if missing:
                        for row in await asyncio.to_thread(client.stocks, missing):
                            metadata[row["symbol"]] = row
                            raw.write(
                                json.dumps({"kind": "stock_info", "received_at": now(), "row": row})
                                + "\n"
                            )
                    chosen.extend(s for s in batch if stock_is_eligible(metadata.get(s, {})))
                    if len(chosen) >= 50:
                        break
                runtime.publish("selection", chosen[:50])
                if market is US:
                    fx = await asyncio.to_thread(feed.fx)
                    raw.write(json.dumps({"kind": "fx", "received_at": now(), "result": fx}) + "\n")
                    runtime.publish("fx", fx)
                watch = subscription_set(chosen[:50], runtime.protected())
                # Sequential REST calls share the client's rate controller off the event loop.
                for symbol in watch:
                    if time.monotonic() >= deadline:
                        break
                    try:
                        candles = await asyncio.to_thread(client.candles, symbol, now(), 90)
                        raw.write(
                            json.dumps(
                                {
                                    "kind": "candles",
                                    "received_at": now(),
                                    "symbol": symbol,
                                    "result": candles,
                                }
                            )
                            + "\n"
                        )
                        runtime.publish("feature", (symbol, candles["candles"]))
                        book = await asyncio.to_thread(client.book, symbol)
                        raw.write(
                            json.dumps(
                                {
                                    "kind": "rest_book",
                                    "received_at": now(),
                                    "symbol": symbol,
                                    "result": book,
                                }
                            )
                            + "\n"
                        )
                        runtime.publish("book", (symbol, book))
                    except ApiError as error:
                        record(
                            "rest_symbol_failure",
                            symbol=symbol,
                            status=error.status,
                            code=error.code,
                            retry_at=error.retry_at,
                        )
                        await wait_for_rest_cooldown(error, deadline)
                elapsed = time.monotonic() - begin
                stats["rest_cycles"].append(
                    {
                        "cycle": cycle,
                        "seconds": elapsed,
                        "prices": len(rows),
                        "ranked": len(ranked),
                        "selected": watch,
                    }
                )
                record("rest_cycle", **stats["rest_cycles"][-1])
                cycle += 1
                try:
                    await asyncio.wait_for(
                        resync.wait(), max(0.01, min(60 - elapsed, deadline - time.monotonic()))
                    )
                    resync.clear()
                except TimeoutError:
                    pass

        async def collect_until_cutoff() -> None:
            try:
                await collect()
            except CampaignCutoff:
                record("rest_cutoff")

        async def stream() -> None:
            injected = False
            while time.monotonic() < deadline:
                if not runtime.desired:
                    await asyncio.sleep(0.1)
                    continue
                try:
                    async with connect(
                        URL,
                        additional_headers={"Authorization": "Bearer " + token},
                        proxy=None,
                        ping_interval=20,
                        ping_timeout=20,
                        open_timeout=15,
                        close_timeout=5,
                    ) as ws:
                        stats["connections"] += 1
                        record("connected", connection=stats["connections"])
                        resync.set()
                        selected: list[str] = []
                        accepted: set[str] = set()
                        expected: set[str] = set()
                        ack_pending = False
                        declaration_at = 0.0
                        next_ping = 0.0
                        ping_at: float | None = None
                        while time.monotonic() < deadline:
                            tick = time.monotonic()
                            if not runtime.desired:
                                await ws.close(code=1000, reason="No candidates or holdings")
                                break
                            if not adaptive and not injected and tick - started >= duration / 2:
                                injected = True
                                stats["faults"].append(
                                    {"kind": "controlled_disconnect", "at": now()}
                                )
                                await ws.close(code=1000, reason="Controlled integration test")
                                record("controlled_disconnect")
                                break
                            if (
                                runtime.desired != selected
                                and not ack_pending
                                and tick - declaration_at >= 1
                            ):
                                selected = list(runtime.desired)
                                expected = {
                                    f"{c}:{stream_market}:{s}"
                                    for c in ("trade", "orderbook")
                                    for s in selected
                                }
                                await ws.send(json.dumps(declaration(stream_market, selected)))
                                declaration_at, ack_pending = tick, True
                            if ack_pending and tick - declaration_at > 20:
                                raise TimeoutError("Subscription acknowledgement timeout")
                            if ping_at is not None and tick - ping_at > 20:
                                raise TimeoutError("Application pong timeout")
                            if tick >= next_ping:
                                await ws.send("PING")
                                ping_at, next_ping = tick, tick + 60
                            try:
                                message = json.loads(await asyncio.wait_for(ws.recv(), 0.25))
                            except TimeoutError:
                                continue
                            kind = message.get("type")
                            if kind == "subscriptions":
                                actual = set(message.get("subscribed", []))
                                if actual != expected or message.get("rejected"):
                                    raise ValueError("Subscription rejection or mismatch")
                                accepted, ack_pending = actual, False
                                stats["acks"].append(
                                    {
                                        "at": now(),
                                        "connection": stats["connections"],
                                        "symbols": selected,
                                        "protected": sorted(runtime.protected()),
                                    }
                                )
                                record("subscription_ack", symbols=selected)
                            elif kind == "pong":
                                ping_at = None
                            elif kind == "message":
                                topic = message["topic"]
                                if topic not in accepted | expected:
                                    raise ValueError("Unexpected stream topic")
                                raw.write(
                                    json.dumps({"received_at": now(), "frame": message}) + "\n"
                                )
                                stats["market_messages"] = stats.get("market_messages", 0) + 1
                                if topic.startswith(f"orderbook:{stream_market}:"):
                                    runtime.publish(
                                        "book", (topic.rsplit(":", 1)[1], message["data"])
                                    )
                            else:
                                raise ValueError("Unexpected provider frame")
                except InvalidStatus as error:
                    status = error.response.status_code
                    if not adaptive or status not in (429, 503):
                        raise
                    record("connection_failure", **error_details(error, token))
                    raise ApiError(
                        "WebSocket handshake temporarily unavailable",
                        status=status,
                        code="cooldown",
                        retry_at=websocket_retry_at(
                            error.response.headers.get_all("Retry-After"), time.time()
                        ),
                    ) from error
                except (ConnectionClosed, OSError, TimeoutError) as error:
                    record("connection_failure", **error_details(error, token))
                if time.monotonic() < deadline:
                    record("reconnect_backoff", seconds=2)
                    await asyncio.sleep(2)

        consumer = asyncio.create_task(runtime.consume())
        collector = asyncio.create_task(collect_until_cutoff() if adaptive else collect())
        streamer = asyncio.create_task(stream())
        tasks = [consumer, collector, streamer]
        while time.monotonic() < deadline:
            if slots is not None:
                slots.check_reviews()
                slots.poll_controls()
            elif (
                isinstance(runtime, Runtime)
                and runtime.review_task is not None
                and runtime.review_task.done()
            ):
                runtime.review_task.result()
            for task in tasks:
                if task.done():
                    task.result()
                    if task is consumer:
                        raise RuntimeError("Consumer stopped")
            runtime.publish("clock", None)
            if slots is not None:
                async with slots.committing:
                    if consumer.done():
                        consumer.result()
                    status = slots.status()
                    slots.control.snapshot(status["shared_slots"])
            else:
                status = runtime.status()
            private_json(output / "status.json", {**stats, "at": now(), **status})
            raw.flush()
            await asyncio.sleep(1)
        if slots is not None:
            slots.check_reviews()
            await slots.quiesce_reviews()
            slots.control.close_submissions()
            slots.poll_controls()
        elif isinstance(runtime, Runtime) and runtime.review_task is not None:
            runtime.review_task.cancel()
        for producer in (collector, streamer):
            if producer.done():
                producer.result()
            if not adaptive or producer is streamer:
                producer.cancel()
        if adaptive:
            await collector
        await asyncio.gather(collector, streamer, return_exceptions=True)
        if consumer.done():
            consumer.result()
        drain = asyncio.create_task(runtime.queue.join())
        done, _ = await asyncio.wait({consumer, drain}, return_when=asyncio.FIRST_COMPLETED)
        if consumer in done:
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
            consumer.result()
        await drain
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        at = now()
        if slots is not None:
            slots.check_reviews()
            await slots.marker(
                "session_close" if campaign_seeds is not None else "final_advance", at
            )
        else:
            assert isinstance(runtime, Runtime)
            if journal is not None:
                await runtime.mark_advance("final_advance", at)
            else:
                marker = {"sequence": runtime.sequence, "kind": "final_advance"}
                at = runtime.processing_clock.stamp(marker, at, record)
                for account in runtime.accounts.values():
                    await account.advance(at)
        await runtime.close()
        stats.update(state="complete", at=now(), **runtime.status())
        checks = {
            "stream_observed": stats.get("market_messages", 0) > 0,
            "features_observed": runtime.counts[
                "feature" if slots is not None else "features_valid"
            ]
            > 0,
            "rest_delay_did_not_stop_books": any(
                x["kind"] == "rest_delay" and x["book_events_during"] > 0 for x in stats["faults"]
            ),
            "reconnected": len({a["connection"] for a in stats["acks"]}) >= 2,
            "queue_p95_under_one_second": latency_summary(runtime.queue_delays).get("p95", math.inf)
            < 1,
            "subscription_replaced": len({tuple(a["symbols"]) for a in stats["acks"]}) >= 2,
            "protected_subscription_retained": all(
                set(a["protected"]) <= set(a["symbols"]) for a in stats["acks"]
            ),
            "cash_not_overreserved": all(
                account.reserved_cash <= account.book.cash + 1e-9
                for pair in (
                    [slot.runtime for slot in slots.slots.values()]
                    if slots is not None
                    else [runtime]
                )
                if isinstance(pair, Runtime)
                for account in pair.accounts.values()
            ),
        }
        if adaptive:
            for key in ("rest_delay_did_not_stop_books", "reconnected", "subscription_replaced"):
                del checks[key]
            if slots is not None:
                rows = stats["shared_slots"]
                checks["matched_inputs"] = bool(rows) and all(
                    row["comparison"]["inputs_aligned"] for row in rows
                )
            else:
                checks["matched_inputs"] = runtime.status()["comparison"]["inputs_aligned"]
        stats.update(checks=checks, result="passed" if all(checks.values()) else "inconclusive")
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        OverflowError,
        RuntimeError,
        SafetyError,
        ApiError,
        WebSocketException,
        CampaignCutoff,
    ) as error:
        stats.update(state="failed", at=now(), **runtime.status(), **error_details(error, token))
        record("failed", **error_details(error, token))
        raise SystemExit("Live validation failed; inspect sanitized status") from None
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runtime.close()
        if stats.get("state") not in ("complete", "failed", "interrupted"):
            stats.update(state="interrupted", ended_at=now(), **runtime.status())
        elif stats.get("state") == "complete":
            stats.setdefault("ended_at", stats.get("at", now()))
        stats.update(**runtime.status())
        if journal is not None:
            stats["journal"] = journal.evidence()
        private_json(output / "http-audit.json", {"requests": http_audit})
        files.close()
        comparison = (
            write_comparison(output, stats, runtime) if isinstance(runtime, Runtime) else None
        )
        if slots is not None:
            assert journal is not None
            source_unchanged = all(
                (ROOT / name).is_file()
                and hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected
                for name, expected in journal.identity["source_sha256"].items()
            )
            stats["shared_feed"] = {
                "complete": stats["state"] == "complete" and source_unchanged,
                "source_unchanged": source_unchanged,
            }
            if stats["state"] == "complete":
                slots.control.snapshot(stats["shared_slots"])
                if not source_unchanged:
                    stats.update(state="failed", result="inconclusive")
        if comparison is not None:
            stats.update(
                state=comparison["state"],
                result=comparison.get("result") or stats.get("result"),
                **runtime.status(),
            )
            stats.setdefault("comparison", {}).update(
                complete=comparison["complete"],
                source_unchanged=comparison["source_unchanged"],
                freeze_matches=comparison["freeze_matches"],
            )
        if journal is not None and stats["state"] == "complete":
            journal.complete()
        private_json(output / "status.json", stats)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration", type=int, default=1200)
    parser.add_argument("--capital", type=float, default=100)
    parser.add_argument("--candidate-policy", choices=("router", *FAMILIES))
    parser.add_argument("--profile", choices=("legacy", "broker-constrained"), default="legacy")
    parser.add_argument("--market", choices=MARKETS, default="US")
    parser.add_argument("--adaptive", action="store_true")
    parser.add_argument("--end-at", help="Absolute adaptive experiment cutoff with UTC offset")
    parser.add_argument("--journal", type=Path, help="Durable journal for an adaptive campaign")
    parser.add_argument(
        "--slot-control", type=Path, help="Enable shared slots with this command spool"
    )
    parser.add_argument(
        "--campaign-seeds",
        type=Path,
        help="Enable recurring sessions with slot continuations from this JSON file",
    )
    parser.add_argument(
        "--resume", action="store_true", help="Recover an existing campaign journal"
    )
    args = parser.parse_args()
    if args.slot_control is not None and (args.journal is None or not args.adaptive):
        parser.error("--slot-control requires --adaptive and --journal")
    campaign_seeds = None
    if args.campaign_seeds is not None:
        try:
            if args.slot_control is None:
                raise ValueError("--campaign-seeds requires --slot-control")
            campaign_seeds = json.loads(args.campaign_seeds.read_text())
            validate_session_seeds(campaign_seeds)
        except (OSError, TypeError, ValueError) as error:
            parser.error(str(error))
    if args.market == "KR" and args.profile != "broker-constrained":
        parser.error("--market KR requires --profile broker-constrained")
    if (args.journal is not None and not args.adaptive) or (args.resume and args.journal is None):
        parser.error("--journal requires --adaptive and --resume requires --journal")
    if (
        (not args.adaptive and args.duration < 360)
        or not math.isfinite(args.capital)
        or args.capital <= 0
    ):
        parser.error("Use duration >= 360 and positive finite capital")
    end_epoch = None
    if args.adaptive:
        if args.profile != "broker-constrained" or args.end_at is None:
            parser.error("--adaptive requires --profile broker-constrained and --end-at")
        try:
            end_epoch = seconds(args.end_at)
        except ValueError:
            parser.error("--end-at must be an ISO timestamp with UTC offset")
        if end_epoch <= time.time():
            parser.error("--end-at must be in the future")
    elif args.end_at is not None:
        parser.error("--end-at requires --adaptive")
    if args.profile == "broker-constrained" and args.candidate_policy is not None:
        parser.error("The broker-constrained profile runs a single trend baseline")

    async def run() -> None:
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        if args.adaptive:
            assert task is not None
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            await campaign(
                args.output.resolve(),
                args.duration,
                args.capital,
                candidate_policy=args.candidate_policy,
                profile=args.profile,
                adaptive=args.adaptive,
                end_epoch=end_epoch,
                journal_path=args.journal.resolve() if args.journal else None,
                slot_control=args.slot_control.resolve() if args.slot_control else None,
                resume=args.resume,
                market=MARKETS[args.market],
                campaign_seeds=campaign_seeds,
            )
        finally:
            if args.adaptive:
                loop.remove_signal_handler(signal.SIGTERM)

    try:
        asyncio.run(run())
    except asyncio.CancelledError:
        raise SystemExit("Live validation interrupted") from None
    except JournalError:
        raise SystemExit("Live validation journal unavailable or incompatible") from None


if __name__ == "__main__":
    main()
