"""Market collection, scheduled checks, and virtual execution only."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from .ai import Reviewer
from .api import ApiError, MarketClient
from .audit import Audit
from .clock import now_utc
from .core import (
    NEW_YORK,
    ONE,
    SEOUL,
    SafetyError,
    Session,
    Settings,
    decimal,
    market_date,
    probable_regular,
    regular_session,
    stamp,
)
from .jsonio import paper_json as encode
from .store import Store
from .strategy import (
    aggregate,
    book_prices,
    eligible,
    entry_plan,
    exit_reason,
    fill_price,
    signal,
    trading_paused,
)


class Engine:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        client: MarketClient | None = None,
        reviewer: Reviewer | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.client = client or MarketClient(
            settings, lambda row: store.event("http", row), store=store
        )
        self.reviewer = reviewer or Reviewer(settings, store)
        self.audit = Audit(store)

    def calendar(self, market: str, date: str, at: datetime) -> dict[str, Any]:
        key = f"calendar:{market}:{date}"
        cached = self.store.cache(key)
        if cached and at - cached[0] < timedelta(hours=6):
            return cached[1]
        result = self.client.calendar(market, date)
        self.store.put_cache(key, result, at)
        return result

    def sessions(self, market: str, at: datetime) -> list[Session]:
        calendar = self.calendar(market, market_date(market, at), at)
        days = [calendar["today"], calendar["previousBusinessDay"]]
        previous = self.calendar(market, days[-1]["date"], at)
        days.append(previous["previousBusinessDay"])
        return sorted(
            [session for day in days if (session := regular_session(market, day))],
            key=lambda s: s.start,
        )

    def fx(self, market: str, at: datetime) -> Decimal:
        if market == "US":
            return ONE
        key = "fx:USD:KRW"
        cached = self.store.cache(key)
        if (
            cached
            and isinstance(cached[1], dict)
            and at - cached[0] < timedelta(seconds=60)
            and stamp(cached[1]["valid_until"]) > now_utc()
        ):
            return decimal(cached[1]["rate"])
        result = self.client.get("/api/v1/exchange-rate", baseCurrency="USD", quoteCurrency="KRW")
        observed = now_utc()
        if not result.get("validUntil") or stamp(result["validUntil"]) <= observed:
            raise SafetyError("Fresh USD/KRW market data is required")
        rate = decimal(result["midRate"])
        if rate <= 0:
            raise SafetyError("Invalid FX rate")
        self.store.put_cache(
            key, {"rate": str(rate), "valid_until": result["validUntil"]}, observed
        )
        return rate

    def validate_fx(self, market: str, at: datetime) -> None:
        if market == "US":
            return
        cached = self.store.cache("fx:USD:KRW")
        if not cached or not isinstance(cached[1], dict) or stamp(cached[1]["valid_until"]) <= at:
            raise SafetyError("FX expired before the executable quote was observed")

    def history(self, symbol: str, sessions: list[Session], at: datetime) -> list[Any]:
        config = self.settings.data["strategy"]
        minutes = int(config.get("bar_minutes", 30))
        ranges = []
        remaining = None
        if config.get("profile", "v1") != "v1":
            required = max(
                int(config["atr_period"]) + 1,
                int(config.get("breakout_bars", 6)) + 1,
                int(config.get("relative_minutes", 60)) // minutes + 1,
            )
            remaining = (required + 1) * minutes
        if sessions:
            revision_key = "history_day:" + symbol
            revision = self.store.cache(revision_key)
            if revision is None or revision[1] != sessions[-1].date:
                with self.store.transaction(write=True):
                    self.store.db.execute("DELETE FROM candles WHERE symbol=?", (symbol,))
                    self.store.put_cache(revision_key, sessions[-1].date, at)
        for session in reversed(sessions):
            end = min(session.end, at.replace(second=0, microsecond=0))
            if end <= session.start:
                continue
            start = (
                max(session.start, end - timedelta(minutes=remaining))
                if remaining is not None
                else session.start
            )
            ranges.append((start, end))
            cached = self.store.candles(symbol, start, end)
            expected = int((end - start).total_seconds() // 60)
            if remaining is not None:
                remaining -= expected
            present = {stamp(row["timestamp"]) for row in cached}
            missing = [
                start + timedelta(minutes=index)
                for index in range(expected)
                if start + timedelta(minutes=index) not in present
            ]
            if not missing:
                if remaining is not None and remaining <= 0:
                    break
                continue
            cursor = (missing[-1] + timedelta(minutes=1)).isoformat()
            cursors = set()
            while cursor not in cursors:
                cursors.add(cursor)
                result = self.client.candles(symbol, cursor)
                rows = [
                    {
                        **row,
                        "source_timestamp": row["timestamp"],
                        "timestamp": (stamp(row["timestamp"]) - timedelta(minutes=1)).isoformat(),
                    }
                    for row in result["candles"]
                ]
                if not rows:
                    break
                self.store.put_candles(
                    symbol,
                    [row for row in rows if stamp(row["source_timestamp"]) <= at],
                )
                oldest = min(stamp(row["timestamp"]) for row in rows)
                if oldest <= start:
                    break
                next_cursor = result.get("nextBefore")
                if not next_cursor or stamp(next_cursor) >= stamp(cursor):
                    break
                cursor = next_cursor
            if remaining is not None and remaining <= 0:
                break
        if not ranges:
            return []
        rows = self.store.candles(symbol, min(start for start, _end in ranges), at)
        return aggregate(rows, sessions, at, minutes=minutes)

    def resolve_settlements(self, at: datetime) -> None:
        rows = self.store.db.execute(
            "SELECT id,market,sale_date FROM cash WHERE available_at IS NULL",
        ).fetchall()
        for row in rows:
            try:
                market, date = row[1], row[2]
                steps = 1 if market == "US" else 2
                day = None
                for _ in range(steps):
                    calendar = self.calendar(market, date, at)
                    day = calendar["nextBusinessDay"]
                    date = day["date"]
                settlement = regular_session(market, day or {})
                if settlement is None:
                    raise SafetyError("Settlement calendar is unavailable; proceeds remain locked")
                self.store.db.execute(
                    "UPDATE cash SET available_at=? WHERE id=?",
                    (settlement.start.isoformat(), row[0]),
                )
                self.audit.recovered("settlement", cash_id=row[0])
            except Exception as error:  # noqa: BLE001 - keep other settlements independent
                self.audit.exception("settlement", error, cash_id=row[0], market=row[1])

    def inspect_positions(self, market: str, session: Session, at: datetime) -> None:
        if not session.can_fill(at):
            self.audit.record(
                "market", "auction_or_closed", {"market": market}, state="waiting_market"
            )
            return
        for position in self.store.positions():
            if position["market"] != market:
                continue
            symbol = position["symbol"]
            try:
                if not self.check_actions(position, session):
                    continue
                quantity = decimal(position["quantity"])
                if quantity != quantity.to_integral_value():
                    raise SafetyError(
                        "Fractional corporate-action entitlement requires cash-in-lieu evidence"
                    )
                warnings = self.client.warnings(symbol)
                stock = self.client.stocks([symbol])[0]
                if trading_paused(stock, warnings):
                    raise SafetyError("Trading pause prevents immediate execution")
                fx = self.fx(market, now_utc())
                book = self.client.book(symbol)
                observed = now_utc()
                book_prices(
                    book,
                    observed,
                    int(self.settings.get("limits", "quote_age_seconds")),
                    "USD" if market == "US" else "KRW",
                )
                if not session.can_fill(observed):
                    raise SafetyError("The continuous session ended before the quote was observed")
                self.validate_fx(market, observed)
                executable = fill_price(book, int(quantity), buying=False)
                value = (
                    quantity
                    * executable
                    / fx
                    * (ONE - self.settings.fee_rate(market, selling=True))
                )
                self.store.mark(symbol, value, observed)
                reason = exit_reason(position, executable, observed, session, self.settings)
                if reason:
                    self.store.sell(
                        symbol,
                        value,
                        executable,
                        f"exit:{symbol}:{position['entered_at']}",
                        reason,
                        observed,
                        market_date(market, observed),
                    )
                    self.store.event(
                        "paper_exit",
                        {
                            "symbol": symbol,
                            "reason": reason,
                            "price": executable,
                            "quote_at": book["timestamp"],
                        },
                    )
                self.audit.recovered("position", symbol=symbol)
            except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
                self.audit.exception("position", error, symbol=symbol, market=market)
                self.store.event(
                    "position_not_filled", {"symbol": symbol, "reason": type(error).__name__}
                )

    def action_anchor(self, symbol: str, market: str, at: datetime) -> dict[str, Any] | None:
        try:
            previous = self.calendar(market, market_date(market, at), at)["previousBusinessDay"][
                "date"
            ]
            before = (
                datetime.fromisoformat(previous + "T23:59:59")
                .replace(tzinfo=SEOUL if market == "KR" else NEW_YORK)
                .isoformat()
            )
            raw = self.client.daily(symbol, before, False)["candles"]
            adjusted = self.client.daily(symbol, before, True)["candles"]
            indexed = {row["timestamp"]: row for row in adjusted}
            row = next(row for row in raw if row["timestamp"] in indexed)
            factor = decimal(row["closePrice"]) / decimal(indexed[row["timestamp"]]["closePrice"])
            return {
                "timestamp": row["timestamp"],
                "raw_close": row["closePrice"],
                "factor": str(factor),
            }
        except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
            self.audit.exception("corporate_action", error, symbol=symbol, market=market)
            return None

    def check_actions(self, position: dict[str, Any], session: Session) -> bool:
        if (
            position.get("corporate_checked_date") == session.date
            or position["session_date"] == session.date
        ):
            return True
        symbol = position["symbol"]
        anchor = position.get("action_anchor")
        if not anchor:
            self.audit.record(
                "corporate_action",
                "missing_anchor",
                {"symbol": symbol, "market": session.market},
                state="needs_evidence",
            )
            return False
        current = self.client.daily(
            symbol, (stamp(anchor["timestamp"]) + timedelta(seconds=1)).isoformat(), True
        )["candles"]
        row = next(
            (row for row in current if stamp(row["timestamp"]) == stamp(anchor["timestamp"])), None
        )
        if row is None:
            raise SafetyError("Corporate-action reference price is unavailable")
        factor = decimal(anchor["raw_close"]) / decimal(row["closePrice"])
        if abs(factor / decimal(anchor["factor"]) - ONE) > decimal("0.000001"):
            self.audit.record(
                "corporate_action",
                "adjustment_requires_evidence",
                {
                    "symbol": symbol,
                    "market": session.market,
                    "old_factor": anchor["factor"],
                    "new_factor": str(factor),
                },
                state="needs_evidence",
            )
            return False
        position["corporate_checked_date"] = session.date
        self.store.db.execute(
            "UPDATE positions SET data=? WHERE symbol=?", (encode(position), symbol)
        )
        self.audit.recovered("corporate_action", symbol=symbol)
        return True

    def shortlist(
        self, market: str, at: datetime, fx: Decimal, *, refresh: bool = False
    ) -> list[dict[str, Any]]:
        snapshot_key = "universe_snapshot:" + market
        snapshot = self.store.cache(snapshot_key)
        if not refresh and snapshot and 0 <= (at - snapshot[0]).total_seconds() < 60:
            return snapshot[1]
        priority: dict[str, int] = {}
        for kind in ("MARKET_TRADING_AMOUNT", "MARKET_TRADING_VOLUME", "TOP_GAINERS", "TOP_LOSERS"):
            result = self.client.ranking(market, kind, 100)
            if (
                not result.get("rankedAt")
                or not -5 <= (now_utc() - stamp(result["rankedAt"])).total_seconds() <= 180
            ):
                self.store.event(
                    "ranking_not_used",
                    {
                        "market": market,
                        "kind": kind,
                        "ranked_at": result.get("rankedAt"),
                        "reason": "stale_or_missing_observation_time",
                    },
                )
                continue
            for item in result["rankings"]:
                priority.setdefault(item["symbol"], len(priority))
        key = f"listed:{market}:{market_date(market, at)}"
        cached = self.store.cache(key)
        if cached:
            listings = cached[1]
        else:
            exchanges = (
                ("KOSPI", "KOSDAQ", "KR_ETC")
                if market == "KR"
                else ("NYSE", "NASDAQ", "AMEX", "US_ETC")
            )
            listings = [row for exchange in exchanges for row in self.client.listed(exchange)]
            self.store.put_cache(key, listings, at)
        by_symbol = {row["symbol"]: row for row in listings}
        symbols = sorted(
            by_symbol, key=lambda symbol: (priority.get(symbol, len(priority)), symbol)
        )
        selected = []
        cash = self.store.total_cash()
        for offset in range(0, len(symbols), 200):
            prices = self.client.prices(symbols[offset : offset + 200])
            for price in prices:
                symbol = price["symbol"]
                if symbol not in by_symbol:
                    continue
                try:
                    stock = {**by_symbol[symbol], "status": "ACTIVE", "currency": price["currency"]}
                    if eligible(
                        stock, decimal(price["lastPrice"]), market, cash, fx, self.settings
                    ):
                        selected.append({"symbol": symbol, "stock": stock, "price": price})
                except Exception as error:  # noqa: BLE001 - reject only the malformed quote
                    self.audit.exception("universe_quote", error, symbol=symbol, market=market)
        selected.sort(
            key=lambda item: (priority.get(item["symbol"], len(priority)), item["symbol"])
        )
        self.store.event(
            "universe",
            {
                "market": market,
                "listed": len(by_symbol),
                "eligible": len(selected),
                "detailed": [row["symbol"] for row in selected],
            },
        )
        self.store.put_cache(snapshot_key, selected, now_utc())
        return selected

    def scan(self, market: str, at: datetime) -> list[dict[str, Any]]:
        if self.settings.data.get("simulation", {}).get("enabled"):
            from .simulation import Simulation

            return Simulation(self.settings, self.store).propose(market, enqueue=False)
        return list(self.scan_iter(market, at))

    def scan_iter(
        self, market: str, at: datetime, manage_positions: bool = False
    ) -> Iterator[dict[str, Any]]:
        sessions = self.sessions(market, at)
        fx = self.fx(market, at)
        checkpoint_key = f"scan_pending:{market}"
        pending = self.store.cache(checkpoint_key)
        items = (
            pending[1]
            if pending and pending[1] and market_date(market, pending[0]) == market_date(market, at)
            else self.shortlist(market, at, fx)
        )
        saved_index = self.store.cache(checkpoint_key + ":index")
        start_index = int(saved_index[1]) if pending and items == pending[1] and saved_index else 0
        self.store.put_cache(checkpoint_key, items, at)
        benchmark = "SPY" if market == "US" else "069500"
        relative_minutes = int(self.settings.data["strategy"].get("relative_minutes", 60))
        benchmark_bars = self.history(benchmark, sessions, at)
        if len(benchmark_bars) < 3:
            self.store.event("warmup", {"symbol": benchmark, "completed_bars": len(benchmark_bars)})
            return
        benchmark_anchor = next(
            (
                bar
                for bar in benchmark_bars
                if bar.at == benchmark_bars[-1].at - timedelta(minutes=relative_minutes)
            ),
            None,
        )
        if benchmark_anchor is None:
            return
        benchmark_return = benchmark_bars[-1].close / benchmark_anchor.close - ONE
        held = {position["symbol"] for position in self.store.positions()}
        position_checked_at = at
        found = []
        for item_index, item in enumerate(items[start_index:], start=start_index):
            observed = now_utc()
            active = next((session for session in sessions if session.contains(observed)), None)
            if active is None or not active.entry_allowed(
                observed, int(self.settings.get("strategy", "entry_cutoff_minutes"))
            ):
                break
            if manage_positions and observed - position_checked_at >= timedelta(minutes=15):
                self.inspect_positions(market, active, observed)
                self.resolve_settlements(observed)
                position_checked_at = observed
            symbol = item["symbol"]
            self.store.put_cache(checkpoint_key + ":index", item_index, now_utc())
            self.store.heartbeat(
                self.store.role,
                getattr(self.store, "owner", "manual"),
                "scanning",
                symbol=symbol,
                market=market,
            )
            if symbol in held:
                continue
            try:
                bars = self.history(symbol, sessions, observed)
                self.audit.recovered("candidate", symbol=symbol, market=market)
                self.audit.recovered("service", role=self.store.role)
                if bars and bars[-1].at != benchmark_bars[-1].at:
                    benchmark_bars = self.history(benchmark, sessions, observed)
                    if not benchmark_bars:
                        continue
                    benchmark_anchor = next(
                        (
                            bar
                            for bar in benchmark_bars
                            if bar.at == benchmark_bars[-1].at - timedelta(minutes=relative_minutes)
                        ),
                        None,
                    )
                    if benchmark_anchor is None:
                        continue
                    benchmark_return = benchmark_bars[-1].close / benchmark_anchor.close - ONE
                if not bars or bars[-1].at != benchmark_bars[-1].at:
                    continue
                diagnostics: dict[str, Any] = {}
                candidate = signal(
                    symbol,
                    market,
                    bars,
                    benchmark_return,
                    observed,
                    self.settings,
                    diagnostics=diagnostics,
                )
                self.store.event(
                    "signal_evaluation", {"symbol": symbol, "market": market, **diagnostics}
                )
                if candidate is None:
                    continue
                warnings = self.client.warnings(symbol)
                stock = self.client.stocks([symbol])[0]
                if trading_paused(stock, warnings):
                    continue
                fx = self.fx(market, now_utc())
                book = self.client.book(symbol)
                observed = now_utc()
                self.validate_fx(market, observed)
                plan = entry_plan(
                    candidate,
                    book,
                    observed,
                    self.store.equity(),
                    self.store.total_cash(),
                    fx,
                    self.settings,
                )
                plan.update(
                    signal_key=candidate["signal_key"] + ":" + book["timestamp"],
                    name=stock.get("englishName") or stock.get("name"),
                    security_type=stock["securityType"],
                    leverage_factor=stock.get("leverageFactor"),
                    warnings=warnings,
                    visible_asks=book["asks"],
                    visible_bids=book["bids"],
                    observed_at=observed.isoformat(),
                    news_verified=False,
                )
                self.audit.recovered("candidate", symbol=symbol, market=market)
                found.append(symbol)
                yield plan
            except Exception as error:
                self.audit.exception("candidate", error, symbol=symbol, market=market)
                self.store.event(
                    "candidate_rejected", {"symbol": symbol, "reason": type(error).__name__}
                )
                if (
                    isinstance(error, ApiError)
                    and error.status not in (400, 404, 422)
                    and error.code != "invalid_response"
                ):
                    raise
        self.store.put_cache(checkpoint_key, [], now_utc())
        self.store.put_cache(checkpoint_key + ":index", 0, now_utc())
        self.store.event("scan", {"market": market, "signals": found})

    def enter_approved(
        self,
        candidates: list[dict[str, Any]],
        decisions: dict[str, dict[str, Any]],
        session: Session,
    ) -> None:
        approved = [
            row for row in candidates if decisions.get(row["symbol"], {}).get("action") == "BUY"
        ]
        for candidate in approved:
            at = now_utc()
            if not session.entry_allowed(
                at, int(self.settings.get("strategy", "entry_cutoff_minutes"))
            ):
                return
            positions = self.store.positions()
            symbol = candidate["symbol"]
            held = {position["symbol"] for position in positions}
            if symbol in held:
                continue
            if at - stamp(candidate["observed_at"]) > timedelta(minutes=5):
                self.store.event("approval_expired", {"symbol": symbol})
                continue
            try:
                warnings = self.client.warnings(symbol)
                current = self.client.stocks([symbol])[0]
                if trading_paused(current, warnings):
                    continue
                if warnings != candidate.get("warnings", []):
                    self.store.event("approval_context_changed", {"symbol": symbol})
                    continue
                anchor = self.action_anchor(symbol, session.market, at)
                if anchor is None:
                    raise SafetyError("Corporate-action baseline is required before entry")
                fx = self.fx(session.market, now_utc())
                book = self.client.book(symbol)
                filled_at = now_utc()
                if not session.entry_allowed(
                    filled_at, int(self.settings.get("strategy", "entry_cutoff_minutes"))
                ):
                    return
                if not -5 <= (filled_at - stamp(candidate["observed_at"])).total_seconds() <= 300:
                    raise SafetyError("Approval expired before the execution quote was observed")
                self.validate_fx(session.market, filled_at)
                position = entry_plan(
                    candidate,
                    book,
                    filled_at,
                    self.store.equity(),
                    self.store.total_cash(),
                    fx,
                    self.settings,
                    quantity=decisions[symbol]["quantity"],
                )
                position.update(
                    entered_at=filled_at.isoformat(),
                    session_date=session.date,
                    rationale=decisions[symbol]["reason"],
                    marked_at=filled_at.isoformat(),
                    action_anchor=anchor,
                    corporate_checked_date=session.date,
                )
                self.store.buy(
                    position,
                    decimal(position["cost_usd"]),
                    "entry:" + candidate["signal_key"],
                    filled_at,
                )
                self.store.event(
                    "paper_entry",
                    {
                        "symbol": symbol,
                        "price": position["entry_price"],
                        "quantity": position["quantity"],
                    },
                )
            except Exception as error:  # noqa: BLE001 - isolate and audit each work unit
                self.audit.exception("execution", error, symbol=symbol, market=session.market)
                self.store.event(
                    "approval_not_filled", {"symbol": symbol, "reason": type(error).__name__}
                )

    def tick(self, at: datetime) -> dict[str, Any]:
        if not self.store.claim_tick(at):
            return {"status": "duplicate_slot", "ai_invoked": False}
        try:
            result = self.collect(at)
            self.store.finish_tick(at, "completed")
            return result
        except BaseException:
            self.store.finish_tick(at, "failed")
            raise

    def collect(self, at: datetime) -> dict[str, Any]:
        if self.settings.data.get("simulation", {}).get("enabled"):
            from .simulation import Simulation

            simulation = Simulation(self.settings, self.store)
            simulation.observe()
            candidates = simulation.propose()
            simulation.inspect()
            return {"status": "free_simulation", "proposals": len(candidates), "ai_invoked": False}
        markets = [market for market in ("KR", "US") if probable_regular(market, at)]
        if not markets:
            if at.minute == 0:
                self.observe_off_hours(at)
            return {"status": "outside_regular_sessions", "ai_invoked": False}
        reviewed = False
        active_markets = []
        for market in markets:
            calendar = self.calendar(market, market_date(market, at), at)
            session = regular_session(market, calendar["today"])
            if session is None or not session.contains(at):
                continue
            active_markets.append(market)
            try:
                self.resolve_settlements(now_utc())
            except (ApiError, SafetyError) as error:
                self.store.event("settlement_unresolved", {"reason": str(error)})
                continue
            if not session.entry_allowed(
                now_utc(), int(self.settings.get("strategy", "entry_cutoff_minutes"))
            ):
                continue
            if self.store.total_cash() <= 0:
                continue
            for candidate in self.scan_iter(market, now_utc(), manage_positions=False):
                self.store.enqueue(candidate)
        if at.minute == 0:
            self.observe_off_hours(at)
        if not active_markets:
            return {"status": "outside_regular_sessions", "ai_invoked": False}
        return {"status": "checked", "markets": active_markets, "ai_decisions_received": reviewed}

    def observe_off_hours(self, at: datetime) -> None:
        local = at.astimezone(SEOUL)
        eastern = at.astimezone(NEW_YORK)
        possible_kr = local.weekday() < 5 and 8 <= local.hour < 20
        possible_us = (eastern.weekday() < 5 and 4 <= eastern.hour < 20) or (
            local.weekday() < 5 and 9 <= local.hour < 18
        )
        for market, possible in (("KR", possible_kr), ("US", possible_us)):
            if not possible:
                continue
            dates = {market_date(market, at), local.date().isoformat()}
            active = []
            for date in sorted(dates):
                today = self.calendar(market, date, at)["today"]
                sessions = today.get("integrated") if market == "KR" else today
                for name in ("dayMarket", "preMarket", "afterMarket"):
                    raw = sessions.get(name) if sessions else None
                    if raw and stamp(raw["startTime"]) <= at < stamp(raw["endTime"]):
                        active.append(name)
            if not active:
                continue
            ranking = self.client.ranking(market, "MARKET_TRADING_AMOUNT", 100)
            self.store.put_cache(f"off_hours:{market}", ranking, at)
            self.store.event(
                "off_hours",
                {
                    "market": market,
                    "sessions": sorted(set(active)),
                    "ranked_at": ranking.get("rankedAt"),
                    "symbols": [row["symbol"] for row in ranking["rankings"]],
                    "ai_invoked": False,
                },
            )
