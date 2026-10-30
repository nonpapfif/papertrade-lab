"""Unrestricted virtual execution with event-driven model consultation."""

from __future__ import annotations

import hashlib
import json
import uuid
from decimal import Decimal
from typing import Any

from .ai import Reviewer
from .api import MarketClient
from .audit import Audit
from .core import ONE, ZERO, SafetyError, Settings, decimal, encode, now_utc
from .store import Store


class Simulation:
    def __init__(
        self, settings: Settings, store: Store, client: Any = None, reviewer: Any = None
    ) -> None:
        self.settings = settings
        self.store = store
        self.audit = Audit(store)
        self.client = client or MarketClient(
            settings, lambda row: store.event("http", row), store=store
        )
        self.reviewer = reviewer or Reviewer(settings, store)

    def fx(self, market: str) -> tuple[Decimal, str]:
        if market == "US":
            return ONE, "USD_base_currency"
        cached = self.store.cache("simulation_fx") or self.store.cache("fx:USD:KRW")
        if cached and (now_utc() - cached[0]).total_seconds() < 60:
            data = cached[1]
            return decimal(data["rate"] if isinstance(data, dict) else data), "cached_reference"
        try:
            data = self.client.get("/api/v1/exchange-rate", baseCurrency="USD", quoteCurrency="KRW")
            rate = decimal(data["midRate"])
            if rate <= 0:
                raise SafetyError("Invalid reference FX rate")
            self.store.put_cache("simulation_fx", {"rate": str(rate)}, now_utc())
            return rate, "observed_reference"
        except Exception as error:
            self.audit.exception("simulation_fx", error, market=market)
            if cached:
                data = cached[1]
                return decimal(
                    data["rate"] if isinstance(data, dict) else data
                ), "last_known_reference"
            raise

    def save_quote(
        self,
        market: str,
        row: dict[str, Any],
        fx: Decimal,
        fx_basis: str,
        *,
        lead: bool = False,
        ranking_evidence: dict[str, Any] | None = None,
    ) -> Decimal:
        symbol = row["symbol"]
        price = decimal(row["lastPrice"])
        if price <= 0:
            raise SafetyError("A positive reference price is required for arithmetic")
        previous = self.store.db.execute(
            "SELECT data FROM simulation_quotes WHERE symbol=?", (symbol,)
        ).fetchone()
        old = json.loads(previous[0]) if previous else {}
        change = price / decimal(old["price"]) - ONE if old else ZERO
        samples = int(old.get("samples", 0))
        noise = decimal(old.get("noise", "0"))
        old_change = decimal(old.get("last_return", "0"))
        pattern = None
        if change and (not noise or abs(change) >= noise):
            pattern = (
                "reversal"
                if change * old_change < 0
                else "acceleration"
                if abs(change) > abs(old_change) and old_change * change > 0
                else "momentum"
            )
            pattern += ":up" if change > 0 else ":down"
        elif not old and lead:
            pattern = "initial_strategy_review"
        sequence = int(old.get("sequence", 0))
        proposal = old.get("proposal")
        if pattern and pattern != old.get("pattern"):
            sequence += 1
            key = (
                "simulation:"
                + hashlib.sha256(encode([symbol, sequence, pattern]).encode()).hexdigest()
            )
            proposal = {
                "symbol": symbol,
                "market": market,
                "signal_key": key,
                "observed_at": now_utc().isoformat(),
                "hypothesis": pattern,
                "entry_price": str(price),
                "atr": "0",
                "relative_return": str(change),
            }
        data = {
            **old,
            "symbol": symbol,
            "market": market,
            "price": str(price),
            "currency": row.get("currency", "USD" if market == "US" else "KRW"),
            "source_timestamp": row.get("timestamp"),
            "observed_at": now_utc().isoformat(),
            "fx": str(fx),
            "fx_basis": fx_basis,
            "reference_kind": row.get("reference_kind", "api_last_trade"),
            "ranking_evidence": ranking_evidence
            if ranking_evidence is not None
            else old.get("ranking_evidence", {}),
            "noise": str((noise * samples + abs(change)) / (samples + 1)),
            "samples": samples + 1,
            "last_return": str(change if change else old_change),
            "sequence": sequence,
            "pattern": pattern or old.get("pattern"),
            "proposal": proposal,
        }
        self.store.db.execute(
            "INSERT OR REPLACE INTO simulation_quotes VALUES (?,?,?)",
            (symbol, market, encode(data)),
        )
        return change

    def observe(self) -> None:
        started = now_utc()
        for market, exchanges in (
            ("KR", ("KOSPI", "KOSDAQ", "KR_ETC")),
            ("US", ("NYSE", "NASDAQ", "AMEX", "US_ETC")),
        ):
            try:
                fx, basis = self.fx(market)
                listings: list[dict[str, Any]] = []
                for exchange in exchanges:
                    for status in ("ACTIVE", "SCHEDULED", "DELISTED"):
                        key = f"simulation_list:{exchange}:{status}:{started.date()}"
                        cached = self.store.cache(key)
                        if cached is None:
                            result = self.client.listed(exchange, status=status)
                            self.store.put_cache(key, result, now_utc())
                        else:
                            result = cached[1]
                        listings.extend(result)
                symbols = {row["symbol"] for row in listings}
                symbols.update(p["symbol"] for p in self.store.positions() if p["market"] == market)
                leaders: set[str] = set()
                rankings: dict[str, dict[str, Any]] = {}
                for kind in ("MARKET_TRADING_AMOUNT", "TOP_GAINERS", "TOP_LOSERS"):
                    try:
                        ranking = self.client.ranking(market, kind, 100)
                        for row in ranking["rankings"]:
                            leaders.add(row["symbol"])
                            rankings.setdefault(row["symbol"], {})[kind] = {
                                "ranked_at": ranking.get("rankedAt"),
                                "data": row,
                            }
                    except Exception as error:  # noqa: BLE001 - audit this independent simulation operation
                        self.audit.exception("simulation_ranking", error, market=market)
                ordered = sorted(symbols, key=lambda symbol: (symbol not in leaders, symbol))
                received = 0
                movements = []
                for offset in range(0, len(ordered), 200):
                    self.store.heartbeat(
                        self.store.role,
                        self.store.owner,
                        "collecting",
                        market=market,
                        batch=offset // 200,
                    )
                    try:
                        rows = self.client.prices(ordered[offset : offset + 200])
                        with self.store.transaction(write=True):
                            for row in rows:
                                try:
                                    movements.append(
                                        self.save_quote(
                                            market,
                                            row,
                                            fx,
                                            basis,
                                            lead=row["symbol"] in leaders,
                                            ranking_evidence=rankings.get(row["symbol"], {}),
                                        )
                                    )
                                    received += 1
                                except (SafetyError, ValueError, KeyError, TypeError) as error:
                                    self.audit.exception(
                                        "simulation_quote",
                                        error,
                                        symbol=row.get("symbol"),
                                        market=market,
                                    )
                    except Exception as error:  # noqa: BLE001 - audit this independent simulation operation
                        self.audit.exception("simulation_observe", error, market=market)
                self.store.event(
                    "market_check",
                    {
                        "market": market,
                        "started_at": started,
                        "completed_at": now_utc(),
                        "received": received,
                        "market_hours_enforced": False,
                        "ai_invoked": False,
                    },
                )
                total = sum(movements, ZERO)
                noise = sum((value * value for value in movements), ZERO).sqrt()
                regime = (
                    "unchanged"
                    if not noise
                    else "rising"
                    if total > noise
                    else "falling"
                    if total < -noise
                    else "mixed"
                )
                self.store.put_cache("simulation_regime:" + market, regime, now_utc())
                self.audit.recovered("simulation_observe", market=market)
            except Exception as error:  # noqa: BLE001 - audit this independent simulation operation
                self.audit.exception("simulation_observe", error, market=market)

    def set_watches(self, watches: list[dict[str, Any]]) -> None:
        initialized = []
        for watch in watches:
            row = self.store.db.execute(
                "SELECT data FROM simulation_quotes WHERE symbol=?", (watch["symbol"],)
            ).fetchone()
            price = decimal(json.loads(row[0])["price"]) if row else None
            matches = price is not None and (
                (watch.get("above") is not None and price > decimal(watch["above"]))
                or (watch.get("below") is not None and price < decimal(watch["below"]))
            )
            initialized.append(
                {**watch, "id": uuid.uuid4().hex, "was_true": bool(matches), "used": False}
            )
        self.store.put_cache("simulation_watches", initialized, now_utc())

    def propose(self, market: str | None = None, *, enqueue: bool = True) -> list[dict[str, Any]]:
        if (
            enqueue
            and self.store.db.execute(
                "SELECT 1 FROM jobs WHERE status IN ('queued','reviewing')"
            ).fetchone()
        ):
            return []
        initial = self.store.cache("simulation_initialized")
        latest_review = self.store.db.execute(
            "SELECT MAX(id) FROM events WHERE kind='paper_trade_review'"
        ).fetchone()[0]
        previous_review = self.store.cache("simulation_consulted_outcome")
        closed_changed = bool(
            initial and latest_review != (previous_review[1] if previous_review else None)
        )
        manual = self.store.cache("simulation_manual_request")
        previous_manual = self.store.cache("simulation_consulted_request")
        manual_changed = bool(
            manual and (previous_manual is None or manual[1] != previous_manual[1])
        )
        cached_watches = self.store.cache("simulation_watches")
        watches = cached_watches[1] if cached_watches else []
        triggered = set()
        triggered_ids = []
        for watch in watches:
            if watch.get("used"):
                continue
            row = self.store.db.execute(
                "SELECT data FROM simulation_quotes WHERE symbol=?", (watch["symbol"],)
            ).fetchone()
            if not row:
                continue
            price = decimal(json.loads(row[0])["price"])
            matches = (watch.get("above") is not None and price > decimal(watch["above"])) or (
                watch.get("below") is not None and price < decimal(watch["below"])
            )
            if matches and not watch["was_true"]:
                triggered.add(watch["symbol"])
                triggered_ids.append(watch["id"])
                if enqueue:
                    watch["used"] = True
            if enqueue:
                watch["was_true"] = bool(matches)
        if initial and not closed_changed and not manual_changed and not triggered:
            if enqueue:
                self.store.put_cache("simulation_watches", watches, now_utc())
            return []
        if self.store.db.execute(
            "SELECT 1 FROM jobs WHERE status IN ('queued','reviewing')"
        ).fetchone():
            return []
        query = "SELECT data FROM simulation_quotes" + (" WHERE market=?" if market else "")
        rows = self.store.db.execute(query, (market,) if market else ()).fetchall()
        result = []
        context = encode(
            [latest_review, manual[1] if manual else None, sorted(triggered_ids), not bool(initial)]
        )
        outcome_only = closed_changed and not manual_changed and not triggered
        review_symbols = {watch["symbol"] for watch in watches}
        review_symbols.update(position["symbol"] for position in self.store.positions())
        if outcome_only:
            for row in self.store.db.execute(
                "SELECT data FROM events WHERE kind='paper_trade_review' AND id>?",
                (previous_review[1] if previous_review and previous_review[1] else 0,),
            ):
                symbol = json.loads(row[0]).get("symbol")
                if symbol:
                    review_symbols.add(symbol)
        for row in rows:
            quote = json.loads(row[0])
            proposal = quote.get("proposal")
            if triggered and quote["symbol"] not in triggered:
                continue
            if outcome_only:
                if quote["symbol"] not in review_symbols and not quote.get("ranking_evidence"):
                    continue
            elif not triggered and not proposal:
                continue
            key = (
                "simulation-review:"
                + hashlib.sha256(encode([context, quote["symbol"]]).encode()).hexdigest()
            )
            candidate = {
                "symbol": quote["symbol"],
                "market": quote["market"],
                "signal_key": key,
                "observed_at": now_utc().isoformat(),
                "hypothesis": "price_crossing"
                if triggered
                else "closed_trade_review"
                if closed_changed
                else "requested_strategy_review",
                "entry_price": quote["price"],
                "atr": "0",
                "relative_return": quote.get("last_return", "0"),
                "reference": quote,
            }
            result.append(candidate)
        if enqueue and result:
            with self.store.transaction(write=True):
                if self.store.db.execute(
                    "SELECT 1 FROM jobs WHERE status IN ('queued','reviewing')"
                ).fetchone():
                    return []
                for candidate in result:
                    self.store.enqueue(candidate)
                self.store.put_cache("simulation_watches", watches, now_utc())
                self.store.put_cache("simulation_initialized", True, now_utc())
                self.store.put_cache("simulation_consulted_outcome", latest_review, now_utc())
                if manual:
                    self.store.put_cache("simulation_consulted_request", manual[1], now_utc())
                self.store.event(
                    "simulation_consultation_trigger",
                    {
                        "trigger": "price_crossing"
                        if triggered
                        else "closed_trade"
                        if closed_changed
                        else "explicit_or_initial_review",
                        "proposals": len(result),
                        "symbols_triggered": sorted(triggered),
                        "timer_only": False,
                    },
                )
        return result

    def quote(self, symbol: str, market: str, *, refresh: bool = True) -> dict[str, Any]:
        if refresh:
            try:
                rows = self.client.prices([symbol])
                row = next(row for row in rows if row["symbol"] == symbol)
                fx, basis = self.fx(market)
                self.save_quote(market, row, fx, basis)
            except Exception as error:  # noqa: BLE001 - audit this independent simulation operation
                self.audit.exception("simulation_reference", error, symbol=symbol, market=market)
        row = self.store.db.execute(
            "SELECT data FROM simulation_quotes WHERE symbol=?", (symbol,)
        ).fetchone()
        if row is None:
            raise SafetyError("No observed or explicitly supplied reference price exists")
        return json.loads(row[0])

    def fill(
        self,
        symbol: str,
        market: str,
        delta: Decimal,
        event_key: str,
        reason: str,
        plan: dict[str, Any],
        *,
        refresh: bool = True,
        expected_revision: str | None = None,
    ) -> None:
        quote = self.quote(symbol, market, refresh=refresh)
        self.store.simulate(
            symbol,
            market,
            delta,
            decimal(quote["price"]),
            decimal(quote["fx"]),
            event_key,
            reason,
            {
                "reference_price": quote["price"],
                "source_timestamp": quote.get("source_timestamp"),
                "observed_at": quote["observed_at"],
                "fill_at": now_utc().isoformat(),
                "fill_basis": "assumed_fill_at_reference_price",
                "fx_basis": quote["fx_basis"],
                "reference_kind": quote.get("reference_kind", "api_last_trade"),
                "market_hours_liquidity_settlement_ignored": True,
            },
            plan,
            expected_revision=expected_revision,
        )

    def review(self) -> int:
        owner = self.store.owner
        with self.store.transaction(write=True):
            rows = self.store.db.execute(
                "SELECT id,data FROM jobs WHERE status='queued' ORDER BY observed_at"
            ).fetchall()
            candidates = [json.loads(row[1]) for row in rows]
            self.store.db.execute(
                "UPDATE jobs SET status='reviewing',owner=?,attempt=attempt+1 WHERE status='queued'",
                (owner,),
            )
        if not candidates:
            return 0
        snapshots = {row["symbol"]: row for row in self.store.positions()}
        by_symbol = {row["symbol"]: row for row in candidates}
        try:
            for position in self.store.positions():
                if position["symbol"] in by_symbol:
                    continue
                quote = self.quote(position["symbol"], position["market"], refresh=False)
                by_symbol[position["symbol"]] = {
                    "symbol": position["symbol"],
                    "market": position["market"],
                    "signal_key": "position-context:"
                    + position["symbol"]
                    + ":"
                    + str(position.get("revision", position.get("entered_at"))),
                    "entry_price": quote["price"],
                    "atr": "0",
                    "relative_return": quote.get("last_return", "0"),
                    "reference": quote,
                    "hypothesis": "existing_position_context",
                }
            model_inputs = []
            for candidate in by_symbol.values():
                reference = self.quote(candidate["symbol"], candidate["market"], refresh=False)
                model_inputs.append(
                    {
                        "symbol": candidate["symbol"],
                        "market": candidate["market"],
                        "signal_key": candidate["signal_key"],
                        "hypothesis": candidate.get("hypothesis"),
                        "entry_price": reference["price"],
                        "atr": "0",
                        "relative_return": reference.get("last_return", "0"),
                        "reference": {
                            key: reference.get(key)
                            for key in (
                                "price",
                                "currency",
                                "source_timestamp",
                                "observed_at",
                                "fx",
                                "fx_basis",
                                "noise",
                                "ranking_evidence",
                            )
                        },
                    }
                )
            decisions = self.reviewer.review(model_inputs, now_utc())
            call = self.store.db.execute("SELECT MAX(id) FROM ai_calls").fetchone()[0]
            detail = self.store.db.execute(
                "SELECT data FROM ai_calls WHERE id=?", (call,)
            ).fetchone()
            self.set_watches(json.loads(detail[0]).get("review_when", []) if detail else [])
            for index, decision in decisions.items():
                symbol = decision["symbol"]
                candidate = by_symbol[symbol]
                plan = {
                    "stop": decision.get("stop"),
                    "target": decision.get("target"),
                    "strategy": decision["strategy"],
                }
                if decision["action"] in ("BUY", "SELL"):
                    delta = decimal(decision["quantity"]) * (
                        1 if decision["action"] == "BUY" else -1
                    )
                    self.fill(
                        symbol,
                        candidate["market"],
                        delta,
                        f"simulation-ai:{call}:{index}",
                        decision["reason"],
                        plan,
                        expected_revision=snapshots.get(symbol, {}).get("revision"),
                    )
                    current = self.store.db.execute(
                        "SELECT data FROM positions WHERE symbol=?", (symbol,)
                    ).fetchone()
                    snapshots[symbol] = json.loads(current[0]) if current else {}
                elif decision["action"] == "PLAN":
                    with self.store.transaction(write=True):
                        row = self.store.db.execute(
                            "SELECT data FROM positions WHERE symbol=?", (symbol,)
                        ).fetchone()
                        if row:
                            position = json.loads(row[0])
                            position["plan"] = plan
                            self.store.db.execute(
                                "UPDATE positions SET data=? WHERE symbol=?",
                                (encode(position), symbol),
                            )
                            self.store.event(
                                "simulation_plan",
                                {"symbol": symbol, "plan": plan, "reason": decision["reason"]},
                            )
            for row in candidates:
                self.store.finish_job(row["signal_key"], "reviewed", {"decisions": decisions})
            self.audit.recovered("simulation_review")
            return len(candidates)
        except Exception as error:
            for row in candidates:
                self.store.finish_job(row["signal_key"], "failed", {"reason": type(error).__name__})
            self.audit.exception("simulation_review", error)
            raise

    def inspect(self) -> None:
        for position in self.store.positions():
            symbol = position["symbol"]
            try:
                quote = self.quote(symbol, position["market"], refresh=False)
                price = decimal(quote["price"])
                quantity = decimal(position["quantity"])
                plan = position.get("plan", {})
                stop = decimal(plan["stop"]) if plan.get("stop") is not None else None
                target = decimal(plan["target"]) if plan.get("target") is not None else None
                reason = (
                    "optional_stop"
                    if stop is not None and (price <= stop if quantity > 0 else price >= stop)
                    else "optional_target"
                    if target is not None and (price >= target if quantity > 0 else price <= target)
                    else None
                )
                if reason:
                    revision = position.get("revision", position["entered_at"])
                    self.fill(
                        symbol,
                        position["market"],
                        -quantity,
                        f"simulation-exit:{revision}",
                        reason,
                        {},
                        refresh=False,
                        expected_revision=position.get("revision"),
                    )
                else:
                    with self.store.transaction(write=True):
                        current = self.store.db.execute(
                            "SELECT data FROM positions WHERE symbol=?", (symbol,)
                        ).fetchone()
                        if current:
                            latest = json.loads(current[0])
                            latest["mark_usd"] = str(
                                decimal(latest["quantity"]) * price / decimal(quote["fx"])
                            )
                            latest["marked_at"] = now_utc().isoformat()
                            latest["mark_quality"] = "simulation_reference"
                            latest["price_evidence"] = quote
                            self.store.db.execute(
                                "UPDATE positions SET data=? WHERE symbol=?",
                                (encode(latest), symbol),
                            )
                self.audit.recovered("simulation_position", symbol=symbol)
            except Exception as error:  # noqa: BLE001 - audit this independent simulation operation
                self.audit.exception("simulation_position", error, symbol=symbol)
