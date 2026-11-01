"""Causal online strategy selectors over shared daily shadow portfolios."""

from __future__ import annotations

import math
import random
from collections import deque
from datetime import timedelta
from typing import Any

from tools.search_portfolios import INITIAL, Book, parse, regular

MODES = frozenset(
    {"fixed30", "static", "random", "daily", "cumulative", "rolling", "previous_day", "prior_days"}
)


class WeekEngine:
    """Replay one market; actual policy accounts never inherit shadow-account wealth."""

    def __init__(
        self,
        configs: list[dict[str, Any]],
        specs: list[dict[str, Any]],
        fixed_ids: dict[str, list[int]],
        policies: list[dict[str, Any]],
        *,
        market: str,
        detailed: bool = True,
        seed: int = 20261102,
    ) -> None:
        if market not in ("KR", "US"):
            raise ValueError("Unknown market")
        self.market, self.market_index = market, int(market == "US")
        self.configs = sorted(configs, key=lambda config: config["id"])
        self.config_by_id = {config["id"]: config for config in self.configs}
        if len(self.config_by_id) != len(self.configs) or not self.configs:
            raise ValueError("Configurations must have distinct identifiers")
        self.specs, self.detailed = specs, detailed
        self.policies = policies
        self.needs_shadows = any(
            policy["mode"] in MODES - {"fixed30", "static", "random"} for policy in policies
        )
        if len({policy["id"] for policy in policies}) != len(policies):
            raise ValueError("Hypothesis identifiers must be distinct")
        self.day: str | None = None
        self.scopes: dict[str, Any] = {}
        self.prices: dict[int, float] = {}
        self.times: dict[int, str] = {}
        self.last_at: str | None = None
        self.last_tick = -1
        self.day_steps = 0
        self.day_open = False
        self.shadows: list[Book] = []
        self.completed_scores: list[list[float]] = []
        self.completed_sum = [0.0] * len(self.configs)
        self.shadow_daily_scores: list[dict[str, Any]] = []
        self.history: deque[tuple[str, list[float]]] = deque(
            maxlen=max((int(p.get("window", 1)) for p in policies), default=1) + 2
        )
        self.states: list[dict[str, Any]] = []
        self.index_by_id = {config["id"]: index for index, config in enumerate(self.configs)}
        for policy in policies:
            mode = policy["mode"]
            if mode not in MODES or int(policy.get("refresh_ticks", 1)) < 1:
                raise ValueError("Invalid hypothesis mode or refresh interval")
            if mode == "rolling" and int(policy.get("window", 0)) < 1:
                raise ValueError("Rolling score needs a positive check window")
            eligible = [
                config
                for config in self.configs
                if not policy.get("families")
                or self.specs[config["signal"]]["family"] in policy["families"]
            ]
            for leverage in sorted({config["leverage"] for config in eligible}):
                candidates = [c["id"] for c in eligible if c["leverage"] == leverage]
                fixed = [
                    identifier
                    for identifier in fixed_ids.get(market, [])
                    if identifier in candidates
                ]
                if mode == "fixed30" and not fixed:
                    raise ValueError("Frozen October 30 reference is missing for this cohort")
                chosen = fixed[0] if mode == "fixed30" else candidates[0]
                self.states.append(
                    {
                        "policy": policy,
                        "leverage": leverage,
                        "candidates": candidates,
                        "chosen": chosen,
                        "book": Book(self.config_by_id[chosen], market, policy["id"], detailed),
                        "owners": {},
                        "choices": [],
                        "daily": [],
                        "past_trades": [],
                        "past_curve": [],
                        "start_equity": INITIAL,
                        "rng": random.Random(
                            f"{policy.get('seed', seed)}:{market}:{policy['id']}:{leverage}"
                        ),
                    }
                )

    def day_start(self, day: str, scopes: dict[str, Any]) -> None:
        if self.day_open or self.day is not None and day <= self.day:
            raise ValueError("Market days must be opened once in increasing order")
        if self.market not in scopes:
            raise ValueError("Missing market scope")
        self.day, self.scopes, self.day_open, self.day_steps = day, scopes, True, 0
        self.shadows = (
            [Book(config, self.market, "daily_shadow") for config in self.configs]
            if self.needs_shadows
            else []
        )
        for state in self.states:
            if state["policy"].get("reset_daily", False) and state["daily"]:
                old = state["book"]
                state["past_trades"].extend(
                    {"day": state["daily"][-1]["day"], **trade} for trade in old.trades
                )
                state["past_curve"].extend(
                    {"day": state["daily"][-1]["day"], **point} for point in old.curve
                )
                state["book"] = Book(
                    self.config_by_id[state["chosen"]],
                    self.market,
                    state["policy"]["id"],
                    self.detailed,
                )
                state["owners"] = {}
            state["book"].mark(self.prices)
            state["start_equity"] = state["book"].equity

    def _score(self, mode: str, window: int, current: list[float], at: str) -> list[float]:
        if mode == "daily":
            return [(book.equity - INITIAL) / INITIAL for book in self.shadows]
        if mode == "cumulative":
            return current
        if mode == "previous_day":
            return self.completed_scores[-1] if self.completed_scores else [0.0] * len(current)
        if mode == "prior_days":
            return self.completed_sum
        if mode == "rolling":
            cutoff = parse(at) - timedelta(minutes=window * 5)
            previous = self.history[0][1] if self.history else current
            for stamp, scores in self.history:
                if parse(stamp) > cutoff:
                    break
                previous = scores
            return [now - before for now, before in zip(current, previous, strict=True)]
        raise ValueError("This hypothesis does not use a score vector")

    def _actual_step(
        self,
        state: dict[str, Any],
        frame: dict[str, Any],
        fresh: dict[int, float],
        entry_allowed: bool,
    ) -> None:
        book: Book = state["book"]
        owners: dict[int, int] = state["owners"]
        ranks = frame["ranks"]
        at, tick = frame["at"], frame["tick"]
        for identifier in sorted(set(owners.values())):
            owned = {
                symbol: fresh[symbol]
                for symbol, owner in owners.items()
                if owner == identifier and symbol in fresh
            }
            if not owned:
                continue
            config = self.config_by_id[identifier]
            book.config = config
            curve_length = len(book.curve)
            trade_length = len(book.trades)
            book.step(
                at, tick, owned, self.prices, ranks[config["signal"]][self.market_index], False
            )
            del book.curve[curve_length:]
            for trade in book.trades[trade_length:]:
                trade["configuration"] = identifier
                trade["day"] = self.day
            owners = {symbol: owner for symbol, owner in owners.items() if symbol in book.positions}
            state["owners"] = owners
        config = self.config_by_id[state["chosen"]]
        book.config = config
        available = {
            symbol: value for symbol, value in fresh.items() if symbol not in book.positions
        }
        before = set(book.positions)
        trade_length = len(book.trades)
        book.step(
            at,
            tick,
            available,
            self.prices,
            ranks[config["signal"]][self.market_index],
            entry_allowed and len(book.positions) < config["slots"],
        )
        for symbol in set(book.positions) - before:
            owners[symbol] = config["id"]
        for trade in book.trades[trade_length:]:
            trade["configuration"] = config["id"]
            trade["day"] = self.day
        if book.curve:
            book.curve[-1]["day"] = self.day

    def step(self, frame: dict[str, Any]) -> None:
        if not self.day_open:
            raise ValueError("Open a market day before providing frames")
        at, tick = frame["at"], frame["tick"]
        scope = self.scopes[self.market]
        if not scope["start"] <= at <= scope["end"]:
            return
        if self.last_at is not None and at <= self.last_at or tick <= self.last_tick:
            raise ValueError("Frames must increase in observation time and global check number")
        fresh = {int(symbol): float(value) for symbol, value in frame["quotes"].items()}
        for symbol, mark in frame["marks"].items():
            symbol = int(symbol)
            value, stamp = float(mark[0]), mark[1]
            if stamp > at or stamp < self.times.get(symbol, stamp):
                raise ValueError("Observed mark crosses the current information boundary")
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Observed prices must be positive and finite")
            self.prices[symbol], self.times[symbol] = value, stamp
        if any(
            self.prices.get(symbol) != value or self.times.get(symbol) != at
            for symbol, value in fresh.items()
        ):
            raise ValueError("A fresh execution quote needs an exact current observed mark")
        is_regular = regular(at, self.market, scope)
        ranks = frame["ranks"]
        for book in self.shadows:
            config = book.config
            allowed = at < scope["end"] and (
                config["session"] == "all" or is_regular == (config["session"] == "regular")
            )
            book.step(
                at, tick, fresh, self.prices, ranks[config["signal"]][self.market_index], allowed
            )
        current = [
            self.completed_sum[index] + (book.equity - INITIAL) / INITIAL
            for index, book in enumerate(self.shadows)
        ]
        if not self.history:
            self.history.append((scope["start"], list(self.completed_sum)))
        self.history.append((at, current))
        score_cache: dict[tuple[str, int], list[float]] = {}
        winner_cache: dict[tuple[str, int, int, tuple[str, ...]], int] = {}
        for state in self.states:
            policy = state["policy"]
            mode = policy["mode"]
            if self.day_steps % int(policy.get("refresh_ticks", 1)) == 0:
                choices = state["candidates"]
                if mode == "random":
                    state["chosen"] = state["rng"].choice(choices)
                elif mode not in ("fixed30", "static"):
                    window = int(policy.get("window", 1))
                    key = (mode, window)
                    if key not in score_cache:
                        score_cache[key] = self._score(mode, window, current, at)
                    scores = score_cache[key]
                    cohort = (mode, window, state["leverage"], tuple(policy.get("families", ())))
                    if cohort not in winner_cache:
                        winner_cache[cohort] = min(
                            choices,
                            key=lambda identifier: (
                                -scores[self.index_by_id[identifier]],
                                identifier,
                            ),
                        )
                    state["chosen"] = winner_cache[cohort]
                state["choices"].append(
                    {
                        "day": self.day,
                        "at": at,
                        "tick": tick,
                        "configuration": state["chosen"],
                        "completed_training_days": len(self.completed_scores),
                        "information_through": at,
                        "classification": "retrospective_30_fit" if mode == "fixed30" else "causal",
                    }
                )
            selected = self.config_by_id[state["chosen"]]
            allowed = at < scope["end"] and (
                selected["session"] == "all" or is_regular == (selected["session"] == "regular")
            )
            self._actual_step(state, frame, fresh, allowed)
        self.day_steps += 1
        self.last_at, self.last_tick = at, tick

    def day_end(self) -> dict[str, Any]:
        if not self.day_open or self.last_at is None or not self.day_steps:
            raise ValueError("The day has no observed market frames")
        scores = (
            [(book.equity - INITIAL) / INITIAL for book in self.shadows]
            if self.needs_shadows
            else [0.0] * len(self.configs)
        )
        self.completed_scores.append(scores)
        self.completed_sum = [
            previous + score for previous, score in zip(self.completed_sum, scores, strict=True)
        ]
        self.shadow_daily_scores.append(
            {
                "day": self.day,
                "scores": [
                    [config["id"], score]
                    for config, score in zip(self.configs, scores, strict=True)
                ],
            }
        )
        snapshots = []
        for state in self.states:
            book: Book = state["book"]
            summary = book.summary(self.prices, self.times, self.last_at)
            start = state["start_equity"]
            snapshot = {
                "day": self.day,
                "policy": state["policy"]["id"],
                "market": self.market,
                "leverage": state["leverage"],
                "start_equity": start,
                "end_equity": summary["equity"],
                "pnl": summary["equity"] - start,
                "return_pct": (summary["equity"] / start - 1) * 100 if start > 0 else None,
                "accounting": "independent_daily_accounts"
                if state["policy"].get("reset_daily")
                else "continuous_account",
                "summary": summary,
            }
            state["daily"].append(snapshot)
            snapshots.append(snapshot)
        self.day_open = False
        return {
            "day": self.day,
            "market": self.market,
            "hypotheses": snapshots,
            "shadow_scores": self.shadow_daily_scores[-1],
        }

    def report(self) -> dict[str, Any]:
        hypotheses = []
        for state in self.states:
            book: Book = state["book"]
            reset = state["policy"].get("reset_daily", False)
            daily = state["daily"]
            summary = book.summary(self.prices, self.times, self.last_at) if self.last_at else None
            if reset and daily:
                funded = INITIAL * len(daily)
                ending = sum(row["end_equity"] for row in daily)
                summary = {
                    **daily[-1]["summary"],
                    "initial_equity": funded,
                    "equity": ending,
                    "return_pct": (ending / funded - 1) * 100,
                    "pnl": ending - funded,
                    "continuous_equity": None,
                    "independent_accounts": len(daily),
                    "capital_refills": max(0, len(daily) - 1) * INITIAL,
                }
                for key in (
                    "cash",
                    "debt",
                    "realized",
                    "unrealized",
                    "turnover",
                    "entries",
                    "closed",
                    "wins",
                    "open_positions",
                    "stale_open_positions",
                ):
                    summary[key] = sum(row["summary"][key] for row in daily)
                for key in ("max_drawdown_pct", "gross_exposure_peak", "debt_peak"):
                    summary[key] = max(row["summary"][key] for row in daily)
                summary["ever_nonpositive_equity"] = any(
                    row["summary"]["ever_nonpositive_equity"] for row in daily
                )
            hypotheses.append(
                {
                    "id": state["policy"]["id"],
                    "policy": state["policy"],
                    "market": self.market,
                    "leverage": state["leverage"],
                    "summary": summary,
                    "classification": "retrospective_30_fit"
                    if state["policy"]["mode"] == "fixed30"
                    else "causal_replay",
                    "accounting": "independent_daily_accounts" if reset else "continuous_account",
                    "daily": daily,
                    "choices": state["choices"],
                    "trades": state["past_trades"]
                    + [{"day": self.day, **trade} for trade in book.trades],
                    "curve": state["past_curve"]
                    + [{"day": self.day, **point} for point in book.curve],
                    "open_position_origins": dict(state["owners"]),
                }
            )
        return {
            "market": self.market,
            "days": [row["day"] for row in self.shadow_daily_scores],
            "hypotheses": hypotheses,
            "shadow_daily_scores": self.shadow_daily_scores,
            "learning_score": "Sum of daily shadow P&L divided by the original daily capital; "
            "not a continuous shadow portfolio or compounded account return.",
            "actual_accounting": "Actual accounts retain their own cash, liabilities and "
            "positions. Original entry configurations manage open-position exits.",
            "ai_calls": 0,
        }
