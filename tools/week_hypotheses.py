"""Causal online strategy selectors over shared daily shadow portfolios."""

from __future__ import annotations

import math
import random
from collections import deque
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

from tools.search_portfolios import INITIAL, Book, parse, regular

MODES = frozenset(
    {
        "fixed30",
        "static",
        "random",
        "daily",
        "cumulative",
        "rolling",
        "previous_day",
        "prior_days",
        "weighted_equal",
        "weighted_linear",
        "weighted_exponential",
    }
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
        long_only: bool = False,
        initial_equity: float = INITIAL,
        whole_shares: bool = False,
    ) -> None:
        if market not in ("KR", "US"):
            raise ValueError("Unknown market")
        self.market, self.market_index = market, int(market == "US")
        self.configs = sorted(configs, key=lambda config: config["id"])
        self.config_by_id = {config["id"]: config for config in self.configs}
        if len(self.config_by_id) != len(self.configs) or not self.configs:
            raise ValueError("Configurations must have distinct identifiers")
        self.specs, self.detailed = specs, detailed
        self.long_only = long_only
        self.initial_equity, self.whole_shares = initial_equity, whole_shares
        if not math.isfinite(initial_equity) or initial_equity <= 0:
            raise ValueError("Initial equity must be positive and finite")
        if whole_shares and (not long_only or any(c["leverage"] != 1 for c in configs)):
            raise ValueError("Whole-share learning requires long-only cash accounts")
        if long_only and any(specs[config["signal"]].get("side") != "long" for config in configs):
            raise ValueError("Long-only learning requires exclusively long-entry configurations")
        self.long_signals = {config["signal"] for config in configs} if long_only else set()
        self.long_candidate_cache: dict[int, list[list[Any]]] = {}
        self.whole_bases: dict[tuple[int, str], list[list[Any]]] = {}
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
            if mode.startswith("weighted_") and int(policy.get("window", 0)) not in (0, 1):
                raise ValueError("Weighted day scores use window zero or one for today's inclusion")
            if policy.get("reset_learning") and (
                not policy.get("reset_daily")
                or mode not in {"static", "random", "daily", "rolling"}
            ):
                raise ValueError("Cold daily learning requires independent daily accounts")
            if policy.get("allocation") not in (None, "winner", "family_ensemble"):
                raise ValueError("Unknown capital allocation")
            if policy.get("allocation") == "family_ensemble" and (
                not long_only or not mode.startswith("weighted_")
            ):
                raise ValueError("Family ensembles require long-only weighted scores")
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
                rng_seed = f"{policy.get('seed', seed)}:{market}:{policy['id']}:{leverage}"
                self.states.append(
                    {
                        "policy": policy,
                        "leverage": leverage,
                        "candidates": candidates,
                        "chosen": chosen,
                        "book": self._new_book(self.config_by_id[chosen], policy["id"], detailed),
                        "owners": {},
                        "choices": [],
                        "daily": [],
                        "past_trades": [],
                        "past_curve": [],
                        "start_equity": self.initial_equity,
                        "rng_seed": rng_seed,
                        "rng": random.Random(rng_seed),
                        "allocations": [],
                    }
                )

    def _new_book(self, config: dict[str, Any], phase: str, detailed: bool) -> Book:
        return Book(
            config,
            self.market,
            phase,
            detailed,
            initial_equity=self.initial_equity,
            whole_shares=self.whole_shares,
        )

    def day_start(self, day: str, scopes: dict[str, Any]) -> None:
        if self.day_open or self.day is not None and day <= self.day:
            raise ValueError("Market days must be opened once in increasing order")
        if self.market not in scopes:
            raise ValueError("Missing market scope")
        self.day, self.scopes, self.day_open, self.day_steps = day, scopes, True, 0
        self.shadows = (
            [self._new_book(config, "daily_shadow", False) for config in self.configs]
            if self.needs_shadows
            else []
        )
        if self.long_only:
            self.history.clear()
        for state in self.states:
            if state["policy"].get("reset_learning"):
                state["chosen"] = state["candidates"][0]
                state["rng"] = random.Random(state["rng_seed"])
                state["allocations"] = []
            if state["policy"].get("reset_daily", False) and state["daily"]:
                old = state["book"]
                state["past_trades"].extend(
                    {"day": state["daily"][-1]["day"], **trade} for trade in old.trades
                )
                state["past_curve"].extend(
                    {"day": state["daily"][-1]["day"], **point} for point in old.curve
                )
                state["book"] = self._new_book(
                    self.config_by_id[state["chosen"]], state["policy"]["id"], self.detailed
                )
                state["owners"] = {}
            state["book"].mark(self.prices)
            state["start_equity"] = state["book"].equity

    def _score(self, mode: str, window: int, current: list[float], at: str) -> list[float]:
        if mode == "daily":
            return [
                (book.equity - self.initial_equity) / self.initial_equity for book in self.shadows
            ]
        if mode == "cumulative":
            return current
        if mode == "previous_day":
            return self.completed_scores[-1] if self.completed_scores else [0.0] * len(current)
        if mode == "prior_days":
            return self.completed_sum
        if mode.startswith("weighted_"):
            days = list(self.completed_scores)
            if window:
                days.append(
                    [
                        (book.equity - self.initial_equity) / self.initial_equity
                        for book in self.shadows
                    ]
                )
            if not days:
                return [0.0] * len(self.configs)
            weights = [
                1
                if mode == "weighted_equal"
                else index + 1
                if mode == "weighted_linear"
                else 2**index
                for index in range(len(days))
            ]
            total = sum(weights)
            return [
                sum(weight / total * day[index] for weight, day in zip(weights, days, strict=True))
                for index in range(len(self.configs))
            ]
        if mode == "rolling":
            cutoff = parse(at) - timedelta(minutes=window * 5)
            previous = self.history[0][1] if self.history else current
            for stamp, scores in self.history:
                if parse(stamp) > cutoff:
                    break
                previous = scores
            observed = (
                [(book.equity - self.initial_equity) / self.initial_equity for book in self.shadows]
                if self.long_only
                else current
            )
            return [now - before for now, before in zip(observed, previous, strict=True)]
        raise ValueError("This hypothesis does not use a score vector")

    def _family_allocations(
        self, state: dict[str, Any], scores: list[float]
    ) -> list[dict[str, Any]]:
        winners: dict[str, int] = {}
        for identifier in state["candidates"]:
            family = self.specs[self.config_by_id[identifier]["signal"]]["family"]
            previous = winners.get(family)
            if previous is None or (-scores[self.index_by_id[identifier]], identifier) < (
                -scores[self.index_by_id[previous]],
                previous,
            ):
                winners[family] = identifier
        maximum = max(scores[self.index_by_id[identifier]] for identifier in winners.values())
        weights = {
            family: math.exp(scores[self.index_by_id[identifier]] - maximum)
            for family, identifier in winners.items()
        }
        total = sum(weights.values())
        return [
            {"family": family, "configuration": identifier, "weight": weights[family] / total}
            for family, identifier in sorted(winners.items())
        ]

    def _long_candidates(self, config: dict[str, Any], frame: dict[str, Any]) -> list[list[Any]]:
        signal = config["signal"]
        if "long_features" not in frame:
            return frame["ranks"][signal][self.market_index]  # type: ignore[no-any-return]
        if signal not in self.long_candidate_cache:
            spec = self.specs[signal]
            rows = []
            for symbol, scores, volumes, noises in frame["long_features"]:
                score = scores[spec["column"]]
                if score is None or not math.isfinite(score) or score <= 0 or score < spec["move"]:
                    continue
                volume, noise = volumes[spec["look"]], noises[spec["look"]]
                if spec["volume"] and (volume is None or volume < spec["volume"]):
                    continue
                if spec["noise"] and (noise is None or noise < spec["noise"]):
                    continue
                rows.append([int(symbol), 1, score])
            if spec["ranking"] == "strongest":
                rows.sort(key=lambda row: (-row[2], row[0]))
            else:
                rows.sort(key=lambda row: (row[2], row[0]))
            self.long_candidate_cache[signal] = rows
        return self.long_candidate_cache[signal]

    def _whole_candidates(
        self,
        config: dict[str, Any],
        frame: dict[str, Any],
        book: Book,
        fresh: dict[int, float],
        budget: float | None,
    ) -> Iterator[list[Any]]:
        spec = self.specs[config["signal"]]
        key = spec["column"], spec["ranking"]
        if key not in self.whole_bases:
            rows = []
            for symbol, scores, volumes, noises in frame["long_features"]:
                score = scores[spec["column"]]
                if score is not None and math.isfinite(score) and score > 0:
                    rows.append(
                        [int(symbol), 1, score, volumes[spec["look"]], noises[spec["look"]]]
                    )
            rows.sort(
                key=lambda row: (-row[2] if spec["ranking"] == "strongest" else row[2], row[0])
            )
            self.whole_bases[key] = rows
        for row in self.whole_bases[key]:
            symbol, _, score, volume, noise = row
            if score < spec["move"]:
                if spec["ranking"] == "strongest":
                    break
                continue
            if spec["volume"] and (volume is None or volume < spec["volume"]):
                continue
            if spec["noise"] and (noise is None or noise < spec["noise"]):
                continue
            if symbol not in fresh:
                continue
            if budget is not None:
                if symbol in book.positions or book.cooldowns.get(symbol, -1) > frame["tick"]:
                    continue
                if fresh[symbol] > budget:
                    book.rejected_unaffordable += 1
                    continue
            yield row[:3]

    def _whole_shadow_step(
        self, book: Book, frame: dict[str, Any], fresh: dict[int, float], allowed: bool
    ) -> None:
        config = book.config
        at, tick = frame["at"], frame["tick"]
        targets = []
        if config["rotate"]:
            for row in self._whole_candidates(config, frame, book, fresh, None):
                targets.append(row)
                if len(targets) == config["slots"]:
                    break
        book.step(at, tick, fresh, self.prices, targets, False)
        if allowed:
            capital = max(book.equity, 0) if config["compound"] else self.initial_equity
            free = max(0, config["slots"] - len(book.positions))
            slot_budget = capital / config["slots"]
            budget = min(book.cash, slot_budget * free)
            candidates = self._whole_candidates(config, frame, book, fresh, budget)
            for row, quantity in book.whole_orders(candidates, fresh, free, slot_budget, budget):
                symbol, _, strength = row
                book.fill(symbol, quantity, fresh[symbol], at, tick, "entry", strength)
        book.mark(self.prices)

    def _winner_entries(
        self, state: dict[str, Any], frame: dict[str, Any], fresh: dict[int, float], allowed: bool
    ) -> None:
        book: Book = state["book"]
        config = self.config_by_id[state["chosen"]]
        book.config = config
        at, tick = frame["at"], frame["tick"]
        book.mark(self.prices)
        free = max(0, config["slots"] - len(book.positions))
        if self.whole_shares:
            capital = max(book.equity, 0) if config["compound"] else self.initial_equity
            slot_budget = capital / config["slots"]
            budget = min(book.cash, slot_budget * free) if allowed else 0.0
            candidates_whole = self._whole_candidates(config, frame, book, fresh, budget)
            for row, quantity in book.whole_orders(
                candidates_whole, fresh, free, slot_budget, budget
            ):
                symbol, _, strength = row
                book.fill(symbol, quantity, fresh[symbol], at, tick, "entry", strength)
                state["owners"][symbol] = config["id"]
                if book.detailed:
                    book.trades[-1].update(configuration=config["id"], day=self.day)
            book.step(at, tick, {}, self.prices, [], False)
            if book.curve:
                book.curve[-1]["day"] = self.day
            return
        candidates = (
            [
                row
                for row in frame["ranks"][config["signal"]][self.market_index]
                if row[0] not in book.positions
                and row[0] in fresh
                and book.cooldowns.get(row[0], -1) <= tick
            ][:free]
            if allowed
            else []
        )
        remaining = max(
            0.0,
            max(book.equity, 0) * state["leverage"]
            - sum(p.quantity * self.prices[s] for s, p in book.positions.items()),
        )
        capital = max(book.equity, 0) if config["compound"] else self.initial_equity
        budget = min(remaining, capital * state["leverage"] / config["slots"] * len(candidates))
        weights = [row[2] if config["sizing"] == "score" else 1.0 for row in candidates]
        total = sum(weights)
        if budget > 0 and total > 0:
            for row, weight in zip(candidates, weights, strict=True):
                symbol, _, strength = row
                book.fill(
                    symbol,
                    budget * weight / total / fresh[symbol],
                    fresh[symbol],
                    at,
                    tick,
                    "entry",
                    strength,
                )
                state["owners"][symbol] = config["id"]
                if book.detailed:
                    book.trades[-1].update(configuration=config["id"], day=self.day)
        book.step(at, tick, {}, self.prices, [], False)
        if book.curve:
            book.curve[-1]["day"] = self.day

    def _ensemble_entries(
        self, state: dict[str, Any], frame: dict[str, Any], fresh: dict[int, float], allowed: bool
    ) -> None:
        book: Book = state["book"]
        at, tick = frame["at"], frame["tick"]
        scope = self.scopes[self.market]
        is_regular = regular(at, self.market, scope)
        book.mark(self.prices)
        equity = max(book.equity, 0)
        remaining = max(
            0.0,
            equity * state["leverage"]
            - sum(
                position.quantity * self.prices[symbol]
                for symbol, position in book.positions.items()
            ),
        )
        for allocation in state["allocations"]:
            config = self.config_by_id[allocation["configuration"]]
            book.config = config
            if not allowed or not (
                config["session"] == "all" or is_regular == (config["session"] == "regular")
            ):
                continue
            family_positions = [
                symbol
                for symbol, owner in state["owners"].items()
                if self.specs[self.config_by_id[owner]["signal"]]["family"] == allocation["family"]
            ]
            free = max(0, config["slots"] - len(family_positions))
            if self.whole_shares:
                capital = equity if config["compound"] else self.initial_equity
                target = capital * allocation["weight"]
                exposure = sum(
                    book.positions[s].quantity * self.prices[s] for s in family_positions
                )
                slot_budget = target / config["slots"]
                budget = max(0.0, min(book.cash, target - exposure, slot_budget * free))
                candidates_whole = self._whole_candidates(config, frame, book, fresh, budget)
                for row, quantity in book.whole_orders(
                    candidates_whole, fresh, free, slot_budget, budget
                ):
                    symbol, _, strength = row
                    book.fill(symbol, quantity, fresh[symbol], at, tick, "entry", strength)
                    state["owners"][symbol] = config["id"]
                    if book.detailed:
                        book.trades[-1].update(configuration=config["id"], day=self.day)
                continue
            candidates = [
                row
                for row in self._long_candidates(config, frame)
                if row[0] not in book.positions
                and row[0] in fresh
                and book.cooldowns.get(row[0], -1) <= tick
            ][:free]
            capital = equity if config["compound"] else self.initial_equity
            target = capital * state["leverage"] * allocation["weight"]
            exposure = sum(book.positions[s].quantity * self.prices[s] for s in family_positions)
            budget = min(
                remaining,
                max(0.0, target - exposure),
                target / config["slots"] * len(candidates),
            )
            weights = [row[2] if config["sizing"] == "score" else 1.0 for row in candidates]
            total = sum(weights)
            if budget <= 0 or total <= 0:
                continue
            for row, weight in zip(candidates, weights, strict=True):
                symbol, side, strength = row
                if side != 1:
                    raise ValueError("A long-only ensemble cannot enter a short position")
                investment = budget * weight / total
                book.fill(
                    symbol, investment / fresh[symbol], fresh[symbol], at, tick, "entry", strength
                )
                state["owners"][symbol] = config["id"]
                if book.detailed:
                    book.trades[-1].update(configuration=config["id"], day=self.day)
                remaining = max(0.0, remaining - investment)
        book.config = self.config_by_id[state["chosen"]]
        book.step(at, tick, {}, self.prices, [], False)
        if book.curve:
            book.curve[-1]["day"] = self.day

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
        if state["policy"].get("allocation") == "family_ensemble":
            self._ensemble_entries(state, frame, fresh, entry_allowed)
            return
        if self.long_only:
            self._winner_entries(state, frame, fresh, entry_allowed)
            return
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
        shared_boundary = (
            self.long_only
            and self.day_steps == 0
            and at == scope["start"]
            and at == self.last_at
            and tick == self.last_tick
        )
        if (
            self.last_at is not None and at <= self.last_at or tick <= self.last_tick
        ) and not shared_boundary:
            raise ValueError("Frames must increase in observation time and global check number")
        self.long_candidate_cache.clear()
        self.whole_bases.clear()
        if self.whole_shares and "long_features" not in frame:
            raise ValueError("Whole-share evaluation requires full current long features")
        if self.long_only and any(
            row[1] != 1
            for signal in self.long_signals
            for row in frame["ranks"][signal][self.market_index]
        ):
            raise ValueError("A long-entry signal contains a non-long direction")
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
            if self.whole_shares:
                self._whole_shadow_step(book, frame, fresh, allowed)
            else:
                book.step(
                    at,
                    tick,
                    fresh,
                    self.prices,
                    ranks[config["signal"]][self.market_index],
                    allowed,
                )
        current = [
            self.completed_sum[index] + (book.equity - self.initial_equity) / self.initial_equity
            for index, book in enumerate(self.shadows)
        ]
        if not self.history:
            baseline = [0.0] * len(current) if self.long_only else list(self.completed_sum)
            self.history.append((scope["start"], baseline))
        observed = (
            [(book.equity - self.initial_equity) / self.initial_equity for book in self.shadows]
            if self.long_only
            else current
        )
        self.history.append((at, observed))
        score_cache: dict[tuple[str, int], list[float]] = {}
        winner_cache: dict[tuple[str, int, int, tuple[str, ...]], int] = {}
        allocation_cache: dict[tuple[str, int, int, tuple[str, ...]], list[dict[str, Any]]] = {}
        for state in self.states:
            policy = state["policy"]
            mode = policy["mode"]
            refresh = (
                self.day_steps == 0
                if policy.get("refresh_day")
                else (self.day_steps % int(policy.get("refresh_ticks", 1)) == 0)
            )
            if refresh:
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
                    if policy.get("allocation") == "family_ensemble":
                        if cohort not in allocation_cache:
                            allocation_cache[cohort] = self._family_allocations(state, scores)
                        state["allocations"] = allocation_cache[cohort]
                state["choices"].append(
                    {
                        "day": self.day,
                        "at": at,
                        "tick": tick,
                        "configuration": state["chosen"],
                        "completed_training_days": 0
                        if policy.get("reset_learning")
                        else len(self.completed_scores),
                        "information_through": at,
                        "classification": "retrospective_30_fit" if mode == "fixed30" else "causal",
                    }
                )
                if policy.get("allocation") == "family_ensemble":
                    state["choices"][-1]["allocations"] = state["allocations"]
            selected = self.config_by_id[state["chosen"]]
            allowed = at < scope["end"] and (
                policy.get("allocation") == "family_ensemble"
                or selected["session"] == "all"
                or is_regular == (selected["session"] == "regular")
            )
            if shared_boundary and not policy.get("reset_daily"):
                state["book"].mark(self.prices)
            else:
                self._actual_step(state, frame, fresh, allowed)
        self.day_steps += 1
        self.last_at, self.last_tick = at, tick

    def day_end(self) -> dict[str, Any]:
        if not self.day_open or self.last_at is None or not self.day_steps:
            raise ValueError("The day has no observed market frames")
        scores = (
            [(book.equity - self.initial_equity) / self.initial_equity for book in self.shadows]
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
                funded = self.initial_equity * len(daily)
                ending = sum(row["end_equity"] for row in daily)
                summary = {
                    **daily[-1]["summary"],
                    "initial_equity": funded,
                    "equity": ending,
                    "return_pct": (ending / funded - 1) * 100,
                    "pnl": ending - funded,
                    "continuous_equity": None,
                    "independent_accounts": len(daily),
                    "capital_refills": max(0, len(daily) - 1) * self.initial_equity,
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
                if self.whole_shares:
                    for key in ("rejected_unaffordable", "zero_quantity"):
                        summary[key] = sum(row["summary"][key] for row in daily)
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
        result = {
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
        if self.long_only:
            result["entry_policy"] = "Long entries only; sells close existing long positions."
            result["weighted_learning"] = (
                f"Completed daily shadow P&L divided by USD {self.initial_equity:g}, "
                "averaged with equal, linear "
                "or exponential date weights. Window one includes today's observed score."
            )
            result["ensemble_allocation"] = (
                "Best configuration in each family receives a softmax(score) target capital "
                "weight. Targets apply to new entries only; surviving family exposure and "
                "total account exposure consume budgets. No forced rebalancing or shadow wealth."
            )
        if self.whole_shares:
            result["whole_share_execution"] = (
                "Native integer share quantities; cash-only entries; full current feature "
                "ranking screens affordability. Rounded-zero candidates are skipped in rank "
                "order without transferring shadow profit or refilling actual cash."
            )
        return result
