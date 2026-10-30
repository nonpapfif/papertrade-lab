"""Causal completed-bar signals and fills at observed executable quotes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from itertools import pairwise
from typing import Any

from .core import NEW_YORK, ONE, SEOUL, ZERO, SafetyError, Session, Settings, decimal, stamp


@dataclass(frozen=True)
class Bar:
    at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


def aggregate(
    rows: list[dict[str, Any]], sessions: list[Session], at: datetime, minutes: int = 30
) -> list[Bar]:
    buckets: dict[datetime, dict[datetime, dict[str, Any]]] = {}
    for row in rows:
        minute = stamp(row["timestamp"])
        if minute + timedelta(minutes=1) > at:
            continue
        for session in sessions:
            if not session.contains(minute):
                continue
            elapsed = int((minute - session.start).total_seconds() // 60)
            start = session.start + timedelta(minutes=(elapsed // minutes) * minutes)
            if start + timedelta(minutes=minutes) <= min(at, session.end):
                buckets.setdefault(start, {})[minute] = row
            break
    bars = []
    for start, minute_rows in sorted(buckets.items()):
        expected = [start + timedelta(minutes=n) for n in range(minutes)]
        if any(minute not in minute_rows for minute in expected):
            continue
        values = [minute_rows[minute] for minute in expected]
        prices = [
            decimal(row[field])
            for row in values
            for field in ("openPrice", "highPrice", "lowPrice", "closePrice")
        ]
        if min(prices) <= 0:
            raise SafetyError("Candles must have positive prices")
        if any(
            decimal(row["volume"]) < 0
            or decimal(row["lowPrice"]) > min(decimal(row["openPrice"]), decimal(row["closePrice"]))
            or decimal(row["highPrice"])
            < max(decimal(row["openPrice"]), decimal(row["closePrice"]))
            for row in values
        ):
            raise SafetyError("Invalid candle range or volume")
        bars.append(
            Bar(
                start,
                decimal(values[0]["openPrice"]),
                max(decimal(row["highPrice"]) for row in values),
                min(decimal(row["lowPrice"]) for row in values),
                decimal(values[-1]["closePrice"]),
                sum((decimal(row["volume"]) for row in values), ZERO),
            )
        )
    return bars


def atr(bars: list[Bar], period: int) -> Decimal:
    if len(bars) < period + 1:
        raise SafetyError("Insufficient completed candles for ATR")
    ranges = [
        max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        )
        for previous, current in pairwise(bars)
    ]
    result = sum(ranges[:period], ZERO) / period
    for value in ranges[period:]:
        result = (result * (period - 1) + value) / period
    return result


def book_prices(
    book: dict[str, Any], at: datetime, maximum_age: int, currency: str
) -> tuple[Decimal, Decimal]:
    if book.get("currency") != currency or not book.get("timestamp"):
        raise SafetyError("Orderbook currency or timestamp is missing")
    age = (at - stamp(book["timestamp"])).total_seconds()
    if not -5 <= age <= maximum_age:
        raise SafetyError("Stale or future-dated executable quote")
    asks = [decimal(item["price"]) for item in book.get("asks", []) if decimal(item["volume"]) > 0]
    bids = [decimal(item["price"]) for item in book.get("bids", []) if decimal(item["volume"]) > 0]
    if not asks or not bids or min(asks) <= 0 or max(bids) <= 0 or max(bids) > min(asks):
        raise SafetyError("Missing, invalid, or crossed executable book")
    return max(bids), min(asks)


def fill_price(book: dict[str, Any], quantity: int, buying: bool) -> Decimal:
    if type(quantity) is not int or quantity <= 0:
        raise SafetyError("Fill quantity must be a positive whole share count")
    levels = sorted(
        book["asks" if buying else "bids"],
        key=lambda item: decimal(item["price"]),
        reverse=not buying,
    )
    remaining = Decimal(quantity)
    amount = ZERO
    for level in levels:
        volume = decimal(level["volume"])
        price = decimal(level["price"])
        if volume < 0 or price <= 0:
            raise SafetyError("Invalid orderbook level")
        used = min(remaining, volume)
        amount += used * price
        remaining -= used
        if remaining == 0:
            return amount / quantity
    raise SafetyError("Insufficient visible depth for a complete paper fill")


def eligible(
    stock: dict[str, Any],
    price: Decimal,
    market: str,
    equity: Decimal,
    fx: Decimal,
    settings: Settings,
) -> bool:
    if stock.get("status") != "ACTIVE":
        return False
    if stock.get("currency") != ("USD" if market == "US" else "KRW"):
        return False
    return price > 0 and fx > 0 and price / fx <= equity


def trading_paused(stock: dict[str, Any], warnings: list[dict[str, Any]]) -> bool:
    if stock.get("status") != "ACTIVE":
        return True
    if any(str(row.get("warningType", "")).startswith("VI_") for row in warnings):
        return True
    detail = stock.get("koreanMarketDetail") or {}
    return bool(detail.get("krxTradingSuspended")) and (
        not detail.get("nxtSupported") or bool(detail.get("nxtTradingSuspended"))
    )


def signal(
    symbol: str,
    market: str,
    bars: list[Bar],
    benchmark_return: Decimal,
    at: datetime,
    settings: Settings,
    diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    period = int(settings.get("strategy", "atr_period"))
    config = settings.data["strategy"]
    minutes = int(config.get("bar_minutes", 30))
    lookback = int(config.get("breakout_bars", 6))
    relative_minutes = int(config.get("relative_minutes", 60))
    profile = config.get("profile", "v1")
    detail = diagnostics if diagnostics is not None else {}
    detail.update(strategy_profile=profile, bar_minutes=minutes, bars=len(bars), stage="warmup")
    if len(bars) < max(period + 1, lookback + 1):
        return None
    last = bars[-1]
    detail["stage"] = "unfinished_or_stale"
    if last.at + timedelta(minutes=minutes) > at or at - last.at > timedelta(
        minutes=minutes * 2 + 5
    ):
        return None
    zone = SEOUL if market == "KR" else NEW_YORK
    detail["stage"] = "missing_intervals"
    if any(
        right.at - left.at != timedelta(minutes=minutes)
        and right.at.astimezone(zone).date() == left.at.astimezone(zone).date()
        for left, right in pairwise(bars[-lookback - 1 :])
    ):
        return None
    previous = bars[-lookback - 1 : -1]
    breakout = max(bar.high for bar in previous)
    mean_volume = sum((bar.volume for bar in previous), ZERO) / lookback
    detail.update(stage="breakout", close=last.close, breakout=breakout)
    if last.close <= breakout or mean_volume <= 0:
        return None
    volume_ratio = last.volume / mean_volume
    detail.update(stage="volume", volume_ratio=volume_ratio)
    if volume_ratio < decimal(settings.get("strategy", "volume_multiple")):
        return None
    volatility = atr(bars, period)
    detail.update(stage="volatility", atr=volatility)
    if volatility <= 0:
        return None
    anchor = next(
        (bar for bar in bars if bar.at == last.at - timedelta(minutes=relative_minutes)), None
    )
    detail["stage"] = "relative_anchor"
    if anchor is None:
        return None
    relative = last.close / anchor.close - ONE - benchmark_return
    detail.update(stage="relative_strength", relative_return=relative)
    if relative <= 0:
        return None
    detail["stage"] = "passed"
    return {
        "symbol": symbol,
        "strategy_profile": profile,
        "bar_minutes": minutes,
        "breakout_bars": lookback,
        "relative_minutes": relative_minutes,
        "market": market,
        "bar_at": last.at.isoformat(),
        "atr": str(volatility),
        "breakout": str(breakout),
        "support": str(min(bar.low for bar in bars[-3:])),
        "volume_ratio": str(volume_ratio),
        "relative_return": str(relative),
        "signal_key": f"{profile}:{market}:{symbol}:{last.at.isoformat()}",
    }


def entry_plan(
    candidate: dict[str, Any],
    book: dict[str, Any],
    at: datetime,
    equity: Decimal,
    cash: Decimal,
    fx: Decimal,
    settings: Settings,
    quantity: int | None = None,
) -> dict[str, Any]:
    market = candidate["market"]
    bid, ask = book_prices(
        book,
        at,
        int(settings.get("limits", "quote_age_seconds")),
        "USD" if market == "US" else "KRW",
    )
    spread = (ask - bid) / ((ask + bid) / 2)
    if spread > decimal(settings.get("strategy", "max_spread_fraction")):
        raise SafetyError("Spread exceeds the entry threshold")
    breakout = decimal(candidate["breakout"])
    volatility = decimal(candidate["atr"])
    ceiling = breakout + volatility * decimal(settings.get("strategy", "chase_atr"))
    if not breakout < ask <= ceiling:
        raise SafetyError("Breakout failed or entry would chase the price")
    buy_fee = settings.fee_rate(market)
    sell_fee = settings.fee_rate(market, selling=True)
    max_quantity = int(
        (cash / (ask / fx * (ONE + buy_fee))).to_integral_value(rounding=ROUND_FLOOR)
    )
    requested = 1 if quantity is None else quantity
    if type(requested) is not int or not 0 < requested <= max_quantity:
        raise SafetyError("Requested whole shares exceed paper buying power")
    fill = fill_price(book, requested, buying=True)
    if fill > ceiling:
        raise SafetyError("Observed execution depth invalidates the breakout entry")
    stop = min(
        decimal(candidate["support"]),
        fill - volatility * decimal(settings.get("strategy", "atr_multiple")),
    )
    if stop <= 0 or stop >= fill:
        raise SafetyError("Invalid initial stop")
    distance = fill - stop
    unit_cost = fill / fx * (ONE + buy_fee)
    unit_risk = (distance + fill * buy_fee + stop * sell_fee) / fx
    if unit_cost * requested > cash:
        raise SafetyError("Observed depth and costs exceed paper buying power")
    return {
        **candidate,
        "entry_price": str(fill),
        "quantity": requested,
        "max_quantity": max_quantity,
        "stop": str(stop),
        "risk_distance": str(distance),
        "target": str(fill + distance * decimal(settings.get("strategy", "reward_multiple"))),
        "fx": str(fx),
        "cost_usd": str(unit_cost * requested),
        "planned_risk_usd": str(unit_risk * requested),
        "equity_usd": str(equity),
        "mark_usd": str(
            fill_price(book, requested, buying=False) / fx * requested * (ONE - sell_fee)
        ),
        "mark_quality": "executable_depth",
        "quote_at": book["timestamp"],
        "exit_rules": {
            key: settings.get("strategy", key)
            for key in ("time_exit_minutes", "time_exit_r", "flatten_minutes")
        },
        "spread_fraction": str(spread),
    }


def exit_reason(
    position: dict[str, Any], bid: Decimal, at: datetime, session: Session, settings: Settings
) -> str | None:
    rules = position.get("exit_rules", settings.data["strategy"])
    if not session.can_fill(at):
        return None
    if position["session_date"] != session.date:
        return "missed_previous_session_exit"
    if bid <= decimal(position["stop"]):
        return "stop_at_observed_quote"
    if bid >= decimal(position["target"]):
        return "target_at_observed_quote"
    if at >= session.end - timedelta(minutes=int(rules["flatten_minutes"])):
        return "session_end"
    held = at - stamp(position["entered_at"])
    weak = bid - decimal(position["entry_price"]) < decimal(position["risk_distance"]) * decimal(
        rules["time_exit_r"]
    )
    if held >= timedelta(minutes=int(rules["time_exit_minutes"])) and weak:
        return "time_exit"
    return None
