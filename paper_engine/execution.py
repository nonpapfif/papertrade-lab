"""Match a paper request against attributed, later whole-share book depth."""

from decimal import ROUND_DOWN, localcontext

from .contracts import (
    ZERO,
    ConfirmedFill,
    Level,
    MarketRules,
    PaperFill,
    PendingIntent,
    Quote,
    Side,
    arithmetic,
)


def book_issue(quote: Quote) -> str:
    bids = tuple(level.price for level in quote.bids if level.quantity >= 1)
    asks = tuple(level.price for level in quote.asks if level.quantity >= 1)
    if not bids or not asks:
        return "missing_depth"
    if max(bids) >= min(asks):
        return "locked_or_crossed_book"
    return ""


def match(
    pending: PendingIntent, quote: Quote, available: Quote, rules: MarketRules, at: int
) -> tuple[PaperFill | None, str]:
    intent = pending.intent
    if quote.symbol != intent.symbol:
        return None, "different_symbol"
    if (
        quote.observed_at <= pending.submitted_at
        or quote.available_at < pending.submitted_at + rules.confirmation_delay
        or at >= pending.submitted_at + rules.intent_ttl
        or quote.available_at > at
        or at - quote.observed_at > rules.max_quote_age
    ):
        return None, "quote_time"
    issue = book_issue(quote)
    if issue:
        return None, issue

    is_buy = intent.side == Side.BUY
    side = available.asks if is_buy else available.bids
    levels = sorted(
        (level for level in side if level.quantity >= 1),
        key=lambda level: level.price,
        reverse=not is_buy,
    )
    if not levels:
        return None, "depth_consumed"
    matched: list[Level] = []
    with localcontext(arithmetic()):
        remaining = pending.remaining
        for level in levels:
            if is_buy and level.price > intent.limit_price:
                break
            if not is_buy and level.price < intent.limit_price:
                break
            quantity = min(remaining, level.quantity.to_integral_value(rounding=ROUND_DOWN))
            matched.append(Level(level.price, quantity))
            remaining -= quantity
            if remaining == ZERO:
                break
        if not matched:
            return None, "limit_price"

        quantity = pending.remaining - remaining
        notional = sum((level.price * level.quantity for level in matched), ZERO)
        fee = notional * (rules.buy_fee_rate if is_buy else rules.sell_fee_rate)
        fill = ConfirmedFill(
            intent.intent_id, intent.symbol, intent.owner, intent.side,
            quantity, notional / quantity, fee, notional,
        )
    return PaperFill(fill, quote.quote_id, at, tuple(matched)), ""
