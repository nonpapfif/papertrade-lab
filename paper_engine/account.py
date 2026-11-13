"""Pure account transitions and reservations for confirmed or simulated fills."""

from collections import Counter
from dataclasses import replace
from decimal import Decimal, localcontext

from .contracts import (
    ZERO,
    AccountInput,
    AccountResult,
    AccountState,
    ConfirmedFill,
    Intent,
    IntentResult,
    IntentStatus,
    MarketRules,
    PaperFill,
    PendingIntent,
    Position,
    Quote,
    Side,
    arithmetic,
    require_decimal,
    require_time,
)
from .execution import match


def reserved_cash(state: AccountState) -> Decimal:
    with localcontext(arithmetic()):
        return sum((pending.reserved_cash for pending in state.pending), ZERO)


def reserved_shares(state: AccountState, symbol: str) -> Decimal:
    with localcontext(arithmetic()):
        return sum((
            pending.remaining for pending in state.pending
            if pending.intent.symbol == symbol and pending.intent.side == Side.SELL
        ), ZERO)


def validate_account(state: AccountState, rules: MarketRules | None = None) -> None:
    if not state.account_id or not state.entry_strategy:
        raise ValueError("Account requires an ID and entry strategy version")
    require_decimal(state.initial_cash)
    require_decimal(state.cash)
    require_decimal(state.fees_paid)
    if not isinstance(state.realized_pnl, Decimal) or not state.realized_pnl.is_finite():
        raise ValueError("Invalid realized P&L")
    if len({position.symbol for position in state.positions}) != len(state.positions):
        raise ValueError("Duplicate position symbol")
    if len({pending.intent.intent_id for pending in state.pending}) != len(state.pending):
        raise ValueError("Duplicate pending intent")
    if reserved_cash(state) > state.cash:
        raise ValueError("Cash reservations exceed account cash")

    positions = {position.symbol: position for position in state.positions}
    pending_owners: dict[str, tuple[str, Side]] = {}
    for pending in state.pending:
        intent = pending.intent
        position = positions.get(intent.symbol)
        if position is not None and position.owner != intent.owner:
            raise ValueError("Pending intent conflicts with position owner")
        if intent.side == Side.SELL and position is None:
            raise ValueError("Sale reservation has no position")
        if rules is not None and pending.reserved_cash != cash_required(
            intent, pending.remaining, rules,
        ):
            raise ValueError("Pending cash reservation does not match remaining quantity and limit")
        owner_and_side = intent.owner, intent.side
        if pending_owners.setdefault(intent.symbol, owner_and_side) != owner_and_side:
            raise ValueError("Conflicting pending intents for one position")
    for position in state.positions:
        if reserved_shares(state, position.symbol) > position.quantity:
            raise ValueError("Share reservations exceed held quantity")


def apply_confirmed_fill(state: AccountState, fill: ConfirmedFill) -> AccountState:
    """Apply attributed economics after the caller updates the matching reservation."""
    positions = {position.symbol: position for position in state.positions}
    position = positions.get(fill.symbol)
    if position is not None and position.owner != fill.owner:
        raise ValueError("Fill conflicts with position strategy owner")

    with localcontext(arithmetic()):
        notional = fill.notional if fill.notional is not None else fill.quantity * fill.price
        realized = state.realized_pnl
        if fill.side == Side.BUY:
            cost = notional + fill.fee
            cash = state.cash - cost
            positions[fill.symbol] = Position(
                fill.symbol,
                fill.owner,
                fill.quantity + (position.quantity if position else ZERO),
                cost + (position.cost_basis if position else ZERO),
            )
        else:
            if position is None or position.quantity < fill.quantity:
                raise ValueError("Sale exceeds held quantity")
            net = notional - fill.fee
            cash = state.cash + net
            remaining = position.quantity - fill.quantity
            if remaining:
                released = position.cost_basis * fill.quantity / position.quantity
                positions[fill.symbol] = replace(
                    position, quantity=remaining, cost_basis=position.cost_basis - released,
                )
            else:
                released = position.cost_basis
                del positions[fill.symbol]
            realized += net - released

        updated = replace(
            state,
            cash=cash,
            positions=tuple(positions.values()),
            fees_paid=state.fees_paid + fill.fee,
            realized_pnl=realized,
        )
    validate_account(updated)
    return updated


def cash_required(intent: Intent, remaining: Decimal, rules: MarketRules) -> Decimal:
    if intent.side == Side.SELL:
        return ZERO
    with localcontext(arithmetic()):
        return remaining * intent.limit_price * (1 + rules.buy_fee_rate)


def _submit(
    state: AccountState, intent: Intent, rules: MarketRules, at: int
) -> tuple[AccountState, IntentResult]:
    position = next((p for p in state.positions if p.symbol == intent.symbol), None)
    existing = next((p.intent for p in state.pending if p.intent.symbol == intent.symbol), None)
    owner = position.owner if position else state.entry_strategy
    reason = ""
    reserve = ZERO
    if intent.owner != owner or (existing is not None and existing.owner != intent.owner):
        reason = "strategy_owner"
    elif existing is not None and existing.side != intent.side:
        reason = "opposing_reservation"
    elif intent.side == Side.BUY:
        reserve = cash_required(intent, intent.quantity, rules)
        if reserve > state.cash - reserved_cash(state):
            reason = "insufficient_cash"
    elif position is None or intent.quantity > position.quantity - reserved_shares(
        state, intent.symbol,
    ):
        reason = "insufficient_shares"
    if reason:
        return state, IntentResult(intent.intent_id, IntentStatus.REJECTED, reason)
    pending = PendingIntent(intent, intent.quantity, at, reserve)
    return replace(state, pending=(*state.pending, pending)), IntentResult(
        intent.intent_id, IntentStatus.ACCEPTED,
    )


def _consume(quote: Quote, paper_fill: PaperFill) -> Quote:
    consumed = {level.price: level.quantity for level in paper_fill.levels}
    side = quote.asks if paper_fill.fill.side == Side.BUY else quote.bids
    levels = tuple(
        replace(level, quantity=level.quantity - consumed.get(level.price, ZERO))
        for level in side
    )
    if paper_fill.fill.side == Side.BUY:
        return replace(quote, asks=levels)
    return replace(quote, bids=levels)


def advance(
    state: AccountState,
    rules: MarketRules,
    at: int,
    quotes: tuple[Quote, ...],
    inputs: AccountInput,
    used_intent_ids: frozenset[str] = frozenset(),
) -> AccountResult:
    """Cancel first, match pending requests in arrival order, then reserve new proposals."""
    validate_account(state, rules)
    require_time(at)
    if inputs.account_id != state.account_id:
        raise ValueError("Account input belongs to a different account")
    proposal_counts = Counter(intent.intent_id for intent in inputs.proposals)
    cancellation_counts = Counter(inputs.cancellations)
    unavailable_ids = used_intent_ids | {pending.intent.intent_id for pending in state.pending}

    pending_by_id = {pending.intent.intent_id: pending for pending in state.pending}
    outcomes: list[IntentResult] = []
    fills: list[PaperFill] = []
    for intent_id in inputs.cancellations:
        if cancellation_counts[intent_id] > 1:
            outcomes.append(IntentResult(intent_id, IntentStatus.REJECTED, "duplicate_cancellation"))
        elif pending_by_id.pop(intent_id, None) is None:
            outcomes.append(IntentResult(intent_id, IntentStatus.REJECTED, "unknown_cancellation"))
        else:
            outcomes.append(IntentResult(intent_id, IntentStatus.CANCELLED))
    state = replace(state, pending=tuple(pending_by_id.values()))

    with localcontext(arithmetic()):
        for quote in quotes:
            available = quote
            for intent_id, pending in tuple(pending_by_id.items()):
                intent = pending.intent
                if intent.symbol != available.symbol:
                    continue
                paper_fill, reason = match(pending, quote, available, rules, at)
                if paper_fill is None:
                    outcomes.append(
                        IntentResult(intent_id, IntentStatus.UNFILLED, reason, quote.quote_id)
                    )
                    if rules.single_attempt and reason in ("limit_price", "depth_consumed"):
                        del pending_by_id[intent_id]
                        state = replace(state, pending=tuple(pending_by_id.values()))
                    continue
                remaining = pending.remaining - paper_fill.fill.quantity
                if remaining and not rules.single_attempt:
                    reserve = cash_required(intent, remaining, rules)
                    pending_by_id[intent_id] = replace(
                        pending, remaining=remaining, reserved_cash=reserve,
                    )
                else:
                    del pending_by_id[intent_id]
                state = apply_confirmed_fill(
                    replace(state, pending=tuple(pending_by_id.values())), paper_fill.fill,
                )
                available = _consume(available, paper_fill)
                fills.append(paper_fill)
                status = IntentStatus.PARTIAL if remaining else IntentStatus.FILLED
                outcomes.append(IntentResult(intent_id, status, quote_id=quote.quote_id))

        for intent_id, pending in tuple(pending_by_id.items()):
            if at >= pending.submitted_at + rules.intent_ttl:
                del pending_by_id[intent_id]
                outcomes.append(IntentResult(intent_id, IntentStatus.EXPIRED))
        state = replace(state, pending=tuple(pending_by_id.values()))
        for proposal in inputs.proposals:
            intent_id = proposal.intent_id
            if proposal_counts[intent_id] > 1 or intent_id in unavailable_ids:
                outcomes.append(IntentResult(intent_id, IntentStatus.REJECTED, "duplicate_intent"))
            else:
                state, outcome = _submit(state, proposal, rules, at)
                outcomes.append(outcome)
    validate_account(state, rules)
    return AccountResult(state, tuple(outcomes), tuple(fills))
