"""Immutable version 1 contracts for the whole-share paper engine."""

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal
from enum import StrEnum

VERSION = 1
ZERO = Decimal(0)


def arithmetic() -> Context:
    return Context(prec=48, rounding=ROUND_HALF_EVEN)


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


def require_decimal(value: Decimal, *, minimum: Decimal = ZERO) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value < minimum:
        raise ValueError("Invalid decimal value")


def require_quantity(value: Decimal) -> None:
    require_decimal(value)
    if value == ZERO or value != value.to_integral_value():
        raise ValueError("Quantity must be positive whole shares")


def require_price(value: Decimal) -> None:
    require_decimal(value)
    if value == ZERO:
        raise ValueError("Price must be positive")


def require_ids(*values: str) -> None:
    if any(type(value) is not str or not value for value in values):
        raise ValueError("Identities must be nonempty strings")


def require_time(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("Invalid timestamp")


@dataclass(frozen=True, slots=True)
class MarketRules:
    market: str
    currency: str
    buy_fee_rate: Decimal
    sell_fee_rate: Decimal
    max_quote_age: int
    confirmation_delay: int = 1000
    intent_ttl: int = 30000
    single_attempt: bool = False

    def __post_init__(self) -> None:
        if (self.market, self.currency) not in (("KR", "KRW"), ("US", "USD")):
            raise ValueError("Unsupported whole-share market and currency")
        require_decimal(self.buy_fee_rate)
        require_decimal(self.sell_fee_rate)
        if self.buy_fee_rate >= 1 or self.sell_fee_rate >= 1:
            raise ValueError("Fee rates must be less than one")
        require_time(self.max_quote_age)
        require_time(self.confirmation_delay)
        require_time(self.intent_ttl)
        if self.intent_ttl <= self.confirmation_delay:
            raise ValueError("Intent expiry must follow the minimum confirmation delay")


@dataclass(frozen=True, slots=True)
class Position:
    symbol: str
    owner: str
    quantity: Decimal
    cost_basis: Decimal

    def __post_init__(self) -> None:
        require_ids(self.symbol, self.owner)
        require_quantity(self.quantity)
        require_decimal(self.cost_basis)


@dataclass(frozen=True, slots=True)
class Intent:
    intent_id: str
    symbol: str
    owner: str
    side: Side
    quantity: Decimal
    limit_price: Decimal

    def __post_init__(self) -> None:
        require_ids(self.intent_id, self.symbol, self.owner)
        if not isinstance(self.side, Side):
            raise TypeError("Invalid side")
        require_quantity(self.quantity)
        require_price(self.limit_price)


@dataclass(frozen=True, slots=True)
class PendingIntent:
    intent: Intent
    remaining: Decimal
    submitted_at: int
    reserved_cash: Decimal

    def __post_init__(self) -> None:
        require_quantity(self.remaining)
        require_time(self.submitted_at)
        require_decimal(self.reserved_cash)
        if self.remaining > self.intent.quantity:
            raise ValueError("Remaining quantity exceeds the request")
        if self.intent.side == Side.SELL and self.reserved_cash:
            raise ValueError("A sale reserves shares")


@dataclass(frozen=True, slots=True)
class AccountState:
    account_id: str
    entry_strategy: str
    initial_cash: Decimal
    cash: Decimal
    positions: tuple[Position, ...] = ()
    pending: tuple[PendingIntent, ...] = ()
    fees_paid: Decimal = ZERO
    realized_pnl: Decimal = ZERO

    def __post_init__(self) -> None:
        require_ids(self.account_id, self.entry_strategy)


@dataclass(frozen=True, slots=True)
class Level:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        require_price(self.price)
        require_decimal(self.quantity)


@dataclass(frozen=True, slots=True)
class Quote:
    """A normalized book with source and availability times in epoch milliseconds."""

    quote_id: str
    symbol: str
    source: str
    observed_at: int
    available_at: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]

    def __post_init__(self) -> None:
        require_ids(self.quote_id, self.symbol, self.source)
        require_time(self.observed_at)
        require_time(self.available_at)
        if self.available_at < self.observed_at:
            raise ValueError("A quote cannot be available before observation")
        for levels in (self.bids, self.asks):
            if len({level.price for level in levels}) != len(levels):
                raise ValueError("Book prices must be unique on each side")


@dataclass(frozen=True, slots=True)
class ConfirmedFill:
    """Economic input for an already-confirmed fill, including its recorded fee."""

    intent_id: str
    symbol: str
    owner: str
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    notional: Decimal | None = None

    def __post_init__(self) -> None:
        require_ids(self.intent_id, self.symbol, self.owner)
        if not isinstance(self.side, Side):
            raise TypeError("Invalid side")
        require_quantity(self.quantity)
        require_price(self.price)
        require_decimal(self.fee)
        if self.notional is not None:
            require_price(self.notional)


@dataclass(frozen=True, slots=True)
class PaperFill:
    fill: ConfirmedFill
    quote_id: str
    at: int
    levels: tuple[Level, ...]


class IntentStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    UNFILLED = "unfilled"
    PARTIAL = "partial"
    FILLED = "filled"


@dataclass(frozen=True, slots=True)
class IntentResult:
    intent_id: str
    status: IntentStatus
    reason: str = ""
    quote_id: str | None = None


@dataclass(frozen=True, slots=True)
class AccountInput:
    account_id: str
    proposals: tuple[Intent, ...] = ()
    cancellations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_ids(self.account_id, *self.cancellations)


@dataclass(frozen=True, slots=True)
class AccountResult:
    state: AccountState
    intents: tuple[IntentResult, ...] = ()
    fills: tuple[PaperFill, ...] = ()


@dataclass(frozen=True, slots=True)
class Branch:
    command_id: str
    parent_id: str
    incumbent_id: str
    candidate_id: str
    candidate_strategy: str

    def __post_init__(self) -> None:
        require_ids(
            self.command_id, self.parent_id, self.incumbent_id,
            self.candidate_id, self.candidate_strategy,
        )


@dataclass(frozen=True, slots=True)
class Cut:
    cut_id: str
    expected_sequence: int
    at: int
    quotes: tuple[Quote, ...] = ()
    accounts: tuple[AccountInput, ...] = ()
    branches: tuple[Branch, ...] = ()

    def __post_init__(self) -> None:
        require_ids(self.cut_id)
        require_time(self.expected_sequence)
        require_time(self.at)


@dataclass(frozen=True, slots=True)
class Checkpoint:
    sequence: int
    at: int
    accounts: tuple[AccountState, ...]
    quote_watermarks: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class Commit:
    digest: str
    checkpoint: Checkpoint
    results: tuple[AccountResult, ...]
