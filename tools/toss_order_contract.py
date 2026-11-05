"""Offline subset of the Toss OpenAPI ordinary-order contract."""

import re
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any

from tools.paper_market import KR, US, PaperMarket, kr_stock_tick


def decimal_field(value: object) -> Decimal:
    if (
        not isinstance(value, str)
        or len(value) > 30
        or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is None
    ):
        raise ValueError("Invalid Toss decimal string")
    return Decimal(value)


def whole_share_limit(
    market: PaperMarket,
    symbol: str,
    side: str,
    quantity: int,
    price: Decimal,
    *,
    client_order_id: str,
    held_quantity: int = 0,
) -> dict[str, str]:
    """Build the selected DAY LIMIT subset after paper sizing and price normalization."""
    if market not in (KR, US):
        raise ValueError("Unsupported Toss market")
    pattern = r"[0-9A-Z]{6}" if market == KR else r"[A-Z][A-Z0-9.-]*"
    if re.fullmatch(pattern, symbol) is None:
        raise ValueError("Invalid order symbol")
    if re.fullmatch(r"[A-Za-z0-9_-]{1,36}", client_order_id) is None:
        raise ValueError("Invalid clientOrderId")
    if side not in ("BUY", "SELL") or type(quantity) is not int or quantity <= 0:
        raise ValueError("Expected BUY or SELL with positive whole shares")
    if side == "SELL" and quantity > held_quantity:
        raise ValueError("Sell quantity exceeds held shares")
    if not price.is_finite() or price <= 0 or price.adjusted() >= 30:
        raise ValueError("Invalid limit price")
    places = 0 if market == KR else 4 if price < 1 else 2
    with localcontext() as context:
        context.prec = 60
        canonical = price.quantize(Decimal(1).scaleb(-places))
        if canonical != price or (market == KR and canonical % kr_stock_tick(price)):
            raise ValueError("Limit price does not match the market increment")
        if market == KR and canonical * quantity >= 100_000_000:
            raise ValueError("High-value order confirmation is outside the paper profile")
    price_text = format(canonical, "f")
    if places:
        price_text = price_text.rstrip("0").rstrip(".")
    decimal_field(price_text)
    decimal_field(str(quantity))
    return {
        "clientOrderId": client_order_id,
        "symbol": symbol,
        "side": side,
        "orderType": "LIMIT",
        "timeInForce": "DAY",
        "quantity": str(quantity),
        "price": price_text,
    }


@dataclass(frozen=True)
class OrderRejection:
    code: str
    tick_size: Decimal | None = None


@dataclass(frozen=True)
class OrderProgress:
    """Immutable checkpoint. Each advance returns the incremental quantity and gross amount."""

    order_id: str
    status: str = "PENDING"
    filled_quantity: int = 0
    filled_amount: Decimal = Decimal(0)

    def advance(
        self, request: dict[str, str], market: PaperMarket, response: dict[str, Any]
    ) -> tuple["OrderProgress", int, Decimal]:
        order = response["result"]
        if (
            order["orderId"] != self.order_id
            or order["currency"] != market.currency
            or any(
                order[field] != request[field]
                for field in ("symbol", "side", "orderType", "timeInForce")
            )
            or decimal_field(order["price"]) != decimal_field(request["price"])
        ):
            raise ValueError("Order detail does not match the accepted request")
        requested = decimal_field(request["quantity"])
        quantity = decimal_field(order["quantity"])
        execution = order["execution"]
        filled = decimal_field(execution["filledQuantity"])
        if quantity != requested or filled != filled.to_integral_value() or filled > requested:
            raise ValueError("Unexpected whole-share order quantity")
        raw_amount = execution["filledAmount"]
        amount = Decimal(0) if raw_amount is None else decimal_field(raw_amount)
        if bool(filled) != bool(amount):
            raise ValueError("Order execution requires consistent quantity and amount")
        terminal = {"FILLED", "CANCELED", "REJECTED", "REPLACED"}
        status = order["status"]
        open_statuses = {"PENDING", "PARTIAL_FILLED", "PENDING_CANCEL", "PENDING_REPLACE"}
        if status not in terminal | open_statuses:
            raise ValueError("Unrecognized or unrelated ordinary order status")
        if (
            (status == "FILLED" and filled != requested)
            or (status == "PARTIAL_FILLED" and not 0 < filled < requested)
            or (status == "PENDING" and filled != 0)
        ):
            raise ValueError("Inconsistent order status and fill quantity")

        if filled < self.filled_quantity:
            return self, 0, Decimal(0)
        with localcontext() as context:
            context.prec = 60
            amount_delta = amount - self.filled_amount
            quantity_delta = int(filled) - self.filled_quantity
            limit = decimal_field(request["price"])
            if (quantity_delta == 0 and amount_delta != 0) or (
                quantity_delta > 0
                and (
                    amount_delta <= 0
                    or (request["side"] == "BUY" and amount_delta > quantity_delta * limit)
                    or (request["side"] == "SELL" and amount_delta < quantity_delta * limit)
                )
            ):
                raise ValueError("Inconsistent cumulative fill amount")
        if self.status in terminal:
            if quantity_delta or (status in terminal and status != self.status):
                raise ValueError("Order detail conflicts with terminal state")
            return self, 0, Decimal(0)
        updated = OrderProgress(self.order_id, status, int(filled), amount)
        return updated, quantity_delta, amount_delta


def read_submission(
    request: dict[str, str], http_status: int, response: dict[str, Any]
) -> OrderProgress | OrderRejection:
    """Read a documented response fixture without authorizing a retry or producing a fill."""
    if http_status == 200:
        result = response["result"]
        order_id = result["orderId"]
        if (
            not isinstance(order_id, str)
            or not order_id
            or result.get("clientOrderId") != request["clientOrderId"]
        ):
            raise ValueError("Order acknowledgement does not match the request")
        return OrderProgress(order_id)
    error = response["error"]
    code = error["code"]
    if http_status == 422 and code == "insufficient-buying-power":
        return OrderRejection(code)
    if http_status == 400 and code == "invalid-request":
        data = error.get("data")
        if isinstance(data, dict) and data.get("field") == "price" and "tickSize" in data:
            tick = decimal_field(data["tickSize"])
            if tick <= 0:
                raise ValueError("Invalid tick size in rejection")
            return OrderRejection(code, tick)
    raise ValueError("Submission outcome is outside the verified fixture contract")
