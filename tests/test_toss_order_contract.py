"""Offline order fixtures shaped by Toss OpenAPI 1.2.28."""

import unittest
from decimal import Decimal
from typing import Any

from tools.paper_market import KR, US
from tools.toss_order_contract import (
    OrderProgress,
    OrderRejection,
    read_submission,
    whole_share_limit,
)


class TossOrderContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.request = whole_share_limit(
            KR, "005930", "BUY", 10, Decimal(70000), client_order_id="my-order-001"
        )
        response = {"result": {"orderId": "synthetic-order", "clientOrderId": "my-order-001"}}
        accepted = read_submission(self.request, 200, response)
        assert isinstance(accepted, OrderProgress)
        self.accepted = accepted

    def detail(self, status: str, filled: int, amount: str | None) -> dict[str, Any]:
        return {
            "result": {
                "orderId": "synthetic-order",
                "symbol": "005930",
                "side": "BUY",
                "orderType": "LIMIT",
                "timeInForce": "DAY",
                "status": status,
                "price": "70000",
                "quantity": "10",
                "orderAmount": None,
                "currency": "KRW",
                "orderedAt": "2026-03-28T09:30:00+09:00",
                "canceledAt": None,
                "execution": {
                    "filledQuantity": str(filled),
                    "averageFilledPrice": str(Decimal(amount) / filled) if amount else None,
                    "filledAmount": amount,
                    "commission": None,
                    "tax": None,
                    "filledAt": "2026-03-28T09:31:15+09:00" if filled else None,
                    "settlementDate": None,
                },
            }
        }

    def test_documented_quantity_payload_has_only_supported_fields(self) -> None:
        self.assertEqual(
            self.request,
            {
                "clientOrderId": "my-order-001",
                "symbol": "005930",
                "side": "BUY",
                "orderType": "LIMIT",
                "timeInForce": "DAY",
                "quantity": "10",
                "price": "70000",
            },
        )
        sell = whole_share_limit(
            US, "AAPL", "SELL", 2, Decimal("0.9999"), client_order_id="us_1", held_quantity=2
        )
        self.assertEqual(sell["price"], "0.9999")
        with self.assertRaisesRegex(ValueError, "held"):
            whole_share_limit(
                US, "AAPL", "SELL", 3, Decimal("1.23"), client_order_id="us_2", held_quantity=2
            )

    def test_invalid_request_limits(self) -> None:
        for market, symbol, quantity, price, key in [
            (KR, "005930", 1, "50101", "valid"),
            (KR, "005930", 1, "1000.5", "valid"),
            (KR, "005930", 2000, "50000", "valid"),
            (US, "AAPL", 1, "1.001", "valid"),
            (US, "AAPL", 1, "0.99999", "valid"),
            (US, "AAPL", 1, "NaN", "valid"),
            (US, "AAPL", 1, "0", "valid"),
            (US, "AAPL", 0, "1", "valid"),
            (US, "AAPL", True, "1", "valid"),
            (US, "AAPL", 10**30, "1", "valid"),
            (US, "AAPL", 1, "1", "bad:id"),
            (US, "AAPL", 1, "1", "x" * 37),
        ]:
            with (
                self.subTest(market=market.code, quantity=quantity, price=price, key=key),
                self.assertRaises(ValueError),
            ):
                whole_share_limit(
                    market, symbol, "BUY", quantity, Decimal(price), client_order_id=key
                )

    def test_cash_and_tick_rejections_have_no_accepted_order(self) -> None:
        fixtures = (
            (422, "insufficient-buying-power", {}, None),
            (
                400,
                "invalid-request",
                {"field": "price", "tickSize": "100", "nearestPrices": ["50100", "50200"]},
                Decimal(100),
            ),
        )
        for status, code, data, tick in fixtures:
            with self.subTest(code=code):
                response = {
                    "error": {
                        "requestId": "fixture",
                        "message": "fixture",
                        "code": code,
                        "data": data,
                    }
                }
                self.assertEqual(
                    read_submission(self.request, status, response), OrderRejection(code, tick)
                )
        with self.assertRaises(ValueError):
            read_submission(self.request, 500, {"error": {"code": "internal-error"}})
        with self.assertRaises(ValueError):
            read_submission(
                self.request, 200, {"result": {"orderId": "synthetic", "clientOrderId": "wrong"}}
            )

    def test_acceptance_duplicate_stale_and_partial_fills(self) -> None:
        self.assertEqual((self.accepted.status, self.accepted.filled_quantity), ("PENDING", 0))
        pending = self.detail("PENDING", 0, None)
        partial = self.detail("PARTIAL_FILLED", 3, "209700")
        filled = self.detail("FILLED", 10, "699700")
        checkpoint, quantity, gross = self.accepted.advance(self.request, KR, partial)
        self.assertEqual((quantity, gross), (3, Decimal(209700)))
        for repeat in (partial, pending):
            self.assertEqual(checkpoint.advance(self.request, KR, repeat), (checkpoint, 0, 0))
        checkpoint, quantity, gross = checkpoint.advance(self.request, KR, filled)
        self.assertEqual((quantity, gross), (7, Decimal(490000)))
        for repeat in (filled, partial, pending):
            self.assertEqual(checkpoint.advance(self.request, KR, repeat), (checkpoint, 0, 0))

    def test_terminal_remainder_cannot_erase_partial_fills(self) -> None:
        partial = self.detail("PARTIAL_FILLED", 3, "209700")
        checkpoint, _, _ = self.accepted.advance(self.request, KR, partial)
        for status in ("CANCELED", "REJECTED", "REPLACED"):
            with self.subTest(status=status):
                finished, quantity, gross = checkpoint.advance(
                    self.request, KR, self.detail(status, 3, "209700")
                )
                self.assertEqual((finished.status, finished.filled_quantity), (status, 3))
                self.assertEqual((quantity, gross), (0, 0))
                self.assertEqual(finished.advance(self.request, KR, partial), (finished, 0, 0))
                with self.assertRaisesRegex(ValueError, "terminal"):
                    finished.advance(self.request, KR, self.detail("FILLED", 10, "699700"))

    def test_conflicting_or_unrelated_snapshots_cannot_apply_a_delta(self) -> None:
        partial = self.detail("PARTIAL_FILLED", 3, "209700")
        checkpoint, _, _ = self.accepted.advance(self.request, KR, partial)
        wrong_order = self.detail("PARTIAL_FILLED", 3, "209700")
        wrong_order["result"]["orderId"] = "unrelated"
        for snapshot in (
            wrong_order,
            self.detail("PARTIAL_FILLED", 3, "209699"),
            self.detail("PARTIAL_FILLED", 4, "200000"),
            self.detail("PARTIAL_FILLED", 4, "279701"),
            self.detail("CANCEL_REJECTED", 3, "209700"),
            self.detail("UNKNOWN", 3, "209700"),
        ):
            with self.subTest(order=snapshot["result"]), self.assertRaises(ValueError):
                checkpoint.advance(self.request, KR, snapshot)
