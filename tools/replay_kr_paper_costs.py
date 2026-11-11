"""Follow Korean paper-fill proposals with independent cash and transaction costs."""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from collections.abc import Iterator
from datetime import datetime
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, BinaryIO

from papertrade.clock import now_utc as now
from papertrade.jsonio import atomic_json
from papertrade.jsonio import paper_json as encode
from tools.autotest_jobs import poll_job
from tools.kr_comparison_account import CAPITAL

COMMISSION = Decimal("0.00015")
STOCK_SELL_TAX = Decimal("0.002")
ALLOCATION_ACCOUNTS = ("5pct_fixed", "5pct_ai", "10pct_fixed", "10pct_ai")
LISTING_MARKETS = ("KOSPI", "KOSDAQ")


def listings(path: Path) -> dict[str, tuple[str, str]]:
    data = json.loads(path.read_text())
    result: dict[str, tuple[str, str]] = {}
    for market in (*LISTING_MARKETS, "KR_ETC"):
        for row in data[market]:
            symbol, kind = row["symbol"], row["securityType"]
            classification = (market, kind)
            if symbol in result and result[symbol] != classification:
                raise ValueError("Conflicting listing classifications")
            result[symbol] = classification
    return result


def sell_tax(
    classification: tuple[str, str] | None,
) -> tuple[Decimal | None, str | None]:
    if classification is None:
        return None, "unknown_listing"
    market, kind = classification
    if kind in ("ETF", "ETN"):
        return None, "unverified_product_withholding"
    if kind != "STOCK" or market not in LISTING_MARKETS:
        return None, "unverified_transaction_tax"
    return STOCK_SELL_TAX, None


class Ledger:
    def __init__(self) -> None:
        self.cash = CAPITAL
        self.positions: dict[str, tuple[Decimal, Decimal]] = {}
        self.realized = Decimal(0)
        self.commissions = Decimal(0)
        self.taxes = Decimal(0)
        self.original_commissions = Decimal(0)
        self.original_known_tax = Decimal(0)
        self.original_unpriced_sale_value = Decimal(0)
        self.counts: Counter[str] = Counter()

    def apply(
        self,
        symbol: str,
        proposed: Decimal,
        price: Decimal,
        classification: tuple[str, str] | None,
    ) -> dict[str, Any]:
        if not proposed.is_finite() or not price.is_finite() or not proposed or price <= 0:
            raise ValueError("Invalid source fill proposal")
        self.counts["proposals"] += 1

        def skip(reason: str) -> dict[str, Any]:
            self.counts[reason] += 1
            return {"outcome": reason}

        original_gross = abs(proposed) * price
        self.original_commissions += original_gross * COMMISSION
        tax_rate, reason = sell_tax(classification)
        if tax_rate is None:
            if proposed < 0:
                self.original_unpriced_sale_value += original_gross
            if reason is None:
                raise ValueError("Missing unpriced listing reason")
            return skip(reason)
        if proposed < 0:
            self.original_known_tax += original_gross * tax_rate
        held, basis = self.positions.get(symbol, (Decimal(0), Decimal(0)))
        tax = Decimal(0)
        if proposed > 0:
            if held:
                return skip("existing_position")
            affordable = self.cash / (price * (1 + COMMISSION))
            quantity = min(proposed, affordable).to_integral_value(rounding=ROUND_DOWN)
            gross = quantity * price
            commission = gross * COMMISSION
            if gross + commission > self.cash:
                quantity -= 1
                gross = quantity * price
                commission = gross * COMMISSION
            if quantity <= 0:
                return skip("below_one_share")
            if gross + commission > self.cash:
                return skip("insufficient_cash")
            self.cash -= gross + commission
            self.positions[symbol] = quantity, gross + commission
        else:
            if not held:
                return skip("no_position")
            # A legacy full exit can propose a fractional quantity above the integer holding.
            quantity = min(-proposed, held).to_integral_value(rounding=ROUND_DOWN)
            if not quantity:
                return skip("below_one_share")
            gross = quantity * price
            commission = gross * COMMISSION
            tax = gross * tax_rate
            sold_basis = basis if quantity == held else basis * (quantity / held)
            proceeds = gross - commission - tax
            self.cash += proceeds
            self.realized += proceeds - sold_basis
            if quantity == held:
                del self.positions[symbol]
            else:
                self.positions[symbol] = held - quantity, basis - sold_basis

        self.commissions += commission
        self.taxes += tax
        self.counts["fills"] += 1
        if quantity != abs(proposed):
            self.counts["resized"] += 1
        return {
            "outcome": "filled",
            "quantity": quantity if proposed > 0 else -quantity,
            "commission_krw": commission,
            "transaction_tax_krw": tax,
        }

    def report(self, marks: dict[str, tuple[Decimal, datetime]]) -> dict[str, Any]:
        positions = {
            symbol: {
                "quantity": quantity,
                "basis_krw": basis,
                "mark_krw": marks[symbol][0],
                "source_at": marks[symbol][1],
                "market_value_krw": quantity * marks[symbol][0],
            }
            for symbol, (quantity, basis) in self.positions.items()
        }
        market_value = sum(
            (quantity * marks[symbol][0] for symbol, (quantity, _) in self.positions.items()),
            Decimal(0),
        )
        return {
            "cash_krw": self.cash,
            "equity_krw": self.cash + market_value,
            "realized_krw": self.realized,
            "unrealized_krw": market_value
            - sum((basis for _, basis in self.positions.values()), Decimal(0)),
            "commission_krw": self.commissions,
            "transaction_tax_krw": self.taxes,
            "original_proposal_cost_attribution": {
                "assumed_commission_krw": self.original_commissions,
                "known_transaction_tax_krw": self.original_known_tax,
                "unpriced_sale_value_krw": self.original_unpriced_sale_value,
            },
            "counts": self.counts,
            "positions": positions,
        }


def complete_lines(stream: BinaryIO) -> Iterator[dict[str, Any]]:
    while True:
        offset = stream.tell()
        line = stream.readline()
        if not line.endswith(b"\n"):
            stream.seek(offset)
            return
        yield json.loads(line)


def update_marks(marks: dict[str, tuple[Decimal, datetime]], snapshot: dict[str, Any]) -> datetime:
    observed = datetime.fromisoformat(snapshot["observed_at"])
    for row in snapshot["rows"]:
        if row.get("currency") != "KRW":
            continue
        try:
            price = Decimal(str(row["lastPrice"]))
            source = datetime.fromisoformat(row["timestamp"])
            symbol = row["symbol"]
        except (KeyError, TypeError, ValueError, InvalidOperation):
            continue
        if price.is_finite() and price > 0 and source.tzinfo is not None and source <= observed:
            previous = marks.get(symbol)
            if previous is None or source > previous[1]:
                marks[symbol] = price, source
    return observed


def follow(
    comparison: Path,
    layered: Path,
    listing_path: Path,
    output: Path,
    until: datetime,
    *,
    comparison_job: Path,
    layered_job: Path,
) -> None:
    classification = listings(listing_path)
    ledgers = {key: Ledger() for key in ALLOCATION_ACCOUNTS}
    ledgers["layered"] = Ledger()
    marks: dict[str, tuple[Decimal, datetime]] = {}
    observed = None
    snapshots = 0
    output.mkdir(parents=True, exist_ok=False)

    with (
        (comparison / "market.jsonl").open("rb") as market,
        (comparison / "events.jsonl").open("rb") as allocation_events,
        (layered / "events.jsonl").open("rb") as layered_events,
        (output / "events.jsonl").open("x", buffering=1) as audit,
    ):
        sources = ((allocation_events, "fill"), (layered_events, "assumed_fill"))
        while True:
            comparison_exit = poll_job(comparison_job)
            layered_exit = poll_job(layered_job)
            terminal = comparison_exit is not None and layered_exit is not None
            comparison_status = json.loads((comparison / "status.json").read_text())
            layered_status = json.loads((layered / "status.json").read_text())
            changed = False
            for snapshot in complete_lines(market):
                observed = update_marks(marks, snapshot)
                if observed >= until:
                    raise ValueError("Source mark falls outside authorized observation period")
                snapshots += 1
                changed = True
            for stream, kind in sources:
                for item in complete_lines(stream):
                    if item["kind"] != kind:
                        continue
                    timestamp = datetime.fromisoformat(item["observed_at"])
                    if timestamp >= until:
                        raise ValueError("Source fill falls outside authorized observation period")
                    account = item.get("account", "layered")
                    symbol = item["symbol"]
                    price = Decimal(str(item.get("assumed_price_krw", item.get("price"))))
                    proposed = Decimal(str(item["quantity"]))
                    if kind == "assumed_fill" and proposed != proposed.to_integral_value():
                        raise ValueError("Layered source proposal must use whole shares")
                    if item.get("action") == "SELL":
                        proposed = -proposed
                    result = ledgers[account].apply(
                        symbol, proposed, price, classification.get(symbol)
                    )
                    source = datetime.fromisoformat(item["source_at"])
                    mark = marks.get(symbol)
                    if mark is None or source > mark[1]:
                        marks[symbol] = price, source
                    audit.write(
                        encode(
                            {
                                "account": account,
                                "symbol": symbol,
                                "observed_at": timestamp,
                                "source_at": source,
                                "price_krw": price,
                                "proposed_quantity": proposed,
                                "classification": classification.get(symbol),
                                **result,
                            }
                        )
                        + "\n"
                    )
                    changed = True
            if changed or terminal:
                complete = (
                    comparison_exit is not None
                    and layered_exit is not None
                    and comparison_exit["returncode"] == layered_exit["returncode"] == 0
                    and comparison_status["state"] == layered_status["state"] == "complete"
                    and snapshots == comparison_status["sweeps"]
                    and all(
                        ledgers[key].counts["proposals"]
                        == comparison_status["accounts"][key]["fills"]
                        for key in ALLOCATION_ACCOUNTS
                    )
                    and ledgers["layered"].counts["proposals"]
                    == layered_status["counts"].get("buy_fills", 0)
                    + layered_status["counts"].get("sell_fills", 0)
                )
                unknown = (
                    "unknown_listing",
                    "unverified_product_withholding",
                    "unverified_transaction_tax",
                )
                atomic_json(
                    output / "status.json",
                    json.loads(
                        encode(
                            {
                                "state": "complete"
                                if complete
                                else ("incomplete" if terminal else "running"),
                                "updated_at": now(),
                                "until": until,
                                "last_observation": observed,
                                "market_snapshots": snapshots,
                                "real_orders": False,
                                "quantity_policy": "Whole shares only; proposals below one share are skipped",
                                "scope": "Fee-constrained replay of recorded fill proposals, without strategy or AI recomputation",
                                "commission_venue_assumption": "KRX",
                                "commission_rate": COMMISSION,
                                "commission_rounding": "Unrounded estimate; broker rounding unverified",
                                "cost_sources": [
                                    "https://home-files.tossinvest.com/files/notice/f8b692a8-2b29-46c4-b279-f6bf865e64d5.pdf",
                                    "https://p.tossinvest.com/ko/post?category=52&id=18410&type=notice",
                                ],
                                "stock_sale_transaction_tax_rate": STOCK_SELL_TAX,
                                "tax_coverage_complete": not any(
                                    ledger.counts[reason]
                                    for ledger in ledgers.values()
                                    for reason in unknown
                                ),
                                "tax_policy": "Replay ordinary stocks only; reject products with unresolved transaction tax or withholding",
                                "source_status": {
                                    "allocation": comparison_status["state"],
                                    "layered": layered_status["state"],
                                },
                                "accounts": {
                                    name: ledger.report(marks) for name, ledger in ledgers.items()
                                },
                            }
                        )
                    ),
                )
            if terminal:
                return
            time.sleep(0.25 if now() >= until else 2)


def main() -> None:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", required=True, type=Path)
    parser.add_argument("--layered", required=True, type=Path)
    parser.add_argument("--comparison-job", required=True, type=Path)
    parser.add_argument("--layered-job", required=True, type=Path)
    parser.add_argument("--listings", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--until", required=True, type=datetime.fromisoformat)
    args = parser.parse_args()
    if args.until.tzinfo is None:
        parser.error("--until requires a timezone offset")
    follow(
        args.comparison,
        args.layered,
        args.listings,
        args.output,
        args.until,
        comparison_job=args.comparison_job,
        layered_job=args.layered_job,
    )


if __name__ == "__main__":
    main()
