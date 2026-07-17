"""
True effective price after retailer discount and cashback — the single
source of truth shared by the ROI cost basis (app/services/pricing.py) and
the dashboard's price breakdown display, so the two never diverge.

    price_after_discount = list_price - discount
    effective_price      = price_after_discount * (1 - cashback_rate)

`discount` is a percent of list_price when discount_percent is set, otherwise
the flat discount_amount (dollars) — never both.
"""
from dataclasses import dataclass
from typing import Optional

from app.cashback_rates import CashbackRate, get_cashback


@dataclass
class PriceBreakdown:
    list_price: float
    discount_amount: float
    price_after_discount: float
    cashback_rate: float
    cashback_portal: str
    cashback_amount: float
    effective_price: float


def compute_effective_price(
    list_price: float,
    discount_percent: float = 0.0,
    discount_amount: float = 0.0,
    supplier_name: Optional[str] = None,
    cashback: Optional[CashbackRate] = None,
) -> PriceBreakdown:
    discount = list_price * (discount_percent / 100.0) if discount_percent else (discount_amount or 0.0)
    price_after_discount = max(list_price - discount, 0.0)

    if cashback is None:
        cashback = get_cashback(supplier_name) if supplier_name else CashbackRate(0.0, "none")

    cashback_amount = price_after_discount * cashback.rate
    effective_price = price_after_discount - cashback_amount

    return PriceBreakdown(
        list_price=list_price,
        discount_amount=discount,
        price_after_discount=price_after_discount,
        cashback_rate=cashback.rate,
        cashback_portal=cashback.portal,
        cashback_amount=cashback_amount,
        effective_price=effective_price,
    )
