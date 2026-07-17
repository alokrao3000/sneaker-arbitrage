"""
Margin / opportunity classification — the single source of truth shared by the
batch scrape job (app/services/arbitrage.py) and the live SKU lookup endpoint
(app/api/routes/sku.py), so the two never diverge.

    effective_price = (original_price × (1 − retailer_discount)) × (1 − cashback_rate)
    payout          = resale_price − seller_fee_estimate
    margin          = payout − effective_price          (dollars)
    roi             = margin / effective_price × 100    (display only)

Seller fees: StockX rows use the real fee structure in app/stockx_fees.py
(transaction fee by seller level + 3% payment processing + $5 minimum +
shipping) — NOT the old generic ~12% figure. GOAT/Alias rows still use
settings.commission_rate as a rough estimate.

An opportunity is flagged when margin > settings.min_margin_threshold (dollars),
plus the sales gate below.

`sales_last_7_days` is Optional[int]: pass an actual int (0 or more) only
when a platform genuinely reported a count (e.g. Alias, when a valid key
exists). Pass None when no source is available. None is treated as "unknown,
don't block on it" rather than "confirmed zero" — so opportunities surface on
margin alone until a real count is available, at which point the gate tightens
automatically for that platform.
"""
from dataclasses import dataclass
from typing import Optional

from app.config import settings
from app.services.effective_price import compute_effective_price
from app.stockx_fees import estimate_seller_fees


@dataclass
class OpportunityResult:
    cost: float                     # effective price — the true out-of-pocket cost basis
    payout: float
    margin: float                   # payout - cost, dollars
    roi: float
    sales_last_7_days: Optional[int]
    is_opportunity: bool
    price_after_discount: float
    cashback_rate: float
    cashback_portal: str
    cashback_amount: float
    resale_price_type: str          # lowest_ask | last_sale


def classify_opportunity(
    original_price: float,
    discount_percent: float,
    listing_price: float,
    sales_last_7_days: Optional[int],
    platform: str = "stockx",
    resale_price_type: str = "lowest_ask",
    min_margin_threshold: float = None,
    commission_rate: float = None,
    min_sales_7d: int = 1,
    discount_amount: float = 0.0,
    supplier_name: Optional[str] = None,
) -> OpportunityResult:
    if min_margin_threshold is None:
        min_margin_threshold = settings.min_margin_threshold
    if commission_rate is None:
        commission_rate = settings.commission_rate

    breakdown = compute_effective_price(
        list_price=original_price,
        discount_percent=discount_percent,
        discount_amount=discount_amount,
        supplier_name=supplier_name,
    )
    cost = breakdown.effective_price

    if platform == "stockx":
        fees = estimate_seller_fees(listing_price)
    else:
        fees = listing_price * commission_rate
    payout = listing_price - fees
    margin = payout - cost
    roi = margin / cost * 100.0 if cost > 0 else 0.0

    sales_known = sales_last_7_days is not None
    sales_ok = (not sales_known) or (sales_last_7_days >= min_sales_7d)
    is_opportunity = cost > 0 and margin > min_margin_threshold and sales_ok

    return OpportunityResult(
        cost=cost,
        payout=payout,
        margin=margin,
        roi=roi,
        sales_last_7_days=sales_last_7_days,
        is_opportunity=is_opportunity,
        price_after_discount=breakdown.price_after_discount,
        cashback_rate=breakdown.cashback_rate,
        cashback_portal=breakdown.cashback_portal,
        cashback_amount=breakdown.cashback_amount,
        resale_price_type=resale_price_type,
    )
