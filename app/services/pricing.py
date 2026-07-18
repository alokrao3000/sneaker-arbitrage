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

An opportunity is flagged when BOTH gates pass:
  1. margin > settings.min_margin_threshold (dollars), AND
  2. the sales-liquidity gate (app/services/liquidity.py): ≥1 sale in the
     last 7 days OR ≥5 sales in the last 30 days (thresholds configurable).
     Known-illiquid products are excluded outright; when no sales source
     exists the live highest bid acts as the demand proxy — see the liquidity
     module docstring for the source hierarchy and the documented StockX API
     limitation (its public API exposes no sales history at all).

Sales counts are Optional[int]: an actual int (0 or more) only when a source
genuinely reported/derived a count; None means "no source available". The
liquidity gate, not the caller, decides how unknown is treated.
"""
from dataclasses import dataclass
from typing import Optional

from app.config import settings
from app.ebay_fees import estimate_ebay_seller_fees
from app.services.effective_price import compute_effective_price
from app.services.liquidity import (
    LiquiditySnapshot, LiquidityVerdict, evaluate_liquidity,
)
from app.stockx_fees import estimate_seller_fees


@dataclass
class OpportunityResult:
    cost: float                     # effective price — the true out-of-pocket cost basis
    payout: float
    margin: float                   # payout - cost, dollars
    roi: float
    sales_last_7_days: Optional[int]
    sales_last_30_days: Optional[int]
    liquidity: LiquidityVerdict     # status + pass/fail + reason (for logs)
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
    sales_last_7_days: Optional[int] = None,
    sales_last_30_days: Optional[int] = None,
    liquidity_snapshot: Optional[LiquiditySnapshot] = None,
    highest_bid: Optional[float] = None,
    platform: str = "stockx",
    resale_price_type: str = "lowest_ask",
    min_margin_threshold: float = None,
    commission_rate: float = None,
    discount_amount: float = 0.0,
    supplier_name: Optional[str] = None,
) -> OpportunityResult:
    """Pass either a ready LiquiditySnapshot (batch pipeline) or the raw
    sales_last_7_days/sales_last_30_days counts (live lookup) — the snapshot
    wins when both are given. highest_bid is this size's live bid, consulted
    only when sales counts are unknown."""
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

    snapshot = liquidity_snapshot or LiquiditySnapshot(
        sales_last_7_days=sales_last_7_days,
        sales_last_30_days=sales_last_30_days,
        source="caller" if (sales_last_7_days is not None
                            or sales_last_30_days is not None) else "none",
    )
    liquidity = evaluate_liquidity(snapshot, highest_bid=highest_bid)

    is_opportunity = (cost > 0 and margin > min_margin_threshold
                      and liquidity.passes)

    return OpportunityResult(
        cost=cost,
        payout=payout,
        margin=margin,
        roi=roi,
        sales_last_7_days=snapshot.sales_last_7_days,
        sales_last_30_days=snapshot.sales_last_30_days,
        liquidity=liquidity,
        is_opportunity=is_opportunity,
        price_after_discount=breakdown.price_after_discount,
        cashback_rate=breakdown.cashback_rate,
        cashback_portal=breakdown.cashback_portal,
        cashback_amount=breakdown.cashback_amount,
        resale_price_type=resale_price_type,
    )


# ── eBay reference margin + platform recommendation ──────────────────────────
# eBay has no public sold-data source (see app/scrapers/ebay.py's module
# docstring), so its numbers here are a REFERENCE computed against a current
# ACTIVE ask. They exist for context and tie-breaking only; classify_opportunity
# above deliberately has no eBay inputs, so nothing eBay-derived can reach the
# sales-liquidity gate.

@dataclass
class EbayReference:
    ask: float                        # median current ACTIVE ask — not a realized price
    price_type: str                   # always "active_ask" until Marketplace Insights lands
    fees: float
    payout: float
    margin: float
    roi: float


def compute_ebay_reference(cost: float, active_ask: float) -> EbayReference:
    """Reference economics if this pair were sold on eBay AT THE CURRENT
    MEDIAN ASK. Labeled active_ask because that price is a live listing you'd
    have to match, not evidence anything cleared at it."""
    fees = estimate_ebay_seller_fees(active_ask)
    payout = active_ask - fees
    margin = payout - cost
    roi = margin / cost * 100.0 if cost > 0 else 0.0
    return EbayReference(ask=active_ask, price_type="active_ask", fees=fees,
                         payout=payout, margin=margin, roi=roi)


def recommend_platform(
    stockx_sales_7d: Optional[int],
    stockx_sales_30d: Optional[int],
    primary_platform: str,
    primary_margin: Optional[float],
    ebay_margin: Optional[float] = None,
    ebay_watch_count: Optional[int] = None,
    ebay_demand_rank: Optional[int] = None,
) -> "tuple[str, str]":
    """(recommended_platform, recommendation_confidence).

    primary_platform is the platform whose real market data backs the margin
    (stockx, or goat/alias on fallback). Rules, per the eBay-data reality:

      1. When the primary platform's sales counts are GENUINELY KNOWN (either
         window is an int), it wins outright — confidence 'high'. eBay ask
         prices and watch counts are never allowed to outvote actual sales.
      2. Only when sales data is truly unknown may eBay break the tie, and
         only with an actual demand signal (watch count > 0 or a merchandised
         rank) AND a better reference margin — flagged confidence 'low'
         because it rests on asks + watches, not realized sales.
      3. Otherwise stay on the primary platform at confidence 'low' (its
         margin is at least backed by a real bid/ask book).
    """
    sales_known = stockx_sales_7d is not None or stockx_sales_30d is not None
    if sales_known:
        return primary_platform, "high"

    has_demand_signal = ((ebay_watch_count or 0) > 0
                         or ebay_demand_rank is not None)
    if (has_demand_signal and ebay_margin is not None
            and (primary_margin is None or ebay_margin > primary_margin)):
        return "ebay", "low"
    return primary_platform, "low"
