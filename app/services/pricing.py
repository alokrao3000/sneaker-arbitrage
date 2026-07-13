"""
ROI / opportunity classification — the single source of truth shared by the
batch scrape job (app/services/arbitrage.py) and the live SKU lookup endpoint
(app/api/routes/sku.py), so the two never diverge.

`sales_last_7_days` is Optional[int]: pass an actual int (0 or more) only
when a platform genuinely reported a count (e.g. Alias, when a valid key
exists). Pass None when no source is available — as of this session, that's
every platform: no ALIAS_API_KEY exists, GOAT's web product page was
confirmed to expose no sales history at all, and StockX's source is still
unresolved (see stockx_market.py/goat_market.py docstrings). None is treated
as "unknown, don't block on it" rather than "confirmed zero" — so opportunities
surface on ROI alone until a real count is available, at which point the gate
tightens automatically for that platform.
"""
from dataclasses import dataclass
from typing import Optional

from app.config import settings


@dataclass
class OpportunityResult:
    cost: float
    payout: float
    roi: float
    sales_last_7_days: Optional[int]
    is_opportunity: bool


def classify_opportunity(
    original_price: float,
    discount_percent: float,
    listing_price: float,
    sales_last_7_days: Optional[int],
    roi_threshold: float = None,
    commission_rate: float = None,
    min_sales_7d: int = 1,
) -> OpportunityResult:
    if roi_threshold is None:
        roi_threshold = settings.roi_threshold
    if commission_rate is None:
        commission_rate = settings.commission_rate

    cost = original_price * (1.0 - discount_percent / 100.0)
    payout = listing_price * (1.0 - commission_rate)
    roi = (payout - cost) / cost * 100.0 if cost > 0 else 0.0
    sales_known = sales_last_7_days is not None
    sales_ok = (not sales_known) or (sales_last_7_days >= min_sales_7d)
    is_opportunity = cost > 0 and roi >= roi_threshold and sales_ok
    return OpportunityResult(
        cost=cost,
        payout=payout,
        roi=roi,
        sales_last_7_days=sales_last_7_days,
        is_opportunity=is_opportunity,
    )
