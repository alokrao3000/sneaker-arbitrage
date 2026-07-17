"""
Per-SKU gate for StockX market lookups — the app is rate-limited by StockX's
shared daily budget now, not wall-clock time, so every skippable call matters.

Rules (in order):
  CHECK when the SKU has never been checked, OR the new effective price is
  STRICTLY lower than the best (lowest) effective price already evaluated, OR
  the cached market data is older than the TTL (settings.stockx_market_ttl_hours
  — deliberately shorter than retail-side caching; resale prices move faster).

  SKIP otherwise — the price is >= the best already on record and that price
  was already evaluated:
    - last verdict not_profitable / no_market_data → just bump last_seen_at,
      no StockX call, no re-evaluation.
    - last verdict profitable → no StockX call either, but the caller should
      re-evaluate against the CACHED market rows (market_prices.fetched_at
      tells the dashboard it's cached, not fresh).
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.config import settings
from app.database import SkuMarketCache

logger = logging.getLogger(__name__)

VERDICT_PROFITABLE = "profitable"
VERDICT_NOT_PROFITABLE = "not_profitable"
VERDICT_NO_MARKET_DATA = "no_market_data"


@dataclass
class GateDecision:
    check: bool
    reason: str            # never_checked | lower_price | ttl_expired |
                           # cached_profitable | cached_not_profitable
    cached: Optional[SkuMarketCache] = None

    @property
    def evaluate_from_cache(self) -> bool:
        return not self.check and self.reason == "cached_profitable"


def gate_stockx_check(db: Session, sku: str, effective_price: float,
                      now: Optional[datetime] = None) -> GateDecision:
    now = now or datetime.utcnow()
    row = db.get(SkuMarketCache, sku)

    if row is None or row.last_checked_at is None:
        return GateDecision(check=True, reason="never_checked", cached=row)
    if row.best_effective_price is None or effective_price < float(row.best_effective_price):
        return GateDecision(check=True, reason="lower_price", cached=row)
    if now - row.last_checked_at > timedelta(hours=settings.stockx_market_ttl_hours):
        return GateDecision(check=True, reason="ttl_expired", cached=row)

    reason = ("cached_profitable" if row.last_verdict == VERDICT_PROFITABLE
              else "cached_not_profitable")
    return GateDecision(check=False, reason=reason, cached=row)


def record_seen(db: Session, sku: str, now: Optional[datetime] = None):
    """A skip still proves the SKU is alive — bump last_seen_at only."""
    now = now or datetime.utcnow()
    row = db.get(SkuMarketCache, sku)
    if row is None:
        row = SkuMarketCache(sku=sku)
        db.add(row)
    row.last_seen_at = now


def record_check(db: Session, sku: str, effective_price: float, verdict: str,
                 resale_price: Optional[float] = None,
                 resale_price_type: Optional[str] = None,
                 now: Optional[datetime] = None):
    """Persist the outcome of an actual StockX market fetch + evaluation."""
    now = now or datetime.utcnow()
    row = db.get(SkuMarketCache, sku)
    if row is None:
        row = SkuMarketCache(sku=sku)
        db.add(row)
    if row.best_effective_price is None or effective_price < float(row.best_effective_price):
        row.best_effective_price = effective_price
    row.last_verdict = verdict
    row.last_checked_at = now
    row.last_seen_at = now
    row.resale_price = resale_price
    row.resale_price_type = resale_price_type
