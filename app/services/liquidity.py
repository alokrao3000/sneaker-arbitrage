"""
Sales-liquidity gate — the single source of truth for "is this shoe actually
selling", shared by the batch pipeline (app/services/arbitrage.py) and the
live SKU lookup (app/api/routes/sku.py) via app/services/pricing.py.

Eligibility rule (on top of the existing margin gate):
    a product is liquid enough to surface when EITHER
      - sales in the last 7 days  >= settings.liquidity_min_sales_7d  (default 1), OR
      - sales in the last 30 days >= settings.liquidity_min_sales_30d (default 5).
    When sales counts are KNOWN and neither condition holds, the product is
    NOT an opportunity — not returned, not displayed, not persisted.

Data sources, in preference order:
  1. Alias recent_sales (live, 30-day window) — the only partner API that
     exposes individual sale events. The official StockX public API exposes
     NO sales history: the market-data endpoint has no last-sale field and no
     sales-count/sales-list endpoint exists (live-verified 2026-07-17 — see
     app/scrapers/stockx_api.py's module docstring). GOAT's public site
     exposes none either.
  2. The sale_records table (events persisted by previous successful Alias
     fetches). Only trusted while collection is demonstrably current — if the
     newest record for a SKU was RECORDED more than 30 days ago, a count of 0
     means "we stopped collecting", not "nothing sold", and the snapshot
     degrades to unknown.
  3. Nothing → sales counts are UNKNOWN. Documented limitation: with no valid
     ALIAS_API_KEY there is no sales-history source at all. The best available
     alternative from the market data we do have is a live highest bid — a
     real committed buyer at this exact size. When
     settings.liquidity_allow_unknown_with_bid is true (default), an
     unknown-liquidity product passes the gate only if the size has a live
     bid; its status is persisted as "unknown" so it ranks below confirmed-
     liquid rows and remains filterable/auditable.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import settings

LIQUIDITY_ELIGIBLE = "eligible"
LIQUIDITY_NOT_ELIGIBLE = "not_eligible"
LIQUIDITY_UNKNOWN = "unknown"


@dataclass
class LiquiditySnapshot:
    """Per-SKU sales activity. Counts are Optional: None = no source available
    (unknown), never a silent 0."""
    sales_last_7_days: Optional[int] = None
    sales_last_30_days: Optional[int] = None
    last_sale_date: Optional[datetime] = None
    source: str = "none"      # alias_live | sale_history | none

    @property
    def known(self) -> bool:
        return self.sales_last_7_days is not None or self.sales_last_30_days is not None


@dataclass
class LiquidityVerdict:
    status: str        # eligible | not_eligible | unknown
    passes: bool       # may this product surface as an opportunity?
    reason: str        # human-readable, for logs/diagnostics


def summarize_sales_events(events: List[dict],
                           now: Optional[datetime] = None) -> LiquiditySnapshot:
    """Fold a list of {"price", "sale_date", "size"} events (Alias shape,
    newest-first, already capped at a 30-day window) into a snapshot.
    Events without a parseable date are counted in both windows — same
    conservative-inclusive treatment the 7-day counter always used."""
    now = now or datetime.utcnow()
    c7 = c30 = 0
    last_sale: Optional[datetime] = None
    for ev in events:
        dt = ev.get("sale_date")
        if dt is None:
            c7 += 1
            c30 += 1
            continue
        naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
        age = now - naive
        if age <= timedelta(days=30):
            c30 += 1
            if age <= timedelta(days=7):
                c7 += 1
        if last_sale is None or naive > last_sale:
            last_sale = naive
    return LiquiditySnapshot(sales_last_7_days=c7, sales_last_30_days=c30,
                             last_sale_date=last_sale, source="alias_live")


def snapshot_from_sale_records(db: Session, sku: str,
                               now: Optional[datetime] = None) -> LiquiditySnapshot:
    """Best-effort snapshot from previously persisted sale events, for runs
    where the live Alias fetch is unavailable. Counts are only trusted while
    collection is current (a record RECORDED within the last 30 days) —
    otherwise 0 would conflate "stopped collecting" with "stopped selling"."""
    from app.database import SaleRecord   # local import: avoid a cycle at module load

    now = now or datetime.utcnow()
    newest_recorded = (
        db.query(func.max(SaleRecord.recorded_at))
        .filter(SaleRecord.sku == sku)
        .scalar()
    )
    if newest_recorded is None or now - newest_recorded > timedelta(days=30):
        return LiquiditySnapshot(source="none")

    c7, c30, last_sale = (
        db.query(
            func.count().filter(SaleRecord.sale_date >= now - timedelta(days=7)),
            func.count().filter(SaleRecord.sale_date >= now - timedelta(days=30)),
            func.max(SaleRecord.sale_date),
        )
        .filter(SaleRecord.sku == sku)
        .one()
    )
    return LiquiditySnapshot(sales_last_7_days=int(c7 or 0),
                             sales_last_30_days=int(c30 or 0),
                             last_sale_date=last_sale, source="sale_history")


def evaluate_liquidity(snapshot: LiquiditySnapshot,
                       highest_bid: Optional[float] = None) -> LiquidityVerdict:
    """Apply the eligibility rule. `highest_bid` is the live bid for the exact
    size being classified — only consulted on the unknown path (see module
    docstring §3)."""
    min7 = settings.liquidity_min_sales_7d
    min30 = settings.liquidity_min_sales_30d

    if snapshot.known:
        s7, s30 = snapshot.sales_last_7_days, snapshot.sales_last_30_days
        if (s7 is not None and s7 >= min7) or (s30 is not None and s30 >= min30):
            return LiquidityVerdict(
                status=LIQUIDITY_ELIGIBLE, passes=True,
                reason=f"{s7 if s7 is not None else '?'} sales/7d, "
                       f"{s30 if s30 is not None else '?'} sales/30d [{snapshot.source}]",
            )
        return LiquidityVerdict(
            status=LIQUIDITY_NOT_ELIGIBLE, passes=False,
            reason=f"insufficient sales ({s7 or 0}/7d < {min7} and "
                   f"{s30 or 0}/30d < {min30}) [{snapshot.source}]",
        )

    # No sales source at all — fall back to the live-bid demand proxy.
    if settings.liquidity_allow_unknown_with_bid:
        if highest_bid is not None and highest_bid > 0:
            return LiquidityVerdict(
                status=LIQUIDITY_UNKNOWN, passes=True,
                reason=f"sales unknown; live bid ${highest_bid:.0f} as demand proxy",
            )
        return LiquidityVerdict(
            status=LIQUIDITY_UNKNOWN, passes=False,
            reason="sales unknown and no live bid — no demand evidence",
        )
    return LiquidityVerdict(
        status=LIQUIDITY_UNKNOWN, passes=False,
        reason="sales unknown (strict mode: unknown is excluded)",
    )
