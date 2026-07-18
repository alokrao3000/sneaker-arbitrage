from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import case, desc, func, or_

from app.config import settings
from app.database import get_db, Opportunity, Supplier
from app.stockx_fees import US_SHIPPING_FEE_USD

router = APIRouter()


def _confidence(o: Opportunity, market_is_fresh: bool) -> str:
    """Coarse trust score for the quoted margin.

    high   — fresh market data AND priced from a live lowest ask
    medium — one of the two degraded (cached data, or priced from last sale)
    low    — both degraded, or no market timestamp at all
    """
    fresh = market_is_fresh
    live_ask = (o.resale_price_type or "lowest_ask") == "lowest_ask"
    if fresh and live_ask:
        return "high"
    if fresh or live_ask:
        return "medium"
    return "low"


def _opp_to_dict(o: Opportunity) -> dict:
    # "fresh" = the market data behind this row is younger than the StockX
    # market TTL; anything older is a cached evaluation.
    ttl = timedelta(hours=settings.stockx_market_ttl_hours)
    market_is_fresh = bool(o.market_fetched_at and datetime.utcnow() - o.market_fetched_at < ttl)
    listing_price = float(o.listing_price or 0)
    payout_price = float(o.payout_price or 0)
    # seller_fees was added later — derive it for rows written before the column existed
    seller_fees = (float(o.seller_fees) if o.seller_fees is not None
                   else round(listing_price - payout_price, 2) if listing_price and payout_price
                   else None)
    return {
        "id":               o.id,
        "shoe_name":        o.shoe_name,
        "sku":              o.sku,
        "size":             o.size,
        # Retailer side
        "supplier_name":    o.supplier_name,
        "supplier_url":     o.supplier_url,
        "image_url":        o.image_url,
        "inventory_confidence": o.inventory_confidence or "UNKNOWN",
        "cart_status":      o.cart_status,   # CART_* reason; null = not validated
        "cart_checked_at":  o.cart_checked_at.isoformat() if o.cart_checked_at else None,
        "original_price":   float(o.original_price or 0),
        "discounted_price": float(o.discounted_price or 0),
        "discount_applied": o.discount_applied,
        "cashback_rate":    float(o.cashback_rate or 0),
        "cashback_portal":  o.cashback_portal,
        "cashback_amount":  float(o.cashback_amount or 0),
        "effective_price":  float(o.effective_price if o.effective_price is not None else o.discounted_price or 0),
        # Market side
        "listing_platform": o.listing_platform,
        "market_url":       o.market_url,
        "listing_price":    listing_price,
        "resale_price_type": o.resale_price_type or "lowest_ask",
        "highest_bid":      float(o.highest_bid) if o.highest_bid is not None else None,
        "last_sale":        float(o.last_sale) if o.last_sale is not None else None,
        # Financials
        "seller_fees":      seller_fees,
        "shipping":         US_SHIPPING_FEE_USD if o.listing_platform == "stockx" else 0.0,
        "payout_price":     payout_price,
        # gross = payout vs what the register charges (before cashback);
        # net   = payout vs true effective cost (margin — the headline number)
        "gross_profit":     round(payout_price - float(o.discounted_price or 0), 2)
                            if payout_price else None,
        "net_profit":       round(float(o.margin), 2) if o.margin is not None else None,
        "margin":           round(float(o.margin), 2) if o.margin is not None else None,
        "roi":              round(float(o.roi or 0), 2),
        "confidence":       _confidence(o, market_is_fresh),
        "market_fetched_at": o.market_fetched_at.isoformat() if o.market_fetched_at else None,
        "market_is_fresh":  market_is_fresh,
        # Liquidity — null counts = unknown, not confirmed zero. Rows that
        # predate the liquidity gate have NULL status → reported as unknown.
        "sales_last_7_days": o.sales_last_7_days,
        "sales_last_30_days": o.sales_last_30_days,
        "last_sale_date":   o.last_sale_date.isoformat() if o.last_sale_date else None,
        "liquidity_status": o.liquidity_status or "unknown",
        # eBay context — ebay_price is a current ACTIVE ASK (see
        # ebay_price_type), never a realized sale price; watch count / demand
        # rank are soft signals and are not sales evidence.
        "ebay_price":       float(o.ebay_price) if o.ebay_price is not None else None,
        "ebay_price_type":  o.ebay_price_type,
        "ebay_min_ask":     float(o.ebay_min_ask) if o.ebay_min_ask is not None else None,
        "ebay_max_ask":     float(o.ebay_max_ask) if o.ebay_max_ask is not None else None,
        "ebay_active_listings": o.ebay_active_listings,
        "ebay_watch_count": o.ebay_watch_count,
        "ebay_demand_rank": o.ebay_demand_rank,
        "ebay_margin":      float(o.ebay_margin) if o.ebay_margin is not None else None,
        "ebay_roi":         float(o.ebay_roi) if o.ebay_roi is not None else None,
        "ebay_fetched_at":  o.ebay_fetched_at.isoformat() if o.ebay_fetched_at else None,
        "recommended_platform": o.recommended_platform,
        "recommendation_confidence": o.recommendation_confidence,
        "found_at":         o.found_at.isoformat() if o.found_at else None,
        "updated_at":       o.updated_at.isoformat() if o.updated_at else None,
    }


@router.get("/", summary="List all active opportunities")
def list_opportunities(
    min_roi: float = Query(20.0, description="Minimum ROI %"),
    min_profit: float = Query(0.0, description="Minimum net profit (margin) in dollars"),
    min_sales_7d: int = Query(0, description="Minimum sales in the last 7 days"),
    min_sales_30d: int = Query(0, description="Minimum sales in the last 30 days"),
    min_sales: int = Query(0, description="Deprecated alias of min_sales_7d"),
    liquidity: Optional[str] = Query(None, description="'eligible' to only return confirmed-liquid rows"),
    platform: Optional[str] = Query(None, description="stockx or goat"),
    supplier: Optional[str] = Query(None, description="Filter by supplier name (partial match)"),
    brand: Optional[str] = Query(None, description="Filter by brand/model (partial match on shoe name)"),
    sku: Optional[str] = Query(None, description="Filter by SKU"),
    sort_by: str = Query("best", description="Sort order: best (liquidity-weighted profitability), "
                                             "newest (recently added/restocked first), "
                                             "roi (highest ROI first), or margin"),
    limit: int = Query(500, le=2000),
    db: Session = Depends(get_db),
):
    min_sales_7d = max(min_sales_7d, min_sales)   # honor the deprecated param
    q = db.query(Opportunity).filter(
        Opportunity.is_active == True,
        Opportunity.roi >= min_roi,
    )
    if min_profit > 0:
        q = q.filter(Opportunity.margin >= min_profit)
    # NULL sales counts mean "unknown", not "confirmed zero" — only exclude
    # them when the caller actually asked for a positive minimum. The default
    # (0) includes opportunities with no sales data available.
    if min_sales_7d > 0:
        q = q.filter(Opportunity.sales_last_7_days >= min_sales_7d)
    if min_sales_30d > 0:
        q = q.filter(Opportunity.sales_last_30_days >= min_sales_30d)
    if liquidity == "eligible":
        q = q.filter(Opportunity.liquidity_status == "eligible")
    else:
        # Mirror the persistence gate (app/services/liquidity.py) at read time
        # so rows written before the gate existed (NULL status) can't slip
        # through the UI: non-eligible rows only display with the live-bid
        # demand proxy, exactly as new evaluations are admitted.
        if settings.liquidity_allow_unknown_with_bid:
            q = q.filter(or_(
                Opportunity.liquidity_status == "eligible",
                Opportunity.highest_bid > 0,
            ))
        else:
            q = q.filter(Opportunity.liquidity_status == "eligible")

    if platform:
        q = q.filter(Opportunity.listing_platform == platform.lower())
    if supplier:
        q = q.filter(Opportunity.supplier_name.ilike(f"%{supplier}%"))
    if brand:
        q = q.filter(Opportunity.shoe_name.ilike(f"%{brand}%"))
    if sku:
        q = q.filter(Opportunity.sku.ilike(f"%{sku}%"))

    # Confirmed-liquid rows always outrank unknown-liquidity ones (NULL status
    # predates the gate and counts as unknown; not_eligible is never persisted).
    liq_rank = case((Opportunity.liquidity_status == "eligible", 2), else_=1)
    # Liquidity-weighted profitability: margin scaled by a 30-day volume boost
    # (1.0× at 0 known sales up to 2.0× at ≥30), so a $40 flip that sells
    # daily ranks above a $60 flip that sold five times this month.
    volume_boost = 1.0 + func.least(func.coalesce(Opportunity.sales_last_30_days, 0), 30) / 30.0
    score = func.coalesce(Opportunity.margin, 0) * volume_boost

    if sort_by == "roi":
        order = [desc(Opportunity.roi), desc(Opportunity.updated_at)]
    elif sort_by == "margin":
        order = [desc(Opportunity.margin), desc(Opportunity.roi)]
    elif sort_by == "newest":
        # Most recently detected or refreshed opportunity first. updated_at is
        # bumped every time a scrape confirms the opportunity, so restocks and
        # new drops both surface at the top.
        order = [desc(Opportunity.updated_at), desc(Opportunity.roi)]
    else:
        # "best" (default) — see liq_rank/score above.
        order = [desc(liq_rank), desc(score), desc(Opportunity.roi)]

    results = q.order_by(*order).limit(limit).all()
    return [_opp_to_dict(o) for o in results]


@router.get("/stats", summary="Summary statistics")
def get_stats(db: Session = Depends(get_db)):
    from sqlalchemy import func
    active = db.query(Opportunity).filter(Opportunity.is_active == True)
    total  = active.count()
    liquid = active.filter(Opportunity.liquidity_status == "eligible").count()
    avg_roi = db.query(func.avg(Opportunity.roi)).filter(Opportunity.is_active == True).scalar()
    best_roi = db.query(func.max(Opportunity.roi)).filter(Opportunity.is_active == True).scalar()
    total_suppliers = db.query(Supplier).filter(Supplier.active == True).count()

    from app.database import ScrapeJob
    last_job = (
        db.query(ScrapeJob)
        .order_by(desc(ScrapeJob.started_at))
        .first()
    )

    # StockX daily budget — the app's real constraint once the official API
    # is in use. calls_today counts HTTP requests (a SKU lookup = 2–3).
    from app.scrapers import stockx_api as sx
    stockx_budget = None
    if sx.is_configured():
        from app.database import StockXApiUsage
        row = db.get(StockXApiUsage, datetime.utcnow().strftime("%Y-%m-%d"))
        stockx_budget = {
            "calls_today": row.calls if row else 0,
            "daily_limit": sx.DAILY_REQUEST_LIMIT,
        }

    return {
        "total_opportunities": total,
        "liquid_opportunities": liquid,   # confirmed-liquid (liquidity_status = eligible)
        "avg_roi": round(float(avg_roi or 0), 2),
        "best_roi": round(float(best_roi or 0), 2),
        "total_active_suppliers": total_suppliers,
        "stockx_budget": stockx_budget,
        "last_job": {
            "id": last_job.id if last_job else None,
            "status": last_job.status if last_job else None,
            "started_at": last_job.started_at.isoformat() if last_job else None,
            "finished_at": last_job.finished_at.isoformat() if last_job and last_job.finished_at else None,
            "opportunities_found": last_job.opportunities_found if last_job else 0,
            "stockx_calls_made": last_job.stockx_calls_made or 0,
            "stockx_calls_skipped": last_job.stockx_calls_skipped or 0,
        } if last_job else None,
    }


@router.get("/{opp_id}", summary="Get a single opportunity")
def get_opportunity(opp_id: int, db: Session = Depends(get_db)):
    from fastapi import HTTPException
    o = db.get(Opportunity, opp_id)
    if not o:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    return _opp_to_dict(o)
