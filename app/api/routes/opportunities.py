from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.database import get_db, Opportunity, Supplier

router = APIRouter()


def _opp_to_dict(o: Opportunity) -> dict:
    return {
        "id":               o.id,
        "shoe_name":        o.shoe_name,
        "sku":              o.sku,
        "size":             o.size,
        "supplier_name":    o.supplier_name,
        "original_price":   float(o.original_price or 0),
        "discounted_price": float(o.discounted_price or 0),
        "discount_applied": o.discount_applied,
        "listing_platform": o.listing_platform,
        "listing_price":    float(o.listing_price or 0),
        "payout_price":     float(o.payout_price or 0),
        "roi":              round(float(o.roi or 0), 2),
        "sales_last_7_days":o.sales_last_7_days or 0,
        "supplier_url":     o.supplier_url,
        "market_url":       o.market_url,
        "found_at":         o.found_at.isoformat() if o.found_at else None,
        "updated_at":       o.updated_at.isoformat() if o.updated_at else None,
    }


@router.get("/", summary="List all active opportunities")
def list_opportunities(
    min_roi: float = Query(20.0, description="Minimum ROI %"),
    min_sales: int = Query(0, description="Minimum 7-day sales"),
    platform: Optional[str] = Query(None, description="stockx or goat"),
    supplier: Optional[str] = Query(None, description="Filter by supplier name (partial match)"),
    sku: Optional[str] = Query(None, description="Filter by SKU"),
    sort_by: str = Query("newest", description="Sort order: newest (recently added/restocked first) or roi (highest ROI first)"),
    limit: int = Query(500, le=2000),
    db: Session = Depends(get_db),
):
    q = db.query(Opportunity).filter(
        Opportunity.is_active == True,
        Opportunity.roi >= min_roi,
        Opportunity.sales_last_7_days >= min_sales,
    )

    if platform:
        q = q.filter(Opportunity.listing_platform == platform.lower())
    if supplier:
        q = q.filter(Opportunity.supplier_name.ilike(f"%{supplier}%"))
    if sku:
        q = q.filter(Opportunity.sku.ilike(f"%{sku}%"))

    if sort_by == "roi":
        order = [desc(Opportunity.roi), desc(Opportunity.updated_at)]
    else:
        # "newest" — most recently detected or refreshed opportunity first.
        # updated_at is bumped every time a scrape confirms the opportunity,
        # so restocks and new drops both surface at the top.
        order = [desc(Opportunity.updated_at), desc(Opportunity.roi)]

    results = q.order_by(*order).limit(limit).all()
    return [_opp_to_dict(o) for o in results]


@router.get("/stats", summary="Summary statistics")
def get_stats(db: Session = Depends(get_db)):
    from sqlalchemy import func
    active = db.query(Opportunity).filter(Opportunity.is_active == True)
    total  = active.count()
    avg_roi = db.query(func.avg(Opportunity.roi)).filter(Opportunity.is_active == True).scalar()
    best_roi = db.query(func.max(Opportunity.roi)).filter(Opportunity.is_active == True).scalar()
    total_suppliers = db.query(Supplier).filter(Supplier.active == True).count()

    from app.database import ScrapeJob
    last_job = (
        db.query(ScrapeJob)
        .order_by(desc(ScrapeJob.started_at))
        .first()
    )

    return {
        "total_opportunities": total,
        "avg_roi": round(float(avg_roi or 0), 2),
        "best_roi": round(float(best_roi or 0), 2),
        "total_active_suppliers": total_suppliers,
        "last_job": {
            "id": last_job.id if last_job else None,
            "status": last_job.status if last_job else None,
            "started_at": last_job.started_at.isoformat() if last_job else None,
            "finished_at": last_job.finished_at.isoformat() if last_job and last_job.finished_at else None,
            "opportunities_found": last_job.opportunities_found if last_job else 0,
        } if last_job else None,
    }


@router.get("/{opp_id}", summary="Get a single opportunity")
def get_opportunity(opp_id: int, db: Session = Depends(get_db)):
    from fastapi import HTTPException
    o = db.get(Opportunity, opp_id)
    if not o:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    return _opp_to_dict(o)
