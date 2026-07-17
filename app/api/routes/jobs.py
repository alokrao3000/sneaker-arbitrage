"""
Job control and Server-Sent Events (SSE) for live progress streaming.
"""
import asyncio
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
from sqlalchemy.orm import Session

from app.database import get_db, ScrapeJob, Supplier, ScrapeSupplierResult
from app.services import scheduler as sched
from app.services.supplier_loader import load_suppliers
from app.cashback_rates import get_cashback

router = APIRouter()


@router.post("/scrape", summary="Trigger a full scrape run")
def trigger_scrape():
    job_id = sched.trigger_scrape()
    if job_id is None and sched.get_status()["is_running"]:
        return JSONResponse({"status": "already_running"}, status_code=409)
    return {"status": "started", "job_id": job_id}


@router.post("/scrape/quick", summary="Quick scan — tier0 stores with discounts only")
def trigger_quick_scan():
    job_id = sched.trigger_tier0_scan()
    if job_id is None and sched.get_status()["is_running"]:
        return JSONResponse({"status": "already_running"}, status_code=409)
    return {"status": "started", "job_id": job_id}


@router.get("/status", summary="Current scheduler status")
def get_status():
    return sched.get_status()


@router.get("/progress", summary="SSE stream of live scrape messages")
async def progress_stream():
    """
    Server-Sent Events endpoint. The dashboard connects here and receives
    live log lines while a scrape is running.
    """
    async def event_generator():
        sent = 0
        while True:
            msgs = sched.get_progress_messages()
            for msg in msgs[sent:]:
                yield {"data": msg}
                sent += 1

            status = sched.get_status()
            if not status["is_running"] and sent >= len(msgs):
                yield {"event": "done", "data": "scrape_complete"}
                break

            await asyncio.sleep(0.5)

    return EventSourceResponse(event_generator())


@router.post("/init-suppliers", summary="(Re)seed suppliers from CSV")
def init_suppliers(db: Session = Depends(get_db)):
    count = load_suppliers(db)
    return {"loaded": count}


@router.get("/suppliers", summary="List all suppliers with stats")
def list_suppliers(db: Session = Depends(get_db)):
    suppliers = db.query(Supplier).order_by(Supplier.category, Supplier.name).all()
    result = []
    for s in suppliers:
        cashback = get_cashback(s.name)
        result.append({
            "id":               s.id,
            "name":             s.name,
            "url":              s.url,
            "category":         s.category,
            "platform_type":    s.platform_type,
            "discount_percent": float(s.discount_percent or 0),
            "discount_amount":  float(s.discount_amount or 0),
            "discount_notes":   s.discount_notes,
            "cashback_rate":    cashback.rate,
            "cashback_portal":  cashback.portal,
            "active":           s.active,
        })
    return result


@router.patch("/suppliers/{supplier_id}/toggle", summary="Enable or disable a supplier")
def toggle_supplier(supplier_id: int, db: Session = Depends(get_db)):
    s = db.get(Supplier, supplier_id)
    if not s:
        raise HTTPException(status_code=404, detail="Supplier not found")
    s.active = not s.active
    db.commit()
    return {"id": s.id, "name": s.name, "active": s.active}


@router.get("/{job_id}/suppliers", summary="Per-retailer scrape status for one job")
def job_supplier_results(job_id: int, db: Session = Depends(get_db)):
    rows = (
        db.query(ScrapeSupplierResult)
        .filter_by(job_id=job_id)
        .order_by(ScrapeSupplierResult.supplier_name)
        .all()
    )
    return [
        {
            "supplier_name":   r.supplier_name,
            "status":          r.status,
            "items_found":     r.items_found,
            "error_message":   r.error_message,
            "elapsed_seconds": float(r.elapsed_seconds) if r.elapsed_seconds is not None else None,
        }
        for r in rows
    ]


@router.get("/history", summary="Recent scrape job history")
def job_history(limit: int = 20, db: Session = Depends(get_db)):
    from sqlalchemy import desc
    jobs = db.query(ScrapeJob).order_by(desc(ScrapeJob.started_at)).limit(limit).all()
    return [
        {
            "id": j.id,
            "status": j.status,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "finished_at": j.finished_at.isoformat() if j.finished_at else None,
            "suppliers_scraped": j.suppliers_scraped,
            "skus_found": j.skus_found,
            "opportunities_found": j.opportunities_found,
            "stockx_calls_made": j.stockx_calls_made or 0,
            "stockx_calls_skipped": j.stockx_calls_skipped or 0,
            "error_message": j.error_message,
        }
        for j in jobs
    ]
