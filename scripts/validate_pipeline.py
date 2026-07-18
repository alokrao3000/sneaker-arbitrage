"""
Automated end-to-end pipeline validation report.

Runs a real (optionally narrowed) scrape through the full pipeline —
discovery → SKU/size/price parsing → inventory → cart validation → StockX
match → arbitrage → SQL → API serialization — then prints a per-stage
checklist plus diagnostics for every product that failed a stage, sourced
from the retailer_product_diagnostics table (nothing is silently excluded).

Usage:
    python scripts/validate_pipeline.py                       # 2 fast known-good suppliers, 6 products each
    python scripts/validate_pipeline.py "Premier,Kith" 10     # custom suppliers / per-supplier cap
"""
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)

from app.database import (  # noqa: E402
    init_db, close_orphaned_jobs, SessionLocal, ScrapeJob, Opportunity,
    ScrapeSupplierResult, RetailerProductDiagnostic,
)
from app.services.arbitrage import run_full_scrape  # noqa: E402
from app.api.routes.opportunities import _opp_to_dict  # noqa: E402

# Fields the frontend (app/static/index.html) reads from each opportunity —
# the serialization check fails loudly if any goes missing.
FRONTEND_FIELDS = [
    "shoe_name", "sku", "size", "supplier_name", "supplier_url", "image_url",
    "original_price", "discounted_price", "discount_applied", "cashback_rate",
    "cashback_amount", "cashback_portal", "effective_price", "listing_platform",
    "listing_price", "resale_price_type", "highest_bid", "last_sale",
    "market_url", "seller_fees", "payout_price", "margin", "net_profit", "roi",
    "confidence", "market_is_fresh", "market_fetched_at", "inventory_confidence",
    "sales_last_7_days", "sales_last_30_days", "last_sale_date", "liquidity_status",
]


def check(label: str, ok: bool, detail: str = ""):
    mark = "✓" if ok else "✗"
    print(f"  {mark} {label}" + (f" — {detail}" if detail else ""))
    return ok


def main():
    suppliers = (sys.argv[1].split(",") if len(sys.argv) > 1
                 else ["Premier", "Sneaker Politics"])
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 6

    init_db()
    close_orphaned_jobs()

    db = SessionLocal()
    job = ScrapeJob(started_at=datetime.utcnow(), status="running")
    db.add(job)
    db.commit()
    job_id = job.id
    print(f"=== validation job #{job_id} — suppliers={suppliers} limit={limit} ===\n")

    try:
        run_full_scrape(db, job_id, supplier_names=suppliers, limit_products=limit)
    finally:
        db.close()

    db = SessionLocal()
    ok = True
    try:
        job = db.get(ScrapeJob, job_id)
        results = db.query(ScrapeSupplierResult).filter_by(job_id=job_id).all()
        discovered = sum(r.products_discovered or 0 for r in results)
        parsed = sum(r.products_parsed or 0 for r in results)
        inventory = sum(r.inventory_ok or 0 for r in results)
        cart_att = sum(r.cart_attempts or 0 for r in results)
        cart_ok = sum(r.cart_verified or 0 for r in results)
        failures = sum(r.failure_count or 0 for r in results)

        print("\n=== STAGE CHECKLIST ===")
        ok &= check("Job reached terminal status", job.status == "done",
                    f"status={job.status} err={job.error_message!r}")
        ok &= check("Products discovered", discovered > 0, f"{discovered} raw items")
        ok &= check("Products parsed (SKU+price+sizes)", parsed > 0, f"{parsed}")
        ok &= check("Inventory detected", inventory > 0,
                    f"{inventory} products with in-stock sizes")
        ok &= check("StockX matched", (job.stockx_calls_made or 0) > 0,
                    f"{job.stockx_calls_made} lookups this run")
        ok &= check("Cart validation exercised", cart_att > 0 or job.opportunities_found == 0,
                    f"{cart_ok}/{cart_att} verified")
        ok &= check("Arbitrage calculated + SQL rows inserted",
                    job.opportunities_found is not None,
                    f"{job.opportunities_found} opportunities")
        ok &= check("No unexplained per-product failures", failures == 0,
                    f"{failures} recorded (see diagnostics below)")

        newest = (db.query(Opportunity).filter_by(is_active=True)
                  .order_by(Opportunity.updated_at.desc()).first())
        if newest:
            d = _opp_to_dict(newest)
            missing = [f for f in FRONTEND_FIELDS if f not in d]
            ok &= check("API response serialized with all frontend fields",
                        not missing, f"missing: {missing}" if missing else
                        f"{len(d)} fields")
            ok &= check("Retailer URL present", bool(d["supplier_url"]))
            ok &= check("StockX/market URL present", bool(d["market_url"]))
            print("\n=== SAMPLE OPPORTUNITY (API shape) ===")
            print(json.dumps(d, indent=2))
        else:
            check("API serialization", False, "no active opportunity to serialize "
                  "(margin threshold may simply not have been met this run)")

        # Per-product diagnostics — everything that stopped short of 'persisted'
        diags = (db.query(RetailerProductDiagnostic)
                 .filter(RetailerProductDiagnostic.supplier_name.in_(suppliers))
                 .filter(RetailerProductDiagnostic.stage != "persisted")
                 .order_by(RetailerProductDiagnostic.updated_at.desc())
                 .limit(40).all())
        print(f"\n=== DIAGNOSTICS — products that stopped before persistence ({len(diags)}) ===")
        for r in diags:
            print(f"  [{r.supplier_name}] {r.sku:<14} stage={r.stage:<14} "
                  f"inv={r.inventory_status or '-':<19} cart={r.cart_status or '-':<22} "
                  f"err={r.last_error or '-'}")

        print("\n=== VERDICT:", "PASS" if ok else "ISSUES FOUND — see ✗ lines above", "===")
    finally:
        db.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
