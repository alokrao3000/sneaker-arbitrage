"""
Core arbitrage engine.

For each active supplier:
  1. Scrape shoes using the appropriate scraper (Shopify / Footlocker)
  2. For each shoe SKU found, look up market prices on StockX (primary) then GOAT
  3. Calculate ROI per available size:
       discounted_price = original_price * (1 - discount_percent / 100)
       payout           = listing_price  * (1 - COMMISSION_RATE)
       roi              = (payout - discounted_price) / discounted_price * 100
  4. Persist opportunities with ROI >= threshold
  5. Record sales history for 7-day volume tracking
"""
import logging
import time
from datetime import datetime
from typing import List, Optional, Callable

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import settings
from app.database import (
    Supplier, SupplierProduct, SupplierProductSize,
    MarketPrice, SaleRecord, Opportunity, ScrapeJob
)
from app.scrapers.base import ScrapedProduct, rate_limit
from app.scrapers.shopify import ShopifyScraper
from app.scrapers.footlocker import FootlockerScraper
from app.scrapers.kicks_dev import KicksDevClient
from app.scrapers.stockx import StockXKicksClient, StockXProduct
from app.scrapers.goat import GoatScraper
from app.scrapers.alias import AliasClient

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], None]   # called with status messages


# ── Scraper dispatch ──────────────────────────────────────────────────────────

def _get_supplier_scraper(supplier: Supplier):
    pt = (supplier.platform_type or "").lower()
    if pt == "shopify":
        return ShopifyScraper(supplier.url)
    if pt == "footlocker":
        return FootlockerScraper(supplier.url)
    if pt == "custom":
        # Many boutique retailers run on Shopify; ShopifyScraper returns an
        # empty list (with a warning) if /products.json doesn't exist, so
        # trying it here is safe and catches sites that are mis-labelled.
        logger.info(
            f"  [{supplier.name}] platform_type='custom' — "
            "attempting Shopify endpoint as fallback"
        )
        return ShopifyScraper(supplier.url)
    logger.warning(f"  [{supplier.name}] no scraper for platform_type='{pt}' — skipping")
    return None


# ── Main orchestration ────────────────────────────────────────────────────────

def run_full_scrape(
    db: Session,
    job_id: int,
    progress: Optional[ProgressCallback] = None,
    categories: Optional[List[str]] = None,
    min_discount: float = 0,
):
    """
    Full pipeline: scrape all active suppliers → look up market data →
    calculate ROI → persist opportunities.
    Called by the scheduler or the manual "Run Scrape" button.

    Pass categories (e.g. ["tier0_qs"]) and min_discount (e.g. 1) to limit
    the run to a targeted subset — useful for a fast first-pass scan.
    """
    def emit(msg: str):
        logger.info(msg)
        if progress:
            progress(msg)

    job: ScrapeJob = db.get(ScrapeJob, job_id)

    query = db.query(Supplier).filter_by(active=True)
    if categories:
        query = query.filter(Supplier.category.in_(categories))
    if min_discount > 0:
        query = query.filter(Supplier.discount_percent >= min_discount)
    active_suppliers: List[Supplier] = query.all()

    label = ""
    if categories:
        label += f" categories={categories}"
    if min_discount > 0:
        label += f" min_discount={min_discount}%"
    emit(f"Starting scrape — {len(active_suppliers)} active suppliers{label}")

    kicks  = KicksDevClient(settings.kicks_dev_api_key)
    stockx = StockXKicksClient(kicks)
    goat   = GoatScraper(kicks)
    alias: Optional[AliasClient] = None
    if settings.alias_api_key:
        try:
            alias = AliasClient(settings.alias_api_key)
        except ValueError as exc:
            logger.warning(f"Alias API disabled: {exc}")
    else:
        logger.info("ALIAS_API_KEY not set — Alias pricing/sales unavailable")

    total_skus = 0
    total_opps = 0

    try:
        for supplier in active_suppliers:
            scraper = _get_supplier_scraper(supplier)
            if scraper is None:
                emit(f"  [SKIP] {supplier.name} — platform '{supplier.platform_type}' not yet implemented")
                continue

            # For Shopify suppliers, use incremental mode: only fetch products
            # updated since the last time we scraped this supplier.  This turns
            # a full 2000-product crawl into a handful of pages on most runs.
            last_scraped_at = (
                db.query(func.max(SupplierProduct.scraped_at))
                .filter(SupplierProduct.supplier_id == supplier.id)
                .scalar()
            )

            emit(f"  Scraping {supplier.name} ({supplier.url}) …")
            try:
                if isinstance(scraper, ShopifyScraper) and last_scraped_at:
                    products: List[ScrapedProduct] = scraper.scrape(since_dt=last_scraped_at)
                else:
                    products: List[ScrapedProduct] = scraper.scrape()
            except Exception as exc:
                emit(f"  [ERROR] {supplier.name}: {exc}")
                continue

            emit(f"  {supplier.name} → {len(products)} sneaker products found")

            # Mark this supplier's previously-active opportunities as stale so the
            # main page only shows what the current run actually confirms.
            # We only do this when the scrape returned data — a failed/empty run
            # shouldn't wipe valid opportunities from a successful previous run.
            if products:
                db.query(Opportunity).filter_by(supplier_id=supplier.id).update(
                    {"is_active": False}, synchronize_session="fetch"
                )
                db.commit()

            # Process genuinely new products (not yet in our DB) before existing
            # ones — a fresh restock or new drop should surface immediately.
            known_skus = {
                row[0] for row in
                db.query(SupplierProduct.sku)
                .filter(SupplierProduct.supplier_id == supplier.id)
                .all()
            }
            products.sort(key=lambda p: p.sku in known_skus)   # False (new) sorts first

            skus_this_supplier = 0

            for product in products:
                _upsert_supplier_product(db, supplier, product)
                skus_this_supplier += 1

                opps = _process_sku(
                    db, supplier, product, stockx, goat, alias, emit
                )
                total_opps += opps
                rate_limit(0.3, 0.8)   # light delay between SKUs

            total_skus += skus_this_supplier
            job.suppliers_scraped = (job.suppliers_scraped or 0) + 1
            job.skus_found = total_skus
            job.opportunities_found = total_opps
            db.commit()

    finally:
        stockx.close()
        goat.close()
        kicks.close()
        if alias:
            alias.close()

    job.status = "done"
    job.finished_at = datetime.utcnow()
    job.skus_found = total_skus
    job.opportunities_found = total_opps
    db.commit()
    emit(f"Scrape complete — {total_skus} SKUs, {total_opps} opportunities (ROI ≥ {settings.roi_threshold}%)")


# ── Per-SKU processing ────────────────────────────────────────────────────────

def _process_sku(
    db: Session,
    supplier: Supplier,
    product: ScrapedProduct,
    stockx: StockXKicksClient,
    goat: GoatScraper,
    alias: Optional[AliasClient],
    emit: Callable,
) -> int:
    """Look up market data and write opportunities. Returns count of new opportunities."""
    opps_created = 0

    # Try StockX first; only fall back to GOAT if StockX has no data (saves API quota)
    stockx_data: Optional[StockXProduct] = stockx.get_product(product.sku, name=product.name)
    rate_limit(settings.scrape_delay_min, settings.scrape_delay_max)

    if stockx_data and stockx_data.sizes:
        emit(f"    [StockX] {product.sku} → {len(stockx_data.sizes)} size(s) found")
    else:
        emit(f"    [StockX] {product.sku} → no data; trying GOAT …")

    goat_data = None
    if not stockx_data or not stockx_data.sizes:
        goat_data = goat.get_product(product.sku, name=product.name)
        rate_limit(settings.scrape_delay_min, settings.scrape_delay_max)
        if goat_data and goat_data.sizes:
            emit(f"    [GOAT]   {product.sku} → {len(goat_data.sizes)} size(s) found")
        else:
            emit(f"    [GOAT]   {product.sku} → no data; will try Alias …")

    kicks_dev_found = bool(stockx_data or goat_data)

    # Persist kicks.dev market prices
    if stockx_data:
        for sz in stockx_data.sizes:
            _upsert_market_price(db, product.sku, "stockx", sz.size,
                                 sz.lowest_ask, sz.highest_bid, sz.last_sale,
                                 stockx_data.name, stockx_data.stockx_url)

    if goat_data:
        for sz in goat_data.sizes:
            _upsert_market_price(db, product.sku, "goat", sz.size,
                                 sz.lowest_ask, None, sz.last_sale,
                                 goat_data.name, goat_data.goat_url)

    db.commit()

    # Fetch 7-day sales once per product from Alias (product-level demand signal).
    # kicks.dev always returns 0 for both StockX and GOAT, so Alias is authoritative.
    alias_sales_7d = 0
    if alias:
        try:
            alias_sales_7d = alias.get_sales_last_7_days(product.sku)
        except Exception:
            alias_sales_7d = 0
        rate_limit(0.5, 1.0)

    # Check each in-stock size
    for avail_size in product.available_sizes():
        # Prefer StockX; fall back to GOAT
        platform, listing_price, platform_url, shoe_name = _best_listing(
            product.sku, avail_size.size, stockx_data, goat_data
        )

        # If kicks.dev had no data for this shoe, try Alias as pricing source
        if listing_price is None and alias and not kicks_dev_found:
            avail = alias.get_availability(product.sku, size=avail_size.size)
            if avail:
                listing_price = alias.extract_lowest_ask(avail)
                if listing_price:
                    platform = "alias"
                    platform_url = f"https://alias.org/catalog/{product.sku}"
                    shoe_name = product.name
                    last_sale = alias.extract_last_sale(avail)
                    _upsert_market_price(
                        db, product.sku, "alias", avail_size.size,
                        listing_price, None, last_sale,
                        shoe_name, platform_url,
                    )
                    logger.info(
                        f"    [Alias]  {product.sku} Sz {avail_size.size} "
                        f"→ ask ${listing_price:.2f}"
                    )
            rate_limit(0.5, 1.0)

        if listing_price is None:
            continue

        sales_7d = alias_sales_7d

        # ROI calculation
        cost = _discounted_price(avail_size.price or product.original_price, supplier)
        payout = listing_price * (1.0 - settings.commission_rate)
        if cost <= 0:
            continue
        roi = (payout - cost) / cost * 100.0

        if roi < settings.roi_threshold:
            continue

        # Write opportunity
        _upsert_opportunity(
            db=db,
            sku=product.sku,
            shoe_name=shoe_name or product.name,
            size=avail_size.size,
            supplier=supplier,
            original_price=float(avail_size.price or product.original_price),
            discounted_price=cost,
            platform=platform,
            listing_price=listing_price,
            payout_price=payout,
            roi=roi,
            sales_7d=sales_7d,
            supplier_url=product.url,
            market_url=platform_url,
        )
        opps_created += 1
        emit(
            f"    ✓ {product.name} | {product.sku} | Sz {avail_size.size} | "
            f"${cost:.2f} → ${payout:.2f} payout | ROI {roi:.1f}% | "
            f"{sales_7d} sales/7d [{platform.upper()}]"
        )

    db.commit()
    return opps_created


# ── Helpers ───────────────────────────────────────────────────────────────────

def _discounted_price(original: float, supplier: Supplier) -> float:
    pct = float(supplier.discount_percent or 0)
    return original * (1.0 - pct / 100.0)


def _best_listing(sku, size, stockx_data, goat_data):
    """Return (platform, listing_price, url, name) for the best available market."""
    # StockX
    if stockx_data:
        for sz in stockx_data.sizes:
            if _size_matches(sz.size, size) and sz.lowest_ask:
                return "stockx", float(sz.lowest_ask), stockx_data.stockx_url, stockx_data.name

    # GOAT
    if goat_data:
        for sz in goat_data.sizes:
            if _size_matches(sz.size, size) and sz.lowest_ask:
                return "goat", float(sz.lowest_ask), goat_data.goat_url, goat_data.name

    return None, None, None, None


def _size_matches(a: str, b: str) -> bool:
    try:
        return abs(float(a) - float(b)) < 0.01
    except (ValueError, TypeError):
        return str(a).strip() == str(b).strip()


# ── DB upsert helpers ─────────────────────────────────────────────────────────

def _upsert_supplier_product(db: Session, supplier: Supplier, product: ScrapedProduct):
    existing = (
        db.query(SupplierProduct)
        .filter_by(supplier_id=supplier.id, sku=product.sku)
        .first()
    )
    now = datetime.utcnow()
    if existing:
        existing.name           = product.name
        existing.original_price = product.original_price
        existing.product_url    = product.url
        existing.scraped_at     = now
        if product.published_at:
            existing.published_at = product.published_at
        # first_seen_at is immutable — never overwritten
        sp = existing
    else:
        sp = SupplierProduct(
            supplier_id    = supplier.id,
            sku            = product.sku,
            name           = product.name,
            original_price = product.original_price,
            product_url    = product.url,
            first_seen_at  = now,
            published_at   = product.published_at,
        )
        db.add(sp)
        db.flush()

    # Refresh sizes — deduplicate by size value (prefer in_stock=True on collision)
    seen: dict[str, bool] = {}
    for sz in product.sizes:
        if sz.size not in seen or sz.in_stock:
            seen[sz.size] = sz.in_stock
    # Flush first so any pending SupplierProductSize rows (from a previous call
    # for the same supplier_product_id) become persistent and are cleared by
    # the DELETE — synchronize_session='evaluate' only handles persistent objects.
    db.flush()
    db.query(SupplierProductSize).filter_by(supplier_product_id=sp.id).delete()
    for size_val, in_stock in seen.items():
        db.add(SupplierProductSize(
            supplier_product_id=sp.id,
            size=size_val,
            in_stock=in_stock,
        ))


def _upsert_market_price(db: Session, sku, platform, size,
                         lowest_ask, highest_bid, last_sale,
                         shoe_name, platform_url):
    existing = (
        db.query(MarketPrice)
        .filter_by(sku=sku, platform=platform, size=size)
        .first()
    )
    if existing:
        existing.lowest_ask   = lowest_ask
        existing.highest_bid  = highest_bid
        existing.last_sale    = last_sale
        existing.shoe_name    = shoe_name
        existing.platform_url = platform_url
        existing.fetched_at   = datetime.utcnow()
    else:
        db.add(MarketPrice(
            sku=sku, platform=platform, size=size,
            lowest_ask=lowest_ask, highest_bid=highest_bid,
            last_sale=last_sale, shoe_name=shoe_name,
            platform_url=platform_url,
        ))


def _upsert_opportunity(db: Session, sku, shoe_name, size, supplier: Supplier,
                        original_price, discounted_price, platform,
                        listing_price, payout_price, roi, sales_7d,
                        supplier_url, market_url):
    existing = (
        db.query(Opportunity)
        .filter_by(sku=sku, size=size, supplier_id=supplier.id, listing_platform=platform)
        .first()
    )
    now = datetime.utcnow()
    discount_label = (
        f"{supplier.discount_percent}% off"
        if supplier.discount_percent
        else "No discount"
    )
    if existing:
        existing.shoe_name        = shoe_name
        existing.original_price   = original_price
        existing.discounted_price = discounted_price
        existing.discount_applied = discount_label
        existing.listing_price    = listing_price
        existing.payout_price     = payout_price
        existing.roi              = roi
        existing.sales_last_7_days= sales_7d
        existing.supplier_url     = supplier_url
        existing.market_url       = market_url
        existing.is_active        = True
        existing.updated_at       = now
    else:
        db.add(Opportunity(
            sku              = sku,
            shoe_name        = shoe_name,
            size             = size,
            supplier_id      = supplier.id,
            supplier_name    = supplier.name,
            original_price   = original_price,
            discounted_price = discounted_price,
            discount_applied = discount_label,
            listing_platform = platform,
            listing_price    = listing_price,
            payout_price     = payout_price,
            roi              = roi,
            sales_last_7_days= sales_7d,
            supplier_url     = supplier_url,
            market_url       = market_url,
            found_at         = now,
            updated_at       = now,
        ))
