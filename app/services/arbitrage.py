"""
Core arbitrage engine.

For each active supplier:
  1. Scrape shoes using the appropriate scraper (Shopify / Footlocker) —
     supplier sites are fetched concurrently (Phase A).
  2. For each shoe SKU found, decide via the per-SKU cache gate
     (app/services/sku_cache.py) whether a StockX market lookup is warranted —
     StockX's shared daily budget is the real constraint now, not wall-clock.
  3. Fetch StockX market data — official API (app/scrapers/stockx_api.py) when
     credentials are configured, self-hosted browser fallback otherwise; GOAT /
     Alias as secondary sources when StockX has nothing.
  4. Classify per available size (app/services/pricing.py):
       effective_price = (original × (1 − discount)) × (1 − cashback)
       margin          = resale_price − seller_fee_estimate − effective_price
     and persist opportunities with margin > settings.min_margin_threshold.
  5. Record sales history for 7-day volume tracking.

Every product write is isolated — one bad row logs and continues, it never
kills the batch. The job row itself is guaranteed a terminal status by
app/services/scheduler.py.
"""
import logging
import time
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Callable, Tuple

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from playwright.sync_api import sync_playwright

from app.config import settings
from app.database import (
    Supplier, SupplierProduct, SupplierProductSize,
    MarketPrice, SaleRecord, Opportunity, ScrapeJob, ScrapeSupplierResult,
    StockXMatchFailure,
)
from app.scrapers.base import ScrapedProduct, rate_limit
from app.scrapers.shopify import ShopifyScraper
from app.scrapers.footlocker import FootlockerScraper
from app.scrapers.browser import BrowserSession, ThreadBoundProxy
from app.scrapers.stockx_market import StockXBrowserClient, StockXProduct, StockXSizeMarket
from app.scrapers import stockx_api
from app.scrapers.stockx_api import (
    StockXAPIClient, StockXLookupResult, StockXBudgetExhausted, DAILY_REQUEST_LIMIT,
)
from app.scrapers.goat_market import GoatBrowserClient
from app.scrapers.alias import AliasClient
from app.services.pricing import classify_opportunity
from app.services.effective_price import compute_effective_price
from app.services import sku_cache
from app.services.sku_cache import GateDecision

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], None]   # called with status messages


def _make_browser_client(platform: str, client_cls, use_stealth: bool = True):
    """Build a client whose entire lifecycle (Playwright start, browser
    launch, every method call, teardown) runs on one dedicated thread — see
    BrowserSession's module docstring: Playwright's sync API and
    ShopifyScraper's asyncio.run() calls can't share a thread, and this loop
    calls both across the same supplier list. Returns (proxy, teardown_fn).
    """
    proxy = ThreadBoundProxy()
    state: dict = {}

    def factory():
        pw = sync_playwright().start()
        proxy_url = settings.stockx_proxy_url if platform == "stockx" else settings.goat_proxy_url
        session = BrowserSession(
            platform, headless=settings.browser_headless,
            proxy_url=proxy_url,
            state_dir=settings.browser_state_dir,
            nav_timeout_ms=settings.browser_nav_timeout_ms,
            use_stealth=use_stealth,
            playwright=pw,
        )
        session.start()
        state["playwright"] = pw
        state["session"] = session
        return client_cls(session)

    proxy.setup(factory)

    def teardown():
        # Playwright objects are bound (via a greenlet fiber) to the thread
        # that created them — must close/stop on that same worker thread,
        # not whatever thread calls teardown(), or it raises a greenlet
        # cross-thread error.
        proxy.teardown(lambda: (state["session"].close(), state["playwright"].stop()))

    return proxy, teardown


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


@dataclass
class _ScrapeResult:
    products: Optional[List[ScrapedProduct]]
    error: Optional[Exception]
    elapsed: float


def _scrape_supplier_worker(scraper, since_dt: Optional[datetime]) -> _ScrapeResult:
    """Runs on a ThreadPoolExecutor worker thread — takes the already-built
    scraper, never the Supplier ORM row or db session (neither is thread-safe).
    Never raises: any exception from scraper.scrape() is captured here so one
    bad supplier can't fail the whole pool or block collection of the rest.
    """
    start = time.perf_counter()
    try:
        if isinstance(scraper, ShopifyScraper) and since_dt:
            products = scraper.scrape(since_dt=since_dt)
        else:
            products = scraper.scrape()
        return _ScrapeResult(products=products, error=None, elapsed=time.perf_counter() - start)
    except Exception as exc:
        return _ScrapeResult(products=None, error=exc, elapsed=time.perf_counter() - start)


@dataclass
class _StockXCounters:
    """Calls made vs skipped are the run's real constraint now (shared daily
    API budget) — tracked here and surfaced on the job row + dashboard."""
    made: int = 0
    skipped: int = 0
    budget_exhausted: bool = False
    matched: int = 0
    unmatched: int = 0


# ── Main orchestration ────────────────────────────────────────────────────────

def run_full_scrape(
    db: Session,
    job_id: int,
    progress: Optional[ProgressCallback] = None,
    categories: Optional[List[str]] = None,
    min_discount: float = 0,
    supplier_names: Optional[List[str]] = None,
    limit_products: Optional[int] = None,
):
    """
    Full pipeline: scrape all active suppliers → look up market data →
    calculate margin → persist opportunities.
    Called by the scheduler or the manual "Run Scrape" button.

    Pass categories (e.g. ["tier0_qs"]) and min_discount (e.g. 1) to limit
    the run to a targeted subset — useful for a fast first-pass scan.
    supplier_names / limit_products narrow further (specific retailers, cap
    on products evaluated per retailer) — used for targeted test runs.
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
    if supplier_names:
        query = query.filter(Supplier.name.in_(supplier_names))
    active_suppliers: List[Supplier] = query.all()

    label = ""
    if categories:
        label += f" categories={categories}"
    if min_discount > 0:
        label += f" min_discount={min_discount}%"
    emit(f"Starting scrape — {len(active_suppliers)} active suppliers{label}")

    # StockX source: official API when credentials are configured (fast,
    # per-size, rate-limited by the shared daily budget), stealth-browser
    # fallback otherwise. GOAT always runs through its browser client.
    use_api = stockx_api.is_configured()
    api_client: Optional[StockXAPIClient] = None
    stockx = None
    _stockx_teardown = lambda: None   # noqa: E731
    if use_api:
        api_client = StockXAPIClient()
        emit("StockX source: official API (developer.stockx.com)")
    else:
        emit("StockX source: browser scraper (set STOCKX_CLIENT_ID/SECRET/API_KEY "
             "+ run scripts/stockx_auth.py to switch to the official API)")
        stockx, _stockx_teardown = _make_browser_client("stockx", StockXBrowserClient)

    goat, _goat_teardown = _make_browser_client("goat", GoatBrowserClient, use_stealth=False)
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
    counters = _StockXCounters()

    try:
        # ── Phase A: scrape all supplier sites concurrently ──────────────────
        # Each site fetch is I/O-bound and independent of the others, so we
        # dispatch them to a thread pool instead of scraping one-at-a-time —
        # the whole batch then takes roughly as long as the slowest single
        # site, not the sum of all of them. DB/ORM access (`db`, `supplier`)
        # stays on this thread only; workers get a plain scraper + since_dt.
        scrape_jobs = []   # (supplier, scraper, since_dt), in active_suppliers order
        for supplier in active_suppliers:
            scraper = _get_supplier_scraper(supplier)
            if scraper is None:
                emit(f"  [SKIP] {supplier.name} — platform '{supplier.platform_type}' not yet implemented")
                db.add(ScrapeSupplierResult(
                    job_id=job_id, supplier_name=supplier.name, status="skipped",
                    error_message=f"no scraper for platform_type='{supplier.platform_type}'",
                ))
                continue

            # For Shopify suppliers, use incremental mode: only fetch products
            # updated since the last time we scraped this supplier.  This turns
            # a full 2000-product crawl into a handful of pages on most runs.
            last_scraped_at = (
                db.query(func.max(SupplierProduct.scraped_at))
                .filter(SupplierProduct.supplier_id == supplier.id)
                .scalar()
            )
            scrape_jobs.append((supplier, scraper, last_scraped_at))

        scrape_results: dict = {}   # supplier.id -> _ScrapeResult
        if scrape_jobs:
            max_workers = max(1, min(settings.scrape_concurrency, len(scrape_jobs)))
            phase_start = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="site-scraper"
            ) as pool:
                future_to_supplier_id = {}
                for supplier, scraper, since_dt in scrape_jobs:
                    emit(f"  Scraping {supplier.name} ({supplier.url}) …")
                    future = pool.submit(_scrape_supplier_worker, scraper, since_dt)
                    future_to_supplier_id[future] = supplier.id
                for future in concurrent.futures.as_completed(future_to_supplier_id):
                    scrape_results[future_to_supplier_id[future]] = future.result()
            emit(
                f"Scraped {len(scrape_jobs)} supplier sites "
                f"(concurrency={max_workers}) in {time.perf_counter() - phase_start:.1f}s"
            )

        # ── Phase B: DB writes + market lookups ──────────────────────────────
        for supplier, _scraper, _since_dt in scrape_jobs:
            result = scrape_results[supplier.id]
            if result.error is not None:
                emit(f"  [ERROR] {supplier.name}: {result.error}")
                db.add(ScrapeSupplierResult(
                    job_id=job_id, supplier_name=supplier.name, status="failed",
                    error_message=str(result.error) or repr(result.error),
                    elapsed_seconds=result.elapsed,
                ))
                db.commit()
                continue
            products: List[ScrapedProduct] = result.products

            emit(f"  {supplier.name} → {len(products)} sneaker products found")
            db.add(ScrapeSupplierResult(
                job_id=job_id, supplier_name=supplier.name, status="succeeded",
                items_found=len(products), elapsed_seconds=result.elapsed,
            ))

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
            if limit_products is not None:
                products = products[:limit_products]

            # ── B1: persist supplier products — each isolated so one bad row
            # logs and continues instead of killing the whole batch ──────────
            skus_this_supplier = 0
            for product in products:
                try:
                    _upsert_supplier_product(db, supplier, product)
                    db.commit()
                    skus_this_supplier += 1
                except Exception as exc:
                    db.rollback()
                    logger.exception(f"  [{supplier.name}] failed to persist {product.sku}")
                    emit(f"  [WARN] {supplier.name}: skipping bad row {product.sku}: {exc!r}")

            # ── B2: gate StockX lookups per SKU (rate-limit protection) ──────
            # One decision per SKU using the lowest effective price among the
            # duplicates a scrape can produce for the same base SKU.
            gate_price: Dict[str, float] = {}
            for product in products:
                eff = _gate_effective_price(supplier, product)
                if product.sku not in gate_price or eff < gate_price[product.sku]:
                    gate_price[product.sku] = eff
            decisions: Dict[str, GateDecision] = {
                sku: sku_cache.gate_stockx_check(db, sku, price)
                for sku, price in gate_price.items()
            }

            # ── B3: prefetch StockX API lookups on their own small pool —
            # sized for the API rate limit (settings.stockx_lookup_concurrency),
            # deliberately NOT the retailer-scrape concurrency. Workers do
            # network only; every DB write stays on this thread. ─────────────
            prefetched: Dict[str, StockXLookupResult] = {}
            to_check = [sku for sku, d in decisions.items() if d.check]
            if use_api and to_check and not counters.budget_exhausted:
                sku_names = {}
                for product in products:
                    sku_names.setdefault(product.sku, product.name)
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max(1, settings.stockx_lookup_concurrency),
                    thread_name_prefix="stockx-api",
                ) as pool:
                    futs = {pool.submit(api_client.get_market, sku, sku_names.get(sku, "")): sku
                            for sku in to_check}
                    for fut in concurrent.futures.as_completed(futs):
                        sku = futs[fut]
                        try:
                            prefetched[sku] = fut.result()
                        except StockXBudgetExhausted as exc:
                            counters.budget_exhausted = True
                            emit(f"  [StockX] {exc} — remaining lookups this run fall through unchecked")
                            for f in futs:
                                f.cancel()
                            break
                        except Exception as exc:
                            logger.warning(f"StockX lookup failed for {sku}: {exc!r}")

            # ── B4: evaluate each product — again isolated per row ───────────
            seen_gate_skus: set = set()
            for product in products:
                try:
                    opps = _evaluate_product(
                        db, supplier, product,
                        decision=decisions[product.sku],
                        gate_eff_price=gate_price[product.sku],
                        use_api=use_api,
                        prefetched=prefetched,
                        stockx_browser=stockx,
                        goat=goat, alias=alias,
                        counters=counters,
                        count_gate=product.sku not in seen_gate_skus,
                        emit=emit,
                    )
                    seen_gate_skus.add(product.sku)
                    db.commit()
                    total_opps += opps
                except Exception as exc:
                    db.rollback()
                    logger.exception(f"  [{supplier.name}] failed to evaluate {product.sku}")
                    emit(f"  [WARN] {supplier.name}: evaluation failed for {product.sku}: {exc!r}")
                if not use_api:
                    rate_limit(0.3, 0.8)   # politeness delay only matters for the browser path

            total_skus += skus_this_supplier
            job.suppliers_scraped = (job.suppliers_scraped or 0) + 1
            job.skus_found = total_skus
            job.opportunities_found = total_opps
            job.stockx_calls_made = counters.made
            job.stockx_calls_skipped = counters.skipped
            db.commit()

    finally:
        _stockx_teardown()   # persists cookies/storage state even on error
        _goat_teardown()
        if api_client:
            api_client.close()
        if alias:
            alias.close()

    job.status = "done"
    job.finished_at = datetime.utcnow()
    job.skus_found = total_skus
    job.opportunities_found = total_opps
    job.stockx_calls_made = counters.made
    job.stockx_calls_skipped = counters.skipped
    db.commit()
    emit(f"Scrape complete — {total_skus} SKUs, {total_opps} opportunities "
         f"(margin > ${settings.min_margin_threshold:.0f})")
    # The StockX budget is the run's real constraint — always surface it.
    emit(f"StockX market lookups: {counters.made} made, {counters.skipped} skipped by cache"
         + (f", match rate {counters.matched}/{counters.matched + counters.unmatched}"
            if use_api and (counters.matched + counters.unmatched) else ""))
    if use_api and api_client:
        emit(f"StockX API budget: {api_client.calls_today()}/{DAILY_REQUEST_LIMIT} "
             f"requests used today (shared app-wide)")


# ── Per-product evaluation ────────────────────────────────────────────────────

def _gate_effective_price(supplier: Supplier, product: ScrapedProduct) -> float:
    """Effective price used for cache gating: the cheapest in-stock size
    (or the product's base price) after discount + cashback."""
    prices = [s.price for s in product.available_sizes() if s.price]
    base = min(prices) if prices else float(product.original_price or 0)
    return compute_effective_price(
        list_price=float(base or 0),
        discount_percent=float(supplier.discount_percent or 0),
        discount_amount=float(supplier.discount_amount or 0),
        supplier_name=supplier.name,
    ).effective_price


def _evaluate_product(
    db: Session,
    supplier: Supplier,
    product: ScrapedProduct,
    decision: GateDecision,
    gate_eff_price: float,
    use_api: bool,
    prefetched: Dict[str, StockXLookupResult],
    stockx_browser,
    goat: GoatBrowserClient,
    alias: Optional[AliasClient],
    counters: _StockXCounters,
    count_gate: bool,
    emit: Callable,
) -> int:
    """Resolve StockX data per the gate decision, then classify sizes.
    Returns count of opportunities created."""
    now = datetime.utcnow()
    stockx_data: Optional[StockXProduct] = None
    market_fetched_at: Optional[datetime] = None
    fresh = False

    if decision.check:
        if use_api:
            lookup = prefetched.get(product.sku)
            if lookup is None:
                # Never actually fetched (budget exhausted mid-pool, or the
                # worker errored) — leave the gate state untouched so the SKU
                # is re-gated next run, and don't burn slow browser fallbacks.
                sku_cache.record_seen(db, product.sku)
                return 0
            stockx_data = lookup.product
            if count_gate:
                counters.made += 1
                if stockx_data is not None:
                    counters.matched += 1
                else:
                    counters.unmatched += 1
                    _record_match_failure(db, product, lookup.failure_reason or "unknown")
        else:
            if count_gate:
                counters.made += 1
            stockx_data = stockx_browser.get_product(product.sku, name=product.name)
            rate_limit(settings.scrape_delay_min, settings.scrape_delay_max)
        fresh = True
        market_fetched_at = now

        if stockx_data and stockx_data.sizes:
            emit(f"    [StockX] {product.sku} → {len(stockx_data.sizes)} size(s) found "
                 f"({decision.reason})")
        else:
            emit(f"    [StockX] {product.sku} → no data ({decision.reason}); trying GOAT …")

    elif decision.evaluate_from_cache:
        # Cached-profitable: re-evaluate against cached market rows, zero calls.
        if count_gate:
            counters.skipped += 1
        sku_cache.record_seen(db, product.sku)
        stockx_data, market_fetched_at = _stockx_from_cache(db, product.sku)

    else:
        # Cached not-profitable at an equal-or-worse price: just prove life.
        if count_gate:
            counters.skipped += 1
        sku_cache.record_seen(db, product.sku)
        return 0

    # ── GOAT / Alias fallback — only on a fresh miss (a cached evaluation
    # shouldn't trigger slow browser lookups) ────────────────────────────────
    goat_data = None
    if fresh and (not stockx_data or not stockx_data.sizes):
        goat_data = goat.get_product(product.sku, name=product.name)
        rate_limit(settings.scrape_delay_min, settings.scrape_delay_max)
        if goat_data and goat_data.sizes:
            emit(f"    [GOAT]   {product.sku} → {len(goat_data.sizes)} size(s) found")
        else:
            emit(f"    [GOAT]   {product.sku} → no data; will try Alias …")

    stockx_or_goat_found = bool(stockx_data or goat_data)

    # Persist fresh market prices
    if fresh and stockx_data:
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
    # StockX's sales-count source is unconfirmed and GOAT's public site was
    # confirmed to expose none at all, so Alias is the only possible source —
    # see app/services/pricing.py's docstring for how None is handled.
    alias_sales_7d: Optional[int] = None
    if alias and fresh:
        try:
            recent_sales = alias.get_recent_sales(product.sku)
            if recent_sales is not None:
                alias_sales_7d = len(recent_sales)
                _upsert_sale_records(db, product.sku, "alias", recent_sales)
        except Exception:
            alias_sales_7d = None
        rate_limit(0.5, 1.0)

    # ── Classify each in-stock size ──────────────────────────────────────────
    opps_created = 0
    best_resale: Optional[float] = None
    best_resale_type: Optional[str] = None

    for avail_size in product.available_sizes():
        platform, resale_price, resale_type, platform_url, shoe_name = _pick_resale(
            avail_size.size, stockx_data, goat_data
        )

        # If StockX/GOAT had no data for this shoe, try Alias as pricing source
        if resale_price is None and alias and fresh and not stockx_or_goat_found:
            avail = alias.get_availability(product.sku, size=avail_size.size)
            if avail:
                ask = alias.extract_lowest_ask(avail)
                if ask:
                    platform, resale_price, resale_type = "alias", ask, "lowest_ask"
                    platform_url = f"https://alias.org/catalog/{product.sku}"
                    shoe_name = product.name
                    last_sale = alias.extract_last_sale(avail)
                    _upsert_market_price(
                        db, product.sku, "alias", avail_size.size,
                        ask, None, last_sale, shoe_name, platform_url,
                    )
                    logger.info(f"    [Alias]  {product.sku} Sz {avail_size.size} → ask ${ask:.2f}")
            rate_limit(0.5, 1.0)

        if resale_price is None:
            continue
        if best_resale is None or resale_price < best_resale:
            best_resale, best_resale_type = resale_price, resale_type

        sales_7d = alias_sales_7d

        result = classify_opportunity(
            original_price=float(avail_size.price or product.original_price),
            discount_percent=float(supplier.discount_percent or 0),
            discount_amount=float(supplier.discount_amount or 0),
            supplier_name=supplier.name,
            listing_price=resale_price,
            platform=platform,
            resale_price_type=resale_type,
            sales_last_7_days=sales_7d,
        )
        if not result.is_opportunity:
            continue

        _upsert_opportunity(
            db=db,
            sku=product.sku,
            shoe_name=shoe_name or product.name,
            size=avail_size.size,
            supplier=supplier,
            original_price=float(avail_size.price or product.original_price),
            discounted_price=result.price_after_discount,
            cashback_rate=result.cashback_rate,
            cashback_portal=result.cashback_portal,
            cashback_amount=result.cashback_amount,
            effective_price=result.cost,
            platform=platform,
            listing_price=resale_price,
            resale_price_type=resale_type,
            payout_price=result.payout,
            margin=result.margin,
            roi=result.roi,
            market_fetched_at=market_fetched_at,
            sales_7d=sales_7d,
            supplier_url=product.url,
            market_url=platform_url,
        )
        opps_created += 1
        sales_label = f"{sales_7d} sales/7d" if sales_7d is not None else "sales/7d unknown"
        freshness = "fresh" if fresh else "cached"
        emit(
            f"    ✓ {product.name} | {product.sku} | Sz {avail_size.size} | "
            f"${result.cost:.2f} → ${result.payout:.2f} payout | "
            f"margin ${result.margin:.2f} ({result.roi:.1f}%) | "
            f"{sales_label} [{platform.upper()}/{resale_type}/{freshness}]"
        )

    # ── Update the gate cache after a fresh StockX evaluation ────────────────
    if fresh:
        if stockx_data is None or not stockx_data.sizes:
            verdict = sku_cache.VERDICT_NO_MARKET_DATA
        elif opps_created > 0:
            verdict = sku_cache.VERDICT_PROFITABLE
        else:
            verdict = sku_cache.VERDICT_NOT_PROFITABLE
        sku_cache.record_check(
            db, product.sku, gate_eff_price, verdict,
            resale_price=best_resale, resale_price_type=best_resale_type,
        )

    db.commit()
    return opps_created


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pick_resale(size: str, stockx_data: Optional[StockXProduct], goat_data) \
        -> Tuple[Optional[str], Optional[float], Optional[str], Optional[str], Optional[str]]:
    """(platform, resale_price, resale_price_type, url, name) for a size.

    resale_price defaults to the LOWEST ASK (a live listing you can undercut/
    match — conservative for 'can I realize this today'), falling back to
    LAST SALE (a cleared transaction — real, but possibly stale) only when no
    ask exists. The type is always labeled since they mean different things;
    tradeoff discussed in README §Resale price.

    A size entry of "ANY" is aggregate product-level data (browser scraper
    fallback) and matches every requested size.
    """
    for data, platform, url, name in (
        (stockx_data, "stockx", getattr(stockx_data, "stockx_url", None), getattr(stockx_data, "name", None)),
        (goat_data, "goat", getattr(goat_data, "goat_url", None), getattr(goat_data, "name", None)),
    ):
        if not data:
            continue
        for sz in data.sizes:
            if _size_matches(sz.size, size) or sz.size == "ANY":
                if sz.lowest_ask:
                    return platform, float(sz.lowest_ask), "lowest_ask", url, name
                if sz.last_sale:
                    return platform, float(sz.last_sale), "last_sale", url, name
    return None, None, None, None, None


def _size_matches(a: str, b: str) -> bool:
    try:
        return abs(float(a) - float(b)) < 0.01
    except (ValueError, TypeError):
        return str(a).strip() == str(b).strip()


def _stockx_from_cache(db: Session, sku: str) -> Tuple[Optional[StockXProduct], Optional[datetime]]:
    """Rebuild a StockXProduct from cached market_prices rows (no API call).
    Returns (product, fetched_at) — fetched_at powers the fresh-vs-cached
    indicator in the dashboard."""
    rows = db.query(MarketPrice).filter_by(sku=sku, platform="stockx").all()
    if not rows:
        return None, None
    sizes = [
        StockXSizeMarket(
            size=r.size,
            lowest_ask=float(r.lowest_ask) if r.lowest_ask is not None else None,
            highest_bid=float(r.highest_bid) if r.highest_bid is not None else None,
            last_sale=float(r.last_sale) if r.last_sale is not None else None,
        )
        for r in rows
    ]
    fetched = max((r.fetched_at for r in rows if r.fetched_at), default=None)
    return StockXProduct(
        sku=sku, name=rows[0].shoe_name or "", url_key="",
        stockx_url=rows[0].platform_url or "", sizes=sizes,
    ), fetched


def _record_match_failure(db: Session, product: ScrapedProduct, reason: str):
    """Unmatched products are logged, never silently dropped — match-rate
    quality is queryable via the stockx_match_failures table."""
    now = datetime.utcnow()
    row = db.query(StockXMatchFailure).filter_by(sku=product.sku).first()
    if row:
        row.attempts = (row.attempts or 0) + 1
        row.last_failed_at = now
        row.reason = reason
        row.name = product.name
    else:
        db.add(StockXMatchFailure(
            sku=product.sku, name=product.name, reason=reason,
            attempts=1, first_failed_at=now, last_failed_at=now,
        ))
    logger.info(f"    [StockX] unmatched: {product.sku} ({reason})")


# ── DB upsert helpers ─────────────────────────────────────────────────────────

def _upsert_supplier_product(db: Session, supplier: Supplier, product: ScrapedProduct):
    """INSERT ... ON CONFLICT (supplier_id, sku) DO UPDATE — atomic at the DB
    level, so a concurrent writer or an in-batch duplicate SKU can no longer
    raise UniqueViolation and kill the run (the old read-then-insert did)."""
    now = datetime.utcnow()
    update_cols = {
        "name": product.name,
        "original_price": product.original_price,
        "product_url": product.url,
        "scraped_at": now,
        # first_seen_at intentionally absent — immutable, set once on insert
    }
    if product.published_at:
        update_cols["published_at"] = product.published_at

    stmt = (
        pg_insert(SupplierProduct)
        .values(
            supplier_id=supplier.id,
            sku=product.sku,
            name=product.name,
            original_price=product.original_price,
            product_url=product.url,
            scraped_at=now,
            first_seen_at=now,
            published_at=product.published_at,
        )
        .on_conflict_do_update(constraint="uq_supplier_sku", set_=update_cols)
        .returning(SupplierProduct.id)
    )
    sp_id = db.execute(stmt).scalar_one()

    # Refresh sizes — deduplicate by size value (prefer in_stock=True on collision)
    seen: dict[str, bool] = {}
    for sz in product.sizes:
        if sz.size not in seen or sz.in_stock:
            seen[sz.size] = sz.in_stock
    db.query(SupplierProductSize).filter_by(supplier_product_id=sp_id).delete(
        synchronize_session=False
    )
    for size_val, in_stock in seen.items():
        db.add(SupplierProductSize(
            supplier_product_id=sp_id,
            size=size_val,
            in_stock=in_stock,
        ))


def _upsert_sale_records(db: Session, sku: str, platform: str, sales: List[dict]):
    """Persist individual sale events (skips any without a parseable date —
    sale_date is NOT NULL on the table; those still count toward the
    aggregate 7-day number via len(), just aren't itemized)."""
    for sale in sales:
        sale_date = sale.get("sale_date")
        if sale_date is None:
            continue
        size = sale.get("size") or "ANY"
        exists = (
            db.query(SaleRecord)
            .filter_by(sku=sku, platform=platform, size=size, sale_date=sale_date)
            .first()
        )
        if not exists:
            db.add(SaleRecord(
                sku=sku, platform=platform, size=size,
                sale_price=sale.get("price"), sale_date=sale_date,
            ))
    db.commit()


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
                        original_price, discounted_price, cashback_rate,
                        cashback_portal, cashback_amount, effective_price,
                        platform, listing_price, resale_price_type,
                        payout_price, margin, roi, market_fetched_at,
                        sales_7d, supplier_url, market_url):
    existing = (
        db.query(Opportunity)
        .filter_by(sku=sku, size=size, supplier_id=supplier.id, listing_platform=platform)
        .first()
    )
    now = datetime.utcnow()
    if supplier.discount_percent:
        discount_label = f"{supplier.discount_percent}% off"
    elif supplier.discount_amount:
        discount_label = f"${supplier.discount_amount} off"
    else:
        discount_label = "No discount"
    if existing:
        existing.shoe_name        = shoe_name
        existing.original_price   = original_price
        existing.discounted_price = discounted_price
        existing.discount_applied = discount_label
        existing.cashback_rate    = cashback_rate
        existing.cashback_portal  = cashback_portal
        existing.cashback_amount  = cashback_amount
        existing.effective_price  = effective_price
        existing.listing_price    = listing_price
        existing.resale_price_type= resale_price_type
        existing.payout_price     = payout_price
        existing.margin           = margin
        existing.roi              = roi
        existing.market_fetched_at= market_fetched_at
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
            cashback_rate    = cashback_rate,
            cashback_portal  = cashback_portal,
            cashback_amount  = cashback_amount,
            effective_price  = effective_price,
            listing_platform = platform,
            listing_price    = listing_price,
            resale_price_type= resale_price_type,
            payout_price     = payout_price,
            margin           = margin,
            roi              = roi,
            market_fetched_at= market_fetched_at,
            sales_last_7_days= sales_7d,
            supplier_url     = supplier_url,
            market_url       = market_url,
            found_at         = now,
            updated_at       = now,
        ))
