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
     and persist opportunities with margin > settings.min_margin_threshold
     that ALSO pass the sales-liquidity gate (app/services/liquidity.py —
     ≥1 sale/7d or ≥5 sales/30d; known-illiquid products are excluded).
  5. Record sales history (30-day window) powering the liquidity gate.

Every product write is isolated — one bad row logs and continues, it never
kills the batch. The job row itself is guaranteed a terminal status by
app/services/scheduler.py.
"""
import logging
import time
import concurrent.futures
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Callable, Tuple

from app import runtime

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from playwright.sync_api import sync_playwright

from app.config import settings
from app.database import (
    Supplier, SupplierProduct, SupplierProductSize,
    MarketPrice, SaleRecord, Opportunity, ScrapeJob, ScrapeSupplierResult,
    StockXMatchFailure, RetailerProductDiagnostic,
)
from app.scrapers.base import (
    ScrapedProduct, CartValidationResult, rate_limit,
    CONFIDENCE_VERIFIED_CART, CONFIDENCE_OUT_OF_STOCK,
)
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
from app.scrapers import ebay as ebay_api
from app.scrapers.ebay import EbayClient
from app.services.pricing import (
    EbayReference, classify_opportunity, compute_ebay_reference,
    recommend_platform,
)
from app.services.effective_price import compute_effective_price
from app.services import liquidity as liquidity_svc
from app.services.liquidity import LiquiditySnapshot
from app.services import sku_cache
from app.services.sku_cache import GateDecision

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], None]   # called with status messages

# Products per prefetch/evaluate batch. Small enough that a crash or shutdown
# loses at most one chunk of already-paid-for StockX lookups (the old design
# prefetched an entire supplier before evaluating anything — job #41 burned
# ~2k API calls and persisted nothing), large enough to keep the lookup pool
# busy between DB write bursts.
LOOKUP_CHUNK_SIZE = 20


class ScrapeInterrupted(RuntimeError):
    """Raised at a safe boundary when app shutdown is requested mid-run —
    everything committed so far stays committed; the job closes as error
    with this message instead of hanging the interpreter."""


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
    # sku -> failure reason, for lookups that FAILED (HTTP/transport errors
    # after retries) as opposed to cleanly unmatched. Never aborts the run;
    # summarized at the end and retried next run (gate state left untouched).
    lookup_failures: Dict[str, str] = field(default_factory=dict)


@dataclass
class _SupplierRunStats:
    """Per-retailer stage counters for one run — feeds the end-of-run summary
    table and the extended scrape_supplier_results columns."""
    discovered: int = 0
    parsed: int = 0
    inventory_ok: int = 0
    cart_attempts: int = 0
    cart_verified: int = 0
    opportunities: int = 0
    sql_inserts: int = 0
    failures: int = 0
    http_retries: int = 0
    elapsed: float = 0.0
    cart_ms_total: int = 0   # for avg cart-validation latency


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

    # eBay: demand proxies only (active-ask stats + watch/merchandising
    # signals) — no public API exposes market-wide sold data, so eBay never
    # feeds the sales gate. See app/scrapers/ebay.py.
    ebay_client: Optional[EbayClient] = None
    if ebay_api.is_configured():
        ebay_client = EbayClient()
        emit("eBay source: Browse/Marketing demand proxies (active asks — NOT sold data)")
    else:
        logger.info("EBAY_APP_ID/EBAY_CERT_ID not set — eBay context disabled")

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
    run_stats: Dict[str, _SupplierRunStats] = {}   # supplier name -> stage counters

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
        for supplier, scraper, _since_dt in scrape_jobs:
            if runtime.shutdown_event.is_set():
                raise ScrapeInterrupted("interrupted by shutdown request — progress up to this supplier is committed")
            result = scrape_results[supplier.id]
            scraper_stats = getattr(scraper, "stats", None)
            sup_stats = run_stats.setdefault(supplier.name, _SupplierRunStats())
            sup_stats.elapsed = result.elapsed or 0.0
            if scraper_stats is not None:
                sup_stats.discovered = scraper_stats.discovered
                sup_stats.parsed = scraper_stats.parsed
                sup_stats.http_retries = scraper_stats.http_retries
            if result.error is not None:
                emit(f"  [ERROR] {supplier.name}: {result.error}")
                sup_stats.failures += 1
                db.add(ScrapeSupplierResult(
                    job_id=job_id, supplier_name=supplier.name, status="failed",
                    error_message=str(result.error) or repr(result.error),
                    elapsed_seconds=result.elapsed,
                ))
                db.commit()
                continue
            products: List[ScrapedProduct] = result.products

            emit(f"  {supplier.name} → {len(products)} sneaker products found")
            sup_result = ScrapeSupplierResult(
                job_id=job_id, supplier_name=supplier.name, status="succeeded",
                items_found=len(products), elapsed_seconds=result.elapsed,
            )
            db.add(sup_result)

            # Cart validator: the scraper itself, when it supports the flow
            cart_validator = (scraper if settings.cart_validation_enabled
                              and getattr(scraper, "supports_cart_validation", False) else None)

            # Timestamp for the deactivate-unconfirmed sweep AFTER this
            # supplier's evaluation completes. The old design deactivated
            # everything up front and re-activated confirmed rows, so any
            # mid-run crash left the dashboard permanently empty (29 rows,
            # 1 active in prod). Now stale rows are only deactivated once a
            # full successful pass over this supplier proves what's current.
            supplier_eval_start = datetime.utcnow()

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
                    sup_stats.sql_inserts += 1
                except Exception as exc:
                    db.rollback()
                    sup_stats.failures += 1
                    logger.exception(f"  [{supplier.name}] failed to persist {product.sku}")
                    emit(f"  [WARN] {supplier.name}: skipping bad row {product.sku}: {exc!r}")
                    _upsert_diagnostic(db, supplier, product, stage="parsed",
                                       error=f"persist failed: {exc!r}")
                    db.commit()
            sup_stats.inventory_ok = sum(1 for p in products if p.available_sizes())

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

            # ── B3+B4 interleaved: prefetch a small CHUNK of StockX lookups,
            # evaluate + COMMIT that chunk, then move to the next. Bounds the
            # API budget a crash/shutdown can waste to one chunk, instead of a
            # whole supplier's worth of unpersisted lookups. The lookup pool
            # stays sized for the API rate limit (stockx_lookup_concurrency),
            # NOT the retailer-scrape concurrency; workers do network only —
            # every DB write happens on this thread. ─────────────────────────
            seen_gate_skus: set = set()
            sku_names: Dict[str, str] = {}
            for product in products:
                sku_names.setdefault(product.sku, product.name)

            # Accumulates lookups across this supplier's chunks — a duplicate
            # SKU whose second occurrence lands in a later chunk must reuse
            # the earlier result, not refetch or count as "never fetched".
            prefetched: Dict[str, StockXLookupResult] = {}

            for chunk_start in range(0, len(products), LOOKUP_CHUNK_SIZE):
                if runtime.shutdown_event.is_set():
                    raise ScrapeInterrupted("interrupted by shutdown request — progress up to this chunk is committed")
                chunk = products[chunk_start:chunk_start + LOOKUP_CHUNK_SIZE]

                if use_api and not counters.budget_exhausted:
                    chunk_skus, chunk_seen = [], set()
                    for p in chunk:
                        if (p.sku not in chunk_seen and decisions[p.sku].check
                                and p.sku not in prefetched
                                and p.sku not in counters.lookup_failures):
                            chunk_seen.add(p.sku)
                            chunk_skus.append(p.sku)
                    if chunk_skus:
                        with concurrent.futures.ThreadPoolExecutor(
                            max_workers=max(1, settings.stockx_lookup_concurrency),
                            thread_name_prefix="stockx-api",
                        ) as pool:
                            futs = {pool.submit(api_client.get_market, sku, sku_names.get(sku, "")): sku
                                    for sku in chunk_skus}
                            for fut in concurrent.futures.as_completed(futs):
                                sku = futs[fut]
                                if runtime.shutdown_event.is_set():
                                    for f in futs:
                                        f.cancel()
                                    break
                                try:
                                    result = fut.result()
                                    prefetched[sku] = result
                                    if result.failure_reason and result.failure_reason.startswith("error:"):
                                        counters.lookup_failures[sku] = result.failure_reason
                                except StockXBudgetExhausted as exc:
                                    counters.budget_exhausted = True
                                    emit(f"  [StockX] {exc} — remaining lookups this run fall through unchecked")
                                    for f in futs:
                                        f.cancel()
                                    break
                                except Exception as exc:
                                    counters.lookup_failures[sku] = repr(exc)
                                    logger.exception(f"StockX lookup worker failed for {sku}")

                # Evaluate this chunk — each product isolated so one bad row
                # logs and continues instead of killing the batch.
                for product in chunk:
                    eval_start = time.perf_counter()
                    try:
                        opps = _evaluate_product(
                            db, supplier, product,
                            decision=decisions[product.sku],
                            gate_eff_price=gate_price[product.sku],
                            use_api=use_api,
                            prefetched=prefetched,
                            stockx_browser=stockx,
                            goat=goat, alias=alias,
                            ebay_client=ebay_client,
                            counters=counters,
                            count_gate=product.sku not in seen_gate_skus,
                            emit=emit,
                            cart_validator=cart_validator,
                            sup_stats=sup_stats,
                            eval_start=eval_start,
                        )
                        seen_gate_skus.add(product.sku)
                        db.commit()
                        total_opps += opps
                        sup_stats.opportunities += opps
                        sup_stats.sql_inserts += opps
                    except Exception as exc:
                        db.rollback()
                        sup_stats.failures += 1
                        logger.exception(f"  [{supplier.name}] failed to evaluate {product.sku}")
                        emit(f"  [WARN] {supplier.name}: evaluation failed for {product.sku}: {exc!r}")
                        try:
                            _upsert_diagnostic(
                                db, supplier, product, stage="inventory",
                                error=f"evaluation failed: {exc!r}",
                                duration_ms=int((time.perf_counter() - eval_start) * 1000),
                            )
                            db.commit()
                        except Exception:
                            db.rollback()
                            logger.exception(f"  [{supplier.name}] diagnostic write failed for {product.sku}")
                    if not use_api:
                        rate_limit(0.3, 0.8)   # politeness delay only matters for the browser path

            # ── Deactivate rows this successful pass did NOT confirm ─────────
            # (every confirmed opportunity got updated_at >= supplier_eval_start
            # in _upsert_opportunity). Runs only after the full supplier pass,
            # so a crash above leaves the previous run's rows visible.
            if products:
                stale = deactivate_stale_opportunities(db, supplier.id, supplier_eval_start)
                db.commit()
                if stale:
                    logger.info(f"  [{supplier.name}] deactivated {stale} stale opportunit(ies) "
                                "not confirmed by this run")

            total_skus += skus_this_supplier
            # Persist the per-stage numbers on this supplier's result row
            sup_result.products_discovered = sup_stats.discovered
            sup_result.products_parsed = sup_stats.parsed
            sup_result.inventory_ok = sup_stats.inventory_ok
            sup_result.cart_attempts = sup_stats.cart_attempts
            sup_result.cart_verified = sup_stats.cart_verified
            sup_result.opportunities_found = sup_stats.opportunities
            sup_result.failure_count = sup_stats.failures
            sup_result.http_retries = sup_stats.http_retries
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
        if ebay_client:
            ebay_client.close()
        if alias:
            alias.close()
        for _sup, scraper, _dt in scrape_jobs:
            close = getattr(scraper, "close", None)
            if callable(close):
                try:
                    close()   # releases the reusable cart-validation client
                except Exception:
                    logger.exception(f"scraper close failed for {_sup.name}")

    job.status = "done"
    job.finished_at = datetime.utcnow()
    job.skus_found = total_skus
    job.opportunities_found = total_opps
    job.stockx_calls_made = counters.made
    job.stockx_calls_skipped = counters.skipped
    db.commit()
    _emit_run_summary(emit, run_stats)
    emit(f"Scrape complete — {total_skus} SKUs, {total_opps} opportunities "
         f"(margin > ${settings.min_margin_threshold:.0f})")
    # The StockX budget is the run's real constraint — always surface it.
    emit(f"StockX market lookups: {counters.made} made, {counters.skipped} skipped by cache"
         + (f", match rate {counters.matched}/{counters.matched + counters.unmatched}"
            if use_api and (counters.matched + counters.unmatched) else ""))
    # Partial-failure summary — failed lookups never abort a run; they are
    # collected here and retried next run (their gate state is untouched).
    if counters.lookup_failures:
        sample = ", ".join(f"{s}: {r}" for s, r in
                           list(counters.lookup_failures.items())[:10])
        more = len(counters.lookup_failures) - 10
        emit(f"StockX lookup failures this run: {len(counters.lookup_failures)} SKU(s) "
             f"— {sample}" + (f" … and {more} more" if more > 0 else ""))
    if use_api and api_client:
        emit(f"StockX API budget: {api_client.calls_today()}/{DAILY_REQUEST_LIMIT} "
             f"requests used today (shared app-wide)")


def _emit_run_summary(emit: Callable, run_stats: Dict[str, "_SupplierRunStats"]):
    """Per-retailer stage summary table at the end of every run — discovered,
    parsed, inventory-confirmed, cart-verified, opportunities, SQL writes,
    failures, retries, timing."""
    if not run_stats:
        return
    emit("Run summary (per retailer):")
    header = (f"  {'retailer':<22} {'found':>6} {'parsed':>6} {'inv_ok':>6} "
              f"{'cart':>9} {'opps':>5} {'sql':>5} {'fail':>5} {'retry':>5} "
              f"{'cart_ms':>8} {'secs':>7}")
    emit(header)
    emit("  " + "-" * (len(header) - 2))
    tot = _SupplierRunStats()
    for name, s in sorted(run_stats.items()):
        cart = f"{s.cart_verified}/{s.cart_attempts}" if s.cart_attempts else "-"
        avg_cart = (f"{s.cart_ms_total // s.cart_attempts}" if s.cart_attempts else "-")
        emit(f"  {name[:22]:<22} {s.discovered:>6} {s.parsed:>6} {s.inventory_ok:>6} "
             f"{cart:>9} {s.opportunities:>5} {s.sql_inserts:>5} {s.failures:>5} "
             f"{s.http_retries:>5} {avg_cart:>8} {s.elapsed:>7.1f}")
        tot.discovered += s.discovered; tot.parsed += s.parsed
        tot.inventory_ok += s.inventory_ok; tot.cart_attempts += s.cart_attempts
        tot.cart_verified += s.cart_verified; tot.opportunities += s.opportunities
        tot.sql_inserts += s.sql_inserts; tot.failures += s.failures
        tot.http_retries += s.http_retries
    cart = f"{tot.cart_verified}/{tot.cart_attempts}" if tot.cart_attempts else "-"
    emit(f"  {'TOTAL':<22} {tot.discovered:>6} {tot.parsed:>6} {tot.inventory_ok:>6} "
         f"{cart:>9} {tot.opportunities:>5} {tot.sql_inserts:>5} {tot.failures:>5} "
         f"{tot.http_retries:>5} {'':>8} {'':>7}")


# ── Stale-opportunity expiry ──────────────────────────────────────────────────

def deactivate_stale_opportunities(db: Session, supplier_id: int,
                                   confirmed_after: datetime) -> int:
    """Mark is_active=False on THIS supplier's opportunities that the current
    run did not re-confirm (every confirmed row got updated_at bumped past
    `confirmed_after` by _upsert_opportunity, which also re-activates rows on
    reappearance). Scoped per supplier so a failed scrape elsewhere never
    wrongly deactivates another retailer's rows; the caller only invokes this
    after a full successful pass over the supplier. Returns rows deactivated."""
    return (
        db.query(Opportunity)
        .filter(
            Opportunity.supplier_id == supplier_id,
            Opportunity.is_active.is_(True),
            Opportunity.updated_at < confirmed_after,
        )
        .update({"is_active": False}, synchronize_session=False)
    )


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
    ebay_client: Optional[EbayClient],
    counters: _StockXCounters,
    count_gate: bool,
    emit: Callable,
    cart_validator=None,                      # scraper with validate_cart(), or None
    sup_stats: Optional[_SupplierRunStats] = None,
    eval_start: Optional[float] = None,
) -> int:
    """Resolve StockX data per the gate decision, then classify sizes.
    Opportunity candidates get an end-to-end cart validation for their exact
    size when the retailer supports it. Returns count of opportunities created."""
    now = datetime.utcnow()
    stockx_data: Optional[StockXProduct] = None
    market_fetched_at: Optional[datetime] = None
    fresh = False
    stockx_transient_failure = False

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
            # A request failure (5xx/transport after retries) is NOT the same
            # as "StockX doesn't have this shoe" — don't log it as a match
            # failure, and below, don't poison the gate cache with a
            # no_market_data verdict that would suppress retries for hours.
            stockx_transient_failure = bool(
                stockx_data is None and lookup.failure_reason
                and lookup.failure_reason.startswith("error:")
            )
            if count_gate:
                counters.made += 1
                if stockx_data is not None:
                    counters.matched += 1
                elif not stockx_transient_failure:
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

    # Sales-liquidity snapshot, once per product (product-level demand signal).
    # Alias is the only live source of individual sale events — the official
    # StockX API exposes no sales history and GOAT's public site none at all
    # (see app/services/liquidity.py's docstring for the source hierarchy).
    # A failed/absent live fetch falls back to previously persisted
    # sale_records; with neither, counts stay unknown and the liquidity gate
    # applies its live-bid demand proxy per size.
    liq_snapshot = LiquiditySnapshot(source="none")
    if alias and fresh:
        try:
            recent_sales = alias.get_recent_sales(product.sku, days=30)
            if recent_sales is not None:
                liq_snapshot = liquidity_svc.summarize_sales_events(recent_sales)
                _upsert_sale_records(db, product.sku, "alias", recent_sales)
        except Exception:
            # The failure itself must stay visible; the history fallback below
            # still runs.
            logger.warning(f"Alias recent-sales fetch failed for {product.sku}",
                           exc_info=True)
        rate_limit(0.5, 1.0)
    if not liq_snapshot.known:
        liq_snapshot = liquidity_svc.snapshot_from_sale_records(db, product.sku)

    # ── eBay context (demand proxies — never sales evidence) ─────────────────
    # Fetched at most once per SKU per TTL via its own cache gate; only on
    # fresh evaluations so cached passes stay zero-network. A None context
    # leaves any previously persisted ebay_* columns untouched.
    ebay_ctx: Optional[_EbayContext] = None
    if ebay_client is not None and fresh and sku_cache.gate_ebay_check(db, product.sku):
        ebay_ctx = _fetch_ebay_context(ebay_client, product, emit)
        sku_cache.record_ebay_check(db, product.sku)

    # ── Classify each in-stock size ──────────────────────────────────────────
    opps_created = 0
    best_resale: Optional[float] = None
    best_resale_type: Optional[str] = None
    last_cart: Optional[CartValidationResult] = None   # for the diagnostics row
    cart_rejected_sizes: List[str] = []
    liquidity_rejected_sizes: List[str] = []

    for avail_size in product.available_sizes():
        platform, resale_price, resale_type, platform_url, shoe_name, highest_bid, last_sale = \
            _pick_resale(avail_size.size, stockx_data, goat_data)

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

        result = classify_opportunity(
            original_price=float(avail_size.price or product.original_price),
            discount_percent=float(supplier.discount_percent or 0),
            discount_amount=float(supplier.discount_amount or 0),
            supplier_name=supplier.name,
            listing_price=resale_price,
            platform=platform,
            resale_price_type=resale_type,
            liquidity_snapshot=liq_snapshot,
            highest_bid=highest_bid,
        )
        # eBay reference margin (vs the median ACTIVE ask) + where-to-sell
        # recommendation. classify_opportunity has no eBay inputs, so nothing
        # here can influence the sales gate above.
        ebay_ref: Optional[EbayReference] = None
        if ebay_ctx is not None and ebay_ctx.median_ask:
            ebay_ref = compute_ebay_reference(result.cost, ebay_ctx.median_ask)
        rec_platform, rec_confidence = recommend_platform(
            stockx_sales_7d=result.sales_last_7_days,
            stockx_sales_30d=result.sales_last_30_days,
            primary_platform=platform,
            primary_margin=result.margin,
            ebay_margin=ebay_ref.margin if ebay_ref else None,
            ebay_watch_count=ebay_ctx.watch_count if ebay_ctx else None,
            ebay_demand_rank=ebay_ctx.demand_rank if ebay_ctx else None,
        )

        if not result.is_opportunity:
            # A margin that cleared the threshold but failed the liquidity
            # gate is exactly the class of junk this gate exists to remove —
            # log it distinctly so exclusions stay auditable.
            if result.margin > settings.min_margin_threshold and not result.liquidity.passes:
                liquidity_rejected_sizes.append(avail_size.size)
                emit(f"    ✗ {product.sku} Sz {avail_size.size}: excluded by liquidity gate "
                     f"({result.liquidity.reason}) despite ${result.margin:.2f} margin")
            continue

        # ── Cart validation: the margin is only real if this exact size can
        # actually be bought right now. Only opportunity candidates reach
        # here, so the volume is a handful of requests per run. ──────────────
        confidence = product.base_confidence()
        cart_status: Optional[str] = None
        cart_checked_at: Optional[datetime] = None
        if cart_validator is not None:
            # validate_cart retries throttle blips internally (Retry-After
            # honored); pace successive size validations against the same
            # store so we don't create the 429s ourselves.
            cart = cart_validator.validate_cart(avail_size.variant_id)
            rate_limit(0.8, 1.6)
            last_cart = cart
            cart_status = cart.reason
            cart_checked_at = datetime.utcnow()
            if sup_stats is not None:
                sup_stats.cart_attempts += 1
                sup_stats.cart_ms_total += cart.elapsed_ms or 0
            if cart.ok:
                confidence = CONFIDENCE_VERIFIED_CART
                if sup_stats is not None:
                    sup_stats.cart_verified += 1
            elif not cart.inconclusive:
                # Definitive rejection (sold out / size unavailable / dead
                # variant): NOT a real opportunity, whatever inventory said.
                cart_rejected_sizes.append(avail_size.size)
                if settings.require_cart_verification:
                    emit(f"    ✗ {product.sku} Sz {avail_size.size}: cart validation failed "
                         f"({cart.reason}) — excluded despite ${result.margin:.2f} margin")
                    continue
                # Operator opted out of hard gating — persist it flagged.
                confidence = CONFIDENCE_OUT_OF_STOCK
            # inconclusive: keep the opportunity at inventory-level confidence;
            # the reason is persisted so it is auditable, never silent.

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
            sales_7d=result.sales_last_7_days,
            sales_30d=result.sales_last_30_days,
            last_sale_date=liq_snapshot.last_sale_date,
            liquidity_status=result.liquidity.status,
            supplier_url=product.url,
            market_url=platform_url,
            highest_bid=highest_bid,
            last_sale=last_sale,
            seller_fees=round(resale_price - result.payout, 2),
            image_url=product.image_url,
            inventory_confidence=confidence,
            cart_status=cart_status,
            cart_checked_at=cart_checked_at,
            ebay_ctx=ebay_ctx,
            ebay_ref=ebay_ref,
            recommended_platform=rec_platform,
            recommendation_confidence=rec_confidence,
        )
        opps_created += 1
        if liq_snapshot.known:
            sales_label = (f"{result.sales_last_7_days}/7d {result.sales_last_30_days}/30d sales "
                           f"({result.liquidity.status})")
        else:
            sales_label = f"sales unknown ({result.liquidity.reason})"
        freshness = "fresh" if fresh else "cached"
        emit(
            f"    ✓ {product.name} | {product.sku} | Sz {avail_size.size} | "
            f"${result.cost:.2f} → ${result.payout:.2f} payout | "
            f"margin ${result.margin:.2f} ({result.roi:.1f}%) | "
            f"{sales_label} [{platform.upper()}/{resale_type}/{freshness}/{confidence}]"
        )

    # ── Update the gate cache after a fresh StockX evaluation ────────────────
    if fresh:
        if stockx_transient_failure and stockx_data is None:
            # StockX was unreachable, not empty — leave the verdict alone so
            # the SKU is re-checked next run instead of sitting out the TTL.
            sku_cache.record_seen(db, product.sku)
        else:
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

    # ── Diagnostics: record how far this product got and why ─────────────────
    if opps_created > 0:
        stage = "persisted"
    elif last_cart is not None:
        stage = "cart_validated"
    elif product.available_sizes():
        stage = "inventory"
    else:
        stage = "parsed"
    error = None
    if cart_rejected_sizes:
        error = f"cart rejected for size(s) {', '.join(cart_rejected_sizes)}"
    elif liquidity_rejected_sizes and not opps_created:
        error = (f"liquidity gate excluded size(s) "
                 f"{', '.join(liquidity_rejected_sizes)}")
    elif not product.available_sizes():
        error = "no in-stock sizes"
    _upsert_diagnostic(
        db, supplier, product, stage=stage,
        inventory_status=(CONFIDENCE_VERIFIED_CART if opps_created and last_cart and last_cart.ok
                          else product.base_confidence()),
        cart=last_cart, error=error,
        duration_ms=int((time.perf_counter() - eval_start) * 1000) if eval_start else None,
    )

    db.commit()
    return opps_created


# ── Helpers ───────────────────────────────────────────────────────────────────

@dataclass
class _EbayContext:
    """Product-level eBay context for one evaluation. Ask stats are CURRENT
    ACTIVE listings (price_type='active_ask'); watch_count/demand_rank are
    soft signals. None of this is ever passed to classify_opportunity."""
    active_listings: Optional[int] = None
    min_ask: Optional[float] = None
    median_ask: Optional[float] = None
    max_ask: Optional[float] = None
    watch_count: Optional[int] = None
    demand_rank: Optional[int] = None
    fetched_at: datetime = field(default_factory=datetime.utcnow)


def _fetch_ebay_context(ebay_client: EbayClient, product: ScrapedProduct,
                        emit: Callable) -> Optional["_EbayContext"]:
    """Catalog-resolve the SKU (ID-based queries beat free-text), then pull
    active-listing ask stats and the soft demand signal. Any endpoint failure
    (403 / out-of-scope / network) has already been reduced to None inside the
    client — this never raises into the batch."""
    try:
        match = ebay_client.resolve_catalog(product.sku, product.name)
        stats = ebay_client.get_active_listing_stats(
            product.sku,
            gtin=match.gtin if match else None,
            epid=match.epid if match else None,
        )
        signal = ebay_client.get_demand_signal(
            epid=match.epid if match else None,
            item_id=stats.top_item_id if stats else None,
        )
        if stats is None and signal is None:
            return None
        ctx = _EbayContext(
            active_listings=stats.active_count if stats else None,
            min_ask=stats.min_ask if stats else None,
            median_ask=stats.median_ask if stats else None,
            max_ask=stats.max_ask if stats else None,
            watch_count=signal.watch_count if signal else None,
            demand_rank=signal.demand_rank if signal else None,
        )
        if stats and stats.median_ask:
            emit(f"    [eBay]   {product.sku} → {stats.active_count} active listing(s), "
                 f"median ask ${stats.median_ask:.2f} (ask price, not sold)"
                 + (f", {ctx.watch_count} watchers" if ctx.watch_count else ""))
        return ctx
    except Exception:
        # Defensive belt on top of the client's own belt — eBay is strictly
        # optional context and must never sink a product evaluation.
        logger.warning(f"eBay context fetch failed for {product.sku}", exc_info=True)
        return None


def _pick_resale(size: str, stockx_data: Optional[StockXProduct], goat_data) \
        -> Tuple[Optional[str], Optional[float], Optional[str], Optional[str],
                 Optional[str], Optional[float], Optional[float]]:
    """(platform, resale_price, resale_price_type, url, name,
    highest_bid, last_sale) for a size.

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
                bid = getattr(sz, "highest_bid", None)
                bid = float(bid) if bid else None
                sale = float(sz.last_sale) if sz.last_sale else None
                if sz.lowest_ask:
                    return platform, float(sz.lowest_ask), "lowest_ask", url, name, bid, sale
                if sz.last_sale:
                    return platform, sale, "last_sale", url, name, bid, sale
    return None, None, None, None, None, None, None


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


def _upsert_diagnostic(db: Session, supplier: Supplier, product: ScrapedProduct,
                       stage: str, inventory_status: Optional[str] = None,
                       cart: Optional[CartValidationResult] = None,
                       error: Optional[str] = None,
                       duration_ms: Optional[int] = None):
    """Latest pipeline diagnostics per (retailer, SKU) — every product's
    furthest stage, availability verdict, cart response, and last error are
    queryable from SQL without rerunning a scrape."""
    row = (db.query(RetailerProductDiagnostic)
           .filter_by(supplier_id=supplier.id, sku=product.sku).first())
    if row is None:
        row = RetailerProductDiagnostic(supplier_id=supplier.id, sku=product.sku)
        db.add(row)
    row.supplier_name = supplier.name
    row.product_url = product.url
    row.image_url = product.image_url
    row.sizes_detected = " ".join(
        f"{s.size}{'✓' if s.in_stock else '✗'}" for s in product.sizes
    )[:2000] or None
    if inventory_status is not None:
        row.inventory_status = inventory_status
    if cart is not None:
        row.cart_status = cart.reason
        row.cart_token = (cart.cart_token or "")[:255] or None
        row.cart_quantity = cart.quantity
        row.cart_message = cart.message
    row.stage = stage
    row.last_error = error
    if duration_ms is not None:
        row.scrape_duration_ms = duration_ms
    row.updated_at = datetime.utcnow()


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
        "image_url": product.image_url,
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
            image_url=product.image_url,
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
                        sales_7d, supplier_url, market_url,
                        sales_30d=None, last_sale_date=None, liquidity_status=None,
                        highest_bid=None, last_sale=None, seller_fees=None,
                        image_url=None, inventory_confidence=None,
                        cart_status=None, cart_checked_at=None,
                        ebay_ctx: Optional[_EbayContext] = None,
                        ebay_ref: Optional[EbayReference] = None,
                        recommended_platform=None, recommendation_confidence=None):
    # Both marketplace links are contractual for the frontend — a row missing
    # either indicates an upstream parsing bug, so make it loud.
    if not supplier_url or not market_url:
        logger.error(f"Opportunity {sku} Sz {size} missing URL "
                     f"(supplier_url={supplier_url!r}, market_url={market_url!r}) — "
                     "persisting anyway; fix the upstream parser")
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
        existing.sales_last_30_days = sales_30d
        existing.last_sale_date   = last_sale_date
        existing.liquidity_status = liquidity_status
        existing.supplier_url     = supplier_url
        existing.market_url       = market_url
        existing.highest_bid      = highest_bid
        existing.last_sale        = last_sale
        existing.seller_fees      = seller_fees
        existing.image_url        = image_url
        existing.inventory_confidence = inventory_confidence
        existing.cart_status      = cart_status
        existing.cart_checked_at  = cart_checked_at
        # eBay context is TTL-gated and often skipped on a given run — only
        # overwrite when this run actually fetched it, so a skip preserves the
        # last real snapshot (its ebay_fetched_at shows its age).
        if ebay_ctx is not None:
            _apply_ebay_columns(existing, ebay_ctx, ebay_ref)
        existing.recommended_platform = recommended_platform
        existing.recommendation_confidence = recommendation_confidence
        existing.is_active        = True
        existing.updated_at       = now
    else:
        row = Opportunity(
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
            sales_last_30_days = sales_30d,
            last_sale_date   = last_sale_date,
            liquidity_status = liquidity_status,
            supplier_url     = supplier_url,
            market_url       = market_url,
            highest_bid      = highest_bid,
            last_sale        = last_sale,
            seller_fees      = seller_fees,
            image_url        = image_url,
            inventory_confidence = inventory_confidence,
            cart_status      = cart_status,
            cart_checked_at  = cart_checked_at,
            recommended_platform = recommended_platform,
            recommendation_confidence = recommendation_confidence,
            found_at         = now,
            updated_at       = now,
        )
        if ebay_ctx is not None:
            _apply_ebay_columns(row, ebay_ctx, ebay_ref)
        db.add(row)


def _apply_ebay_columns(row: Opportunity, ctx: _EbayContext,
                        ref: Optional[EbayReference]):
    """Write the eBay context onto an Opportunity row. ebay_price carries the
    median ACTIVE ask and is always labeled via ebay_price_type='active_ask' —
    'sold_avg' is reserved for a future Marketplace Insights integration."""
    row.ebay_price = ref.ask if ref else ctx.median_ask
    row.ebay_price_type = ref.price_type if ref else (
        "active_ask" if ctx.median_ask is not None else None)
    row.ebay_min_ask = ctx.min_ask
    row.ebay_max_ask = ctx.max_ask
    row.ebay_active_listings = ctx.active_listings
    row.ebay_watch_count = ctx.watch_count
    row.ebay_demand_rank = ctx.demand_rank
    row.ebay_seller_fees = round(ref.fees, 2) if ref else None
    row.ebay_payout = round(ref.payout, 2) if ref else None
    row.ebay_margin = round(ref.margin, 2) if ref else None
    row.ebay_roi = round(ref.roi, 4) if ref else None
    row.ebay_fetched_at = ctx.fetched_at
