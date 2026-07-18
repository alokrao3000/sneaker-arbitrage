import logging
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Numeric, Boolean, DateTime,
    Text, ForeignKey, UniqueConstraint, Index, text
)
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker
from app.config import settings
from dotenv import load_dotenv
load_dotenv()


engine = create_engine(settings.database_url, pool_pre_ping=True, pool_size=5)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Models ────────────────────────────────────────────────────────────────────

class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    url = Column(String(500), nullable=False)
    category = Column(String(100))          # footsite | tier0_qs | shopify_*
    platform_type = Column(String(50))      # shopify | footlocker | nike | custom
    discount_percent = Column(Numeric(5, 2), default=0)
    discount_amount = Column(Numeric(10, 2), default=0)   # flat $ discount, used only when discount_percent is 0
    discount_notes = Column(Text)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    products = relationship("SupplierProduct", back_populates="supplier", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Supplier {self.name}>"


class SupplierProduct(Base):
    """A shoe found on a supplier site during a scrape run."""
    __tablename__ = "supplier_products"

    id = Column(Integer, primary_key=True, index=True)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"), nullable=False)
    sku = Column(String(100), nullable=False, index=True)
    name = Column(String(500))
    original_price = Column(Numeric(10, 2))
    product_url = Column(String(1000))
    image_url = Column(String(1000))
    scraped_at = Column(DateTime, default=datetime.utcnow)
    first_seen_at = Column(DateTime, default=datetime.utcnow, index=True)  # immutable; set once
    published_at = Column(DateTime)   # publication date reported by the source site

    supplier = relationship("Supplier", back_populates="products")
    sizes = relationship("SupplierProductSize", back_populates="product", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("supplier_id", "sku", name="uq_supplier_sku"),)


class SupplierProductSize(Base):
    """In-stock sizes for a supplier product."""
    __tablename__ = "supplier_product_sizes"

    id = Column(Integer, primary_key=True)
    supplier_product_id = Column(Integer, ForeignKey("supplier_products.id"), nullable=False)
    size = Column(String(20), nullable=False)
    in_stock = Column(Boolean, default=True)

    product = relationship("SupplierProduct", back_populates="sizes")

    __table_args__ = (UniqueConstraint("supplier_product_id", "size", name="uq_product_size"),)


class MarketPrice(Base):
    """Current market prices from StockX / GOAT for a specific SKU + size."""
    __tablename__ = "market_prices"

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String(100), nullable=False, index=True)
    platform = Column(String(50), nullable=False)   # stockx | goat
    size = Column(String(20), nullable=False)
    shoe_name = Column(String(500))
    lowest_ask = Column(Numeric(10, 2))
    highest_bid = Column(Numeric(10, 2))
    last_sale = Column(Numeric(10, 2))
    platform_url = Column(String(1000))
    fetched_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("sku", "platform", "size", name="uq_sku_platform_size"),
        Index("ix_market_prices_sku_platform", "sku", "platform"),
    )


class SaleRecord(Base):
    """Individual sale events pulled from StockX / GOAT — used for 7-day volume."""
    __tablename__ = "sale_records"

    id = Column(Integer, primary_key=True)
    sku = Column(String(100), nullable=False, index=True)
    platform = Column(String(50), nullable=False)
    size = Column(String(20), nullable=False)
    sale_price = Column(Numeric(10, 2))
    sale_date = Column(DateTime, nullable=False, index=True)
    recorded_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("sku", "platform", "size", "sale_date", name="uq_sale_record"),
    )


class Opportunity(Base):
    """A profitable flip: supplier listing vs. resale market payout."""
    __tablename__ = "opportunities"

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String(100), nullable=False, index=True)
    shoe_name = Column(String(500))
    size = Column(String(20))

    supplier_id = Column(Integer, ForeignKey("suppliers.id"))
    supplier_name = Column(String(255))
    original_price = Column(Numeric(10, 2))
    discounted_price = Column(Numeric(10, 2))
    discount_applied = Column(String(255))

    listing_platform = Column(String(50))       # stockx | goat
    listing_price = Column(Numeric(10, 2))      # resale price on the platform (see resale_price_type)
    highest_bid = Column(Numeric(10, 2))        # platform highest bid for this size (instant-sale floor)
    last_sale = Column(Numeric(10, 2))          # platform last sale for this size (None on StockX API — not exposed)
    seller_fees = Column(Numeric(10, 2))        # estimated platform fees incl. shipping (listing_price - payout_price)
    resale_price_type = Column(String(20), default="lowest_ask")  # lowest_ask | last_sale — an ask is a
                                                # listing price, a last-sale is a cleared transaction; README §Resale price
    payout_price = Column(Numeric(10, 2))       # listing_price - estimated seller fees
    margin = Column(Numeric(10, 2))             # payout_price - effective_price (dollars)
    roi = Column(Numeric(8, 4))                 # margin / cost * 100
    market_fetched_at = Column(DateTime)        # when the market data behind this row was actually fetched
                                                # (fresh vs cached indicator in the dashboard)

    # Cashback breakdown — cost/roi above are computed against effective_price,
    # not discounted_price, once cashback_rates.py has a real rate for this
    # supplier. See app/services/effective_price.py.
    cashback_rate = Column(Numeric(5, 4), default=0)     # fraction, e.g. 0.08
    cashback_portal = Column(String(50), default="none")
    cashback_amount = Column(Numeric(10, 2), default=0)
    effective_price = Column(Numeric(10, 2))             # discounted_price * (1 - cashback_rate); the true ROI cost basis

    # No default — NULL means "no sales source available" (unknown), which the
    # API and filters treat differently from a confirmed 0. A scalar default
    # here would silently turn explicit None inserts into 0.
    sales_last_7_days = Column(Integer)
    sales_last_30_days = Column(Integer)        # same NULL-means-unknown convention
    last_sale_date = Column(DateTime)           # most recent completed sale we know of
    # eligible | unknown (see app/services/liquidity.py). not_eligible rows are
    # never persisted — the gate excludes them before this table. NULL = row
    # predates the liquidity gate; treated as unknown by the API.
    liquidity_status = Column(String(20))
    supplier_url = Column(String(1000))
    image_url = Column(String(1000))            # retailer product image
    market_url = Column(String(1000))

    # ── eBay context (app/scrapers/ebay.py) — DEMAND PROXIES, never sold data.
    # ebay_price is a current ACTIVE ASK (median of live listings), not a
    # realized sale price — ebay_price_type says so explicitly so downstream
    # consumers can't mistake it for a sold average. Watch count / demand rank
    # are soft signals for display/sort only; they are structurally excluded
    # from the sales-liquidity gate (sales_last_*_days stay StockX/Alias-fed).
    # All NULL when eBay credentials are absent or every endpoint was out of
    # scope for this SKU.
    ebay_price = Column(Numeric(10, 2))             # median active ask
    ebay_price_type = Column(String(20))            # always 'active_ask' for now; 'sold_avg'
                                                    # reserved for Marketplace Insights if approved
    ebay_min_ask = Column(Numeric(10, 2))
    ebay_max_ask = Column(Numeric(10, 2))
    ebay_active_listings = Column(Integer)          # live listing count (supply context)
    ebay_watch_count = Column(Integer)              # soft demand signal — NOT sales evidence
    ebay_demand_rank = Column(Integer)              # merchandised-products rank — NOT sales evidence
    ebay_seller_fees = Column(Numeric(10, 2))       # est. fees on the reference ask (app/ebay_fees.py)
    ebay_payout = Column(Numeric(10, 2))
    ebay_margin = Column(Numeric(10, 2))            # reference margin vs the ACTIVE ask
    ebay_roi = Column(Numeric(8, 4))
    ebay_fetched_at = Column(DateTime)

    # Where to sell (app/services/pricing.py:recommend_platform). Defaults to
    # the platform whose market data backs the margin (StockX) whenever its
    # sales data is genuinely known; eBay-based reasoning only breaks the tie
    # when StockX sales are unknown, and is then flagged confidence='low'.
    recommended_platform = Column(String(20))
    recommendation_confidence = Column(String(10))  # high | low

    # Inventory confidence ladder (see app/scrapers/base.py):
    # VERIFIED_CART | VERIFIED_INVENTORY | INVENTORY_ONLY | UNKNOWN | OUT_OF_STOCK
    inventory_confidence = Column(String(20))
    cart_status = Column(String(40))            # CART_* reason from the last validation attempt
    cart_checked_at = Column(DateTime)

    is_active = Column(Boolean, default=True)
    found_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("sku", "size", "supplier_id", "listing_platform", name="uq_opportunity"),
        Index("ix_opportunities_roi", "roi"),
    )


class ScrapeJob(Base):
    """Tracks each scrape run for status display in the dashboard."""
    __tablename__ = "scrape_jobs"

    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)
    status = Column(String(50), default="running")   # running | done | error
    suppliers_scraped = Column(Integer, default=0)
    skus_found = Column(Integer, default=0)
    opportunities_found = Column(Integer, default=0)
    # StockX budget accounting — calls actually made vs skipped via the per-SKU
    # cache gate (app/services/sku_cache.py). The API budget is shared app-wide,
    # so this is the run's real constraint; surfaced in the dashboard summary.
    stockx_calls_made = Column(Integer, default=0)
    stockx_calls_skipped = Column(Integer, default=0)
    error_message = Column(Text)


class ScrapeSupplierResult(Base):
    """Per-retailer outcome of one scrape job — succeeded/failed/skipped, item
    count, and timing, so the dashboard can show scrape status per retailer."""
    __tablename__ = "scrape_supplier_results"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("scrape_jobs.id"), nullable=False, index=True)
    supplier_name = Column(String(255), nullable=False)
    status = Column(String(20), nullable=False)   # succeeded | failed | skipped
    items_found = Column(Integer, default=0)
    error_message = Column(Text)
    elapsed_seconds = Column(Numeric(8, 2))
    finished_at = Column(DateTime, default=datetime.utcnow)
    # Per-stage observability (see the end-of-run summary table)
    products_discovered = Column(Integer)   # raw items from the retailer
    products_parsed = Column(Integer)       # complete products with SKU
    inventory_ok = Column(Integer)          # products with >=1 in-stock size
    cart_attempts = Column(Integer)         # per-size cart validations attempted
    cart_verified = Column(Integer)         # ... that came back VERIFIED_CART
    opportunities_found = Column(Integer)
    failure_count = Column(Integer)         # per-product evaluation/persist failures
    http_retries = Column(Integer)          # transient-failure retries during scrape


class RetailerProductDiagnostic(Base):
    """Latest per-(retailer, SKU) pipeline diagnostics — which stage the
    product reached, availability/cart verdicts, and the last error. Upserted
    every scrape so failed retailers can be debugged from SQL without
    rerunning anything. `stage` is the furthest stage reached:
    parsed → inventory → cart_validated → opportunity → persisted."""
    __tablename__ = "retailer_product_diagnostics"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(Integer, ForeignKey("suppliers.id"), nullable=False)
    supplier_name = Column(String(255))
    sku = Column(String(100), nullable=False, index=True)
    product_url = Column(String(1000))
    image_url = Column(String(1000))
    sizes_detected = Column(Text)           # e.g. "9✓ 9.5✓ 10✗" (✓ = in stock)
    inventory_status = Column(String(20))   # confidence ladder value
    cart_status = Column(String(40))        # CART_* reason (last validation on any size)
    cart_token = Column(String(255))        # retailer cart identifier when returned
    cart_quantity = Column(Integer)         # quantity the retailer confirmed
    cart_message = Column(Text)             # raw retailer response detail
    stage = Column(String(30), nullable=False)
    last_error = Column(Text)
    scrape_duration_ms = Column(Integer)    # per-product evaluation duration
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("supplier_id", "sku", name="uq_diag_supplier_sku"),
    )


class SkuMarketCache(Base):
    """Per-SKU gate for StockX market lookups (rate-limit protection).

    Tracks the lowest effective retail price ever evaluated for a SKU and the
    verdict that evaluation produced, so re-seeing the same shoe at the same
    (or worse) price doesn't burn another StockX call. See
    app/services/sku_cache.py for the gating rules."""
    __tablename__ = "sku_market_cache"

    sku = Column(String(100), primary_key=True)
    best_effective_price = Column(Numeric(10, 2))   # lowest effective price ever evaluated
    last_verdict = Column(String(20))               # profitable | not_profitable | no_market_data
    last_checked_at = Column(DateTime)              # last actual StockX market fetch
    last_seen_at = Column(DateTime)                 # last time any scrape saw this SKU
    resale_price = Column(Numeric(10, 2))           # resale price used in the last evaluation
    resale_price_type = Column(String(20))          # lowest_ask | last_sale
    # eBay context is gated separately (its endpoints have their own quota and
    # its data is secondary) — see sku_cache.gate_ebay_check.
    ebay_last_checked_at = Column(DateTime)


class StockXMatchFailure(Base):
    """SKUs that could not be resolved to a StockX catalog product — logged
    instead of silently dropped, so match-rate quality is visible."""
    __tablename__ = "stockx_match_failures"

    id = Column(Integer, primary_key=True)
    sku = Column(String(100), nullable=False, unique=True, index=True)
    name = Column(String(500))
    reason = Column(String(255))          # no_search_results | no_style_match | error:<detail>
    attempts = Column(Integer, default=1)
    first_failed_at = Column(DateTime, default=datetime.utcnow)
    last_failed_at = Column(DateTime, default=datetime.utcnow)


class StockXApiUsage(Base):
    """One row per UTC day — persistent daily request counter for the StockX
    API. The daily budget is per developer account and shared by every part of
    the app (scrape runs, live SKU lookups), so it must survive restarts."""
    __tablename__ = "stockx_api_usage"

    day = Column(String(10), primary_key=True)      # 'YYYY-MM-DD' (UTC)
    calls = Column(Integer, default=0, nullable=False)


class StockXOAuthToken(Base):
    """Single-row (id=1) persistence for StockX OAuth tokens. The refresh
    token is seeded by scripts/stockx_auth.py (or STOCKX_REFRESH_TOKEN in the
    environment) and updated here whenever StockX rotates it on refresh."""
    __tablename__ = "stockx_oauth_tokens"

    id = Column(Integer, primary_key=True)          # always 1
    refresh_token = Column(Text)
    access_token = Column(Text)
    access_token_expires_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)
    _reconcile_schema()


def _literal_default(col) -> "str | None":
    """SQL literal for a column's scalar Python default, or None when there is
    no default or it's a callable (e.g. datetime.utcnow — those stay app-side)."""
    if col.default is None or col.default.is_callable:
        return None
    val = col.default.arg
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"
    return None


def _reconcile_schema():
    """Schema/model reconciliation, run before anything else at startup.

    create_all() only creates missing tables, never columns on tables that
    already exist. There's no Alembic in this project, so instead of a
    hand-maintained ALTER list (the old approach — it drifted, and scrape runs
    died on UndefinedColumn), diff every model against the live schema and
    ADD COLUMN IF NOT EXISTS for anything missing, then re-inspect and refuse
    to start if any drift remains."""
    from sqlalchemy import inspect as sa_inspect

    inspector = sa_inspect(engine)
    statements = []
    for table in Base.metadata.sorted_tables:
        existing = {c["name"] for c in inspector.get_columns(table.name)}
        for col in table.columns:
            if col.name in existing:
                continue
            ddl = (
                f"ALTER TABLE {table.name} ADD COLUMN IF NOT EXISTS "
                f"{col.name} {col.type.compile(engine.dialect)}"
            )
            default = _literal_default(col)
            if default is not None:
                ddl += f" DEFAULT {default}"
            statements.append(ddl)

    if statements:
        with engine.begin() as conn:
            for stmt in statements:
                logging.getLogger(__name__).info(f"Schema reconciliation: {stmt}")
                conn.execute(text(stmt))

    # Assert: no drift may survive startup — failing loudly here beats an
    # UndefinedColumn mid-scrape.
    inspector = sa_inspect(engine)
    missing = [
        f"{table.name}.{col.name}"
        for table in Base.metadata.sorted_tables
        for col in table.columns
        if col.name not in {c["name"] for c in inspector.get_columns(table.name)}
    ]
    if missing:
        raise RuntimeError(
            f"Schema drift persists after reconciliation — refusing to start: {missing}"
        )


def close_orphaned_jobs():
    """Mark jobs left in 'running' by a previous process (crash, Ctrl-C,
    uvicorn reload) as terminal. Called at startup, before the scheduler
    starts, so no legitimately-running job can exist yet in this process."""
    with engine.begin() as conn:
        result = conn.execute(text(
            "UPDATE scrape_jobs SET status = 'error', "
            "finished_at = COALESCE(finished_at, (NOW() AT TIME ZONE 'utc')), "
            "error_message = 'interrupted — app restarted while job was still running' "
            "WHERE status = 'running'"
        ))
        if result.rowcount:
            logging.getLogger(__name__).warning(
                f"Closed {result.rowcount} orphaned 'running' scrape job(s) from a previous process"
            )
