from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, String, Numeric, Boolean, DateTime,
    Text, ForeignKey, UniqueConstraint, Index
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
    listing_price = Column(Numeric(10, 2))      # lowest ask on resale platform
    payout_price = Column(Numeric(10, 2))       # listing_price * (1 - commission)
    roi = Column(Numeric(8, 4))                 # (payout - cost) / cost * 100

    sales_last_7_days = Column(Integer, default=0)
    supplier_url = Column(String(1000))
    market_url = Column(String(1000))

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
    error_message = Column(Text)


def init_db():
    Base.metadata.create_all(bind=engine)
