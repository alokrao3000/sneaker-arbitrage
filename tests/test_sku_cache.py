"""Regression: the app session runs autoflush=False, and one product
evaluation touches the same SkuMarketCache row via record_ebay_check (early)
and record_seen/record_check (after the size loop). For a never-cached SKU
both used to add a second identical primary key → UniqueViolation at commit
(killed 11 evaluations on the 2026-07-18 validation run)."""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, SkuMarketCache
from app.services import sku_cache


@pytest.fixture()
def noflush_db(tmp_path):
    """Mirrors the app's SessionLocal config (autoflush=False) — the plain
    conftest fixture autoflushes and would mask the bug."""
    engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False)()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def test_ebay_then_stockx_check_on_new_sku_single_row(noflush_db):
    db = noflush_db
    assert sku_cache.gate_ebay_check(db, "NEW-SKU")
    sku_cache.record_ebay_check(db, "NEW-SKU")
    sku_cache.record_check(db, "NEW-SKU", effective_price=90.0,
                           verdict=sku_cache.VERDICT_PROFITABLE,
                           resale_price=200.0, resale_price_type="lowest_ask")
    db.commit()   # used to raise IntegrityError (duplicate pkey)

    rows = db.query(SkuMarketCache).filter_by(sku="NEW-SKU").all()
    assert len(rows) == 1
    assert rows[0].ebay_last_checked_at is not None
    assert rows[0].last_verdict == sku_cache.VERDICT_PROFITABLE


def test_ebay_then_record_seen_on_new_sku_single_row(noflush_db):
    db = noflush_db
    sku_cache.record_ebay_check(db, "NEW-SKU-2")
    sku_cache.record_seen(db, "NEW-SKU-2")   # transient-failure path pairing
    db.commit()
    assert db.query(SkuMarketCache).filter_by(sku="NEW-SKU-2").count() == 1
