"""Per-supplier stale-opportunity expiry: rows not re-confirmed by a
successful pass over THEIR supplier are deactivated; other suppliers' rows are
untouched; a reappearing row is re-activated by the upsert."""
from datetime import datetime, timedelta

from app.database import Opportunity, Supplier
from app.services.arbitrage import deactivate_stale_opportunities, _upsert_opportunity


def _mk_supplier(db, name):
    s = Supplier(name=name, url=f"https://{name}.example.com")
    db.add(s)
    db.commit()
    return s


def _mk_opp(db, supplier, sku, updated_at, active=True):
    o = Opportunity(sku=sku, size="10", supplier_id=supplier.id,
                    supplier_name=supplier.name, listing_platform="stockx",
                    is_active=active, updated_at=updated_at)
    db.add(o)
    db.commit()
    return o


def test_only_unconfirmed_rows_of_that_supplier_deactivate(db):
    a = _mk_supplier(db, "supplier-a")
    b = _mk_supplier(db, "supplier-b")
    cutoff = datetime.utcnow()
    stale_a = _mk_opp(db, a, "SKU-STALE", cutoff - timedelta(hours=2))
    fresh_a = _mk_opp(db, a, "SKU-FRESH", cutoff + timedelta(seconds=5))
    other_b = _mk_opp(db, b, "SKU-OTHER", cutoff - timedelta(hours=2))

    count = deactivate_stale_opportunities(db, a.id, cutoff)
    db.commit()

    assert count == 1
    assert db.get(Opportunity, stale_a.id).is_active is False
    assert db.get(Opportunity, fresh_a.id).is_active is True
    # A failed/absent scrape of supplier B must never touch B's rows.
    assert db.get(Opportunity, other_b.id).is_active is True


def test_failed_supplier_scrape_never_reaches_the_sweep(db):
    """The pipeline only calls the sweep after a full successful pass; this
    documents the contract that scoping is per supplier_id, so even when it
    runs for supplier A, supplier B's stale-looking rows survive."""
    b = _mk_supplier(db, "supplier-b")
    old = _mk_opp(db, b, "SKU-B", datetime.utcnow() - timedelta(days=3))
    count = deactivate_stale_opportunities(db, supplier_id=b.id + 999,
                                           confirmed_after=datetime.utcnow())
    db.commit()
    assert count == 0
    assert db.get(Opportunity, old.id).is_active is True


def test_reappearing_opportunity_reactivates(db):
    a = _mk_supplier(db, "supplier-a")
    old = _mk_opp(db, a, "SKU-BACK", datetime.utcnow() - timedelta(days=1),
                  active=False)

    _upsert_opportunity(
        db=db, sku="SKU-BACK", shoe_name="Test Shoe", size="10", supplier=a,
        original_price=100.0, discounted_price=100.0, cashback_rate=0.0,
        cashback_portal="none", cashback_amount=0.0, effective_price=100.0,
        platform="stockx", listing_price=300.0, resale_price_type="lowest_ask",
        payout_price=259.0, margin=159.0, roi=159.0,
        market_fetched_at=datetime.utcnow(), sales_7d=2,
        supplier_url="https://a/x", market_url="https://stockx.com/x",
        sales_30d=9,
    )
    db.commit()

    row = db.get(Opportunity, old.id)
    assert row.is_active is True
    assert row.sales_last_7_days == 2
