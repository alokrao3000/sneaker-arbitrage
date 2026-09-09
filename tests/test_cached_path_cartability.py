"""The cached-evaluation cartability guard: a cached-profitable market
verdict may only be refreshed from cache while the SUPPLIER side is
verifiably still buyable — otherwise the SKU is escalated to a full fresh
re-check and the stale opportunity gets deactivated instead of silently
persisting run after run."""
from datetime import datetime, timedelta

import pytest

from app.config import settings
from app.database import (
    MarketPrice, Opportunity, Supplier, SupplierProduct, SupplierProductSize,
)
from app.scrapers.base import ScrapedProduct, ScrapedSize
from app.services import sku_cache
from app.services.arbitrage import (
    _cartable_evidence_ok, _evaluate_product, _StockXCounters,
    deactivate_stale_opportunities,
)
from app.services.sku_cache import GateDecision

SKU = "DZ5485-612"


def make_supplier(db, name="Premier"):
    s = Supplier(name=name, url="https://premier.example.com",
                 platform_type="shopify", active=True)
    db.add(s)
    db.commit()
    return s


def seed_size_row(db, supplier, sku=SKU, is_cartable=None, verified_at=None):
    sp = SupplierProduct(supplier_id=supplier.id, sku=sku, name="Shoe",
                         original_price=110)
    db.add(sp)
    db.flush()
    row = SupplierProductSize(supplier_product_id=sp.id, size="10",
                              in_stock=True, is_cartable=is_cartable,
                              verified_cartable_at=verified_at)
    db.add(row)
    db.commit()
    return row


def make_product(cart_probe=None, in_stock=True):
    return ScrapedProduct(
        name="Shoe", sku=SKU, url="https://premier.example.com/p/x",
        original_price=110.0, cart_probe=cart_probe,
        sizes=[ScrapedSize(size="10", price=110.0, in_stock=in_stock,
                           variant_id="v1")],
    )


class TestCartableEvidence:
    def test_fresh_ok_probe_passes(self, db):
        supplier = make_supplier(db)
        assert _cartable_evidence_ok(db, supplier, SKU,
                                     [make_product(cart_probe="ok")])

    def test_no_sizes_claiming_stock_trivially_passes(self, db):
        supplier = make_supplier(db)
        assert _cartable_evidence_ok(db, supplier, SKU,
                                     [make_product(in_stock=False)])

    def test_recent_persisted_verification_passes(self, db):
        supplier = make_supplier(db)
        seed_size_row(db, supplier, is_cartable=True,
                      verified_at=datetime.utcnow() - timedelta(hours=1))
        assert _cartable_evidence_ok(db, supplier, SKU,
                                     [make_product(cart_probe="inconclusive")])

    def test_stale_verification_fails(self, db):
        supplier = make_supplier(db)
        stale = datetime.utcnow() - timedelta(
            hours=settings.cart_verification_ttl_hours + 1)
        seed_size_row(db, supplier, is_cartable=True, verified_at=stale)
        assert not _cartable_evidence_ok(db, supplier, SKU,
                                         [make_product(cart_probe="inconclusive")])

    def test_is_cartable_false_fails(self, db):
        supplier = make_supplier(db)
        seed_size_row(db, supplier, is_cartable=False,
                      verified_at=datetime.utcnow())
        assert not _cartable_evidence_ok(db, supplier, SKU, [make_product()])

    def test_never_verified_fails(self, db):
        supplier = make_supplier(db)
        seed_size_row(db, supplier)   # is_cartable NULL — unknown
        assert not _cartable_evidence_ok(db, supplier, SKU, [make_product()])


def seed_cached_profitable(db, supplier):
    """Cached market state + an active opportunity, as a previous run left it."""
    sku_cache.record_check(db, SKU, 110.0, sku_cache.VERDICT_PROFITABLE)
    db.add(MarketPrice(sku=SKU, platform="stockx", size="10",
                       lowest_ask=300.0, highest_bid=180.0, last_sale=None,
                       shoe_name="Shoe", platform_url="https://stockx.com/x",
                       fetched_at=datetime.utcnow()))
    opp = Opportunity(sku=SKU, size="10", supplier_id=supplier.id,
                      supplier_name=supplier.name, listing_platform="stockx",
                      listing_price=300.0, margin=100.0, roi=90.0,
                      is_active=True,
                      found_at=datetime.utcnow() - timedelta(days=1),
                      updated_at=datetime.utcnow() - timedelta(days=1))
    db.add(opp)
    db.commit()
    return opp


class TestCachedPathRegression:
    """The reported bug shape: a SKU cached as profitable resurfaces every run
    from the cache path even though the listing is no longer actually
    buyable. With the guard, the decision escalates to a fresh re-check and
    the stale opportunity is deactivated."""

    def test_uncartable_cached_profitable_is_escalated_and_deactivated(self, db):
        supplier = make_supplier(db)
        # Discovered after caching: the release was delayed — cart-add failed.
        seed_size_row(db, supplier, is_cartable=False,
                      verified_at=datetime.utcnow() - timedelta(hours=1))
        opp = seed_cached_profitable(db, supplier)
        product = make_product(cart_probe="inconclusive")   # probe throttled this run

        # The market cache alone says: shortcut from cache.
        decision = sku_cache.gate_stockx_check(db, SKU, 110.0)
        assert decision.evaluate_from_cache

        # The cartability guard overrules it — exactly the must-re-check branch.
        assert not _cartable_evidence_ok(db, supplier, SKU, [product])
        decision = GateDecision(check=True, reason="cart_unverified",
                                cached=decision.cached)

        eval_start = datetime.utcnow()
        opps = _evaluate_product(
            db, supplier, product, decision=decision, gate_eff_price=110.0,
            use_api=True, prefetched={},   # fresh lookup not available this run
            stockx_browser=None, goat=None, alias=None, ebay_client=None,
            counters=_StockXCounters(), count_gate=True, emit=lambda m: None,
        )
        assert opps == 0

        deactivate_stale_opportunities(db, supplier.id, confirmed_after=eval_start)
        db.commit()
        db.refresh(opp)
        assert opp.is_active is False          # deactivated, not silently persisted

    def test_verified_cartable_cache_path_still_refreshes(self, db):
        # Control: with valid cart evidence the cache path must keep working —
        # the opportunity is refreshed with zero fresh StockX calls.
        supplier = make_supplier(db)
        seed_size_row(db, supplier, is_cartable=True,
                      verified_at=datetime.utcnow() - timedelta(minutes=30))
        opp = seed_cached_profitable(db, supplier)
        product = make_product(cart_probe="inconclusive")

        decision = sku_cache.gate_stockx_check(db, SKU, 110.0)
        assert decision.evaluate_from_cache
        assert _cartable_evidence_ok(db, supplier, SKU, [product])

        eval_start = datetime.utcnow()
        opps = _evaluate_product(
            db, supplier, product, decision=decision, gate_eff_price=110.0,
            use_api=True, prefetched={},
            stockx_browser=None, goat=None, alias=None, ebay_client=None,
            counters=_StockXCounters(), count_gate=True, emit=lambda m: None,
        )
        assert opps == 1
        deactivate_stale_opportunities(db, supplier.id, confirmed_after=eval_start)
        db.commit()
        db.refresh(opp)
        assert opp.is_active is True
        assert opp.updated_at >= eval_start
