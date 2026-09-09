"""Preorder/future-release gate — the signal is independent of the cart
check: a preorder page can report available=true AND accept a cart-add, so a
confident preorder/future-release marker must exclude the product anyway
(the SNS false-positive class)."""
import httpx
import pytest
from datetime import datetime, timedelta

from app.database import Opportunity, Supplier
from app.scrapers import footlocker as fl_mod
from app.scrapers.footlocker import FootlockerScraper, _future_release_date
from app.scrapers.shopify import ShopifyScraper
from app.services.arbitrage import _evaluate_product, _StockXCounters
from app.services.sku_cache import GateDecision


def shopify_raw(tags=None, title="Nike Dunk Low Panda", published_at=None):
    return {
        "id": 1, "title": title, "handle": "dunk-low",
        "product_type": "Sneakers",
        "tags": tags or [],
        "published_at": published_at,
        "options": [{"name": "Size"}],
        "variants": [
            {"id": 11, "sku": "DD1391-100-10", "option1": "10",
             "price": "110.00", "available": True},
            {"id": 12, "sku": "DD1391-100-11", "option1": "11",
             "price": "110.00", "available": True},
        ],
    }


class TestShopifyPreorderSignal:
    def test_preorder_tag_excludes_despite_available_true(self, caplog):
        scraper = ShopifyScraper("https://store.example.com")
        with caplog.at_level("INFO"):
            product = scraper._parse(shopify_raw(tags=["Nike", "Pre-Order"]))
        assert product is not None
        assert all(not s.in_stock for s in product.sizes)
        assert product.available_sizes() == []
        msgs = [r.message for r in caplog.records
                if "preorder/future release detected" in r.message]
        assert len(msgs) == 1 and "DD1391-100" in msgs[0]

    def test_presale_title_excludes(self):
        product = ShopifyScraper("https://store.example.com")._parse(
            shopify_raw(title="Nike Dunk Low Panda (PRESALE)"))
        assert product.available_sizes() == []

    def test_future_published_at_excludes(self):
        future = (datetime.utcnow() + timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        product = ShopifyScraper("https://store.example.com")._parse(
            shopify_raw(published_at=future))
        assert product.available_sizes() == []

    def test_ordinary_release_tags_do_not_trigger(self):
        # "new-release" etc. must not be confused with "pre-release"
        product = ShopifyScraper("https://store.example.com")._parse(
            shopify_raw(tags=["new-release", "nike"]))
        assert len(product.available_sizes()) == 2


class TestFootlockerReleaseDate:
    def test_future_launch_date_detected_in_variant_attributes(self):
        future = (datetime.utcnow() + timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        detail = {"variantAttributes": [{"sku": "X", "skuLaunchDate": future}]}
        assert _future_release_date(detail) is not None

    def test_past_launch_date_is_ignored(self):
        past = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        detail = {"launchDate": past, "variantAttributes": []}
        assert _future_release_date(detail) is None

    def test_unparseable_or_absent_dates_are_ignored(self):
        assert _future_release_date({"launchDate": "not-a-date"}) is None
        assert _future_release_date({}) is None


def _launch_handler(cart_status: int, launch_date: str):
    """Footlocker mock: fully in-stock detail payload carrying a launch date;
    the bag endpoint ACCEPTS the add (the exact case the cart probe alone
    cannot catch)."""
    def handler(request):
        path = request.url.path
        if request.method == "POST" and path == "/api/users/carts/current/entries":
            return httpx.Response(cart_status, json={})
        if path == "/api/session":
            return httpx.Response(200, json={"data": {"csrfToken": "tok"}})
        if path == "/api/products/search":
            return httpx.Response(200, json={
                "products": [{"productId": "launch1", "name": "AJ4 SNS",
                              "price": {"value": 215.0}}],
                "pagination": {"totalPages": 1},
            })
        if path == "/api/products/launch1":
            return httpx.Response(200, json={
                "styleVariants": [{"skuCode": "HF9989-100"}],
                "variantAttributes": [{"sku": "244001", "skuLaunchDate": launch_date}],
                "variants": [{"type": "SIZE", "entryValues": [
                    {"value": "10", "stockLevelStatus": "inStock", "code": "244001"},
                ]}],
            })
        return httpx.Response(200, text="<html></html>")
    return handler


class TestSNSFutureReleaseRegression:
    """API says instock AND cart-add succeeds, but the release date is in the
    future → zero available sizes, zero opportunities."""

    def _scrape(self, monkeypatch, cart_status=200):
        monkeypatch.setattr(fl_mod, "rate_limit", lambda *a, **k: None)
        future = (datetime.utcnow() + timedelta(days=4)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        scraper = FootlockerScraper(
            "https://www.footlocker.com",
            transport=httpx.MockTransport(_launch_handler(cart_status, future)),
        )
        return scraper.scrape()

    def test_future_release_beats_successful_cart_add(self, monkeypatch, caplog):
        with caplog.at_level("INFO"):
            products = self._scrape(monkeypatch, cart_status=200)
        assert len(products) == 1
        assert products[0].available_sizes() == []
        assert any("preorder/future release detected" in r.message
                   for r in caplog.records)

    def test_zero_opportunities_created(self, monkeypatch, db):
        products = self._scrape(monkeypatch, cart_status=200)
        supplier = Supplier(name="Footlocker", url="https://www.footlocker.com",
                            platform_type="footlocker", active=True)
        db.add(supplier)
        db.commit()

        opps = _evaluate_product(
            db, supplier, products[0],
            decision=GateDecision(check=False, reason="cached_profitable"),
            gate_eff_price=215.0,
            use_api=True, prefetched={},
            stockx_browser=None, goat=None, alias=None, ebay_client=None,
            counters=_StockXCounters(), count_gate=True,
            emit=lambda m: None,
        )
        assert opps == 0
        assert db.query(Opportunity).count() == 0
