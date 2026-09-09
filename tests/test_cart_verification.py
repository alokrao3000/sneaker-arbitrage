"""Cart-verification gate: the shared base.verify_cartable semantics, the
Footlocker bag-probe classification, and the SNS-class regression — a shoe the
retailer API reports as in-stock (stockLevelStatus=instock / available=true)
whose cart-add is refused must yield ZERO opportunities."""
import asyncio

import httpx
import pytest

from app.database import Opportunity, Supplier
from app.scrapers import footlocker as fl_mod
from app.scrapers.base import (
    ScrapedProduct, ScrapedSize, ScraperStats, verify_cartable,
)
from app.scrapers.footlocker import FootlockerScraper
from app.scrapers.shopify import ShopifyScraper
from app.services.arbitrage import (
    _evaluate_product, _get_supplier_scraper, _StockXCounters,
)
from app.services.sku_cache import GateDecision


def make_product(sku="DV0831-101", in_stock=True):
    return ScrapedProduct(
        name="Test Shoe", sku=sku, url="https://store.example/p/x",
        original_price=100.0,
        sizes=[
            ScrapedSize(size="10", price=100.0, in_stock=in_stock, variant_id="111"),
            ScrapedSize(size="11", price=100.0, in_stock=in_stock, variant_id="222"),
        ],
    )


def run_verify(products, outcome, stats=None):
    async def probe(_product):
        return outcome
    return asyncio.run(verify_cartable(products, probe, concurrency=2,
                                       stats=stats, supplier_label="teststore"))


class TestVerifyCartableHelper:
    def test_blocked_marks_every_size_unavailable(self, caplog):
        product = make_product()
        with caplog.at_level("INFO"):
            run_verify([product], "blocked")
        assert product.cart_probe == "blocked"
        assert all(not s.in_stock for s in product.sizes)
        assert product.available_sizes() == []
        # The downgrade log is distinct from ordinary out-of-stock logging
        msgs = [r.message for r in caplog.records if "cart-add failed" in r.message]
        assert len(msgs) == 1
        assert "DV0831-101" in msgs[0] and "teststore" in msgs[0]

    def test_ok_keeps_sizes_and_records_probe(self):
        product = make_product()
        stats = ScraperStats()
        run_verify([product], "ok", stats=stats)
        assert product.cart_probe == "ok"
        assert all(s.in_stock for s in product.sizes)
        assert (stats.cart_probes, stats.cart_probe_blocked) == (1, 0)

    def test_inconclusive_is_not_a_stock_verdict(self):
        # A throttled/bot-blocked probe must never zero real inventory.
        product = make_product()
        stats = ScraperStats()
        run_verify([product], "inconclusive", stats=stats)
        assert product.cart_probe == "inconclusive"
        assert all(s.in_stock for s in product.sizes)
        assert stats.cart_probe_inconclusive == 1

    def test_unprobeable_product_left_untouched(self):
        product = make_product()
        run_verify([product], None)
        assert product.cart_probe is None
        assert all(s.in_stock for s in product.sizes)

    def test_out_of_stock_products_are_not_probed(self):
        calls = []

        async def probe(p):
            calls.append(p.sku)
            return "ok"

        product = make_product(in_stock=False)
        asyncio.run(verify_cartable([product], probe, concurrency=2))
        assert calls == []


class TestFootlockerCartProbe:
    """_check_cartable classification: only responses where the API understood
    the request and refused count as 'blocked'; bot-manager/throttle/server
    failures are inconclusive."""

    @pytest.mark.parametrize("status,expected", [
        (200, "ok"), (201, "ok"),
        (400, "blocked"), (404, "blocked"), (412, "blocked"), (422, "blocked"),
        (401, "inconclusive"), (403, "inconclusive"), (406, "inconclusive"),
        (429, "inconclusive"), (500, "inconclusive"), (503, "inconclusive"),
    ])
    def test_status_classification(self, status, expected):
        def handler(request):
            assert request.url.path == "/api/users/carts/current/entries"
            assert request.method == "POST"
            return httpx.Response(status, json={})

        scraper = FootlockerScraper("https://www.footlocker.com")
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            assert scraper._check_cartable(client, "244001") == expected

    def test_network_error_is_inconclusive(self):
        def handler(request):
            raise httpx.ConnectError("akamai says no")

        scraper = FootlockerScraper("https://www.footlocker.com")
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            assert scraper._check_cartable(client, "244001") == "inconclusive"


def _sns_handler_factory(cart_status: int, seen: dict):
    """Mock Footlocker backend: search + detail report a fully in-stock launch
    shoe; the bag endpoint answers with `cart_status`."""
    def handler(request):
        path = request.url.path
        if request.method == "POST" and path == "/api/users/carts/current/entries":
            seen["cart_posts"] = seen.get("cart_posts", 0) + 1
            seen["cart_body"] = request.read().decode()
            return httpx.Response(cart_status, json={
                "errors": [{"type": "ProductNotBuyableException"}]})
        if path == "/api/session":
            return httpx.Response(200, json={"data": {"csrfToken": "csrf-tok"}})
        if path == "/api/products/search":
            return httpx.Response(200, json={
                "products": [{"productId": "sns1", "name": "AJ4 SNS",
                              "price": {"value": 215.0}}],
                "pagination": {"totalPages": 1},
            })
        if path == "/api/products/sns1":
            return httpx.Response(200, json={
                "styleVariants": [{"skuCode": "HF9989-100"}],
                "variants": [{"type": "SIZE", "entryValues": [
                    {"value": "10", "stockLevelStatus": "inStock", "code": "244001"},
                    {"value": "11", "stockLevelStatus": "inStock", "code": "244002"},
                ]}],
            })
        return httpx.Response(200, text="<html></html>")   # homepage/session init
    return handler


@pytest.fixture()
def fast_footlocker(monkeypatch):
    monkeypatch.setattr(fl_mod, "rate_limit", lambda *a, **k: None)


class TestSNSRegression:
    """The exact false-positive class from the SNS case: detail API says
    stockLevelStatus=instock, cart-add returns non-2xx → no size may survive
    as in-stock, and zero opportunities may be created for the SKU."""

    def _scrape(self, cart_status):
        seen = {}
        scraper = FootlockerScraper(
            "https://www.footlocker.com",
            transport=httpx.MockTransport(_sns_handler_factory(cart_status, seen)),
        )
        return scraper.scrape(), seen

    def test_cart_refusal_downgrades_api_reported_stock(self, fast_footlocker):
        products, seen = self._scrape(cart_status=400)
        assert len(products) == 1
        product = products[0]
        assert product.sku == "HF9989-100"
        assert product.cart_probe == "blocked"
        assert all(not s.in_stock for s in product.sizes)
        assert product.available_sizes() == []
        # exactly one probe per product, carrying the sellable-unit code
        assert seen["cart_posts"] == 1
        assert "244001" in seen["cart_body"]

    def test_cart_ok_keeps_stock(self, fast_footlocker):
        products, _ = self._scrape(cart_status=200)
        assert products[0].cart_probe == "ok"
        assert len(products[0].available_sizes()) == 2

    def test_blocked_product_creates_zero_opportunities(self, fast_footlocker, db):
        products, _ = self._scrape(cart_status=400)
        product = products[0]

        supplier = Supplier(name="Footlocker", url="https://www.footlocker.com",
                            platform_type="footlocker", active=True)
        db.add(supplier)
        db.commit()

        opps = _evaluate_product(
            db, supplier, product,
            decision=GateDecision(check=False, reason="cached_profitable"),
            gate_eff_price=215.0,
            use_api=True, prefetched={},
            stockx_browser=None, goat=None, alias=None, ebay_client=None,
            counters=_StockXCounters(), count_gate=True,
            emit=lambda m: None,
        )
        assert opps == 0
        assert db.query(Opportunity).count() == 0


class TestCustomPlatformInheritsCartGate:
    def test_custom_routes_through_shopify_scraper(self):
        supplier = Supplier(name="Boutique", url="https://boutique.example.com",
                            platform_type="custom")
        scraper = _get_supplier_scraper(supplier)
        assert isinstance(scraper, ShopifyScraper)

    def test_custom_scraper_runs_the_same_blocking_gate(self):
        # The instance the 'custom' fallback returns must downgrade a blocked
        # product exactly like a first-class Shopify supplier — same class,
        # same _filter_cartable → verify_cartable path.
        supplier = Supplier(name="Boutique", url="https://boutique.example.com",
                            platform_type="custom")
        scraper = _get_supplier_scraper(supplier)
        product = make_product(sku="CW2288-111")

        def handler(request):
            assert request.url.path == "/cart/add.json"
            return httpx.Response(422, json={"description": "coming soon"})

        async def run():
            async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler)) as client:
                return await scraper._filter_cartable(client, [product])

        asyncio.run(run())
        assert product.cart_probe == "blocked"
        assert product.available_sizes() == []
