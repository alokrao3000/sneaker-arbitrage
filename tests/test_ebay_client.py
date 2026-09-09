"""EbayClient behavior: OAuth client-credentials flow, active-ask stats,
demand signal, the tiered sold→listing fallback, and — most importantly —
that a 403/out-of-scope endpoint is logged once, disabled, and returns None
instead of crashing the batch."""
import json
from datetime import datetime, timedelta

import httpx
import pytest

from app.config import settings
from app.scrapers.ebay import (
    EbayClient, BROWSE_SAMPLE_SIZE, filter_price_outliers,
)


TOKEN_PATH = "/identity/v1/oauth2/token"


def make_client(handler):
    return EbayClient(app_id="app", cert_id="cert",
                      transport=httpx.MockTransport(handler))


def token_response():
    return httpx.Response(200, json={"access_token": "tok-1", "expires_in": 7200})


class TestOAuth:
    def test_token_fetched_once_and_cached(self):
        calls = {"token": 0}

        def handler(request):
            if request.url.path == TOKEN_PATH:
                calls["token"] += 1
                assert request.headers["Authorization"].startswith("Basic ")
                assert b"grant_type=client_credentials" in request.content
                return token_response()
            return httpx.Response(200, json={"itemSummaries": [], "total": 0})

        c = make_client(handler)
        c.get_active_listing_stats("DZ5485-612")
        c.get_active_listing_stats("DZ5485-612")
        assert calls["token"] == 1

    def test_token_failure_disables_client_gracefully(self):
        def handler(request):
            return httpx.Response(500, text="boom")

        c = make_client(handler)
        assert c.get_active_listing_stats("DZ5485-612") is None
        assert c.resolve_catalog("DZ5485-612") is None   # no crash, still None


class TestListingStats:
    def test_happy_path_min_median_max_are_ask_prices(self):
        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            assert request.url.path == "/buy/browse/v1/item_summary/search"
            assert request.url.params.get("fieldgroups") == "EXTENDED"
            return httpx.Response(200, json={
                "total": 37,
                "itemSummaries": [
                    {"itemId": "v1|1|0", "price": {"value": "150.00"}, "watchCount": 12},
                    {"itemId": "v1|2|0", "price": {"value": "100.00"}, "watchCount": 31},
                    {"itemId": "v1|3|0", "price": {"value": "200.00"}},
                ],
            })

        stats = make_client(handler).get_active_listing_stats("DZ5485-612")
        assert stats.active_count == 37
        assert (stats.min_ask, stats.median_ask, stats.max_ask) == (100.0, 150.0, 200.0)
        assert stats.price_type == "active_ask"     # never a sold price
        assert stats.top_item_id == "v1|1|0"
        assert stats.top_watch_count == 31          # max across the sample

    def test_prefers_epid_over_free_text(self):
        seen = {}

        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            seen.update(dict(request.url.params))
            return httpx.Response(200, json={"itemSummaries": [], "total": 0})

        make_client(handler).get_active_listing_stats("DZ5485-612", epid="12345")
        assert seen.get("epid") == "12345"
        assert "q" not in seen


class TestGracefulDegradation:
    def test_403_returns_none_and_disables_endpoint(self, caplog):
        calls = {"browse": 0}

        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            calls["browse"] += 1
            return httpx.Response(403, json={"errors": [{"errorId": 1100,
                                                         "message": "Insufficient permissions"}]})

        c = make_client(handler)
        with caplog.at_level("WARNING"):
            assert c.get_active_listing_stats("SKU-1") is None
            assert c.get_active_listing_stats("SKU-2") is None
            assert c.get_active_listing_stats("SKU-3") is None
        assert calls["browse"] == 1        # disabled after the first 403
        warnings = [r for r in caplog.records if "disabled for this process" in r.message]
        assert len(warnings) == 1          # logged once, not per SKU

    def test_403_on_marketing_leaves_browse_working(self):
        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            if "merchandised_product" in request.url.path:
                return httpx.Response(403, json={"errors": []})
            return httpx.Response(200, json={
                "total": 1,
                "itemSummaries": [{"itemId": "v1|9|0", "price": {"value": "99.0"}}],
            })

        c = make_client(handler)
        assert c.get_demand_signal(epid="123") is None     # marketing out of scope
        stats = c.get_active_listing_stats("SKU-1")        # browse unaffected
        assert stats is not None and stats.min_ask == 99.0

    def test_network_error_returns_none(self):
        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            raise httpx.ConnectError("nope")

        assert make_client(handler).get_active_listing_stats("SKU-1") is None


class TestDemandSignal:
    def test_merchandised_rank_and_watch_count(self):
        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            if "merchandised_product" in request.url.path:
                return httpx.Response(200, json={"merchandisedProducts": [
                    {"epid": "111"}, {"epid": "222"}, {"epid": "333"},
                ]})
            if request.url.path.startswith("/buy/browse/v1/item/"):
                assert "fieldgroups" not in request.url.params   # 400s live
                return httpx.Response(200, json={"watchCount": 42})
            return httpx.Response(404)

        sig = make_client(handler).get_demand_signal(epid="222", item_id="v1|7|0")
        assert sig.demand_rank == 2
        assert sig.watch_count == 42
        assert sig.source == "both"

    def test_search_derived_watch_count_skips_item_lookup(self):
        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            if "merchandised_product" in request.url.path:
                return httpx.Response(200, json={"merchandisedProducts": [{"epid": "9"}]})
            raise AssertionError(f"unexpected call: {request.url.path}")

        sig = make_client(handler).get_demand_signal(epid="9", item_id="v1|7|0",
                                                     watch_count=17)
        assert sig.watch_count == 17
        assert sig.demand_rank == 1


class TestSoldStatsTier:
    """Tier 1 (get_sold_stats): hard-gated off by default, outlier-filtered
    when enabled, and never crashes the batch."""

    def test_disabled_by_default_returns_none_and_logs_once(self, caplog):
        calls = {"insights": 0}

        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            calls["insights"] += 1
            return httpx.Response(200, json={"itemSales": []})

        c = make_client(handler)
        with caplog.at_level("WARNING"):
            assert c.get_sold_stats("DZ5485-612") is None
            assert c.get_sold_stats("DZ5485-612") is None
        assert calls["insights"] == 0      # never even hits the network
        warnings = [r for r in caplog.records if "disabled for this process" in r.message]
        assert len(warnings) == 1

    def test_enabled_returns_outlier_filtered_avg_and_counts(self, monkeypatch):
        monkeypatch.setattr(settings, "ebay_sold_data_enabled", True)
        now = datetime.utcnow()
        recent = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        old = (now - timedelta(days=20)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            assert "marketplace_insights" in request.url.path
            return httpx.Response(200, json={"itemSales": [
                {"lastSoldPrice": {"value": "200.00"}, "lastSoldDate": recent},
                {"lastSoldPrice": {"value": "210.00"}, "lastSoldDate": recent},
                {"lastSoldPrice": {"value": "190.00"}, "lastSoldDate": old},
                # fake/junk listing at 5× the median — must not skew the avg,
                # but it IS still a sale and must still count
                {"lastSoldPrice": {"value": "1000.00"}, "lastSoldDate": old},
            ]})

        stats = make_client(handler).get_sold_stats("DZ5485-612")
        assert stats.price_type == "sold_avg"
        assert stats.sales_count_7d == 2
        assert stats.sales_count_30d == 4          # outlier still counts as a sale
        assert stats.sample_size == 3              # ...but not toward the average
        assert stats.avg_sold_price_30d == pytest.approx(200.0)

    def test_enabled_but_403_fails_soft(self, monkeypatch):
        monkeypatch.setattr(settings, "ebay_sold_data_enabled", True)

        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            if "marketplace_insights" in request.url.path:
                return httpx.Response(403, json={"errors": []})
            return httpx.Response(200, json={"itemSummaries": [], "total": 0})

        c = make_client(handler)
        assert c.get_sold_stats("DZ5485-612") is None
        # Browse tier is unaffected by the insights 403
        assert c.get_active_listing_stats("DZ5485-612") is not None


class TestTierFallbackInPipeline:
    """_fetch_ebay_context (arbitrage.py) runs the tiers in order: sold stats
    first, active-listing stats as the fallback — and labels the result so
    downstream code can tell which tier answered."""

    def _handler(self, sold_response=None):
        def handler(request):
            if request.url.path == TOKEN_PATH:
                return token_response()
            if "marketplace_insights" in request.url.path:
                return sold_response or httpx.Response(403, json={"errors": []})
            if "catalog" in request.url.path:
                return httpx.Response(200, json={"productSummaries": []})
            if "merchandised_product" in request.url.path:
                return httpx.Response(200, json={"merchandisedProducts": []})
            return httpx.Response(200, json={
                "total": 40,
                "itemSummaries": [
                    {"itemId": "v1|1|0", "price": {"value": "170.00"}},
                    {"itemId": "v1|2|0", "price": {"value": "190.00"}},
                ],
            })
        return handler

    def _product(self):
        from app.scrapers.base import ScrapedProduct
        return ScrapedProduct(name="AJ4", sku="HF9989-100", url="u",
                              original_price=215.0, sizes=[])

    def test_unauthorized_tier1_falls_back_to_listing_avg(self):
        from app.services.arbitrage import _fetch_ebay_context
        ctx = _fetch_ebay_context(make_client(self._handler()),
                                  self._product(), emit=lambda m: None)
        assert ctx.price_type == "active_ask"
        assert ctx.price == 180.0                 # median of the live asks
        assert ctx.active_listings == 40          # listing count persists regardless of tier
        assert ctx.sales_count_7d is None         # never fabricated from listings
        assert ctx.sales_count_30d is None

    def test_tier1_wins_when_it_returns_data(self, monkeypatch):
        monkeypatch.setattr(settings, "ebay_sold_data_enabled", True)
        from app.services.arbitrage import _fetch_ebay_context
        now = datetime.utcnow()
        recent = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        sold = httpx.Response(200, json={"itemSales": [
            {"lastSoldPrice": {"value": "200.00"}, "lastSoldDate": recent},
        ]})
        ctx = _fetch_ebay_context(make_client(self._handler(sold_response=sold)),
                                  self._product(), emit=lambda m: None)
        assert ctx.price_type == "sold_avg"
        assert ctx.price == 200.0
        assert (ctx.sales_count_7d, ctx.sales_count_30d) == (1, 1)
        assert ctx.active_listings == 40          # tier 2 still fetched for supply context


class TestOutlierFilter:
    def test_drops_beyond_median_band_and_two_sigma(self):
        prices = [100.0, 105.0, 110.0, 95.0, 1000.0, 10.0]
        kept = filter_price_outliers(prices)
        assert 1000.0 not in kept and 10.0 not in kept
        assert set(kept) == {100.0, 105.0, 110.0, 95.0}

    def test_small_samples_pass_through(self):
        assert filter_price_outliers([100.0, 900.0]) == [100.0, 900.0]
