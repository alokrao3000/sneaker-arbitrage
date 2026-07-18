"""EbayClient behavior: OAuth client-credentials flow, active-ask stats,
demand signal, and — most importantly — that a 403/out-of-scope endpoint is
logged once, disabled, and returns None instead of crashing the batch."""
import json

import httpx
import pytest

from app.scrapers.ebay import EbayClient, BROWSE_SAMPLE_SIZE


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


class TestMarketplaceInsightsStub:
    def test_sold_items_search_is_a_guarded_stub(self):
        def handler(request):
            return token_response()

        c = make_client(handler)
        with pytest.raises(NotImplementedError):
            c.sold_items_search("DZ5485-612")
