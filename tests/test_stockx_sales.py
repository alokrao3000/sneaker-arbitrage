"""StockX sales-volume scraping: extraction of realized-sale events from
intercepted product-page responses, window counting with exact-edge
handling, None-means-unknown propagation, and the pricing gate consuming
real (not stubbed) counts."""
from datetime import datetime, timedelta

import pytest

from app.scrapers.stockx_api import _sales_count
from app.scrapers.stockx_market import (
    StockXProduct, _count_sales, _extract_sales_events,
)
from app.services.liquidity import LiquiditySnapshot
from app.services.pricing import classify_opportunity


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


NOW = datetime(2026, 7, 19, 12, 0, 0)


class TestExtractSalesEvents:
    def test_rest_activity_shape_is_parsed_newest_first(self):
        bodies = [{"ProductActivity": [
            {"amount": 250, "createdAt": iso(NOW - timedelta(days=1)), "shoeSize": "10"},
            {"amount": "240", "createdAt": iso(NOW - timedelta(days=3)), "shoeSize": "9.5"},
            {"amount": 260, "createdAt": iso(NOW - timedelta(days=10))},
        ]}]
        events = _extract_sales_events(bodies)
        assert [e["price"] for e in events] == [250.0, 240.0, 260.0]
        assert events[0]["size"] == "10"
        assert events[2]["size"] is None
        assert events[0]["sale_date"] > events[1]["sale_date"]

    def test_graphql_sales_history_shape_is_parsed(self):
        bodies = [{"data": {"product": {"salesHistory": {"edges": [
            {"node": {"localAmount": 199.0, "saleDate": iso(NOW - timedelta(days=2)),
                      "size": "11"}},
        ]}}}}]
        events = _extract_sales_events(bodies)
        assert len(events) == 1
        assert events[0]["price"] == 199.0 and events[0]["size"] == "11"

    def test_ask_bid_ladders_are_not_misread_as_sales(self):
        # Variant/market entries carry nested {amount} dicts but no
        # amount+date pair on the same object — must yield None (unknown).
        bodies = [{"data": {"variants": [
            {"size": "10", "market": {"lowestAsk": {"amount": 150},
                                      "highestBid": {"amount": 120}}},
        ]}}]
        assert _extract_sales_events(bodies) is None

    def test_no_sales_payload_returns_none_not_zero(self):
        assert _extract_sales_events([]) is None
        assert _extract_sales_events([{"data": {"foo": "bar"}}]) is None

    def test_explicitly_empty_activity_is_a_real_zero(self):
        events = _extract_sales_events([{"ProductActivity": []}])
        assert events == []          # confirmed zero, distinct from None


class TestCountSales:
    def test_window_counting(self):
        events = [
            {"sale_date": NOW - timedelta(days=1)},
            {"sale_date": NOW - timedelta(days=6)},
            {"sale_date": NOW - timedelta(days=20)},
        ]
        assert _count_sales(events, now=NOW) == (2, 3)

    def test_exact_edge_sales_are_inclusive(self):
        events = [
            {"sale_date": NOW - timedelta(days=7)},    # exactly 7d → in both
            {"sale_date": NOW - timedelta(days=30)},   # exactly 30d → in 30d
        ]
        assert _count_sales(events, now=NOW) == (1, 2)

    def test_early_break_past_30_days(self):
        # Newest-first: once past the 30d cutoff, older events must not count.
        events = [
            {"sale_date": NOW - timedelta(days=2)},
            {"sale_date": NOW - timedelta(days=31)},
            {"sale_date": NOW - timedelta(days=1)},    # unreachable after break
        ]
        assert _count_sales(events, now=NOW) == (1, 1)

    def test_dateless_events_count_conservatively(self):
        assert _count_sales([{"sale_date": None}], now=NOW) == (1, 1)


class TestNonePropagation:
    def test_product_defaults_to_unknown_not_zero(self):
        p = StockXProduct(sku="X", name="", url_key="", stockx_url="")
        assert p.sales_last_7_days is None
        assert p.sales_last_30_days is None
        assert p.sales_events is None


class TestPricingGateWithRealCounts:
    def test_high_margin_zero_sales_is_blocked(self):
        # A genuine zero from the sales-history feed (not a stub, not
        # unknown) must fail the OR-gate no matter how fat the margin is.
        snapshot = LiquiditySnapshot(sales_last_7_days=0, sales_last_30_days=0,
                                     source="stockx_page")
        result = classify_opportunity(
            original_price=100.0, discount_percent=40.0, listing_price=400.0,
            liquidity_snapshot=snapshot, highest_bid=250.0,
        )
        assert result.margin > 100
        assert not result.is_opportunity
        assert result.liquidity.status == "not_eligible"

    def test_liquid_counts_pass_the_gate(self):
        snapshot = LiquiditySnapshot(sales_last_7_days=3, sales_last_30_days=12,
                                     source="stockx_page")
        result = classify_opportunity(
            original_price=100.0, discount_percent=40.0, listing_price=400.0,
            liquidity_snapshot=snapshot,
        )
        assert result.is_opportunity


class TestOfficialApiSalesField:
    def test_sales_information_container(self):
        assert _sales_count({"salesInformation": {"salesLast72Hours": 4}}) == 4

    def test_flat_field(self):
        assert _sales_count({"salesLast72Hours": "2"}) == 2

    def test_absent_field_is_none(self):
        assert _sales_count({"lowestAskAmount": "150",
                             "standardMarketData": {"lowestAsk": "150"}}) is None
