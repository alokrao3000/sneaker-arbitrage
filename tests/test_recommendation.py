"""recommend_platform: StockX wins whenever its sales data is genuinely
known; eBay ask prices / watch counts can only break the tie when sales are
truly unknown, and then only at confidence='low'. eBay evidence is never
treated as sales volume."""
from app.services.pricing import (
    compute_ebay_reference, recommend_platform,
)


class TestStockXAuthoritative:
    def test_known_sales_beats_any_ebay_signal(self):
        platform, conf = recommend_platform(
            stockx_sales_7d=3, stockx_sales_30d=12,
            primary_platform="stockx", primary_margin=40.0,
            ebay_margin=95.0,          # even a much better eBay reference margin
            ebay_watch_count=5000,     # and huge demand signals
            ebay_demand_rank=1,
        )
        assert (platform, conf) == ("stockx", "high")

    def test_single_known_window_is_enough(self):
        platform, conf = recommend_platform(
            stockx_sales_7d=None, stockx_sales_30d=0,   # known (a real zero)
            primary_platform="stockx", primary_margin=40.0,
            ebay_margin=95.0, ebay_watch_count=100,
        )
        assert (platform, conf) == ("stockx", "high")


class TestUnknownFallback:
    def test_ebay_breaks_tie_only_with_demand_and_better_margin(self):
        platform, conf = recommend_platform(
            stockx_sales_7d=None, stockx_sales_30d=None,
            primary_platform="stockx", primary_margin=40.0,
            ebay_margin=60.0, ebay_watch_count=25,
        )
        assert (platform, conf) == ("ebay", "low")

    def test_ask_price_alone_is_not_evidence(self):
        # A better eBay margin with NO demand signal must not flip the
        # recommendation — an ask is just a hope, not a sale.
        platform, conf = recommend_platform(
            stockx_sales_7d=None, stockx_sales_30d=None,
            primary_platform="stockx", primary_margin=40.0,
            ebay_margin=60.0, ebay_watch_count=0, ebay_demand_rank=None,
        )
        assert (platform, conf) == ("stockx", "low")

    def test_demand_without_margin_advantage_stays_primary(self):
        platform, conf = recommend_platform(
            stockx_sales_7d=None, stockx_sales_30d=None,
            primary_platform="stockx", primary_margin=40.0,
            ebay_margin=30.0, ebay_watch_count=500, ebay_demand_rank=3,
        )
        assert (platform, conf) == ("stockx", "low")

    def test_no_ebay_data_at_all(self):
        platform, conf = recommend_platform(
            stockx_sales_7d=None, stockx_sales_30d=None,
            primary_platform="stockx", primary_margin=40.0,
        )
        assert (platform, conf) == ("stockx", "low")


class TestEbayReference:
    def test_reference_is_labeled_active_ask_by_default(self):
        ref = compute_ebay_reference(cost=100.0, price=200.0)
        assert ref.price_type == "active_ask"
        # >= $150 → Authenticity Guarantee flat 8%, no per-order fee
        assert ref.fees == 200.0 * 0.08
        assert ref.payout == 200.0 - 16.0
        assert ref.margin == 84.0

    def test_sold_tier_keeps_its_label(self):
        # Tier-1 sold data runs the same math but must stay labeled sold_avg
        # so the UI can tell a realized average from a hoped-for ask.
        ref = compute_ebay_reference(cost=100.0, price=200.0, price_type="sold_avg")
        assert ref.price_type == "sold_avg"
        assert ref.price == 200.0

    def test_below_auth_threshold_uses_standard_fvf(self):
        ref = compute_ebay_reference(cost=50.0, price=100.0)
        assert abs(ref.fees - (100.0 * 0.1325 + 0.30)) < 1e-9
