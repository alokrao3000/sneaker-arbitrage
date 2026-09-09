"""/api/opportunities serializer: eBay numbers must reach the UI with their
provenance label intact — an active-listing average can never be presented as
(or confused with) a sold price, and null sales counts stay null (unknown),
never zero."""
from app.api.routes.opportunities import _opp_to_dict
from app.database import Opportunity


def make_opp(**overrides):
    o = Opportunity(sku="HF9989-100", size="10", shoe_name="AJ4 SNS",
                    listing_platform="stockx", listing_price=250.0,
                    payout_price=220.0, margin=40.0, roi=25.0)
    for key, val in overrides.items():
        setattr(o, key, val)
    return o


class TestEbayLabeling:
    def test_active_listing_tier_is_labeled_and_counts_stay_null(self):
        d = _opp_to_dict(make_opp(
            ebay_price=180.0, ebay_price_type="active_ask",
            ebay_active_listings=40,
        ))
        assert d["ebay_price"] == 180.0
        assert d["ebay_price_type"] == "active_ask"      # a hope, not a sale
        assert d["ebay_active_listings"] == 40           # supply signal, still surfaced
        # unknown ≠ zero: no sold source means null counts
        assert d["ebay_sales_count_7d"] is None
        assert d["ebay_sales_count_30d"] is None

    def test_sold_tier_keeps_its_label_and_counts(self):
        d = _opp_to_dict(make_opp(
            ebay_price=195.0, ebay_price_type="sold_avg",
            ebay_sales_count_7d=3, ebay_sales_count_30d=11,
            ebay_active_listings=17,
        ))
        assert d["ebay_price_type"] == "sold_avg"
        assert d["ebay_sales_count_7d"] == 3
        assert d["ebay_sales_count_30d"] == 11
        assert d["ebay_active_listings"] == 17

    def test_no_ebay_data_serializes_as_null_not_zero(self):
        d = _opp_to_dict(make_opp())
        assert d["ebay_price"] is None
        assert d["ebay_price_type"] is None
        assert d["ebay_sales_count_7d"] is None
        assert d["ebay_sales_count_30d"] is None
