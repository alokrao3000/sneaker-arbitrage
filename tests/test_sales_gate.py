"""The hard sales-volume gate: >=1 sale/7d OR >=5 sales/30d, enforced whenever
at least one count is genuinely known; None = unknown never blocks by itself
(the live-bid proxy decides). StockX/Alias are the only count sources — eBay
cannot supply counts and classify_opportunity has no eBay inputs at all."""
import inspect

import pytest

from app.services.pricing import classify_opportunity
from app.services.liquidity import (
    LiquiditySnapshot, evaluate_liquidity,
    LIQUIDITY_ELIGIBLE, LIQUIDITY_NOT_ELIGIBLE, LIQUIDITY_UNKNOWN,
)

# A comfortably profitable setup so only the liquidity gate decides the outcome:
# cost 100 → StockX payout on a $300 ask = 300 - (27 + 9 + 5) = 259, margin 159.
PROFITABLE = dict(original_price=100.0, discount_percent=0.0, listing_price=300.0)


def _classify(**kw):
    return classify_opportunity(**PROFITABLE, **kw)


class TestOrLogic:
    def test_passes_on_7d_alone(self):
        r = _classify(sales_last_7_days=1, sales_last_30_days=0)
        assert r.is_opportunity and r.liquidity.status == LIQUIDITY_ELIGIBLE

    def test_passes_on_30d_alone(self):
        r = _classify(sales_last_7_days=0, sales_last_30_days=5)
        assert r.is_opportunity and r.liquidity.status == LIQUIDITY_ELIGIBLE

    def test_fails_when_both_below(self):
        r = _classify(sales_last_7_days=0, sales_last_30_days=4)
        assert not r.is_opportunity
        assert r.liquidity.status == LIQUIDITY_NOT_ELIGIBLE

    def test_partial_knowledge_30d_only_passes(self):
        r = _classify(sales_last_7_days=None, sales_last_30_days=5)
        assert r.is_opportunity

    def test_partial_knowledge_7d_zero_blocks(self):
        # 7d is genuinely known to be 0 and 30d unknown → the known count
        # fails its threshold and nothing else can rescue it.
        r = _classify(sales_last_7_days=0, sales_last_30_days=None)
        assert not r.is_opportunity
        assert r.liquidity.status == LIQUIDITY_NOT_ELIGIBLE


class TestUnknownIsNotZero:
    def test_unknown_with_live_bid_passes_as_unknown(self):
        r = _classify(sales_last_7_days=None, sales_last_30_days=None,
                      highest_bid=120.0)
        assert r.is_opportunity
        assert r.liquidity.status == LIQUIDITY_UNKNOWN

    def test_unknown_without_bid_fails(self):
        r = _classify(sales_last_7_days=None, sales_last_30_days=None)
        assert not r.is_opportunity
        assert r.liquidity.status == LIQUIDITY_UNKNOWN

    def test_snapshot_wins_over_raw_counts(self):
        snap = LiquiditySnapshot(sales_last_7_days=3, sales_last_30_days=10,
                                 source="alias_live")
        r = _classify(liquidity_snapshot=snap)
        assert r.is_opportunity
        assert r.sales_last_7_days == 3


class TestMarginStillGates:
    def test_liquid_but_unprofitable_is_rejected(self):
        r = classify_opportunity(original_price=100.0, discount_percent=0.0,
                                 listing_price=110.0,
                                 sales_last_7_days=10, sales_last_30_days=40)
        assert r.liquidity.passes
        assert not r.is_opportunity   # margin below threshold


class TestEbayCannotFeedTheGate:
    def test_classify_has_no_ebay_inputs(self):
        """Structural guarantee: no parameter of classify_opportunity accepts
        eBay data, so ask prices / watch counts can never masquerade as sales
        counts. If someone adds one, this fails and forces a design review."""
        params = inspect.signature(classify_opportunity).parameters
        assert not [p for p in params if "ebay" in p.lower()]

    def test_evaluate_liquidity_ignores_extraneous_signals(self):
        # An unknown snapshot stays unknown regardless of any demand context
        # the caller might hold — only a bid (real committed buyer) matters.
        v = evaluate_liquidity(LiquiditySnapshot(source="none"), highest_bid=None)
        assert v.status == LIQUIDITY_UNKNOWN and not v.passes
