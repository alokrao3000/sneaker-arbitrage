"""
GOAT market data via a self-hosted stealth browser (see app/scrapers/browser.py),
replacing the kicks.dev-backed GoatScraper.

No ALIAS_API_KEY exists (user has no Alias developer account), so GOAT is the
sole source for this inventory — not just a URL/fallback-price resolver as
originally scoped. Live research this session found two endpoints:

  - `www.goat.com/search?query={sku}` → client-side fetch to
    `web-api/consumer-search/get-product-search-results`: slug, title,
    numeric product id, and one AGGREGATE lowest price (used to resolve the
    SKU and as a last-resort fallback if the per-size call below fails).

  - `www.goat.com/sneakers/{slug}` → client-side fetch to
    `web-api/v1/product_variants/buy_bar_data?productTemplateId={id}`:
    REAL per-size, per-condition pricing (`lowestPriceCents`, one row per
    (size, shoeCondition, boxCondition) combo) plus each combo's single most
    recent `lastSoldPriceCents` — but no sale date or count. Filtered here to
    `new_no_defects` / `good_condition` (standard retail condition, matching
    what a supplier sells).

Confirmed dead end, not assumed: GOAT's web product page exposes NO sales
history, sale count, or sale date anywhere — checked via full-page scroll
(to trigger any lazy-loaded chart), a raw HTML search for
priceHistory/salesHistory/recentSales, and size-selector interaction. This
means `get_sales_last_7_days()` has no real data source today, same
unresolved gap as StockX — see stockx_market.py's docstring.

Important: this page must be loaded WITHOUT playwright-stealth's patching —
see app/scrapers/browser.py's docstring for why (stealth's navigator overrides
crash GOAT's own bot-detection script, which breaks React hydration before
any request fires). Construct this platform's BrowserSession with
`use_stealth=False`.
"""
import logging
from dataclasses import dataclass, field
from typing import List, Optional

from app.scrapers.browser import BrowserSession, capture_json_response

logger = logging.getLogger(__name__)

_SEARCH_RESULTS_MARKER = "get-product-search-results"
_BUY_BAR_MARKER = "buy_bar_data"

_TARGET_SHOE_CONDITION = "new_no_defects"
_TARGET_BOX_CONDITION = "good_condition"


@dataclass
class GoatSizeMarket:
    size: str
    lowest_ask: Optional[float]
    last_sale: Optional[float]


@dataclass
class GoatProduct:
    sku: str
    name: str
    goat_slug: str
    goat_url: str
    sizes: List[GoatSizeMarket] = field(default_factory=list)


class GoatBrowserClient:
    """Drop-in replacement for the kicks.dev-backed GoatScraper — same method signatures."""

    def __init__(self, session: BrowserSession):
        self._session = session

    def close(self):
        pass  # session lifecycle managed by the caller (run_full_scrape)

    def get_sales_last_7_days(self, slug: str = "", size: str = "") -> Optional[int]:
        # No confirmed data source — GOAT's web product page exposes no sale
        # history/count/date anywhere. None = unknown, not zero — see
        # app/services/pricing.py's docstring for how this is handled.
        return None

    def get_product(self, sku: str, name: str = "") -> Optional[GoatProduct]:
        page = self._session.new_page()
        try:
            search_body = capture_json_response(
                page,
                lambda url: _SEARCH_RESULTS_MARKER in url,
                lambda: page.goto(
                    f"https://www.goat.com/search?query={sku}",
                    wait_until="load",
                    timeout=self._session.nav_timeout_ms,
                ),
                timeout_ms=self._session.nav_timeout_ms,
            )
            if not search_body:
                logger.debug(f"GOAT: no search response captured for SKU {sku}")
                return None

            products = search_body.get("data", {}).get("productsList", [])
            if not products:
                logger.debug(f"GOAT: no results for SKU {sku}")
                return None

            product = products[0]
            slug = product.get("slug")
            if not slug:
                logger.debug(f"GOAT: search result for {sku} had no slug")
                return None

            title = product.get("title") or name
            goat_url = f"https://www.goat.com/sneakers/{slug}"

            sizes = self._fetch_per_size_pricing(page, slug)

            if not sizes:
                # Fallback: the search page's own aggregate lowest price
                aggregate_ask = None
                for variant in product.get("variantsList", []):
                    cents = variant.get("localizedLowestPriceCents", {}).get("amountCents")
                    if cents is not None:
                        aggregate_ask = cents / 100.0
                        break
                if aggregate_ask is None:
                    logger.debug(f"GOAT: no price found for {sku}")
                    return None
                logger.info(f"GOAT: using aggregate-only pricing for {sku} "
                            f"(per-size buy_bar_data unavailable)")
                sizes = [GoatSizeMarket(size="ANY", lowest_ask=aggregate_ask, last_sale=None)]

            return GoatProduct(sku=sku, name=title, goat_slug=slug, goat_url=goat_url, sizes=sizes)
        finally:
            page.close()

    def _fetch_per_size_pricing(self, page, slug: str) -> List[GoatSizeMarket]:
        body = capture_json_response(
            page,
            lambda url: _BUY_BAR_MARKER in url,
            lambda: page.goto(
                f"https://www.goat.com/sneakers/{slug}",
                wait_until="load",
                timeout=self._session.nav_timeout_ms,
            ),
            timeout_ms=self._session.nav_timeout_ms,
        )
        if not body or not isinstance(body, list):
            return []

        sizes: List[GoatSizeMarket] = []
        for entry in body:
            if (entry.get("shoeCondition") != _TARGET_SHOE_CONDITION
                    or entry.get("boxCondition") != _TARGET_BOX_CONDITION):
                continue
            lowest = entry.get("lowestPriceCents", {}).get("amount")
            last_sale = entry.get("lastSoldPriceCents", {}).get("amount")
            if lowest is None and last_sale is None:
                continue
            size = entry.get("sizeOption", {}).get("presentation")
            if not size:
                continue
            sizes.append(GoatSizeMarket(
                size=str(size),
                lowest_ask=lowest / 100.0 if lowest is not None else None,
                last_sale=last_sale / 100.0 if last_sale is not None else None,
            ))
        return sizes
