"""
StockX market data via a self-hosted stealth browser (see app/scrapers/browser.py),
replacing the kicks.dev-backed StockXKicksClient.

Live research this session found:
  - stockx.com/search?s={sku} renders fine (no block) and its server-rendered
    __NEXT_DATA__ (query "getDiscoveryData", flow=SEARCH_RESULTS) already
    contains an urlKey + an AGGREGATE market.state.{lowestAsk,highestBid}
    (best price across all sizes, not per-size).
  - stockx.com/{urlKey} (the product detail page, where StockX's own UI shows
    a per-size ask/bid ladder) is Cloudflare-blocked from this environment's
    IP — a hard 403 "Just a moment..." interstitial that does not auto-resolve
    with wait time. This is the network-reputation risk flagged in the plan,
    now confirmed live rather than theoretical.

Per user decision: ship aggregate-only pricing today (one synthetic size
entry per product, using the same overall ask/bid for every size — lower
fidelity than true per-size data, but functional with no proxy). The
product-page interception path below is still implemented and wired in so
it activates automatically the moment `settings.stockx_proxy_url` points at
a working residential/mobile proxy — no further code changes needed then.
"""
import json
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

from app.scrapers.browser import BrowserSession

logger = logging.getLogger(__name__)

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)


@dataclass
class StockXSizeMarket:
    size: str
    lowest_ask: Optional[float]
    highest_bid: Optional[float]
    last_sale: Optional[float]


@dataclass
class StockXProduct:
    sku: str
    name: str
    url_key: str
    stockx_url: str
    sizes: List[StockXSizeMarket] = field(default_factory=list)


class StockXBrowserClient:
    """Drop-in replacement for StockXKicksClient — same method signatures."""

    def __init__(self, session: BrowserSession):
        self._session = session

    def close(self):
        pass  # session lifecycle managed by the caller (run_full_scrape)

    def get_product(self, sku: str, name: str = "") -> Optional[StockXProduct]:
        page = self._session.new_page()
        try:
            edge = self._search(page, sku)
            if edge is None:
                logger.debug(f"StockX: no search result for SKU {sku}")
                return None

            node = edge["node"]
            url_key = node.get("urlKey")
            title = node.get("title") or node.get("name") or name
            if not url_key:
                logger.debug(f"StockX: search result for {sku} had no urlKey")
                return None

            stockx_url = f"https://stockx.com/{url_key}"
            sizes = self._try_per_size_market(page, url_key)

            if not sizes:
                # Fallback: one synthetic entry from the search page's aggregate
                # market data — same ask/bid applied to every size we don't have
                # a real breakdown for. Explicitly lower fidelity; see module docstring.
                market = node.get("market", {}).get("state", {})
                lowest_ask = _extract_amount(market.get("lowestAsk"))
                highest_bid = _extract_amount(market.get("highestBid"))
                if lowest_ask is None and highest_bid is None:
                    logger.debug(f"StockX: no market data at all for {sku}")
                    return None
                logger.info(
                    f"StockX: using aggregate-only pricing for {sku} "
                    f"(per-size data unavailable — product page likely blocked)"
                )
                sizes = [StockXSizeMarket(
                    size="ANY", lowest_ask=lowest_ask,
                    highest_bid=highest_bid, last_sale=None,
                )]

            return StockXProduct(
                sku=sku, name=title, url_key=url_key,
                stockx_url=stockx_url, sizes=sizes,
            )
        finally:
            page.close()

    def get_sales_last_7_days(self, url_key: str = "", size: str = "") -> Optional[int]:
        # No confirmed data source yet — StockX doesn't expose this on the
        # search page, and the product page (where a sales history chart
        # might expose it) is currently blocked. None = unknown, not zero —
        # see app/services/pricing.py's docstring for how this is handled.
        return None

    # ── internals ────────────────────────────────────────────────────────────

    def _search(self, page, sku: str) -> Optional[dict]:
        page.goto(f"https://stockx.com/search?s={sku}", wait_until="networkidle",
                   timeout=self._session.nav_timeout_ms)
        html = page.content()
        m = _NEXT_DATA_RE.search(html)
        if not m:
            logger.warning("StockX: __NEXT_DATA__ not found on search page")
            return None
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            logger.warning("StockX: failed to parse __NEXT_DATA__ JSON")
            return None

        edges = self._find_search_edges(data)
        return edges[0] if edges else None

    @staticmethod
    def _find_search_edges(data: dict) -> List[dict]:
        try:
            queries = data["props"]["pageProps"]["req"]["appContext"]["states"]["query"]["value"]["queries"]
        except (KeyError, TypeError):
            return []
        for q in queries:
            key = q.get("queryKey") or []
            if key and key[0] == "getDiscoveryData":
                edges = (
                    q.get("state", {}).get("data", {})
                    .get("browse", {}).get("results", {}).get("edges", [])
                )
                if edges:
                    return edges
        return []

    def _try_per_size_market(self, page, url_key: str) -> List[StockXSizeMarket]:
        """Best-effort: navigate to the product page and look for per-size
        market data in intercepted GraphQL responses. Returns [] (not an
        exception) if the page is Cloudflare-blocked or no size breakdown
        is found — caller falls back to aggregate pricing in that case.
        """
        graphql_bodies: List[dict] = []

        def _on_response(response):
            if "api/graphql" not in response.url:
                return
            try:
                graphql_bodies.append(response.json())
            except Exception:
                pass

        page.on("response", _on_response)
        try:
            resp = page.goto(f"https://stockx.com/{url_key}",
                              wait_until="networkidle",
                              timeout=self._session.nav_timeout_ms)
            if resp and resp.status == 403:
                logger.info(f"StockX: product page for {url_key} blocked (HTTP 403 — "
                            f"likely Cloudflare challenge, needs a working proxy)")
                return []
            if "just a moment" in page.title().lower():
                logger.info(f"StockX: product page for {url_key} served a Cloudflare "
                            f"challenge page instead of content")
                return []
        except Exception as exc:
            logger.debug(f"StockX: product page navigation failed for {url_key}: {exc}")
            return []
        finally:
            page.remove_listener("response", _on_response)

        sizes = _extract_size_variants(graphql_bodies)
        if not sizes:
            logger.debug(f"StockX: product page for {url_key} loaded but no "
                          f"per-size market data was found in captured responses")
        return sizes


def _extract_amount(price_obj) -> Optional[float]:
    if isinstance(price_obj, dict):
        val = price_obj.get("amount")
        return float(val) if val is not None else None
    return None


def _extract_size_variants(graphql_bodies: List[dict]) -> List[StockXSizeMarket]:
    """Scan captured GraphQL response bodies for anything shaped like a
    per-size market ladder (a list of dicts, each with a "size" key
    alongside ask/bid-like fields). Deliberately generic since the real
    product-page response shape hasn't been observed yet (blocked in
    testing) — returns [] rather than raising if nothing matches.
    """
    found: List[StockXSizeMarket] = []

    def _walk(obj):
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict) and "size" in item:
                    market = item.get("market") or item
                    lowest_ask = _extract_amount(market.get("lowestAsk"))
                    highest_bid = _extract_amount(market.get("highestBid"))
                    last_sale = _extract_amount(market.get("lastSale"))
                    if lowest_ask is not None or highest_bid is not None:
                        found.append(StockXSizeMarket(
                            size=str(item["size"]), lowest_ask=lowest_ask,
                            highest_bid=highest_bid, last_sale=last_sale,
                        ))
                else:
                    _walk(item)
        elif isinstance(obj, dict):
            for v in obj.values():
                _walk(v)

    for body in graphql_bodies:
        _walk(body)
    return found
