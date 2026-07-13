"""
StockX scraper — two implementations:

StockXKicksClient  — uses kicks.dev API (primary; avoids 403 anti-bot blocks).
StockXScraper      — parses stockx.com HTML via __NEXT_DATA__ (fallback; often 403'd).
"""
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


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


class StockXScraper:
    def __init__(self):
        self._http = httpx.Client(
            headers=_HEADERS,
            timeout=30,
            follow_redirects=True,
        )

    def close(self):
        self._http.close()

    # ── Public interface ──────────────────────────────────────────────────────

    def get_product(self, sku: str) -> Optional[StockXProduct]:
        """Search StockX for a product by brand SKU and return market data for all sizes."""
        url_key = self._search_for_sku(sku)
        if not url_key:
            return None
        time.sleep(0.8)
        return self._fetch_product_detail(sku, url_key)

    def get_sales_last_7_days(self, *args, **kwargs) -> int:
        return 0  # not exposed in server-rendered pages

    # ── Private ───────────────────────────────────────────────────────────────

    def _search_for_sku(self, sku: str) -> Optional[str]:
        """Search stockx.com and return the urlKey of the best-matching product."""
        try:
            resp = self._http.get("https://stockx.com/search", params={"s": sku})
        except httpx.RequestError as exc:
            logger.warning(f"StockX search request error for {sku}: {exc}")
            return None

        if resp.status_code != 200:
            logger.warning(f"StockX search HTTP {resp.status_code} for {sku}")
            return None

        data = _extract_next_data(resp.text)
        if not data:
            logger.warning(f"StockX: no __NEXT_DATA__ in search page for {sku}")
            return None

        products = _find_search_products(data)
        if not products:
            logger.info(f"StockX: no products in search results for {sku}")
            return None

        sku_norm = _norm(sku)

        # Pass 1 — exact styleId match (normalised)
        for p in products:
            style = _norm(p.get("styleId") or p.get("style_id") or "")
            if style and style == sku_norm:
                url_key = p.get("urlKey") or p.get("url_key") or ""
                if url_key:
                    logger.debug(f"StockX: styleId match {sku} → {url_key}")
                    return url_key

        # Pass 2 — SKU appears in the product slug
        for p in products:
            url_key = p.get("urlKey") or p.get("url_key") or ""
            if sku_norm in _norm(url_key):
                logger.debug(f"StockX: slug match {sku} → {url_key}")
                return url_key

        logger.info(
            f"StockX: no SKU match in {len(products)} search results for {sku} "
            f"(first styleId={products[0].get('styleId')!r})"
        )
        return None

    def _fetch_product_detail(self, sku: str, url_key: str) -> Optional[StockXProduct]:
        """Fetch the product page and extract per-size market data."""
        url = f"https://stockx.com/{url_key}"
        try:
            resp = self._http.get(url)
        except httpx.RequestError as exc:
            logger.warning(f"StockX product page request error for {url_key}: {exc}")
            return None

        if resp.status_code != 200:
            logger.warning(f"StockX product page HTTP {resp.status_code} for {url_key}")
            return None

        data = _extract_next_data(resp.text)
        if not data:
            logger.warning(f"StockX: no __NEXT_DATA__ in product page for {url_key}")
            return None

        product_data = _find_product_data(data)
        if not product_data:
            logger.warning(f"StockX: could not locate product data for {url_key}")
            return None

        name = (
            product_data.get("title") or
            product_data.get("name") or
            product_data.get("shortDescription") or
            url_key
        )
        sizes = _parse_variants(product_data)

        if not sizes:
            logger.debug(f"StockX: no variant price data for {url_key}")

        return StockXProduct(
            sku=sku,
            name=name,
            url_key=url_key,
            stockx_url=url,
            sizes=sizes,
        )


# ── HTML / JSON parsing helpers ───────────────────────────────────────────────

def _extract_next_data(html: str) -> Optional[Dict]:
    """Pull the __NEXT_DATA__ JSON blob out of the page HTML."""
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def _find_search_products(data: Dict) -> List[Dict]:
    """Navigate __NEXT_DATA__ to the list of search-result product nodes."""
    try:
        page_props = data["props"]["pageProps"]
        for key in ("results", "searchResults", "initialSearchResults"):
            results = page_props.get(key)
            if results and isinstance(results, dict):
                edges = results.get("edges", [])
                nodes = [e["node"] for e in edges if "node" in e]
                if nodes:
                    return nodes
    except (KeyError, TypeError):
        pass

    # Fallback: recursive hunt for any "edges" list in the tree
    return _deep_find_edges(data)


def _find_product_data(data: Dict) -> Optional[Dict]:
    """Navigate __NEXT_DATA__ to the main product object on a product page."""
    try:
        page_props = data["props"]["pageProps"]
        for key in ("product", "productData", "productDetails"):
            obj = page_props.get(key)
            if obj and isinstance(obj, dict):
                return obj
    except (KeyError, TypeError):
        pass

    # Some Next.js versions nest data under "queries" or "dehydratedState"
    try:
        queries = data["props"]["pageProps"]["queries"]
        for q in (queries if isinstance(queries, list) else []):
            inner = (q.get("result") or q.get("data") or {})
            for key in ("product", "data"):
                obj = inner.get(key)
                if obj and isinstance(obj, dict):
                    return obj
    except (KeyError, TypeError, AttributeError):
        pass

    try:
        # React Query dehydrated state
        state = data["props"]["pageProps"]["dehydratedState"]
        for query in state.get("queries", []):
            obj = (query.get("state") or {}).get("data") or {}
            product = obj.get("product") or (obj.get("data") or {}).get("product")
            if product and isinstance(product, dict):
                return product
    except (KeyError, TypeError, AttributeError):
        pass

    return None


def _parse_variants(product: Dict) -> List[StockXSizeMarket]:
    """Extract per-size lowest ask / highest bid / last sale from a product dict."""
    raw_variants = product.get("variants", [])
    if not isinstance(raw_variants, list):
        return []

    sizes: List[StockXSizeMarket] = []
    for v in raw_variants:
        # Size field lives under traits on StockX
        traits = v.get("traits") or {}
        size = str(
            traits.get("size") or v.get("size") or v.get("shoeSize") or ""
        ).strip()
        if not size:
            continue

        market = v.get("market") or {}
        bid_ask = market.get("bidAskData") or market

        lowest_ask = _to_float(
            bid_ask.get("lowestAsk") or bid_ask.get("lowest_ask") or market.get("lowestAsk")
        )
        highest_bid = _to_float(
            bid_ask.get("highestBid") or bid_ask.get("highest_bid") or market.get("highestBid")
        )
        last_sale = _to_float(
            (market.get("statistics") or {}).get("lastSale", {}).get("amount")
            or bid_ask.get("lastSale")
            or market.get("lastSale")
        )

        if lowest_ask is not None or last_sale is not None:
            sizes.append(StockXSizeMarket(
                size=size,
                lowest_ask=lowest_ask,
                highest_bid=highest_bid,
                last_sale=last_sale,
            ))

    return sizes


def _deep_find_edges(obj: Any, depth: int = 0) -> List[Dict]:
    """Recursively search a nested structure for any list of edge-nodes."""
    if depth > 8:
        return []
    if isinstance(obj, dict):
        if "edges" in obj and isinstance(obj["edges"], list):
            nodes = [e.get("node") for e in obj["edges"] if "node" in e]
            if nodes:
                return nodes
        for v in obj.values():
            found = _deep_find_edges(v, depth + 1)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _deep_find_edges(item, depth + 1)
            if found:
                return found
    return []


def _norm(s: str) -> str:
    return s.upper().replace("-", "").replace(" ", "")


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ── kicks.dev-backed StockX client ───────────────────────────────────────────

class StockXKicksClient:
    """
    Fetches StockX market data via the kicks.dev API instead of scraping
    stockx.com directly (which returns 403 anti-bot responses).

    Implements the same .get_product() / .get_sales_last_7_days() / .close()
    interface as StockXScraper so it can be used as a drop-in replacement.
    """

    def __init__(self, kicks):
        # kicks is a KicksDevClient; typed as Any to avoid circular imports
        self._kicks = kicks

    def get_product(self, sku: str, name: str = "") -> Optional[StockXProduct]:
        from app.scrapers.kicks_dev import parse_variants  # local import avoids circular dep

        summary = self._kicks.search("stockx", sku)
        # Fall back to name-based search when SKU lookup returns irrelevant results.
        if not summary and name:
            summary = self._kicks.search_by_name("stockx", sku, name)
        if not summary:
            return None

        slug = (
            summary.get("slug") or
            summary.get("urlKey") or summary.get("url_key") or
            str(summary.get("id") or "")
        )
        if not slug:
            return None

        detail = self._kicks.get_product("stockx", slug) or summary

        name = (
            detail.get("title") or detail.get("name") or
            summary.get("title") or summary.get("name") or sku
        )
        url_key = summary.get("slug") or summary.get("urlKey") or slug

        sizes = [
            StockXSizeMarket(
                size=v["size"],
                lowest_ask=v["lowest_ask"],
                highest_bid=v["highest_bid"],
                last_sale=v["last_sale"],
            )
            for v in parse_variants(detail)
            if v["lowest_ask"] is not None or v["last_sale"] is not None
        ]

        return StockXProduct(
            sku=sku,
            name=name,
            url_key=url_key,
            stockx_url=f"https://stockx.com/{url_key}",
            sizes=sizes,
        )

    def get_sales_last_7_days(self, url_key: str = "", size: str = "") -> int:
        return 0

    def close(self):
        pass  # lifecycle owned by the KicksDevClient caller
