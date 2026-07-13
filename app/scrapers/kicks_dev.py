"""
kicks.dev API client — aggregates StockX, GOAT, and other market data.

Sign up for a free API key at https://kicks.dev/
Set KICKS_DEV_API_KEY in your .env file.

Endpoints used:
  GET /v3/{platform}/products?q={sku}        → search by SKU
  GET /v3/{platform}/products/{slug}          → product detail with variants
  GET /v3/{platform}/products/{slug}/sales/daily → daily sales aggregates

Rate limits: 640 req/min (standard), 60 req/min (real-time).
Free tier: 1,000 requests/month. Starter: 50,000/month at €29.
"""
import time
import logging
from typing import Optional, Dict, List, Any

import httpx

logger = logging.getLogger(__name__)

BASE = "https://api.kicks.dev/v3"


class KicksDevClient:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("KICKS_DEV_API_KEY is required — sign up at https://kicks.dev/")
        self._session = httpx.Client(
            headers={
                "Authorization": api_key,
                "Accept": "application/json",
            },
            timeout=30,
            follow_redirects=True,
        )

    def close(self):
        self._session.close()

    # All field names kicks.dev might use for the brand SKU across API versions
    _SKU_FIELDS = (
        "sku", "style_id", "styleId", "style_code", "styleCode",
        "colorway_id", "colorwayId", "model_number", "modelNumber",
        "brand_sku", "brandSku", "product_id", "productId",
    )

    @classmethod
    def _extract_sku(cls, item: Dict) -> str:
        """Return the first non-empty SKU-like field from an API item."""
        for field in cls._SKU_FIELDS:
            val = item.get(field)
            if val:
                return str(val).upper()
        return ""

    def search(self, platform: str, sku: str) -> Optional[Dict]:
        """
        Search for a product by SKU on the given platform.
        Returns the best-matching product dict, or None.

        Matching order (most to least strict):
          1. Exact case-insensitive match across all known SKU fields
          2. Hyphen/space-normalised match
          3. SKU appears as substring in the product slug or title
          4. Single result — trust the API
        """
        data = self._get_json(f"{BASE}/{platform}/products", params={"q": sku})
        if not data:
            return None

        items = data.get("data", [])
        if not isinstance(items, list) or not items:
            logger.info(f"kicks.dev [{platform}]: no results for SKU {sku}")
            return None

        sku_upper = sku.upper()

        # Pass 1 — exact match across all known SKU field names
        for item in items:
            if self._extract_sku(item) == sku_upper:
                return item

        # Pass 2 — normalise hyphens/spaces
        sku_norm = sku_upper.replace("-", "").replace(" ", "")
        for item in items:
            item_sku_norm = self._extract_sku(item).replace("-", "").replace(" ", "")
            if item_sku_norm == sku_norm:
                logger.debug(f"kicks.dev [{platform}]: hyphen-normalised match for {sku}")
                return item

        # Pass 3 — SKU appears in slug or title (handles "nike-dunk-low-DV0831-101" slugs)
        for item in items:
            slug = str(item.get("slug") or item.get("url_key") or item.get("urlKey") or "").upper()
            title = str(item.get("title") or item.get("name") or "").upper()
            if sku_norm in slug.replace("-", "").replace(" ", "") or \
               sku_norm in title.replace("-", "").replace(" ", ""):
                logger.debug(f"kicks.dev [{platform}]: slug/title match for {sku}")
                return item

        # Pass 4 — single unambiguous result
        if len(items) == 1:
            logger.debug(
                f"kicks.dev [{platform}]: accepting sole result for {sku} "
                f"(sku in response: {self._extract_sku(items[0])})"
            )
            return items[0]

        # Log first item structure to diagnose unexpected API field names
        if items:
            first = items[0]
            logger.info(
                f"kicks.dev [{platform}]: response shape for {sku} — "
                f"keys={list(first.keys())} | "
                f"sku={first.get('sku')!r} style_id={first.get('style_id')!r} "
                f"styleId={first.get('styleId')!r} slug={first.get('slug')!r} "
                f"title={str(first.get('title') or first.get('name') or '')[:60]!r}"
            )

        logger.info(
            f"kicks.dev [{platform}]: no SKU match for {sku} "
            f"({len(items)} results returned — none matched)"
        )
        return None

    def get_product(self, platform: str, slug: str) -> Optional[Dict]:
        """
        Fetch full product detail including per-size variant pricing.
        Returns the 'data' object from the response.
        """
        data = self._get_json(f"{BASE}/{platform}/products/{slug}")
        if not data:
            return None
        inner = data.get("data")
        if isinstance(inner, list):
            return inner[0] if inner else None
        return inner

    def search_by_name(self, platform: str, sku: str, name: str) -> Optional[Dict]:
        """
        Fallback: search by product name and filter results by SKU.
        Used when SKU-based search returns irrelevant results.
        """
        data = self._get_json(f"{BASE}/{platform}/products", params={"q": name})
        if not data:
            return None
        items = data.get("data", [])
        if not isinstance(items, list) or not items:
            return None

        sku_upper = sku.upper()
        sku_norm = sku_upper.replace("-", "").replace(" ", "")

        for item in items:
            if self._extract_sku(item) == sku_upper:
                logger.debug(f"kicks.dev [{platform}]: name-search exact match for {sku}")
                return item
        for item in items:
            if self._extract_sku(item).replace("-", "").replace(" ", "") == sku_norm:
                logger.debug(f"kicks.dev [{platform}]: name-search normalised match for {sku}")
                return item
        for item in items:
            slug_val = str(item.get("slug") or "").upper().replace("-", "").replace(" ", "")
            if sku_norm in slug_val:
                logger.debug(f"kicks.dev [{platform}]: name-search slug match for {sku}")
                return item

        logger.info(
            f"kicks.dev [{platform}]: name-search '{name[:40]}' found no SKU match for {sku} "
            f"({len(items)} results)"
        )
        return None

    def get_sales_daily(self, platform: str, slug: str) -> List[Dict]:
        """
        Fetch daily sales aggregates (avg_price, order_count per day).
        Returns a list of day-records, most recent first, or [] on failure.
        """
        data = self._get_json(f"{BASE}/{platform}/products/{slug}/sales/daily")
        if not data:
            return []
        result = data.get("data", [])
        return result if isinstance(result, list) else []

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def _get_json(self, url: str, params: dict = None) -> Optional[Dict]:
        for attempt in range(3):
            try:
                resp = self._session.get(url, params=params)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 401:
                    logger.error(
                        "kicks.dev: invalid or missing API key — "
                        "set KICKS_DEV_API_KEY in .env (get one at https://kicks.dev/)"
                    )
                    return None
                if resp.status_code == 429:
                    wait = 15 * (attempt + 1)
                    logger.warning(f"kicks.dev rate-limited — waiting {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code == 403:
                    logger.warning("kicks.dev: endpoint may require a higher tier plan")
                    return None
                logger.debug(
                    f"kicks.dev {url} → HTTP {resp.status_code}: {resp.text[:300]}"
                )
                return None
            except httpx.RequestError as exc:
                logger.warning(f"kicks.dev request error: {exc}")
                if attempt < 2:
                    time.sleep(3 * (attempt + 1))

        return None


def parse_variants(product: Dict) -> List[Dict]:
    """
    Extract a flat list of {size, lowest_ask, highest_bid, last_sale, variant_id}
    from a kicks.dev product dict, handling different API shapes.
    """
    variants_raw = product.get("variants", [])
    if not isinstance(variants_raw, list):
        return []

    results = []
    for v in variants_raw:
        # Size: try several field names
        size = str(
            v.get("size") or v.get("shoe_size") or v.get("us_size") or ""
        ).strip()
        if not size:
            continue

        # Prices may be nested under "market" or flat on the variant
        _market_field = v.get("market")
        market = _market_field if isinstance(_market_field, dict) else v
        lowest_ask = _to_float(
            market.get("lowest_ask") or market.get("lowestAsk") or
            market.get("ask") or market.get("price")
        )
        highest_bid = _to_float(
            market.get("highest_bid") or market.get("highestBid") or market.get("bid")
        )
        last_sale = _to_float(
            market.get("last_sale") or market.get("lastSale") or
            market.get("last_sale_price")
        )

        results.append({
            "size": size,
            "lowest_ask": lowest_ask,
            "highest_bid": highest_bid,
            "last_sale": last_sale,
            "variant_id": str(v.get("id") or ""),
        })

    return results


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        # kicks.dev sometimes returns prices in cents for certain plans
        return f if f < 10_000 else f / 100.0
    except (ValueError, TypeError):
        return None
