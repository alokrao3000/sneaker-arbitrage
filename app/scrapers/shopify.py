"""
Generic Shopify scraper — works for any store that exposes /products.json.
This covers the majority of Tier 0/QS and Shopify-column stores in the CSV.
"""
import asyncio
import logging
import httpx
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urlparse

from app.scrapers.base import (
    ScrapedProduct, ScrapedSize, is_valid_sku, extract_base_sku,
    normalize_size, async_rate_limit,
    _HTML_TAG_RE, _BODY_SKU_RE,
)
from app.config import settings

logger = logging.getLogger(__name__)

SNEAKER_TYPES: frozenset = frozenset({
    # exact product_type values found across Shopify sneaker stores
    "shoes", "footwear", "sneakers", "sneaker", "trainers",
    "boots", "running", "basketball",
    # broader terms some stores use
    "athletic shoes", "athletic footwear", "sports footwear",
    "lifestyle", "lifestyle shoes", "casual shoes",
    "low top sneakers", "high top sneakers",
    "low-top sneakers", "high-top sneakers",
    "running shoes", "training shoes",
    "sports", "sport",
    # some stores (e.g. European) use these
    "schuhe", "chaussures", "scarpe",
})
SNEAKER_TAGS: frozenset = frozenset({
    "sneaker", "sneakers", "shoe", "shoes", "footwear",
    "runner", "trainer", "trainers",
    "nike", "adidas", "jordan", "yeezy", "new balance",
    "puma", "reebok", "asics", "salomon", "saucony",
    "hoka", "on running", "new-balance", "nb",
    "converse", "vans", "timberland", "ugg",
    "under armour", "brooks", "mizuno", "altra",
})

# Partial product_type keywords — checked if exact SNEAKER_TYPES match fails
_TYPE_PARTIALS = ("shoe", "sneaker", "footwear", "trainer", "running")

# Partial tag keywords — checked if SNEAKER_TAGS intersection fails
_TAG_PARTIALS = ("shoe", "sneaker", "footwear")

# Model names / silhouette keywords checked against the product title last
# (most expensive check — only reached if type + tag checks both fail)
_BRAND_HINTS: tuple = (
    # Nike
    "air force", "air max", "dunk", "blazer", "cortez", "react",
    "vapormax", "pegasus", "free run",
    # Jordan
    "jordan", "aj1", "aj4", "aj11",
    # Adidas
    "yeezy", "ultraboost", "nmd", "stan smith", "gazelle",
    "superstar", "forum", "campus", "samba",
    # New Balance
    "new balance", " 574", " 990", " 992", " 993", " 2002",
    " 327", " 550", " 650",
    # Other brands
    "foam runner", "crocs", "vans old skool", "sk8-hi",
    "chuck taylor", "converse",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

_BATCH_SIZE = 15  # pages fetched concurrently per round
_CART_CHECK_CONCURRENCY = 10  # simultaneous cart-add probes


class ShopifyScraper:
    def __init__(self, supplier_url: str):
        parsed = urlparse(supplier_url)
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"
        self.supplier_url = supplier_url

    # ── Public sync entry point ───────────────────────────────────────────────

    def scrape(self, since_dt: Optional[datetime] = None) -> List[ScrapedProduct]:
        """
        Scrape all sneaker products from the store.

        since_dt — if provided, only fetch products updated on or after this
        timestamp (Shopify `updated_at_min`).  Pass the supplier's last scrape
        time for fast incremental runs; omit for a full refresh.

        Pages are fetched in concurrent batches of 5, cutting wall-clock time
        by ~5× compared to a sequential page-by-page crawl.
        """
        return asyncio.run(self._scrape_async(since_dt))

    # ── Async implementation ──────────────────────────────────────────────────

    async def _scrape_async(self, since_dt: Optional[datetime] = None) -> List[ScrapedProduct]:
        results: List[ScrapedProduct] = []

        params: dict = {"limit": 250}
        if since_dt:
            dt_utc = since_dt.replace(tzinfo=timezone.utc) if since_dt.tzinfo is None else since_dt
            params["updated_at_min"] = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.debug(f"{self.base_url} incremental scrape since {params['updated_at_min']}")

        async with httpx.AsyncClient(headers=HEADERS, timeout=30, follow_redirects=True) as client:
            page = 1
            while True:
                # Fetch a batch of pages concurrently
                batch_nums = list(range(page, page + _BATCH_SIZE))
                batch_products = await asyncio.gather(
                    *[self._fetch_page(client, p, params) for p in batch_nums]
                )

                # Process results in page order; stop when a page is empty or partial
                stop = False
                for prods in batch_products:
                    if not prods:
                        stop = True
                        continue   # still process any non-empty pages in this batch
                    for raw in prods:
                        product = self._parse(raw)
                        if product:
                            results.append(product)
                    if len(prods) < 250:
                        stop = True

                if stop:
                    break

                page += _BATCH_SIZE
                # One sleep per batch — not per page
                await async_rate_limit(settings.scrape_delay_min, settings.scrape_delay_max)

            # Drop products whose variants are published but not yet purchasable
            # (pre-drop / notify-me pages where available=true in the API but cart is blocked).
            results = await self._filter_cartable(client, results)

        mode = f"incremental since {params.get('updated_at_min')}" if since_dt else "full"
        logger.info(f"{self.base_url} — found {len(results)} sneaker products ({mode})")
        return results

    async def _fetch_page(
        self, client: httpx.AsyncClient, page: int, params: dict
    ) -> list:
        """Fetch a single /products.json page. Returns product list or [] on any error."""
        try:
            resp = await client.get(
                f"{self.base_url}/products.json",
                params={**params, "page": page},
            )
            if resp.status_code == 404:
                if page == 1:
                    logger.warning(
                        f"{self.base_url} — /products.json returned 404 (not Shopify?)"
                    )
                return []
            if resp.status_code != 200:
                logger.warning(f"{self.base_url} page {page} — HTTP {resp.status_code}")
                return []
            return resp.json().get("products", [])
        except (httpx.RequestError, ValueError) as exc:
            logger.error(f"{self.base_url} scrape error on page {page}: {exc}")
            return []

    # ── Cart availability verification ───────────────────────────────────────

    async def _check_cartable(self, client: httpx.AsyncClient, variant_id: str) -> bool:
        """
        POST to Shopify's cart endpoint to confirm a variant can be purchased right now.
        Returns False for pre-drop / notify-me products where available=True in the API
        but the store hasn't actually opened sales yet.
        """
        try:
            resp = await client.post(
                f"{self.base_url}/cart/add.json",
                json={"items": [{"id": int(variant_id), "quantity": 1}]},
                headers={**HEADERS, "Content-Type": "application/json"},
            )
            return resp.status_code == 200
        except (httpx.RequestError, ValueError):
            return False

    async def _filter_cartable(
        self, client: httpx.AsyncClient, products: List[ScrapedProduct]
    ) -> List[ScrapedProduct]:
        """
        For each product that has at least one available size, verify that a variant
        can actually be added to cart. Products where cart is blocked (pre-drop,
        future release, notify-me) have all sizes marked not-in-stock.

        Checks run concurrently (up to _CART_CHECK_CONCURRENCY at a time) to avoid
        the O(n × delay) wall-clock cost of sequential probing.
        """
        to_check = [
            p for p in products
            if any(s.in_stock and s.variant_id for s in p.sizes)
        ]
        if not to_check:
            return products

        sem = asyncio.Semaphore(_CART_CHECK_CONCURRENCY)

        async def _probe(product: ScrapedProduct) -> None:
            available = [s for s in product.sizes if s.in_stock and s.variant_id]
            async with sem:
                cartable = await self._check_cartable(client, available[0].variant_id)
            if not cartable:
                logger.debug(f"Cart blocked for '{product.name}' — marking as unavailable")
                for s in product.sizes:
                    s.in_stock = False

        await asyncio.gather(*[_probe(p) for p in to_check])
        return products

    # ── Product classification ────────────────────────────────────────────────

    def _is_sneaker(self, raw: dict) -> bool:
        # Check cheapest signals first so we exit early in the common case
        ptype = raw.get("product_type", "").lower().strip()
        if ptype in SNEAKER_TYPES:
            return True
        if any(st in ptype for st in _TYPE_PARTIALS):
            return True

        tags = {t.lower().strip() for t in raw.get("tags", [])}
        if tags & SNEAKER_TAGS:
            return True
        if any(any(st in t for st in _TAG_PARTIALS) for t in tags):
            return True

        # Title check is last — most expensive (linear scan of _BRAND_HINTS)
        title = raw.get("title", "").lower()
        return any(h in title for h in _BRAND_HINTS)

    # ── Product parsing ───────────────────────────────────────────────────────

    def _parse(self, raw: dict) -> Optional[ScrapedProduct]:
        if not self._is_sneaker(raw):
            return None

        variants = raw.get("variants", [])
        if not variants:
            return None

        # Resolve the size option index once for the whole product (O(options)),
        # not inside the variant loop (which would be O(variants × options)).
        options = raw.get("options", [])
        size_index = self._find_size_option_index(options)

        sizes: List[ScrapedSize] = []
        base_sku: Optional[str] = None
        base_price: float = 0.0

        for v in variants:
            raw_sku = (v.get("sku") or "").strip()

            size_raw = self._size_from_variant(v, size_index)
            size = normalize_size(size_raw) if size_raw else None

            price_str = v.get("price", "0") or "0"
            try:
                price = float(price_str)
            except ValueError:
                price = 0.0

            available = v.get("available", False)
            variant_id = str(v["id"]) if v.get("id") else None

            if raw_sku and base_sku is None:
                candidate = extract_base_sku(raw_sku)
                if candidate:
                    base_sku = candidate
                    base_price = price

            # Capture the first non-zero price even when variant has no SKU,
            # so base_price isn't left at 0 when SKU falls back to body HTML.
            if price > 0 and base_price == 0.0:
                base_price = price

            if size:
                sizes.append(ScrapedSize(
                    size=size, price=price, in_stock=bool(available), variant_id=variant_id
                ))

        if base_sku is None:
            base_sku = self._sku_from_body(raw.get("body_html", ""))
            if base_sku is None:
                logger.debug(f"No brand SKU found for '{raw.get('title', '')}' — skipping")
                return None

        handle = raw.get("handle", "")
        return ScrapedProduct(
            name=raw.get("title", ""),
            sku=base_sku.upper(),
            url=f"{self.base_url}/products/{handle}",
            original_price=base_price,
            sizes=sizes,
            published_at=_parse_shopify_dt(raw.get("published_at")),
        )

    # ── Size extraction helpers ───────────────────────────────────────────────

    @staticmethod
    def _find_size_option_index(options: list) -> Optional[int]:
        """Return the 1-based option slot index whose name indicates size, or None."""
        size_keywords = {"size", "shoe size", "us size", "uk size", "eu size"}
        for i, opt in enumerate(options, start=1):
            if opt.get("name", "").lower() in size_keywords:
                return i
        return None

    @staticmethod
    def _size_from_variant(variant: dict, size_index: Optional[int]) -> Optional[str]:
        """Extract the raw size string using the pre-computed option index."""
        if size_index is not None:
            return variant.get(f"option{size_index}")
        # No labelled size option — pick the first option that contains a digit
        for i in range(1, 4):
            val = variant.get(f"option{i}", "")
            if val and any(c.isdigit() for c in val):
                return val
        return None

    # ── SKU fallback ──────────────────────────────────────────────────────────

    @staticmethod
    def _sku_from_body(html: str) -> Optional[str]:
        """Scan product description HTML for a brand SKU (e.g. 'Style: DV0831-101')."""
        if not html:
            return None
        text = _HTML_TAG_RE.sub(' ', html)
        matches = _BODY_SKU_RE.findall(text)
        return matches[0].upper() if matches else None


def _parse_shopify_dt(raw: Optional[str]) -> Optional[datetime]:
    """Parse a Shopify ISO-8601 timestamp string into a naive UTC datetime."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None
