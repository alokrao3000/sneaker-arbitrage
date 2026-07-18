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

import random
import time

from app import runtime
from app.scrapers.base import (
    ScrapedProduct, ScrapedSize, ScraperStats, CartValidationResult,
    is_valid_sku, extract_base_sku, normalize_size, async_rate_limit,
    CART_OK, CART_OUT_OF_STOCK, CART_SIZE_UNAVAILABLE, CART_REJECTED,
    CART_QUANTITY_LIMIT, CART_PRODUCT_UNAVAILABLE, CART_RATE_LIMITED,
    CART_NETWORK_ERROR, CART_UNKNOWN_RESPONSE,
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
# Kept low: /cart/add.json is the most aggressively throttled endpoint on
# Shopify stores, and probe 429s burn the same budget the per-size opportunity
# validations need later in the run.
_CART_CHECK_CONCURRENCY = 4


class ShopifyScraper:
    supports_cart_validation = True

    def __init__(self, supplier_url: str):
        parsed = urlparse(supplier_url)
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"
        self.supplier_url = supplier_url
        self.stats = ScraperStats()
        self._cart_client: Optional[httpx.Client] = None   # lazy; reused across validations

    def close(self):
        if self._cart_client is not None:
            self._cart_client.close()
            self._cart_client = None

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
            while not runtime.shutdown_event.is_set():
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
        """Fetch a single /products.json page. Transient failures (429, 5xx,
        network) are retried with backoff + jitter; permanent client errors
        are not. Returns product list or [] when the page can't be fetched."""
        max_attempts = 3
        for attempt in range(max_attempts):
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
                if resp.status_code == 200:
                    return resp.json().get("products", [])
                if resp.status_code == 429 or resp.status_code >= 500:
                    if attempt < max_attempts - 1 and not runtime.shutdown_event.is_set():
                        self.stats.http_retries += 1
                        retry_after = resp.headers.get("Retry-After")
                        try:
                            delay = min(float(retry_after), 30.0) if retry_after else 0.0
                        except ValueError:
                            delay = 0.0
                        delay = max(delay, (2 ** attempt) * (0.5 + random.random()))
                        logger.info(f"{self.base_url} page {page} — HTTP {resp.status_code}, "
                                    f"retry {attempt + 1}/{max_attempts - 1} in {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue
                logger.warning(f"{self.base_url} page {page} — HTTP {resp.status_code}")
                return []
            except (httpx.RequestError, ValueError) as exc:
                if attempt < max_attempts - 1 and not runtime.shutdown_event.is_set():
                    self.stats.http_retries += 1
                    await asyncio.sleep((2 ** attempt) * (0.5 + random.random()))
                    continue
                logger.error(f"{self.base_url} scrape error on page {page}: {exc}")
                return []
        return []

    # ── Cart availability verification ───────────────────────────────────────

    async def _check_cartable(self, client: httpx.AsyncClient, variant_id: str) -> str:
        """
        POST to Shopify's cart endpoint to probe whether a variant can be
        purchased right now. Returns:
          "ok"           — the store accepted the cart addition
          "blocked"      — definitive "cannot buy" (422 pre-drop/notify-me, 404)
          "inconclusive" — rate limit (429), server error, or network failure;
                           NOT a stock verdict (stores throttle this endpoint
                           hard — live runs showed hundreds of 429s that used
                           to silently zero real inventory)
        """
        try:
            resp = await client.post(
                f"{self.base_url}/cart/add.json",
                json={"items": [{"id": int(variant_id), "quantity": 1}]},
                headers={**HEADERS, "Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                return "ok"
            if resp.status_code in (422, 404):
                return "blocked"
            return "inconclusive"
        except (httpx.RequestError, ValueError):
            return "inconclusive"

    async def _filter_cartable(
        self, client: httpx.AsyncClient, products: List[ScrapedProduct]
    ) -> List[ScrapedProduct]:
        """
        For each product that has at least one available size, probe one variant
        against the cart. Definitively blocked products (pre-drop, notify-me)
        have all sizes marked not-in-stock; every product records its probe
        outcome in .cart_probe so downstream confidence levels are honest.

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
                outcome = await self._check_cartable(client, available[0].variant_id)
            product.cart_probe = outcome
            self.stats.cart_probes += 1
            if outcome == "blocked":
                self.stats.cart_probe_blocked += 1
                logger.debug(f"Cart blocked for '{product.name}' — marking as unavailable")
                for s in product.sizes:
                    s.in_stock = False
            elif outcome == "inconclusive":
                self.stats.cart_probe_inconclusive += 1

        await asyncio.gather(*[_probe(p) for p in to_check])
        return products

    # ── Per-size cart validation (called at opportunity time) ────────────────

    def validate_cart(self, variant_id: str, quantity: int = 1) -> CartValidationResult:
        """End-to-end add-to-cart for one specific size variant, using the
        store's normal purchase flow (POST /cart/add.json). Synchronous —
        called from the pipeline thread for opportunity candidates only, so
        the volume is a handful of requests per run, not one per product.

        Classifies the retailer's response; 429/network failures are reported
        as inconclusive, never as an out-of-stock verdict."""
        if not variant_id:
            return CartValidationResult(ok=False, reason=CART_PRODUCT_UNAVAILABLE,
                                        message="no variant id captured for this size")
        if self._cart_client is None:
            self._cart_client = httpx.Client(headers=HEADERS, timeout=15,
                                             follow_redirects=True)
        start = time.perf_counter()
        resp = None
        for attempt in range(3):
            try:
                resp = self._cart_client.post(
                    f"{self.base_url}/cart/add.json",
                    json={"items": [{"id": int(variant_id), "quantity": quantity}]},
                    headers={**HEADERS, "Content-Type": "application/json"},
                )
            except (httpx.RequestError, ValueError) as exc:
                if attempt < 2 and not runtime.shutdown_event.is_set():
                    time.sleep((2 ** attempt) * (0.5 + random.random()))
                    continue
                return CartValidationResult(ok=False, reason=CART_NETWORK_ERROR,
                                            message=repr(exc)[:300])
            # 429/5xx: honor Retry-After and try again — a throttle blip must
            # not decide an availability verdict.
            if (resp.status_code == 429 or resp.status_code >= 500) \
                    and attempt < 2 and not runtime.shutdown_event.is_set():
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = min(float(retry_after), 10.0) if retry_after else 0.0
                except ValueError:
                    delay = 0.0
                delay = max(delay, (2 ** attempt) * (1.0 + random.random()))
                time.sleep(delay)
                continue
            break
        elapsed_ms = int((time.perf_counter() - start) * 1000)

        if resp.status_code == 200:
            token = resp.cookies.get("cart") or self._cart_client.cookies.get("cart")
            confirmed_qty = None
            try:
                items = resp.json().get("items") or []
                if items:
                    confirmed_qty = items[0].get("quantity")
                    token = token or items[0].get("key")
            except ValueError:
                pass
            return CartValidationResult(ok=True, reason=CART_OK, cart_token=token,
                                        quantity=confirmed_qty, elapsed_ms=elapsed_ms)

        detail = ""
        try:
            body = resp.json()
            detail = str(body.get("description") or body.get("message") or body)[:300]
        except ValueError:
            detail = resp.text[:300]
        low = detail.lower()

        if resp.status_code == 422:
            if "sold out" in low or "out of stock" in low:
                reason = CART_OUT_OF_STOCK
            elif "unavailable" in low or "not available" in low:
                reason = CART_SIZE_UNAVAILABLE
            elif "maximum" in low or "limit" in low or "quantity" in low:
                reason = CART_QUANTITY_LIMIT
            else:
                reason = CART_REJECTED
        elif resp.status_code == 404:
            reason = CART_PRODUCT_UNAVAILABLE
        elif resp.status_code in (401, 403):
            reason = CART_SESSION_EXPIRED
        elif resp.status_code == 429 or resp.status_code >= 500:
            reason = CART_RATE_LIMITED if resp.status_code == 429 else CART_NETWORK_ERROR
        else:
            reason = CART_UNKNOWN_RESPONSE
        return CartValidationResult(ok=False, reason=reason,
                                    message=f"HTTP {resp.status_code}: {detail}",
                                    elapsed_ms=elapsed_ms)

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
        self.stats.discovered += 1
        if not self._is_sneaker(raw):
            return None
        self.stats.sneaker_matched += 1

        variants = raw.get("variants", [])
        if not variants:
            self.stats.size_parse_failed += 1
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
                self.stats.sku_parse_failed += 1
                logger.debug(f"No brand SKU found for '{raw.get('title', '')}' — skipping")
                return None

        if not sizes:
            self.stats.size_parse_failed += 1
        if base_price <= 0:
            self.stats.price_parse_failed += 1
        self.stats.parsed += 1

        handle = raw.get("handle", "")
        images = raw.get("images") or []
        image_url = images[0].get("src") if images and isinstance(images[0], dict) else None
        return ScrapedProduct(
            name=raw.get("title", ""),
            sku=base_sku.upper(),
            url=f"{self.base_url}/products/{handle}",
            original_price=base_price,
            sizes=sizes,
            published_at=_parse_shopify_dt(raw.get("published_at")),
            image_url=image_url,
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
