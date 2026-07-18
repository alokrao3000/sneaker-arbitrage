"""
Footlocker-family scraper — covers Footlocker, Champs Sports, and Eastbay.
All three share the same backend API (Foot Locker Inc. platform).

NOTE ON BOT DETECTION
The Foot Locker family uses Akamai Bot Manager, which issues a JavaScript
challenge and fingerprints TLS/browser behaviour.  A plain httpx request can
still succeed if the initial homepage GET captures the right session cookies
before we hit the API.  If the site has tightened its rules, expect HTTP 403
or 406; in that case the only reliable fix is Playwright + playwright-stealth
(or a residential-proxy service).  The scraper logs a clear message when
blocked so you know exactly what happened.
"""
import uuid
import logging
import httpx
from typing import List, Optional

import random
import time as _time

from app import runtime
from app.scrapers.base import (
    ScrapedProduct, ScrapedSize, ScraperStats, is_valid_sku, normalize_size, rate_limit
)
from app.config import settings

logger = logging.getLogger(__name__)

SITE_CONFIG = {
    "footlocker.com": {
        "api_base": "https://www.footlocker.com/api",
        "site_url": "https://www.footlocker.com",
    },
    "champssports.com": {
        "api_base": "https://www.champssports.com/api",
        "site_url": "https://www.champssports.com",
    },
    "eastbay.com": {
        "api_base": "https://www.eastbay.com/api",
        "site_url": "https://www.eastbay.com",
    },
}


def _api_headers(site_url: str) -> dict:
    """Headers for JSON API calls — mimic a real Chrome XHR request."""
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": site_url,
        "Referer": f"{site_url}/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "x-fl-request-id": str(uuid.uuid4()),
        "x-fl-product-id": "",
    }


def _html_headers(site_url: str) -> dict:
    """Headers for the initial homepage GET — needed to pick up session cookies."""
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }


def _first_image_url(payload: Optional[dict]) -> Optional[str]:
    """Best-effort primary image from a Footlocker search/detail payload.
    The images block varies by banner ({'images': [{'url': ...}]} vs plain
    strings); absent or unrecognized shapes return None rather than guessing."""
    if not isinstance(payload, dict):
        return None
    images = payload.get("images")
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            for key in ("url", "src", "baseUrl"):
                if isinstance(first.get(key), str):
                    return first[key]
        elif isinstance(first, str):
            return first
    return None


class FootlockerScraper:
    PAGE_SIZE = 120
    # Footlocker's cart flow sits behind Akamai bot protection and its size
    # entries carry no cart-addable variant id — per-size cart validation is
    # not supported; inventory confidence caps at VERIFIED_INVENTORY (the
    # detail endpoint's per-size stockLevelStatus).
    supports_cart_validation = False

    def __init__(self, supplier_url: str):
        domain = self._detect_domain(supplier_url)
        cfg = SITE_CONFIG.get(domain, SITE_CONFIG["footlocker.com"])
        self.api_base = cfg["api_base"]
        self.site_url = cfg["site_url"]
        self._domain = domain
        self.stats = ScraperStats()

    def scrape(self) -> List[ScrapedProduct]:
        results: List[ScrapedProduct] = []

        # Use a persistent session so cookies are shared between homepage and API calls
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            # Step 1: visit homepage to collect Akamai / session cookies
            self._init_session(client)

            # Step 2: sweep product pages
            page = 0
            while not runtime.shutdown_event.is_set():
                # Refresh per-request ID each page
                headers = {**_api_headers(self.site_url), "x-fl-request-id": str(uuid.uuid4())}
                params = {
                    "currentPage": page,
                    "pageSize": self.PAGE_SIZE,
                    "sort": "newArrivals",
                    "inStockOnly": "false",
                    "query": "",
                }
                try:
                    resp = None
                    for attempt in range(3):
                        resp = client.get(
                            f"{self.api_base}/products/search",
                            params=params,
                            headers=headers,
                        )
                        # 429/5xx are transient — retry with backoff + jitter.
                        # Akamai 403/406 and other 4xx are permanent for this
                        # session; retrying just digs the hole deeper.
                        if (resp.status_code == 429 or resp.status_code >= 500) \
                                and attempt < 2 and not runtime.shutdown_event.is_set():
                            self.stats.http_retries += 1
                            delay = (2 ** attempt) * (0.5 + random.random())
                            logger.info(f"{self._domain} search HTTP {resp.status_code} — "
                                        f"retrying in {delay:.1f}s")
                            _time.sleep(delay)
                            continue
                        break

                    if resp.status_code in (403, 406):
                        logger.warning(
                            f"{self._domain}: HTTP {resp.status_code} — Akamai is blocking the "
                            "request.  To reliably bypass this, integrate Playwright + "
                            "playwright-stealth or route through a residential proxy."
                        )
                        break

                    if resp.status_code != 200:
                        logger.warning(
                            f"{self._domain} search returned HTTP {resp.status_code} on page {page}"
                        )
                        break

                    data = resp.json()
                    raw_products = data.get("products", [])
                    if not raw_products:
                        break

                    self.stats.discovered += len(raw_products)
                    for raw in raw_products:
                        if runtime.shutdown_event.is_set():
                            break   # each listing costs a throttled detail fetch — bail fast
                        product = self._parse_listing(raw, client)
                        if product:
                            results.append(product)

                    total_pages = data.get("pagination", {}).get("totalPages", 1)
                    page += 1
                    if page >= total_pages:
                        break

                    rate_limit(0.6, 1.2)   # page-level delay; detail calls throttle per product

                except (httpx.RequestError, ValueError) as exc:
                    logger.error(f"{self._domain} scrape error on page {page}: {exc}")
                    break

        logger.info(f"{self._domain} — found {len(results)} products")
        return results

    # ── Session init ──────────────────────────────────────────────────────────

    def _init_session(self, client: httpx.Client):
        """
        Visit the homepage to pick up session/Akamai cookies before hitting the API.
        A failed homepage GET is non-fatal; we still attempt the API.
        """
        try:
            resp = client.get(self.site_url, headers=_html_headers(self.site_url))
            logger.debug(
                f"{self._domain}: session init → HTTP {resp.status_code}, "
                f"{len(client.cookies)} cookie(s) acquired"
            )
            rate_limit(1.5, 3.0)
        except httpx.RequestError as exc:
            logger.debug(f"{self._domain}: homepage init request failed: {exc}")

    # ── Product parsing ───────────────────────────────────────────────────────

    def _parse_listing(self, raw: dict, client: httpx.Client) -> Optional[ScrapedProduct]:
        product_id = raw.get("productId") or raw.get("code", "")
        if not product_id:
            return None

        name = raw.get("name", "")
        price_data = raw.get("price", {})
        price = float(price_data.get("value", 0) or 0)

        detail = self._fetch_detail(product_id, client)
        if detail is None:
            return None

        sku, sizes = self._extract_sku_and_sizes(detail, price)
        if not sku:
            self.stats.sku_parse_failed += 1
            return None
        if not sizes:
            self.stats.size_parse_failed += 1
        if price <= 0:
            self.stats.price_parse_failed += 1
        self.stats.sneaker_matched += 1
        self.stats.parsed += 1

        url = f"{self.site_url.rstrip('/')}/product/~/{product_id}.html"
        return ScrapedProduct(name=name, sku=sku, url=url, original_price=price, sizes=sizes,
                              image_url=_first_image_url(raw) or _first_image_url(detail))

    def _fetch_detail(self, product_id: str, client: httpx.Client) -> Optional[dict]:
        try:
            # Short delay — same session/cookies means the API handles higher rates fine
            rate_limit(0.15, 0.4)
            headers = {**_api_headers(self.site_url), "x-fl-request-id": str(uuid.uuid4())}
            resp = client.get(
                f"{self.api_base}/products/{product_id}",
                headers=headers,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.debug(f"Detail fetch {product_id} → HTTP {resp.status_code}")
        except httpx.RequestError as exc:
            logger.debug(f"Detail fetch {product_id} error: {exc}")
        return None

    @staticmethod
    def _extract_sku_and_sizes(detail: dict, fallback_price: float):
        sku: Optional[str] = None

        for sv in detail.get("styleVariants", []):
            candidate = (sv.get("skuCode") or sv.get("styleColor") or "").strip()
            if is_valid_sku(candidate):
                sku = candidate.upper()
                break

        if not sku:
            for key in ("styleId", "sku", "code"):
                candidate = (detail.get(key) or "").strip()
                if is_valid_sku(candidate):
                    sku = candidate.upper()
                    break

        if not sku:
            return None, []

        sizes: List[ScrapedSize] = []
        for variant_group in detail.get("variants", []):
            if variant_group.get("type", "").upper() != "SIZE":
                continue
            for entry in variant_group.get("entryValues", []):
                raw_size = str(entry.get("value", "")).strip()
                size = normalize_size(raw_size)
                if size is None:
                    continue
                in_stock = entry.get("stockLevelStatus", "").lower() == "instock"
                price = float(
                    entry.get("price", {}).get("value", fallback_price) or fallback_price
                )
                sizes.append(ScrapedSize(size=size, price=price, in_stock=in_stock))

        return sku, sizes

    @staticmethod
    def _detect_domain(url: str) -> str:
        for domain in SITE_CONFIG:
            if domain in url:
                return domain
        return "footlocker.com"
