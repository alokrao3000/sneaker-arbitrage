"""
eBay Buy-API client — DEMAND PROXIES ONLY, by design.

eBay developer support confirmed (July 2026) that NO public API exposes
market-wide historical sold-item data: Marketplace Insights, even if the
pending application is approved, only covers sales the account itself made.
So this client deliberately does NOT produce sold prices or sales counts.
What it does produce:

  - get_active_listing_stats(): number of live listings + min/median/max
    CURRENT ASKING price via the Browse API. Ask prices are labeled
    price_type='active_ask' end-to-end and must never be fed into
    sales_last_7_days / sales_last_30_days — those fields are reserved for
    genuinely realized sales (StockX/Alias side, see app/services/liquidity.py).
  - get_demand_signal(): watch count (Browse getItem) and merchandised-product
    rank (Marketing API) as SOFT demand indicators, stored separately
    (ebay_watch_count / ebay_demand_rank) and structurally excluded from the
    hard sales gate.
  - resolve_catalog(): SKU → ePID/GTIN via the Catalog API, because Browse and
    Marketing queries are far more reliable by ID than by free-text SKU.

Auth: OAuth2 client-credentials flow
(https://developer.ebay.com/develop/guides-v2/authorization#overview) —
Basic auth of App ID / Cert ID against the token endpoint, token cached in
memory and auto-refreshed before expiry / on 401.

Failure policy (matches the rest of the scrapers): a 403 / insufficient-scope
response disables that endpoint for the process with ONE warning log, and
every public method returns None on any failure — an eBay hiccup never
crashes the batch.

Marketplace Insights: sold_items_search() below is the drop-in point if the
pending application is ever approved — the rest of the pipeline needs no
changes, the method just starts returning data instead of raising.
"""
import base64
import logging
import statistics
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

EBAY_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_BROWSE_ITEM_URL = "https://api.ebay.com/buy/browse/v1/item/{item_id}"
EBAY_CATALOG_SEARCH_URL = "https://api.ebay.com/commerce/catalog/v1/product_summary/search"
EBAY_MERCHANDISED_URL = "https://api.ebay.com/buy/marketing/v1_beta/merchandised_product"

# How many active listings the ask-stats sample reads (one Browse page).
# min/median/max are computed over this sample, not eBay's full book — fine
# for a context signal, documented on EbayListingStats.
BROWSE_SAMPLE_SIZE = 100


def is_configured() -> bool:
    return bool(settings.ebay_app_id and settings.ebay_cert_id)


@dataclass
class EbayCatalogMatch:
    """SKU resolved against the eBay catalog — used to make Browse/Marketing
    queries ID-based instead of free-text."""
    epid: Optional[str] = None
    gtin: Optional[str] = None
    title: Optional[str] = None


@dataclass
class EbayListingStats:
    """Live-listing supply/price context. ALL prices here are CURRENT ASKS
    (price_type='active_ask') from a sample of up to BROWSE_SAMPLE_SIZE
    listings — never realized sale prices, never sales-volume evidence."""
    active_count: int
    min_ask: Optional[float]
    median_ask: Optional[float]
    max_ask: Optional[float]
    price_type: str = "active_ask"
    sample_size: int = 0
    top_item_id: Optional[str] = None   # feeds the watch-count fallback lookup
    # Highest watchCount across the sampled listings. Requested via
    # fieldgroups=EXTENDED, but live-verified 2026-07-18: the basic
    # client-credentials scope returns NO watchCount at all (search or
    # getItem), so this stays None until eBay grants a richer scope. The
    # plumbing is kept so it lights up without code changes if that happens.
    top_watch_count: Optional[int] = None
    # ePID straight from the search response (live-verified present) — makes
    # the Catalog API unnecessary for ID resolution when it is out of scope.
    top_epid: Optional[str] = None


@dataclass
class EbayDemandSignal:
    """Soft demand indicators — display/sort context only, excluded from the
    sales-liquidity gate by construction (nothing here is a sale)."""
    watch_count: Optional[int] = None
    demand_rank: Optional[int] = None   # 1-based rank in merchandised products
    source: str = "none"                # browse_watch_count | merchandised | both | none


class EbayClient:
    """Thread-safe enough for the pipeline's single evaluation thread; the
    token manager holds its own lock in case that ever changes. Pass a
    transport to hit a mock in tests."""

    def __init__(self, app_id: Optional[str] = None, cert_id: Optional[str] = None,
                 transport: Optional[httpx.BaseTransport] = None):
        self._app_id = app_id or settings.ebay_app_id
        self._cert_id = cert_id or settings.ebay_cert_id
        if not (self._app_id and self._cert_id):
            raise ValueError("EBAY_APP_ID / EBAY_CERT_ID not configured")
        self._http = httpx.Client(timeout=httpx.Timeout(20.0, connect=8.0),
                                  transport=transport)
        self._token_lock = threading.Lock()
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        # Endpoints that came back 403/out-of-scope — disabled for the process
        # after one warning, per the module failure policy.
        self._disabled: set = set()

    def close(self):
        self._http.close()

    # ── OAuth2 client-credentials ────────────────────────────────────────────

    def _get_token(self, force_refresh: bool = False) -> Optional[str]:
        with self._token_lock:
            if (not force_refresh and self._access_token and self._token_expires_at
                    and datetime.utcnow() < self._token_expires_at - timedelta(seconds=60)):
                return self._access_token
            basic = base64.b64encode(
                f"{self._app_id}:{self._cert_id}".encode()).decode()
            try:
                resp = self._http.post(
                    EBAY_TOKEN_URL,
                    headers={"Authorization": f"Basic {basic}",
                             "Content-Type": "application/x-www-form-urlencoded"},
                    data={"grant_type": "client_credentials",
                          "scope": settings.ebay_oauth_scope},
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                self._log_disabled("oauth", f"token request failed: {exc!r}")
                return None
            self._access_token = payload.get("access_token")
            self._token_expires_at = datetime.utcnow() + timedelta(
                seconds=int(payload.get("expires_in", 7200)))
            return self._access_token

    # ── HTTP plumbing ────────────────────────────────────────────────────────

    def _log_disabled(self, endpoint: str, detail: str):
        if endpoint not in self._disabled:
            self._disabled.add(endpoint)
            logger.warning(
                f"[ebay] {endpoint} disabled for this process — {detail}. "
                "All calls to it will return None (batch continues)."
            )

    def _get(self, endpoint: str, url: str, params: Optional[dict] = None,
             context: str = "") -> Optional[dict]:
        """GET with bearer auth. Returns parsed JSON, or None on ANY failure.
        403/oauth-scope errors disable the endpoint for the process (logged
        once); other errors log per call at debug/warning level."""
        if endpoint in self._disabled or "oauth" in self._disabled:
            return None
        token = self._get_token()
        if token is None:
            return None
        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": settings.ebay_marketplace_id,
        }
        try:
            resp = self._http.get(url, params=params, headers=headers)
            if resp.status_code == 401:
                # stale token — one forced refresh, then retry once
                token = self._get_token(force_refresh=True)
                if token is None:
                    return None
                headers["Authorization"] = f"Bearer {token}"
                resp = self._http.get(url, params=params, headers=headers)
            if resp.status_code == 403:
                self._log_disabled(endpoint,
                                   f"403 (insufficient scope / not granted): "
                                   f"{resp.text[:200]!r}")
                return None
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning(f"[ebay] {endpoint} request failed [{context}]: {exc!r}")
            return None

    # ── Catalog: SKU → ePID/GTIN ─────────────────────────────────────────────

    def resolve_catalog(self, sku: str, name: str = "") -> Optional[EbayCatalogMatch]:
        """Resolve a manufacturer SKU (style code) to an eBay catalog entry.
        Tries the SKU as a query first, then the scraped name. Returns None
        when the catalog has no match (or the API is out of scope)."""
        for query in filter(None, (sku, name)):
            data = self._get(
                "catalog", EBAY_CATALOG_SEARCH_URL,
                {"q": query, "category_ids": settings.ebay_sneaker_category_id,
                 "limit": 5},
                context=f"sku={sku}",
            )
            summaries = (data or {}).get("productSummaries") or []
            for p in summaries:
                if not isinstance(p, dict):
                    continue
                epid = p.get("epid")
                gtins = p.get("gtins") or p.get("gtin") or []
                gtin = gtins[0] if isinstance(gtins, list) and gtins else (
                    gtins if isinstance(gtins, str) else None)
                if epid or gtin:
                    return EbayCatalogMatch(epid=epid, gtin=gtin, title=p.get("title"))
        return None

    # ── Browse: active-listing ask stats ─────────────────────────────────────

    def get_active_listing_stats(self, sku_or_gtin: str,
                                 gtin: Optional[str] = None,
                                 epid: Optional[str] = None) -> Optional[EbayListingStats]:
        """Supply/price CONTEXT from live listings: count + min/median/max
        current asking price. Prefers ID-based lookups (epid > gtin) over
        free-text SKU search. Never returns sold data — there is none to get."""
        # EXTENDED adds watchCount to each item summary (search only — the
        # getItem endpoint 400s on this fieldgroup, live-verified 2026-07-18).
        params = {"limit": BROWSE_SAMPLE_SIZE,
                  "filter": "conditions:{NEW},priceCurrency:USD",
                  "fieldgroups": "EXTENDED"}
        if epid:
            params["epid"] = epid
        elif gtin:
            params["gtin"] = gtin
        else:
            params["q"] = sku_or_gtin
            params["category_ids"] = settings.ebay_sneaker_category_id

        data = self._get("browse_search", EBAY_BROWSE_SEARCH_URL, params,
                         context=f"query={sku_or_gtin}")
        if data is None:
            return None
        items = data.get("itemSummaries") or []
        prices: List[float] = []
        top_item_id: Optional[str] = None
        top_watch_count: Optional[int] = None
        top_epid: Optional[str] = None
        for it in items:
            if not isinstance(it, dict):
                continue
            if top_item_id is None and it.get("itemId"):
                top_item_id = it["itemId"]
            if top_epid is None and it.get("epid"):
                top_epid = str(it["epid"])
            try:
                wc = int(it.get("watchCount"))
                if top_watch_count is None or wc > top_watch_count:
                    top_watch_count = wc
            except (TypeError, ValueError):
                pass
            try:
                prices.append(float((it.get("price") or {}).get("value")))
            except (TypeError, ValueError):
                continue
        total = data.get("total")
        active_count = int(total) if isinstance(total, (int, float, str)) and str(total).isdigit() \
            else len(items)
        if not prices:
            return EbayListingStats(active_count=active_count, min_ask=None,
                                    median_ask=None, max_ask=None,
                                    sample_size=0, top_item_id=top_item_id,
                                    top_watch_count=top_watch_count,
                                    top_epid=top_epid)
        return EbayListingStats(
            active_count=active_count,
            min_ask=min(prices),
            median_ask=float(statistics.median(prices)),
            max_ask=max(prices),
            sample_size=len(prices),
            top_item_id=top_item_id,
            top_watch_count=top_watch_count,
            top_epid=top_epid,
        )

    # ── Marketing/Browse: soft demand signal ─────────────────────────────────

    def get_demand_signal(self, epid: Optional[str] = None,
                          item_id: Optional[str] = None,
                          watch_count: Optional[int] = None) -> Optional[EbayDemandSignal]:
        """Watch count + merchandised-product rank. Explicitly NOT sales
        volume — callers must keep this out of the sales gate (they do:
        it's persisted to ebay_watch_count/ebay_demand_rank only).

        watch_count normally arrives free with the listing-stats search
        (EbayListingStats.top_watch_count); the per-item lookup is only a
        fallback when a caller has an item_id but no search-derived count."""
        if watch_count is None and item_id:
            watch_count = self._watch_count(item_id)
        demand_rank = self._merchandised_rank(epid) if epid else None
        if watch_count is None and demand_rank is None:
            return None
        source = ("both" if watch_count is not None and demand_rank is not None
                  else "browse_watch_count" if watch_count is not None
                  else "merchandised")
        return EbayDemandSignal(watch_count=watch_count, demand_rank=demand_rank,
                                source=source)

    def _watch_count(self, item_id: str) -> Optional[int]:
        # Plain getItem — no fieldgroups param: EXTENDED is search-only and
        # the item endpoint rejects it with a 400 (live-verified 2026-07-18).
        data = self._get("browse_item",
                         EBAY_BROWSE_ITEM_URL.format(item_id=item_id),
                         None, context=f"item={item_id}")
        if data is None:
            return None
        wc = data.get("watchCount")
        try:
            return int(wc) if wc is not None else None
        except (TypeError, ValueError):
            return None

    def _merchandised_rank(self, epid: str) -> Optional[int]:
        data = self._get(
            "marketing", EBAY_MERCHANDISED_URL,
            {"category_id": settings.ebay_sneaker_category_id,
             "metric_name": "BEST_SELLING", "limit": 100},
            context=f"epid={epid}",
        )
        if data is None:
            return None
        for rank, product in enumerate(data.get("merchandisedProducts") or [], start=1):
            if isinstance(product, dict) and str(product.get("epid")) == str(epid):
                return rank
        return None

    # ── Marketplace Insights — pending-approval stub ─────────────────────────

    def sold_items_search(self, sku_or_gtin: str):
        """RESERVED for the Marketplace Insights API (application pending).

        If access is granted, implement this against
        /buy/marketplace_insights/v1_beta/item_sales/search and have it return
        realized-sale events shaped like app/services/liquidity.py's
        summarize_sales_events() input ({"price", "sale_date", "size"}) — at
        that point sold data may legitimately feed the sales gate and
        ebay_price_type='sold_avg' becomes valid. Until then this must never
        be called: eBay support confirmed the grant would still only cover
        OUR OWN sales, so even an approved key may not be market-wide.
        """
        raise NotImplementedError(
            "eBay Marketplace Insights access not granted — no public API "
            "exposes market-wide sold-item data (confirmed by eBay dev support, "
            "July 2026). Do not fabricate sold prices from active listings."
        )
