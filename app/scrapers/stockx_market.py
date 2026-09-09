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
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from app.scrapers.browser import BrowserSession

logger = logging.getLogger(__name__)

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)

# Keys the sales-history payloads use for a sale's price / timestamp / size —
# covers both the REST activity endpoint (amount/createdAt/shoeSize) and the
# GraphQL salesHistory nodes seen in community captures. The extractor
# requires an amount-key AND a date-key on the same dict before it treats a
# list as sale events, so ask/bid ladders can't be misread as sales.
_SALE_AMOUNT_KEYS = ("amount", "localAmount", "salePrice", "price")
_SALE_DATE_KEYS = ("createdAt", "saleDate", "soldAt", "timestamp")
_SALE_SIZE_KEYS = ("shoeSize", "size", "variantValue")


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
    # Realized-sale volume for the whole product (aggregate across sizes —
    # lower fidelity than per-size, mirroring the size="ANY" ask/bid fallback).
    # None = unknown/no source, never a confirmed zero.
    sales_last_7_days: Optional[int] = None
    sales_last_30_days: Optional[int] = None
    # Itemized events ({"price", "sale_date", "size"}, newest-first) when the
    # source exposed them — persisted to sale_records for 30-day tracking.
    sales_events: Optional[List[dict]] = None


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
            sizes, sales_events = self._scrape_product_page(page, url_key)

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

            product = StockXProduct(
                sku=sku, name=title, url_key=url_key,
                stockx_url=stockx_url, sizes=sizes,
            )
            if sales_events is not None:
                c7, c30 = _count_sales(sales_events)
                product.sales_last_7_days = c7
                product.sales_last_30_days = c30
                product.sales_events = sales_events
                logger.info(
                    f"StockX: {sku} — {c7} sale(s)/7d, {c30}/30d from the product-page "
                    f"sales history (aggregate across sizes — lower fidelity than per-size)"
                )
            return product
        finally:
            page.close()

    def get_sales_last_7_days(self, url_key: str = "", size: str = "") -> Optional[int]:
        """Count realized sales in the last 7 days from the product page's
        intercepted sales-history feed. None (never 0) when the page is
        blocked or no sales payload was captured."""
        counts = self._sales_counts(url_key)
        return counts[0] if counts else None

    def get_sales_last_30_days(self, url_key: str = "", size: str = "") -> Optional[int]:
        """30-day companion of get_sales_last_7_days — same source, same
        None-means-unknown contract."""
        counts = self._sales_counts(url_key)
        return counts[1] if counts else None

    def _sales_counts(self, url_key: str) -> "Optional[Tuple[int, int]]":
        if not url_key:
            return None
        page = self._session.new_page()
        try:
            _sizes, events = self._scrape_product_page(page, url_key)
        finally:
            page.close()
        return _count_sales(events) if events is not None else None

    # ── internals ────────────────────────────────────────────────────────────

    def _search(self, page, sku: str) -> Optional[dict]:
        # domcontentloaded, not networkidle: live-verified 2026-07-31 that
        # StockX strips __NEXT_DATA__ from the DOM once client-side hydration
        # finishes, so waiting for the network to fully quiet down reads the
        # page too late — the search page's data is only there right after
        # the initial HTML lands.
        page.goto(f"https://stockx.com/search?s={sku}", wait_until="domcontentloaded",
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

    def _scrape_product_page(self, page, url_key: str) \
            -> "Tuple[List[StockXSizeMarket], Optional[List[dict]]]":
        """Best-effort: navigate to the product page once and harvest BOTH
        per-size market data and the sales-history feed from intercepted
        XHR/fetch responses (GraphQL plus the /api/products/... activity
        endpoint that populates the sales chart/'View All Sales' modal —
        interception beats scraping rendered chart pixels).

        Returns ([], None) — never raises — when the page is Cloudflare-
        blocked or nothing recognizable was captured: [] means no size
        ladder (caller falls back to aggregate pricing), None means sales
        volume is UNKNOWN, not zero."""
        captured_bodies: List[dict] = []

        def _on_response(response):
            url = response.url
            if "api/graphql" not in url and "/api/products/" not in url:
                return
            # The REST activity endpoint serves asks/bids with the SAME
            # amount+createdAt shape as sales — only state=480 is completed
            # sales, so drop other activity states at capture time.
            if "/api/products/" in url and "activity" in url and "state=480" not in url:
                return
            try:
                body = response.json()
            except Exception:
                return
            if isinstance(body, (dict, list)):
                captured_bodies.append(body if isinstance(body, dict) else {"data": body})

        page.on("response", _on_response)
        try:
            # domcontentloaded for the same reason as _search — but the sizes/
            # sales data here comes purely from intercepted XHR/GraphQL
            # responses (never read from the DOM), which fire AFTER this
            # event. So give them a bounded window to land: best-effort wait
            # for networkidle, but don't fail the scrape if it never quiets
            # down (StockX keeps some connections — analytics/polling — alive
            # indefinitely, which is what made the old networkidle-as-the-nav-
            # wait approach fragile).
            resp = page.goto(f"https://stockx.com/{url_key}",
                              wait_until="domcontentloaded",
                              timeout=self._session.nav_timeout_ms)
            if resp and resp.status == 403:
                logger.info(f"StockX: product page for {url_key} blocked (HTTP 403 — "
                            f"likely Cloudflare challenge, needs a working proxy)")
                return [], None
            if "just a moment" in page.title().lower():
                logger.info(f"StockX: product page for {url_key} served a Cloudflare "
                            f"challenge page instead of content")
                return [], None
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass  # best-effort — proceed with whatever XHRs landed by now
        except Exception as exc:
            logger.debug(f"StockX: product page navigation failed for {url_key}: {exc}")
            return [], None
        finally:
            page.remove_listener("response", _on_response)

        sizes = _extract_size_variants(captured_bodies)
        if not sizes:
            logger.debug(f"StockX: product page for {url_key} loaded but no "
                          f"per-size market data was found in captured responses")
        events = _extract_sales_events(captured_bodies)
        if events is None:
            logger.debug(f"StockX: product page for {url_key} exposed no "
                          f"sales-history payload — sales counts stay unknown")
        return sizes, events


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


# ── Sales-history extraction ──────────────────────────────────────────────────

def _parse_sale_dt(raw) -> Optional[datetime]:
    """ISO-8601 string or epoch seconds/millis → naive UTC datetime, or None."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return datetime.utcfromtimestamp(raw / 1000.0 if raw > 1e11 else raw)
        except (OverflowError, OSError, ValueError):
            return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, AttributeError):
        return None


def _first_key(d: dict, keys) -> "Optional[object]":
    for k in keys:
        if d.get(k) is not None:
            return d[k]
    return None


def _extract_sales_events(bodies: List[dict]) -> Optional[List[dict]]:
    """Scan captured response bodies for lists of realized-sale events — dicts
    carrying BOTH an amount-like and a timestamp-like key (so ask/bid ladders
    and variant lists can never be misread as sales). Returns newest-first
    events shaped like alias.get_recent_sales' output
    ({"price", "sale_date", "size"}), or None when no sales payload was seen
    anywhere — None is "unknown", an empty list is a genuine zero."""
    events: List[dict] = []
    seen_sales_payload = False

    def _is_sale_like(d: dict) -> bool:
        return (_first_key(d, _SALE_AMOUNT_KEYS) is not None
                and _first_key(d, _SALE_DATE_KEYS) is not None)

    def _walk(obj):
        nonlocal seen_sales_payload
        if isinstance(obj, list):
            for item in obj:
                _walk(item)
        elif isinstance(obj, dict):
            if _is_sale_like(obj):
                # A sale event itself — record it, don't recurse into it
                # (nothing nested inside one is another sale).
                seen_sales_payload = True
                try:
                    price = float(_first_key(obj, _SALE_AMOUNT_KEYS))
                except (TypeError, ValueError):
                    price = None
                size = _first_key(obj, _SALE_SIZE_KEYS)
                events.append({
                    "price": price,
                    "sale_date": _parse_sale_dt(_first_key(obj, _SALE_DATE_KEYS)),
                    "size": str(size) if size is not None else None,
                })
                return
            # A ProductActivity/salesHistory container that answered with an
            # explicitly empty list is a REAL zero, not an unknown.
            for key in ("ProductActivity", "salesHistory", "activities", "sales"):
                if isinstance(obj.get(key), list) and not obj[key]:
                    seen_sales_payload = True
            for v in obj.values():
                _walk(v)

    for body in bodies:
        _walk(body)
    if not seen_sales_payload:
        return None
    events.sort(key=lambda e: e["sale_date"] or datetime.max, reverse=True)
    return events


def _count_sales(events: List[dict], now: Optional[datetime] = None) -> "Tuple[int, int]":
    """(sales_last_7_days, sales_last_30_days), inclusive at the window edge.
    Newest-first with early break past the 30-day cutoff (alias.py pattern);
    dateless events count in both windows — same conservative-inclusive
    treatment as liquidity.summarize_sales_events."""
    now = now or datetime.utcnow()
    c7 = c30 = 0
    for ev in events:
        dt = ev.get("sale_date")
        if dt is None:
            c7 += 1
            c30 += 1
            continue
        age = now - dt
        if age > timedelta(days=30):
            break   # newest-first: everything after this is older still
        c30 += 1
        if age <= timedelta(days=7):
            c7 += 1
    return c7, c30
