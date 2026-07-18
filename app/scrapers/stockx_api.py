"""
Official StockX public API client (developer.stockx.com) — the market-data
source used whenever credentials are configured, replacing browser scraping
for StockX (the browser path in stockx_market.py stays as fallback).

Auth model:
  - One-time interactive OAuth 2.0 authorization-code login
    (scripts/stockx_auth.py) captures the initial refresh_token.
  - StockXTokenManager exchanges refresh_token -> access_token before runs /
    on 401, caching the access token in memory with its expiry (and mirroring
    it to the stockx_oauth_tokens table so restarts don't force a refresh).
    Rotated refresh tokens are persisted to the same table.
  - EVERY API request carries BOTH headers:
        Authorization: Bearer {access_token}
        x-api-key: {api_key}

RESPONSE SHAPES (verified against LIVE responses on 2026-07-17) ──────────────
  - GET /v2/catalog/search?query=...&pageNumber=1&pageSize=N
        -> {"count", "pageNumber", "pageSize", "hasNextPage",
            "products": [{"productId", "urlKey", "styleId", "productType",
                          "title", "brand",
                          "productAttributes": {"colorway", "gender",
                                                "releaseDate", "retailPrice"}}]}
    ⚠ styleId can be COMPOUND: "315122-111/CW2288-111" or
      "(Cactus Jack Utopia Edition) TSUT-AF01/(...) CW2288-111" — style-code
      matching must compare each '/'-separated segment, not the whole string.
  - GET /v2/catalog/products/{productId}/variants
        -> [{"productId", "variantId", "variantName", "variantValue" ("9.5"),
             "sizeChart": {"defaultConversion": {"size", "type" e.g. "us m"},
                           "availableConversions": [...]},
             "gtins", "isFlexEligible", "isDirectEligible"}]
  - GET /v2/catalog/products/{productId}/market-data?currencyCode=USD
        -> [{"productId", "variantId", "currencyCode",
             "highestBidAmount": "35" (STRING amounts, may be null),
             "lowestAskAmount", "sellFasterAmount", "earnMoreAmount",
             "flexLowestAskAmount",
             "standardMarketData"/"flexMarketData"/"directMarketData":
                 {"lowestAsk", "highestBidAmount", "sellFaster", "earnMore"}}]
    ⚠ There is NO last-sale field in the public market-data endpoint —
      last_sale is always None from this source.
Parsers stay defensive: unexpected shapes log a warning with the raw
top-level keys instead of crashing ([stockx-api shape] log tag).
──────────────────────────────────────────────────────────────────────────────
"""
import email.utils
import json
import logging
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import List, Optional

import httpx
from sqlalchemy import text

from app import runtime
from app.config import settings
from app.database import SessionLocal, StockXApiUsage, StockXOAuthToken
from app.scrapers.stockx_market import StockXProduct, StockXSizeMarket

logger = logging.getLogger(__name__)

STOCKX_API_BASE = "https://api.stockx.com/v2"
STOCKX_TOKEN_URL = "https://accounts.stockx.com/oauth/token"
STOCKX_AUTHORIZE_URL = "https://accounts.stockx.com/authorize"
STOCKX_AUDIENCE = "gateway.stockx.com"

# ── Rate limits ───────────────────────────────────────────────────────────────
# Documented public-API defaults as of 2026-07-17:
#   25,000 requests/day and ~1 request/second.
# Source: developer portal (https://developer.stockx.com/portal/api-introduction
# — log in and check YOUR app's dashboard; limits are per-account and change),
# corroborated by https://kicks.dev/blog/2024-10-about-stockx-developers.
# ⚠ If your portal shows different numbers, update these two constants — they
# are the single source of truth for the app-wide throttle below.
DAILY_REQUEST_LIMIT = 25_000
REQUESTS_PER_SECOND = 1.0
# Stop this far short of the ceiling so live SKU lookups / a concurrent run
# never push the account into hard 429 territory.
DAILY_SAFETY_MARGIN = 500

# Minimum fuzzy-match ratio (difflib) for the name+colorway fallback when a
# style-code lookup misses.
FUZZY_MATCH_THRESHOLD = 0.60

# ── Retry policy ──────────────────────────────────────────────────────────────
# Transient statuses are retried with exponential backoff + jitter; 4xx client
# errors (other than 408/429) are never retried — they will not heal on their
# own and retrying them just burns the daily budget.
RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
MAX_RETRIES = 4              # total attempts = 1 + MAX_RETRIES
BACKOFF_BASE_SECONDS = 1.0   # 1s, 2s, 4s, 8s (+ jitter), capped below
BACKOFF_CAP_SECONDS = 30.0

_ALNUM_RE = re.compile(r"[^A-Z0-9]")


class StockXBudgetExhausted(RuntimeError):
    """Raised when the shared daily request budget is used up."""


class StockXRequestFailed(RuntimeError):
    """A request still failed after exhausting the retry policy (or hit a
    non-retryable client error). Carries structured context for logging."""

    def __init__(self, path: str, status: "int | None", detail: str, attempts: int):
        self.path = path
        self.status = status
        self.detail = detail
        self.attempts = attempts
        label = f"HTTP {status}" if status is not None else "transport error"
        super().__init__(f"{label} on {path} after {attempts} attempt(s): {detail}")


def _retry_after_seconds(resp: "httpx.Response | None") -> "float | None":
    """Parse Retry-After (delta-seconds or HTTP-date). None when absent/bad."""
    if resp is None:
        return None
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        return max(0.0, (dt - datetime.now(dt.tzinfo)).total_seconds())
    except (TypeError, ValueError):
        return None


def is_configured() -> bool:
    """True when enough credentials exist to use the official API. The
    refresh token may live in .env OR already be persisted in the DB."""
    if not (settings.stockx_client_id and settings.stockx_client_secret
            and settings.stockx_api_key):
        return False
    if settings.stockx_refresh_token:
        return True
    session = SessionLocal()
    try:
        row = session.get(StockXOAuthToken, 1)
        return bool(row and row.refresh_token)
    finally:
        session.close()


# ── Rate limiter ──────────────────────────────────────────────────────────────

class StockXRateLimiter:
    """Per-second spacing (in-process lock) + persistent per-day counter (DB),
    because the daily budget is shared across the entire app and must survive
    restarts. Thread-safe; each acquire uses its own short-lived session."""

    def __init__(self):
        self._lock = threading.Lock()
        self._last_request_at = 0.0

    def calls_today(self) -> int:
        session = SessionLocal()
        try:
            row = session.get(StockXApiUsage, datetime.utcnow().strftime("%Y-%m-%d"))
            return row.calls if row else 0
        finally:
            session.close()

    def acquire(self):
        """Blocks to honor the per-second rate, then increments the daily
        counter. Raises StockXBudgetExhausted at the (margin-adjusted) cap."""
        with self._lock:
            wait = (1.0 / REQUESTS_PER_SECOND) - (time.monotonic() - self._last_request_at)
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()

        day = datetime.utcnow().strftime("%Y-%m-%d")
        session = SessionLocal()
        try:
            calls = session.execute(text(
                "INSERT INTO stockx_api_usage (day, calls) VALUES (:day, 1) "
                "ON CONFLICT (day) DO UPDATE SET calls = stockx_api_usage.calls + 1 "
                "RETURNING calls"
            ), {"day": day}).scalar_one()
            session.commit()
        finally:
            session.close()
        if calls > DAILY_REQUEST_LIMIT - DAILY_SAFETY_MARGIN:
            raise StockXBudgetExhausted(
                f"StockX daily budget exhausted: {calls}/{DAILY_REQUEST_LIMIT} "
                f"(safety margin {DAILY_SAFETY_MARGIN})"
            )


# module-level singletons — the budget/throttle must be app-wide, not per-run
_limiter = StockXRateLimiter()


# ── Token manager ─────────────────────────────────────────────────────────────

class StockXTokenManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._access_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    def get_access_token(self, force_refresh: bool = False) -> str:
        with self._lock:
            if (not force_refresh and self._access_token
                    and self._expires_at
                    and datetime.utcnow() < self._expires_at - timedelta(seconds=60)):
                return self._access_token

            session = SessionLocal()
            try:
                row = session.get(StockXOAuthToken, 1)
                # Reuse a persisted, still-valid access token (e.g. after an
                # app restart) before spending a refresh.
                if (not force_refresh and row and row.access_token
                        and row.access_token_expires_at
                        and datetime.utcnow() < row.access_token_expires_at - timedelta(seconds=60)):
                    self._access_token = row.access_token
                    self._expires_at = row.access_token_expires_at
                    return self._access_token

                refresh_token = (row.refresh_token if row and row.refresh_token
                                 else settings.stockx_refresh_token)
                if not refresh_token:
                    raise RuntimeError(
                        "No StockX refresh token — run scripts/stockx_auth.py once "
                        "to complete the interactive login."
                    )

                resp = httpx.post(STOCKX_TOKEN_URL, data={
                    "grant_type": "refresh_token",
                    "client_id": settings.stockx_client_id,
                    "client_secret": settings.stockx_client_secret,
                    "refresh_token": refresh_token,
                    "audience": STOCKX_AUDIENCE,
                }, timeout=30)
                resp.raise_for_status()
                payload = resp.json()
                access_token = payload["access_token"]
                expires_in = int(payload.get("expires_in", 43200))
                expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

                if row is None:
                    row = StockXOAuthToken(id=1)
                    session.add(row)
                row.access_token = access_token
                row.access_token_expires_at = expires_at
                # StockX may rotate the refresh token on use — persist the new
                # one or the old .env value stops working silently.
                if payload.get("refresh_token"):
                    row.refresh_token = payload["refresh_token"]
                elif not row.refresh_token:
                    row.refresh_token = refresh_token
                session.commit()

                self._access_token = access_token
                self._expires_at = expires_at
                logger.info(f"StockX access token refreshed (expires {expires_at:%Y-%m-%d %H:%M} UTC)")
                return access_token
            finally:
                session.close()


_token_manager = StockXTokenManager()


# ── Client ────────────────────────────────────────────────────────────────────

@dataclass
class StockXLookupResult:
    """Outcome of one SKU lookup. product=None means unmatched/no data —
    failure_reason says why, so the caller can log it (never drop silently)."""
    sku: str
    product: Optional[StockXProduct]
    matched_by: Optional[str] = None       # style_id | name_fuzzy
    failure_reason: Optional[str] = None   # no_search_results | no_style_match | no_market_data | error:<detail>


def _norm_style(s: str) -> str:
    """Style codes appear as 'CW2288-111', 'CW2288 111', 'cw2288111' — compare
    on alphanumerics only."""
    return _ALNUM_RE.sub("", (s or "").upper())


def _style_segments(style_id: str) -> List[str]:
    """StockX styleId is often compound — '315122-111/CW2288-111' or
    '(Cactus Jack Utopia Edition) TSUT-AF01/(...) CW2288-111' (live-verified).
    Yield each '/'-separated segment normalized, with any parenthetical label
    stripped, so equality matching works per style code."""
    out = []
    for seg in (style_id or "").split("/"):
        seg = re.sub(r"\([^)]*\)", " ", seg)   # drop '(… Edition)' labels
        norm = _norm_style(seg)
        if norm:
            out.append(norm)
    return out


def _style_matches(style_id: str, target_norm: str) -> bool:
    return any(seg == target_norm for seg in _style_segments(style_id))


def _amount(val) -> Optional[float]:
    """Amounts have been seen as numbers and as strings in StockX payloads."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


class StockXAPIClient:
    """Thread-safe: httpx.Client is safe for concurrent requests, and the
    shared limiter/token manager hold their own locks. Workers in the lookup
    pool call get_market() concurrently; nothing here touches the caller's
    ORM session."""

    def __init__(self):
        self._http = httpx.Client(
            base_url=STOCKX_API_BASE,
            # connect fast-fail, generous read: the gateway regularly takes
            # multi-second pauses before answering under load
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5,
                                keepalive_expiry=30.0),
        )
        self._shape_warned: set = set()

    def close(self):
        self._http.close()

    def calls_today(self) -> int:
        return _limiter.calls_today()

    # ── HTTP plumbing ────────────────────────────────────────────────────────

    def _request(self, path: str, params: Optional[dict] = None,
                 context: str = "") -> "dict | list | None":
        """GET with auth, app-wide throttle, and the retry policy above.

        context tags every log line with caller context (SKU / product id) so
        a failure in the middle of a 1000-SKU run is attributable.
        Raises StockXRequestFailed when the request cannot be completed;
        StockXBudgetExhausted passes through untouched.
        """
        auth_refreshed = False
        start = time.perf_counter()
        last_status: Optional[int] = None
        last_detail = ""

        attempt = 0
        while True:
            _limiter.acquire()   # every physical request counts vs the budget
            headers = {
                "Authorization": f"Bearer {_token_manager.get_access_token()}",
                "x-api-key": settings.stockx_api_key,
            }
            try:
                resp = self._http.get(path, params=params, headers=headers)
            except httpx.TransportError as exc:
                # DNS/connect/read failures are as transient as a 502 — same policy
                last_status, last_detail = None, repr(exc)
                resp = None
            else:
                last_status = resp.status_code

                if resp.status_code == 404:
                    return None
                if resp.is_success:
                    try:
                        return resp.json()
                    except ValueError as exc:
                        raise StockXRequestFailed(
                            path, resp.status_code,
                            f"non-JSON body: {resp.text[:200]!r}", attempt + 1,
                        ) from exc

                if resp.status_code == 401 and not auth_refreshed:
                    # Stale/revoked access token — force one refresh, don't
                    # count it against the retry budget.
                    logger.info(f"StockX 401 on {path} — refreshing access token "
                                f"[{context}]")
                    _token_manager.get_access_token(force_refresh=True)
                    auth_refreshed = True
                    continue

                last_detail = resp.text[:300]
                if resp.status_code not in RETRYABLE_STATUSES:
                    logger.error(
                        f"StockX non-retryable HTTP {resp.status_code} on {path} "
                        f"[{context}] params={params} elapsed={time.perf_counter() - start:.1f}s "
                        f"body={last_detail!r} headers={dict(resp.headers)}"
                    )
                    raise StockXRequestFailed(path, resp.status_code, last_detail, attempt + 1)

            if attempt >= MAX_RETRIES:
                logger.error(
                    f"StockX request exhausted retries: {path} [{context}] "
                    f"status={last_status} attempts={attempt + 1} "
                    f"elapsed={time.perf_counter() - start:.1f}s body={last_detail!r}"
                )
                raise StockXRequestFailed(path, last_status, last_detail, attempt + 1)

            # Exponential backoff + full jitter; Retry-After (when sane) wins.
            delay = min(BACKOFF_CAP_SECONDS,
                        BACKOFF_BASE_SECONDS * (2 ** attempt)) * (0.5 + random.random() / 2)
            ra = _retry_after_seconds(resp)
            if ra is not None:
                delay = min(max(delay, ra), 120.0)
            logger.warning(
                f"StockX transient failure on {path} [{context}] "
                f"status={last_status} attempt={attempt + 1}/{MAX_RETRIES + 1} "
                f"retrying in {delay:.1f}s — body={last_detail[:120]!r}"
            )
            # Event.wait doubles as an interruptible sleep — an app shutdown
            # aborts the backoff instead of holding a worker thread hostage.
            if runtime.shutdown_event.wait(delay):
                raise StockXRequestFailed(path, last_status,
                                          "aborted: app shutdown requested", attempt + 1)
            attempt += 1

    def _warn_shape(self, tag: str, payload):
        """One warning per shape mismatch per process — see module docstring."""
        if tag in self._shape_warned:
            return
        self._shape_warned.add(tag)
        keys = list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__
        logger.warning(
            f"[stockx-api shape] {tag}: response didn't match the assumed shape "
            f"(top-level: {keys}). Fix the parser in app/scrapers/stockx_api.py "
            f"and the assumptions in README §StockX API. Raw (truncated): "
            f"{json.dumps(payload, default=str)[:500]}"
        )

    # ── Catalog matching ─────────────────────────────────────────────────────

    def _search(self, query: str, page_size: int = 10, context: str = "") -> List[dict]:
        data = self._request("/catalog/search", {"query": query, "pageNumber": 1,
                                                 "pageSize": page_size}, context=context)
        if data is None:
            return []
        if isinstance(data, dict) and isinstance(data.get("products"), list):
            return data["products"]
        self._warn_shape("catalog/search", data)
        return []

    def _match_product(self, sku: str, name: str) -> "tuple[Optional[dict], Optional[str], Optional[str]]":
        """Resolve (catalog_product, matched_by, failure_reason).
        SKU/style-code equality first; name+colorway fuzzy match as fallback."""
        target = _norm_style(sku)

        products = self._search(sku, context=f"sku={sku}")
        # styleId may be compound ('315122-111/CW2288-111') — match per segment.
        # Prefer the FIRST product whose styleId contains the target code;
        # search relevance puts the canonical product first.
        for p in products:
            if _style_matches(p.get("styleId", ""), target):
                return p, "style_id", None

        # Fallback: fuzzy on title + colorway against the scraped name
        if name:
            candidates = products or self._search(name, context=f"sku={sku}")
            best, best_ratio = None, 0.0
            for p in candidates:
                colorway = (p.get("productAttributes") or {}).get("colorway") or ""
                label = f"{p.get('title', '')} {colorway}".strip().lower()
                ratio = SequenceMatcher(None, name.lower(), label).ratio()
                if ratio > best_ratio:
                    best, best_ratio = p, ratio
            if best is not None and best_ratio >= FUZZY_MATCH_THRESHOLD:
                logger.info(
                    f"StockX: {sku} matched by name fuzz ({best_ratio:.2f}) → "
                    f"'{best.get('title')}' [{best.get('styleId')}]"
                )
                return best, "name_fuzzy", None

        return (None, None, "no_search_results" if not products else "no_style_match")

    # ── Market data ──────────────────────────────────────────────────────────

    def _get_variants(self, product_id: str, context: str = "") -> dict:
        """variantId -> US size label ('9.5', or 'W8' for women's sizing).

        Live shape: variantValue is the size in the product's native sizing;
        sizeChart.defaultConversion carries {'size', 'type': 'us m'|'us w'|…}.
        Women's sizes get the 'W' prefix to match app/scrapers/base.py's
        normalize_size() convention used on the retail side."""
        data = self._request(f"/catalog/products/{product_id}/variants", context=context)
        out = {}
        if isinstance(data, list):
            for v in data:
                if not (isinstance(v, dict) and v.get("variantId")):
                    continue
                conv = (v.get("sizeChart") or {}).get("defaultConversion") or {}
                size = str(conv.get("size") or v.get("variantValue")
                           or v.get("variantName") or "")
                conv_type = str(conv.get("type") or "").lower()
                if conv_type == "us w" and size and not size.upper().startswith("W"):
                    size = f"W{size}"
                out[v["variantId"]] = size
        elif data is not None:
            self._warn_shape("catalog/variants", data)
        return out

    def get_market(self, sku: str, name: str = "") -> StockXLookupResult:
        """Full lookup: catalog match → per-variant market data. Returns a
        StockXProduct (same dataclass the browser client emits, so the
        arbitrage engine is source-agnostic). Never raises on match/data
        misses — only on infrastructure failures worth aborting for
        (budget exhausted is one; plain HTTP errors are caught)."""
        try:
            product, matched_by, why = self._match_product(sku, name)
            if product is None:
                return StockXLookupResult(sku=sku, product=None, failure_reason=why)

            product_id = product.get("productId") or product.get("id")
            if not product_id:
                self._warn_shape("catalog/search:productId", product)
                return StockXLookupResult(sku=sku, product=None, failure_reason="error:no_productId")

            ctx = f"sku={sku} product_id={product_id}"
            variants = self._get_variants(product_id, context=ctx)
            market = self._request(f"/catalog/products/{product_id}/market-data",
                                   {"currencyCode": "USD"}, context=ctx)

            sizes: List[StockXSizeMarket] = []
            rows = market if isinstance(market, list) else None
            if rows is None and isinstance(market, dict):
                # tolerate a {"variants": [...]}-style wrapper
                for key in ("variants", "marketData", "data"):
                    if isinstance(market.get(key), list):
                        rows = market[key]
                        break
            if rows is None:
                if market is not None:
                    self._warn_shape("catalog/market-data", market)
                rows = []

            for row in rows:
                if not isinstance(row, dict):
                    continue
                size = variants.get(row.get("variantId"), "") or str(row.get("variantValue") or "ANY")
                std = row.get("standardMarketData") or {}
                lowest_ask = _amount(row.get("lowestAskAmount")) or _amount(std.get("lowestAsk"))
                highest_bid = _amount(row.get("highestBidAmount")) or _amount(std.get("highestBidAmount"))
                # Live-verified: the public market-data endpoint has NO
                # last-sale field — this stays None unless StockX adds one.
                last_sale = _amount(row.get("lastSaleAmount") or row.get("lastSale"))
                if lowest_ask is None and highest_bid is None and last_sale is None:
                    continue
                sizes.append(StockXSizeMarket(
                    size=size, lowest_ask=lowest_ask,
                    highest_bid=highest_bid, last_sale=last_sale,
                ))

            if not sizes:
                return StockXLookupResult(sku=sku, product=None, matched_by=matched_by,
                                          failure_reason="no_market_data")

            url_key = product.get("urlKey") or ""
            return StockXLookupResult(
                sku=sku,
                matched_by=matched_by,
                product=StockXProduct(
                    sku=sku,
                    name=product.get("title") or name,
                    url_key=url_key,
                    stockx_url=f"https://stockx.com/{url_key}" if url_key else "https://stockx.com",
                    sizes=sizes,
                ),
            )
        except StockXBudgetExhausted:
            raise
        except StockXRequestFailed as exc:
            # Already logged in full by _request — record an attributable,
            # queryable reason (endpoint + status) instead of a bare class name.
            status = exc.status if exc.status is not None else "transport"
            return StockXLookupResult(sku=sku, product=None,
                                      failure_reason=f"error:{status}:{exc.path}")
        except Exception as exc:
            logger.exception(f"StockX API lookup failed for {sku} (unexpected)")
            return StockXLookupResult(sku=sku, product=None,
                                      failure_reason=f"error:{type(exc).__name__}")
