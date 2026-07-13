"""
Alias marketplace API client.

Provides per-SKU pricing insights and recent sales history.
Requires ALIAS_API_KEY in .env — sign up at https://alias.org/

API reference: https://docs.alias.org/#api-reference

Endpoints used:
  GET /api/v1/pricing_insights/availability    → lowest ask, highest bid, last sold
  GET /api/v1/pricing_insights/recent_sales    → chronological sales list (newest first)
"""
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any

import httpx

logger = logging.getLogger(__name__)

BASE = "https://api.alias.org"


class AliasClient:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("ALIAS_API_KEY is required — sign up at https://alias.org/")
        self._session = httpx.Client(
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            },
            timeout=30,
            follow_redirects=True,
        )

    def close(self):
        self._session.close()

    def get_availability(
        self,
        catalog_id: str,
        size: Optional[str] = None,
        region_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Get current marketplace pricing for a catalog item (brand SKU = catalog_id).

        Returns a dict with fields such as:
          lowest_listing_price, highest_offer_price, last_sold_price,
          global_indicator_price
        or None if the shoe is not found / request fails.
        """
        params: Dict[str, Any] = {"catalog_id": catalog_id}
        if size:
            params["size"] = size
        if region_id:
            params["region_id"] = region_id

        data = self._get_json(f"{BASE}/api/v1/pricing_insights/availability", params)
        if not data:
            return None

        inner = data.get("data")
        return inner if isinstance(inner, dict) else (data if isinstance(data, dict) else None)

    def get_recent_sales(
        self,
        catalog_id: str,
        size: Optional[str] = None,
        region_id: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch individual marketplace sale events in the last 7 days for a
        catalog item, as a list of {"price": float|None, "sale_date": datetime}.

        Uses Pattern 1 (catalog-level) from the recent_sales endpoint so that a
        single call covers all sizes — giving a reliable overall demand signal.
        With size supplied it narrows to that specific variant.

        Returns None (not []) when the request failed or the SKU isn't in
        Alias's catalog — "unknown", not "confirmed zero sales". Only a
        successful response with a genuinely empty sales list returns [].
        """
        params: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "consigned": "true",  # required non-null for Pattern 1
            "limit": 200,
        }
        if size:
            params["size"] = size
        if region_id:
            params["region_id"] = region_id

        data = self._get_json(f"{BASE}/api/v1/pricing_insights/recent_sales", params)
        if not data:
            return None

        sales = data.get("data", [])
        if not isinstance(sales, list):
            return None

        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        events: List[Dict[str, Any]] = []
        for sale in sales:
            sale_date = _parse_sale_date(sale)
            if sale_date is None:
                # No date info — count it conservatively (can't tell if within window)
                events.append({"price": _parse_sale_price(sale), "sale_date": None,
                               "size": _parse_sale_size(sale)})
                continue
            if sale_date >= cutoff:
                events.append({"price": _parse_sale_price(sale), "sale_date": sale_date,
                               "size": _parse_sale_size(sale)})
            else:
                # Results are newest-first; once we pass the 7-day cutoff we're done
                break

        return events

    def get_sales_last_7_days(
        self,
        catalog_id: str,
        size: Optional[str] = None,
        region_id: Optional[str] = None,
    ) -> Optional[int]:
        """Count marketplace sales in the last 7 days for a catalog item.
        Returns None (unknown) if the request failed, rather than 0."""
        sales = self.get_recent_sales(catalog_id, size=size, region_id=region_id)
        return len(sales) if sales is not None else None

    def extract_lowest_ask(self, availability: Dict) -> Optional[float]:
        """Pull the lowest listing price out of a get_availability() result."""
        for field in (
            "lowest_listing_price", "lowestListingPrice",
            "lowest_ask", "lowestAsk",
            "ask", "price",
        ):
            val = availability.get(field)
            if val is not None:
                return _to_float(val)
        return None

    def extract_last_sale(self, availability: Dict) -> Optional[float]:
        """Pull the last sold price out of a get_availability() result."""
        for field in (
            "last_sold_price", "lastSoldPrice",
            "last_sale", "lastSale",
            "last_sale_price",
        ):
            val = availability.get(field)
            if val is not None:
                return _to_float(val)
        return None

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def _get_json(self, url: str, params: dict = None) -> Optional[Dict]:
        for attempt in range(3):
            try:
                resp = self._session.get(url, params=params)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (401, 403):
                    logger.error(
                        f"Alias API: authentication failed ({resp.status_code}) — "
                        "check ALIAS_API_KEY in .env"
                    )
                    return None
                if resp.status_code == 404:
                    return None  # shoe not in Alias catalog — not an error
                if resp.status_code == 422:
                    logger.debug(
                        f"Alias API: validation error for {url} params={params} — "
                        f"{resp.text[:300]}"
                    )
                    return None
                if resp.status_code == 429:
                    wait = 15 * (attempt + 1)
                    logger.warning(f"Alias API rate-limited — waiting {wait}s")
                    time.sleep(wait)
                    continue
                logger.debug(
                    f"Alias API {url} → HTTP {resp.status_code}: {resp.text[:300]}"
                )
                return None
            except httpx.RequestError as exc:
                logger.warning(f"Alias API request error: {exc}")
                if attempt < 2:
                    time.sleep(3 * (attempt + 1))

        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

# Field names Alias might use for the sale timestamp across API versions
_DATE_FIELDS = (
    "sold_at", "sale_date", "created_at", "date",
    "transaction_date", "saleDate", "soldAt", "createdAt",
)

# Field names Alias might use for the sale price across API versions
_PRICE_FIELDS = (
    "price", "sale_price", "sold_price", "amount",
    "salePrice", "soldPrice",
)

# Field names Alias might use for the sale's size across API versions
_SIZE_FIELDS = ("size", "us_size", "usSize", "sizeLabel")


def _parse_sale_price(sale: Dict) -> Optional[float]:
    for field in _PRICE_FIELDS:
        val = sale.get(field)
        if val is not None:
            return _to_float(val)
    return None


def _parse_sale_size(sale: Dict) -> Optional[str]:
    for field in _SIZE_FIELDS:
        val = sale.get(field)
        if val is not None:
            return str(val)
    return None


def _parse_sale_date(sale: Dict) -> Optional[datetime]:
    for field in _DATE_FIELDS:
        val = sale.get(field)
        if not val:
            continue
        try:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, AttributeError):
            continue
    return None


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
