"""
GOAT market data — powered by kicks.dev API.

kicks.dev aggregates GOAT pricing and returns per-size lowest ask
and last sale. Requires KICKS_DEV_API_KEY in .env.
"""
import logging
from typing import List, Optional
from dataclasses import dataclass, field

from app.scrapers.kicks_dev import KicksDevClient, parse_variants

logger = logging.getLogger(__name__)

PLATFORM = "goat"


@dataclass
class GoatSizeMarket:
    size: str
    lowest_ask: Optional[float]
    last_sale: Optional[float]


@dataclass
class GoatProduct:
    sku: str
    name: str
    goat_slug: str
    goat_url: str
    sizes: List[GoatSizeMarket] = field(default_factory=list)


class GoatScraper:
    def __init__(self, client: KicksDevClient):
        self._client = client

    def close(self):
        pass  # client lifecycle managed by caller

    # ── Public interface ──────────────────────────────────────────────────────

    def get_product(self, sku: str, name: str = "") -> Optional[GoatProduct]:
        """Search for a product by brand SKU and return market data for all sizes."""
        summary = self._client.search(PLATFORM, sku)
        # GOAT often stores SKUs with spaces instead of hyphens (e.g. "DJ5162 400").
        if not summary and "-" in sku:
            summary = self._client.search(PLATFORM, sku.replace("-", " "))
        # Fall back to name-based search when SKU lookups return irrelevant results.
        if not summary and name:
            summary = self._client.search_by_name(PLATFORM, sku, name)
        if not summary:
            logger.debug(f"GOAT: no result for SKU {sku}")
            return None

        slug = (
            summary.get("slug") or
            summary.get("url_key") or
            str(summary.get("id") or "")
        )
        if not slug:
            logger.debug(f"GOAT: could not determine slug for SKU {sku}")
            return None

        detail = self._client.get_product(PLATFORM, slug)
        if not detail:
            detail = summary

        name = (
            detail.get("title") or detail.get("name") or
            summary.get("title") or summary.get("name") or ""
        )

        sizes = [
            GoatSizeMarket(
                size=v["size"],
                lowest_ask=v["lowest_ask"],
                last_sale=v["last_sale"],
            )
            for v in parse_variants(detail)
            if v["lowest_ask"] is not None or v["last_sale"] is not None
        ]

        return GoatProduct(
            sku=sku,
            name=name,
            goat_slug=slug,
            goat_url=f"https://www.goat.com/sneakers/{slug}",
            sizes=sizes,
        )
