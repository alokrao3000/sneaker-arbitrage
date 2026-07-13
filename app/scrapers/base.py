import re
import time
import random
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)

# Canonical brand SKU: XXXXX-NNN  (e.g. CW2288-111, 919701-101, FZ5808-100)
_BASE_SKU_RE = re.compile(r'^[A-Z0-9]{4,8}-\d{3}[A-Z0-9]?$', re.IGNORECASE)

# Extended form stores sometimes use: XXXXX-NNN-<size>  (e.g. CW2288-111-10, CW2288-111-10.5)
_SKU_RE = re.compile(r'^[A-Z0-9]{4,8}-\d{3}[A-Z0-9]?(-\d{1,2}(\.\d+)?)?$', re.IGNORECASE)

# Women's sizing: "W8", "WMNS8", "Women's 8", "8W", etc.
_WOMENS_PREFIX_RE = re.compile(
    r'^(?:WMNS|WOMEN\'?S?|WOMAN\'?S?|W)\s*(\d[\d.]*)',
    re.IGNORECASE,
)
_WOMENS_SUFFIX_RE = re.compile(r'^(\d[\d.]*)\s*W$', re.IGNORECASE)

# Strips generic men's/US size qualifiers — compiled once
_SIZE_STRIP_RE = re.compile(r"MEN'?S?|US|[()]", re.IGNORECASE)

# Keeps only digits and decimal point after qualifier stripping
_NON_NUMERIC_RE = re.compile(r'[^\d.]')

# Reusable HTML / body-SKU patterns — exported so scrapers share one compiled copy
_HTML_TAG_RE = re.compile(r'<[^>]+>')
_BODY_SKU_RE = re.compile(r'\b([A-Z0-9]{4,8}-\d{3}[A-Z0-9]?)\b', re.IGNORECASE)

# Sizes we care about: US men's 4 – 18 in 0.5 increments
VALID_US_SIZES: frozenset = frozenset({str(s) for s in [
    4, 4.5, 5, 5.5, 6, 6.5, 7, 7.5, 8, 8.5, 9, 9.5,
    10, 10.5, 11, 11.5, 12, 12.5, 13, 14, 15, 16, 17, 18
]})


@dataclass
class ScrapedSize:
    size: str           # US size string, e.g. "10", "10.5", or "W8" for women's
    price: float
    in_stock: bool = True
    variant_id: Optional[str] = None   # supplier-specific variant ID for cart verification


@dataclass
class ScrapedProduct:
    name: str
    sku: str            # brand SKU, e.g. CW2288-111
    url: str
    original_price: float
    sizes: List[ScrapedSize] = field(default_factory=list)
    published_at: Optional[datetime] = None   # when the product was listed on the source site

    def available_sizes(self) -> List[ScrapedSize]:
        return [s for s in self.sizes if s.in_stock]


def is_valid_sku(sku: str) -> bool:
    """Returns True if sku matches brand format with optional size suffix."""
    return bool(_SKU_RE.match(sku.strip()))


def extract_base_sku(sku: str) -> Optional[str]:
    """
    Extract the canonical brand SKU (XXXXX-NNN) from a raw variant SKU.

    Handles stores that append a size suffix, e.g.:
      DV0831-101-10   → DV0831-101
      CW2288-111-10.5 → CW2288-111
      CW2288-111      → CW2288-111  (already canonical)

    Returns None if the input doesn't look like a brand SKU at all.
    """
    clean = sku.strip().upper()
    if not clean:
        return None
    parts = clean.split('-')
    # Strip a trailing size segment (1–2 digits, optionally .5 / .0 etc.)
    if len(parts) >= 3 and re.match(r'^\d{1,2}(\.\d+)?$', parts[-1]):
        clean = '-'.join(parts[:-1])
    if _BASE_SKU_RE.match(clean):
        return clean
    return None


def normalize_size(raw: str) -> Optional[str]:
    """
    Converts various size representations to a US size string.

    Women's sizes (W/WMNS prefix, W suffix) are preserved as "W{num}"
    (e.g. "WMNS 8" → "W8", "8.5W" → "W8.5").  Men's / ungendered sizes
    return plain numeric strings (e.g. "US 10.5" → "10.5").

    Returns None if the size cannot be parsed.
    """
    if not raw:
        return None
    su = raw.strip().upper()

    # Check for women's sizing before any stripping — order matters
    m = _WOMENS_PREFIX_RE.match(su) or _WOMENS_SUFFIX_RE.match(su)
    if m:
        try:
            val = float(m.group(1))
            return f"W{int(val) if val == int(val) else val}"
        except (ValueError, TypeError):
            return None

    # Men's / ungendered: strip known qualifiers, then keep only digits + decimal
    clean = _NON_NUMERIC_RE.sub('', _SIZE_STRIP_RE.sub('', su))
    try:
        val = float(clean)
        return str(int(val)) if val == int(val) else str(val)
    except ValueError:
        return None


def rate_limit(min_sec: float = 1.5, max_sec: float = 4.0):
    time.sleep(random.uniform(min_sec, max_sec))


async def async_rate_limit(min_sec: float = 1.5, max_sec: float = 4.0):
    """Async sleep so other coroutines can run while waiting between requests."""
    await asyncio.sleep(random.uniform(min_sec, max_sec))
