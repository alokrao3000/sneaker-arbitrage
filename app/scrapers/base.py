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


# ── Inventory confidence ──────────────────────────────────────────────────────
# Ladder replacing the old binary in_stock flag. Assignment rules:
#   VERIFIED_CART      — this exact size passed an end-to-end add-to-cart
#   VERIFIED_INVENTORY — the retailer's inventory endpoint explicitly confirmed
#                        stock (and, for Shopify, the product-level cart probe
#                        did not contradict it)
#   INVENTORY_ONLY     — listed, but the stronger availability signal was
#                        inconclusive (e.g. cart probe rate-limited)
#   UNKNOWN            — no availability signal at all
#   OUT_OF_STOCK       — a definitive negative (cart rejected: sold out)
CONFIDENCE_VERIFIED_CART = "VERIFIED_CART"
CONFIDENCE_VERIFIED_INVENTORY = "VERIFIED_INVENTORY"
CONFIDENCE_INVENTORY_ONLY = "INVENTORY_ONLY"
CONFIDENCE_UNKNOWN = "UNKNOWN"
CONFIDENCE_OUT_OF_STOCK = "OUT_OF_STOCK"

# Classified cart-validation failure reasons (persisted — keep values stable)
CART_OK = "ok"
CART_OUT_OF_STOCK = "out_of_stock"
CART_SIZE_UNAVAILABLE = "size_unavailable"
CART_REJECTED = "cart_rejected"
CART_QUANTITY_LIMIT = "quantity_limit_exceeded"
CART_PRODUCT_UNAVAILABLE = "product_unavailable"
CART_SESSION_EXPIRED = "session_expired"
CART_RATE_LIMITED = "rate_limited"          # inconclusive — NOT a stock verdict
CART_NETWORK_ERROR = "network_error"        # inconclusive — NOT a stock verdict
CART_UNSUPPORTED = "unsupported_retailer"
CART_UNKNOWN_RESPONSE = "unknown_retailer_response"

# Reasons that mean "we could not tell", as opposed to "definitely can't buy".
CART_INCONCLUSIVE_REASONS = frozenset({
    CART_RATE_LIMITED, CART_NETWORK_ERROR, CART_UNSUPPORTED, CART_UNKNOWN_RESPONSE,
})


@dataclass
class CartValidationResult:
    """Outcome of one add-to-cart attempt for a specific size variant."""
    ok: bool
    reason: str                          # CART_* constant above
    cart_token: Optional[str] = None     # retailer cart/session identifier when returned
    quantity: Optional[int] = None       # quantity the retailer confirmed in-cart
    message: Optional[str] = None        # raw retailer response detail (truncated)
    elapsed_ms: Optional[int] = None

    @property
    def inconclusive(self) -> bool:
        return not self.ok and self.reason in CART_INCONCLUSIVE_REASONS


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
    image_url: Optional[str] = None           # primary product image on the source site
    # Product-level cart probe outcome from the scrape phase (Shopify):
    # "ok" | "blocked" | "inconclusive" | None (not probed / unsupported)
    cart_probe: Optional[str] = None

    def available_sizes(self) -> List[ScrapedSize]:
        return [s for s in self.sizes if s.in_stock]

    def base_confidence(self) -> str:
        """Inventory confidence before any per-size cart validation."""
        if not self.available_sizes():
            return CONFIDENCE_OUT_OF_STOCK
        if self.cart_probe == "blocked":
            return CONFIDENCE_OUT_OF_STOCK
        if self.cart_probe == "inconclusive":
            return CONFIDENCE_INVENTORY_ONLY
        # in-stock per the retailer's inventory data ("ok" probe or unsupported)
        return CONFIDENCE_VERIFIED_INVENTORY


@dataclass
class ScraperStats:
    """Per-stage counters a scraper fills during one scrape() call, so the
    pipeline can report exactly where products were lost instead of silently
    dropping them (discovery → classification → SKU → sizes → price)."""
    discovered: int = 0          # raw items returned by the retailer
    sneaker_matched: int = 0     # passed the sneaker classification
    sku_parse_failed: int = 0    # no brand SKU could be extracted
    size_parse_failed: int = 0   # product had no parseable sizes
    price_parse_failed: int = 0  # no usable price found
    parsed: int = 0              # complete ScrapedProduct emitted
    http_retries: int = 0        # transient-failure retries performed
    cart_probes: int = 0         # product-level cart probes attempted
    cart_probe_blocked: int = 0  # probes that came back definitively blocked
    cart_probe_inconclusive: int = 0


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
