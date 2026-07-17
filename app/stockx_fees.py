"""
StockX seller fee constants — named and easily updated, NOT the old generic
~12-13% guess.

Source (checked 2026-07-17):
  https://stockx.com/help/articles/what-are-stockxs-fees-for-sellers
  - Transaction fee by seller level: L1 9%, L2 8.5%, L3 8%, L4 7.5%, L5 7%
  - Payment processing fee: 3% on all sales
  - Regional minimum transaction fee: $5.00 (US)
  https://stockx.com/news/updates-to-the-stockx-seller-program/
  - US seller shipping fee for standard (non-Flex) sales: $5 as of March 2026

Fees change (the March 2026 update moved several of them) — re-check the help
article above when a payout looks off, and update ONLY this file.
"""
from app.config import settings

TRANSACTION_FEE_BY_LEVEL: dict[int, float] = {
    1: 0.090,
    2: 0.085,
    3: 0.080,
    4: 0.075,
    5: 0.070,
}
PAYMENT_PROCESSING_FEE = 0.03
MINIMUM_TRANSACTION_FEE_USD = 5.00
# Prepaid label for standard (non-Flex) US sales. Set to 0.0 if you ship Flex
# or want margin before shipping.
US_SHIPPING_FEE_USD = 5.00


def estimate_seller_fees(sale_price: float, seller_level: int | None = None,
                         include_shipping: bool = True) -> float:
    """Estimated total StockX seller fees (dollars) for one sale."""
    level = seller_level if seller_level is not None else settings.stockx_seller_level
    rate = TRANSACTION_FEE_BY_LEVEL.get(level, TRANSACTION_FEE_BY_LEVEL[1])
    transaction_fee = max(sale_price * rate, MINIMUM_TRANSACTION_FEE_USD)
    fees = transaction_fee + sale_price * PAYMENT_PROCESSING_FEE
    if include_shipping:
        fees += US_SHIPPING_FEE_USD
    return fees


def estimate_payout(sale_price: float, seller_level: int | None = None) -> float:
    return sale_price - estimate_seller_fees(sale_price, seller_level)
