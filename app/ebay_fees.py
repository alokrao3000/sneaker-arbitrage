"""
eBay seller fee constants — named and easily updated, mirroring
app/stockx_fees.py.

Source (checked 2026-07-18):
  https://www.ebay.com/help/selling/fees-credits-invoices/selling-fees
  - Standard final value fee for most categories: ~13.25% + $0.30 per order.
  https://www.ebay.com/help/selling/fees-credits-invoices/fees-authenticity-guarantee
  - Sneakers sold >= $150 go through the Authenticity Guarantee program at a
    flat 8% final value fee (no per-order fee).

These estimates back the eBay REFERENCE margin only — computed against a
current ACTIVE ask (ebay_price_type='active_ask'), never a realized sale
price. Fees change; re-check the pages above and update ONLY this file.
"""

FINAL_VALUE_FEE_RATE = 0.1325
PER_ORDER_FEE_USD = 0.30
# Authenticity Guarantee program for sneakers at/above the threshold
SNEAKER_AUTH_FEE_RATE = 0.08
SNEAKER_AUTH_MIN_PRICE_USD = 150.00


def estimate_ebay_seller_fees(sale_price: float,
                              authenticity_guarantee: "bool | None" = None) -> float:
    """Estimated total eBay seller fees (dollars) for one sneaker sale.

    authenticity_guarantee=None (default) infers the program from the price
    threshold; pass an explicit bool to override.
    """
    if authenticity_guarantee is None:
        authenticity_guarantee = sale_price >= SNEAKER_AUTH_MIN_PRICE_USD
    if authenticity_guarantee:
        return sale_price * SNEAKER_AUTH_FEE_RATE
    return sale_price * FINAL_VALUE_FEE_RATE + PER_ORDER_FEE_USD


def estimate_payout(sale_price: float,
                    authenticity_guarantee: "bool | None" = None) -> float:
    return sale_price - estimate_ebay_seller_fees(sale_price, authenticity_guarantee)
