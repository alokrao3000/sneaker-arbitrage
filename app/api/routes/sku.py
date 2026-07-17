"""
Live per-SKU market lookup — bypasses the DB cache entirely, hitting
StockX/GOAT (self-hosted stealth browser) and Alias (real partner API) live.

`cost` is an optional query param the caller supplies (e.g. a retail price
they're manually checking) — if omitted, this returns raw market data with
no ROI/opportunity classification, rather than silently mixing in a
possibly-stale DB-cached supplier price.
"""
import concurrent.futures
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from playwright.sync_api import sync_playwright

from app.config import settings
from app.scrapers.browser import BrowserSession
from app.scrapers.stockx_market import StockXBrowserClient
from app.scrapers import stockx_api
from app.scrapers.stockx_api import StockXAPIClient
from app.scrapers.goat_market import GoatBrowserClient
from app.scrapers.alias import AliasClient
from app.services.pricing import classify_opportunity

router = APIRouter()

# Playwright's sync API refuses to run on a thread that has ever had an
# asyncio event loop associated with it — which FastAPI's own request
# threadpool (anyio) does have. A dedicated plain-thread executor sidesteps
# that entirely.
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="sku-lookup")


@router.get("/{sku}", summary="Live per-size market lookup (bypasses DB cache)")
def get_sku_live(
    sku: str,
    name: str = "",
    cost: Optional[float] = Query(
        None, description="Retail/supplier price to compute ROI against; omit for raw market data only"
    ),
):
    return _executor.submit(_lookup_sku, sku, name, cost).result()


def _lookup_sku(sku: str, name: str, cost: Optional[float]) -> dict:
    # StockX: official API when configured (fast, counts against the shared
    # daily budget); stealth-browser fallback otherwise.
    use_api = stockx_api.is_configured()
    stockx_data = None
    if use_api:
        client = StockXAPIClient()
        try:
            stockx_data = client.get_market(sku, name=name).product
        except Exception:
            pass
        finally:
            client.close()

    # One shared Playwright driver — it only supports one instance per thread
    # at a time, so StockX and GOAT sessions must reuse it (see BrowserSession's
    # docstring in app/scrapers/browser.py).
    playwright = sync_playwright().start()
    stockx_session = None
    if not use_api:
        stockx_session = BrowserSession(
            "stockx", headless=settings.browser_headless,
            proxy_url=settings.stockx_proxy_url,
            state_dir=settings.browser_state_dir,
            nav_timeout_ms=settings.browser_nav_timeout_ms,
            playwright=playwright,
        )
        stockx_session.start()
    goat_session = BrowserSession(
        "goat", headless=settings.browser_headless,
        proxy_url=settings.goat_proxy_url,
        state_dir=settings.browser_state_dir,
        nav_timeout_ms=settings.browser_nav_timeout_ms,
        use_stealth=False,  # stealth patches crash GOAT's own bot-detection JS
        playwright=playwright,
    )
    goat_session.start()

    try:
        goat_data = None
        if stockx_session is not None:
            try:
                stockx_data = StockXBrowserClient(stockx_session).get_product(sku, name=name)
            except Exception:
                pass
        try:
            goat_data = GoatBrowserClient(goat_session).get_product(sku, name=name)
        except Exception:
            pass

        sales_last_7_days: Optional[int] = None  # unknown unless Alias confirms a real count
        alias_lowest_ask = None
        alias_last_sale = None
        if settings.alias_api_key:
            try:
                alias = AliasClient(settings.alias_api_key)
                try:
                    sales_last_7_days = alias.get_sales_last_7_days(sku)
                    avail = alias.get_availability(sku)
                    if avail:
                        alias_lowest_ask = alias.extract_lowest_ask(avail)
                        alias_last_sale = alias.extract_last_sale(avail)
                finally:
                    alias.close()
            except ValueError:
                pass  # no/invalid ALIAS_API_KEY

        if not stockx_data and not goat_data and alias_lowest_ask is None:
            raise HTTPException(status_code=404, detail=f"No market data found for SKU {sku}")

        # Merge per-size entries — prefer StockX ask, then GOAT, then Alias.
        # Today every source returns one aggregate "ANY" entry (see
        # stockx_market.py/goat_market.py docstrings); real per-size StockX
        # data will slot in here unchanged once a working proxy is added.
        sizes_by_key: dict = {}
        product_name = name
        if stockx_data:
            product_name = product_name or stockx_data.name
            for sz in stockx_data.sizes:
                sizes_by_key[sz.size] = {
                    "size": sz.size, "platform": "stockx",
                    "lowest_ask": sz.lowest_ask, "highest_bid": sz.highest_bid,
                    "last_sale": sz.last_sale, "market_url": stockx_data.stockx_url,
                }
        if goat_data:
            product_name = product_name or goat_data.name
            for sz in goat_data.sizes:
                if sz.size not in sizes_by_key and sz.lowest_ask is not None:
                    sizes_by_key[sz.size] = {
                        "size": sz.size, "platform": "goat",
                        "lowest_ask": sz.lowest_ask, "highest_bid": None,
                        "last_sale": sz.last_sale, "market_url": goat_data.goat_url,
                    }
        if alias_lowest_ask is not None and "ANY" not in sizes_by_key:
            # Alias has no public website — link to GOAT's page for the same
            # inventory if we resolved one, since that's what a user can open.
            market_url = goat_data.goat_url if goat_data else None
            sizes_by_key["ANY"] = {
                "size": "ANY", "platform": "alias",
                "lowest_ask": alias_lowest_ask, "highest_bid": None,
                "last_sale": alias_last_sale, "market_url": market_url,
            }

        sizes_out = []
        for entry in sizes_by_key.values():
            result = None
            if cost is not None and entry["lowest_ask"] is not None:
                result = classify_opportunity(
                    original_price=cost, discount_percent=0.0,
                    listing_price=entry["lowest_ask"],
                    platform=entry["platform"],
                    resale_price_type="lowest_ask",
                    sales_last_7_days=sales_last_7_days,
                )
            sizes_out.append({
                **entry,
                "sales_last_7_days": sales_last_7_days,
                "payout_after_fees": round(result.payout, 2) if result else None,
                "margin": round(result.margin, 2) if result else None,
                "roi": round(result.roi, 2) if result else None,
                "is_opportunity": result.is_opportunity if result else None,
            })

        return {"sku": sku, "name": product_name, "sizes": sizes_out}
    finally:
        if stockx_session is not None:
            stockx_session.close()
        goat_session.close()
        playwright.stop()
