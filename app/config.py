from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://arbitrage:arbitrage_dev@localhost:5432/sneaker_arbitrage"
    host: str = "0.0.0.0"
    port: int = 8000

    roi_threshold: float = 20.0
    commission_rate: float = 0.12   # generic fallback fee rate for GOAT/Alias payout estimates
    # An opportunity must clear this many DOLLARS of margin
    # (resale − StockX seller fees − effective price) to be flagged.
    min_margin_threshold: float = 10.0

    # ── Sales-liquidity gate (app/services/liquidity.py) ──
    # A margin-positive product only surfaces when it is actually selling:
    # >= liquidity_min_sales_7d sales in the last 7 days OR
    # >= liquidity_min_sales_30d sales in the last 30 days. Products with
    # KNOWN counts below both are excluded (not shown, not persisted).
    liquidity_min_sales_7d: int = 1
    liquidity_min_sales_30d: int = 5
    # The official StockX API exposes no sales history, so counts come from
    # Alias / persisted sale_records and may be UNKNOWN. When True (default),
    # an unknown-liquidity size still passes if it has a live highest bid
    # (a real committed buyer — the best demand signal in the market data we
    # do have); it is persisted with liquidity_status='unknown' and ranks
    # below confirmed-liquid rows. Set False for strict per-spec exclusion
    # of anything without confirmed sales.
    liquidity_allow_unknown_with_bid: bool = True

    # ── StockX official API (developer.stockx.com) ──
    # client_id / client_secret / api_key come from your StockX developer app.
    # refresh_token is captured once via scripts/stockx_auth.py; rotations are
    # persisted in the stockx_oauth_tokens table thereafter. Keep all four in
    # .env (git-ignored) or a secrets manager — never committed.
    stockx_client_id: str = ""
    stockx_client_secret: str = ""
    stockx_api_key: str = ""
    stockx_refresh_token: str = ""
    stockx_redirect_uri: str = "http://localhost:8017/stockx/callback"
    stockx_seller_level: int = 1        # 1–5, picks the transaction-fee tier in app/stockx_fees.py
    # Market-data cache TTL. Deliberately shorter than retail-side caching:
    # resale prices move faster than retail prices (6–12h is the sane band).
    stockx_market_ttl_hours: float = 8.0
    # StockX lookups get their own small pool — sized for the API rate limit
    # (~1 req/s), NOT the retailer-scrape concurrency above.
    stockx_lookup_concurrency: int = 2

    # ── Cart validation ──
    # When enabled, every opportunity candidate's exact size is verified with a
    # real add-to-cart on retailers whose scraper supports it (Shopify). A
    # definitive rejection kills the opportunity; a rate-limit/network blip is
    # inconclusive and only lowers the confidence level.
    cart_validation_enabled: bool = True
    # When True (default), retailers that support cart validation only surface
    # opportunities whose size actually passed it (VERIFIED_CART). Retailers
    # without a cart API (Footlocker) cap at VERIFIED_INVENTORY and are still
    # surfaced — their per-size inventory endpoint is the strongest signal
    # that exists for them.
    require_cart_verification: bool = True

    scrape_delay_min: float = 1.5
    scrape_delay_max: float = 4.0
    scrape_interval_minutes: int = 60
    scrape_concurrency: int = 6   # max supplier sites scraped in parallel (network-bound; distinct domains)

    kicks_dev_api_key: str = ""
    alias_api_key: str = ""

    # ── eBay Buy APIs (developer.ebay.com) ──
    # DEMAND PROXIES ONLY. eBay confirmed (July 2026) that no public API
    # exposes market-wide sold-item history — Marketplace Insights, even if
    # granted, only covers our own sales. The client therefore surfaces
    # ACTIVE-listing ask stats (Browse) and watch/merchandising demand signals
    # (Marketing), which are context/ranking inputs and are NEVER fed into the
    # sales-liquidity gate. See app/scrapers/ebay.py.
    ebay_app_id: str = ""            # eBay "App ID" (Client ID)
    ebay_cert_id: str = ""           # eBay "Cert ID" (Client Secret)
    ebay_marketplace_id: str = "EBAY_US"
    ebay_oauth_scope: str = "https://api.ebay.com/oauth/api_scope"
    # Category powering the merchandised-product demand rank (Men's Athletic
    # Shoes on EBAY_US).
    ebay_sneaker_category_id: str = "15709"
    # Per-SKU TTL for eBay context lookups (sku_cache.gate_ebay_check) —
    # ask-stats/demand signals are secondary, so they refresh slower than
    # StockX market data.
    ebay_market_ttl_hours: float = 12.0
    # Tier-1 sold data (EbayClient.get_sold_stats → Marketplace Insights).
    # Flip ONLY once eBay has granted genuinely MARKET-WIDE sold-item access —
    # the pending grant may cover our own sales only, which must not be
    # presented as market history. Off (default): get_sold_stats() returns
    # None and the pipeline stays on the active-listing tier.
    ebay_sold_data_enabled: bool = False

    # How long a successful cart verification (SupplierProductSize.
    # verified_cartable_at) keeps vouching for a size when a later run's probe
    # is inconclusive. Past this, a cached-profitable SKU is escalated to a
    # full fresh re-check instead of being refreshed from cache.
    cart_verification_ttl_hours: float = 6.0

    browser_headless: bool = True
    browser_state_dir: str = "data/browser_state"
    stockx_proxy_url: str = ""
    goat_proxy_url: str = ""
    browser_nav_timeout_ms: int = 30000


settings = Settings()
