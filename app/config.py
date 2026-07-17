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

    scrape_delay_min: float = 1.5
    scrape_delay_max: float = 4.0
    scrape_interval_minutes: int = 60
    scrape_concurrency: int = 6   # max supplier sites scraped in parallel (network-bound; distinct domains)

    kicks_dev_api_key: str = ""
    alias_api_key: str = ""

    browser_headless: bool = True
    browser_state_dir: str = "data/browser_state"
    stockx_proxy_url: str = ""
    goat_proxy_url: str = ""
    browser_nav_timeout_ms: int = 30000


settings = Settings()
