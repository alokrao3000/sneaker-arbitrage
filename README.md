# Sneaker Arbitrage Tool

Scans ~80 sneaker retailers in parallel, compares each shoe's true effective
price (retailer discount + cashback) against StockX/GOAT resale value, and
surfaces flips that clear the configured margin threshold on a local dashboard.

```
effective_price = (original_price × (1 − retailer_discount)) × (1 − cashback_rate)
margin          = resale_price − stockx_seller_fee_estimate − effective_price
```

An opportunity is flagged when `margin > MIN_MARGIN_THRESHOLD` (dollars).
StockX seller fees are the real current structure (level-based transaction fee
+ 3% payment processing + $5 minimum + shipping) in `app/stockx_fees.py`, with
sources and a checked-on date in that file — update there when StockX changes
them.

## Running it

```
docker compose up -d          # starts Postgres
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open http://localhost:8000. The dashboard triggers scrapes, streams live
progress, and shows per-retailer scrape status (succeeded / failed / skipped
/ items found) after each run, plus StockX budget usage (calls made vs
skipped by the cache, and requests used against the daily API limit).

At startup the app (1) reconciles the DB schema against the models and
refuses to boot if any drift survives (`app/database.py::_reconcile_schema` —
no more `UndefinedColumn` mid-scrape), and (2) closes any jobs orphaned in
`running` by a killed process. Every scrape job is guaranteed to reach
`done`/`error` with `finished_at` set (`app/services/scheduler.py`).

## StockX API

Market data comes from the official StockX public API
(https://developer.stockx.com) when credentials are configured; otherwise the
app falls back to the slower stealth-browser scraper (aggregate, one price
for all sizes).

Setup (one time):

1. Create an app at developer.stockx.com; put `STOCKX_CLIENT_ID`,
   `STOCKX_CLIENT_SECRET`, `STOCKX_API_KEY` in `.env` (git-ignored — never
   commit credentials; `.env` was untracked from git for this reason).
2. Register the redirect URI (`STOCKX_REDIRECT_URI`, default
   `http://localhost:8017/stockx/callback`) on the app.
3. Run `python scripts/stockx_auth.py` — interactive login, captures the
   refresh token into the `stockx_oauth_tokens` table. Access tokens are
   refreshed automatically before runs / on 401 and cached with their expiry;
   rotated refresh tokens are persisted.

Every request sends both `Authorization: Bearer …` and `x-api-key: …`.

**Matching:** SKU/style-code equality against catalog search first, then a
name+colorway fuzzy fallback. Unmatched products land in the
`stockx_match_failures` table (reason + attempt counts) — check it to see
match-rate quality; the per-run summary also prints the match rate.

**Rate limiting:** documented defaults (25,000 requests/day, ~1 req/s) are
named constants in `app/scrapers/stockx_api.py` with the source linked.
⚠ Verify them against *your* developer-portal dashboard — they are
per-account and change. The throttle is app-wide: a token-bucket for pacing
plus a persistent per-day counter (`stockx_api_usage` table) shared by scrape
runs and live SKU lookups.

**Response shapes (verified against live responses on 2026-07-17):**
- `catalog/search` returns `{"count", "hasNextPage", "products": [...]}`;
  each product has `productId`, `urlKey`, `styleId`, `title`,
  `productAttributes.{colorway,gender,releaseDate,retailPrice}`.
  **`styleId` can be compound** (`"315122-111/CW2288-111"`) — matching splits
  on `/` and compares each segment.
- `variants` returns `variantId`, `variantValue` (size), and
  `sizeChart.defaultConversion.{size,type}` (`"us m"` / `"us w"` — women's
  sizes are normalized to the `W{n}` convention used on the retail side).
- `market-data` returns per-variant `lowestAskAmount` / `highestBidAmount`
  as **strings**, plus `standardMarketData`/`flexMarketData`/
  `directMarketData` sub-objects. **There is no last-sale field** in the
  public API — `last_sale` stays NULL for StockX rows.

Requests retry on 408/429/500/502/503/504 (exponential backoff + jitter,
`Retry-After` honored, 4xx client errors never retried); each physical
request counts against the daily budget. Failed lookups are collected and
summarized at the end of a run — they never abort it — and their gate state
stays untouched so they're retried next run.

## Resale price: ask vs last-sale

Each opportunity stores `resale_price_type` (`lowest_ask` | `last_sale`) —
shown as an ASK/SALE badge in the dashboard — because the two mean different
things: a lowest ask is a *live listing price* (someone is asking it; you can
undercut or match it and be next in the queue), while a last sale is a
*cleared transaction* (real money moved, but possibly at a stale price on a
thin book). We default to **lowest ask** as the realizable-arbitrage estimate
— matching the ask is the price at which you can actually list and expect to
transact next — and fall back to last-sale only when no ask exists. The
tradeoff: asks can be fantasy on illiquid shoes (nobody may pay it), so a
margin computed off a lonely high ask overstates reality; when the ASK badge
sits far above last-sale, trust the sale.

## Sales liquidity

A high theoretical ROI is worthless if the shoe isn't actually selling, so a
margin-positive size only becomes an opportunity when it also passes the
sales-liquidity gate (`app/services/liquidity.py`):

- **≥ 1 completed sale in the last 7 days**, OR
- **≥ 5 completed sales in the last 30 days**
  (`LIQUIDITY_MIN_SALES_7D` / `LIQUIDITY_MIN_SALES_30D`).

Products whose *known* counts fail both conditions are excluded outright: not
returned by the API, not shown in the UI, not persisted as opportunities.
Per-SKU metrics (`sales_last_7_days`, `sales_last_30_days`, `last_sale_date`,
`liquidity_status`) are persisted on each opportunity for filtering,
analytics, and debugging, and the default "best" sort weights profitability
by 30-day volume so liquid flips outrank illiquid ones.

**Data sources & documented limitation.** The official StockX public API
exposes **no sales history at all** — the market-data endpoint has no
last-sale field and no sales-count/sales-list endpoint exists (live-verified
2026-07-17, see `app/scrapers/stockx_api.py`); GOAT's public site exposes
none either. Sale events therefore come from the Alias partner API (30-day
window, persisted to `sale_records`), with previously persisted records as a
fallback while collection is demonstrably current (newest record recorded
within 30 days). When no sales source exists (e.g. no valid `ALIAS_API_KEY`),
counts are *unknown* — the best remaining demand signal in the market data we
do have is a **live highest bid** for the exact size (a committed buyer), so
with `LIQUIDITY_ALLOW_UNKNOWN_WITH_BID=true` (default) an unknown-liquidity
size surfaces only if it has one, tagged `liquidity_status=unknown` so it
ranks below and is filterable from confirmed-liquid rows. Set it to `false`
to strictly exclude anything without confirmed sales.

## Rate-limit-aware caching

The real constraint is StockX's shared daily budget, not wall-clock. Per-SKU
gate (`sku_market_cache` table, rules in `app/services/sku_cache.py`):

- **Skip the StockX call** when the SKU's new effective price is ≥ the best
  price already evaluated *and* that evaluation was non-profitable — just
  bump `last_seen_at`.
- **Re-check** only when: never checked, new price strictly lower than the
  recorded best, or cached market data older than `STOCKX_MARKET_TTL_HOURS`
  (default 8h — deliberately shorter than retail-side caching; resale prices
  move faster).
- Cached-profitable SKUs are re-evaluated from cached `market_prices` rows
  with zero calls; the dashboard's ●-dot marks fresh vs cached data.

Calls made vs skipped are logged per run, stored on the job row, and shown in
the dashboard.

## Cart validation & inventory confidence

Price + inventory endpoints alone can lie (pre-drops, notify-me pages, stale
caches). Availability is graded on a confidence ladder
(`app/scrapers/base.py`):

| Level | Meaning |
|---|---|
| `VERIFIED_CART` | this exact size passed a real add-to-cart via the retailer's normal purchase flow |
| `VERIFIED_INVENTORY` | the retailer's inventory endpoint explicitly confirmed stock |
| `INVENTORY_ONLY` | listed, but the stronger signal was inconclusive (e.g. cart probe rate-limited) |
| `UNKNOWN` | no availability signal |
| `OUT_OF_STOCK` | definitive negative |

Every **opportunity candidate** gets a per-size cart validation on retailers
whose scraper supports it (Shopify: `POST /cart/add.json`, capturing the cart
token + confirmed quantity). A definitive rejection (sold out / size
unavailable / dead variant) excludes the opportunity — recorded, never silent
— while a 429/network blip is *inconclusive* and only lowers confidence
(`CART_VALIDATION_ENABLED` / `REQUIRE_CART_VERIFICATION` in `.env`).
Footlocker's cart sits behind Akamai and its sizes carry no cart-addable
variant id, so it caps at `VERIFIED_INVENTORY` (its per-size
`stockLevelStatus` is the strongest signal that exists there). Validation
volume is deliberately tiny: only candidates are checked, not all products.

Diagnostics: `retailer_product_diagnostics` keeps the latest per-(retailer,
SKU) pipeline outcome — furthest stage reached, sizes detected, inventory/cart
verdicts, cart token/quantity, last error, evaluation duration — so failed
retailers are debuggable from SQL without rerunning a scrape. Each run ends
with a per-retailer summary table (discovered / parsed / inventory-ok /
cart-verified / opportunities / SQL writes / failures / retries / timing),
also persisted on `scrape_supplier_results`.
`python scripts/validate_pipeline.py` runs the full pipeline on two fast
suppliers and prints a stage-by-stage validation report.

## Concurrency

Retailer sites scrape in parallel (`SCRAPE_CONCURRENCY`, network-bound,
distinct domains). StockX API lookups run on their **own** small pool
(`STOCKX_LOOKUP_CONCURRENCY`, default 2) sized for the API rate limit — never
the retailer concurrency. Workers do network only; all DB writes stay on the
orchestrating thread.

## Adding a retailer

1. Add a row to `data/suppliers.csv` with its `name`, `url`, `category`,
   `platform_type` (`shopify`, `footlocker`, or `custom`), and its discount —
   either `discount_percent` or `discount_amount` (flat $), not both.
2. If it needs a scraper beyond the existing Shopify/Footlocker handlers,
   add a subclass in `app/scrapers/` and route to it in
   `app/services/arbitrage.py::_get_supplier_scraper`.
3. Add an entry for it in `app/cashback_rates.py` (see below) — optional,
   defaults to 0%/"none" if omitted.

The CSV is re-read and upserted into the `suppliers` table on every app
startup (`app/services/supplier_loader.py`), or on demand via
`POST /api/jobs/init-suppliers`.

## Updating cashback rates

Edit `app/cashback_rates.py` directly — it's a plain dict keyed by the
retailer's `name` from `suppliers.csv`:

```python
"Footlocker": CashbackRate(0.08, "Rakuten"),
```

Rates change often across Rakuten/TopCashback/RetailMeNot/Honey, so this
file is the only place they live — no rate is ever hardcoded elsewhere.
The effective price shown in the dashboard and used as the ROI cost basis is:

```
price_after_discount = list_price - discount        # % of list, or flat $
effective_price      = price_after_discount * (1 - cashback_rate)
```

Both the ROI engine (`app/services/pricing.py`) and the dashboard breakdown
call the same `compute_effective_price()` in `app/services/effective_price.py`,
so they can't diverge.

## Assumptions

- `suppliers.csv` treats `discount_percent` and `discount_amount` as
  mutually exclusive per row — percent wins if both are set.
- Cashback rates are manually maintained, not scraped — `cashback_rates.py`
  ships with every current supplier name present at 0%/"none" as a
  fill-in-the-blanks starting point.
