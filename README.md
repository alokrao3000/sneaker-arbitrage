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

**⚠ Response-shape assumptions:** the client was written without having seen
a live response from this account. Field names (`styleId`, `variantId`,
`lowestAskAmount`, …) follow the public docs; parsers are defensive and log
`[stockx-api shape]` warnings when reality differs. On the first real run,
grep the logs for that tag and fix `stockx_api.py` + this section before
trusting the data model. Whether the market-data endpoint returns last-sale
at all is UNCONFIRMED — it's parsed opportunistically.

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
