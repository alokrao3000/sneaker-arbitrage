"""Report whether the scrapers are actually populating the database.

Run with the project venv, from the repo root:
    venv/Scripts/python.exe scripts/check_scrape_health.py

Prints scrape_jobs history + row counts/freshness for the tables scrapers
write to, so a human (or agent) can spot silent scraper failures.
"""
import sys
from datetime import datetime, timezone

sys.path.insert(0, ".")

from sqlalchemy import text  # noqa: E402

from app.config import settings  # noqa: E402
from app.database import engine  # noqa: E402

STALE_MULTIPLIER = 2  # flag data older than N x scrape_interval_minutes

FRESHNESS_TABLES = [
    ("supplier_products", "scraped_at"),
    ("market_prices", "fetched_at"),
    ("sale_records", "recorded_at"),
    ("opportunities", "found_at"),
]


def fmt_age(ts):
    if ts is None:
        return "never"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - ts
    mins = delta.total_seconds() / 60
    if mins < 60:
        return f"{mins:.0f}m ago"
    if mins < 1440:
        return f"{mins / 60:.1f}h ago"
    return f"{mins / 1440:.1f}d ago"


def main():
    print(f"DB: {settings.database_url.split('@')[-1]}")
    print(f"Expected scrape interval: {settings.scrape_interval_minutes} min")
    print(f"kicks_dev_api_key set: {bool(settings.kicks_dev_api_key)}")
    print(f"alias_api_key set: {bool(settings.alias_api_key)}")
    print()

    with engine.connect() as conn:
        print("== scrape_jobs (last 10) ==")
        rows = conn.execute(text(
            "SELECT id, status, started_at, finished_at, suppliers_scraped, "
            "skus_found, opportunities_found, error_message "
            "FROM scrape_jobs ORDER BY started_at DESC LIMIT 10"
        )).fetchall()
        if not rows:
            print("  NO ROWS — scheduler/job runner has never recorded a run.")
        for r in rows:
            print(f"  #{r.id} [{r.status}] started={r.started_at} "
                  f"finished={r.finished_at} suppliers={r.suppliers_scraped} "
                  f"skus={r.skus_found} opps={r.opportunities_found}")
            if r.error_message:
                print(f"      error: {r.error_message[:200]}")

        stuck = [r for r in rows if r.status == "running" and r.finished_at is None]
        if stuck:
            print(f"\n  {len(stuck)} job(s) stuck in 'running' with no finished_at "
                  f"(likely crashed/killed process) -> WARN")

        if rows:
            last_ok = next((r for r in rows if r.status == "done" and r.finished_at), None)
            if last_ok:
                age_min = (datetime.now(timezone.utc) - last_ok.finished_at.replace(
                    tzinfo=last_ok.finished_at.tzinfo or timezone.utc)).total_seconds() / 60
                verdict = "OK" if age_min < settings.scrape_interval_minutes * STALE_MULTIPLIER else "STALE"
                print(f"  Last completed ('done') job: {fmt_age(last_ok.finished_at)} -> {verdict}")
            else:
                print("  No 'done' job found in last 10 runs -> FAIL")

        print("\n== data tables ==")
        for table, ts_col in FRESHNESS_TABLES:
            row = conn.execute(text(
                f"SELECT COUNT(*) AS n, MAX({ts_col}) AS last_ts FROM {table}"
            )).fetchone()
            verdict = "EMPTY" if row.n == 0 else fmt_age(row.last_ts)
            print(f"  {table:20s} rows={row.n:<8} last {ts_col}={verdict}")


if __name__ == "__main__":
    main()
