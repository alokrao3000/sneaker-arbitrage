"""
Reads data/suppliers.csv and seeds (or refreshes) the suppliers table.
Safe to run multiple times — uses upsert logic on (name, url).
"""
import csv
import logging
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session
from app.database import Supplier

logger = logging.getLogger(__name__)

CSV_PATH = Path(__file__).parent.parent.parent / "data" / "suppliers.csv"


def load_suppliers(db: Session) -> int:
    """Insert or update all suppliers from CSV. Returns number of rows processed."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Supplier CSV not found: {CSV_PATH}")

    count = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"].strip()
            url  = row["url"].strip()
            if not name or not url:
                continue

            active_val = row.get("active", "true").strip().lower()
            active = active_val not in ("false", "0", "no")

            discount_raw = row.get("discount_percent", "0").strip()
            try:
                discount_pct = float(discount_raw) if discount_raw else 0.0
            except ValueError:
                discount_pct = 0.0

            # Upsert: find by name + URL, then update or create
            existing: Optional[Supplier] = (
                db.query(Supplier).filter_by(name=name, url=url).first()
            )
            if existing:
                existing.category       = row.get("category", "").strip()
                existing.platform_type  = row.get("platform_type", "custom").strip()
                existing.discount_notes = row.get("discount_notes", "").strip()
                existing.discount_percent = discount_pct
                existing.active         = active
            else:
                db.add(Supplier(
                    name            = name,
                    url             = url,
                    category        = row.get("category", "").strip(),
                    platform_type   = row.get("platform_type", "custom").strip(),
                    discount_notes  = row.get("discount_notes", "").strip(),
                    discount_percent= discount_pct,
                    active          = active,
                ))
            count += 1

    db.commit()
    logger.info(f"Loaded {count} suppliers from CSV")
    return count
