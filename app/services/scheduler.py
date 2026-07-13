"""
Background scheduler — runs the full scrape on a configurable interval.
Also exposes shared state so the dashboard can show live job progress.
"""
import logging
import threading
from datetime import datetime
from typing import Optional, List

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from app.config import settings
from app.database import SessionLocal, ScrapeJob

logger = logging.getLogger(__name__)

# ── Shared state (thread-safe via lock) ───────────────────────────────────────

_lock = threading.Lock()
_progress_log: List[str] = []   # ring buffer of last 200 messages
_current_job_id: Optional[int] = None
_is_running = False

_scheduler: Optional[BackgroundScheduler] = None


def get_status() -> dict:
    with _lock:
        return {
            "is_running": _is_running,
            "current_job_id": _current_job_id,
            "recent_messages": list(_progress_log[-50:]),
        }


def get_progress_messages() -> List[str]:
    with _lock:
        return list(_progress_log)


def _push_progress(msg: str):
    with _lock:
        _progress_log.append(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}")
        if len(_progress_log) > 200:
            _progress_log.pop(0)


# ── Job runner ────────────────────────────────────────────────────────────────

def _run_scrape_job(categories=None, min_discount=0):
    global _is_running, _current_job_id

    with _lock:
        if _is_running:
            logger.info("Scrape already running — skipping this tick")
            return
        _is_running = True
        _progress_log.clear()

    db = SessionLocal()
    job = ScrapeJob(started_at=datetime.utcnow(), status="running")
    db.add(job)
    db.commit()
    db.refresh(job)

    with _lock:
        _current_job_id = job.id

    _push_progress(f"Job #{job.id} started")

    try:
        from app.services.arbitrage import run_full_scrape
        run_full_scrape(
            db, job.id,
            progress=_push_progress,
            categories=categories,
            min_discount=min_discount,
        )
    except Exception as exc:
        logger.exception("Scrape job failed")
        db.rollback()
        job.status = "error"
        job.error_message = str(exc)
        job.finished_at = datetime.utcnow()
        db.commit()
        _push_progress(f"ERROR: {exc}")
    finally:
        db.close()
        with _lock:
            _is_running = False

    _push_progress("Job finished")


def trigger_scrape() -> Optional[int]:
    """Manually trigger a full scrape in a background thread. Returns job id if started."""
    with _lock:
        if _is_running:
            return None

    t = threading.Thread(target=_run_scrape_job, daemon=True, name="scrape-job")
    t.start()
    return _current_job_id


def trigger_tier0_scan() -> Optional[int]:
    """Trigger a quick scan limited to tier0_qs suppliers that have a discount. Returns job id if started."""
    with _lock:
        if _is_running:
            return None

    t = threading.Thread(
        target=_run_scrape_job,
        kwargs={"categories": ["tier0_qs"], "min_discount": 1},
        daemon=True,
        name="scrape-job",
    )
    t.start()
    return _current_job_id


# ── Scheduler lifecycle ────────────────────────────────────────────────────────

def start_scheduler():
    global _scheduler
    _scheduler = BackgroundScheduler(
        executors={"default": ThreadPoolExecutor(1)},
        job_defaults={"coalesce": True, "max_instances": 1},
    )
    _scheduler.add_job(
        _run_scrape_job,
        "interval",
        minutes=settings.scrape_interval_minutes,
        id="full_scrape",
        replace_existing=True,
        next_run_time=datetime.utcnow(),
    )
    _scheduler.start()
    logger.info(
        f"Scheduler started — scraping every {settings.scrape_interval_minutes} minutes"
    )


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
