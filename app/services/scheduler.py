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

from app import runtime
from app.config import settings
from app.database import SessionLocal, ScrapeJob

logger = logging.getLogger(__name__)

# ── Shared state (thread-safe via lock) ───────────────────────────────────────

_lock = threading.Lock()
_progress_log: List[str] = []   # ring buffer of last 200 messages
_current_job_id: Optional[int] = None
_is_running = False
_scrape_thread: Optional[threading.Thread] = None   # thread of the active run, joined on shutdown

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

def _finalize_job(job_id: int, error: Optional[str]):
    """Guarantee the job reaches a terminal status. Uses a FRESH session —
    the run's own session may be in a failed-transaction state, and a close
    attempt through it would itself raise, leaving the job stuck 'running'."""
    session = SessionLocal()
    try:
        j = session.get(ScrapeJob, job_id)
        if j is not None and j.status == "running":
            j.status = "error"
            j.error_message = error or "job exited without reaching a terminal status"
            j.finished_at = datetime.utcnow()
            session.commit()
            logger.warning(f"Job #{job_id} force-closed as error: {j.error_message}")
    except Exception:
        logger.exception(f"Failed to finalize job #{job_id}")
    finally:
        session.close()


def _run_scrape_job(categories=None, min_discount=0):
    global _is_running, _current_job_id, _scrape_thread

    if runtime.shutdown_event.is_set():
        logger.info("Shutdown in progress — not starting a new scrape job")
        return

    with _lock:
        if _is_running:
            logger.info("Scrape already running — skipping this tick")
            return
        _is_running = True
        _scrape_thread = threading.current_thread()
        _progress_log.clear()

    db = SessionLocal()
    job = ScrapeJob(started_at=datetime.utcnow(), status="running")
    db.add(job)
    db.commit()
    db.refresh(job)
    job_id = job.id

    with _lock:
        _current_job_id = job_id

    _push_progress(f"Job #{job_id} started")

    error: Optional[str] = None
    try:
        from app.services.arbitrage import run_full_scrape
        run_full_scrape(
            db, job_id,
            progress=_push_progress,
            categories=categories,
            min_discount=min_discount,
        )
    except BaseException as exc:   # noqa: BLE001 — even SystemExit must close the job
        logger.exception("Scrape job failed")
        # str(exc) can be empty (seen in job history as blank error rows) —
        # fall back to repr so the failure is always identifiable.
        error = str(exc) or repr(exc)
        try:
            db.rollback()
        except Exception:
            pass
        _push_progress(f"ERROR: {error}")
    finally:
        db.close()
        # Terminal-status guarantee: whatever path got us here (success close
        # inside run_full_scrape, the except above, or an exception thrown by
        # the except block itself), the job must not stay 'running'.
        _finalize_job(job_id, error)
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
    """Graceful shutdown: stop scheduling new runs, signal the active run to
    stop at its next safe boundary (supplier / lookup-chunk edge — everything
    committed so far stays committed), and wait briefly for it to drain.

    Without the signal+join, the scrape's non-daemon ThreadPoolExecutor
    workers are joined at interpreter exit with a full queue, which is the
    multi-minute hang (and KeyboardInterrupt-in-thread-join traceback) seen
    on Ctrl-C."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")

    runtime.shutdown_event.set()
    thread = _scrape_thread
    if thread is not None and thread.is_alive():
        logger.info("Waiting up to 30s for the active scrape job to stop …")
        thread.join(timeout=30)
        if thread.is_alive():
            logger.warning(
                "Scrape job still draining after 30s — its in-flight HTTP calls "
                "are bounded by client timeouts; exit may take a little longer."
            )
        else:
            logger.info("Active scrape job stopped cleanly")
