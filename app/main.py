import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.database import init_db, close_orphaned_jobs, SessionLocal
from app.services.scheduler import start_scheduler, stop_scheduler
from app.services.supplier_loader import load_suppliers
from app.api.routes.opportunities import router as opp_router
from app.api.routes.jobs import router as jobs_router
from app.api.routes.sku import router as sku_router

# uvicorn's --reload supervisor sets WindowsSelectorEventLoopPolicy in the
# reloaded worker process on Windows, which can't create subprocesses —
# breaking Playwright (used by the StockX/GOAT browser scrapers), which
# launches its browser driver as a subprocess. Force Proactor explicitly so
# --reload and plain `uvicorn` behave the same way.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──
    logger.info("Initialising database …")
    init_db()   # raises on schema drift — nothing else may run against a stale schema
    close_orphaned_jobs()   # jobs stuck 'running' from a killed process → terminal error

    # Seed suppliers on first run (or refresh from CSV)
    db = SessionLocal()
    try:
        load_suppliers(db)
    finally:
        db.close()

    logger.info("Starting background scheduler …")
    start_scheduler()

    yield   # app is live

    # ── Shutdown ──
    logger.info("Stopping scheduler …")
    stop_scheduler()


app = FastAPI(
    title="BrickFinder Arbitrage Tool",
    description="Retail arbitrage scanner for sneakers — tracks ROI vs StockX/GOAT",
    version="1.0.0",
    lifespan=lifespan,
)

# API routes
app.include_router(opp_router, prefix="/api/opportunities", tags=["Opportunities"])
app.include_router(jobs_router, prefix="/api/jobs", tags=["Jobs"])
app.include_router(sku_router, prefix="/api/sku", tags=["Live SKU Lookup"])

# Serve static files (JS, CSS)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/health")
def health():
    return {"status": "ok"}
