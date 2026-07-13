import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.database import init_db, SessionLocal
from app.services.scheduler import start_scheduler, stop_scheduler
from app.services.supplier_loader import load_suppliers
from app.api.routes.opportunities import router as opp_router
from app.api.routes.jobs import router as jobs_router
from app.api.routes.sku import router as sku_router

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
    init_db()

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
