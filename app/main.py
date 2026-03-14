"""
main.py
───────
WHAT THIS FILE IS:
  Entry point for the FastAPI application.
  Ties together every other module.

WHAT COMES IN:
  HTTP requests from the outside world (browser, curl, other services,
  Render's health check pings).

WHAT GOES OUT:
  - On startup: creates DB tables via database.py → models.py
  - All requests routed to handlers in app/routes/
  - /health returns a quick 200 for uptime monitoring

DATA FLOW (request lifecycle):
  HTTP request
    → main.py        (picks the right router)
    → routes/*.py    (validates params, calls pipeline or queries DB)
    → etl_pipeline.py / models.py  (does the work)
    → schemas.py     (serialises ORM objects to JSON)
    → HTTP response

DEPENDS ON:
  app/database.py      → engine, Base  (startup)
  app/models.py        → registers both ORM models with Base (side-effect import)
  app/routes/etl.py    → /etl/* endpoints
  app/routes/assets.py → /assets/* endpoints
"""

import logging
from dotenv import load_dotenv

# ── Load .env before any other app import ─────────────────────────────────────
# This must happen before database.py is imported, because database.py reads
# DATABASE_URL from the environment the moment it is imported.
load_dotenv()

from fastapi import FastAPI
from app.database import engine, Base
import app.models  # noqa: F401 — registers CryptoAsset + ETLJob with Base.metadata
from app.routes import etl as etl_routes
from app.routes import assets as assets_routes

# ── Logging ───────────────────────────────────────────────────────────────────
# Configure once at the top level. All loggers in every module (etl_pipeline,
# retry_utils, routes) inherit this config and write to stdout.
# Render captures stdout automatically — no file handler needed.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── App instance ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="Crypto ETL Service",
    description=(
        "Mini ETL pipeline: pulls top-10 crypto coins from CoinGecko, "
        "merges with local CSV metadata, and loads into PostgreSQL.\n\n"
        "**Usage:** Call `POST /etl/run` first to populate the database, "
        "then query `GET /assets`."
    ),
    version="1.0.0",
)


# ── Startup: create tables ─────────────────────────────────────────────────────
# Runs once when uvicorn boots.
# create_all() is idempotent — skips tables that already exist.
# On Render, this runs every deploy, so new columns added to models.py
# won't auto-migrate (use Alembic for that in production).
@app.on_event("startup")
def on_startup():
    """Create all DB tables on server start if they don't already exist."""
    logger.info("Server starting — ensuring database tables exist")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables ready")


# ── Routers ───────────────────────────────────────────────────────────────────
# Each router handles a group of related endpoints.
# The prefix here combines with the path defined in the router file:
#   etl_routes:    prefix="/etl"    + "/run"      → POST /etl/run
#                                  + "/jobs"     → GET  /etl/jobs
#   assets_routes: prefix="/assets" + "/"         → GET  /assets
#                                  + "/{symbol}" → GET  /assets/{symbol}
app.include_router(etl_routes.router,    prefix="/etl",    tags=["ETL Pipeline"])
app.include_router(assets_routes.router, prefix="/assets", tags=["Crypto Assets"])


# ── Health check ──────────────────────────────────────────────────────────────
# Render (and most deployment platforms) pings a health endpoint to decide
# whether to route traffic to this instance.
# Keep this fast and dependency-free — don't query the DB here.
@app.get("/health", tags=["Health"])
def health_check():
    """Quick liveness check. Returns 200 if the server process is running."""
    return {"status": "ok", "service": "crypto-etl"}