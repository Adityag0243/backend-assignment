"""
main.py
───────
Entry point for the FastAPI application.

WHAT THIS FILE DOES:
  - Creates the FastAPI app instance
  - Loads environment variables from .env on startup
  - Creates all DB tables on startup (via database.py)
  - Registers all route modules (added in Stage 6)
  - Exposes a /health endpoint for quick sanity checks

WHAT COMES IN:
  - HTTP requests from the outside world (browser, curl, other services)

WHAT GOES OUT:
  - Responses routed to handlers in app/routes/
  - On startup: calls database.py → create_all() to ensure tables exist

DEPENDS ON (added in later stages):
  - app/database.py   (Stage 2)
  - app/routes/       (Stage 6)
"""

from fastapi import FastAPI
from dotenv import load_dotenv

# Load .env file into os.environ before anything else runs
load_dotenv()

app = FastAPI(
    title="Crypto ETL Service",
    description="ETL pipeline: CoinGecko API + CSV → PostgreSQL",
    version="1.0.0",
)


# ── Startup event ──────────────────────────────────────────────────────────────
# Runs once when uvicorn starts.
# Imports models.py so Base.metadata knows about both tables,
# then calls create_all() — Postgres creates the tables if they don't exist yet.
# Safe to run repeatedly: create_all() skips tables that already exist.

from app.database import engine, Base
import app.models  # noqa: F401 — side-effect import registers models with Base

@app.on_event("startup")
def on_startup():
    """Create all tables on server start if they don't already exist."""
    Base.metadata.create_all(bind=engine)


# ── Routes ─────────────────────────────────────────────────────────────────────
# When Stage 6 (routes/) is ready, register routers here like:
# from app.routes import etl, assets
# app.include_router(etl.router,    prefix="/etl",    tags=["ETL"])
# app.include_router(assets.router, prefix="/assets", tags=["Assets"])


# ── Health check ───────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
def health_check():
    """
    Simple ping endpoint.
    Returns 200 if the server is running.
    Used in production to confirm the service is alive.
    """
    return {"status": "ok", "service": "crypto-etl"}