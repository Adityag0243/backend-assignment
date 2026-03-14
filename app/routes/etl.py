"""
routes/etl.py
─────────────
WHAT THIS FILE IS:
  FastAPI route handlers for the ETL pipeline endpoints.

ENDPOINTS:
  POST /etl/run    — triggers the full ETL pipeline, returns job result
  GET  /etl/jobs   — returns history of all ETL runs (newest first)

WHAT COMES IN:
  POST /etl/run:
    No request body needed — just the HTTP POST itself triggers the run.
    db session injected by FastAPI via Depends(get_db).

  GET /etl/jobs:
    Optional query params:
      ?limit=N  (default 20) — how many recent jobs to return

WHAT GOES OUT:
  POST /etl/run  → calls run_pipeline(db) in etl_pipeline.py
                 → returns ETLRunResponse (job_id + status)

  GET /etl/jobs  → queries etl_jobs table via SQLAlchemy
                 → returns list[ETLJobResponse]

DEPENDS ON:
  app/database.py       → get_db() session dependency
  app/etl_pipeline.py   → run_pipeline()
  app/models.py         → ETLJob ORM model (for the SELECT query)
  app/schemas.py        → ETLRunResponse, ETLJobResponse
"""

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.etl_pipeline import run_pipeline
from app.models import ETLJob
from app.schemas import ETLJobResponse, ETLRunResponse

logger = logging.getLogger(__name__)

# APIRouter groups these endpoints under the /etl prefix
# (the prefix is set in main.py when we do app.include_router)
router = APIRouter()


# ── POST /etl/run ──────────────────────────────────────────────────────────────
@router.post(
    "/run",
    response_model=ETLRunResponse,
    summary="Trigger ETL pipeline",
    description=(
        "Runs the full Extract → Transform → Load pipeline synchronously. "
        "Creates an etl_jobs audit record, fetches CoinGecko data, merges "
        "with CSV metadata, and upserts into crypto_assets."
    ),
)
def trigger_etl(db: Session = Depends(get_db)):
    """
    Triggers the ETL pipeline.

    FLOW:
      1. FastAPI injects a DB session via Depends(get_db)
      2. We call run_pipeline(db) which handles the full ETL + job tracking
      3. run_pipeline returns {"job_id": "...", "status": "success"|"failed"}
      4. FastAPI serialises that dict through ETLRunResponse and returns JSON

    WHY NO try/except HERE:
      run_pipeline() catches all exceptions internally and writes them to
      etl_jobs.error_message. It always returns a dict — never raises.
      So HTTP 500s from this endpoint mean the server itself crashed,
      not the pipeline failing (that's a 200 with status="failed").
    """
    logger.info("[Route] POST /etl/run — starting pipeline")
    result = run_pipeline(db)
    logger.info(f"[Route] POST /etl/run — job {result['job_id']} → {result['status']}")
    return result


# ── GET /etl/jobs ──────────────────────────────────────────────────────────────
@router.get(
    "/jobs",
    response_model=List[ETLJobResponse],
    summary="ETL job history",
    description="Returns a list of past ETL runs, newest first.",
)
def get_etl_jobs(
    limit: int = 20,
    db: Session = Depends(get_db),
):
    """
    Returns ETL job history from the etl_jobs table.

    Query params:
      limit  (int, default 20) — max number of rows to return.
                                  Capped at 100 to prevent abuse.

    QUERY:
      SELECT * FROM etl_jobs ORDER BY started_at DESC LIMIT :limit

    SQLAlchemy ORM handles parameterisation — no raw SQL strings,
    no SQL injection risk.
    """
    # Cap limit to prevent someone passing ?limit=9999999
    limit = min(limit, 100)

    jobs = (
        db.query(ETLJob)
        .order_by(ETLJob.started_at.desc())
        .limit(limit)
        .all()
    )

    logger.info(f"[Route] GET /etl/jobs — returning {len(jobs)} records")
    return jobs