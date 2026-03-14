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
    Optional JSON body (all fields have defaults, empty body works fine):
      {
        "per_page":    10,               // 1–250, how many coins to fetch
        "page":        1,               // which page of results
        "vs_currency": "usd",          // pricing currency
        "order":       "market_cap_desc"  // sort order
      }
    db session injected by FastAPI via Depends(get_db).

  GET /etl/jobs:
    Optional query params:
      ?limit=N  (default 20) — how many recent jobs to return

WHAT GOES OUT:
  POST /etl/run  → calls run_pipeline(db, **params) in etl_pipeline.py
                 → returns ETLRunResponse (job_id + status + records_processed)

  GET /etl/jobs  → queries etl_jobs table via SQLAlchemy
                 → returns list[ETLJobResponse]

DEPENDS ON:
  app/database.py       → get_db() session dependency
  app/etl_pipeline.py   → run_pipeline()
  app/models.py         → ETLJob ORM model (for the SELECT query)
  app/schemas.py        → ETLRunRequest, ETLRunResponse, ETLJobResponse
"""

import logging
from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.etl_pipeline import run_pipeline
from app.models import ETLJob
from app.schemas import ETLJobResponse, ETLRunRequest, ETLRunResponse

logger = logging.getLogger(__name__)

router = APIRouter()


# ── POST /etl/run ──────────────────────────────────────────────────────────────
@router.post(
    "/run",
    response_model=ETLRunResponse,
    summary="Trigger ETL pipeline",
    description=(
        "Runs the full Extract → Transform → Load pipeline.\n\n"
        "All request body fields are **optional** — omit the body entirely "
        "to use the defaults (top 10 coins by market cap, priced in USD).\n\n"
        "**Examples:**\n"
        "- `{}` — top 10 coins, USD, market cap order (same as always)\n"
        "- `{\"per_page\": 50}` — top 50 coins\n"
        "- `{\"page\": 2, \"per_page\": 10}` — coins ranked 11–20\n"
        "- `{\"vs_currency\": \"eur\"}` — prices in EUR\n"
        "- `{\"order\": \"volume_desc\"}` — top coins by 24h trading volume"
    ),
)
def trigger_etl(
    params: ETLRunRequest = ETLRunRequest(),  # entire body is optional — defaults kick in if omitted
    db: Session = Depends(get_db),
):
    """
    Triggers the ETL pipeline with optional configuration.

    FLOW:
      1. FastAPI validates the request body against ETLRunRequest
         (or uses all defaults if the body is empty/omitted)
      2. We unpack params and call run_pipeline(db, ...)
      3. run_pipeline returns {"job_id": "...", "status": ..., "records_processed": N}
      4. FastAPI serialises through ETLRunResponse and returns JSON

    WHY THE BODY IS OPTIONAL:
      ETLRunRequest() with no args produces all defaults.
      Assigning it as the default value of `params` means FastAPI treats
      an empty or missing body as valid — backwards compatible with existing callers.
    """
    logger.info(
        f"[Route] POST /etl/run — "
        f"page={params.page} per_page={params.per_page} "
        f"currency={params.vs_currency} order={params.order}"
    )

    result = run_pipeline(
        db,
        page=params.page,
        per_page=params.per_page,
        vs_currency=params.vs_currency,
        order=params.order,
    )

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