"""
schemas.py
──────────
WHAT THIS FILE IS:
  Pydantic models that define the exact shape of every API response.
  These are NOT the database models (those live in models.py).
  These are the JSON contracts we expose to API callers.

WHY SEPARATE FROM models.py:
  ORM models (models.py) are coupled to SQLAlchemy and the DB schema.
  Pydantic schemas (this file) are what the outside world sees.
  Keeping them separate means we can change the DB shape without
  breaking the API contract, and vice versa.

WHAT COMES IN:
  Raw SQLAlchemy ORM instances from route handlers (routes/assets.py,
  routes/etl.py). FastAPI serialises them using these schemas.

WHAT GOES OUT:
  → CryptoAssetResponse  to routes/assets.py  (GET /assets, GET /assets/{symbol})
  → ETLJobResponse       to routes/etl.py     (GET /etl/jobs)
  → ETLRunResponse       to routes/etl.py     (POST /etl/run)

model_config = ConfigDict(from_attributes=True):
  Tells Pydantic to read values from ORM object attributes (e.g. asset.price)
  instead of expecting a plain dict. Required for SQLAlchemy → Pydantic conversion.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


# ── Response: single crypto asset ─────────────────────────────────────────────
class CryptoAssetResponse(BaseModel):
    """
    Returned by:
      GET /assets           (as a list)
      GET /assets/{symbol}  (as a single object)

    Maps directly to the crypto_assets table columns.
    All fields except symbol and name are Optional because a coin might
    be loaded from CSV-only data with no live price yet, or vice versa.
    """

    # model_config tells Pydantic: "read from ORM object attributes, not a dict"
    model_config = ConfigDict(from_attributes=True)

    symbol:           str
    name:             Optional[str]   = None
    price:            Optional[float] = None
    market_cap:       Optional[int]   = None
    price_change_24h: Optional[float] = None
    category:         Optional[str]   = None
    founding_year:    Optional[int]   = None
    origin_country:   Optional[str]   = None
    last_updated:     Optional[datetime] = None


# ── Response: ETL job history row ─────────────────────────────────────────────
class ETLJobResponse(BaseModel):
    """
    Returned by:
      GET /etl/jobs  (as a list, newest first)

    Maps to the etl_jobs table. error_message is None for successful runs.
    """

    model_config = ConfigDict(from_attributes=True)

    job_id:             str
    status:             str            # "running" | "success" | "failed"
    records_processed:  Optional[int]  = None
    started_at:         datetime
    finished_at:        Optional[datetime] = None
    error_message:      Optional[str]  = None


# ── Response: POST /etl/run ────────────────────────────────────────────────────
class ETLRunResponse(BaseModel):
    """
    Returned immediately by POST /etl/run.
    The pipeline runs synchronously, so by the time this is returned
    the status will already be "success" or "failed" (not "running").
    "running" would only appear if we moved to background tasks later.
    """

    job_id: str
    status: str   # "success" | "failed"