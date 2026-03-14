"""
schemas.py
──────────
WHAT THIS FILE IS:
  Pydantic models that define the exact shape of every API request and response.
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
  → ETLRunRequest        to routes/etl.py     (POST /etl/run  request body)
  → CryptoAssetResponse  to routes/assets.py  (GET /assets, GET /assets/{symbol})
  → ETLJobResponse       to routes/etl.py     (GET /etl/jobs)
  → ETLRunResponse       to routes/etl.py     (POST /etl/run  response)

model_config = ConfigDict(from_attributes=True):
  Tells Pydantic to read values from ORM object attributes (e.g. asset.price)
  instead of expecting a plain dict. Required for SQLAlchemy → Pydantic conversion.
"""

from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, ConfigDict, Field


# ── Request: POST /etl/run ─────────────────────────────────────────────────────
class ETLRunRequest(BaseModel):
    """
    Optional request body for POST /etl/run.

    All fields have defaults so calling POST /etl/run with an empty body
    works exactly as before. Override any field to customise the pipeline run.

    Example body (all optional):
      {
        "per_page": 20,
        "page": 1,
        "vs_currency": "usd",
        "order": "market_cap_desc"
      }
    """

    per_page: int = Field(
        default=10,
        ge=1,
        le=250,
        description=(
            "How many coins to fetch from CoinGecko per page. "
            "Free tier supports up to 250. Default: 10."
        ),
    )

    page: int = Field(
        default=1,
        ge=1,
        description=(
            "Which page of CoinGecko results to fetch (1-indexed). "
            "page=1 per_page=10 → top 10 coins. "
            "page=2 per_page=10 → coins ranked 11–20. "
            "Default: 1."
        ),
    )

    vs_currency: str = Field(
        default="usd",
        min_length=3,
        max_length=10,
        description=(
            "Currency to price coins in. "
            "Any CoinGecko-supported currency code (e.g. usd, eur, inr, btc). "
            "Default: usd."
        ),
    )

    order: Literal[
        "market_cap_desc",
        "market_cap_asc",
        "volume_desc",
        "volume_asc",
        "id_desc",
        "id_asc",
    ] = Field(
        default="market_cap_desc",
        description=(
            "How to sort results from CoinGecko. "
            "Default: market_cap_desc (highest market cap first)."
        ),
    )


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

    job_id:           str
    status:           str              # "success" | "failed"
    records_processed: Optional[int]  = None  # how many rows upserted