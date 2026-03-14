"""
routes/assets.py
────────────────
WHAT THIS FILE IS:
  FastAPI route handlers for querying the crypto_assets table.

ENDPOINTS:
  GET /assets              — all assets, with optional filters
  GET /assets/{symbol}     — single asset by symbol (e.g. /assets/btc)

WHAT COMES IN:
  GET /assets:
    Optional query params:
      ?category=store_of_value  — filter by category (exact match)
      ?limit=5                  — max rows returned (default 50, cap 100)
    db session via Depends(get_db)

  GET /assets/{symbol}:
    Path param: symbol (str) — e.g. "btc", "eth"
    Normalised to lowercase before querying so /assets/BTC works too.
    db session via Depends(get_db)

WHAT GOES OUT:
  GET /assets          → list[CryptoAssetResponse]
  GET /assets/{symbol} → CryptoAssetResponse  (or 404 if not found)

DEPENDS ON:
  app/database.py  → get_db()
  app/models.py    → CryptoAsset ORM model
  app/schemas.py   → CryptoAssetResponse
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import CryptoAsset
from app.schemas import CryptoAssetResponse

logger = logging.getLogger(__name__)

router = APIRouter()


# ── GET /assets ────────────────────────────────────────────────────────────────
@router.get(
    "/",
    response_model=List[CryptoAssetResponse],
    summary="List all crypto assets",
    description=(
        "Returns all assets loaded by the ETL pipeline. "
        "Optionally filter by category or limit the number of results."
    ),
)
def get_assets(
    category: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """
    Lists crypto assets with optional filters.

    Query params:
      category  (str,  optional) — exact match on the category column
                                   e.g. ?category=store_of_value
      limit     (int,  default 50) — max rows to return (capped at 100)

    QUERY (no filter):
      SELECT * FROM crypto_assets ORDER BY market_cap DESC LIMIT :limit

    QUERY (with category):
      SELECT * FROM crypto_assets
      WHERE category = :category
      ORDER BY market_cap DESC
      LIMIT :limit

    Ordered by market_cap DESC so highest-value coins appear first —
    matches what users expect from a crypto list.
    """
    limit = min(limit, 100)  # hard cap — never return more than 100 rows

    # Start with the base query
    query = db.query(CryptoAsset)

    # ── Apply optional category filter ────────────────────────────────────────
    # SQLAlchemy parameterises this automatically — no SQL injection risk
    if category:
        query = query.filter(CryptoAsset.category == category)

    assets = (
        query
        .order_by(CryptoAsset.market_cap.desc().nullslast())  # NULLs go to end
        .limit(limit)
        .all()
    )

    logger.info(
        f"[Route] GET /assets — category={category!r} limit={limit} "
        f"→ {len(assets)} rows"
    )
    return assets


# ── GET /assets/{symbol} ───────────────────────────────────────────────────────
@router.get(
    "/{symbol}",
    response_model=CryptoAssetResponse,
    summary="Get asset by symbol",
    description="Returns a single crypto asset by its ticker symbol (e.g. btc, eth).",
)
def get_asset_by_symbol(
    symbol: str,
    db: Session = Depends(get_db),
):
    """
    Returns one asset by symbol.

    Path param:
      symbol  (str) — ticker symbol. Normalised to lowercase before querying
                      so /assets/BTC and /assets/btc both work.

    QUERY:
      SELECT * FROM crypto_assets WHERE symbol = :symbol LIMIT 1

    Returns 404 if no asset with that symbol exists in the database.
    This means the ETL hasn't been run yet, or that coin isn't in our top-10.
    """
    # Normalise case — the DB always stores lowercase symbols
    symbol_lower = symbol.lower().strip()

    asset = (
        db.query(CryptoAsset)
        .filter(CryptoAsset.symbol == symbol_lower)
        .first()  # returns None if not found
    )

    if asset is None:
        logger.warning(f"[Route] GET /assets/{symbol_lower} — not found")
        raise HTTPException(
            status_code=404,
            detail=f"Asset '{symbol_lower}' not found. "
                   f"Run POST /etl/run first to load data.",
        )

    logger.info(f"[Route] GET /assets/{symbol_lower} — found: {asset.name}")
    return asset