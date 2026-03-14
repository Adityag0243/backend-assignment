"""
etl_pipeline.py
───────────────
WHAT THIS FILE IS:
  The core of the entire project. Implements the full ETL pipeline:
    Extract  → extract_from_csv()  +  extract_from_api()
    Transform → transform()
    Load      → load()
  Orchestrated by run_pipeline(), which also writes the etl_jobs audit row.

WHAT COMES IN:
  Stage 4a — extract_from_csv():
    CSV file path from .env → CSV_PATH
    Reads:  data/crypto_metadata.csv

  Stage 4b — extract_from_api():
    CoinGecko base URL from .env → COINGECKO_BASE_URL
    HTTP GET to /coins/markets

  Stage 5 — transform() + load() + run_pipeline():
    db session from database.py (passed in by routes/etl.py)

WHAT GOES OUT:
  → list[dict]  from extract_from_csv()  →  into transform()
  → list[dict]  from extract_from_api()  →  into transform()
  → list[dict]  from transform()         →  into load()
  → int         from load()              →  records_processed count in ETLJob
  → dict        from run_pipeline()      →  back to routes/etl.py as API response
                                            {"job_id": ..., "status": ...}
"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests

# pg_insert is used in load() for the UPSERT statement.
# Imported at module level so tests can patch it via app.etl_pipeline.pg_insert.
# Guarded so the file still imports cleanly in environments without SQLAlchemy
# (e.g. running only the transform tests).
try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except ImportError:
    pg_insert = None  # type: ignore[assignment]

from app.retry_utils import retry_with_backoff

logger = logging.getLogger(__name__)

# ── Config from environment ────────────────────────────────────────────────────
# COINGECKO_BASE_URL is safe to read at import time (doesn't change per-call)
# CSV_PATH is read inside extract_from_csv() so tests can override it via env
COINGECKO_BASE_URL = os.environ.get(
    "COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"
)

# Fields we want from the CoinGecko response (ignore the rest)
API_FIELDS = ["id", "symbol", "name", "current_price", "market_cap",
              "price_change_percentage_24h"]


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4a — Extract from CSV
# ══════════════════════════════════════════════════════════════════════════════

def extract_from_csv() -> list[dict]:
    """
    Stage 4a — Load local CSV metadata into a list of normalised dicts.

    SOURCE:  data/crypto_metadata.csv  (path set by CSV_PATH in .env)
    COLUMNS: symbol, category, founding_year, origin_country

    NORMALISATION done here:
      - symbol → lowercase, stripped of whitespace
        Reason: CoinGecko returns "btc", CSV might contain "BTC" or " btc "

    RETURNS:
      list[dict] — one dict per CSV row, e.g.:
        [
          {"symbol": "btc", "category": "store_of_value",
           "founding_year": 2009, "origin_country": "global"},
          ...
        ]

    RAISES:
      FileNotFoundError if CSV_PATH does not exist
      ValueError        if required columns are missing from the CSV
    """
    # Read at call time (not module-level) so tests can override via os.environ
    csv_path = os.environ.get("CSV_PATH", "data/crypto_metadata.csv")
    logger.info(f"[Extract CSV] Reading from: {csv_path}")

    # ── Read the CSV file ──────────────────────────────────────────────────────
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"CSV metadata file not found at '{csv_path}'. "
            "Check CSV_PATH in your .env file."
        )

    # ── Validate required columns exist ───────────────────────────────────────
    required_cols = {"symbol", "category", "founding_year", "origin_country"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {missing}. "
            f"Found: {list(df.columns)}"
        )

    # ── Normalise symbol to lowercase + strip whitespace ──────────────────────
    # This ensures "BTC", "btc", " Btc " all match the API's "btc"
    df["symbol"] = df["symbol"].str.lower().str.strip()

    # ── Drop rows where symbol is empty or NaN (unusable for merging) ─────────
    before = len(df)
    df = df.dropna(subset=["symbol"])
    df = df[df["symbol"] != ""]
    dropped = before - len(df)
    if dropped:
        logger.warning(f"[Extract CSV] Dropped {dropped} rows with empty symbol")

    # ── Convert to list of plain dicts (easier to merge with API data) ─────────
    records = df.to_dict(orient="records")
    logger.info(f"[Extract CSV] Loaded {len(records)} metadata rows")
    return records


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 4b — Extract from CoinGecko API
# ══════════════════════════════════════════════════════════════════════════════

@retry_with_backoff(max_attempts=3, base_delay=1.0)
def _fetch_coingecko_page(page: int) -> list[dict]:
    """
    Private helper — fetches a single page from the CoinGecko /coins/markets endpoint.
    Decorated with @retry_with_backoff so retries + 429 handling are automatic.

    Called by: extract_from_api()
    NOT called directly by anything else.

    Args:
        page: page number (1-indexed)

    Returns:
        Raw JSON list from the API response

    Raises:
        requests.HTTPError  on 4xx/5xx after all retries exhausted
        requests.Timeout    if the request takes too long
    """
    url = f"{COINGECKO_BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": page,
    }

    logger.info(f"[Extract API] GET {url} page={page}")
    response = requests.get(url, params=params, timeout=10)

    # raise_for_status() converts 4xx/5xx into requests.HTTPError
    # which retry_with_backoff catches and applies backoff/429 logic to
    response.raise_for_status()

    return response.json()


def extract_from_api() -> list[dict]:
    """
    Stage 4b — Fetch the top 10 coins by market cap from CoinGecko.

    SOURCE:  GET https://api.coingecko.com/api/v3/coins/markets
    FIELDS KEPT: id, symbol, name, current_price, market_cap,
                 price_change_percentage_24h

    NORMALISATION done here:
      - symbol → lowercase  (CoinGecko already returns lowercase, but be safe)
      - Only the 6 required fields are kept — everything else from the API
        response is discarded to keep our data model clean

    RETURNS:
      list[dict] — one dict per coin, e.g.:
        [
          {"symbol": "btc", "name": "Bitcoin",
           "current_price": 65000.0, "market_cap": 1280000000000,
           "price_change_percentage_24h": 1.23},
          ...
        ]

    RAISES:
      requests.HTTPError after 3 failed attempts (re-raised by retry decorator)
    """
    raw_coins = _fetch_coingecko_page(page=1)

    # ── Keep only the fields we need, normalise symbol case ───────────────────
    coins = []
    for coin in raw_coins:
        coins.append({
            "id":                       coin.get("id"),
            "symbol":                   (coin.get("symbol") or "").lower().strip(),
            "name":                     coin.get("name"),
            "current_price":            coin.get("current_price"),
            "market_cap":               coin.get("market_cap"),
            "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
        })

    # ── Drop any coin with an empty symbol (can't merge or store it) ──────────
    before = len(coins)
    coins = [c for c in coins if c["symbol"]]
    if len(coins) < before:
        logger.warning(f"[Extract API] Dropped {before - len(coins)} coins with empty symbol")

    logger.info(f"[Extract API] Fetched {len(coins)} coins from CoinGecko")
    return coins


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 5 — Transform
# ══════════════════════════════════════════════════════════════════════════════

def transform(api_coins: list[dict], csv_rows: list[dict]) -> list[dict]:
    """
    Stage 5 — Merge API data with CSV metadata and produce the final schema.

    WHAT COMES IN:
      api_coins  — from extract_from_api():  list of dicts with keys:
                   symbol, name, current_price, market_cap,
                   price_change_percentage_24h
      csv_rows   — from extract_from_csv():  list of dicts with keys:
                   symbol, category, founding_year, origin_country

    WHAT GOES OUT:
      list[dict] — one dict per coin, ready to UPSERT into crypto_assets:
        {
          "symbol":           "btc",
          "name":             "Bitcoin",
          "price":            65000.0,
          "market_cap":       1280000000000,
          "price_change_24h": 1.23,
          "category":         "store_of_value",   # None if not in CSV
          "founding_year":    2009,               # None if not in CSV
          "origin_country":   "global",           # None if not in CSV
          "last_updated":     datetime(...)       # UTC, set here
        }

    HANDLES:
      Missing metadata  — coin in API but not in CSV → category/founding_year/
                          origin_country all set to None (not an error)
      Case mismatch     — both sides normalised to lowercase before merge
                          so "BTC" in CSV matches "btc" from API
      Duplicate symbols — if either source has duplicate symbols, we keep
                          the first occurrence (API coins are already unique
                          from CoinGecko; CSV dedup is a safety measure)
    """
    logger.info(f"[Transform] Merging {len(api_coins)} API coins with {len(csv_rows)} CSV rows")

    # ── Build a symbol → metadata lookup dict from CSV ────────────────────────
    # Keyed by lowercase symbol for O(1) lookup during merge.
    # If CSV has duplicate symbols (shouldn't happen but be safe),
    # the last one wins — log a warning so it's visible.
    csv_lookup: dict[str, dict] = {}
    for row in csv_rows:
        sym = row["symbol"]  # already lowercased by extract_from_csv()
        if sym in csv_lookup:
            logger.warning(f"[Transform] Duplicate symbol in CSV: '{sym}' — keeping last occurrence")
        csv_lookup[sym] = row

    # ── Merge: for each API coin, look up its CSV metadata ────────────────────
    now_utc = datetime.now(timezone.utc)
    merged: list[dict] = []
    seen_symbols: set[str] = set()

    for coin in api_coins:
        sym = coin["symbol"]  # already lowercased by extract_from_api()

        # ── Deduplicate API coins (defensive — CoinGecko shouldn't send dupes) ─
        if sym in seen_symbols:
            logger.warning(f"[Transform] Duplicate symbol from API: '{sym}' — skipping")
            continue
        seen_symbols.add(sym)

        # ── Look up CSV metadata (None if coin not in CSV — that's fine) ──────
        meta = csv_lookup.get(sym)  # returns None if symbol not in CSV
        if meta is None:
            logger.info(f"[Transform] No CSV metadata for '{sym}' — fields will be NULL")

        # ── Build the final row matching crypto_assets schema ─────────────────
        merged.append({
            "symbol":           sym,
            "name":             coin.get("name"),
            "price":            coin.get("current_price"),
            "market_cap":       coin.get("market_cap"),
            "price_change_24h": coin.get("price_change_percentage_24h"),
            # CSV fields — all default to None if the coin wasn't in the CSV
            "category":         meta.get("category")      if meta else None,
            "founding_year":    meta.get("founding_year") if meta else None,
            "origin_country":   meta.get("origin_country") if meta else None,
            # Timestamp of this ETL run
            "last_updated":     now_utc,
        })

    logger.info(f"[Transform] Produced {len(merged)} merged rows")
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 5 — Load (UPSERT into PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════════

def load(rows: list[dict], db) -> int:
    """
    Stage 5 — UPSERT transformed rows into the crypto_assets table.

    WHAT COMES IN:
      rows  — from transform():   list of dicts with final schema
      db    — SQLAlchemy Session: injected by run_pipeline() from the route

    WHAT GOES OUT:
      int — count of rows successfully upserted
            → stored in ETLJob.records_processed by run_pipeline()

    IDEMPOTENCY:
      Uses PostgreSQL's ON CONFLICT(symbol) DO UPDATE.
      Running this function 10 times with the same data produces exactly
      the same result as running it once — no duplicates, just updates.

      symbol is the conflict key because it's the natural business identity
      of a cryptocurrency. id (SERIAL) is just a surrogate key for joins.

    WHY RAW SQL FOR THE UPSERT:
      SQLAlchemy's ORM doesn't have a built-in upsert for all backends.
      We use sqlalchemy.dialects.postgresql.insert() which generates the
      correct PostgreSQL INSERT ... ON CONFLICT ... DO UPDATE syntax.
      This is still parameterised (no string formatting of values) so
      SQL injection via the ORM layer is not possible.
    """
    from app.models import CryptoAsset

    if not rows:
        logger.warning("[Load] No rows to upsert — skipping")
        return 0

    logger.info(f"[Load] Upserting {len(rows)} rows into crypto_assets")

    # ── Build the upsert statement ─────────────────────────────────────────────
    # pg_insert(CryptoAsset) generates:
    #   INSERT INTO crypto_assets (symbol, name, price, ...) VALUES (...)
    #   ON CONFLICT (symbol) DO UPDATE SET name=EXCLUDED.name, price=EXCLUDED.price, ...
    #
    # EXCLUDED refers to the row that *would* have been inserted.
    # This ensures existing rows are updated with fresh API data on every run.

    stmt = pg_insert(CryptoAsset).values(rows)

    # Columns to update on conflict — everything except the PK and the symbol key
    update_cols = {
        col.name: stmt.excluded[col.name]
        for col in CryptoAsset.__table__.columns
        if col.name not in ("id", "symbol")  # never overwrite PK or conflict key
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol"],   # the column(s) that trigger a conflict
        set_=update_cols,            # what to update when conflict is detected
    )

    # ── Execute and commit ─────────────────────────────────────────────────────
    # execute() runs the upsert for ALL rows in a single round-trip to Postgres
    # commit() makes the changes permanent and visible to other connections
    db.execute(stmt)
    db.commit()

    logger.info(f"[Load] Successfully upserted {len(rows)} rows")
    return len(rows)


# ══════════════════════════════════════════════════════════════════════════════
# STAGE 5 — Orchestrator: run_pipeline()
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(db) -> dict:
    """
    Orchestrates the full ETL pipeline: Extract → Transform → Load.
    Creates and updates an ETLJob audit record throughout.

    WHAT COMES IN:
      db — SQLAlchemy Session from routes/etl.py via Depends(get_db)

    WHAT GOES OUT:
      dict → back to routes/etl.py as the API response body:
        {"job_id": "uuid-string", "status": "success" | "failed"}

    JOB LIFECYCLE:
      1. Create ETLJob row with status="running", started_at=now
      2. Commit immediately so the job is visible in GET /etl/jobs right away
      3. Run Extract → Transform → Load
      4a. Success → update status="success", records_processed=N, finished_at=now
      4b. Failure → update status="failed", error_message=str(exc), finished_at=now
      5. Commit final status
      6. Return job_id + status to the route handler

    DATA LINEAGE:
      Every ETL run is tracked in etl_jobs regardless of success or failure.
      The error_message field captures the full exception for debugging.
      This satisfies the "Data Lineage Tracking" requirement.
    """
    import uuid as _uuid
    from app.models import ETLJob

    # ── Step 1: Create the job record ─────────────────────────────────────────
    job = ETLJob(
        job_id=str(_uuid.uuid4()),
        status="running",
        started_at=datetime.now(timezone.utc),
    )
    db.add(job)
    db.commit()  # commit now so GET /etl/jobs shows "running" immediately
    logger.info(f"[Pipeline] Started job {job.job_id}")

    # ── Steps 2–4: Run ETL with full error capture ────────────────────────────
    try:
        # EXTRACT
        api_coins = extract_from_api()    # → list[dict] from CoinGecko
        csv_rows  = extract_from_csv()    # → list[dict] from CSV

        # TRANSFORM
        merged = transform(api_coins, csv_rows)  # → list[dict] final schema

        # LOAD
        count = load(merged, db)          # → int: rows upserted

        # ── Step 4a: Mark success ─────────────────────────────────────────────
        job.status             = "success"
        job.records_processed  = count
        job.finished_at        = datetime.now(timezone.utc)
        db.commit()

        logger.info(f"[Pipeline] Job {job.job_id} succeeded — {count} records processed")
        return {"job_id": job.job_id, "status": "success"}

    except Exception as exc:
        # ── Step 4b: Mark failure — capture the error message ─────────────────
        # Roll back any partial writes from this run before writing the failure
        db.rollback()

        job.status          = "failed"
        job.error_message   = str(exc)
        job.finished_at     = datetime.now(timezone.utc)
        db.add(job)   # re-add after rollback
        db.commit()

        logger.error(f"[Pipeline] Job {job.job_id} failed: {exc}", exc_info=True)
        return {"job_id": job.job_id, "status": "failed"}