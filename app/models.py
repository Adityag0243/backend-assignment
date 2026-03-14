"""
models.py
─────────
WHAT THIS FILE IS:
  ORM table definitions. Each class here = one table in PostgreSQL.
  SQLAlchemy reads these classes and knows exactly what columns to create.

WHAT COMES IN:
  - Base from database.py  (parent class that registers these models with SQLAlchemy)

WHAT GOES OUT:
  → CryptoAsset  to etl_pipeline.py  (used in UPSERT during Load step)
  → CryptoAsset  to routes/assets.py (used in SELECT queries)
  → ETLJob       to etl_pipeline.py  (written at start/end of every pipeline run)
  → ETLJob       to routes/etl.py    (read for GET /etl/jobs history)

TABLES CREATED:
  crypto_assets — one row per coin, updated in place on each ETL run (UPSERT)
  etl_jobs      — one row per ETL run, append-only (audit log)
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import BigInteger, Float, Integer, Text, Timestamp
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


# ── Table 1: crypto_assets ─────────────────────────────────────────────────────
class CryptoAsset(Base):
    """
    Maps to the `crypto_assets` table in PostgreSQL.

    One row per cryptocurrency symbol.
    On each ETL run, rows are UPSERTed — existing rows are updated in place,
    new symbols get a fresh row. symbol is the unique key for conflict detection.

    Populated by: etl_pipeline.py → load()
    Queried by:   routes/assets.py
    """

    __tablename__ = "crypto_assets"

    # Auto-incrementing surrogate PK (Postgres SERIAL equivalent)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Ticker symbol — the UPSERT key. Must be unique so ON CONFLICT works.
    # Always stored lowercase (normalised in transform step).
    symbol: Mapped[str] = mapped_column(Text, unique=True, nullable=False, index=True)

    # Human-readable name from CoinGecko (e.g. "Bitcoin")
    name: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Current USD price from CoinGecko
    price: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Total market cap in USD — can be very large, so BigInteger not Integer
    market_cap: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # 24-hour price change percentage (can be negative)
    price_change_24h: Mapped[float | None] = mapped_column(Float, nullable=True)

    # From CSV — e.g. "store_of_value", "smart_contract". NULL if not in CSV.
    category: Mapped[str | None] = mapped_column(Text, nullable=True)

    # From CSV — year the project launched. NULL if not in CSV.
    founding_year: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # From CSV — e.g. "global", "usa", "china". NULL if not in CSV.
    origin_country: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Timestamp of the ETL run that last wrote this row.
    # Set in etl_pipeline.py transform step. Stored as UTC.
    last_updated: Mapped[datetime | None] = mapped_column(
        Timestamp(timezone=True), nullable=True
    )

    def __repr__(self) -> str:
        return f"<CryptoAsset symbol={self.symbol} price={self.price}>"


# ── Table 2: etl_jobs ──────────────────────────────────────────────────────────
class ETLJob(Base):
    """
    Maps to the `etl_jobs` table in PostgreSQL.

    One row per ETL pipeline run — this is the audit/lineage table.
    Rows are NEVER updated after being written (append-only).
    The pipeline writes a row at the start (status="running"),
    then updates it to "success" or "failed" when done.

    Written by: etl_pipeline.py → run_pipeline()
    Queried by: routes/etl.py   → GET /etl/jobs
    """

    __tablename__ = "etl_jobs"

    # UUID primary key — globally unique, safe to expose in API responses
    job_id: Mapped[str] = mapped_column(
        Text,
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )

    # "running" → set when job starts
    # "success" → set when pipeline completes without error
    # "failed"  → set when an exception is caught
    status: Mapped[str] = mapped_column(Text, nullable=False, default="running")

    # How many crypto_assets rows were upserted in this run
    records_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # UTC timestamp when run_pipeline() was called
    started_at: Mapped[datetime] = mapped_column(
        Timestamp(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    # UTC timestamp when the pipeline finished (success or fail)
    finished_at: Mapped[datetime | None] = mapped_column(
        Timestamp(timezone=True), nullable=True
    )

    # Populated only on failure — the exception message
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<ETLJob job_id={self.job_id} status={self.status}>"