"""
database.py
───────────
WHAT THIS FILE IS:
  The single source of truth for the database connection.
  Every other file that needs to talk to PostgreSQL imports from here.

WHAT COMES IN:
  - DATABASE_URL from .env  (loaded by main.py at startup via load_dotenv())

WHAT GOES OUT:
  → engine        to main.py         (used in create_all() on startup)
  → SessionLocal  to routes/         (each request gets its own DB session)
  → Base          to models.py       (all ORM models inherit from this)
  → get_db()      to routes/         (FastAPI dependency injection)

HOW IT WORKS:
  1. Reads DATABASE_URL from environment
  2. Creates an SQLAlchemy engine (the actual connection pool to Postgres)
  3. Creates SessionLocal — a factory that stamps out new sessions per request
  4. Defines Base — the parent class all ORM models inherit from
  5. get_db() yields one session per HTTP request, then closes it cleanly
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase


# ── 1. Read the connection string from environment ─────────────────────────────
# Set by .env → loaded by load_dotenv() in main.py before this runs
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. "
        "Copy .env.example to .env and fill in your Postgres credentials."
    )


# ── 2. Create the engine ───────────────────────────────────────────────────────
# pool_pre_ping=True — tests the connection before handing it to a request.
# Prevents "connection closed" errors after Postgres restarts or idle timeouts.
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# ── 3. Session factory ─────────────────────────────────────────────────────────
# autocommit=False → we control commits manually (important for ETL atomicity)
# autoflush=False  → we flush manually so partial writes don't sneak into queries
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)


# ── 4. Declarative base ────────────────────────────────────────────────────────
# All ORM models in models.py inherit from this Base.
# Base.metadata.create_all(engine) in main.py reads those models and
# creates the actual tables in Postgres if they don't exist yet.
class Base(DeclarativeBase):
    pass


# ── 5. Dependency: one session per request ─────────────────────────────────────
# FastAPI injects this into route handlers via:  db: Session = Depends(get_db)
# The try/finally guarantees the session is always closed, even on errors.
def get_db():
    """
    Yields a database session for the duration of one HTTP request.

    Usage in a route:
        from app.database import get_db
        from sqlalchemy.orm import Session
        from fastapi import Depends

        @router.get("/example")
        def example(db: Session = Depends(get_db)):
            return db.query(...)...
    """
    db = SessionLocal()
    try:
        yield db          # hand the session to the route handler
    finally:
        db.close()        # always close, even if the handler raised an exception