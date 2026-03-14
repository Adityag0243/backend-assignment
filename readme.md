# Backend Assignment
# Crypto ETL Service

Mini ETL pipeline: pulls crypto market data from CoinGecko + a local CSV,
merges them, and loads into PostgreSQL. Exposed via FastAPI.

## Project structure

```
project/
├── app/
│   ├── main.py          ← FastAPI app + startup
│   ├── database.py      ← SQLAlchemy engine + session (Stage 2)
│   ├── models.py        ← ORM table definitions (Stage 2)
│   ├── schemas.py       ← Pydantic response models (Stage 6)
│   ├── etl_pipeline.py  ← Extract / Transform / Load logic (Stages 4+5)
│   ├── retry_utils.py   ← Retry decorator with backoff (Stage 3)
│   └── routes/
│       ├── etl.py       ← POST /etl/run, GET /etl/jobs (Stage 6)
│       └── assets.py    ← GET /assets, GET /assets/{symbol} (Stage 6)
├── data/
│   └── crypto_metadata.csv   ← local metadata source for ETL
├── .env                 ← secrets (never commit)
├── .env.example         ← safe template to commit
├── requirements.txt
└── README.md
```

## Setup

### 1. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment
```bash
cp .env.example .env
# Edit .env and set your DATABASE_URL
```

### 4. Create PostgreSQL database
```sql
CREATE DATABASE crypto_etl;
```

### 5. Run the server
```bash
uvicorn app.main:app --reload
```

### 6. Check it works
```
GET http://localhost:8000/health   → { "status": "ok" }
GET http://localhost:8000/docs     → Swagger UI
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Server health check |
| POST | `/etl/run` | Trigger ETL pipeline |
| GET | `/etl/jobs` | ETL run history |
| GET | `/assets` | All assets (optional filters) |
| GET | `/assets/{symbol}` | Single asset by symbol |