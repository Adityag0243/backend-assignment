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

## Deploying to Render

### 1. Push to GitHub
```bash
git init && git add . && git commit -m "initial"
git remote add origin https://github.com/YOUR_USERNAME/crypto-etl.git
git push -u origin main
```

### 2. Create a PostgreSQL database on Render
- Render Dashboard → New → PostgreSQL
- Copy the **Internal Database URL** (used within Render's network)

### 3. Create a Web Service on Render
- New → Web Service → connect your GitHub repo
- **Environment:** Python 3
- **Build command:** `pip install -r requirements.txt`
- **Start command:** `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

### 4. Set environment variables in Render
```
DATABASE_URL      = <Internal Database URL from step 2>
COINGECKO_BASE_URL = https://api.coingecko.com/api/v3
CSV_PATH          = data/crypto_metadata.csv
```

### 5. Deploy and verify
```
GET https://your-service.onrender.com/health   → {"status": "ok"}
GET https://your-service.onrender.com/docs     → Swagger UI
POST https://your-service.onrender.com/etl/run → triggers the pipeline
```