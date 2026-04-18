# SCADA Alarm System — Backend

A production-grade data pipeline and REST API for processing, storing, and querying industrial SCADA alarm data. Built as a technical evaluation for an industrial client modernizing their infrastructure.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Dataset Design](#3-dataset-design)
4. [Data Cleaning Strategy](#4-data-cleaning-strategy)
5. [Database Design](#5-database-design)
6. [API Design](#6-api-design)
7. [Performance Considerations](#7-performance-considerations)
8. [How to Run](#8-how-to-run)
9. [Testing](#9-testing)
10. [Future Improvements](#10-future-improvements)

---

## 1. Project Overview

An industrial client has legacy SCADA alarm exports in CSV/JSON with typical real-world data quality problems: null values, inconsistent date formats, mixed casing, duplicate records, embedded units in numeric fields, and invalid timestamps.

This system:
- **Generates** a realistic synthetic dataset (10,000+ rows) with 14 intentional quality issues
- **Processes** it through a modular ETL pipeline (ingest → clean → normalize → load)
- **Stores** normalized records in a relational database (SQLite, portable to SQL Server)
- **Exposes** them via a FastAPI REST API with filtering, pagination, and aggregated metrics

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  FastAPI REST API                                             │
│                                                              │
│  ┌─────────────┐  ┌──────────────────┐  ┌─────────────────┐ │
│  │   Routers   │  │    Services      │  │  ETL Pipeline   │ │
│  │  /alarms    │→ │  AlarmService    │  │ generate        │ │
│  │  /metrics   │→ │  MetricsService  │  │ → ingest        │ │
│  │  /etl       │→ │                  │  │ → clean         │ │
│  └─────────────┘  └──────────────────┘  │ → normalize     │ │
│                           │             │ → load          │ │
│                           │             └────────┬────────┘ │
└───────────────────────────┼──────────────────────┼──────────┘
                            ▼                      ▼
                   ┌──────────────────────────────────┐
                   │  SQLite (SQL Server-portable)     │
                   │  tables: alarms, tags             │
                   │  5 indexes on alarms              │
                   └──────────────────────────────────┘
```

### Design Principles

- **Separation of concerns**: ETL, API, and DB layers are fully independent modules
- **Defensive ingestion**: all input is read as raw strings — type coercion happens in normalize, not ingest
- **DB-level operations**: all filtering, grouping, and pagination run in SQL — no Python-level post-processing of query results
- **Bulk operations**: ETL loads use chunked SQLAlchemy Core inserts, not row-by-row ORM adds
- **Portability**: SQLite for zero-infrastructure deployment; schema and queries are SQL Server-compatible with a connection string swap

---

## 3. Dataset Design

The generator (`etl/generate.py`) produces 10,000+ rows of realistic industrial alarm data simulating a SCADA export with 14 intentional data quality issues. The RNG seed is fixed (`numpy.random.default_rng(42)`) for reproducibility.

### Realistic Domain

| Field | Examples |
|---|---|
| Tags | FIC-101, TIC-201, PIC-301, LIC-401, AI-901 (25 tags) |
| Areas | Reactor, Distillation, Compression, Storage, Utilities |
| Systems | DCS, SCADA, PLC, HMI |
| Criticality | CRITICAL (15%), HIGH (25%), MEDIUM (35%), LOW (25%) |
| Status | ACTIVE (50%), ACKNOWLEDGED (30%), CLEARED (20%) |
| Date range | 2023-01-01 → 2024-06-30 |

### Intentional Data Quality Issues

| # | Issue | Scope | Simulates |
|---|---|---|---|
| 01 | **NULL criticality** | ~8% | Source system not exporting priority field |
| 02 | **NULL value** | ~12% | Sensor offline or communication failure |
| 03 | **NULL tag** | ~3% | Export bug — tag field empty |
| 04 | **NULL description** | ~15% | Optional field not populated |
| 05 | **Mixed datetime formats** | All rows (4 rotating) | Different SCADA export configurations |
| 06 | **Mixed criticality casing** | ~30% | Multiple source systems with different conventions |
| 07 | **Boolean status variants** | ~25% | Some systems export ACTIVE/CLEARED as true/false |
| 08 | **Exact duplicates** | ~5% | Double-export from historian |
| 09 | **Near-duplicates** | ~3% | Same event logged by two systems |
| 10 | **Values with embedded units** | ~10% | e.g., "45.2 psi", "300 rpm" |
| 11 | **Invalid/unparseable timestamps** | ~2% | Garbage strings: "N/A", "####", "" |
| 12 | **Future timestamps** | ~2% | Clock drift in field devices |
| 13 | **Tag name inconsistencies** | ~20% | fic-101, FIC101, FIC_101, " FIC-101" |
| 14 | **Leading/trailing whitespace** | ~5% | Manual data entry or export artifacts |

**Date format variants (Issue 05):**
```
Format 0: 2023-06-15 14:30:00       ISO-ish (most common)
Format 1: 06/15/2023 14:30          US format
Format 2: 15-Jun-2023 14:30:00      Day-Mon-Year
Format 3: 1686836400                Unix epoch as string
```

---

## 4. Data Cleaning Strategy

The ETL pipeline applies transformations in a strict sequence. Each step is a separate module with a single responsibility.

### Step 1: Ingest (`etl/ingest.py`)

**Decision**: All columns read as `dtype=str` with `keep_default_na=False`.

**Why**: If pandas auto-converts, it silently turns `"NULL"` into `NaN` and `"1"` into `int` before we can log them as issues. We need to observe the raw data exactly as it came from the source, then normalize explicitly.

### Step 2: Clean (`etl/clean.py`)

| Problem | Strategy | Why |
|---|---|---|
| Whitespace (Issue 14) | `str.strip()` on all string columns | Must happen before sentinel detection |
| NULL sentinels | Replace `"NULL"`, `"N/A"`, `"undefined"`, `""` with `NaN` | Uniform downstream handling |
| Exact duplicates (Issue 08) | `drop_duplicates(subset=[tag, timestamp, criticality, value])` | Key excludes `source` — same event from two systems is a near-dupe |
| Near-duplicates (Issue 09) | Sort by source (non-null first), then `drop_duplicates(subset=[tag, timestamp, value])` | Preserves the row with more information |
| NULL tag (Issue 03) | **Reject row** | No tag = no meaningful alarm; cannot resolve FK |
| NULL criticality (Issue 01) | **Impute "UNKNOWN"** | Criticality is important but not fatal; UNKNOWN is a valid queryable value |
| NULL description (Issue 04) | **Impute "[NO DESCRIPTION]"** | Preserves the row while making the absence explicit |

**Reject vs Impute**: rows are rejected only when missing data makes the record structurally useless. All other nulls are imputed and logged in run stats.

### Step 3: Normalize (`etl/normalize.py`)

| Problem | Strategy |
|---|---|
| Mixed datetime formats (Issue 05) | Try explicit format list in order; regex-detect Unix epoch strings separately |
| Future timestamps (Issue 12) | Clamp to `now()` — event happened, device clock is wrong |
| Unparseable timestamps (Issue 11) | Set to `NaT` → row rejected in final step |
| Mixed criticality casing (Issue 06) | Explicit `CRITICALITY_MAP` dict covers all known variants; unknown → UNKNOWN |
| Boolean status variants (Issue 07) | `STATUS_MAP`: `{"true": "ACTIVE", "1": "ACTIVE", "false": "CLEARED", ...}` |
| Values with units (Issue 10) | Regex `^([+-]?\d+(?:\.\d+)?)` extracts leading numeric; non-matching → NULL |
| Tag inconsistencies (Issue 13) | `strip().upper().replace('_', '-')` + remove internal whitespace |
| Tag ID resolution | Upsert into `tags` table with name→id cache; O(unique_tags) queries total |

### Step 4: Load (`etl/load.py`)

Bulk-insert using **SQLAlchemy Core `insert()`** in chunks of 500. See [Performance Considerations](#7-performance-considerations).

---

## 5. Database Design

### Tables

```sql
-- Dimension table: normalized tag names with optional metadata
CREATE TABLE tags (
    id          INTEGER      PRIMARY KEY AUTOINCREMENT,
    name        VARCHAR(100) NOT NULL,
    area        VARCHAR(100),
    system      VARCHAR(100),
    description VARCHAR(500)
);

-- Fact table: one row per alarm event
CREATE TABLE alarms (
    id            INTEGER        PRIMARY KEY AUTOINCREMENT,
    tag_id        INTEGER        REFERENCES tags(id) ON DELETE SET NULL,
    raw_tag       VARCHAR(100)   NOT NULL,     -- original value before normalization
    description   VARCHAR(500),
    criticality   VARCHAR(20)    NOT NULL DEFAULT 'UNKNOWN',
    timestamp     DATETIME       NOT NULL,
    value         NUMERIC(10,4),              -- SQL Server-compatible precision
    unit          VARCHAR(20),
    status        VARCHAR(20)    NOT NULL DEFAULT 'ACTIVE',
    source        VARCHAR(100),
    quality_notes VARCHAR(500),               -- tracks which cleaning steps applied
    ingested_at   DATETIME       NOT NULL     -- server-set, not from source
);
```

**Why a separate `tags` table?**
- `GROUP BY tag_id` (integer) is faster than `GROUP BY tag_name` (string compare)
- Area/system metadata lives in one place
- ETL idempotency: subsequent runs detect existing tags without duplicates
- `raw_tag` preserved on `alarms` for audit trail

### Index Strategy

| Index | Columns | Justification |
|---|---|---|
| `ix_alarms_timestamp` | `timestamp` | Primary filter for all time-range queries — every date-filtered alarm list and dashboard call uses this |
| `ix_alarms_criticality` | `criticality` | Low-cardinality (5 values) but high-frequency filter; used by `/alarms?criticality=CRITICAL` and `/metrics/by-criticality` |
| `ix_alarms_tag_id` | `tag_id` | FK index — without it every JOIN between `alarms` and `tags` triggers a full scan |
| `ix_alarms_ts_crit` | `timestamp, criticality` | Composite covering index for the dominant combined query pattern; planner prefers this over merging two separate indexes |
| `ix_alarms_ingested_at` | `ingested_at` | ETL audit: `SELECT MAX(ingested_at)` to inspect last run |
| `uix_tags_name` | `name` | Unique — prevents duplicate tags; speeds up ETL `WHERE name = ?` lookup |

### SQL Server Portability

Schema uses only ANSI SQL types (`NUMERIC(10,4)`, `DATETIME`, `VARCHAR(n)`, `ON DELETE SET NULL`).

**To migrate**: swap `DATABASE_URL` to `mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server`. Replace `func.strftime()` in `metrics_service.py` with `DATEADD/DATEDIFF` (documented in the file). No schema changes required.

---

## 6. API Design

**Base URL**: `http://localhost:8000/api/v1`

All responses use a consistent envelope: paginated lists return `{ data: [...], meta: {...} }`, errors return `{ error: { message, detail } }`.

### Endpoints

#### `GET /alarms`

| Param | Type | Description |
|---|---|---|
| `start_date` | ISO 8601 | Filter from (inclusive) |
| `end_date` | ISO 8601 | Filter to (inclusive) |
| `criticality` | Enum, repeatable | `CRITICAL` \| `HIGH` \| `MEDIUM` \| `LOW` \| `UNKNOWN` |
| `tag` | string | Partial match on tag name (case-insensitive) |
| `page` | int ≥ 1 | Page number (default 1) |
| `size` | int 1–500 | Items per page (default 50) |

```json
{
  "data": [
    {
      "id": 42, "raw_tag": "FIC-101", "criticality": "CRITICAL",
      "timestamp": "2023-06-15T14:30:00", "value": 87.5, "unit": "m3/h",
      "status": "ACTIVE",
      "tag": { "id": 1, "name": "FIC-101", "area": "Reactor", "system": "DCS" }
    }
  ],
  "meta": { "page": 1, "size": 50, "total": 9243, "pages": 185 }
}
```

#### `GET /alarms/{id}`
Single alarm by ID or 404.

#### `GET /metrics/top-tags?limit=10`
Top N tags by total alarm count. `limit` range: 1–100.

#### `GET /metrics/by-criticality`
Count + percentage per criticality level. Percentages sum to 100.

#### `GET /metrics/timeline?interval=day`
Alarm counts bucketed by `hour` or `day`. Optional `start_date` / `end_date`. Returns `critical_count` and `high_count` per bucket.

#### `POST /etl/run`
Trigger the ETL pipeline. Returns full run statistics. Returns 409 if already running.

### Error Responses

| Code | Condition |
|---|---|
| 400 | Malformed input (invalid date string) |
| 404 | Resource not found |
| 409 | ETL already running |
| 422 | Validation failed (enum, range, date order) |
| 500 | Unexpected server error (no internals exposed) |

---

## 7. Performance Considerations

### Bulk Insert

Row-by-row ORM inserts generate one `INSERT` per row (~10,000 round-trips for 10k rows). Chunked Core inserts generate one multi-row `INSERT` per chunk.

```python
# Slow — 10,000 round-trips
for record in records:
    db.add(Alarm(**record))

# Fast — 20 round-trips for 10,000 rows at chunk_size=500
db.execute(insert(Alarm), chunk_of_500_dicts)
```

Benchmark on 10,000 rows:
- Row-by-row ORM: ~15–30 seconds
- Chunked Core insert: ~0.5–2 seconds (~15× faster)

### DB-Level Filtering

All `WHERE`, `GROUP BY`, and `LIMIT/OFFSET` run in SQL. Only the requested page transfers from DB to API. Memory usage scales with `page_size`, not dataset size.

### SQLite WAL Mode

`PRAGMA journal_mode=WAL` allows concurrent reads during an ETL write. Without WAL, any dashboard query during ETL would block.

---

## 8. How to Run

### Option A: Docker (Recommended)

```bash
# Build and start
docker compose up --build

# API:      http://localhost:8000
# Docs:     http://localhost:8000/docs
# ReDoc:    http://localhost:8000/redoc

# Trigger ETL to load data (first run):
curl -X POST http://localhost:8000/api/v1/etl/run
```

The dataset is pre-generated at build time (`etl/generate.py` runs during `docker build`). The named volume `scada_data` persists the SQLite database across container restarts.

### Option B: Local Development

```bash
# 1. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy environment file
cp .env.example .env

# 4. Generate dataset
python etl/generate.py data/raw_alarms.csv

# 5. Start API
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 6. Trigger ETL (separate terminal or Postman)
curl -X POST http://localhost:8000/api/v1/etl/run
```

API docs available at `http://localhost:8000/docs` (Swagger UI).

---

## 9. Testing

Tests use `pytest` with an in-memory SQLite database (isolated from production data). Each test function gets a fresh session that rolls back after completion.

```bash
# Run all tests
pytest tests/ -v

# Run only ETL unit tests
pytest tests/test_etl.py -v

# Run only API integration tests
pytest tests/test_api.py -v
```

### Coverage

| File | Tests | Scope |
|---|---|---|
| `test_etl.py` | 22 | ETL unit tests: clean + normalize stages |
| `test_api.py` | 20 | API integration tests: all endpoints + error cases |

**Key test cases:**

ETL:
- Exact duplicate removal
- Near-duplicate resolution (keeps non-null source)
- Criticality normalization from all known variants (lowercase, abbreviations, typos)
- Status boolean mapping (true/1/yes → ACTIVE, false/0/no → CLEARED)
- Timestamp parsing from all 4 formats + Unix epoch
- Future timestamp clamping
- Garbage timestamp → NaT → row rejected

API:
- Paginated response structure (data + meta)
- Criticality filter (single + multiple values)
- Tag partial match filter
- Pagination bounds (page=0 → 422, size=9999 → 422)
- Invalid criticality enum → 422
- Invalid date string → 400
- `start_date > end_date` → 4xx
- Single alarm by ID (existing → 200, missing → 404)
- Metrics structure, percentage sum, timeline intervals

---

## 10. Future Improvements

### SQL Server Migration

1. Change `DATABASE_URL` to `mssql+pyodbc://...`
2. Replace `func.strftime()` in `app/services/metrics_service.py` with `DATEADD/DATEDIFF` (documented in-file)
3. Add `QueuePool` configuration (replace `StaticPool`)

No schema changes required.

### Streaming Ingestion

For continuous SCADA data instead of batch exports:
- Replace CSV polling with OPC-UA subscription or MQTT consumer
- Use Apache Kafka or Azure Event Hub as message broker
- ETL clean/normalize steps become stream processors
- DB write pattern: micro-batches of 100–500 rows every few seconds

### Authentication & Authorization

- JWT-based auth (`python-jose`)
- Role-based access: operators see active alarms, engineers see full history
- API key for system-to-system integration (OPC middleware, historian)

### Observability

- Structured logging with `structlog`
- Prometheus metrics endpoint for ETL duration, row counts, error rates
- OpenTelemetry tracing for end-to-end request visibility
