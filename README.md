# Metadata-Driven ETL Orchestration Framework

A production-inspired ETL orchestration framework built with **Python, PostgreSQL, Flask, and Docker**. Modelled after Azure Data Factory patterns — adding a new pipeline requires only inserting a new row into a database table. No code changes needed.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [How It Works](#how-it-works)
6. [Pipeline Definitions](#pipeline-definitions)
7. [Key Features Demonstrated](#key-features-demonstrated)
8. [Demo Scenarios](#demo-scenarios)
9. [Verifying Results](#verifying-results)
10. [Environment Variables](#environment-variables)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    docker-compose network                   │
│                                                             │
│  ┌──────────────┐   reads metadata    ┌──────────────────┐  │
│  │              │ ──────────────────▶ │                  │  │
│  │ Orchestrator │                     │  PostgreSQL (db) │  │
│  │  (Python)    │ ◀────────────────── │                  │  │
│  │              │  writes audit/wmark └──────────────────┘  │
│  └──────┬───────┘                                           │
│         │ fetches data                                      │
│         ▼                                                   │
│  ┌──────────────┐                                           │
│  │  Mock API    │  GET /data?since=<timestamp>              │
│  │  (Flask)     │                                           │
│  └──────────────┘                                           │
│                                                             │
│  /data/source_data.csv  (baked into orchestrator image)     │
└─────────────────────────────────────────────────────────────┘
```

### Services

| Service | Technology | Role |
|---|---|---|
| `db` | PostgreSQL 15 | Control tables, audit log, watermarks, source & destination tables |
| `mock-api` | Flask + Gunicorn | Simulates a REST API data source with incremental support |
| `orchestrator` | Python 3.11 | Reads metadata, builds DAG, detects cycles, executes pipelines |

---

## Project Structure

```
etl-framework/
├── docker-compose.yml
├── .env                          ← copy from .env.example
├── .env.example
├── .gitignore
├── README.md
│
├── data/
│   └── source_data.csv           ← sample CSV (10 employee rows)
│
├── db/
│   ├── Dockerfile                ← custom DB image with seeds baked in
│   ├── init-db.sh                ← runs schema + seed SQL on startup
│   └── seeds/
│       ├── 01_schema.sql         ← CREATE TABLE statements
│       └── 02_seed_data.sql      ← pipeline definitions + source data
│
├── mock-api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                    ← Flask API: /data /health /add_records /reset
│
└── orchestrator/
    ├── Dockerfile                ← CSV baked into image at /data/
    ├── requirements.txt
    ├── main.py                   ← entry point + main orchestration loop
    ├── db.py                     ← SQLAlchemy engine + query helpers
    ├── connectors.py             ← CSV / API / DB source connectors
    ├── loaders.py                ← full & incremental destination loaders
    ├── transform.py              ← metadata-driven column transformations
    ├── audit.py                  ← audit log + watermark management
    └── graph.py                  ← DAG build, cycle detection, topo-sort
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- Git

No Python or PostgreSQL installation needed on your machine — everything runs inside Docker.

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/Adarsh12325/etl-framework.git
cd etl-framework

# 2. Create your .env file (default values work out of the box)
cp .env.example .env

# 3. Build and start all services
docker-compose up --build

# 4. In a second terminal, run the orchestrator
docker-compose run --rm orchestrator
```

To run the orchestrator again at any time:

```bash
docker-compose run --rm orchestrator
```

To start completely fresh (wipe all data):

```bash
docker-compose down -v
docker-compose up --build
```

---

## How It Works

### Orchestration Flow

1. **Fetch** — reads all active pipelines from `etl_control` table
2. **Build DAG** — constructs a directed acyclic graph of pipeline dependencies using `networkx`
3. **Cycle Detection** — if any circular dependency exists, logs a clear error and exits without running anything
4. **Topological Sort** — determines a safe execution order so dependencies always run first
5. **Execute** — for each pipeline in order:
   - **Extract** — pulls data from CSV, REST API, or database table
   - **Transform** — applies any metadata-defined column rules
   - **Load** — inserts data using full (TRUNCATE + INSERT) or incremental (append) strategy
   - **Watermark** — saves the latest timestamp for incremental pipelines
   - **Audit** — records start time, end time, rows read/written, and status

### Control Table Schema

The `etl_control` table is the heart of the framework. Each row is one pipeline:

| Column | Type | Description |
|---|---|---|
| `pipeline_id` | SERIAL | Primary key |
| `pipeline_name` | VARCHAR | Unique human-readable name |
| `source_type` | VARCHAR | `csv` / `api` / `db` |
| `source_options` | JSONB | Connection details (path, URL, table name) |
| `destination_table` | VARCHAR | Target PostgreSQL table |
| `load_type` | VARCHAR | `full` / `incremental` |
| `incremental_key` | VARCHAR | Column used for watermarking (NULL for full loads) |
| `dependencies` | TEXT[] | Pipeline names that must succeed first |
| `is_active` | BOOLEAN | Enable/disable without deleting |

### Audit Log Schema

Every execution is recorded in `etl_audit_log`:

| Column | Type | Description |
|---|---|---|
| `run_id` | SERIAL | Primary key |
| `pipeline_name` | VARCHAR | Which pipeline ran |
| `start_time` | TIMESTAMPTZ | When it started |
| `end_time` | TIMESTAMPTZ | When it finished |
| `duration_ms` | INTEGER | Wall-clock time in milliseconds |
| `status` | VARCHAR | `SUCCESS` / `FAILED` / `RUNNING` |
| `rows_read` | INTEGER | Records extracted from source |
| `rows_written` | INTEGER | Records loaded into destination |
| `error_message` | TEXT | Populated on failure |

### Watermark Table

Incremental pipeline state is stored in `etl_watermarks`:

| Column | Type | Description |
|---|---|---|
| `pipeline_name` | VARCHAR PK | Which pipeline |
| `watermark_value` | VARCHAR | Last processed timestamp or ID |

---

## Pipeline Definitions

The seed script creates these pipelines automatically on startup:

| Pipeline | Source | Destination | Load Type | Dependencies | Purpose |
|---|---|---|---|---|---|
| `pipeline-A` | CSV `/data/source_data.csv` | `dest_csv_data` | full | — | CSV connector + dependency chain start |
| `pipeline-B` | DB `source_products` | `dest_products` | full | `pipeline-A` | DB connector + runs after pipeline-A |
| `api-full-load` | Mock API `/data` | `dest_api_data` | full | — | API full load |
| `api-incremental` | Mock API `/data?since=…` | `dest_incremental_api` | incremental | — | Watermark-based incremental load |
| `bad-pipeline` | CSV `/data/nonexistent.csv` | `dest_bad_pipeline` | full | — | **Intentional failure** for audit testing |
| `cycle-A` | CSV | `dest_csv_data` | full | `cycle-B` | Circular dependency test (disabled by default) |
| `cycle-B` | CSV | `dest_csv_data` | full | `cycle-A` | Circular dependency test (disabled by default) |

> `cycle-A` and `cycle-B` are seeded with `is_active = TRUE` but the orchestrator detects the cycle and exits before running any pipeline. Disable them to run the normal pipelines (see Demo Scenarios below).

---

## Key Features Demonstrated

###  Dependency Ordering
`pipeline-B` depends on `pipeline-A`. The topological sort guarantees `pipeline-A` always completes successfully before `pipeline-B` starts.

###  Cycle Detection
`cycle-A` and `cycle-B` depend on each other. When active, the orchestrator prints:
```
Error: Cycle detected in dependency graph: cycle-A → cycle-B → cycle-A
[ORCHESTRATOR] Aborting run due to circular dependency.
```
No audit log entries are created for them.

###  Full Load
Destination table is truncated before each insert. Running the orchestrator twice always produces the same row count — not double.

###  Incremental Load with Watermark
After the first run, the max `last_modified` value is saved to `etl_watermarks`. The next run only fetches records newer than that timestamp.

###  Audit Logging
Every run writes a row to `etl_audit_log`. Successful runs have `status = 'SUCCESS'` with positive counts. The `bad-pipeline` always produces `status = 'FAILED'` with a descriptive error message.

###  Three Source Connectors
- **CSV** — reads from a flat file
- **API** — fetches JSON from a REST endpoint with optional `?since=` filtering
- **DB** — queries another table in the same PostgreSQL database

---

## Demo Scenarios

### Scenario 1 — Normal Pipeline Run

```bash
# Disable the cycle pipelines first
docker exec -it etl_db psql -U etl_user -d etl_db -c \
  "UPDATE etl_control SET is_active = FALSE WHERE pipeline_name IN ('cycle-A', 'cycle-B');"

# Run the orchestrator
docker-compose run --rm orchestrator
```

Expected result: 4 SUCCESS, 1 FAILED (bad-pipeline)

---

### Scenario 2 — Test Cycle Detection

```bash
# Re-enable cycle pipelines
docker exec -it etl_db psql -U etl_user -d etl_db -c \
  "UPDATE etl_control SET is_active = TRUE WHERE pipeline_name IN ('cycle-A', 'cycle-B');"

# Run — orchestrator will detect cycle and abort
docker-compose run --rm orchestrator

# Disable again for normal runs
docker exec -it etl_db psql -U etl_user -d etl_db -c \
  "UPDATE etl_control SET is_active = FALSE WHERE pipeline_name IN ('cycle-A', 'cycle-B');"
```

---

### Scenario 3 — Test Incremental Load (add new records)

```bash
# Step 1: run once to load initial 5 records and save watermark
docker-compose run --rm orchestrator

# Step 2: add 3 new records to the mock API source
curl http://localhost:8080/add_records
# Returns: {"message": "Added 3 new record(s).", "total": 8}

# Step 3: run again — only the 3 new records are fetched
docker-compose run --rm orchestrator

# Verify: dest_incremental_api now has 8 rows, watermark updated
docker exec -it etl_db psql -U etl_user -d etl_db -c \
  "SELECT COUNT(*) FROM dest_incremental_api;"
docker exec -it etl_db psql -U etl_user -d etl_db -c \
  "SELECT * FROM etl_watermarks;"
```

---

### Scenario 4 — Switch a Pipeline from Full to Incremental Load

This demonstrates the power of metadata-driven design — **zero code changes needed**.

```bash
# Connect to the database
docker exec -it etl_db psql -U etl_user -d etl_db

-- Switch api-full-load to incremental
UPDATE etl_control
SET
    load_type       = 'incremental',
    incremental_key = 'last_modified',
    source_options  = source_options || '{"since_param": "since"}'::jsonb
WHERE pipeline_name = 'api-full-load';

-- Switch back to full load
UPDATE etl_control
SET
    load_type       = 'full',
    incremental_key = NULL
WHERE pipeline_name = 'api-full-load';
```

---

### Scenario 5 — Verify Full Load Idempotency

```bash
# Run twice and confirm row count stays the same (not doubling)
docker-compose run --rm orchestrator
docker exec -it etl_db psql -U etl_user -d etl_db -c "SELECT COUNT(*) FROM dest_csv_data;"
# → 10

docker-compose run --rm orchestrator
docker exec -it etl_db psql -U etl_user -d etl_db -c "SELECT COUNT(*) FROM dest_csv_data;"
# → still 10 (table was truncated before reload)
```

---

## Verifying Results

```bash
# Open a psql session
docker exec -it etl_db psql -U etl_user -d etl_db
```

```sql
-- All pipeline definitions
SELECT pipeline_name, source_type, load_type, is_active, dependencies
FROM etl_control ORDER BY pipeline_id;

-- Full execution history
SELECT run_id, pipeline_name, status, rows_read, rows_written,
       duration_ms, error_message
FROM etl_audit_log ORDER BY run_id;

-- Current watermarks
SELECT * FROM etl_watermarks;

-- Destination table row counts
SELECT 'dest_csv_data'        AS table_name, COUNT(*) AS rows FROM dest_csv_data
UNION ALL
SELECT 'dest_api_data',                      COUNT(*) FROM dest_api_data
UNION ALL
SELECT 'dest_products',                      COUNT(*) FROM dest_products
UNION ALL
SELECT 'dest_incremental_api',               COUNT(*) FROM dest_incremental_api;
```

---

## Environment Variables

Copy `.env.example` to `.env` before running. The default values work without any changes.

| Variable | Description | Default |
|---|---|---|
| `POSTGRES_USER` | PostgreSQL username | `etl_user` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `etl_password` |
| `POSTGRES_DB` | Database name | `etl_db` |
| `DATABASE_URL` | SQLAlchemy connection string | `postgresql://etl_user:etl_password@db:5432/etl_db` |
| `MOCK_API_URL` | Base URL of the mock API service | `http://mock-api:8080` |

---

## Mock API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Health check — returns `{"status": "ok"}` |
| `/data` | GET | Returns all records |
| `/data?since=<timestamp>` | GET | Returns only records newer than timestamp |
| `/add_records` | POST | Adds 3 extra records (for incremental testing) |
| `/reset` | GET | Resets dataset back to original 5 records |

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `etl_control does not exist` | Run `docker-compose down -v` then `docker-compose up --build` to wipe and re-seed the DB |
| `mock-api is unhealthy` | Make sure `curl` is installed in the mock-api image (it is by default in this project) |
| CSV file not found | The CSV is baked into the orchestrator image — rebuild with `docker-compose build --no-cache orchestrator` |
| Old data showing up | Run `docker-compose down -v` to wipe volumes and start fresh |
