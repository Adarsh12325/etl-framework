-- ============================================================
-- 01_schema.sql  –  Create all control and destination tables
-- ============================================================

-- ── Control table ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_control (
    pipeline_id      SERIAL PRIMARY KEY,
    pipeline_name    VARCHAR(255) UNIQUE NOT NULL,
    source_type      VARCHAR(50)  NOT NULL,          -- 'csv' | 'api' | 'db'
    source_options   JSONB        NOT NULL,
    destination_table VARCHAR(255) NOT NULL,
    load_type        VARCHAR(50)  NOT NULL,           -- 'full' | 'incremental'
    incremental_key  VARCHAR(255),                    -- NULL for full loads
    dependencies     TEXT[]       DEFAULT '{}',
    is_active        BOOLEAN      DEFAULT TRUE
);

-- ── Audit log ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_audit_log (
    run_id         SERIAL PRIMARY KEY,
    pipeline_name  VARCHAR(255) NOT NULL,
    start_time     TIMESTAMPTZ,
    end_time       TIMESTAMPTZ,
    duration_ms    INTEGER,
    status         VARCHAR(50),
    rows_read      INTEGER,
    rows_written   INTEGER,
    error_message  TEXT
);

-- ── Watermarks ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS etl_watermarks (
    pipeline_name   VARCHAR(255) PRIMARY KEY,
    watermark_value VARCHAR(255)
);

-- ── Source tables (used by 'db' connector) ─────────────────
CREATE TABLE IF NOT EXISTS source_products (
    product_id    SERIAL PRIMARY KEY,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         NUMERIC(10,2),
    last_modified TIMESTAMPTZ DEFAULT NOW()
);

-- ── Destination tables ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS dest_csv_data (
    id            INTEGER,
    name          VARCHAR(255),
    department    VARCHAR(100),
    salary        NUMERIC(10,2),
    hire_date     VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dest_api_data (
    id            INTEGER,
    name          VARCHAR(255),
    email         VARCHAR(255),
    last_modified TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dest_products (
    product_id    INTEGER,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         NUMERIC(10,2),
    last_modified TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dest_incremental_api (
    id            INTEGER,
    name          VARCHAR(255),
    email         VARCHAR(255),
    last_modified TIMESTAMPTZ
);

-- ── Tables for the failing pipeline test ───────────────────
-- (dest table exists, but source path won't exist → triggers FAILED status)
CREATE TABLE IF NOT EXISTS dest_bad_pipeline (
    id   INTEGER,
    data TEXT
);