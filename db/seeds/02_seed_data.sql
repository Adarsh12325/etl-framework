-- ============================================================
-- 02_seed_data.sql  –  Seed source tables and etl_control
-- Uses ON CONFLICT DO NOTHING so this file is safe to re-run.
-- ============================================================

-- ── Seed source_products ───────────────────────────────────
INSERT INTO source_products (product_id, product_name, category, price, last_modified) VALUES
    (1,  'Laptop Pro 15',    'Electronics', 1299.99, '2024-01-10 08:00:00+00'),
    (2,  'Wireless Mouse',   'Electronics',   29.99, '2024-01-11 09:00:00+00'),
    (3,  'Standing Desk',    'Furniture',    499.00, '2024-01-12 10:00:00+00'),
    (4,  'Office Chair',     'Furniture',    349.00, '2024-01-13 11:00:00+00'),
    (5,  'USB-C Hub',        'Electronics',   49.99, '2024-01-14 12:00:00+00'),
    (6,  'Notebook A5',      'Stationery',     5.99, '2024-01-15 13:00:00+00'),
    (7,  'Mechanical Kbd',   'Electronics',  129.00, '2024-01-16 14:00:00+00'),
    (8,  'Monitor 27"',      'Electronics',  399.00, '2024-01-17 15:00:00+00'),
    (9,  'Desk Lamp',        'Furniture',     39.99, '2024-01-18 16:00:00+00'),
    (10, 'Cable Organiser',  'Accessories',   12.99, '2024-01-19 17:00:00+00')
ON CONFLICT (product_id) DO NOTHING;

-- ── pipeline-A: CSV full load (no dependencies) ────────────
INSERT INTO etl_control
    (pipeline_name, source_type, source_options, destination_table,
     load_type, incremental_key, dependencies, is_active)
VALUES (
    'pipeline-A', 'csv',
    '{"path": "/data/source_data.csv"}',
    'dest_csv_data', 'full', NULL, '{}', TRUE
) ON CONFLICT (pipeline_name) DO NOTHING;

-- ── pipeline-B: DB full load (depends on pipeline-A) ───────
INSERT INTO etl_control
    (pipeline_name, source_type, source_options, destination_table,
     load_type, incremental_key, dependencies, is_active)
VALUES (
    'pipeline-B', 'db',
    '{"table": "source_products"}',
    'dest_products', 'full', NULL, ARRAY['pipeline-A'], TRUE
) ON CONFLICT (pipeline_name) DO NOTHING;

-- ── api-full-load: API full load ───────────────────────────
INSERT INTO etl_control
    (pipeline_name, source_type, source_options, destination_table,
     load_type, incremental_key, dependencies, is_active)
VALUES (
    'api-full-load', 'api',
    '{"url": "http://mock-api:8080/data"}',
    'dest_api_data', 'full', NULL, '{}', TRUE
) ON CONFLICT (pipeline_name) DO NOTHING;

-- ── api-incremental: API incremental load ──────────────────
INSERT INTO etl_control
    (pipeline_name, source_type, source_options, destination_table,
     load_type, incremental_key, dependencies, is_active)
VALUES (
    'api-incremental', 'api',
    '{"url": "http://mock-api:8080/data", "since_param": "since"}',
    'dest_incremental_api', 'incremental', 'last_modified', '{}', TRUE
) ON CONFLICT (pipeline_name) DO NOTHING;

-- ── bad-pipeline: intentional failure (missing file) ───────
INSERT INTO etl_control
    (pipeline_name, source_type, source_options, destination_table,
     load_type, incremental_key, dependencies, is_active)
VALUES (
    'bad-pipeline', 'csv',
    '{"path": "/data/nonexistent_file.csv"}',
    'dest_bad_pipeline', 'full', NULL, '{}', TRUE
) ON CONFLICT (pipeline_name) DO NOTHING;

-- ── cycle-A and cycle-B: circular dependency test ──────────
INSERT INTO etl_control
    (pipeline_name, source_type, source_options, destination_table,
     load_type, incremental_key, dependencies, is_active)
VALUES (
    'cycle-A', 'csv',
    '{"path": "/data/source_data.csv"}',
    'dest_csv_data', 'full', NULL, ARRAY['cycle-B'], TRUE
) ON CONFLICT (pipeline_name) DO NOTHING;

INSERT INTO etl_control
    (pipeline_name, source_type, source_options, destination_table,
     load_type, incremental_key, dependencies, is_active)
VALUES (
    'cycle-B', 'csv',
    '{"path": "/data/source_data.csv"}',
    'dest_csv_data', 'full', NULL, ARRAY['cycle-A'], TRUE
) ON CONFLICT (pipeline_name) DO NOTHING;