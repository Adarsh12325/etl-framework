#!/bin/bash
# init-db.sh
# Runs AFTER PostgreSQL has started inside the container.
# Always applies the schema and seed data (uses IF NOT EXISTS / ON CONFLICT
# so it is safe to run multiple times).

set -e

echo "[INIT] Applying schema..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /seeds/01_schema.sql

echo "[INIT] Applying seed data..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /seeds/02_seed_data.sql

echo "[INIT] Database initialisation complete."