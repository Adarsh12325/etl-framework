"""
connectors.py – Source connectors for CSV, REST API, and database tables.

Each connector exposes a single function:
    extract(source_options, watermark=None) -> list[dict]

The `watermark` argument is only used by connectors that support
incremental extraction (currently: api, db).
"""

import csv
import logging
from typing import Any

import requests

import db as database

logger = logging.getLogger(__name__)


# ── CSV connector ─────────────────────────────────────────────────────────

def extract_csv(source_options: dict, watermark: str | None = None) -> list[dict]:
    """
    Read all rows from a CSV file and return them as a list of dicts.
    CSV sources do not support watermark filtering at extraction time;
    the orchestrator will handle full-vs-incremental at load time.
    """
    path: str = source_options["path"]
    logger.info(f"[CSV] Reading file: {path}")

    rows: list[dict] = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(dict(row))

    logger.info(f"[CSV] Read {len(rows)} rows from {path}")
    return rows


# ── API connector ─────────────────────────────────────────────────────────

def extract_api(source_options: dict, watermark: str | None = None) -> list[dict]:
    """
    Fetch records from a REST API endpoint.

    source_options keys:
        url          (required) – full URL of the endpoint
        since_param  (optional) – name of the query parameter used for
                                  incremental filtering (default: 'since')
    """
    url: str = source_options["url"]
    since_param: str = source_options.get("since_param", "since")

    params: dict[str, str] = {}
    if watermark:
        params[since_param] = watermark
        logger.info(f"[API] Fetching incrementally from {url} with {since_param}={watermark}")
    else:
        logger.info(f"[API] Fetching all records from {url}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    records: list[dict] = response.json()
    logger.info(f"[API] Received {len(records)} records")
    return records


# ── DB connector ──────────────────────────────────────────────────────────

def extract_db(source_options: dict, watermark: str | None = None) -> list[dict]:
    """
    Select rows from another table in the same PostgreSQL database.

    source_options keys:
        table            (required) – source table name
        incremental_key  (optional) – column used for watermark filtering;
                                      falls back to source_options key if
                                      provided by orchestrator
    """
    table: str = source_options["table"]
    incremental_key: str | None = source_options.get("incremental_key")

    if watermark and incremental_key:
        query = f"SELECT * FROM {table} WHERE {incremental_key} > :wm"  # noqa: S608
        logger.info(f"[DB] Querying {table} with {incremental_key} > {watermark}")
        rows = database.fetch_all(query, {"wm": watermark})
    else:
        query = f"SELECT * FROM {table}"  # noqa: S608
        logger.info(f"[DB] Querying all rows from {table}")
        rows = database.fetch_all(query)

    logger.info(f"[DB] Fetched {len(rows)} rows from {table}")
    return rows


# ── Dispatcher ────────────────────────────────────────────────────────────

def extract(source_type: str, source_options: dict, watermark: str | None = None) -> list[dict]:
    """Route extraction to the correct connector based on *source_type*."""
    if source_type == "csv":
        return extract_csv(source_options, watermark)
    elif source_type == "api":
        return extract_api(source_options, watermark)
    elif source_type == "db":
        return extract_db(source_options, watermark)
    else:
        raise ValueError(f"Unsupported source_type: '{source_type}'")