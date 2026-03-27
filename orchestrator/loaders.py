"""
loaders.py – Destination loaders for full and incremental strategies.

Exports a single public function:
    load(records, destination_table, load_type) -> int
        Returns the number of rows written.
"""

import logging
from typing import Any

from sqlalchemy import text

import db as database

logger = logging.getLogger(__name__)


def _build_insert(table: str, columns: list[str]) -> str:
    """Build a parameterised INSERT statement for *table*."""
    col_list = ", ".join(columns)
    val_list = ", ".join(f":{c}" for c in columns)
    return f"INSERT INTO {table} ({col_list}) VALUES ({val_list})"


def load(
    records: list[dict],
    destination_table: str,
    load_type: str,
) -> int:
    """
    Load *records* into *destination_table* using the given *load_type*.

    'full'        → TRUNCATE then INSERT (full replacement)
    'incremental' → INSERT only (append new records)

    Returns the number of rows written.
    """
    if not records:
        logger.info(f"[LOAD] No records to load into {destination_table}.")
        return 0

    columns = list(records[0].keys())
    insert_sql = _build_insert(destination_table, columns)

    engine = database.get_engine()
    with engine.begin() as conn:
        if load_type == "full":
            logger.info(f"[LOAD] Truncating {destination_table} (full load).")
            conn.execute(text(f"TRUNCATE TABLE {destination_table}"))

        conn.execute(text(insert_sql), records)

    rows_written = len(records)
    logger.info(f"[LOAD] Wrote {rows_written} rows into {destination_table}.")
    return rows_written