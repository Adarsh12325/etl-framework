"""
audit.py – Audit log and watermark management.

Functions:
    start_run(pipeline_name)           -> run_id
    complete_run(run_id, ...)
    fail_run(run_id, error_message)
    get_watermark(pipeline_name)       -> str | None
    set_watermark(pipeline_name, value)
"""

import logging
from datetime import datetime, timezone

import db as database

logger = logging.getLogger(__name__)


# ── Audit log ─────────────────────────────────────────────────────────────

def start_run(pipeline_name: str) -> int:
    """
    Insert a new RUNNING entry into etl_audit_log.
    Returns the generated run_id.
    """
    now = datetime.now(timezone.utc)
    database.execute(
        """
        INSERT INTO etl_audit_log
            (pipeline_name, start_time, status)
        VALUES
            (:name, :start, 'RUNNING')
        """,
        {"name": pipeline_name, "start": now},
    )
    rows = database.fetch_all(
        "SELECT run_id FROM etl_audit_log WHERE pipeline_name = :name ORDER BY run_id DESC LIMIT 1",
        {"name": pipeline_name},
    )
    run_id: int = rows[0]["run_id"]
    logger.info(f"[AUDIT] Started run #{run_id} for '{pipeline_name}'.")
    return run_id


def complete_run(
    run_id: int,
    start_time: datetime,
    rows_read: int,
    rows_written: int,
) -> None:
    """Mark a run as SUCCESS and record metrics."""
    end_time = datetime.now(timezone.utc)
    duration_ms = int((end_time - start_time).total_seconds() * 1000)

    database.execute(
        """
        UPDATE etl_audit_log
        SET
            end_time     = :end,
            duration_ms  = :dur,
            status       = 'SUCCESS',
            rows_read    = :read,
            rows_written = :written
        WHERE run_id = :id
        """,
        {
            "end": end_time,
            "dur": duration_ms,
            "read": rows_read,
            "written": rows_written,
            "id": run_id,
        },
    )
    logger.info(
        f"[AUDIT] Run #{run_id} SUCCESS — "
        f"{rows_read} read, {rows_written} written, {duration_ms} ms."
    )


def fail_run(run_id: int, start_time: datetime, error_message: str) -> None:
    """Mark a run as FAILED and store the error message."""
    end_time = datetime.now(timezone.utc)
    duration_ms = int((end_time - start_time).total_seconds() * 1000)

    database.execute(
        """
        UPDATE etl_audit_log
        SET
            end_time      = :end,
            duration_ms   = :dur,
            status        = 'FAILED',
            rows_read     = 0,
            rows_written  = 0,
            error_message = :err
        WHERE run_id = :id
        """,
        {
            "end": end_time,
            "dur": duration_ms,
            "err": error_message,
            "id": run_id,
        },
    )
    logger.warning(f"[AUDIT] Run #{run_id} FAILED: {error_message}")


# ── Watermarks ────────────────────────────────────────────────────────────

def get_watermark(pipeline_name: str) -> str | None:
    """Return the stored watermark value for *pipeline_name*, or None."""
    rows = database.fetch_all(
        "SELECT watermark_value FROM etl_watermarks WHERE pipeline_name = :name",
        {"name": pipeline_name},
    )
    value = rows[0]["watermark_value"] if rows else None
    logger.debug(f"[WATERMARK] Get '{pipeline_name}' → {value}")
    return value


def set_watermark(pipeline_name: str, value: str) -> None:
    """Upsert the watermark for *pipeline_name*."""
    database.execute(
        """
        INSERT INTO etl_watermarks (pipeline_name, watermark_value)
        VALUES (:name, :val)
        ON CONFLICT (pipeline_name) DO UPDATE
            SET watermark_value = EXCLUDED.watermark_value
        """,
        {"name": pipeline_name, "val": value},
    )
    logger.info(f"[WATERMARK] Set '{pipeline_name}' → {value}")