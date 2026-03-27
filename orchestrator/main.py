"""
main.py – ETL Orchestrator entry point.

Execution flow:
  1. Fetch active pipelines from etl_control.
  2. Build a dependency DAG with networkx.
  3. Detect cycles → abort if found.
  4. Topological-sort to get safe execution order.
  5. Execute each pipeline: Extract → Transform → Load → Audit.
  6. Update watermarks for incremental pipelines.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

import db as database
import audit
import connectors
import loaders
import transform as transformer
import graph as dag_builder

# ── Logging setup ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ── Fetch active pipeline definitions ────────────────────────────────────

def fetch_active_pipelines() -> list[dict]:
    rows = database.fetch_all(
        "SELECT * FROM etl_control WHERE is_active = TRUE"
    )
    logger.info(f"[ORCHESTRATOR] Found {len(rows)} active pipeline(s).")
    return rows


# ── Compute new watermark value from extracted records ───────────────────

def _compute_new_watermark(records: list[dict], incremental_key: str) -> str | None:
    """Return the maximum value of *incremental_key* across all *records*."""
    values = []
    for r in records:
        v = r.get(incremental_key)
        if v is not None:
            values.append(str(v))
    if not values:
        return None
    return max(values)


# ── Execute a single pipeline ─────────────────────────────────────────────

def run_pipeline(pipeline: dict) -> bool:
    """
    Execute one pipeline end-to-end.
    Returns True on success, False on failure.
    """
    name      = pipeline["pipeline_name"]
    src_type  = pipeline["source_type"]
    src_opts  = pipeline["source_options"]          # already a dict (JSONB)
    dest      = pipeline["destination_table"]
    load_type = pipeline["load_type"]
    inc_key   = pipeline.get("incremental_key")
    tx_rules  = pipeline.get("transformation_rules") # may be None

    logger.info(f"\n{'='*60}")
    logger.info(f"[PIPELINE] Starting: '{name}'")
    logger.info(f"  source_type : {src_type}")
    logger.info(f"  destination : {dest}")
    logger.info(f"  load_type   : {load_type}")

    start_time = datetime.now(timezone.utc)
    run_id = audit.start_run(name)

    try:
        # ── EXTRACT ───────────────────────────────────────────────────────
        watermark = None
        if load_type == "incremental" and inc_key:
            watermark = audit.get_watermark(name)
            logger.info(f"[PIPELINE] Watermark for '{name}': {watermark}")

        records = connectors.extract(src_type, src_opts, watermark=watermark)
        rows_read = len(records)
        logger.info(f"[PIPELINE] Extracted {rows_read} record(s).")

        # ── TRANSFORM (optional) ──────────────────────────────────────────
        if tx_rules:
            records = transformer.apply(records, tx_rules)

        # ── LOAD ──────────────────────────────────────────────────────────
        rows_written = loaders.load(records, dest, load_type)

        # ── UPDATE WATERMARK ──────────────────────────────────────────────
        if load_type == "incremental" and inc_key and records:
            new_wm = _compute_new_watermark(records, inc_key)
            if new_wm:
                audit.set_watermark(name, new_wm)

        # ── LOG SUCCESS ───────────────────────────────────────────────────
        audit.complete_run(run_id, start_time, rows_read, rows_written)
        logger.info(f"[PIPELINE] '{name}' completed successfully.")
        return True

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.error(f"[PIPELINE] '{name}' FAILED — {error_msg}")
        audit.fail_run(run_id, start_time, error_msg)
        return False


# ── Main orchestration loop ───────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("  ETL Orchestrator starting up")
    logger.info("=" * 60)

    # 1. Fetch active pipelines
    pipelines = fetch_active_pipelines()
    if not pipelines:
        logger.info("[ORCHESTRATOR] No active pipelines found. Exiting.")
        return

    pipeline_map = {p["pipeline_name"]: p for p in pipelines}

    # 2. Build dependency graph
    G = dag_builder.build_graph(pipelines)

    # 3. Validate (cycle detection) + topological sort
    try:
        execution_order = dag_builder.validate_and_sort(G)
    except ValueError as exc:
        # Cycle detected – log and exit WITHOUT running any pipeline
        logger.error(str(exc))
        logger.error("[ORCHESTRATOR] Aborting run due to circular dependency.")
        sys.exit(1)

    # Filter execution order to only active pipelines we know about
    execution_order = [n for n in execution_order if n in pipeline_map]
    logger.info(f"[ORCHESTRATOR] Execution plan: {execution_order}")

    # 4. Execute pipelines in dependency order
    failed_pipelines: set[str] = set()

    for name in execution_order:
        pipeline = pipeline_map[name]
        deps = pipeline.get("dependencies") or []

        # Skip if any dependency failed
        blocked_by = [d for d in deps if d in failed_pipelines]
        if blocked_by:
            logger.warning(
                f"[ORCHESTRATOR] Skipping '{name}' — "
                f"blocked by failed dependency: {blocked_by}"
            )
            failed_pipelines.add(name)
            continue

        success = run_pipeline(pipeline)
        if not success:
            failed_pipelines.add(name)

    # 5. Summary
    total     = len(execution_order)
    succeeded = total - len(failed_pipelines)
    logger.info("\n" + "=" * 60)
    logger.info(f"[ORCHESTRATOR] Run complete: {succeeded}/{total} pipeline(s) succeeded.")
    if failed_pipelines:
        logger.warning(f"[ORCHESTRATOR] Failed/skipped: {failed_pipelines}")
    logger.info("=" * 60)


def wait_for_db(max_attempts: int = 20, delay: int = 5) -> None:
    """
    Poll until etl_control exists and is queryable.

    The PostgreSQL healthcheck only confirms the server accepts connections.
    The docker-entrypoint-initdb.d seed scripts run AFTER that — so we must
    wait until the schema is actually in place before proceeding.
    """
    logger.info("[STARTUP] Waiting for database schema to be ready...")
    for attempt in range(1, max_attempts + 1):
        try:
            rows = database.fetch_all(
                "SELECT COUNT(*) AS n FROM information_schema.tables "
                "WHERE table_name = 'etl_control'"
            )
            if rows and rows[0]["n"] > 0:
                logger.info("[STARTUP] Database schema is ready.")
                return
            logger.info(
                f"[STARTUP] Schema not ready yet (attempt {attempt}/{max_attempts}). "
                f"Retrying in {delay}s..."
            )
        except Exception as exc:
            logger.info(
                f"[STARTUP] DB not reachable yet (attempt {attempt}/{max_attempts}): "
                f"{exc}. Retrying in {delay}s..."
            )
        time.sleep(delay)

    logger.error("[STARTUP] Database schema never became ready. Exiting.")
    sys.exit(1)


if __name__ == "__main__":
    wait_for_db()
    main()