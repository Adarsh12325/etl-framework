"""
transform.py – Simple, metadata-driven transformation engine.

Reads transformation rules from the pipeline definition and applies them
to the extracted records before loading.

Supported rules (passed as a JSON object in etl_control):
    rename_columns  – { "old_name": "new_name", ... }
    drop_columns    – ["col1", "col2", ...]

Example etl_control.transformation_rules value:
    {
        "rename_columns": {"emp_id": "id", "emp_name": "name"},
        "drop_columns":   ["internal_code"]
    }
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def apply(records: list[dict], rules: dict | None) -> list[dict]:
    """
    Apply *rules* to every record in *records*.
    Returns the transformed list (new list, does not mutate input).
    """
    if not rules or not records:
        return records

    result = []
    for row in records:
        new_row = dict(row)

        # 1. Rename columns
        for old_name, new_name in rules.get("rename_columns", {}).items():
            if old_name in new_row:
                new_row[new_name] = new_row.pop(old_name)

        # 2. Drop columns
        for col in rules.get("drop_columns", []):
            new_row.pop(col, None)

        result.append(new_row)

    if rules:
        logger.info(f"[TRANSFORM] Applied rules {list(rules.keys())} to {len(result)} records.")

    return result