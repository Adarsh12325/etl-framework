"""
db.py – Database connection and helper utilities.

All other modules import `get_engine()` from here to obtain a shared
SQLAlchemy engine.  Connection pooling is handled automatically.
"""

import os
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return (or lazily create) the shared SQLAlchemy engine."""
    global _engine
    if _engine is None:
        url = os.environ["DATABASE_URL"]
        _engine = create_engine(url, pool_pre_ping=True)
        logger.info("Database engine created.")
    return _engine


def fetch_all(query: str, params: dict | None = None) -> list[dict]:
    """Execute *query* and return a list of row-dicts."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]


def execute(query: str, params: dict | None = None) -> None:
    """Execute a DML statement (INSERT / UPDATE / DELETE / TRUNCATE)."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(query), params or {})