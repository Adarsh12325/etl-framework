"""
Mock REST API service — simulates a real-world data source for the ETL framework.
Supports full and incremental fetches via the `since` query parameter.
"""

from datetime import datetime, timezone
from flask import Flask, jsonify, request

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Static dataset – acts as the "source system"
# ---------------------------------------------------------------------------
INITIAL_RECORDS = [
    {"id": 1,  "name": "Anna Lee",      "email": "anna@example.com",   "last_modified": "2024-01-01T10:00:00Z"},
    {"id": 2,  "name": "Ben Carter",    "email": "ben@example.com",    "last_modified": "2024-01-02T11:00:00Z"},
    {"id": 3,  "name": "Clara Stone",   "email": "clara@example.com",  "last_modified": "2024-01-03T12:00:00Z"},
    {"id": 4,  "name": "Dan Rivers",    "email": "dan@example.com",    "last_modified": "2024-01-04T13:00:00Z"},
    {"id": 5,  "name": "Eva Green",     "email": "eva@example.com",    "last_modified": "2024-01-05T14:00:00Z"},
]

# These records are added by the /add_records endpoint to simulate new data
# arriving between orchestrator runs (used by the incremental-load test).
EXTRA_RECORDS = [
    {"id": 6,  "name": "Frank Miles",   "email": "frank@example.com",  "last_modified": "2024-02-01T08:00:00Z"},
    {"id": 7,  "name": "Grace Hall",    "email": "grace@example.com",  "last_modified": "2024-02-02T09:00:00Z"},
    {"id": 8,  "name": "Henry Ford",    "email": "henry@example.com",  "last_modified": "2024-02-03T10:00:00Z"},
]

# Runtime store – starts with only the initial records
_records: list[dict] = list(INITIAL_RECORDS)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO-8601 timestamp string into a timezone-aware datetime."""
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Health-check endpoint for Docker."""
    return jsonify({"status": "ok"}), 200


@app.get("/data")
def get_data():
    """
    Return all records, optionally filtered by the `since` timestamp.

    Query params:
        since (str, optional): ISO-8601 timestamp.  Only records whose
                               last_modified is STRICTLY GREATER than
                               this value are returned.
    """
    since_param = request.args.get("since")

    if since_param:
        try:
            since_dt = _parse_ts(since_param)
        except ValueError:
            return jsonify({"error": f"Invalid 'since' timestamp: {since_param}"}), 400

        filtered = [
            r for r in _records
            if _parse_ts(r["last_modified"]) > since_dt
        ]
        return jsonify(filtered), 200

    return jsonify(_records), 200


@app.post("/add_records")
def add_records():
    """
    Add the pre-defined extra records to the dataset.
    Call this endpoint between orchestrator runs to simulate new data
    arriving in the source system (used for the incremental-load scenario).
    """
    added = 0
    existing_ids = {r["id"] for r in _records}
    for rec in EXTRA_RECORDS:
        if rec["id"] not in existing_ids:
            _records.append(rec)
            added += 1

    return jsonify({"message": f"Added {added} new record(s).", "total": len(_records)}), 200


@app.get("/reset")
def reset():
    """Reset the dataset to the initial 5 records."""
    global _records
    _records = list(INITIAL_RECORDS)
    return jsonify({"message": "Dataset reset to initial records.", "total": len(_records)}), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)