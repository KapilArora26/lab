"""Flagged data-quality rows and the engineering removal path.

Suspect rows stay in silver until someone asks to remove them. Removal is a
row-level DELETE of that batch's suspect rows, an archive in SILVER_REJECTS,
and a gold recompute. It does not DROP or TRUNCATE.
"""

from __future__ import annotations

from signal_service.pipeline.propagate import clock, load_gold, write_run

_TARGETS = (
    ("silver_groups", "bronze_groups", "group_id"),
    ("silver_support_tickets", "bronze_support_tickets", "ticket_id"),
    ("silver_deploys", "bronze_deploys", "deploy_id"),
    ("silver_demo_meta", "bronze_demo_meta", "key"),
    ("silver_golden_tickets", "bronze_golden_tickets", "example_id"),
    ("silver_external_events", "bronze_external_events", "event_id"),
)


def remove_suspect(warehouse, batch_id: str, reason: str | None = None) -> dict:
    """Archive one batch's suspect rows and recompute its gold.

    A second call finds nothing to delete. Rows that are already ok stay.
    """
    note = (reason or "removed on request").strip() or "removed on request"
    if len(note) > 200:
        note = note[:200]
    removed_at = clock()
    removed = 0
    for table, source, key in _TARGETS:
        rows = warehouse.query(
            f"""
            SELECT * FROM {table}
            WHERE _batch_id = ? AND dq_status = 'suspect'
            """,
            [batch_id],
        )
        if not rows:
            continue
        warehouse.executemany(
            """
            INSERT INTO silver_rejects (
                reject_id, source_table, row_json, reason, _loaded_at, _batch_id,
                row_key, removed_by, removed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    f"remove:{batch_id}:{table}:{index}:{row.get(key) or 'row'}",
                    source,
                    _archive_json(row),
                    _archive_reason(row.get("dq_reason"), note),
                    removed_at,
                    batch_id,
                    None if row.get(key) is None else str(row.get(key)),
                    "engineering",
                    removed_at,
                )
                for index, row in enumerate(rows)
            ],
        )
        warehouse.execute(
            f"DELETE FROM {table} WHERE _batch_id = ? AND dq_status = 'suspect'",
            [batch_id],
        )
        removed += len(rows)
    load_gold(warehouse, batch_id, removed_at)
    write_run(
        warehouse,
        batch_id=batch_id,
        layer="remove_suspect",
        started_at=removed_at,
        status="success",
        rows_in=removed,
        rows_out=0,
        rows_rejected=removed,
    )
    return {
        "batch_id": batch_id,
        "removed": removed,
        "reason": note,
        "status": "success",
    }


def _archive_reason(dq_reason, note: str) -> str:
    base = str(dq_reason or "suspect")
    if note in base:
        return base[:500]
    return f"{base}; {note}"[:500]


def _archive_json(row: dict) -> str:
    from signal_service.pipeline.propagate import _json_row

    return _json_row(row)
