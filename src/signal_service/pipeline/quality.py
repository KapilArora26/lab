"""Flagged data-quality rows and the engineering removal path.

Suspect rows stay in silver until someone asks to remove them. Removal archives
the batch's suspect rows in SILVER_REJECTS and recomputes gold. It does not
reload bronze, so a null business key cannot be inserted again by an anti-join.
It does not DROP or TRUNCATE.

Every archived row gets a non-null ``row_key`` (a hash of the batch, the source
table, and the full business row). A later load or ``RERUN_STAGING`` excludes
that key. A second call finds nothing left to delete.
"""

from __future__ import annotations

from signal_service.hashing import bronze_row_key, timestamp_reason
from signal_service.pipeline.propagate import clock, load_gold, write_run

_TARGETS = (
    ("silver_groups", "bronze_groups", "group_id"),
    ("silver_support_tickets", "bronze_support_tickets", "ticket_id"),
    ("silver_deploys", "bronze_deploys", "deploy_id"),
    ("silver_demo_meta", "bronze_demo_meta", "key"),
    ("silver_golden_tickets", "bronze_golden_tickets", "example_id"),
    ("silver_external_events", "bronze_external_events", "event_id"),
    ("silver_spend", "silver_spend", "row_key"),
)


def remove_suspect(warehouse, batch_id: str, reason: str | None = None) -> dict:
    """Archive one batch's suspect rows and recompute its gold.

    Out-of-range timestamps are flagged first, including rows an older load
    left as ``ok``. A second call finds nothing to delete. Rows that are
    already ok stay. Gold is recomputed from the silver rows that remain.
    Bronze is not reloaded.
    """
    note = (reason or "removed on request").strip() or "removed on request"
    if len(note) > 200:
        note = note[:200]
    _flag_timestamps(warehouse, batch_id)
    removed_at = clock()
    removed = 0
    for table, source, _key in _TARGETS:
        rows = warehouse.query(
            f"""
            SELECT * FROM {table}
            WHERE _batch_id = ? AND dq_status = 'suspect'
            """,
            [batch_id],
        )
        if not rows:
            continue
        already = _archived_keys(warehouse, batch_id, source)
        fresh = []
        for row in rows:
            identity = row.get("row_key") or bronze_row_key(source, batch_id, row)
            if not identity:
                identity = bronze_row_key(source, batch_id, row)
            stamped = dict(row)
            stamped["row_key"] = str(identity)
            if stamped["row_key"] not in already:
                fresh.append(stamped)
        if fresh:
            warehouse.executemany(
                """
                INSERT INTO silver_rejects (
                    reject_id, source_table, row_json, reason, _loaded_at, _batch_id,
                    row_key, removed_by, removed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        f"remove:{batch_id}:{source}:{row['row_key']}",
                        source,
                        _archive_json(row),
                        _archive_reason(row.get("dq_reason"), note),
                        removed_at,
                        batch_id,
                        row["row_key"],
                        "engineering",
                        removed_at,
                    )
                    for row in fresh
                ],
            )
        warehouse.execute(
            f"DELETE FROM {table} WHERE _batch_id = ? AND dq_status = 'suspect'",
            [batch_id],
        )
        removed += len(rows)
    if removed == 0:
        return {
            "batch_id": batch_id,
            "removed": 0,
            "reason": note,
            "status": "success",
        }
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


def _archived_keys(warehouse, batch_id: str, source: str) -> set[str]:
    rows = warehouse.query(
        """
        SELECT row_key
        FROM silver_rejects
        WHERE _batch_id = ?
          AND source_table = ?
          AND removed_by IS NOT NULL
          AND row_key IS NOT NULL
        """,
        [batch_id, source],
    )
    return {str(row["row_key"]) for row in rows}


def _flag_timestamps(warehouse, batch_id: str) -> None:
    """Mark epoch, pre-floor, and future timestamps that an older load left ok.

    Group hour is the session timestamp. Spend uses ``logged_at``.
    """
    groups = warehouse.query("SELECT * FROM silver_groups WHERE _batch_id = ?", [batch_id])
    for row in groups:
        reason = timestamp_reason(row.get("hour"))
        if not reason or row.get("dq_status") == "suspect":
            continue
        warehouse.execute(
            """
            UPDATE silver_groups
            SET dq_status = 'suspect', dq_reason = ?
            WHERE _batch_id = ?
              AND group_id IS NOT DISTINCT FROM ?
              AND hour IS NOT DISTINCT FROM ?
              AND n_sessions IS NOT DISTINCT FROM ?
              AND n_convert IS NOT DISTINCT FROM ?
              AND n_attempt IS NOT DISTINCT FROM ?
              AND COALESCE(dq_status, 'ok') <> 'suspect'
            """,
            [
                reason,
                batch_id,
                row.get("group_id"),
                row.get("hour"),
                row.get("n_sessions"),
                row.get("n_convert"),
                row.get("n_attempt"),
            ],
        )
    try:
        spend_rows = warehouse.query("SELECT * FROM silver_spend WHERE _batch_id = ?", [batch_id])
    except Exception:
        return
    for row in spend_rows:
        reason = timestamp_reason(row.get("logged_at"))
        if not reason or row.get("dq_status") == "suspect":
            continue
        warehouse.execute(
            """
            UPDATE silver_spend
            SET dq_status = 'suspect', dq_reason = ?
            WHERE _batch_id = ?
              AND logged_at IS NOT DISTINCT FROM ?
              AND case_id IS NOT DISTINCT FROM ?
              AND kind IS NOT DISTINCT FROM ?
              AND COALESCE(dq_status, 'ok') <> 'suspect'
            """,
            [reason, batch_id, row.get("logged_at"), row.get("case_id"), row.get("kind")],
        )


def _archive_reason(dq_reason, note: str) -> str:
    base = str(dq_reason or "suspect")
    if note in base:
        return base[:500]
    return f"{base}; {note}"[:500]


def _archive_json(row: dict) -> str:
    from signal_service.pipeline.propagate import _json_row

    return _json_row(row)
