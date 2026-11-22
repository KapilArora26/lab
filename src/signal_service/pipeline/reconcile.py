"""Check that a batch did not drop rows between layers."""

from __future__ import annotations


def _count(warehouse, sql: str, params: list | None = None) -> int:
    rows = warehouse.query(sql, params)
    if not rows:
        return 0
    value = rows[0].get("n")
    if value is None:
        return 0
    return int(value)


def _reject_count(warehouse, batch_id: str, source: str) -> int:
    return _count(
        warehouse,
        """
        SELECT COUNT(*)::INTEGER AS n
        FROM silver_rejects
        WHERE _batch_id = ? AND source_table = ?
        """,
        [batch_id, source],
    )


def _table_count(warehouse, table: str, batch_id: str) -> int:
    return _count(
        warehouse,
        f"SELECT COUNT(*)::INTEGER AS n FROM {table} WHERE _batch_id = ?",
        [batch_id],
    )


def latest_runs(warehouse, batch_id: str) -> dict[str, dict]:
    rows = warehouse.query(
        """
        SELECT layer, status, rows_in, rows_out, rows_rejected, error, started_at
        FROM pipeline_runs
        WHERE batch_id = ?
        ORDER BY started_at
        """,
        [batch_id],
    )
    latest: dict[str, dict] = {}
    for row in rows:
        latest[str(row["layer"])] = row
    return latest


def latest_layer_status(warehouse, batch_id: str) -> str | None:
    """Return an error when the newest row for a layer is not a balanced success."""
    problems = []
    for layer, row in latest_runs(warehouse, batch_id).items():
        if str(row.get("status")) != "success":
            problems.append(f"{layer} status is {row.get('status')}: {row.get('error') or ''}".strip())
            continue
        rows_in = int(row.get("rows_in") or 0)
        rows_out = int(row.get("rows_out") or 0)
        rows_rejected = int(row.get("rows_rejected") or 0)
        if rows_in != rows_out + rows_rejected:
            problems.append(
                f"{layer} rows_in {rows_in} != rows_out {rows_out} + rows_rejected {rows_rejected}"
            )
    if not problems:
        return None
    return "; ".join(problems)


def reconciliation_errors(
    warehouse,
    batch_id: str,
    *,
    expect_topics: int | None = None,
    expect_external: int | None = None,
) -> list[str]:
    """Rows in bronze must equal silver plus rejects, and every layer must balance."""
    errors: list[str] = []
    pairs = (
        ("bronze_groups", "silver_groups", "bronze_groups"),
        ("bronze_support_tickets", "silver_support_tickets", "bronze_support_tickets"),
        ("bronze_deploys", "silver_deploys", "bronze_deploys"),
        ("bronze_demo_meta", "silver_demo_meta", "bronze_demo_meta"),
        ("bronze_golden_tickets", "silver_golden_tickets", "bronze_golden_tickets"),
        ("bronze_external_events", "silver_external_events", "bronze_external_events"),
    )
    for bronze, silver, source in pairs:
        incoming = _table_count(warehouse, bronze, batch_id)
        outgoing = _table_count(warehouse, silver, batch_id)
        rejected = _reject_count(warehouse, batch_id, source)
        if incoming != outgoing + rejected:
            errors.append(
                f"{bronze} {incoming} != {silver} {outgoing} + rejects {rejected}"
            )
    bronze_external = _table_count(warehouse, "bronze_external_events", batch_id)
    silver_external = _table_count(warehouse, "silver_external_events", batch_id)
    gold_external = _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM gold_external_events WHERE _batch_id = ?", [batch_id])
    if bronze_external != silver_external or silver_external != gold_external:
        errors.append(
            f"external events bronze {bronze_external}, silver {silver_external}, gold {gold_external}"
        )
    if expect_external is not None and bronze_external != expect_external:
        errors.append(f"external events are {bronze_external}, expected {expect_external}")

    legacy_topics = _count(
        warehouse,
        """
        SELECT COUNT(*)::INTEGER AS n
        FROM signals.ticket_topics
        WHERE ticket_id IN (
            SELECT ticket_id FROM bronze_support_tickets WHERE _batch_id = ?
        )
        """,
        [batch_id],
    )
    silver_topics = _table_count(warehouse, "silver_ticket_topics", batch_id)
    gold_topics = _count(
        warehouse,
        "SELECT COALESCE(SUM(ticket_count), 0)::INTEGER AS n FROM gold_ticket_topics WHERE _batch_id = ?",
        [batch_id],
    )
    if legacy_topics != silver_topics or silver_topics != gold_topics:
        errors.append(
            f"ticket topics legacy {legacy_topics}, silver {silver_topics}, gold {gold_topics}"
        )
    if expect_topics is not None and silver_topics != expect_topics:
        errors.append(f"ticket topics are {silver_topics}, expected {expect_topics}")

    runs = warehouse.query(
        """
        SELECT layer, status, rows_in, rows_out, rows_rejected
        FROM pipeline_runs
        WHERE batch_id = ?
        ORDER BY started_at
        """,
        [batch_id],
    )
    latest: dict[str, dict] = {}
    for row in runs:
        rows_in = int(row.get("rows_in") or 0)
        rows_out = int(row.get("rows_out") or 0)
        rows_rejected = int(row.get("rows_rejected") or 0)
        if rows_in != rows_out + rows_rejected:
            errors.append(
                f"{row.get('layer')} rows_in {rows_in} != rows_out {rows_out} + rows_rejected {rows_rejected}"
            )
        latest[str(row.get("layer"))] = row
    for layer in ("bronze", "silver", "gold"):
        row = latest.get(layer)
        if row is None:
            errors.append(f"{layer} has no pipeline run")
            continue
        if str(row.get("status")) != "success":
            errors.append(f"{layer} status is {row.get('status')}")
    errors.extend(_suspect_errors(warehouse, batch_id))
    return errors


def _suspect_errors(warehouse, batch_id: str) -> list[str]:
    """Suspect silver rows are listed once in GOLD_DATA_QUALITY and flag gold."""
    errors: list[str] = []
    tables = (
        ("silver_groups", "groups"),
        ("silver_support_tickets", "support_tickets"),
        ("silver_deploys", "deploys"),
        ("silver_demo_meta", "demo_meta"),
        ("silver_golden_tickets", "golden_tickets"),
        ("silver_external_events", "external_events"),
    )
    suspect = 0
    for table, label in tables:
        count = _count(
            warehouse,
            f"""
            SELECT COUNT(*)::INTEGER AS n
            FROM {table}
            WHERE _batch_id = ? AND COALESCE(dq_status, 'ok') = 'suspect'
            """,
            [batch_id],
        )
        listed = _count(
            warehouse,
            """
            SELECT COUNT(*)::INTEGER AS n
            FROM gold_data_quality
            WHERE batch_id = ? AND "table" = ?
            """,
            [batch_id, label],
        )
        suspect += count
        if count != listed:
            errors.append(f"{label} suspects {count} != gold_data_quality {listed}")
    hourly = _count(
        warehouse,
        """
        SELECT COUNT(*)::INTEGER AS n
        FROM silver_groups
        WHERE _batch_id = ? AND COALESCE(dq_status, 'ok') = 'suspect' AND hour IS NOT NULL
        """,
        [batch_id],
    )
    flagged = _count(
        warehouse,
        """
        SELECT COALESCE(SUM(dq_suspect_rows), 0)::INTEGER AS n
        FROM gold_hourly_conversion
        WHERE _batch_id = ?
        """,
        [batch_id],
    )
    if flagged != hourly:
        errors.append(f"gold suspect rows {flagged} != suspect groups {hourly}")
    if suspect and flagged == 0 and hourly:
        errors.append("suspect rows did not flag gold")
    return errors


def assert_reconciled(
    warehouse,
    batch_id: str,
    *,
    expect_topics: int | None = None,
    expect_external: int | None = None,
) -> None:
    errors = reconciliation_errors(
        warehouse,
        batch_id,
        expect_topics=expect_topics,
        expect_external=expect_external,
    )
    if errors:
        detail = "; ".join(errors)
        raise RuntimeError(
            f"batch {batch_id} is not reconciled ({detail}). "
            "Re-run with `python -m signal_service.cli rerun "
            f"{batch_id}`. That path does not DROP or TRUNCATE."
        )
