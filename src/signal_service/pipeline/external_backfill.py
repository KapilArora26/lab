"""Copy legacy EXTERNAL_EVENTS into bronze, then silver.

GOLD_EXTERNAL_EVENTS is a view over silver, so the silver write is what the
gold layer shows. The copy is idempotent on event_id. It does not DROP or
TRUNCATE, and it leaves rows that are already in bronze on their own batch.
"""

from __future__ import annotations

LEGACY_EXTERNAL_BATCH = "legacy-external"
LEGACY_EXTERNAL_SOURCE = "external_events"


def promote_legacy_external_events(warehouse) -> dict:
    from signal_service.signals.tables import ensure_signal_tables

    ensure_signal_tables(warehouse)
    before = _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM bronze_external_events")
    if getattr(warehouse, "backend", "duckdb") == "snowflake":
        from signal_service.pipeline.ddl import medallion_statements

        statements = medallion_statements("external_backfill")
    else:
        statements = duckdb_backfill_statements()
    for statement in statements:
        warehouse.execute(statement)
    after = _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM bronze_external_events")
    visible = _count(
        warehouse,
        """
        SELECT COUNT(*)::INTEGER AS n
        FROM gold_external_events
        WHERE event_id IN (
            SELECT event_id FROM signals.external_events WHERE event_id IS NOT NULL
        )
        """,
    )
    return {
        "batch_id": LEGACY_EXTERNAL_BATCH,
        "inserted": after - before,
        "bronze": after,
        "visible": visible,
    }


def duckdb_backfill_statements() -> list[str]:
    from signal_service.pipeline.ingest import SEED_BATCH_ID

    batch_id = _batch_sql(SEED_BATCH_ID)
    source = _batch_sql(LEGACY_EXTERNAL_SOURCE)
    fallback = _batch_sql(LEGACY_EXTERNAL_BATCH)
    return [
        f"""
        INSERT INTO bronze_external_events (
            event_id, case_id, title, summary, source_url, source_type, relevance,
            observed_at, metric, provider, _ingested_at, _source, _batch_id
        )
        SELECT
            e.event_id, e.case_id, e.title, e.summary, e.source_url, e.source_type,
            e.relevance, e.observed_at, e.metric, e.provider,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
            {source},
            picked.batch_id
        FROM (
            SELECT
                event_id, case_id, title, summary, source_url, source_type, relevance,
                observed_at, metric, provider,
                ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY observed_at) AS rn
            FROM signals.external_events
            WHERE event_id IS NOT NULL
        ) e
        CROSS JOIN (
            SELECT COALESCE(
                (
                    SELECT MAX(batch_id)
                    FROM pipeline_runs
                    WHERE batch_id = {batch_id}
                      AND layer = 'bronze'
                      AND status = 'success'
                ),
                (
                    SELECT batch_id
                    FROM pipeline_runs
                    WHERE layer = 'bronze' AND status = 'success'
                    ORDER BY started_at DESC
                    LIMIT 1
                ),
                {fallback}
            ) AS batch_id
        ) picked
        WHERE e.rn = 1
          AND NOT EXISTS (
              SELECT 1 FROM bronze_external_events b WHERE b.event_id = e.event_id
          )
        """,
        f"""
        INSERT INTO silver_external_events (
            event_id, case_id, title, summary, source_url, source_type, relevance,
            observed_at, metric, provider, _loaded_at, _batch_id
        )
        SELECT
            b.event_id, b.case_id, b.title, b.summary, b.source_url, b.source_type,
            b.relevance, b.observed_at, b.metric, b.provider,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
            b._batch_id
        FROM bronze_external_events b
        WHERE b._source = {source}
          AND b.event_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM silver_external_events s WHERE s.event_id = b.event_id
          )
        """,
    ]


def _batch_sql(value: str) -> str:
    if "'" in value:
        raise ValueError("batch id must not contain a quote")
    return f"'{value}'"


def _count(warehouse, sql: str) -> int:
    rows = warehouse.query(sql)
    if not rows:
        return 0
    value = rows[0].get("n")
    if value is None:
        return 0
    return int(value)
