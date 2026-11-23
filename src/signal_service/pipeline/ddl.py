"""DuckDB medallion objects. Snowflake uses sql/snowflake/002_medallion.sql."""

from __future__ import annotations

from signal_service.paths import sql_path
from signal_service.pipeline.sql_split import split_sql

_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS bronze_groups (
        group_id VARCHAR,
        hour TIMESTAMP,
        platform VARCHAR,
        country VARCHAR,
        channel VARCHAR,
        payment_method VARCHAR,
        sku VARCHAR,
        app_version VARCHAR,
        n_sessions INTEGER,
        n_convert INTEGER,
        n_attempt INTEGER,
        _ingested_at TIMESTAMP,
        _source VARCHAR,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze_support_tickets (
        ticket_id VARCHAR,
        ts TIMESTAMP,
        platform VARCHAR,
        country VARCHAR,
        channel VARCHAR,
        payment_method VARCHAR,
        sku VARCHAR,
        app_version VARCHAR,
        product_area VARCHAR,
        text_redacted VARCHAR,
        _ingested_at TIMESTAMP,
        _source VARCHAR,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze_deploys (
        deploy_id VARCHAR,
        deployed_at TIMESTAMP,
        platform VARCHAR,
        app_version VARCHAR,
        note VARCHAR,
        _ingested_at TIMESTAMP,
        _source VARCHAR,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze_demo_meta (
        key VARCHAR,
        value VARCHAR,
        _ingested_at TIMESTAMP,
        _source VARCHAR,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze_golden_tickets (
        example_id VARCHAR,
        text_redacted VARCHAR,
        topic VARCHAR,
        sentiment VARCHAR,
        severity VARCHAR,
        product_area VARCHAR,
        _ingested_at TIMESTAMP,
        _source VARCHAR,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bronze_external_events (
        event_id VARCHAR,
        case_id VARCHAR,
        title VARCHAR,
        summary VARCHAR,
        source_url VARCHAR,
        source_type VARCHAR,
        relevance DOUBLE,
        observed_at TIMESTAMP,
        metric VARCHAR,
        provider VARCHAR,
        _ingested_at TIMESTAMP,
        _source VARCHAR,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_groups (
        group_id VARCHAR,
        hour TIMESTAMP,
        platform VARCHAR,
        country VARCHAR,
        channel VARCHAR,
        payment_method VARCHAR,
        sku VARCHAR,
        app_version VARCHAR,
        n_sessions INTEGER,
        n_convert INTEGER,
        n_attempt INTEGER,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_support_tickets (
        ticket_id VARCHAR,
        ts TIMESTAMP,
        platform VARCHAR,
        country VARCHAR,
        channel VARCHAR,
        payment_method VARCHAR,
        sku VARCHAR,
        app_version VARCHAR,
        product_area VARCHAR,
        text_redacted VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_deploys (
        deploy_id VARCHAR,
        deployed_at TIMESTAMP,
        platform VARCHAR,
        app_version VARCHAR,
        note VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_demo_meta (
        key VARCHAR,
        value VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_golden_tickets (
        example_id VARCHAR,
        text_redacted VARCHAR,
        topic VARCHAR,
        sentiment VARCHAR,
        severity VARCHAR,
        product_area VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_ticket_topics (
        ticket_id VARCHAR,
        case_id VARCHAR,
        classified_at TIMESTAMP,
        topic VARCHAR,
        sentiment VARCHAR,
        severity VARCHAR,
        product_area VARCHAR,
        evidence_snippet VARCHAR,
        confidence DOUBLE,
        model VARCHAR,
        provider VARCHAR,
        input_hash VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_external_events (
        event_id VARCHAR,
        case_id VARCHAR,
        title VARCHAR,
        summary VARCHAR,
        source_url VARCHAR,
        source_type VARCHAR,
        relevance DOUBLE,
        observed_at TIMESTAMP,
        metric VARCHAR,
        provider VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_spend (
        logged_at TIMESTAMP,
        case_id VARCHAR,
        kind VARCHAR,
        estimated_usd DOUBLE,
        input_tokens INTEGER,
        output_tokens INTEGER,
        provider VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS silver_rejects (
        reject_id VARCHAR,
        source_table VARCHAR,
        row_json VARCHAR,
        reason VARCHAR,
        _loaded_at TIMESTAMP,
        _batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_hourly_conversion (
        hour TIMESTAMP,
        platform VARCHAR,
        country VARCHAR,
        payment_method VARCHAR,
        app_version VARCHAR,
        sessions INTEGER,
        converted INTEGER,
        conversion_rate DOUBLE,
        baseline_rate DOUBLE,
        seasonal_index DOUBLE,
        expected_rate DOUBLE,
        is_anomaly BOOLEAN,
        _batch_id VARCHAR,
        _loaded_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_revenue_at_risk (
        hour TIMESTAMP,
        platform VARCHAR,
        country VARCHAR,
        payment_method VARCHAR,
        app_version VARCHAR,
        lost_conversions DOUBLE,
        aov_usd DOUBLE,
        session_weight DOUBLE,
        revenue_at_risk_usd DOUBLE,
        _batch_id VARCHAR,
        _loaded_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_case_facts (
        case_id VARCHAR,
        metric VARCHAR,
        segment_json VARCHAR,
        status VARCHAR,
        owner VARCHAR,
        opened_at TIMESTAMP,
        started_at TIMESTAMP,
        severity VARCHAR,
        revenue_at_risk_usd DOUBLE,
        _batch_id VARCHAR,
        _loaded_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_ticket_topics (
        topic VARCHAR,
        sentiment VARCHAR,
        severity VARCHAR,
        product_area VARCHAR,
        ticket_count INTEGER,
        avg_confidence DOUBLE,
        _batch_id VARCHAR,
        _loaded_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gold_daily_spend (
        spend_date DATE,
        spend_usd DOUBLE,
        cap_usd DOUBLE,
        headroom_usd DOUBLE,
        _batch_id VARCHAR,
        _loaded_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR,
        batch_id VARCHAR,
        layer VARCHAR,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        status VARCHAR,
        rows_in INTEGER,
        rows_out INTEGER,
        rows_rejected INTEGER,
        error VARCHAR
    )
    """,
    """
    CREATE OR REPLACE VIEW silver_sessions AS
    SELECT
        's-' || g.group_id || '-' || r.range::VARCHAR AS session_id,
        g.hour AS ts,
        g.platform,
        g.country,
        g.channel,
        g.payment_method,
        g.sku,
        g.app_version,
        r.range < g.n_convert AS converted,
        g._batch_id,
        g._loaded_at
    FROM (
        SELECT * FROM silver_groups
        WHERE n_sessions >= 0
          AND n_convert >= 0
          AND n_attempt >= 0
          AND n_sessions >= n_convert
          AND n_attempt >= n_convert
          AND hour IS NOT NULL
          AND platform IS NOT NULL
          AND country IS NOT NULL
    ) g, range(g.n_sessions::BIGINT) r
    """,
    """
    CREATE OR REPLACE VIEW silver_payments AS
    SELECT
        'p-' || g.group_id || '-' || r.range::VARCHAR AS payment_id,
        CASE
            WHEN r.range < g.n_convert THEN 'o-' || g.group_id || '-' || r.range::VARCHAR
            ELSE NULL
        END AS order_id,
        g.hour AS ts,
        g.payment_method AS method,
        CASE WHEN g.payment_method = 'upi' THEN 'razorpay_upi' ELSE 'stripe' END AS gateway,
        CASE WHEN r.range < g.n_convert THEN 'succeeded' ELSE 'failed' END AS status,
        CASE
            WHEN r.range < g.n_convert THEN NULL
            WHEN g.payment_method = 'upi' THEN 'UPI_OTP_TIMEOUT'
            ELSE 'CARD_DECLINED'
        END AS error_code,
        (
            SELECT CAST(value AS DOUBLE) FROM silver_demo_meta WHERE key = 'aov_usd'
        ) AS amount_usd,
        g.platform,
        g.country,
        g.app_version,
        g._batch_id,
        g._loaded_at
    FROM (
        SELECT * FROM silver_groups
        WHERE n_sessions >= 0
          AND n_convert >= 0
          AND n_attempt >= 0
          AND n_sessions >= n_convert
          AND n_attempt >= n_convert
          AND hour IS NOT NULL
          AND platform IS NOT NULL
          AND country IS NOT NULL
    ) g, range(g.n_attempt::BIGINT) r
    """,
    """
    CREATE OR REPLACE VIEW silver_orders AS
    SELECT
        'o-' || g.group_id || '-' || r.range::VARCHAR AS order_id,
        's-' || g.group_id || '-' || r.range::VARCHAR AS session_id,
        g.hour AS ts,
        g.platform,
        g.country,
        g.channel,
        g.payment_method,
        g.sku,
        g.app_version,
        (
            SELECT CAST(value AS DOUBLE) FROM silver_demo_meta WHERE key = 'aov_usd'
        ) AS amount_usd,
        'paid' AS status,
        g._batch_id,
        g._loaded_at
    FROM (
        SELECT * FROM silver_groups
        WHERE n_sessions >= 0
          AND n_convert >= 0
          AND n_attempt >= 0
          AND n_sessions >= n_convert
          AND n_attempt >= n_convert
          AND hour IS NOT NULL
          AND platform IS NOT NULL
          AND country IS NOT NULL
    ) g, range(g.n_convert::BIGINT) r
    """,
    """
    CREATE OR REPLACE VIEW groups AS
    SELECT
        group_id, hour, platform, country, channel, payment_method, sku, app_version,
        n_sessions, n_convert, n_attempt
    FROM silver_groups
    """,
    """
    CREATE OR REPLACE VIEW sessions AS
    SELECT session_id, ts, platform, country, channel, payment_method, sku, app_version, converted
    FROM silver_sessions
    """,
    """
    CREATE OR REPLACE VIEW payments AS
    SELECT
        payment_id, order_id, ts, method, gateway, status, error_code, amount_usd,
        platform, country, app_version
    FROM silver_payments
    """,
    """
    CREATE OR REPLACE VIEW orders AS
    SELECT
        order_id, session_id, ts, platform, country, channel, payment_method, sku,
        app_version, amount_usd, status
    FROM silver_orders
    """,
    """
    CREATE OR REPLACE VIEW support_tickets AS
    SELECT
        ticket_id, ts, platform, country, channel, payment_method, sku, app_version,
        product_area, text_redacted
    FROM bronze_support_tickets
    """,
    """
    CREATE OR REPLACE VIEW deploys AS
    SELECT deploy_id, deployed_at, platform, app_version, note
    FROM silver_deploys
    """,
    """
    CREATE OR REPLACE VIEW demo_meta AS
    SELECT key, value FROM silver_demo_meta
    """,
    """
    CREATE OR REPLACE VIEW golden_tickets AS
    SELECT example_id, text_redacted, topic, sentiment, severity, product_area
    FROM silver_golden_tickets
    """,
    """
    ALTER TABLE silver_groups ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'
    """,
    """
    ALTER TABLE silver_groups ADD COLUMN IF NOT EXISTS dq_reason VARCHAR
    """,
    """
    ALTER TABLE silver_support_tickets ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'
    """,
    """
    ALTER TABLE silver_support_tickets ADD COLUMN IF NOT EXISTS dq_reason VARCHAR
    """,
    """
    ALTER TABLE silver_deploys ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'
    """,
    """
    ALTER TABLE silver_deploys ADD COLUMN IF NOT EXISTS dq_reason VARCHAR
    """,
    """
    ALTER TABLE silver_demo_meta ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'
    """,
    """
    ALTER TABLE silver_demo_meta ADD COLUMN IF NOT EXISTS dq_reason VARCHAR
    """,
    """
    ALTER TABLE silver_golden_tickets ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'
    """,
    """
    ALTER TABLE silver_golden_tickets ADD COLUMN IF NOT EXISTS dq_reason VARCHAR
    """,
    """
    ALTER TABLE silver_external_events ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'
    """,
    """
    ALTER TABLE silver_external_events ADD COLUMN IF NOT EXISTS dq_reason VARCHAR
    """,
    """
    ALTER TABLE silver_rejects ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_groups ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_support_tickets ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_deploys ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_demo_meta ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_golden_tickets ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_external_events ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_spend ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'
    """,
    """
    ALTER TABLE silver_spend ADD COLUMN IF NOT EXISTS dq_reason VARCHAR
    """,
    """
    ALTER TABLE silver_spend ADD COLUMN IF NOT EXISTS row_key VARCHAR
    """,
    """
    ALTER TABLE silver_rejects ADD COLUMN IF NOT EXISTS removed_by VARCHAR
    """,
    """
    ALTER TABLE silver_rejects ADD COLUMN IF NOT EXISTS removed_at TIMESTAMP
    """,
    """
    ALTER TABLE gold_hourly_conversion ADD COLUMN IF NOT EXISTS dq_suspect_rows INTEGER DEFAULT 0
    """,
    """
    ALTER TABLE gold_revenue_at_risk ADD COLUMN IF NOT EXISTS dq_suspect_rows INTEGER DEFAULT 0
    """,
    """
    ALTER TABLE gold_daily_spend ADD COLUMN IF NOT EXISTS dq_suspect_rows INTEGER DEFAULT 0
    """,
    """
    CREATE OR REPLACE VIEW gold_data_quality AS
    SELECT _batch_id AS batch_id, 'groups' AS "table", CAST(group_id AS VARCHAR) AS "key",
           dq_reason AS reason, _loaded_at AS first_seen
    FROM silver_groups
    WHERE dq_status = 'suspect'
    UNION ALL
    SELECT _batch_id, 'support_tickets', CAST(ticket_id AS VARCHAR), dq_reason, _loaded_at
    FROM silver_support_tickets
    WHERE dq_status = 'suspect'
    UNION ALL
    SELECT _batch_id, 'deploys', CAST(deploy_id AS VARCHAR), dq_reason, _loaded_at
    FROM silver_deploys
    WHERE dq_status = 'suspect'
    UNION ALL
    SELECT _batch_id, 'demo_meta', CAST(key AS VARCHAR), dq_reason, _loaded_at
    FROM silver_demo_meta
    WHERE dq_status = 'suspect'
    UNION ALL
    SELECT _batch_id, 'golden_tickets', CAST(example_id AS VARCHAR), dq_reason, _loaded_at
    FROM silver_golden_tickets
    WHERE dq_status = 'suspect'
    UNION ALL
    SELECT _batch_id, 'external_events', CAST(event_id AS VARCHAR), dq_reason, _loaded_at
    FROM silver_external_events
    WHERE dq_status = 'suspect'
    """,
    """
    CREATE OR REPLACE VIEW gold_health AS
    SELECT 'bronze' AS layer,
           (SELECT COUNT(*) FROM bronze_groups) AS row_count,
           (SELECT MAX(_ingested_at) FROM bronze_groups) AS fresh_at,
           (
               SELECT status FROM pipeline_runs
               WHERE layer = 'bronze'
               ORDER BY started_at DESC
               LIMIT 1
           ) AS latest_status,
           0 AS suspect_rows
    UNION ALL
    SELECT 'silver',
           (SELECT COUNT(*) FROM silver_groups),
           (SELECT MAX(_loaded_at) FROM silver_groups),
           (
               SELECT status FROM pipeline_runs
               WHERE layer = 'silver'
               ORDER BY started_at DESC
               LIMIT 1
           ),
           (SELECT COUNT(*) FROM gold_data_quality)
    UNION ALL
    SELECT 'gold',
           (SELECT COUNT(*) FROM gold_hourly_conversion),
           (SELECT MAX(_loaded_at) FROM gold_hourly_conversion),
           (
               SELECT status FROM pipeline_runs
               WHERE layer = 'gold'
               ORDER BY started_at DESC
               LIMIT 1
           ),
           (SELECT COALESCE(SUM(dq_suspect_rows), 0) FROM gold_hourly_conversion)
    """,
    """
    CREATE OR REPLACE VIEW gold_pipeline_lag AS
    WITH bronze AS (
        SELECT _batch_id AS batch_id, MIN(_ingested_at) AS bronze_ingested_at
        FROM (
            SELECT _batch_id, _ingested_at FROM bronze_groups
            UNION ALL
            SELECT _batch_id, _ingested_at FROM bronze_support_tickets
            UNION ALL
            SELECT _batch_id, _ingested_at FROM bronze_deploys
            UNION ALL
            SELECT _batch_id, _ingested_at FROM bronze_demo_meta
            UNION ALL
            SELECT _batch_id, _ingested_at FROM bronze_golden_tickets
            UNION ALL
            SELECT _batch_id, _ingested_at FROM bronze_external_events
        ) landed
        GROUP BY 1
    ),
    silver AS (
        SELECT _batch_id AS batch_id, MIN(_loaded_at) AS silver_loaded_at
        FROM (
            SELECT _batch_id, _loaded_at FROM silver_groups
            UNION ALL
            SELECT _batch_id, _loaded_at FROM silver_support_tickets
            UNION ALL
            SELECT _batch_id, _loaded_at FROM silver_deploys
            UNION ALL
            SELECT _batch_id, _loaded_at FROM silver_demo_meta
            UNION ALL
            SELECT _batch_id, _loaded_at FROM silver_golden_tickets
            UNION ALL
            SELECT _batch_id, _loaded_at FROM silver_external_events
            UNION ALL
            SELECT _batch_id, _loaded_at FROM silver_rejects
        ) loaded
        GROUP BY 1
    ),
    gold AS (
        SELECT _batch_id AS batch_id, MIN(_loaded_at) AS gold_loaded_at
        FROM (
            SELECT _batch_id, _loaded_at FROM gold_hourly_conversion
            UNION ALL
            SELECT _batch_id, _loaded_at FROM gold_revenue_at_risk
            UNION ALL
            SELECT _batch_id, _loaded_at FROM gold_daily_spend
            UNION ALL
            SELECT _batch_id, _loaded_at FROM gold_ticket_topics
            UNION ALL
            SELECT _batch_id, _loaded_at FROM gold_case_facts
        ) loaded
        GROUP BY 1
    )
    SELECT
        b.batch_id,
        b.bronze_ingested_at,
        s.silver_loaded_at,
        g.gold_loaded_at,
        CAST(date_diff('second', b.bronze_ingested_at, g.gold_loaded_at) AS INTEGER)
            AS staging_to_gold_seconds
    FROM bronze b
    LEFT JOIN silver s ON s.batch_id = b.batch_id
    LEFT JOIN gold g ON g.batch_id = b.batch_id
    """,
    """
    CREATE OR REPLACE VIEW gold_external_events AS
    SELECT
        event_id, case_id, title, summary, source_url, source_type, relevance,
        observed_at, metric, provider, dq_status, dq_reason, _batch_id, _loaded_at
    FROM silver_external_events
    """,
]


def duckdb_statements() -> list[str]:
    return [statement.strip() for statement in _STATEMENTS]


def medallion_statements(section: str | None = None) -> list[str]:
    from signal_service.pipeline.row_keys import render_medallion_sql

    raw = render_medallion_sql(sql_path("snowflake", "002_medallion.sql").read_text(encoding="utf-8"))
    pairs = split_sql(raw)
    if section is None:
        return [statement for _, statement in pairs]
    return [statement for name, statement in pairs if name == section]


def ensure_objects(warehouse) -> None:
    """Create medallion objects. Snowflake core DDL runs once per warehouse.

    ``deploy-snowflake`` applies this migration and then calls ingest, which
    calls ``ensure_objects`` again. A second pass would re-issue
    ``ADD COLUMN IF NOT EXISTS ... DEFAULT``. Snowflake rejects that once the
    column exists (``ambiguous column name ''``). The adapter also checks
    ``INFORMATION_SCHEMA.COLUMNS`` before each add, so a later process can
    apply the same file without rewriting rows.
    """
    if getattr(warehouse, "backend", "duckdb") == "snowflake":
        if not getattr(warehouse, "_sandbox_core_applied", False):
            for statement in medallion_statements("core"):
                warehouse.execute(statement)
            warehouse._sandbox_core_applied = True
    else:
        for statement in duckdb_statements():
            warehouse.execute(statement)
    from signal_service.pipeline.external_backfill import promote_legacy_external_events
    from signal_service.pipeline.row_keys import backfill_row_keys

    promote_legacy_external_events(warehouse)
    backfill_row_keys(warehouse)
