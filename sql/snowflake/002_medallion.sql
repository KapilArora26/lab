-- Medallion objects inside MCP_DB.SIGNAL_SANDBOX only.
-- Bronze is append-only. Silver and gold rows carry _loaded_at and _batch_id.
--
-- ALTER TABLE ... ADD COLUMN is additive. ALTER TASK ... RESUME is how a new
-- task starts, because tasks are created suspended. There is no DROP,
-- TRUNCATE, or GRANT. TASK_BRONZE_TO_SILVER and TASK_SILVER_TO_GOLD are
-- serverless triggered tasks: no SCHEDULE and no WAREHOUSE. Each WHEN clause
-- is SYSTEM$STREAM_HAS_DATA on table streams, so the task runs when those
-- streams get rows. TARGET_COMPLETION_INTERVAL is required for a serverless
-- triggered task. OVERLAP_POLICY = NO_OVERLAP is ALLOW_OVERLAPPING_EXECUTION
-- = FALSE. The minimum trigger interval stays at the Snowflake default
-- (30 seconds). This file does not set an account parameter. Each task body
-- is one statement:
-- CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL). A null batch reads every
-- stream with DML inside one transaction so the offset advances on commit,
-- then loads only batch ids that do not already have a gold success. It does
-- not invent a batch id and does not rewrite gold rows for any other batch.
-- LOAD_STAGING keeps the single argument (batch VARCHAR). A second argument
-- would overload the procedure already in the sandbox instead of replacing
-- it. Reprocess a batch with RERUN_STAGING(batch VARCHAR). A compilation
-- error is not a missing privilege. If the role lacks CREATE PROCEDURE,
-- ingest fails closed instead of reading bronze into the client.
--
-- A stream-triggered call that fails its audit still commits, so the stream
-- offset advances, and then drains STREAM_SILVER_GROUPS in a second
-- transaction. The task stays successful. A batch that already has a gold
-- success or gold failure is not loaded again. REMOVE_SUSPECT does not call
-- LOAD_STAGING. It archives suspect rows and recomputes gold.
--
-- Dynamic tables are the alternative when tasks are unavailable. They are not
-- created here because they need a warehouse name. Set SIGNAL_SNOWFLAKE_WAREHOUSE
-- and the installer will try one dynamic table at TARGET_LAG = '1 minute'.

-- @section core

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS (
    group_id VARCHAR,
    hour TIMESTAMP_NTZ,
    platform VARCHAR,
    country VARCHAR,
    channel VARCHAR,
    payment_method VARCHAR,
    sku VARCHAR,
    app_version VARCHAR,
    n_sessions INTEGER,
    n_convert INTEGER,
    n_attempt INTEGER,
    _ingested_at TIMESTAMP_NTZ,
    _source VARCHAR,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS (
    ticket_id VARCHAR,
    ts TIMESTAMP_NTZ,
    platform VARCHAR,
    country VARCHAR,
    channel VARCHAR,
    payment_method VARCHAR,
    sku VARCHAR,
    app_version VARCHAR,
    product_area VARCHAR,
    text_redacted VARCHAR,
    _ingested_at TIMESTAMP_NTZ,
    _source VARCHAR,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.BRONZE_DEPLOYS (
    deploy_id VARCHAR,
    deployed_at TIMESTAMP_NTZ,
    platform VARCHAR,
    app_version VARCHAR,
    note VARCHAR,
    _ingested_at TIMESTAMP_NTZ,
    _source VARCHAR,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.BRONZE_DEMO_META (
    key VARCHAR,
    value VARCHAR,
    _ingested_at TIMESTAMP_NTZ,
    _source VARCHAR,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.BRONZE_GOLDEN_TICKETS (
    example_id VARCHAR,
    text_redacted VARCHAR,
    topic VARCHAR,
    sentiment VARCHAR,
    severity VARCHAR,
    product_area VARCHAR,
    _ingested_at TIMESTAMP_NTZ,
    _source VARCHAR,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS (
    event_id VARCHAR,
    case_id VARCHAR,
    title VARCHAR,
    summary VARCHAR,
    source_url VARCHAR,
    source_type VARCHAR,
    relevance DOUBLE,
    observed_at TIMESTAMP_NTZ,
    metric VARCHAR,
    provider VARCHAR,
    _ingested_at TIMESTAMP_NTZ,
    _source VARCHAR,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS (
    group_id VARCHAR,
    hour TIMESTAMP_NTZ,
    platform VARCHAR,
    country VARCHAR,
    channel VARCHAR,
    payment_method VARCHAR,
    sku VARCHAR,
    app_version VARCHAR,
    n_sessions INTEGER,
    n_convert INTEGER,
    n_attempt INTEGER,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS (
    ticket_id VARCHAR,
    ts TIMESTAMP_NTZ,
    platform VARCHAR,
    country VARCHAR,
    channel VARCHAR,
    payment_method VARCHAR,
    sku VARCHAR,
    app_version VARCHAR,
    product_area VARCHAR,
    text_redacted VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS (
    deploy_id VARCHAR,
    deployed_at TIMESTAMP_NTZ,
    platform VARCHAR,
    app_version VARCHAR,
    note VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META (
    key VARCHAR,
    value VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS (
    example_id VARCHAR,
    text_redacted VARCHAR,
    topic VARCHAR,
    sentiment VARCHAR,
    severity VARCHAR,
    product_area VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_TICKET_TOPICS (
    ticket_id VARCHAR,
    case_id VARCHAR,
    classified_at TIMESTAMP_NTZ,
    topic VARCHAR,
    sentiment VARCHAR,
    severity VARCHAR,
    product_area VARCHAR,
    evidence_snippet VARCHAR,
    confidence DOUBLE,
    model VARCHAR,
    provider VARCHAR,
    input_hash VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS (
    event_id VARCHAR,
    case_id VARCHAR,
    title VARCHAR,
    summary VARCHAR,
    source_url VARCHAR,
    source_type VARCHAR,
    relevance DOUBLE,
    observed_at TIMESTAMP_NTZ,
    metric VARCHAR,
    provider VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND (
    logged_at TIMESTAMP_NTZ,
    case_id VARCHAR,
    kind VARCHAR,
    estimated_usd DOUBLE,
    input_tokens INTEGER,
    output_tokens INTEGER,
    provider VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS (
    reject_id VARCHAR,
    source_table VARCHAR,
    row_json VARCHAR,
    reason VARCHAR,
    _loaded_at TIMESTAMP_NTZ,
    _batch_id VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION (
    hour TIMESTAMP_NTZ,
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
    _loaded_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK (
    hour TIMESTAMP_NTZ,
    platform VARCHAR,
    country VARCHAR,
    payment_method VARCHAR,
    app_version VARCHAR,
    lost_conversions DOUBLE,
    aov_usd DOUBLE,
    session_weight DOUBLE,
    revenue_at_risk_usd DOUBLE,
    _batch_id VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.GOLD_CASE_FACTS (
    case_id VARCHAR,
    metric VARCHAR,
    segment_json VARCHAR,
    status VARCHAR,
    owner VARCHAR,
    opened_at TIMESTAMP_NTZ,
    started_at TIMESTAMP_NTZ,
    severity VARCHAR,
    revenue_at_risk_usd DOUBLE,
    _batch_id VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.GOLD_TICKET_TOPICS (
    topic VARCHAR,
    sentiment VARCHAR,
    severity VARCHAR,
    product_area VARCHAR,
    ticket_count INTEGER,
    avg_confidence DOUBLE,
    _batch_id VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND (
    spend_date DATE,
    spend_usd DOUBLE,
    cap_usd DOUBLE,
    headroom_usd DOUBLE,
    _batch_id VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS (
    run_id VARCHAR,
    batch_id VARCHAR,
    layer VARCHAR,
    started_at TIMESTAMP_NTZ,
    finished_at TIMESTAMP_NTZ,
    status VARCHAR,
    rows_in INTEGER,
    rows_out INTEGER,
    rows_rejected INTEGER,
    error VARCHAR
);

-- Batch ids read out of streams inside LOAD_STAGING. The procedure deletes
-- these rows before it commits. The table exists so the procedure body can
-- name it at compile time.
CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE (
    batch_id VARCHAR
);

-- Additive data-quality columns. Snowflake does not treat
-- `ADD COLUMN IF NOT EXISTS ... DEFAULT` as idempotent: a second execution
-- raises `SQL compilation error: ambiguous column name ''` when the column
-- already exists. The adapter reads INFORMATION_SCHEMA.COLUMNS and skips the
-- ALTER in that case. These statements do not DROP and do not rewrite rows.
ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok';

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS ADD COLUMN IF NOT EXISTS dq_reason VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok';

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS ADD COLUMN IF NOT EXISTS dq_reason VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok';

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS ADD COLUMN IF NOT EXISTS dq_reason VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok';

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META ADD COLUMN IF NOT EXISTS dq_reason VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok';

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS ADD COLUMN IF NOT EXISTS dq_reason VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok';

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS ADD COLUMN IF NOT EXISTS dq_reason VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok';

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND ADD COLUMN IF NOT EXISTS dq_reason VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND ADD COLUMN IF NOT EXISTS row_key VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS ADD COLUMN IF NOT EXISTS removed_by VARCHAR;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS ADD COLUMN IF NOT EXISTS removed_at TIMESTAMP_NTZ;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION ADD COLUMN IF NOT EXISTS dq_suspect_rows INTEGER DEFAULT 0;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK ADD COLUMN IF NOT EXISTS dq_suspect_rows INTEGER DEFAULT 0;

ALTER TABLE MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND ADD COLUMN IF NOT EXISTS dq_suspect_rows INTEGER DEFAULT 0;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.SILVER_SESSIONS AS
SELECT
    's-' || g.group_id || '-' || f.index::VARCHAR AS session_id,
    g.hour AS ts,
    g.platform,
    g.country,
    g.channel,
    g.payment_method,
    g.sku,
    g.app_version,
    f.index < g.n_convert AS converted,
    g._batch_id,
    g._loaded_at
FROM (
    SELECT * FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    WHERE n_sessions >= 0
      AND n_convert >= 0
      AND n_attempt >= 0
      AND n_sessions >= n_convert
      AND n_attempt >= n_convert
      AND hour IS NOT NULL
      AND platform IS NOT NULL
      AND country IS NOT NULL
) g,
LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, g.n_sessions)) f;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.SILVER_PAYMENTS AS
SELECT
    'p-' || g.group_id || '-' || f.index::VARCHAR AS payment_id,
    CASE
        WHEN f.index < g.n_convert THEN 'o-' || g.group_id || '-' || f.index::VARCHAR
        ELSE NULL
    END AS order_id,
    g.hour AS ts,
    g.payment_method AS method,
    CASE WHEN g.payment_method = 'upi' THEN 'razorpay_upi' ELSE 'stripe' END AS gateway,
    CASE WHEN f.index < g.n_convert THEN 'succeeded' ELSE 'failed' END AS status,
    CASE
        WHEN f.index < g.n_convert THEN NULL
        WHEN g.payment_method = 'upi' THEN 'UPI_OTP_TIMEOUT'
        ELSE 'CARD_DECLINED'
    END AS error_code,
    (
        SELECT value::DOUBLE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'aov_usd'
    ) AS amount_usd,
    g.platform,
    g.country,
    g.app_version,
    g._batch_id,
    g._loaded_at
FROM (
    SELECT * FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    WHERE n_sessions >= 0
      AND n_convert >= 0
      AND n_attempt >= 0
      AND n_sessions >= n_convert
      AND n_attempt >= n_convert
      AND hour IS NOT NULL
      AND platform IS NOT NULL
      AND country IS NOT NULL
) g,
LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, g.n_attempt)) f;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.SILVER_ORDERS AS
SELECT
    'o-' || g.group_id || '-' || f.index::VARCHAR AS order_id,
    's-' || g.group_id || '-' || f.index::VARCHAR AS session_id,
    g.hour AS ts,
    g.platform,
    g.country,
    g.channel,
    g.payment_method,
    g.sku,
    g.app_version,
    (
        SELECT value::DOUBLE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'aov_usd'
    ) AS amount_usd,
    'paid' AS status,
    g._batch_id,
    g._loaded_at
FROM (
    SELECT * FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    WHERE n_sessions >= 0
      AND n_convert >= 0
      AND n_attempt >= 0
      AND n_sessions >= n_convert
      AND n_attempt >= n_convert
      AND hour IS NOT NULL
      AND platform IS NOT NULL
      AND country IS NOT NULL
) g,
LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, g.n_convert)) f;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.SESSIONS AS
SELECT session_id, ts, platform, country, channel, payment_method, sku, app_version, converted
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SESSIONS;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.PAYMENTS AS
SELECT
    payment_id, order_id, ts, method, gateway, status, error_code, amount_usd,
    platform, country, app_version
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_PAYMENTS;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.ORDERS AS
SELECT
    order_id, session_id, ts, platform, country, channel, payment_method, sku,
    app_version, amount_usd, status
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_ORDERS;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.GOLD_DATA_QUALITY AS
SELECT _batch_id AS batch_id, 'groups' AS "table", group_id::VARCHAR AS "key",
       dq_reason AS reason, _loaded_at AS first_seen
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
WHERE dq_status = 'suspect'
UNION ALL
SELECT _batch_id, 'support_tickets', ticket_id::VARCHAR, dq_reason, _loaded_at
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS
WHERE dq_status = 'suspect'
UNION ALL
SELECT _batch_id, 'deploys', deploy_id::VARCHAR, dq_reason, _loaded_at
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS
WHERE dq_status = 'suspect'
UNION ALL
SELECT _batch_id, 'demo_meta', key::VARCHAR, dq_reason, _loaded_at
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
WHERE dq_status = 'suspect'
UNION ALL
SELECT _batch_id, 'golden_tickets', example_id::VARCHAR, dq_reason, _loaded_at
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS
WHERE dq_status = 'suspect'
UNION ALL
SELECT _batch_id, 'external_events', event_id::VARCHAR, dq_reason, _loaded_at
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS
WHERE dq_status = 'suspect';

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.GOLD_HEALTH AS
SELECT 'bronze' AS layer,
       (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS) AS row_count,
       (SELECT MAX(_ingested_at) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS) AS fresh_at,
       (
           SELECT status FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
           WHERE layer = 'bronze'
           ORDER BY started_at DESC
           LIMIT 1
       ) AS latest_status,
       0 AS suspect_rows
UNION ALL
SELECT 'silver',
       (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS),
       (SELECT MAX(_loaded_at) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS),
       (
           SELECT status FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
           WHERE layer = 'silver'
           ORDER BY started_at DESC
           LIMIT 1
       ),
       (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DATA_QUALITY)
UNION ALL
SELECT 'gold',
       (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION),
       (SELECT MAX(_loaded_at) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION),
       (
           SELECT status FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
           WHERE layer = 'gold'
           ORDER BY started_at DESC
           LIMIT 1
       ),
       (SELECT COALESCE(SUM(dq_suspect_rows), 0) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION);

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.GOLD_PIPELINE_LAG AS
WITH bronze AS (
    SELECT _batch_id AS batch_id, MIN(_ingested_at) AS bronze_ingested_at
    FROM (
        SELECT _batch_id, _ingested_at FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS
        UNION ALL
        SELECT _batch_id, _ingested_at FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS
        UNION ALL
        SELECT _batch_id, _ingested_at FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEPLOYS
        UNION ALL
        SELECT _batch_id, _ingested_at FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEMO_META
        UNION ALL
        SELECT _batch_id, _ingested_at FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GOLDEN_TICKETS
        UNION ALL
        SELECT _batch_id, _ingested_at FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS
    ) landed
    GROUP BY 1
),
silver AS (
    SELECT _batch_id AS batch_id, MIN(_loaded_at) AS silver_loaded_at
    FROM (
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS
    ) loaded
    GROUP BY 1
),
gold AS (
    SELECT _batch_id AS batch_id, MIN(_loaded_at) AS gold_loaded_at
    FROM (
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.GOLD_TICKET_TOPICS
        UNION ALL
        SELECT _batch_id, _loaded_at FROM MCP_DB.SIGNAL_SANDBOX.GOLD_CASE_FACTS
    ) loaded
    GROUP BY 1
)
SELECT
    b.batch_id,
    b.bronze_ingested_at,
    s.silver_loaded_at,
    g.gold_loaded_at,
    DATEDIFF('second', b.bronze_ingested_at, g.gold_loaded_at) AS staging_to_gold_seconds
FROM bronze b
LEFT JOIN silver s ON s.batch_id = b.batch_id
LEFT JOIN gold g ON g.batch_id = b.batch_id;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.GOLD_EXTERNAL_EVENTS AS
SELECT
    event_id, case_id, title, summary, source_url, source_type, relevance,
    observed_at, metric, provider, _batch_id, _loaded_at
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS;

-- @section external_backfill

-- Idempotent copy of legacy EXTERNAL_EVENTS into bronze, then silver.
-- GOLD_EXTERNAL_EVENTS reads silver, so the merge is what the gold layer shows.
-- A second run inserts nothing. There is no DROP and no TRUNCATE.
INSERT INTO MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS (
    event_id, case_id, title, summary, source_url, source_type, relevance,
    observed_at, metric, provider, _ingested_at, _source, _batch_id
)
SELECT
    e.event_id,
    e.case_id,
    e.title,
    e.summary,
    e.source_url,
    e.source_type,
    e.relevance,
    e.observed_at,
    e.metric,
    e.provider,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
    'external_events',
    picked.batch_id
FROM (
    SELECT
        src.event_id,
        src.case_id,
        src.title,
        src.summary,
        src.source_url,
        src.source_type,
        src.relevance,
        src.observed_at,
        src.metric,
        src.provider,
        ROW_NUMBER() OVER (PARTITION BY src.event_id ORDER BY src.observed_at) AS rn
    FROM MCP_DB.SIGNAL_SANDBOX.EXTERNAL_EVENTS src
    WHERE src.event_id IS NOT NULL
) e
CROSS JOIN (
    SELECT COALESCE(
        (
            SELECT MAX(batch_id)
            FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
            WHERE batch_id = 'seed-2026-11-08'
              AND layer = 'bronze'
              AND status = 'success'
        ),
        (
            SELECT batch_id
            FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
            WHERE layer = 'bronze' AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
        ),
        'legacy-external'
    ) AS batch_id
) picked
WHERE e.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS b
      WHERE b.event_id = e.event_id
  );

MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS t
USING (
    SELECT
        event_id, case_id, title, summary, source_url, source_type, relevance,
        observed_at, metric, provider, _batch_id,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS _loaded_at
    FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS
    WHERE _source = 'external_events'
      AND event_id IS NOT NULL
) s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT (
    event_id, case_id, title, summary, source_url, source_type, relevance,
    observed_at, metric, provider, _loaded_at, _batch_id
) VALUES (
    s.event_id, s.case_id, s.title, s.summary, s.source_url, s.source_type, s.relevance,
    s.observed_at, s.metric, s.provider, s._loaded_at, s._batch_id
);

-- @section stream

-- Trigger sources for the tasks below. Every stream is ON TABLE. Table
-- streams are a supported triggered-task source, and CREATE STREAM ON TABLE
-- enables change tracking on that table, so no ALTER is required. A stream
-- on a view would need CHANGE_TRACKING on the view and its base tables.
-- Hybrid tables and streams on external tables are unsupported and are not
-- created here.

CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GROUPS
    ON TABLE MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_SUPPORT_TICKETS
    ON TABLE MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEPLOYS
    ON TABLE MCP_DB.SIGNAL_SANDBOX.BRONZE_DEPLOYS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEMO_META
    ON TABLE MCP_DB.SIGNAL_SANDBOX.BRONZE_DEMO_META
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GOLDEN_TICKETS
    ON TABLE MCP_DB.SIGNAL_SANDBOX.BRONZE_GOLDEN_TICKETS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_EXTERNAL_EVENTS
    ON TABLE MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.STREAM_SILVER_GROUPS
    ON TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    APPEND_ONLY = TRUE;

-- @section procedure

CREATE OR REPLACE PROCEDURE MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(batch VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_batch VARCHAR;
    v_loaded TIMESTAMP_NTZ;
    v_start TIMESTAMP_NTZ;
    v_run VARCHAR;
    v_status VARCHAR;
    v_error VARCHAR;
    v_floor TIMESTAMP_NTZ;
    v_grace NUMBER;
BEGIN
    v_batch := batch;
    v_loaded := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
    v_start := v_loaded;
    v_run := UUID_STRING();
    v_floor := (
        SELECT COALESCE(
            TRY_TO_TIMESTAMP_NTZ(MAX(value)),
            TO_TIMESTAMP_NTZ('2020-01-01')
        )
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
        WHERE key = 'dq_timestamp_floor'
    );
    v_grace := (
        SELECT COALESCE(TRY_TO_NUMBER(MAX(value)), 86400)
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
        WHERE key = 'dq_future_grace_seconds'
    );
    IF (v_floor IS NULL) THEN
        v_floor := TO_TIMESTAMP_NTZ('2020-01-01');
    END IF;
    IF (v_grace IS NULL) THEN
        v_grace := 86400;
    END IF;

    -- Task calls pass a null batch. Read each stream with DML in this
    -- transaction so the offset advances when it commits. Only batch ids
    -- that do not already have a gold success are loaded. Gold rows for
    -- every other batch stay where they are.
    IF (v_batch IS NULL) THEN
        BEGIN TRANSACTION;
        DELETE FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE;
        INSERT INTO MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE (batch_id)
        SELECT DISTINCT s.batch_id
        FROM (
            SELECT _batch_id AS batch_id, METADATA$ACTION AS action
            FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GROUPS
            UNION ALL
            SELECT _batch_id AS batch_id, METADATA$ACTION AS action
            FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_SUPPORT_TICKETS
            UNION ALL
            SELECT _batch_id AS batch_id, METADATA$ACTION AS action
            FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEPLOYS
            UNION ALL
            SELECT _batch_id AS batch_id, METADATA$ACTION AS action
            FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEMO_META
            UNION ALL
            SELECT _batch_id AS batch_id, METADATA$ACTION AS action
            FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GOLDEN_TICKETS
            UNION ALL
            SELECT _batch_id AS batch_id, METADATA$ACTION AS action
            FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_EXTERNAL_EVENTS
            UNION ALL
            SELECT _batch_id AS batch_id, METADATA$ACTION AS action
            FROM MCP_DB.SIGNAL_SANDBOX.STREAM_SILVER_GROUPS
        ) s
        WHERE s.action = 'INSERT'
          AND s.batch_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS r
              WHERE EQUAL_NULL(r.batch_id, s.batch_id)
                AND r.layer = 'gold'
                AND r.status IN ('success', 'failed')
          );
        IF (NOT EXISTS (SELECT 1 FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)) THEN
            -- Stream offset advanced and no new batch was loaded. A no-op
            -- writes no PIPELINE_RUNS row.
            COMMIT;
            RETURN 'noop';
        END IF;
    ELSE
        DELETE FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE;
        INSERT INTO MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE (batch_id) VALUES (:v_batch);
    END IF;

    -- Classifier rows can arrive after the first gold success. Refresh them
    -- on every call, including the path that then returns noop. Each topic
    -- keeps the bronze batch of its ticket. group_id dedup below is only for
    -- a real repeated key; a truncated client read is not a duplicate.
    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_TICKET_TOPICS t
    USING (
        SELECT
            tt.ticket_id,
            tt.case_id,
            tt.classified_at,
            tt.topic,
            tt.sentiment,
            tt.severity,
            tt.product_area,
            tt.evidence_snippet,
            tt.confidence,
            tt.model,
            tt.provider,
            tt.input_hash,
            COALESCE(bt._batch_id, :v_batch) AS _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS tt
        LEFT JOIN (
            SELECT ticket_id, MAX(_batch_id) AS _batch_id
            FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS
            GROUP BY ticket_id
        ) bt
            ON bt.ticket_id = tt.ticket_id
        WHERE bt._batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
    ) s
    ON EQUAL_NULL(t.ticket_id, s.ticket_id) AND EQUAL_NULL(t.case_id, s.case_id)
    WHEN MATCHED THEN UPDATE SET
        classified_at = s.classified_at,
        topic = s.topic,
        sentiment = s.sentiment,
        severity = s.severity,
        product_area = s.product_area,
        evidence_snippet = s.evidence_snippet,
        confidence = s.confidence,
        model = s.model,
        provider = s.provider,
        input_hash = s.input_hash,
        _loaded_at = :v_loaded,
        _batch_id = s._batch_id
    WHEN NOT MATCHED THEN INSERT (
        ticket_id, case_id, classified_at, topic, sentiment, severity, product_area,
        evidence_snippet, confidence, model, provider, input_hash, _loaded_at, _batch_id
    ) VALUES (
        s.ticket_id, s.case_id, s.classified_at, s.topic, s.sentiment, s.severity, s.product_area,
        s.evidence_snippet, s.confidence, s.model, s.provider, s.input_hash, :v_loaded, s._batch_id
    );

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_TICKET_TOPICS
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_TICKET_TOPICS (
        topic, sentiment, severity, product_area, ticket_count, avg_confidence, _batch_id, _loaded_at
    )
    SELECT
        topic,
        sentiment,
        severity,
        product_area,
        COUNT(*),
        AVG(confidence),
        _batch_id,
        :v_loaded
    FROM MCP_DB.SIGNAL_SANDBOX.SILVER_TICKET_TOPICS
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
    GROUP BY topic, sentiment, severity, product_area, _batch_id;

    IF (v_batch IS NOT NULL AND EXISTS (
        SELECT 1
        FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
        WHERE batch_id = :v_batch AND layer = 'gold' AND status = 'success'
    )) THEN
        DELETE FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE;
        RETURN 'noop';
    END IF;

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
      AND removed_by IS NULL;

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS (
        group_id, hour, platform, country, channel, payment_method, sku, app_version,
        n_sessions, n_convert, n_attempt, _loaded_at, _batch_id, row_key, dq_status, dq_reason
    )
    SELECT
        group_id, hour, platform, country, channel, payment_method, sku, app_version,
        n_sessions, n_convert, n_attempt, :v_loaded, _batch_id, row_key,
        CASE WHEN reason IS NULL THEN 'ok' ELSE 'suspect' END,
        reason
    FROM (
        SELECT
            group_id, hour, platform, country, channel, payment_method, sku, app_version,
            n_sessions, n_convert, n_attempt, _batch_id,
            SHA2(
                'bronze_groups' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) ||
                IFF(group_id IS NULL, '0', '1' || group_id) || CHR(31) ||
                IFF(hour IS NULL, '0', '1' || TO_VARCHAR(hour, 'YYYY-MM-DD HH24:MI:SS')) || CHR(31) ||
                IFF(platform IS NULL, '0', '1' || platform) || CHR(31) ||
                IFF(country IS NULL, '0', '1' || country) || CHR(31) ||
                IFF(channel IS NULL, '0', '1' || channel) || CHR(31) ||
                IFF(payment_method IS NULL, '0', '1' || payment_method) || CHR(31) ||
                IFF(sku IS NULL, '0', '1' || sku) || CHR(31) ||
                IFF(app_version IS NULL, '0', '1' || app_version) || CHR(31) ||
                IFF(n_sessions IS NULL, '0', '1' || TO_VARCHAR(n_sessions)) || CHR(31) ||
                IFF(n_convert IS NULL, '0', '1' || TO_VARCHAR(n_convert)) || CHR(31) ||
                IFF(n_attempt IS NULL, '0', '1' || TO_VARCHAR(n_attempt)),
                256
            ) AS row_key,
            CASE
                WHEN group_id IS NULL THEN 'null group_id'
                WHEN hour IS NULL THEN 'null hour'
                WHEN n_sessions IS NULL OR n_convert IS NULL OR n_attempt IS NULL THEN 'null metric'
                WHEN n_sessions < 0 OR n_convert < 0 OR n_attempt < 0 THEN 'negative_count'
                WHEN n_convert > n_sessions THEN 'conversions_gt_sessions'
                WHEN n_convert > n_attempt THEN 'conversions_gt_attempts'
                WHEN platform IS NULL OR country IS NULL THEN 'null segment'
                WHEN YEAR(hour) = 1970 THEN 'epoch_timestamp'
                WHEN hour < :v_floor THEN 'timestamp_before_floor'
                WHEN hour > DATEADD('second', :v_grace, :v_loaded) THEN 'future_timestamp'
                WHEN rn > 1 THEN 'duplicate group_id'
                ELSE NULL
            END AS reason
        FROM (
            SELECT
                group_id, hour, platform, country, channel, payment_method, sku, app_version,
                n_sessions, n_convert, n_attempt, _batch_id,
                ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY _ingested_at) AS rn
            FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS src
            WHERE src._batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
        )
    ) flagged
    WHERE NOT EXISTS (
            SELECT 1
            FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS removed
            WHERE EQUAL_NULL(removed._batch_id, flagged._batch_id)
              AND removed.source_table = 'bronze_groups'
              AND removed.removed_by IS NOT NULL
              AND (
                  EQUAL_NULL(removed.row_key, flagged.row_key)
                  OR EQUAL_NULL(removed.row_key, flagged.group_id)
              )
       );

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS (
        reject_id, source_table, row_json, reason, _loaded_at, _batch_id
    )
    SELECT
        'bronze_support_tickets:' || :v_run || ':' || COALESCE(ticket_id, 'row'),
        'bronze_support_tickets',
        '{"ticket_id":"' || COALESCE(ticket_id, '') || '","text_redacted":"[redacted]"}',
        'pii',
        :v_loaded,
        _batch_id
    FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
      AND (
          REGEXP_LIKE(text_redacted, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z][A-Za-z]+')
          OR REGEXP_LIKE(text_redacted, '[+][0-9]{1,3}[ ][0-9]{5}[ ][0-9]{5}')
          OR REGEXP_LIKE(text_redacted, '[0-9]{3}-[0-9]{2}-[0-9]{4}')
          OR REGEXP_LIKE(text_redacted, '[0-9]{4}[ ][0-9]{4}[ ][0-9]{4}[ ][0-9]{4}')
      );

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS t
    USING (
        SELECT ticket_id, ts, platform, country, channel, payment_method, sku, app_version,
               product_area, text_redacted, _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS src
        WHERE src._batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
          AND src.ticket_id IS NOT NULL
          AND text_redacted IS NOT NULL
          AND NOT REGEXP_LIKE(text_redacted, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z][A-Za-z]+')
          AND NOT REGEXP_LIKE(text_redacted, '[+][0-9]{1,3}[ ][0-9]{5}[ ][0-9]{5}')
          AND NOT REGEXP_LIKE(text_redacted, '[0-9]{3}-[0-9]{2}-[0-9]{4}')
          AND NOT REGEXP_LIKE(text_redacted, '[0-9]{4}[ ][0-9]{4}[ ][0-9]{4}[ ][0-9]{4}')
          AND NOT EXISTS (
              SELECT 1
              FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS removed
              WHERE EQUAL_NULL(removed._batch_id, src._batch_id)
                AND removed.source_table = 'bronze_support_tickets'
                AND removed.removed_by IS NOT NULL
                AND (
                    EQUAL_NULL(removed.row_key, src.ticket_id)
                    OR EQUAL_NULL(
                        removed.row_key,
                        SHA2(
                            'bronze_support_tickets' || CHR(31) || COALESCE(src._batch_id, '') || CHR(31) ||
                            IFF(src.ticket_id IS NULL, '0', '1' || src.ticket_id) || CHR(31) ||
                            IFF(src.ts IS NULL, '0', '1' || TO_VARCHAR(src.ts, 'YYYY-MM-DD HH24:MI:SS')) || CHR(31) ||
                            IFF(src.text_redacted IS NULL, '0', '1' || src.text_redacted),
                            256
                        )
                    )
                )
          )
    ) s
    ON EQUAL_NULL(t.ticket_id, s.ticket_id)
    WHEN NOT MATCHED THEN INSERT (
        ticket_id, ts, platform, country, channel, payment_method, sku, app_version,
        product_area, text_redacted, _loaded_at, _batch_id
    ) VALUES (
        s.ticket_id, s.ts, s.platform, s.country, s.channel, s.payment_method, s.sku, s.app_version,
        s.product_area, s.text_redacted, :v_loaded, s._batch_id
    );

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS t
    USING (
        SELECT deploy_id, deployed_at, platform, app_version, note, _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEPLOYS src
        WHERE src._batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
          AND src.deploy_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS removed
              WHERE EQUAL_NULL(removed._batch_id, src._batch_id)
                AND removed.source_table = 'bronze_deploys'
                AND removed.removed_by IS NOT NULL
                AND (
                    EQUAL_NULL(removed.row_key, deploy_id)
                    OR EQUAL_NULL(
                        removed.row_key,
                        SHA2('bronze_deploys' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(deploy_id IS NULL, '0', '1' || deploy_id), 256)
                    )
                )
          )
    ) s
    ON EQUAL_NULL(t.deploy_id, s.deploy_id)
    WHEN NOT MATCHED THEN INSERT (
        deploy_id, deployed_at, platform, app_version, note, _loaded_at, _batch_id
    ) VALUES (
        s.deploy_id, s.deployed_at, s.platform, s.app_version, s.note, :v_loaded, s._batch_id
    );

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META t
    USING (
        SELECT key, value, _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEMO_META src
        WHERE src._batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
          AND src.key IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS removed
              WHERE EQUAL_NULL(removed._batch_id, src._batch_id)
                AND removed.source_table = 'bronze_demo_meta'
                AND removed.removed_by IS NOT NULL
                AND (
                    EQUAL_NULL(removed.row_key, key)
                    OR EQUAL_NULL(
                        removed.row_key,
                        SHA2('bronze_demo_meta' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(key IS NULL, '0', '1' || key) || CHR(31) || IFF(value IS NULL, '0', '1' || value), 256)
                    )
                )
          )
    ) s
    ON EQUAL_NULL(t.key, s.key)
    WHEN NOT MATCHED THEN INSERT (key, value, _loaded_at, _batch_id)
    VALUES (s.key, s.value, :v_loaded, s._batch_id);

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS t
    USING (
        SELECT example_id, text_redacted, topic, sentiment, severity, product_area, _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GOLDEN_TICKETS src
        WHERE src._batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
          AND src.example_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS removed
              WHERE EQUAL_NULL(removed._batch_id, src._batch_id)
                AND removed.source_table = 'bronze_golden_tickets'
                AND removed.removed_by IS NOT NULL
                AND (
                    EQUAL_NULL(removed.row_key, example_id)
                    OR EQUAL_NULL(
                        removed.row_key,
                        SHA2('bronze_golden_tickets' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(example_id IS NULL, '0', '1' || example_id), 256)
                    )
                )
          )
    ) s
    ON EQUAL_NULL(t.example_id, s.example_id)
    WHEN NOT MATCHED THEN INSERT (
        example_id, text_redacted, topic, sentiment, severity, product_area, _loaded_at, _batch_id
    ) VALUES (
        s.example_id, s.text_redacted, s.topic, s.sentiment, s.severity, s.product_area, :v_loaded, s._batch_id
    );

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS (
        reject_id, source_table, row_json, reason, _loaded_at, _batch_id
    )
    SELECT
        'bronze_external_events:' || :v_run || ':' || COALESCE(event_id, 'row'),
        'bronze_external_events',
        '{"event_id":"' || COALESCE(event_id, '') || '"}',
        'null event_id',
        :v_loaded,
        _batch_id
    FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
      AND event_id IS NULL;

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS t
    USING (
        SELECT event_id, case_id, title, summary, source_url, source_type, relevance,
               observed_at, metric, provider, _batch_id
        FROM (
            SELECT
                event_id, case_id, title, summary, source_url, source_type, relevance,
                observed_at, metric, provider, _batch_id,
                ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at) AS rn
            FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS src
            WHERE src._batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
              AND src.event_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS removed
                  WHERE EQUAL_NULL(removed._batch_id, src._batch_id)
                    AND removed.source_table = 'bronze_external_events'
                    AND removed.removed_by IS NOT NULL
                    AND (
                        EQUAL_NULL(removed.row_key, src.event_id)
                        OR EQUAL_NULL(
                            removed.row_key,
                            SHA2('bronze_external_events' || CHR(31) || COALESCE(src._batch_id, '') || CHR(31) || IFF(src.event_id IS NULL, '0', '1' || src.event_id), 256)
                        )
                    )
              )
        )
        WHERE rn = 1
    ) s
    ON EQUAL_NULL(t.event_id, s.event_id)
    WHEN NOT MATCHED THEN INSERT (
        event_id, case_id, title, summary, source_url, source_type, relevance,
        observed_at, metric, provider, _loaded_at, _batch_id
    ) VALUES (
        s.event_id, s.case_id, s.title, s.summary, s.source_url, s.source_type, s.relevance,
        s.observed_at, s.metric, s.provider, :v_loaded, s._batch_id
    );

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS (
        reject_id, source_table, row_json, reason, _loaded_at, _batch_id
    )
    SELECT
        'bronze_external_events:' || :v_run || ':dup:' || event_id,
        'bronze_external_events',
        '{"event_id":"' || event_id || '"}',
        'duplicate event_id',
        :v_loaded,
        _batch_id
    FROM (
        SELECT
            event_id,
            _batch_id,
            ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at) AS rn
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS
        WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)
          AND event_id IS NOT NULL
    )
    WHERE rn > 1;

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS (
        run_id, batch_id, layer, started_at, finished_at, status, rows_in, rows_out, rows_rejected, error
    )
    SELECT
        'silver-' || :v_run || '-' || counted.batch_id,
        counted.batch_id,
        'silver',
        :v_start,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        CASE
            WHEN counted.rows_in = counted.rows_out + counted.rows_rejected THEN 'success'
            ELSE 'failed'
        END,
        counted.rows_in,
        counted.rows_out,
        counted.rows_rejected,
        CASE
            WHEN counted.rows_in = counted.rows_out + counted.rows_rejected THEN NULL
            ELSE 'silver rows_in ' || TO_VARCHAR(counted.rows_in) || ' != rows_out ' || TO_VARCHAR(counted.rows_out)
                || ' + rows_rejected ' || TO_VARCHAR(counted.rows_rejected)
        END
    FROM (
        SELECT
            b.batch_id AS batch_id,
            (
                (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEPLOYS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEMO_META t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GOLDEN_TICKETS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS t WHERE t._batch_id = b.batch_id)
            ) AS rows_in,
            (
                (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS t WHERE t._batch_id = b.batch_id)
            ) AS rows_out,
            (
                SELECT COUNT(*)
                FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS t
                WHERE EQUAL_NULL(t._batch_id, b.batch_id)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS g
                      WHERE EQUAL_NULL(g._batch_id, t._batch_id)
                        AND t.source_table = 'bronze_groups'
                        AND t.row_key IS NOT NULL
                        AND EQUAL_NULL(g.row_key, t.row_key)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS s
                      WHERE EQUAL_NULL(s._batch_id, t._batch_id)
                        AND t.source_table = 'bronze_support_tickets'
                        AND t.row_key IS NOT NULL
                        AND EQUAL_NULL(s.row_key, t.row_key)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS d
                      WHERE EQUAL_NULL(d._batch_id, t._batch_id)
                        AND t.source_table = 'bronze_deploys'
                        AND t.row_key IS NOT NULL
                        AND EQUAL_NULL(d.row_key, t.row_key)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META m
                      WHERE EQUAL_NULL(m._batch_id, t._batch_id)
                        AND t.source_table = 'bronze_demo_meta'
                        AND t.row_key IS NOT NULL
                        AND EQUAL_NULL(m.row_key, t.row_key)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS gt
                      WHERE EQUAL_NULL(gt._batch_id, t._batch_id)
                        AND t.source_table = 'bronze_golden_tickets'
                        AND t.row_key IS NOT NULL
                        AND EQUAL_NULL(gt.row_key, t.row_key)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS e
                      WHERE EQUAL_NULL(e._batch_id, t._batch_id)
                        AND t.source_table = 'bronze_external_events'
                        AND t.row_key IS NOT NULL
                        AND EQUAL_NULL(e.row_key, t.row_key)
                  )
            ) AS rows_rejected
        FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE b
    ) counted;

    v_status := (
        SELECT CASE WHEN COUNT(*) > 0 THEN 'failed' ELSE 'success' END
        FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
        WHERE STARTSWITH(run_id, 'silver-' || :v_run || '-')
          AND status = 'failed'
    );
    v_error := (
        SELECT MIN(error)
        FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
        WHERE STARTSWITH(run_id, 'silver-' || :v_run || '-')
          AND status = 'failed'
    );

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE);
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE);
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION (
        hour, platform, country, payment_method, app_version,
        sessions, converted, conversion_rate, baseline_rate, seasonal_index, expected_rate,
        is_anomaly, dq_suspect_rows, _batch_id, _loaded_at
    )
    SELECT
        hour_ts, platform, country, payment_method, app_version,
        sessions, converted, conversion_rate, baseline_rate,
        CASE WHEN slice_mean IS NULL OR slice_mean = 0 OR baseline_rate IS NULL THEN NULL
             ELSE baseline_rate / slice_mean END,
        baseline_rate,
        CASE
            WHEN baseline_rate IS NOT NULL AND sessions >= 100 AND conversion_rate <= baseline_rate * 0.9 THEN TRUE
            ELSE FALSE
        END,
        dq_suspect_rows,
        batch_key,
        :v_loaded
    FROM (
        SELECT
            hour_ts, platform, country, payment_method, app_version,
            sessions, converted, conversion_rate, batch_key, dq_suspect_rows,
            AVG(conversion_rate) OVER (
                PARTITION BY platform, country, payment_method, app_version, DATE_PART('hour', hour_ts)
                ORDER BY hour_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS baseline_rate,
            AVG(conversion_rate) OVER (
                PARTITION BY platform, country, payment_method, app_version
            ) AS slice_mean
        FROM (
            SELECT
                hour AS hour_ts,
                platform,
                country,
                payment_method,
                app_version,
                SUM(n_sessions) AS sessions,
                SUM(n_convert) AS converted,
                SUM(n_convert)::DOUBLE / NULLIF(SUM(n_sessions), 0) AS conversion_rate,
                SUM(CASE WHEN COALESCE(dq_status, 'ok') = 'suspect' THEN 1 ELSE 0 END) AS dq_suspect_rows,
                MIN(_batch_id) AS batch_key
            FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
            GROUP BY hour, platform, country, payment_method, app_version
        )
    )
    WHERE batch_key IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK (
        hour, platform, country, payment_method, app_version,
        lost_conversions, aov_usd, session_weight, revenue_at_risk_usd, dq_suspect_rows, _batch_id, _loaded_at
    )
    SELECT
        hour,
        platform,
        country,
        payment_method,
        app_version,
        CASE
            WHEN baseline_rate IS NULL THEN 0
            ELSE GREATEST(0, (baseline_rate - conversion_rate) * sessions)
        END,
        COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'aov_usd'), 48),
        COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'session_weight'), 1),
        CASE
            WHEN baseline_rate IS NULL THEN 0
            ELSE GREATEST(0, (baseline_rate - conversion_rate) * sessions)
                * COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'aov_usd'), 48)
                * COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'session_weight'), 1)
        END,
        dq_suspect_rows,
        _batch_id,
        :v_loaded
    FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
    WHERE _batch_id IN (SELECT batch_id FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND (
        spend_date, spend_usd, cap_usd, headroom_usd, dq_suspect_rows, _batch_id, _loaded_at
    )
    SELECT
        spend_date,
        spend_usd,
        cap_usd,
        headroom_usd,
        dq_suspect_rows
            + CASE
                WHEN YEAR(spend_date) = 1970
                  OR spend_date < CAST(:v_floor AS DATE)
                  OR spend_date > DATEADD('second', :v_grace, :v_loaded)::DATE
                THEN 1
                ELSE 0
              END,
        batch_id,
        :v_loaded
    FROM (
        SELECT
            COALESCE(MAX(CAST(g.hour AS DATE)), CAST(:v_loaded AS DATE)) AS spend_date,
            COALESCE((SELECT SUM(estimated_usd) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND), 0) AS spend_usd,
            25 AS cap_usd,
            25 - COALESCE((SELECT SUM(estimated_usd) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND), 0) AS headroom_usd,
            COALESCE(SUM(g.dq_suspect_rows), 0) AS dq_suspect_rows,
            b.batch_id AS batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE b
        LEFT JOIN MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION g
            ON EQUAL_NULL(g._batch_id, b.batch_id)
        GROUP BY b.batch_id
    ) spent;

    IF (v_status <> 'success') THEN
        v_error := COALESCE(v_error, 'silver did not reconcile');
    END IF;
    INSERT INTO MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS (
        run_id, batch_id, layer, started_at, finished_at, status, rows_in, rows_out, rows_rejected, error
    )
    SELECT
        'gold-' || :v_run || '-' || counted.batch_id,
        counted.batch_id,
        'gold',
        :v_start,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        :v_status,
        counted.rows_out,
        counted.rows_out,
        0,
        :v_error
    FROM (
        SELECT
            b.batch_id AS batch_id,
            (
                (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND t WHERE t._batch_id = b.batch_id)
                + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_TICKET_TOPICS t WHERE t._batch_id = b.batch_id)
            ) AS rows_out
        FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE b
    ) counted;

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE;
    IF (v_batch IS NULL) THEN
        COMMIT;
        BEGIN TRANSACTION;
        INSERT INTO MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE (batch_id)
        SELECT DISTINCT _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.STREAM_SILVER_GROUPS
        WHERE METADATA$ACTION = 'INSERT' OR METADATA$ACTION = 'DELETE';
        DELETE FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE;
        COMMIT;
    END IF;
    IF (v_status <> 'success') THEN
        RETURN v_error;
    END IF;
    RETURN 'ok';
END;
$$;

-- Reprocess one batch after a gold row was marked success. This is a separate
-- procedure so LOAD_STAGING stays LOAD_STAGING(VARCHAR) and replaces the
-- existing procedure instead of overloading it. The DELETE touches only the
-- success marker in PIPELINE_RUNS. It does not drop or truncate data tables.
CREATE OR REPLACE PROCEDURE MCP_DB.SIGNAL_SANDBOX.RERUN_STAGING(batch VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_batch VARCHAR;
BEGIN
    v_batch := batch;
    IF (v_batch IS NULL) THEN
        RETURN 'batch is required';
    END IF;
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
    WHERE batch_id = :v_batch AND layer = 'gold' AND status = 'success';
    CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(:v_batch);
    RETURN 'ok';
END;
$$;

-- One signature. Engineering asks for a batch, and an optional note is the
-- second argument. There is no overload and no default parameter.
-- Does not call LOAD_STAGING. Reloading bronze put null-key rows back.
-- Gold is recomputed from the silver rows that remain.
CREATE OR REPLACE PROCEDURE MCP_DB.SIGNAL_SANDBOX.REMOVE_SUSPECT(batch VARCHAR, reason VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_batch VARCHAR;
    v_reason VARCHAR;
    v_loaded TIMESTAMP_NTZ;
    v_removed NUMBER;
    v_floor TIMESTAMP_NTZ;
    v_grace NUMBER;
BEGIN
    v_batch := batch;
    v_reason := COALESCE(reason, 'removed on request');
    v_loaded := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
    v_floor := (
        SELECT COALESCE(TRY_TO_TIMESTAMP_NTZ(MAX(value)), TO_TIMESTAMP_NTZ('2020-01-01'))
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
        WHERE key = 'dq_timestamp_floor'
    );
    v_grace := (
        SELECT COALESCE(TRY_TO_NUMBER(MAX(value)), 86400)
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
        WHERE key = 'dq_future_grace_seconds'
    );
    IF (v_floor IS NULL) THEN
        v_floor := TO_TIMESTAMP_NTZ('2020-01-01');
    END IF;
    IF (v_grace IS NULL) THEN
        v_grace := 86400;
    END IF;
    IF (v_batch IS NULL) THEN
        RETURN 'batch is required';
    END IF;

    UPDATE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    SET dq_status = 'suspect',
        dq_reason = CASE
            WHEN YEAR(hour) = 1970 THEN 'epoch_timestamp'
            WHEN hour < :v_floor THEN 'timestamp_before_floor'
            WHEN hour > DATEADD('second', :v_grace, :v_loaded) THEN 'future_timestamp'
            ELSE dq_reason
        END
    WHERE _batch_id = :v_batch
      AND hour IS NOT NULL
      AND COALESCE(dq_status, 'ok') <> 'suspect'
      AND (
          YEAR(hour) = 1970
          OR hour < :v_floor
          OR hour > DATEADD('second', :v_grace, :v_loaded)
      );

    UPDATE MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND
    SET dq_status = 'suspect',
        dq_reason = CASE
            WHEN YEAR(logged_at) = 1970 THEN 'epoch_timestamp'
            WHEN logged_at < :v_floor THEN 'timestamp_before_floor'
            WHEN logged_at > DATEADD('second', :v_grace, :v_loaded) THEN 'future_timestamp'
            ELSE dq_reason
        END
    WHERE _batch_id = :v_batch
      AND logged_at IS NOT NULL
      AND COALESCE(dq_status, 'ok') <> 'suspect'
      AND (
          YEAR(logged_at) = 1970
          OR logged_at < :v_floor
          OR logged_at > DATEADD('second', :v_grace, :v_loaded)
      );

    UPDATE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    SET row_key = SHA2(
        'bronze_groups' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) ||
        IFF(group_id IS NULL, '0', '1' || group_id) || CHR(31) ||
        IFF(hour IS NULL, '0', '1' || TO_VARCHAR(hour, 'YYYY-MM-DD HH24:MI:SS')) || CHR(31) ||
        IFF(platform IS NULL, '0', '1' || platform) || CHR(31) ||
        IFF(country IS NULL, '0', '1' || country) || CHR(31) ||
        IFF(channel IS NULL, '0', '1' || channel) || CHR(31) ||
        IFF(payment_method IS NULL, '0', '1' || payment_method) || CHR(31) ||
        IFF(sku IS NULL, '0', '1' || sku) || CHR(31) ||
        IFF(app_version IS NULL, '0', '1' || app_version) || CHR(31) ||
        IFF(n_sessions IS NULL, '0', '1' || TO_VARCHAR(n_sessions)) || CHR(31) ||
        IFF(n_convert IS NULL, '0', '1' || TO_VARCHAR(n_convert)) || CHR(31) ||
        IFF(n_attempt IS NULL, '0', '1' || TO_VARCHAR(n_attempt)),
        256
    )
    WHERE _batch_id = :v_batch
      AND row_key IS NULL;

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS (
        reject_id, source_table, row_json, reason, _loaded_at, _batch_id, row_key, removed_by, removed_at
    )
    SELECT
        'remove:' || :v_batch || ':' || source_table || ':' || row_key,
        source_table,
        row_json,
        COALESCE(dq_reason, 'suspect') || '; ' || :v_reason,
        :v_loaded,
        :v_batch,
        row_key,
        'engineering',
        :v_loaded
    FROM (
        SELECT 'bronze_groups' AS source_table, row_key,
               '{"group_id":"' || COALESCE(group_id, '') || '"}' AS row_json, dq_reason
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect' AND row_key IS NOT NULL
        UNION ALL
        SELECT 'bronze_support_tickets',
               COALESCE(row_key, SHA2('bronze_support_tickets' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(ticket_id IS NULL, '0', '1' || ticket_id) || CHR(31) || IFF(ts IS NULL, '0', '1' || TO_VARCHAR(ts, 'YYYY-MM-DD HH24:MI:SS')) || CHR(31) || IFF(text_redacted IS NULL, '0', '1' || text_redacted), 256)),
               '{"ticket_id":"' || COALESCE(ticket_id, '') || '"}', dq_reason
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
        UNION ALL
        SELECT 'bronze_deploys',
               COALESCE(row_key, SHA2('bronze_deploys' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(deploy_id IS NULL, '0', '1' || deploy_id), 256)),
               '{"deploy_id":"' || COALESCE(deploy_id, '') || '"}', dq_reason
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
        UNION ALL
        SELECT 'bronze_demo_meta',
               COALESCE(row_key, SHA2('bronze_demo_meta' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(key IS NULL, '0', '1' || key) || CHR(31) || IFF(value IS NULL, '0', '1' || value), 256)),
               '{"key":"' || COALESCE(key, '') || '"}', dq_reason
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
        UNION ALL
        SELECT 'bronze_golden_tickets',
               COALESCE(row_key, SHA2('bronze_golden_tickets' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(example_id IS NULL, '0', '1' || example_id), 256)),
               '{"example_id":"' || COALESCE(example_id, '') || '"}', dq_reason
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
        UNION ALL
        SELECT 'bronze_external_events',
               COALESCE(row_key, SHA2('bronze_external_events' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(event_id IS NULL, '0', '1' || event_id), 256)),
               '{"event_id":"' || COALESCE(event_id, '') || '"}', dq_reason
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
        UNION ALL
        SELECT 'silver_spend',
               COALESCE(row_key, SHA2('silver_spend' || CHR(31) || COALESCE(_batch_id, '') || CHR(31) || IFF(logged_at IS NULL, '0', '1' || TO_VARCHAR(logged_at, 'YYYY-MM-DD HH24:MI:SS')) || CHR(31) || IFF(case_id IS NULL, '0', '1' || case_id) || CHR(31) || IFF(kind IS NULL, '0', '1' || kind), 256)),
               '{"case_id":"' || COALESCE(case_id, '') || '"}', dq_reason
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    ) src
    WHERE NOT EXISTS (
        SELECT 1
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS removed
        WHERE EQUAL_NULL(removed._batch_id, :v_batch)
          AND EQUAL_NULL(removed.source_table, src.source_table)
          AND removed.removed_by IS NOT NULL
          AND EQUAL_NULL(removed.row_key, src.row_key)
    );

    v_removed := (
        SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    ) + (
        SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    ) + (
        SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    ) + (
        SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    ) + (
        SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    ) + (
        SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    ) + (
        SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND
        WHERE _batch_id = :v_batch AND dq_status = 'suspect'
    );

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
    WHERE _batch_id = :v_batch AND dq_status = 'suspect';
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS
    WHERE _batch_id = :v_batch AND dq_status = 'suspect';
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS
    WHERE _batch_id = :v_batch AND dq_status = 'suspect';
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META
    WHERE _batch_id = :v_batch AND dq_status = 'suspect';
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS
    WHERE _batch_id = :v_batch AND dq_status = 'suspect';
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS
    WHERE _batch_id = :v_batch AND dq_status = 'suspect';
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND
    WHERE _batch_id = :v_batch AND dq_status = 'suspect';

    IF (v_removed = 0) THEN
        RETURN 'ok';
    END IF;

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
    WHERE _batch_id = :v_batch;
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK
    WHERE _batch_id = :v_batch;
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND
    WHERE _batch_id = :v_batch;

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION (
        hour, platform, country, payment_method, app_version,
        sessions, converted, conversion_rate, baseline_rate, seasonal_index, expected_rate,
        is_anomaly, dq_suspect_rows, _batch_id, _loaded_at
    )
    SELECT
        hour_ts, platform, country, payment_method, app_version,
        sessions, converted, conversion_rate, baseline_rate,
        CASE WHEN slice_mean IS NULL OR slice_mean = 0 OR baseline_rate IS NULL THEN NULL
             ELSE baseline_rate / slice_mean END,
        baseline_rate,
        CASE
            WHEN baseline_rate IS NOT NULL AND sessions >= 100 AND conversion_rate <= baseline_rate * 0.9 THEN TRUE
            ELSE FALSE
        END,
        dq_suspect_rows,
        :v_batch,
        :v_loaded
    FROM (
        SELECT
            hour_ts, platform, country, payment_method, app_version,
            sessions, converted, conversion_rate, dq_suspect_rows,
            AVG(conversion_rate) OVER (
                PARTITION BY platform, country, payment_method, app_version, DATE_PART('hour', hour_ts)
                ORDER BY hour_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS baseline_rate,
            AVG(conversion_rate) OVER (
                PARTITION BY platform, country, payment_method, app_version
            ) AS slice_mean
        FROM (
            SELECT
                hour AS hour_ts,
                platform,
                country,
                payment_method,
                app_version,
                SUM(n_sessions) AS sessions,
                SUM(n_convert) AS converted,
                SUM(n_convert)::DOUBLE / NULLIF(SUM(n_sessions), 0) AS conversion_rate,
                SUM(CASE WHEN COALESCE(dq_status, 'ok') = 'suspect' THEN 1 ELSE 0 END) AS dq_suspect_rows
            FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
            GROUP BY hour, platform, country, payment_method, app_version
        )
    ) scored
    WHERE EXISTS (
        SELECT 1
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS src
        WHERE src._batch_id = :v_batch
          AND EQUAL_NULL(src.hour, scored.hour_ts)
          AND EQUAL_NULL(src.platform, scored.platform)
          AND EQUAL_NULL(src.country, scored.country)
          AND EQUAL_NULL(src.payment_method, scored.payment_method)
          AND EQUAL_NULL(src.app_version, scored.app_version)
    );

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK (
        hour, platform, country, payment_method, app_version,
        lost_conversions, aov_usd, session_weight, revenue_at_risk_usd, dq_suspect_rows, _batch_id, _loaded_at
    )
    SELECT
        hour,
        platform,
        country,
        payment_method,
        app_version,
        CASE
            WHEN baseline_rate IS NULL THEN 0
            ELSE GREATEST(0, (baseline_rate - conversion_rate) * sessions)
        END,
        COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'aov_usd'), 48),
        COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'session_weight'), 1),
        CASE
            WHEN baseline_rate IS NULL THEN 0
            ELSE GREATEST(0, (baseline_rate - conversion_rate) * sessions)
                * COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'aov_usd'), 48)
                * COALESCE((SELECT MAX(value::DOUBLE) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE key = 'session_weight'), 1)
        END,
        dq_suspect_rows,
        _batch_id,
        :v_loaded
    FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
    WHERE _batch_id = :v_batch;

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND (
        spend_date, spend_usd, cap_usd, headroom_usd, dq_suspect_rows, _batch_id, _loaded_at
    )
    SELECT
        spend_date,
        COALESCE((SELECT SUM(estimated_usd) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND), 0),
        25,
        25 - COALESCE((SELECT SUM(estimated_usd) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND), 0),
        dq_suspect_rows
            + CASE
                WHEN YEAR(spend_date) = 1970
                  OR spend_date < CAST(:v_floor AS DATE)
                  OR spend_date > DATEADD('second', :v_grace, :v_loaded)::DATE
                THEN 1
                ELSE 0
              END,
        :v_batch,
        :v_loaded
    FROM (
        SELECT
            COALESCE(MAX(CAST(hour AS DATE)), CAST(:v_loaded AS DATE)) AS spend_date,
            COALESCE(SUM(dq_suspect_rows), 0) AS dq_suspect_rows
        FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
        WHERE _batch_id = :v_batch
    ) spent;

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS (
        run_id, batch_id, layer, started_at, finished_at, status, rows_in, rows_out, rows_rejected, error
    )
    SELECT
        'remove-' || UUID_STRING(),
        :v_batch,
        'remove_suspect',
        :v_loaded,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        'success',
        :v_removed,
        0,
        :v_removed,
        NULL;
    RETURN 'ok';
END;
$$;

-- @section task

-- Triggered tasks. SCHEDULE is omitted on purpose. The WHEN clause is the
-- trigger. TARGET_COMPLETION_INTERVAL is the serverless completion target
-- (required when WAREHOUSE is omitted). It is not a poll interval.
-- OVERLAP_POLICY = NO_OVERLAP keeps a single run at a time
-- (ALLOW_OVERLAPPING_EXECUTION = FALSE). No account parameter is set.

CREATE OR REPLACE TASK MCP_DB.SIGNAL_SANDBOX.TASK_BRONZE_TO_SILVER
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    OVERLAP_POLICY = NO_OVERLAP
    TARGET_COMPLETION_INTERVAL = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GROUPS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_SUPPORT_TICKETS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEPLOYS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEMO_META')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GOLDEN_TICKETS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_EXTERNAL_EVENTS')
AS
    CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL);

CREATE OR REPLACE TASK MCP_DB.SIGNAL_SANDBOX.TASK_SILVER_TO_GOLD
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    OVERLAP_POLICY = NO_OVERLAP
    TARGET_COMPLETION_INTERVAL = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_SILVER_GROUPS')
AS
    CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL);

-- @section resume

ALTER TASK MCP_DB.SIGNAL_SANDBOX.TASK_SILVER_TO_GOLD RESUME;

ALTER TASK MCP_DB.SIGNAL_SANDBOX.TASK_BRONZE_TO_SILVER RESUME;
