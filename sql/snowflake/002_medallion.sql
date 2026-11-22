-- Medallion objects inside MCP_DB.SIGNAL_SANDBOX only.
-- Bronze is append-only. Silver and gold rows carry _loaded_at and _batch_id.
--
-- ALTER TASK ... RESUME is the only ALTER in this file. Tasks are created
-- suspended, and RESUME is what lets them see streams. There is no DROP,
-- TRUNCATE, GRANT, or ALTER TABLE. Each task body is one statement:
-- CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL). LOAD_STAGING keeps the
-- single argument (batch VARCHAR). A second argument would overload the
-- procedure already in the sandbox instead of replacing it. Reprocess a
-- batch with RERUN_STAGING(batch VARCHAR). A compilation error is not a
-- missing privilege. If the role lacks CREATE PROCEDURE, ingest fails closed
-- instead of reading bronze into the client.
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
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS g,
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
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS g,
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
FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS g,
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

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.GOLD_HEALTH AS
SELECT 'bronze' AS layer,
       (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS) AS row_count,
       (SELECT MAX(_ingested_at) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS) AS fresh_at,
       (
           SELECT status FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
           WHERE layer = 'bronze'
           ORDER BY started_at DESC
           LIMIT 1
       ) AS latest_status
UNION ALL
SELECT 'silver',
       (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS),
       (SELECT MAX(_loaded_at) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS),
       (
           SELECT status FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
           WHERE layer = 'silver'
           ORDER BY started_at DESC
           LIMIT 1
       )
UNION ALL
SELECT 'gold',
       (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION),
       (SELECT MAX(_loaded_at) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION),
       (
           SELECT status FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
           WHERE layer = 'gold'
           ORDER BY started_at DESC
           LIMIT 1
       );

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

-- @section stream

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
    v_in NUMBER;
    v_out NUMBER;
    v_rejected NUMBER;
    v_status VARCHAR;
    v_error VARCHAR;
    v_gold_out NUMBER;
BEGIN
    v_batch := batch;
    v_loaded := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
    v_start := v_loaded;
    v_run := UUID_STRING();

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
            COALESCE(:v_batch, bt._batch_id, 'signals') AS _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS tt
        LEFT JOIN (
            SELECT ticket_id, MAX(_batch_id) AS _batch_id
            FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS
            GROUP BY ticket_id
        ) bt
            ON bt.ticket_id = tt.ticket_id
        WHERE :v_batch IS NULL OR bt._batch_id = :v_batch
    ) s
    ON t.ticket_id = s.ticket_id AND EQUAL_NULL(t.case_id, s.case_id)
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
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch);

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
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
    GROUP BY topic, sentiment, severity, product_area, _batch_id;

    IF (v_batch IS NOT NULL AND EXISTS (
        SELECT 1
        FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS
        WHERE batch_id = :v_batch AND layer = 'gold' AND status = 'success'
    )) THEN
        RETURN 'noop';
    END IF;

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS (
        reject_id, source_table, row_json, reason, _loaded_at, _batch_id
    )
    SELECT
        'bronze_groups:' || :v_run || ':' || group_id,
        'bronze_groups',
        '{"group_id":"' || COALESCE(group_id, '') || '"}',
        reason,
        :v_loaded,
        _batch_id
    FROM (
        SELECT
            group_id,
            _batch_id,
            CASE
                WHEN group_id IS NULL THEN 'null group_id'
                WHEN hour IS NULL THEN 'null hour'
                WHEN n_sessions IS NULL OR n_convert IS NULL OR n_attempt IS NULL THEN 'null metric'
                WHEN n_sessions < 0 OR n_convert < 0 OR n_attempt < 0 THEN 'negative_count'
                WHEN n_convert > n_sessions THEN 'conversions_gt_sessions'
                WHEN n_convert > n_attempt THEN 'conversions_gt_attempts'
                WHEN platform IS NULL OR country IS NULL THEN 'null segment'
                WHEN rn > 1 THEN 'duplicate group_id'
                ELSE NULL
            END AS reason
        FROM (
            SELECT
                group_id, hour, platform, country, n_sessions, n_convert, n_attempt, _batch_id,
                ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY _ingested_at) AS rn
            FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS
            WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
        )
    )
    WHERE reason IS NOT NULL;

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS t
    USING (
        SELECT group_id, hour, platform, country, channel, payment_method, sku, app_version,
               n_sessions, n_convert, n_attempt, _batch_id
        FROM (
            SELECT
                group_id, hour, platform, country, channel, payment_method, sku, app_version,
                n_sessions, n_convert, n_attempt, _batch_id,
                ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY _ingested_at) AS rn
            FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS
            WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
              AND group_id IS NOT NULL
              AND hour IS NOT NULL
              AND platform IS NOT NULL
              AND country IS NOT NULL
              AND n_sessions >= 0
              AND n_convert >= 0
              AND n_attempt >= 0
              AND n_attempt >= n_convert
              AND n_sessions >= n_convert
        )
        WHERE rn = 1
    ) s
    ON t.group_id = s.group_id
    WHEN NOT MATCHED THEN INSERT (
        group_id, hour, platform, country, channel, payment_method, sku, app_version,
        n_sessions, n_convert, n_attempt, _loaded_at, _batch_id
    ) VALUES (
        s.group_id, s.hour, s.platform, s.country, s.channel, s.payment_method, s.sku, s.app_version,
        s.n_sessions, s.n_convert, s.n_attempt, :v_loaded, s._batch_id
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
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
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
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS
        WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
          AND ticket_id IS NOT NULL
          AND text_redacted IS NOT NULL
          AND NOT REGEXP_LIKE(text_redacted, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z][A-Za-z]+')
          AND NOT REGEXP_LIKE(text_redacted, '[+][0-9]{1,3}[ ][0-9]{5}[ ][0-9]{5}')
          AND NOT REGEXP_LIKE(text_redacted, '[0-9]{3}-[0-9]{2}-[0-9]{4}')
          AND NOT REGEXP_LIKE(text_redacted, '[0-9]{4}[ ][0-9]{4}[ ][0-9]{4}[ ][0-9]{4}')
    ) s
    ON t.ticket_id = s.ticket_id
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
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEPLOYS
        WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
          AND deploy_id IS NOT NULL
    ) s
    ON t.deploy_id = s.deploy_id
    WHEN NOT MATCHED THEN INSERT (
        deploy_id, deployed_at, platform, app_version, note, _loaded_at, _batch_id
    ) VALUES (
        s.deploy_id, s.deployed_at, s.platform, s.app_version, s.note, :v_loaded, s._batch_id
    );

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META t
    USING (
        SELECT key, value, _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEMO_META
        WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
          AND key IS NOT NULL
    ) s
    ON t.key = s.key
    WHEN NOT MATCHED THEN INSERT (key, value, _loaded_at, _batch_id)
    VALUES (s.key, s.value, :v_loaded, s._batch_id);

    MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS t
    USING (
        SELECT example_id, text_redacted, topic, sentiment, severity, product_area, _batch_id
        FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GOLDEN_TICKETS
        WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
          AND example_id IS NOT NULL
    ) s
    ON t.example_id = s.example_id
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
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
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
            FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS
            WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
              AND event_id IS NOT NULL
        )
        WHERE rn = 1
    ) s
    ON t.event_id = s.event_id
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
        WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
          AND event_id IS NOT NULL
    )
    WHERE rn > 1;

    v_in :=
        (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GROUPS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_SUPPORT_TICKETS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEPLOYS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_DEMO_META WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_GOLDEN_TICKETS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS WHERE (:v_batch IS NULL OR _batch_id = :v_batch));
    v_out :=
        (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SUPPORT_TICKETS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEPLOYS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_DEMO_META WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GOLDEN_TICKETS WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS WHERE (:v_batch IS NULL OR _batch_id = :v_batch));
    v_rejected := (
        SELECT COUNT(*)
        FROM MCP_DB.SIGNAL_SANDBOX.SILVER_REJECTS
        WHERE (:v_batch IS NULL OR _batch_id = :v_batch)
    );
    v_status := 'success';
    v_error := NULL;
    IF (v_in <> v_out + v_rejected) THEN
        v_status := 'failed';
        v_error := 'silver rows_in ' || TO_VARCHAR(v_in) || ' != rows_out ' || TO_VARCHAR(v_out)
            || ' + rows_rejected ' || TO_VARCHAR(v_rejected);
    END IF;
    INSERT INTO MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS (
        run_id, batch_id, layer, started_at, finished_at, status, rows_in, rows_out, rows_rejected, error
    ) VALUES (
        'silver-' || :v_run,
        COALESCE(:v_batch, 'pending'),
        'silver',
        :v_start,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        :v_status,
        :v_in,
        :v_out,
        :v_rejected,
        :v_error
    );

    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch);
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch);
    DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION (
        hour, platform, country, payment_method, app_version,
        sessions, converted, conversion_rate, baseline_rate, seasonal_index, expected_rate,
        is_anomaly, _batch_id, _loaded_at
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
        batch_key,
        :v_loaded
    FROM (
        SELECT
            hour_ts, platform, country, payment_method, app_version,
            sessions, converted, conversion_rate, batch_key,
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
                MIN(_batch_id) AS batch_key
            FROM MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS
            GROUP BY hour, platform, country, payment_method, app_version
        )
    )
    WHERE (:v_batch IS NULL OR batch_key = :v_batch);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK (
        hour, platform, country, payment_method, app_version,
        lost_conversions, aov_usd, session_weight, revenue_at_risk_usd, _batch_id, _loaded_at
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
        _batch_id,
        :v_loaded
    FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch);

    INSERT INTO MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND (
        spend_date, spend_usd, cap_usd, headroom_usd, _batch_id, _loaded_at
    )
    SELECT
        COALESCE(MAX(CAST(hour AS DATE)), CAST(:v_loaded AS DATE)),
        COALESCE((SELECT SUM(estimated_usd) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND), 0),
        25,
        25 - COALESCE((SELECT SUM(estimated_usd) FROM MCP_DB.SIGNAL_SANDBOX.SILVER_SPEND), 0),
        COALESCE(:v_batch, 'pending'),
        :v_loaded
    FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION
    WHERE (:v_batch IS NULL OR _batch_id = :v_batch);

    v_gold_out :=
        (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_HOURLY_CONVERSION WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_REVENUE_AT_RISK WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND WHERE (:v_batch IS NULL OR _batch_id = :v_batch))
        + (SELECT COUNT(*) FROM MCP_DB.SIGNAL_SANDBOX.GOLD_TICKET_TOPICS WHERE (:v_batch IS NULL OR _batch_id = :v_batch));
    IF (v_status <> 'success') THEN
        v_error := COALESCE(v_error, 'silver did not reconcile');
    END IF;
    INSERT INTO MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS (
        run_id, batch_id, layer, started_at, finished_at, status, rows_in, rows_out, rows_rejected, error
    ) VALUES (
        'gold-' || :v_run,
        COALESCE(:v_batch, 'pending'),
        'gold',
        :v_start,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        :v_status,
        :v_gold_out,
        :v_gold_out,
        0,
        :v_error
    );

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

-- @section task

CREATE OR REPLACE TASK MCP_DB.SIGNAL_SANDBOX.TASK_BRONZE_TO_SILVER
    SCHEDULE = '1 MINUTE'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GROUPS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_SUPPORT_TICKETS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEPLOYS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_DEMO_META')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_GOLDEN_TICKETS')
      OR SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_BRONZE_EXTERNAL_EVENTS')
AS
    CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL);

CREATE OR REPLACE TASK MCP_DB.SIGNAL_SANDBOX.TASK_SILVER_TO_GOLD
    SCHEDULE = '1 MINUTE'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_SILVER_GROUPS')
AS
    CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL);

-- @section resume

ALTER TASK MCP_DB.SIGNAL_SANDBOX.TASK_SILVER_TO_GOLD RESUME;

ALTER TASK MCP_DB.SIGNAL_SANDBOX.TASK_BRONZE_TO_SILVER RESUME;
