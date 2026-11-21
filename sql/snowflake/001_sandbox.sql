-- Signal-to-Decision objects inside MCP_DB.SIGNAL_SANDBOX only.
-- The adapter refuses statements that name another database or schema.
-- sessions, payments, and orders are views. They expand GROUPS with the same
-- 0-based ranges the DuckDB generator materializes, so detection reads the
-- same grain without storing every downsampled session.

CREATE SCHEMA IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX;

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.GROUPS (
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
    n_attempt INTEGER
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SUPPORT_TICKETS (
    ticket_id VARCHAR,
    ts TIMESTAMP_NTZ,
    platform VARCHAR,
    country VARCHAR,
    channel VARCHAR,
    payment_method VARCHAR,
    sku VARCHAR,
    app_version VARCHAR,
    product_area VARCHAR,
    text_redacted VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.DEPLOYS (
    deploy_id VARCHAR,
    deployed_at TIMESTAMP_NTZ,
    platform VARCHAR,
    app_version VARCHAR,
    note VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.DEMO_META (
    key VARCHAR,
    value VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.GOLDEN_TICKETS (
    example_id VARCHAR,
    text_redacted VARCHAR,
    topic VARCHAR,
    sentiment VARCHAR,
    severity VARCHAR,
    product_area VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS (
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
    input_hash VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.EXTERNAL_EVENTS (
    event_id VARCHAR,
    case_id VARCHAR,
    title VARCHAR,
    summary VARCHAR,
    source_url VARCHAR,
    source_type VARCHAR,
    relevance DOUBLE,
    observed_at TIMESTAMP_NTZ,
    metric VARCHAR,
    provider VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SPEND_LOG (
    logged_at TIMESTAMP_NTZ,
    case_id VARCHAR,
    kind VARCHAR,
    estimated_usd DOUBLE,
    input_tokens INTEGER,
    output_tokens INTEGER,
    provider VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.MEMORY_CASES (
    case_id VARCHAR PRIMARY KEY,
    metric VARCHAR,
    segment_json VARCHAR,
    status VARCHAR,
    owner VARCHAR,
    opened_at TIMESTAMP_NTZ,
    started_at TIMESTAMP_NTZ,
    severity VARCHAR,
    acknowledged_at TIMESTAMP_NTZ,
    payload_json VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.MEMORY_CALENDAR (
    event_id VARCHAR,
    kind VARCHAR,
    start_at TIMESTAMP_NTZ,
    end_at TIMESTAMP_NTZ,
    metric VARCHAR,
    segment_json VARCHAR,
    note VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.MEMORY_CAPABILITIES (
    name VARCHAR,
    table_name VARCHAR,
    eval_threshold DOUBLE,
    description VARCHAR,
    registered_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.MEMORY_PREFERENCES (
    stakeholder VARCHAR,
    channel VARCHAR,
    quiet_hours VARCHAR,
    detail VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.MEMORY_LESSONS (
    lesson_id VARCHAR,
    case_id VARCHAR,
    fingerprint VARCHAR,
    summary VARCHAR,
    recorded_at TIMESTAMP_NTZ
);

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.SESSIONS AS
SELECT
    's-' || g.group_id || '-' || f.index::VARCHAR AS session_id,
    g.hour AS ts,
    g.platform,
    g.country,
    g.channel,
    g.payment_method,
    g.sku,
    g.app_version,
    f.index < g.n_convert AS converted
FROM MCP_DB.SIGNAL_SANDBOX.GROUPS g,
LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, g.n_sessions)) f;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.PAYMENTS AS
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
    (SELECT value::DOUBLE FROM MCP_DB.SIGNAL_SANDBOX.DEMO_META WHERE key = 'aov_usd') AS amount_usd,
    g.platform,
    g.country,
    g.app_version
FROM MCP_DB.SIGNAL_SANDBOX.GROUPS g,
LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, g.n_attempt)) f;

CREATE OR REPLACE VIEW MCP_DB.SIGNAL_SANDBOX.ORDERS AS
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
    (SELECT value::DOUBLE FROM MCP_DB.SIGNAL_SANDBOX.DEMO_META WHERE key = 'aov_usd') AS amount_usd,
    'paid' AS status
FROM MCP_DB.SIGNAL_SANDBOX.GROUPS g,
LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, g.n_convert)) f;
