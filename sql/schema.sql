-- Logical signal tables. DuckDB creates these under the schema `signals`.
-- Snowflake writes are allowed only in MCP_DB.SIGNAL_SANDBOX (see the adapter).
-- The migration that creates the sandbox, demo tables, and expansion views is
-- sql/snowflake/001_sandbox.sql.

CREATE SCHEMA IF NOT EXISTS signals;

CREATE TABLE IF NOT EXISTS signals.ticket_topics (
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
    input_hash VARCHAR
);

CREATE TABLE IF NOT EXISTS signals.external_events (
    event_id VARCHAR,
    case_id VARCHAR,
    title VARCHAR,
    summary VARCHAR,
    source_url VARCHAR,
    source_type VARCHAR,
    relevance DOUBLE,
    observed_at TIMESTAMP,
    metric VARCHAR,
    provider VARCHAR
);

CREATE TABLE IF NOT EXISTS signals.spend_log (
    logged_at TIMESTAMP,
    case_id VARCHAR,
    kind VARCHAR,
    estimated_usd DOUBLE,
    input_tokens INTEGER,
    output_tokens INTEGER,
    provider VARCHAR
);

-- Snowflake sandbox equivalents. The adapter refuses any statement that is
-- not fully qualified to MCP_DB.SIGNAL_SANDBOX.

CREATE SCHEMA IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX;

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS (
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
    observed_at TIMESTAMP,
    metric VARCHAR,
    provider VARCHAR
);

CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.SPEND_LOG (
    logged_at TIMESTAMP,
    case_id VARCHAR,
    kind VARCHAR,
    estimated_usd DOUBLE,
    input_tokens INTEGER,
    output_tokens INTEGER,
    provider VARCHAR
);
