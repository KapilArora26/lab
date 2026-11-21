from __future__ import annotations

SIGNAL_DDL = [
    "CREATE SCHEMA IF NOT EXISTS signals",
    """
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
    )
    """,
    """
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
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signals.spend_log (
        logged_at TIMESTAMP,
        case_id VARCHAR,
        kind VARCHAR,
        estimated_usd DOUBLE,
        input_tokens INTEGER,
        output_tokens INTEGER,
        provider VARCHAR
    )
    """,
]


def ensure_signal_tables(warehouse) -> None:
    for statement in SIGNAL_DDL:
        warehouse.execute(statement)
