from __future__ import annotations

from datetime import UTC, datetime, timedelta

from signal_service.hashing import text_hash
from signal_service.safety.redaction import RedactionError, assert_redacted
from signal_service.signals.tables import ensure_signal_tables


def _chunks(items: list, size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def run_ticket_signals(
    warehouse,
    client,
    *,
    case_id: str,
    segment: dict[str, str],
    started_at: datetime,
    ended_at: datetime,
    batch_size: int = 25,
    model: str = "grok-4.7",
) -> dict:
    ensure_signal_tables(warehouse)
    clauses = ["ts >= ?", "ts < ?"]
    params: list = [started_at, ended_at + timedelta(hours=1)]
    for column in ("platform", "country"):
        if column in segment:
            clauses.append(f"{column} = ?")
            params.append(segment[column])
    rows = warehouse.query(
        f"""
        SELECT ticket_id, text_redacted
        FROM support_tickets
        WHERE {' AND '.join(clauses)}
        ORDER BY ticket_id
        """,
        params,
    )
    accepted = []
    rejected = 0
    for row in rows:
        try:
            assert_redacted(row["text_redacted"])
        except RedactionError:
            rejected += 1
            continue
        accepted.append(row)
    classified = 0
    now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    for batch in _chunks(accepted, batch_size):
        predictions = client.classify_batch(
            [{"id": row["ticket_id"], "text": row["text_redacted"]} for row in batch]
        )
        warehouse.executemany(
            """
            INSERT INTO signals.ticket_topics VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    item["id"],
                    case_id,
                    now,
                    item["topic"],
                    item["sentiment"],
                    item["severity"],
                    item["product_area"],
                    item["evidence_snippet"],
                    float(item["confidence"]),
                    model if client.provider == "grok" else "fallback-keywords",
                    client.provider,
                    text_hash(next(row["text_redacted"] for row in batch if row["ticket_id"] == item["id"])),
                )
                for item in predictions
            ],
        )
        classified += len(predictions)
    return {
        "classified": classified,
        "rejected_pii": rejected,
        "provider": client.provider,
        "input_rows": len(rows),
    }


def load_topics(warehouse, case_id: str) -> list[dict]:
    ensure_signal_tables(warehouse)
    return warehouse.query(
        """
        SELECT ticket_id, topic, sentiment, severity, product_area, evidence_snippet, confidence, provider
        FROM signals.ticket_topics
        WHERE case_id = ?
        """,
        [case_id],
    )
