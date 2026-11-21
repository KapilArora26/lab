from __future__ import annotations

import json
from datetime import UTC, datetime

from signal_service.paths import data_path

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS memory_cases (
        case_id VARCHAR PRIMARY KEY,
        metric VARCHAR,
        segment_json VARCHAR,
        status VARCHAR,
        owner VARCHAR,
        opened_at TIMESTAMP,
        started_at TIMESTAMP,
        severity VARCHAR,
        acknowledged_at TIMESTAMP,
        payload_json VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_calendar (
        event_id VARCHAR,
        kind VARCHAR,
        start_at TIMESTAMP,
        end_at TIMESTAMP,
        metric VARCHAR,
        segment_json VARCHAR,
        note VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_capabilities (
        name VARCHAR,
        table_name VARCHAR,
        eval_threshold DOUBLE,
        description VARCHAR,
        registered_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_preferences (
        stakeholder VARCHAR,
        channel VARCHAR,
        quiet_hours VARCHAR,
        detail VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory_lessons (
        lesson_id VARCHAR,
        case_id VARCHAR,
        fingerprint VARCHAR,
        summary VARCHAR,
        recorded_at TIMESTAMP
    )
    """,
]


class Memory:
    def __init__(self, warehouse):
        self.warehouse = warehouse
        for statement in _DDL:
            warehouse.execute(statement)

    def seed_defaults(self) -> None:
        if not self.warehouse.query("SELECT 1 FROM memory_calendar LIMIT 1"):
            raw = json.loads(data_path("calendar", "business_calendar.json").read_text(encoding="utf-8"))
            self.warehouse.executemany(
                "INSERT INTO memory_calendar VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        event["id"],
                        event["kind"],
                        datetime.fromisoformat(event["start"].replace("Z", "")),
                        datetime.fromisoformat(event["end"].replace("Z", "")),
                        event.get("metric"),
                        json.dumps(event.get("segment") or {}, sort_keys=True),
                        event.get("note"),
                    )
                    for event in raw
                ],
            )
        if not self.warehouse.query("SELECT 1 FROM memory_preferences LIMIT 1"):
            self.warehouse.executemany(
                "INSERT INTO memory_preferences VALUES (?,?,?,?)",
                [
                    ("vp_growth", "voice", "22:00-07:00", "exec"),
                    ("analytics_lead", "chat", None, "full"),
                    ("data_owner", "chat", None, "full"),
                ],
            )
        if not self.warehouse.query("SELECT 1 FROM memory_capabilities LIMIT 1"):
            from signal_service.orchestration.capabilities import CAPABILITIES

            now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
            self.warehouse.executemany(
                "INSERT INTO memory_capabilities VALUES (?,?,?,?,?)",
                [
                    (item["name"], item["table"], item["eval_threshold"], item["description"], now)
                    for item in CAPABILITIES
                ],
            )

    def calendar(self) -> list[dict]:
        rows = self.warehouse.query(
            "SELECT event_id, kind, start_at, end_at, metric, segment_json, note FROM memory_calendar"
        )
        events = []
        for row in rows:
            events.append(
                {
                    "id": row["event_id"],
                    "kind": row["kind"],
                    "start": row["start_at"],
                    "end": row["end_at"],
                    "metric": row["metric"],
                    "segment": json.loads(row["segment_json"] or "{}"),
                    "note": row["note"],
                }
            )
        return events

    def preferences(self) -> dict:
        rows = self.warehouse.query("SELECT stakeholder, channel, quiet_hours, detail FROM memory_preferences")
        return {
            row["stakeholder"]: {
                "channel": row["channel"],
                "quiet_hours": row["quiet_hours"],
                "detail": row["detail"],
            }
            for row in rows
        }

    def open_cases(self) -> list[dict]:
        rows = self.warehouse.query(
            """
            SELECT case_id, metric, segment_json, status, owner, opened_at, started_at, severity, acknowledged_at, payload_json
            FROM memory_cases
            """
        )
        return [_case_row(row) for row in rows]

    def save_case(self, card: dict, *, status: str, owner: str, payload: dict | None = None) -> None:
        now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
        started = datetime.fromisoformat(card["started_at"].replace("Z", ""))
        self.warehouse.execute("DELETE FROM memory_cases WHERE case_id = ?", [card["case_id"]])
        self.warehouse.execute(
            "INSERT INTO memory_cases VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                card["case_id"],
                card["metric"],
                json.dumps(card["segment"], sort_keys=True),
                status,
                owner,
                now,
                started,
                card["severity"],
                None,
                json.dumps(payload or card),
            ],
        )

    def update_case(self, case_id: str, *, status: str, payload: dict) -> None:
        self.warehouse.execute(
            "UPDATE memory_cases SET status = ?, payload_json = ? WHERE case_id = ?",
            [status, json.dumps(payload), case_id],
        )

    def record_outcome(self, case_id: str, *, correct: bool, summary: str) -> dict:
        lesson = {
            "lesson_id": f"L-{case_id}",
            "case_id": case_id,
            "fingerprint": summary[:180],
            "summary": summary,
            "correct": correct,
        }
        self.warehouse.execute(
            "INSERT INTO memory_lessons VALUES (?,?,?,?,?)",
            [lesson["lesson_id"], case_id, lesson["fingerprint"], summary, datetime.now(UTC).replace(tzinfo=None)],
        )
        self.warehouse.execute(
            "UPDATE memory_cases SET status = 'closed' WHERE case_id = ?",
            [case_id],
        )
        return lesson


def _case_row(row: dict) -> dict:
    payload = json.loads(row["payload_json"] or "{}")
    payload.update(
        {
            "case_id": row["case_id"],
            "metric": row["metric"],
            "segment": json.loads(row["segment_json"] or "{}"),
            "status": row["status"],
            "owner": row["owner"],
            "opened_at": row["opened_at"],
            "started_at": row["started_at"],
            "severity": row["severity"],
            "acknowledged_at": row["acknowledged_at"],
        }
    )
    return payload
