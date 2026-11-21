"""Create MCP_DB.SIGNAL_SANDBOX and load the seeded demo plus the golden set.

Every statement goes through the Snowflake adapter, which refuses writes
outside that schema. Reloading deletes rows in the sandbox tables and inserts
them again. Views stay in place and read the new groups.
"""

from __future__ import annotations

import json
from datetime import datetime

from signal_service.demo.generate import ANOMALY_START, deploy_rows, group_rows, meta_rows, ticket_rows
from signal_service.paths import data_path, sql_path
from signal_service.warehouse.snowflake import SnowflakeWarehouse

_CLEAR = (
    "groups",
    "support_tickets",
    "deploys",
    "demo_meta",
    "golden_tickets",
    "ticket_topics",
    "external_events",
    "spend_log",
    "memory_cases",
    "memory_calendar",
    "memory_capabilities",
    "memory_preferences",
    "memory_lessons",
)


def load_statements() -> list[str]:
    raw = sql_path("snowflake", "001_sandbox.sql").read_text(encoding="utf-8")
    kept: list[str] = []
    for line in raw.splitlines():
        if line.strip().startswith("--"):
            continue
        kept.append(line)
    return [part.strip() for part in "\n".join(kept).split(";") if part.strip()]


def golden_rows() -> list[tuple]:
    rows: list[tuple] = []
    for line in data_path("golden", "tickets.jsonl").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        rows.append(
            (
                item["id"],
                item["text"],
                item["topic"],
                item["sentiment"],
                item["severity"],
                item["product_area"],
            )
        )
    return rows


def expansion_totals(groups: list[tuple]) -> dict[str, int]:
    sessions = converted = payments = 0
    for row in groups:
        n_sessions, n_convert, n_attempt = int(row[8]), int(row[9]), int(row[10])
        if n_convert > n_sessions or n_convert > n_attempt:
            raise RuntimeError("demo group row expands past its session or attempt count")
        sessions += n_sessions
        converted += n_convert
        payments += n_attempt
    return {
        "sessions": sessions,
        "converted": converted,
        "payments": payments,
        "payment_succeeded": converted,
        "orders": converted,
    }


def _count(warehouse: SnowflakeWarehouse, sql: str) -> dict:
    rows = warehouse.query(sql)
    if not rows:
        raise RuntimeError("snowflake count returned no row")
    return {key: int(value) if value is not None and float(value) == int(float(value)) else value for key, value in rows[0].items()}


def _require(actual: dict, expected: dict, label: str) -> None:
    for key, value in expected.items():
        got = actual.get(key)
        if isinstance(got, float) and float(got) != float(value):
            raise RuntimeError(f"{label} {key} is {got}, expected {value}")
        if not isinstance(got, float) and int(got) != int(value):
            raise RuntimeError(f"{label} {key} is {got}, expected {value}")


def deploy_sandbox(warehouse: SnowflakeWarehouse) -> dict:
    """Create the sandbox objects, reload the demo, and check the expansion."""
    print("creating MCP_DB.SIGNAL_SANDBOX objects", flush=True)
    for statement in load_statements():
        warehouse.execute(statement)
    print("clearing sandbox tables", flush=True)
    for name in _CLEAR:
        warehouse.execute(f"DELETE FROM {name}")

    groups = group_rows()
    tickets = ticket_rows()
    deploys = deploy_rows()
    meta = meta_rows()
    golden = golden_rows()
    print(f"loading {len(groups)} groups, {len(tickets)} tickets, {len(golden)} golden rows", flush=True)
    warehouse.executemany(
        """
        INSERT INTO groups (
            group_id, hour, platform, country, channel, payment_method, sku, app_version,
            n_sessions, n_convert, n_attempt
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        groups,
    )
    warehouse.executemany(
        """
        INSERT INTO support_tickets (
            ticket_id, ts, platform, country, channel, payment_method, sku, app_version,
            product_area, text_redacted
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        tickets,
    )
    warehouse.executemany(
        "INSERT INTO deploys (deploy_id, deployed_at, platform, app_version, note) VALUES (?,?,?,?,?)",
        deploys,
    )
    warehouse.executemany("INSERT INTO demo_meta (key, value) VALUES (?,?)", meta)
    warehouse.executemany(
        """
        INSERT INTO golden_tickets (
            example_id, text_redacted, topic, sentiment, severity, product_area
        ) VALUES (?,?,?,?,?,?)
        """,
        golden,
    )

    expected = expansion_totals(groups)
    print("checking session, payment, and order views", flush=True)
    sessions = _count(
        warehouse,
        """
        SELECT count(*)::INTEGER AS sessions,
               sum(CASE WHEN converted THEN 1 ELSE 0 END)::INTEGER AS converted
        FROM sessions
        """,
    )
    _require(sessions, {"sessions": expected["sessions"], "converted": expected["converted"]}, "sessions view")
    payments = _count(
        warehouse,
        """
        SELECT count(*)::INTEGER AS payments,
               sum(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END)::INTEGER AS payment_succeeded
        FROM payments
        """,
    )
    _require(
        payments,
        {"payments": expected["payments"], "payment_succeeded": expected["payment_succeeded"]},
        "payments view",
    )
    orders = _count(
        warehouse,
        "SELECT count(*)::INTEGER AS orders, avg(amount_usd)::DOUBLE AS aov FROM orders",
    )
    if int(orders["orders"]) != expected["orders"]:
        raise RuntimeError(f"orders view count is {orders['orders']}, expected {expected['orders']}")
    if abs(float(orders["aov"]) - 48.0) > 1e-9:
        raise RuntimeError(f"orders average amount is {orders['aov']}, expected 48")

    hour = datetime(ANOMALY_START.year, ANOMALY_START.month, ANOMALY_START.day, ANOMALY_START.hour)
    slice_volume = slice_converted = 0
    for row in groups:
        if row[2] == "android" and row[3] == "IN" and row[1] == hour:
            slice_volume += int(row[8])
            slice_converted += int(row[9])
    incident = _count(
        warehouse,
        """
        SELECT count(*)::INTEGER AS volume,
               sum(CASE WHEN converted THEN 1 ELSE 0 END)::INTEGER AS positives
        FROM sessions
        WHERE platform = 'android' AND country = 'IN'
          AND ts = '2026-11-20 02:00:00'::TIMESTAMP_NTZ
        """,
    )
    _require(incident, {"volume": slice_volume, "positives": slice_converted}, "android India hour")

    counts = {
        "GROUPS": len(groups),
        "SUPPORT_TICKETS": len(tickets),
        "DEPLOYS": len(deploys),
        "DEMO_META": len(meta),
        "GOLDEN_TICKETS": len(golden),
        "SESSIONS": int(sessions["sessions"]),
        "PAYMENTS": int(payments["payments"]),
        "ORDERS": int(orders["orders"]),
        "TICKET_TOPICS": 0,
        "EXTERNAL_EVENTS": 0,
        "SPEND_LOG": 0,
        "MEMORY_CASES": 0,
        "MEMORY_CALENDAR": 0,
        "MEMORY_CAPABILITIES": 0,
        "MEMORY_PREFERENCES": 0,
        "MEMORY_LESSONS": 0,
    }
    views = {"SESSIONS", "PAYMENTS", "ORDERS"}
    return {
        "schema": warehouse.sandbox,
        "objects": [
            {"name": name, "kind": "view" if name in views else "table", "rows": rows}
            for name, rows in counts.items()
        ],
        "seeded_anomaly": {
            "segment": "android|IN",
            "hour": "2026-11-20T02:00:00Z",
            "sessions": slice_volume,
            "converted": slice_converted,
        },
    }
