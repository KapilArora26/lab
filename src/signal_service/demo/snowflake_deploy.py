"""Create MCP_DB.SIGNAL_SANDBOX and load the seeded demo through ingest.

Every statement goes through the Snowflake adapter, which refuses writes
outside that schema. The seed batch is idempotent, so a second deploy does not
duplicate bronze rows. Sessions, payments, and orders stay views over silver.
"""

from __future__ import annotations

from datetime import datetime

from signal_service.demo.generate import ANOMALY_START, golden_rows, group_rows
from signal_service.paths import sql_path
from signal_service.warehouse.snowflake import SnowflakeWarehouse


def load_statements() -> list[str]:
    raw = sql_path("snowflake", "001_sandbox.sql").read_text(encoding="utf-8")
    kept: list[str] = []
    for line in raw.splitlines():
        if line.strip().startswith("--"):
            continue
        kept.append(line)
    return [part.strip() for part in "\n".join(kept).split(";") if part.strip()]


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


def deploy_sandbox(warehouse: SnowflakeWarehouse, client=None) -> dict:
    """Create the sandbox, ingest the seed batch, and check the expansion."""
    from signal_service.pipeline.ddl import ensure_objects, medallion_statements
    from signal_service.pipeline.ingest import ingest_seed
    from signal_service.pipeline.mode import install_event_driven

    print("creating MCP_DB.SIGNAL_SANDBOX objects", flush=True)
    for statement in load_statements():
        warehouse.execute(statement)
    print("applying medallion tables and views", flush=True)
    ensure_objects(warehouse)
    print("installing streams, tasks, and LOAD_STAGING when the role allows", flush=True)
    installed = install_event_driven(warehouse)
    print(f"loading the seed batch through ingest ({installed.mode})", flush=True)
    ingested = ingest_seed(warehouse, mode="sync")
    if ingested["status"] == "failed":
        raise RuntimeError(ingested.get("error") or "seed ingest failed")

    groups = group_rows()

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
    tickets = _count(warehouse, "SELECT count(*)::INTEGER AS tickets FROM bronze_support_tickets")
    if int(tickets["tickets"]) != 92:
        raise RuntimeError(f"support tickets are {tickets['tickets']}, expected 92")

    if client is None:
        from signal_service.cli import _client
        from signal_service.config import load_settings

        client = _client(load_settings())
    from signal_service.orchestration.pipeline import run_pipeline

    print("classifying the seeded incident so ticket topics can be checked", flush=True)
    demo = run_pipeline(warehouse, client, simulate_build_approval=True)
    topics = _count(warehouse, "SELECT count(*)::INTEGER AS topics FROM signals.ticket_topics")
    if int(topics["topics"]) != 73:
        raise RuntimeError(f"ticket topics are {topics['topics']}, expected 73")
    case_id = (demo.get("case_card") or {}).get("case_id")
    if case_id != "C-2026-1120-001" and demo.get("error") != "no anomaly detected":
        lead = ((demo.get("diagnosis_pack") or {}).get("hypotheses") or [{}])[0].get("claim")
        if case_id is None or lead != "Payment gateway failures on UPI":
            raise RuntimeError(f"seeded case did not match the demo: {case_id} {lead}")

    counts = {
        "GROUPS": len(groups),
        "SUPPORT_TICKETS": 92,
        "DEPLOYS": 3,
        "DEMO_META": 6,
        "GOLDEN_TICKETS": len(golden_rows()),
        "SESSIONS": int(sessions["sessions"]),
        "PAYMENTS": int(payments["payments"]),
        "ORDERS": int(orders["orders"]),
        "TICKET_TOPICS": int(topics["topics"]),
    }
    views = {"SESSIONS", "PAYMENTS", "ORDERS"}
    return {
        "schema": warehouse.sandbox,
        "mode": installed.as_dict(),
        "ingest": {"batch_id": ingested["batch_id"], "status": ingested["status"], "propagation": ingested["propagation"]},
        "event_statements": len(medallion_statements("stream")) + len(medallion_statements("procedure")),
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
