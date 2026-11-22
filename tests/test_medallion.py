"""Ingest reaches silver and gold, records every layer, and keeps PII out of them."""

from __future__ import annotations

import json
import os
import re
import time

import pytest
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from signal_service.api.app import create_app
from signal_service.pipeline.ddl import medallion_statements
from signal_service.pipeline.ingest import SEED_BATCH_ID, Batch, ingest, ingest_batch, ingest_seed, rerun_batch
from signal_service.pipeline.mode import ModeReport, ProcedureCompileError, dynamic_table_sql, install_event_driven
from signal_service.pipeline.reconcile import reconciliation_errors
from signal_service.safety.redaction import pii_kinds
from signal_service.warehouse.duckdb import DuckDBWarehouse


def _count(warehouse, sql: str, params: list | None = None) -> int:
    return int(warehouse.query(sql, params)[0]["n"])


def test_seed_batch_reaches_silver_and_gold(demo_wh):
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups") == 2960
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_groups") == 2960
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM sessions") == 429200
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM payments") == 51800
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM orders") == 42692
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_support_tickets") == 92
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_support_tickets") == 89
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM groups") == 2960
    row = demo_wh.query("SELECT _loaded_at, _batch_id FROM silver_groups LIMIT 1")[0]
    assert row["_loaded_at"] is not None
    assert row["_batch_id"] == SEED_BATCH_ID
    gold = demo_wh.query("SELECT _loaded_at, _batch_id FROM gold_hourly_conversion LIMIT 1")[0]
    assert gold["_loaded_at"] is not None and gold["_batch_id"] == row["_batch_id"]
    session = demo_wh.query("SELECT _loaded_at, _batch_id FROM silver_sessions LIMIT 1")[0]
    assert session["_batch_id"] == row["_batch_id"]
    lag = demo_wh.query("SELECT * FROM gold_pipeline_lag")[0]
    assert lag["bronze_ingested_at"] is not None
    assert lag["silver_loaded_at"] is not None
    assert lag["gold_loaded_at"] is not None
    assert lag["gold_loaded_at"] >= lag["bronze_ingested_at"]
    assert int(lag["staging_to_gold_seconds"]) >= 0
    layers = {
        item["layer"]: item["status"]
        for item in demo_wh.query(
            "SELECT layer, status FROM pipeline_runs WHERE batch_id = ?",
            [SEED_BATCH_ID],
        )
    }
    assert layers == {"bronze": "success", "silver": "success", "gold": "success"}


def test_new_batch_counts_match_across_layers(demo_wh):
    result = ingest(demo_wh, batch="new", hours=2)
    batch_id = result["batch_id"]
    assert result["status"] == "success"
    assert result["mode"] == "sync"
    assert result["noop"] is False
    bronze = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups WHERE _batch_id = ?", [batch_id])
    silver = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE _batch_id = ?", [batch_id])
    assert bronze == silver
    assert bronze == 20
    expanded = demo_wh.query(
        "SELECT COALESCE(SUM(n_sessions), 0)::INTEGER AS n FROM silver_groups WHERE _batch_id = ?",
        [batch_id],
    )[0]["n"]
    sessions = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_sessions WHERE _batch_id = ?", [batch_id])
    assert int(expanded) == sessions
    grains = _count(
        demo_wh,
        """
        SELECT COUNT(*)::INTEGER AS n FROM (
            SELECT hour, platform, country, payment_method, app_version
            FROM silver_groups WHERE _batch_id = ?
            GROUP BY 1, 2, 3, 4, 5
        )
        """,
        [batch_id],
    )
    gold = _count(
        demo_wh,
        "SELECT COUNT(*)::INTEGER AS n FROM gold_hourly_conversion WHERE _batch_id = ?",
        [batch_id],
    )
    assert gold == grains
    assert gold > 0
    lag = demo_wh.query("SELECT * FROM gold_pipeline_lag WHERE batch_id = ?", [batch_id])[0]
    assert lag["staging_to_gold_seconds"] is not None
    assert int(lag["staging_to_gold_seconds"]) >= 0


def test_reingesting_the_same_batch_is_a_noop(demo_wh):
    before = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM pipeline_runs")
    groups = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups")
    again = ingest_seed(demo_wh)
    assert again["noop"] is True
    assert again["status"] == "noop"
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups") == groups
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM pipeline_runs") == before


def test_failure_path_records_the_layer(demo_wh, monkeypatch):
    def boom(*_args, **_kwargs):
        raise RuntimeError("silver merge failed")

    monkeypatch.setattr("signal_service.pipeline.propagate.load_silver", boom)
    result = ingest(demo_wh, batch="new", hours=1)
    assert result["status"] == "failed"
    rows = demo_wh.query(
        "SELECT layer, status, error FROM pipeline_runs WHERE batch_id = ? ORDER BY started_at",
        [result["batch_id"]],
    )
    assert [(row["layer"], row["status"]) for row in rows] == [("bronze", "success"), ("silver", "failed")]
    assert "silver merge failed" in rows[1]["error"]


def test_data_quality_and_pii_stay_out_of_silver_and_gold(demo_wh, tmp_path):
    payload = {
        "batch_id": "dq-batch",
        "groups": [
            {
                "group_id": "bad-1",
                "hour": "2026-11-21T00:00:00",
                "platform": "android",
                "country": "IN",
                "channel": "app",
                "payment_method": "upi",
                "sku": "SKU-100",
                "app_version": "4.2.1",
                "n_sessions": 10,
                "n_convert": 12,
                "n_attempt": 12,
            }
        ],
        "support_tickets": [
            {
                "ticket_id": "t-pii",
                "ts": "2026-11-21T00:00:00",
                "platform": "android",
                "country": "IN",
                "channel": "app",
                "payment_method": "upi",
                "sku": "SKU-100",
                "app_version": "4.2.1",
                "product_area": "payments",
                "text_redacted": "Email me at jane.doe@example.com about the failed payment.",
            }
        ],
    }
    path = tmp_path / "batch.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    result = ingest(demo_wh, batch="file", path=str(path))
    assert result["status"] == "success"
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id = 'bad-1'") == 0
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_support_tickets WHERE ticket_id = 't-pii'") == 0
    reasons = {
        row["reason"]
        for row in demo_wh.query("SELECT reason FROM silver_rejects WHERE _batch_id = 'dq-batch'")
    }
    assert "conversions_gt_sessions" in reasons
    assert any(reason.startswith("pii:") for reason in reasons)
    email = "jane.doe@example.com"
    bronze = demo_wh.query("SELECT text_redacted FROM bronze_support_tickets WHERE ticket_id = 't-pii'")
    assert email in bronze[0]["text_redacted"]
    for table in (
        "silver_support_tickets",
        "silver_rejects",
        "silver_golden_tickets",
        "silver_groups",
        "gold_hourly_conversion",
        "gold_revenue_at_risk",
        "gold_case_facts",
        "gold_ticket_topics",
        "gold_daily_spend",
    ):
        for row in demo_wh.query(f"SELECT * FROM {table}"):
            for value in row.values():
                if isinstance(value, str):
                    assert not pii_kinds(value), (table, value)


def test_missing_procedure_privilege_is_reported_and_skipped():
    class Scripted:
        backend = "snowflake"

        def __init__(self, rejects: tuple[str, ...]):
            self.rejects = rejects
            self.statements: list[str] = []

        def execute(self, sql: str, params=None):
            self.statements.append(sql)
            folded = " ".join(sql.lower().split())
            for token in self.rejects:
                if token in folded:
                    raise RuntimeError(f"Insufficient privileges for {token}")

    denied = install_event_driven(Scripted(("procedure",)))
    assert denied.mode == "sync"
    assert denied.procedure_ready is False
    assert "CREATE PROCEDURE" in denied.missing_privileges
    assert "CREATE TASK" not in denied.missing_privileges

    warehouse = Scripted(())
    allowed = install_event_driven(warehouse)
    assert allowed.mode == "task"
    assert allowed.procedure_ready is True
    assert allowed.tasks_ready is True
    assert allowed.missing_privileges == []
    assert any(statement.upper().startswith("ALTER TASK") for statement in warehouse.statements)


def test_ingest_calls_load_staging_when_the_procedure_is_ready(demo_wh, monkeypatch):
    import signal_service.pipeline.ingest as ingest_module

    monkeypatch.setattr(
        ingest_module,
        "resolve_mode",
        lambda *_args, **_kwargs: ModeReport(mode="procedure", procedure_ready=True),
    )
    calls: list[tuple] = []
    original = demo_wh.execute

    def execute(sql, params=None):
        if sql.lstrip().upper().startswith("CALL"):
            calls.append((sql, params))
            return None
        return original(sql, params)

    demo_wh.execute = execute
    result = ingest(demo_wh, batch="new", hours=1)
    assert result["propagation"] == "procedure"
    assert calls and "LOAD_STAGING" in calls[0][0].upper()
    assert calls[0][1] == [result["batch_id"]]
    assert _count(
        demo_wh,
        "SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE _batch_id = ?",
        [result["batch_id"]],
    ) == 0


def test_post_ingest_on_the_api(demo_wh):
    client = TestClient(create_app(demo_wh))
    response = client.post("/v1/ingest", json={"batch": "new", "hours": 1})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["mode"] == "sync"
    assert len(body["layers"]) == 3
    outside = client.post("/v1/ingest", json={"batch": "file", "path": "/etc/passwd"})
    assert outside.status_code == 400


def test_seed_topics_external_events_and_layer_conservation(demo_wh):
    assert reconciliation_errors(demo_wh, SEED_BATCH_ID, expect_topics=73, expect_external=2) == []
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups") == 2960
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_groups") == 2960
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_external_events") == 2
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_external_events") == 2
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM gold_external_events") == 2
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM signals.ticket_topics") == 73
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_ticket_topics") == 73
    upi = demo_wh.query(
        """
        SELECT topic, ticket_count
        FROM gold_ticket_topics
        WHERE topic = 'upi_payment_failure'
        """
    )
    assert upi and int(upi[0]["ticket_count"]) == 72
    runs = demo_wh.query(
        "SELECT layer, rows_in, rows_out, rows_rejected FROM pipeline_runs WHERE batch_id = ?",
        [SEED_BATCH_ID],
    )
    assert {row["layer"] for row in runs} == {"bronze", "silver", "gold"}
    for row in runs:
        assert int(row["rows_in"]) == int(row["rows_out"]) + int(row["rows_rejected"])


def test_rerun_keeps_bronze_and_replaces_a_false_success(demo_wh):
    before_groups = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups")
    again = rerun_batch(demo_wh, SEED_BATCH_ID)
    assert again["noop"] is False
    assert again["status"] == "success"
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups") == before_groups
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_groups") == before_groups
    assert reconciliation_errors(demo_wh, SEED_BATCH_ID, expect_topics=73, expect_external=2) == []


def _naive(value) -> datetime:
    if isinstance(value, datetime):
        stamp = value
    else:
        stamp = datetime.fromisoformat(str(value).replace("Z", ""))
    if stamp.tzinfo is not None:
        stamp = stamp.astimezone(timezone.utc).replace(tzinfo=None)
    return stamp.replace(microsecond=0)


def _group(group_id: str, sessions: int, converted: int, attempts: int) -> tuple:
    return (
        group_id,
        datetime(2026, 11, 22, 18, 0, 0),
        "android",
        "IN",
        "app",
        "upi",
        "SKU-100",
        "4.2.1",
        sessions,
        converted,
        attempts,
    )


def test_lineage_timestamps_stay_utc_when_the_process_zone_is_pacific(tmp_path):
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "America/Los_Angeles"
    time.tzset()
    warehouse = DuckDBWarehouse(tmp_path / "utc.duckdb")
    try:
        before = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0) - timedelta(seconds=1)
        batch = Batch(
            batch_id="utc-batch",
            source="test",
            groups=[_group("utc-1", 40, 4, 6)],
            tickets=[],
            deploys=[],
            meta=[],
            golden=[],
            external=[],
        )
        result = ingest_batch(warehouse, batch)
        after = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0) + timedelta(seconds=1)
        assert result["status"] == "success"
        local_now = datetime.now().replace(tzinfo=None)
        stamps = [
            warehouse.query("SELECT _ingested_at AS ts FROM bronze_groups WHERE group_id = 'utc-1'")[0]["ts"],
            warehouse.query("SELECT _loaded_at AS ts FROM silver_groups WHERE group_id = 'utc-1'")[0]["ts"],
            warehouse.query("SELECT _loaded_at AS ts FROM gold_hourly_conversion WHERE _batch_id = 'utc-batch' LIMIT 1")[0]["ts"],
        ]
        runs = warehouse.query(
            "SELECT started_at, finished_at FROM pipeline_runs WHERE batch_id = 'utc-batch'"
        )
        for row in runs:
            stamps.append(row["started_at"])
            stamps.append(row["finished_at"])
        assert stamps
        for stamp in stamps:
            moment = _naive(stamp)
            assert before <= moment <= after
            assert abs((moment - local_now).total_seconds()) > 2 * 3600
    finally:
        warehouse.close()
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


def test_chaos_metrics_are_rejected_and_conserved(tmp_path):
    warehouse = DuckDBWarehouse(tmp_path / "chaos.duckdb")
    try:
        batch = Batch(
            batch_id="CHAOS-20261122-1829",
            source="test",
            groups=[
                _group("chaos-over", 100, 4800, 4800),
                _group("chaos-negative", -250, -30, -40),
                _group("chaos-neg-convert", 120, -15, 10),
                _group("chaos-ok", 100, 8, 12),
            ],
            tickets=[],
            deploys=[],
            meta=[],
            golden=[],
            external=[],
        )
        result = ingest_batch(warehouse, batch)
        assert result["status"] == "success"
        assert _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE _batch_id = ?", [batch.batch_id]) == 1
        reasons = {
            row["reason"]
            for row in warehouse.query(
                "SELECT reason FROM silver_rejects WHERE _batch_id = ? AND source_table = 'bronze_groups'",
                [batch.batch_id],
            )
        }
        assert reasons == {"conversions_gt_sessions", "negative_count"}
        assert reconciliation_errors(warehouse, batch.batch_id) == []
        silver = warehouse.query(
            "SELECT rows_in, rows_out, rows_rejected FROM pipeline_runs WHERE batch_id = ? AND layer = 'silver'",
            [batch.batch_id],
        )[0]
        assert int(silver["rows_in"]) == int(silver["rows_out"]) + int(silver["rows_rejected"])
        assert int(silver["rows_in"]) == 4
        assert int(silver["rows_rejected"]) == 3
    finally:
        warehouse.close()


def test_snowflake_lineage_sql_uses_utc_and_names_metric_rejects():
    procedure = medallion_statements("procedure")[0]
    folded = " ".join(procedure.split())
    assert "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ" in folded
    assert ":= CURRENT_TIMESTAMP()" not in folded
    assert "'negative_count'" in procedure
    assert "'conversions_gt_sessions'" in procedure
    dynamic = dynamic_table_sql("COMPUTE_WH")
    assert "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ" in " ".join(dynamic.split())
    assert "n_attempt >= 0" in dynamic


def test_null_batch_consumes_streams_and_never_writes_pending():
    procedure = medallion_statements("procedure")[0]
    upper = procedure.upper()
    assert "BEGIN TRANSACTION" in upper
    assert "COMMIT" in upper
    assert "'PENDING'" not in upper
    assert "COALESCE(:V_BATCH, 'PENDING')" not in upper
    insert_at = upper.find("INSERT INTO MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE")
    assert insert_at > 0
    consumed = upper[insert_at:upper.find("IF (NOT EXISTS", insert_at)]
    for stream in (
        "STREAM_BRONZE_GROUPS",
        "STREAM_BRONZE_SUPPORT_TICKETS",
        "STREAM_BRONZE_DEPLOYS",
        "STREAM_BRONZE_DEMO_META",
        "STREAM_BRONZE_GOLDEN_TICKETS",
        "STREAM_BRONZE_EXTERNAL_EVENTS",
        "STREAM_SILVER_GROUPS",
    ):
        assert stream in consumed
    assert "METADATA$ACTION" in consumed
    assert "LAYER = 'GOLD'" in consumed
    assert "STATUS = 'SUCCESS'" in consumed
    assert "DELETE FROM MCP_DB.SIGNAL_SANDBOX.GOLD_DAILY_SPEND" in upper
    assert "WHERE _BATCH_ID IN (SELECT BATCH_ID FROM MCP_DB.SIGNAL_SANDBOX.STREAM_BATCH_QUEUE)" in upper
    assert "V_BATCH IS NULL OR" not in upper
    assert re.search(r"LOAD_STAGING\s*\(\s*batch\s+VARCHAR\s*\)", procedure, re.IGNORECASE)


def test_legacy_external_events_land_in_bronze_silver_and_gold(tmp_path):
    from signal_service.pipeline.ddl import ensure_objects
    from signal_service.pipeline.external_backfill import promote_legacy_external_events
    from signal_service.signals.tables import ensure_signal_tables

    warehouse = DuckDBWarehouse(tmp_path / "legacy.duckdb")
    try:
        ensure_objects(warehouse)
        ensure_signal_tables(warehouse)
        warehouse.execute(
            """
            INSERT INTO signals.external_events (
                event_id, case_id, title, summary, source_url, source_type, relevance,
                observed_at, metric, provider
            ) VALUES
            ('ext-legacy-1', 'C-1', 'UPI outage', 'Banks report failures', 'https://example.com/a', 'web', 0.9, TIMESTAMP '2026-11-20 02:00:00', 'checkout_conversion', 'mock'),
            ('ext-legacy-1', 'C-1', 'UPI outage duplicate', 'Ignored duplicate id', 'https://example.com/a', 'web', 0.1, TIMESTAMP '2026-11-20 02:01:00', 'checkout_conversion', 'mock'),
            ('ext-legacy-2', 'C-1', 'Status page', 'Elevated errors', 'https://example.com/b', 'x', 0.8, TIMESTAMP '2026-11-20 02:05:00', 'checkout_conversion', 'mock')
            """
        )
        first = promote_legacy_external_events(warehouse)
        assert first["inserted"] == 2
        assert first["visible"] == 2
        assert _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM bronze_external_events") == 2
        assert _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM silver_external_events") == 2
        assert _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM gold_external_events") == 2
        batches = {row["_batch_id"] for row in warehouse.query("SELECT _batch_id FROM gold_external_events")}
        assert batches == {"legacy-external"}
        second = promote_legacy_external_events(warehouse)
        assert second["inserted"] == 0
        assert _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM bronze_external_events") == 2
        assert _count(warehouse, "SELECT COUNT(*)::INTEGER AS n FROM gold_external_events") == 2
    finally:
        warehouse.close()


def test_legacy_external_events_join_an_existing_bronze_batch(demo_wh):
    from signal_service.pipeline.external_backfill import promote_legacy_external_events
    from signal_service.signals.tables import ensure_signal_tables

    demo_wh.execute("DELETE FROM bronze_external_events")
    demo_wh.execute("DELETE FROM silver_external_events")
    ensure_signal_tables(demo_wh)
    demo_wh.execute("DELETE FROM signals.external_events")
    demo_wh.execute(
        """
        INSERT INTO signals.external_events (
            event_id, case_id, title, summary, source_url, source_type, relevance,
            observed_at, metric, provider
        ) VALUES
        ('ext-open-1', 'C-2026-1120-001', 'UPI outage', 'Banks report failures', 'https://example.com/a', 'web', 0.9, TIMESTAMP '2026-11-20 02:00:00', 'checkout_conversion', 'mock'),
        ('ext-open-2', 'C-2026-1120-001', 'Status page', 'Elevated errors', 'https://example.com/b', 'x', 0.8, TIMESTAMP '2026-11-20 02:05:00', 'checkout_conversion', 'mock')
        """
    )
    landed = promote_legacy_external_events(demo_wh)
    assert landed["inserted"] == 2
    assert landed["visible"] == 2
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_external_events") == 2
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM silver_external_events") == 2
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM gold_external_events") == 2
    batches = {row["_batch_id"] for row in demo_wh.query("SELECT _batch_id FROM gold_external_events")}
    assert batches == {SEED_BATCH_ID}
    again = promote_legacy_external_events(demo_wh)
    assert again["inserted"] == 0
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_external_events") == 2


def test_external_backfill_sql_is_idempotent_and_sandbox_scoped():
    from signal_service.pipeline.ingest import SEED_BATCH_ID as seed_batch
    from signal_service.warehouse.snowflake import assert_sandbox_write

    statements = medallion_statements("external_backfill")
    assert len(statements) == 2
    blob = " ".join(statements)
    folded = " ".join(blob.upper().split())
    assert "INSERT INTO MCP_DB.SIGNAL_SANDBOX.BRONZE_EXTERNAL_EVENTS" in folded
    assert "MERGE INTO MCP_DB.SIGNAL_SANDBOX.SILVER_EXTERNAL_EVENTS" in folded
    assert "NOT EXISTS" in folded
    assert "WHEN NOT MATCHED" in folded
    assert seed_batch in blob
    assert "legacy-external" in blob
    assert re.search(r"(?i)\bdrop\b", blob) is None
    assert re.search(r"(?i)\btruncate\b", blob) is None
    for statement in statements:
        assert_sandbox_write(statement)


_BRONZE_TRIGGER_STREAMS = (
    "STREAM_BRONZE_GROUPS",
    "STREAM_BRONZE_SUPPORT_TICKETS",
    "STREAM_BRONZE_DEPLOYS",
    "STREAM_BRONZE_DEMO_META",
    "STREAM_BRONZE_GOLDEN_TICKETS",
    "STREAM_BRONZE_EXTERNAL_EVENTS",
)


def test_triggered_task_ddl_has_no_schedule_and_keeps_stream_conditions():
    tasks = medallion_statements("task")
    assert len(tasks) == 2
    bronze, silver = tasks
    for statement in tasks:
        folded = " ".join(statement.upper().split())
        assert "SCHEDULE" not in folded
        assert re.search(r"\bWAREHOUSE\s*=", folded) is None
        assert "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'" in folded
        assert "OVERLAP_POLICY = NO_OVERLAP" in folded
        assert "ALLOW_OVERLAPPING_EXECUTION = TRUE" not in folded
        assert "TARGET_COMPLETION_INTERVAL = '1 MINUTE'" in folded
        assert "USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS" not in folded
        assert "WHEN" in folded
        assert "CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL)" in folded
    bronze_folded = " ".join(bronze.upper().split())
    silver_folded = " ".join(silver.upper().split())
    assert "TASK_BRONZE_TO_SILVER" in bronze_folded
    assert "TASK_SILVER_TO_GOLD" in silver_folded
    for name in _BRONZE_TRIGGER_STREAMS:
        assert f"SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.{name}')" in bronze_folded
    assert "SYSTEM$STREAM_HAS_DATA('MCP_DB.SIGNAL_SANDBOX.STREAM_SILVER_GROUPS')" in silver_folded
    assert "STREAM_BRONZE_" not in silver_folded

    streams = medallion_statements("stream")
    expected = set(_BRONZE_TRIGGER_STREAMS) | {"STREAM_SILVER_GROUPS"}
    seen: set[str] = set()
    for statement in streams:
        folded = " ".join(statement.upper().split())
        assert " ON TABLE " in f" {folded} "
        assert " ON VIEW " not in f" {folded} "
        assert "EXTERNAL TABLE" not in folded
        assert "HYBRID" not in folded
        for name in expected:
            if f"STREAM {name}" in f" {folded} " or folded.startswith(f"CREATE OR REPLACE STREAM MCP_DB.SIGNAL_SANDBOX.{name} "):
                seen.add(name)
                source = name.removeprefix("STREAM_")
                assert f"ON TABLE MCP_DB.SIGNAL_SANDBOX.{source}" in folded
    assert seen == expected
    core = " ".join(medallion_statements("core")).upper()
    assert "HYBRID TABLE" not in core
    assert "EXTERNAL TABLE" not in core
    for name in expected:
        source = name.removeprefix("STREAM_")
        assert f"CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.{source}" in core


def test_task_ddl_is_a_single_load_staging_call():
    tasks = medallion_statements("task")
    assert len(tasks) == 2
    for statement in tasks:
        folded = " ".join(statement.upper().split())
        assert "$$" not in statement
        assert "INSERT" not in folded
        assert " BEGIN " not in f" {folded} "
        assert folded.count(" CALL ") == 1
        assert "CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL)" in folded
    procedures = medallion_statements("procedure")
    assert len(procedures) == 2
    load_staging = procedures[0]
    rerun_staging = procedures[1]
    assert re.search(r"LOAD_STAGING\s*\(\s*batch\s+VARCHAR\s*\)", load_staging, re.IGNORECASE)
    assert "DEFAULT" not in load_staging.split("$$", 1)[0].upper()
    assert re.search(r"RERUN_STAGING\s*\(\s*batch\s+VARCHAR\s*\)", rerun_staging, re.IGNORECASE)
    assert "SILVER_TICKET_TOPICS" in load_staging.upper()
    assert "GOLD_TICKET_TOPICS" in load_staging.upper()
    assert "BRONZE_EXTERNAL_EVENTS" in load_staging.upper()
    assert "SILVER_EXTERNAL_EVENTS" in load_staging.upper()


def test_migrations_keep_one_load_staging_signature_and_never_drop():
    from signal_service.demo.snowflake_deploy import load_statements

    statements = load_statements() + medallion_statements()
    load_signatures = []
    rerun_signatures = []
    for statement in statements:
        assert re.search(r"(?i)\bdrop\b", statement) is None
        load = re.search(
            r"(?is)create\s+(?:or\s+replace\s+)?procedure\s+[\w.]+\.LOAD_STAGING\s*\((.*?)\)",
            statement,
        )
        if load:
            load_signatures.append(" ".join(load.group(1).split()))
            assert not re.search(r"(?i)\bdefault\b", statement.split("$$", 1)[0])
        rerun = re.search(
            r"(?is)create\s+(?:or\s+replace\s+)?procedure\s+[\w.]+\.RERUN_STAGING\s*\((.*?)\)",
            statement,
        )
        if rerun:
            rerun_signatures.append(" ".join(rerun.group(1).split()))
    assert load_signatures == ["batch VARCHAR"]
    assert rerun_signatures == ["batch VARCHAR"]


def test_procedure_compile_failure_does_not_skip_ahead_to_tasks():
    class Scripted:
        backend = "snowflake"

        def __init__(self):
            self.statements: list[str] = []

        def execute(self, sql, params=None):
            self.statements.append(sql)
            folded = " ".join(sql.lower().split())
            if folded.startswith("create") and " procedure " in folded[:180] and "load_staging" in folded:
                raise RuntimeError(
                    "Cannot overload PROCEDURE LOAD_STAGING as it would cause ambiguous PROCEDURE overloading."
                )

    warehouse = Scripted()
    with pytest.raises(ProcedureCompileError, match="tasks were not created"):
        install_event_driven(warehouse)
    assert not any(" task " in " ".join(statement.lower().split())[:80] for statement in warehouse.statements)


def test_compilation_error_is_not_a_missing_privilege():
    class Scripted:
        backend = "snowflake"

        def execute(self, sql, params=None):
            folded = " ".join(sql.lower().split())
            if folded.startswith("create") and " task " in folded[:180]:
                raise RuntimeError("SQL compilation error: syntax error line 11 at position 0 unexpected '$$'.")

    report = install_event_driven(Scripted())
    assert "CREATE TASK" not in report.missing_privileges
    assert report.procedure_ready is True
    assert report.tasks_ready is False
    assert any("compilation error" in note.lower() for note in report.notes)


def test_direct_batch_object_carries_lineage(demo_wh):
    batch = Batch(
        batch_id="manual-1",
        source="test",
        groups=[
            (
                "m-1",
                datetime(2026, 11, 21, 3, 0, 0),
                "ios",
                "US",
                "app",
                "card",
                "SKU-100",
                "5.0.2",
                40,
                4,
                6,
            )
        ],
        tickets=[],
        deploys=[],
        meta=[],
        golden=[],
        external=[],
    )
    from signal_service.pipeline.ingest import ingest_batch

    result = ingest_batch(demo_wh, batch)
    assert result["status"] == "success"
    row = demo_wh.query("SELECT _source, _batch_id FROM bronze_groups WHERE group_id = 'm-1'")[0]
    assert row["_source"] == "test"
    assert row["_batch_id"] == "manual-1"
    loaded = demo_wh.query("SELECT _loaded_at, _batch_id FROM silver_groups WHERE group_id = 'm-1'")[0]
    assert loaded["_loaded_at"] is not None
    assert loaded["_batch_id"] == "manual-1"
