from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from signal_service.config import SANDBOX_QUALIFIED
from signal_service.pipeline.ddl import medallion_statements
from signal_service.pipeline.mode import dynamic_table_sql
from signal_service.warehouse.snowflake import UnsafeSQL, assert_sandbox_write, prepare_sql
from signal_service.orchestration.atlas import (
    build_case_file,
    freshness_check,
    quiet_hours_note,
    route_next,
    sla_nudges,
)
from signal_service.orchestration.builder import verify_pr


def test_atlas_and_narrator_do_not_query_metric_tables():
    root = Path(__file__).resolve().parents[1]
    for relative in ("src/signal_service/orchestration/atlas.py", "src/signal_service/narrative/brief.py"):
        text = (root / relative).read_text(encoding="utf-8").lower()
        assert "from sessions" not in text
        assert "support_tickets" not in text
        assert "select " not in text


def test_quiet_hours_high_severity_overrides_and_sla_nudges():
    preferences = {"vp_growth": {"quiet_hours": "22:00-07:00"}}
    note = quiet_hours_note(preferences, "high", datetime(2026, 11, 20, 2, 15))
    assert note is not None and "overrides" in note
    assert quiet_hours_note(preferences, "low", datetime(2026, 11, 20, 2, 15)).startswith("VP Growth is inside")
    assert quiet_hours_note(preferences, "high", datetime(2026, 11, 20, 15, 0)) is None
    now = datetime(2026, 11, 20, 4, 0)
    nudges = sla_nudges(
        [{"case_id": "C-1", "status": "investigating", "opened_at": now - timedelta(hours=3), "acknowledged_at": None}],
        now,
    )
    assert {item["action"] for item in nudges} == {"nudge", "escalate"}


def test_freshness_and_verify_pr_never_merges():
    now = datetime(2026, 11, 21, 12)
    fresh = freshness_check(
        latest_classified_at=now - timedelta(hours=2),
        now=now,
        error_rate=0.01,
        spend_usd=1,
        daily_cap_usd=25,
    )
    assert fresh["ok"]
    stale = freshness_check(
        latest_classified_at=now - timedelta(hours=48),
        now=now,
        error_rate=0.2,
        spend_usd=30,
        daily_cap_usd=25,
    )
    assert not stale["ok"]
    report = verify_pr(
        {
            "tests_passed": True,
            "eval_topic_accuracy": 0.88,
            "eval_threshold": 0.85,
            "schema_diff": "signals.ticket_topics",
            "cost_estimate_usd": 0.14,
        }
    )
    assert report["status"] == "pass"
    assert report["merge"] is False
    failed = verify_pr({"tests_passed": False, "eval_topic_accuracy": 0.5, "cost_estimate_usd": 1})
    assert failed["status"] == "fail"
    assert failed["merge"] is False


def test_case_file_shape():
    card = {
        "case_id": "C-2026-1120-001",
        "metric": "checkout_conversion",
        "segment": {"platform": "android", "country": "IN"},
        "delta_pct": -11.2,
        "started_at": "2026-11-20T02:00:00Z",
        "revenue_at_risk_usd_per_hour": 8400,
        "severity": "high",
    }
    pack = {
        "hypotheses": [{"id": "H1", "claim": "Payment gateway failures on UPI", "confidence": 0.8, "evidence": ["q_a"]}],
        "capability_gap": None,
    }
    case_file = build_case_file(card, pack, [{"type": "build", "by": "data_owner", "status": "pending"}])
    for key in (
        "case_id",
        "schema_version",
        "metric",
        "segment",
        "delta_pct",
        "started_at",
        "revenue_at_risk_usd_per_hour",
        "severity",
        "hypotheses",
        "capability_gap",
        "approvals",
    ):
        assert key in case_file
    assert route_next({"capability_gap": {"id": "ticket_topics_missing"}, "hypotheses": []}) == "builder"
    assert route_next(pack) == "narrator"


def test_medallion_statements_stay_inside_the_sandbox():
    statements = medallion_statements()
    kinds = set()
    for statement in statements:
        assert_sandbox_write(statement)
        assert "MCP_DB.SIGNAL_SANDBOX" in statement.upper()
        head = " ".join(statement.lower().split())
        if head.startswith("create") and " stream " in head[:80]:
            kinds.add("stream")
        if head.startswith("create") and " task " in head[:80]:
            kinds.add("task")
        if head.startswith("create") and " procedure " in head[:80]:
            kinds.add("procedure")
        if head.startswith("alter task"):
            kinds.add("resume")
        if head.startswith("merge") or " merge into " in f" {head} ":
            kinds.add("merge")
    assert kinds == {"stream", "task", "procedure", "resume", "merge"}
    procedure = medallion_statements("procedure")[0]
    assert "MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING" in procedure.upper()
    assert "_LOADED_AT" in procedure.upper()
    assert "PIPELINE_RUNS" in procedure.upper()
    assert "GOLD_PIPELINE_LAG" in " ".join(medallion_statements("core")).upper()


def test_new_statement_types_are_sandbox_scoped():
    sandbox = SANDBOX_QUALIFIED
    assert_sandbox_write(
        f"MERGE INTO {sandbox}.SILVER_GROUPS t USING (SELECT 1 AS group_id) s ON t.group_id = s.group_id "
        "WHEN NOT MATCHED THEN INSERT (group_id) VALUES (s.group_id)"
    )
    assert_sandbox_write(
        f"CREATE OR REPLACE STREAM {sandbox}.STREAM_BRONZE_GROUPS ON TABLE {sandbox}.BRONZE_GROUPS APPEND_ONLY = TRUE"
    )
    assert_sandbox_write(
        f"CREATE OR REPLACE TASK {sandbox}.TASK_BRONZE_TO_SILVER "
        "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' "
        "OVERLAP_POLICY = NO_OVERLAP "
        "TARGET_COMPLETION_INTERVAL = '1 MINUTE' "
        f"WHEN SYSTEM$STREAM_HAS_DATA('{sandbox}.STREAM_BRONZE_GROUPS') "
        f"AS CALL {sandbox}.LOAD_STAGING(NULL)"
    )
    assert_sandbox_write(f"ALTER TASK {sandbox}.TASK_BRONZE_TO_SILVER RESUME")
    assert_sandbox_write(f"CALL {sandbox}.LOAD_STAGING('seed-20261108-20261120')")
    assert_sandbox_write(f"CALL {sandbox}.RERUN_STAGING('seed-20261108-20261120')")
    assert_sandbox_write(
        f"CREATE OR REPLACE PROCEDURE {sandbox}.LOAD_STAGING(batch VARCHAR) RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN RETURN batch; END; $$"
    )
    assert_sandbox_write(
        f"CREATE OR REPLACE PROCEDURE {sandbox}.RERUN_STAGING(batch VARCHAR) RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN CALL {sandbox}.LOAD_STAGING(batch); END; $$"
    )
    assert_sandbox_write(dynamic_table_sql("COMPUTE_WH"))
    qualified = prepare_sql(
        "MERGE INTO silver_groups t USING (SELECT group_id FROM bronze_groups WHERE _batch_id = ?) s "
        "ON t.group_id = s.group_id WHEN NOT MATCHED THEN INSERT (group_id) VALUES (s.group_id)",
        ["batch-1"],
    )
    assert f"{sandbox}.silver_groups" in qualified
    assert_sandbox_write(qualified)
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write(f"ALTER TABLE {sandbox}.SILVER_GROUPS ADD COLUMN extra VARCHAR")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write(f"ALTER TASK {sandbox}.TASK_BRONZE_TO_SILVER SUSPEND")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("CALL OTHER_DB.SIGNAL_SANDBOX.LOAD_STAGING('x')")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write(f"CREATE OR REPLACE PROCEDURE {sandbox}.OTHER_PROC() RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN RETURN 'x'; END; $$")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write(
            f"CREATE OR REPLACE PROCEDURE {sandbox}.LOAD_STAGING(batch VARCHAR, rerun BOOLEAN DEFAULT FALSE) "
            "RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN RETURN batch; END; $$"
        )
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("CREATE OR REPLACE STREAM PUBLIC.STREAM_BRONZE_GROUPS ON TABLE PUBLIC.BRONZE_GROUPS")
    with pytest.raises(ValueError):
        dynamic_table_sql("MCP_DB.PUBLIC.WH")
