"""Sandbox fixes for null keys, audit balance, seed reconcile, and timestamps.

The warehouse in these tests is DuckDB, the local stand-in for the Snowflake
adapter. Procedure text is checked directly. Nothing here opens a Snowflake
connection.
"""

from __future__ import annotations

import re
from datetime import datetime

from signal_service.pipeline.ddl import medallion_statements
from signal_service.pipeline.ingest import Batch, ingest_batch, rerun_batch
from signal_service.pipeline.quality import remove_suspect
from signal_service.pipeline.reconcile import reconciliation_errors
from signal_service.pipeline.stream_guard import stream_load_action
from signal_service.warehouse.duckdb import DuckDBWarehouse


def _row(group_id, hour, sessions=100, converted=8, attempts=12):
    return (
        group_id,
        hour,
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


def _overlap(warehouse, batch_id: str) -> int:
    return int(
        warehouse.query(
            """
            SELECT COUNT(*)::INTEGER AS n
            FROM silver_groups g
            JOIN silver_rejects r
              ON r._batch_id = g._batch_id
             AND r.source_table = 'bronze_groups'
             AND r.row_key IS NOT DISTINCT FROM g.row_key
            WHERE g._batch_id = ?
            """,
            [batch_id],
        )[0]["n"]
    )


def _latest_silver(warehouse, batch_id: str) -> dict:
    return warehouse.query(
        """
        SELECT rows_in, rows_out, rows_rejected, status
        FROM pipeline_runs
        WHERE batch_id = ? AND layer = 'silver'
        ORDER BY rowid DESC
        LIMIT 1
        """,
        [batch_id],
    )[0]


def test_null_keys_stay_out_and_the_audit_balances(tmp_path):
    batch_id = "CHAOS-20261123-0100"
    hour = datetime(2026, 11, 20, 2, 0, 0)
    batch = Batch(
        batch_id,
        "chaos",
        [
            _row("keep-ok", hour, 100, 8, 12),
            _row(None, hour, 40, 4, 6),
            _row(None, hour, 55, 5, 7),
            _row("", hour, 10, 1, 2),
        ],
        [],
        [],
        [],
        [],
        [],
    )
    warehouse = DuckDBWarehouse(tmp_path / "nulls.duckdb")
    try:
        assert ingest_batch(warehouse, batch)["status"] == "success"
        assert _overlap(warehouse, batch_id) == 0
        reasons = {
            row["dq_reason"]
            for row in warehouse.query(
                "SELECT dq_reason FROM silver_groups WHERE _batch_id = ? AND dq_status = 'suspect'",
                [batch_id],
            )
        }
        assert reasons == {"null group_id"}
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id IS NULL")[0]["n"] == 2

        removed = remove_suspect(warehouse, batch_id, "engineering request")
        assert removed["removed"] == 3
        assert _overlap(warehouse, batch_id) == 0
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id IS NULL")[0]["n"] == 0
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id = ''")[0]["n"] == 0
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id = 'keep-ok'")[0]["n"] == 1
        archived = warehouse.query(
            """
            SELECT row_key
            FROM silver_rejects
            WHERE _batch_id = ? AND source_table = 'bronze_groups' AND removed_by IS NOT NULL
            """,
            [batch_id],
        )
        assert len(archived) == 3
        assert all(row["row_key"] for row in archived)
        audit = warehouse.query(
            """
            SELECT rows_in, rows_out, rows_rejected, status
            FROM pipeline_runs
            WHERE batch_id = ? AND layer = 'remove_suspect'
            """,
            [batch_id],
        )[0]
        assert audit["status"] == "success"
        assert int(audit["rows_in"]) == int(audit["rows_out"]) + int(audit["rows_rejected"])
        assert int(audit["rows_in"]) == 3
        assert reconciliation_errors(warehouse, batch_id) == []

        again = rerun_batch(warehouse, batch_id)
        assert again["status"] == "success"
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id IS NULL")[0]["n"] == 0
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id = ''")[0]["n"] == 0
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id = 'keep-ok'")[0]["n"] == 1
        assert _overlap(warehouse, batch_id) == 0
        silver = _latest_silver(warehouse, batch_id)
        assert silver["status"] == "success"
        assert int(silver["rows_in"]) == int(silver["rows_out"]) + int(silver["rows_rejected"])
        assert int(silver["rows_in"]) == 4
        assert int(silver["rows_rejected"]) == 3
        assert reconciliation_errors(warehouse, batch_id) == []

        archives = len(archived)
        second = remove_suspect(warehouse, batch_id, "engineering request")
        assert second["removed"] == 0
        assert (
            warehouse.query(
                """
                SELECT COUNT(*)::INTEGER AS n
                FROM silver_rejects
                WHERE _batch_id = ? AND removed_by IS NOT NULL
                """,
                [batch_id],
            )[0]["n"]
            == archives
        )
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_groups WHERE group_id = 'keep-ok'")[0]["n"] == 1
    finally:
        warehouse.close()


def test_out_of_range_timestamps_are_suspect_and_remove_clears_them(tmp_path):
    batch_id = "CHAOS-20261123-0100"
    batch = Batch(
        batch_id,
        "chaos",
        [
            _row("keep-ok", datetime(2026, 11, 20, 2, 0, 0)),
            _row("CHAOS-20261123-0100-f0", datetime(2027, 1, 15, 0, 0, 0), 80, 6, 10),
            _row("CHAOS-20261123-0100-f1", datetime(2027, 1, 15, 1, 0, 0), 90, 7, 11),
            _row("CHAOS-20261123-0100-f2", datetime(2027, 1, 15, 2, 0, 0), 70, 5, 9),
            _row("epoch-row", datetime(1970, 1, 1, 0, 0, 0), 30, 3, 4),
            _row("old-row", datetime(2019, 3, 10, 0, 0, 0), 30, 3, 4),
        ],
        [],
        [],
        [],
        [],
        [],
    )
    warehouse = DuckDBWarehouse(tmp_path / "times.duckdb")
    try:
        assert ingest_batch(warehouse, batch)["status"] == "success"
        reasons = {
            row["group_id"]: row["dq_reason"]
            for row in warehouse.query(
                "SELECT group_id, dq_status, dq_reason FROM silver_groups WHERE _batch_id = ?",
                [batch_id],
            )
        }
        assert reasons["keep-ok"] is None or warehouse.query(
            "SELECT dq_status FROM silver_groups WHERE group_id = 'keep-ok'"
        )[0]["dq_status"] == "ok"
        assert reasons["CHAOS-20261123-0100-f0"] == "future_timestamp"
        assert reasons["CHAOS-20261123-0100-f1"] == "future_timestamp"
        assert reasons["CHAOS-20261123-0100-f2"] == "future_timestamp"
        assert reasons["epoch-row"] == "epoch_timestamp"
        assert reasons["old-row"] == "timestamp_before_floor"
        future_sessions = warehouse.query(
            "SELECT COUNT(*)::INTEGER AS n FROM sessions WHERE ts >= TIMESTAMP '2027-01-01'"
        )[0]["n"]
        assert int(future_sessions) > 0
        assert _overlap(warehouse, batch_id) == 0

        warehouse.execute(
            """
            INSERT INTO silver_spend (
                logged_at, case_id, kind, estimated_usd, input_tokens, output_tokens, provider,
                _loaded_at, _batch_id, dq_status, dq_reason
            ) VALUES (
                TIMESTAMP '2027-01-15 00:00:00', 'C-1', 'classify', 0, 0, 0, 'mock',
                TIMESTAMP '2026-11-22 00:00:00', ?, 'ok', NULL
            )
            """,
            [batch_id],
        )
        warehouse.execute(
            """
            UPDATE silver_groups
            SET dq_status = 'ok', dq_reason = NULL
            WHERE _batch_id = ? AND group_id <> 'keep-ok'
            """,
            [batch_id],
        )
        removed = remove_suspect(warehouse, batch_id, "engineering request")
        assert removed["removed"] == 6
        left = {row["group_id"] for row in warehouse.query("SELECT group_id FROM silver_groups WHERE _batch_id = ?", [batch_id])}
        assert left == {"keep-ok"}
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM silver_spend WHERE _batch_id = ?", [batch_id])[0]["n"] == 0
        assert warehouse.query("SELECT COUNT(*)::INTEGER AS n FROM sessions WHERE ts >= TIMESTAMP '2027-01-01'")[0]["n"] == 0
        assert _overlap(warehouse, batch_id) == 0
        audit = warehouse.query(
            """
            SELECT rows_in, rows_out, rows_rejected
            FROM pipeline_runs
            WHERE batch_id = ? AND layer = 'remove_suspect'
            """,
            [batch_id],
        )[0]
        assert int(audit["rows_in"]) == int(audit["rows_out"]) + int(audit["rows_rejected"])
        assert reconciliation_errors(warehouse, batch_id) == []
        second = remove_suspect(warehouse, batch_id, "engineering request")
        assert second["removed"] == 0
    finally:
        warehouse.close()


def test_failed_audit_does_not_cause_repeated_reprocessing():
    batch_id = "CHAOS-20261123-0100"
    stream = [batch_id]
    gold: dict[str, str] = {}
    audits: list[tuple[str, str, str]] = []
    for _ in range(5):
        if not stream:
            break
        batch = stream.pop(0)
        if stream_load_action(gold.get(batch)) == "consume":
            continue
        audits.append((batch, "silver", "failed"))
        audits.append((batch, "gold", "failed"))
        gold[batch] = "failed"
        stream.append(batch)
        stream.pop(0)
    assert audits == [(batch_id, "silver", "failed"), (batch_id, "gold", "failed")]
    assert stream == []
    assert stream_load_action("failed") == "consume"
    assert stream_load_action("success") == "consume"
    assert stream_load_action(None) == "load"

    procedure = medallion_statements("procedure")[0]
    folded = " ".join(procedure.split())
    assert "IN ('success', 'failed')" in folded
    upper = procedure.upper()
    assert "ROLLBACK" not in upper
    assert upper.count("BEGIN TRANSACTION") >= 2
    drain_at = upper.rfind("STREAM_SILVER_GROUPS")
    assert drain_at > upper.find("GOLD-")
    assert "BEGIN TRANSACTION" in upper[upper.rfind("COMMIT", 0, drain_at) : drain_at]

    remove = medallion_statements("procedure")[2]
    assert "CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING" not in remove.upper()
    assert "epoch_timestamp" in remove
    assert "timestamp_before_floor" in remove
    assert "future_timestamp" in remove
    assert "2020-01-01" in remove
    assert "86400" in remove


def test_procedure_inserts_do_not_put_expressions_in_values():
    for statement in medallion_statements("procedure"):
        for match in re.finditer(r"(?is)\bvalues\s*\((.*?)\)", statement):
            body = match.group(1)
            assert "||" not in body
            assert "UUID_STRING" not in body.upper()
            assert "CONVERT_TIMEZONE" not in body.upper()
    load = medallion_statements("procedure")[0]
    assert "EQUAL_NULL(removed.row_key, flagged.row_key)" in load
    assert "EQUAL_NULL(removed.row_key, flagged.group_id)" in load
    assert "epoch_timestamp" in load
