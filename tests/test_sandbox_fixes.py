"""Sandbox fixes for null keys, audit balance, seed reconcile, and timestamps.

The warehouse in these tests is DuckDB, the local stand-in for the Snowflake
adapter. Procedure text is checked directly. Nothing here opens a Snowflake
connection.
"""

from __future__ import annotations

import re
from datetime import datetime

from signal_service.pipeline.ddl import ensure_objects, medallion_statements
from signal_service.pipeline.ingest import Batch, ingest_batch, rerun_batch
from signal_service.pipeline.quality import remove_suspect
from signal_service.pipeline.reconcile import reconciliation_errors
from signal_service.pipeline.row_keys import procedure_row_key, row_key_payload, row_key_sql
from signal_service.pipeline.segments import canonical_countries, canonical_platforms
from signal_service.pipeline.stream_guard import stream_load_action
from signal_service.warehouse.duckdb import DuckDBWarehouse


def _row(group_id, hour, sessions=100, converted=8, attempts=12, platform="android", country="IN"):
    return (
        group_id,
        hour,
        platform,
        country,
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


def test_typo_platform_and_country_are_suspect_and_removed(tmp_path):
    assert canonical_platforms() == ("android", "ios", "web")
    assert canonical_countries() == ("IN", "US")
    batch_id = "CHAOS-20261123-0100"
    hour = datetime(2026, 11, 20, 2, 0, 0)
    batch = Batch(
        batch_id,
        "chaos",
        [
            _row("keep-ok", hour),
            _row("keep-ios", hour, platform="ios", country="US"),
            _row("keep-web", hour, platform="web", country="US"),
            _row("CHAOS-20261123-0100-m0", hour, platform="andriod", country="IND"),
            _row("CHAOS-20261123-0100-m1", hour, platform="Android", country="IN"),
            _row("CHAOS-20261123-0100-m2", hour, platform="android", country="USA"),
        ],
        [],
        [],
        [],
        [],
        [],
    )
    warehouse = DuckDBWarehouse(tmp_path / "segments.duckdb")
    try:
        assert ingest_batch(warehouse, batch)["status"] == "success"
        reasons = {
            row["group_id"]: row["dq_reason"]
            for row in warehouse.query(
                "SELECT group_id, dq_status, dq_reason FROM silver_groups WHERE _batch_id = ?",
                [batch_id],
            )
        }
        assert reasons["keep-ok"] is None
        assert reasons["keep-ios"] is None
        assert reasons["keep-web"] is None
        assert reasons["CHAOS-20261123-0100-m0"] == "invalid_platform"
        assert reasons["CHAOS-20261123-0100-m1"] == "invalid_platform"
        assert reasons["CHAOS-20261123-0100-m2"] == "invalid_country"
        assert reconciliation_errors(warehouse, batch_id) == []

        warehouse.execute(
            """
            UPDATE silver_groups
            SET dq_status = 'ok', dq_reason = NULL
            WHERE _batch_id = ? AND group_id LIKE '%-m_'
            """,
            [batch_id],
        )
        removed = remove_suspect(warehouse, batch_id, "engineering request")
        assert removed["removed"] == 3
        left = {row["group_id"] for row in warehouse.query("SELECT group_id FROM silver_groups WHERE _batch_id = ?", [batch_id])}
        assert left == {"keep-ok", "keep-ios", "keep-web"}
        assert _overlap(warehouse, batch_id) == 0
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
        second = remove_suspect(warehouse, batch_id, "engineering request")
        assert second["removed"] == 0
        assert left == {row["group_id"] for row in warehouse.query("SELECT group_id FROM silver_groups WHERE _batch_id = ?", [batch_id])}
    finally:
        warehouse.close()


def test_row_key_backfill_matches_the_procedure_and_is_idempotent(tmp_path):
    from signal_service.pipeline.row_keys import backfill_row_keys

    warehouse = DuckDBWarehouse(tmp_path / "keys.duckdb")
    try:
        ensure_objects(warehouse)
        loaded = datetime(2026, 11, 20, 2, 0, 0)
        batch_id = "seed-2026-11-08"
        warehouse.execute(
            """
            INSERT INTO silver_groups (
                group_id, hour, platform, country, channel, payment_method, sku, app_version,
                n_sessions, n_convert, n_attempt, _loaded_at, _batch_id, row_key
            ) VALUES
            (?, ?, 'android', 'IN', 'app', 'upi', 'SKU-100', '4.2.1', 10, 1, 2, ?, ?, NULL),
            (?, ?, 'ios', 'US', 'app', 'card', 'SKU-100', '5.0.2', 8, 1, 1, ?, ?, 'preset-key')
            """,
            ["g-null", loaded, loaded, batch_id, "g-set", loaded, loaded, batch_id],
        )
        warehouse.execute(
            """
            INSERT INTO silver_support_tickets (
                ticket_id, ts, platform, country, channel, payment_method, sku, app_version,
                product_area, text_redacted, _loaded_at, _batch_id
            ) VALUES (?, ?, 'android', 'IN', 'app', 'upi', 'SKU-100', '4.2.1', 'unspecified', 'UPI failed', ?, ?)
            """,
            ["t-1", loaded, loaded, batch_id],
        )
        warehouse.execute(
            """
            INSERT INTO silver_deploys (deploy_id, deployed_at, platform, app_version, note, _loaded_at, _batch_id)
            VALUES ('d-1', ?, 'android', '4.2.1', 'rollout', ?, ?)
            """,
            [loaded, loaded, batch_id],
        )
        warehouse.execute(
            """
            INSERT INTO silver_demo_meta (key, value, _loaded_at, _batch_id)
            VALUES ('aov_usd', '48', ?, ?)
            """,
            [loaded, batch_id],
        )
        warehouse.execute(
            """
            INSERT INTO silver_golden_tickets (
                example_id, text_redacted, topic, sentiment, severity, product_area, _loaded_at, _batch_id
            ) VALUES ('e-1', 'UPI failed', 'payment_failed', 'negative', 'high', 'payments', ?, ?)
            """,
            [loaded, batch_id],
        )
        warehouse.execute(
            """
            INSERT INTO silver_external_events (
                event_id, case_id, title, summary, source_url, source_type, relevance,
                observed_at, metric, provider, _loaded_at, _batch_id
            ) VALUES ('ext-1', 'C-1', 'UPI', 'Banks', 'https://example.com/a', 'web', 0.9, ?, 'checkout_conversion', 'mock', ?, ?)
            """,
            [loaded, loaded, batch_id],
        )
        warehouse.execute(
            """
            INSERT INTO silver_spend (
                logged_at, case_id, kind, estimated_usd, input_tokens, output_tokens, provider,
                _loaded_at, _batch_id
            ) VALUES (?, 'C-1', 'classify', 0, 0, 0, 'mock', ?, ?)
            """,
            [loaded, loaded, batch_id],
        )
        first = backfill_row_keys(warehouse)
        assert first["updated"] == 7
        expected = {
            "silver_groups": procedure_row_key(
                "bronze_groups",
                batch_id,
                warehouse.query("SELECT * FROM silver_groups WHERE group_id = 'g-null'")[0],
            ),
            "silver_support_tickets": procedure_row_key(
                "bronze_support_tickets",
                batch_id,
                warehouse.query("SELECT * FROM silver_support_tickets")[0],
            ),
            "silver_deploys": procedure_row_key(
                "bronze_deploys",
                batch_id,
                warehouse.query("SELECT * FROM silver_deploys")[0],
            ),
            "silver_demo_meta": procedure_row_key(
                "bronze_demo_meta",
                batch_id,
                warehouse.query("SELECT * FROM silver_demo_meta WHERE key = 'aov_usd'")[0],
            ),
            "silver_golden_tickets": procedure_row_key(
                "bronze_golden_tickets",
                batch_id,
                warehouse.query("SELECT * FROM silver_golden_tickets")[0],
            ),
            "silver_external_events": procedure_row_key(
                "bronze_external_events",
                batch_id,
                warehouse.query("SELECT * FROM silver_external_events WHERE event_id = 'ext-1'")[0],
            ),
            "silver_spend": procedure_row_key(
                "silver_spend",
                batch_id,
                warehouse.query("SELECT * FROM silver_spend WHERE case_id = 'C-1'")[0],
            ),
        }
        stored = {
            "silver_groups": warehouse.query("SELECT row_key FROM silver_groups WHERE group_id = 'g-null'")[0]["row_key"],
            "silver_support_tickets": warehouse.query("SELECT row_key FROM silver_support_tickets")[0]["row_key"],
            "silver_deploys": warehouse.query("SELECT row_key FROM silver_deploys WHERE deploy_id = 'd-1'")[0]["row_key"],
            "silver_demo_meta": warehouse.query("SELECT row_key FROM silver_demo_meta WHERE key = 'aov_usd'")[0]["row_key"],
            "silver_golden_tickets": warehouse.query("SELECT row_key FROM silver_golden_tickets WHERE example_id = 'e-1'")[0]["row_key"],
            "silver_external_events": warehouse.query("SELECT row_key FROM silver_external_events WHERE event_id = 'ext-1'")[0]["row_key"],
            "silver_spend": warehouse.query("SELECT row_key FROM silver_spend WHERE case_id = 'C-1'")[0]["row_key"],
        }
        assert stored == expected
        assert all(value for value in stored.values())
        group_row = warehouse.query("SELECT * FROM silver_groups WHERE group_id = 'g-null'")[0]
        payload = row_key_payload("bronze_groups", batch_id, group_row)
        digest = warehouse.query("SELECT sha256(?) AS digest", [payload])[0]["digest"]
        assert digest == stored["silver_groups"]
        assert warehouse.query("SELECT row_key FROM silver_groups WHERE group_id = 'g-set'")[0]["row_key"] == "preset-key"

        second = backfill_row_keys(warehouse)
        assert second["updated"] == 0
        again = warehouse.query("SELECT group_id, row_key FROM silver_groups ORDER BY group_id")
        assert {row["group_id"]: row["row_key"] for row in again} == {
            "g-null": stored["silver_groups"],
            "g-set": "preset-key",
        }

        load = medallion_statements("procedure")[0]
        remove = medallion_statements("procedure")[2]
        backfill_sql = "\n".join(medallion_statements("row_key_backfill"))
        for source in (
            "bronze_groups",
            "bronze_support_tickets",
            "bronze_deploys",
            "bronze_demo_meta",
            "bronze_golden_tickets",
            "bronze_external_events",
            "silver_spend",
        ):
            expr = row_key_sql(source)
            assert expr in backfill_sql
            assert expr in load or expr in remove
        assert row_key_sql("bronze_groups") in load
        assert row_key_sql("bronze_groups") in remove
        assert "invalid_platform" in load and "invalid_platform" in remove
        assert "invalid_country" in load and "invalid_country" in remove
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
