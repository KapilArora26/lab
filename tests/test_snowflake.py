from __future__ import annotations

import json

import httpx
import pytest

from datetime import datetime

from signal_service.demo.generate import deploy_rows, group_rows, meta_rows, ticket_rows
from signal_service.demo.snowflake_deploy import golden_rows, load_statements
from signal_service.warehouse.snowflake import (
    SnowflakeWarehouse,
    UnsafeSQL,
    assert_read_sql,
    assert_sandbox_write,
    parse_sql_payload,
    prepare_sql,
)


def test_write_guard_allows_only_the_sandbox():
    assert_sandbox_write("CREATE SCHEMA IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX")
    assert_sandbox_write(
        "CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS (ticket_id VARCHAR)"
    )
    assert_sandbox_write("INSERT INTO MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS (ticket_id) VALUES ('t1')")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("DROP TABLE MCP_DB.PUBLIC.X")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("ALTER TABLE MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS ADD COLUMN extra VARCHAR")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("CREATE TABLE MCP_DB.PUBLIC.TICKET_TOPICS (id VARCHAR)")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("CREATE SCHEMA IF NOT EXISTS MCP_DB.PUBLIC")
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("DELETE FROM OTHER_DB.SIGNAL_SANDBOX.TICKET_TOPICS")


def test_read_guard_rejects_writes():
    assert assert_read_sql("SELECT 1 AS ok") == "SELECT 1 AS ok"
    with pytest.raises(UnsafeSQL):
        assert_read_sql("DROP TABLE MCP_DB.SIGNAL_SANDBOX.TICKET_TOPICS")
    with pytest.raises(UnsafeSQL):
        assert_read_sql("SELECT 1; SELECT 2")


def test_parser_reads_json_rpc_result_set():
    inner = {
        "result_set": {
            "data": [["CK63139", "AWS_AP_SOUTHEAST_1"]],
            "resultSetMetaData": {"rowType": [{"name": "ACCOUNT"}, {"name": "REGION"}]},
        }
    }
    payload = {"jsonrpc": "2.0", "id": "1", "result": {"content": [{"type": "text", "text": json.dumps(inner)}]}}
    assert parse_sql_payload(payload) == [{"account": "CK63139", "region": "AWS_AP_SOUTHEAST_1"}]
    assert parse_sql_payload(inner)[0]["account"] == "CK63139"


def test_adapter_sends_pat_header_and_refuses_without_token():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers["authorization"]
        seen["token_type"] = request.headers["x-snowflake-authorization-token-type"]
        body = json.loads(request.content)
        seen["tool"] = body["params"]["name"]
        seen["sql"] = body["params"]["arguments"]["sql"]
        inner = {
            "result_set": {
                "data": [[1]],
                "resultSetMetaData": {"rowType": [{"name": "OK"}]},
            }
        }
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": inner})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    warehouse = SnowflakeWarehouse(token="pat-test", url="https://example.test/mcp", http=client)
    rows = warehouse.query("SELECT 1 AS ok")
    assert rows == [{"ok": 1}]
    assert seen["auth"] == "Bearer pat-test"
    assert seen["token_type"] == "PROGRAMMATIC_ACCESS_TOKEN"
    assert seen["tool"] == "sql_read_tool"
    with pytest.raises(UnsafeSQL):
        warehouse.execute("DROP TABLE MCP_DB.PUBLIC.X")
    bare = SnowflakeWarehouse(token="", http=client)
    with pytest.raises(UnsafeSQL):
        bare.query("SELECT 1")


def test_string_literals_do_not_trip_the_write_guard():
    assert_sandbox_write(
        "INSERT INTO MCP_DB.SIGNAL_SANDBOX.SUPPORT_TICKETS (text_redacted) "
        "VALUES ('rupees stayed put; email jane.doe@example.com')"
    )
    with pytest.raises(UnsafeSQL):
        assert_sandbox_write("DELETE FROM MCP_DB.SIGNAL_SANDBOX.GROUPS; DELETE FROM MCP_DB.SIGNAL_SANDBOX.DEPLOYS")


def test_pipeline_sql_is_qualified_to_the_sandbox():
    qualified = prepare_sql(
        "SELECT count(*)::DOUBLE AS volume FROM sessions WHERE platform = ? AND ts >= ?",
        ["android", datetime(2026, 11, 20, 2, 0, 0)],
    )
    assert "FROM MCP_DB.SIGNAL_SANDBOX.sessions" in qualified
    assert "platform = 'android'" in qualified
    assert "2026-11-20 02:00:00" in qualified
    assert_read_sql(qualified)
    created = prepare_sql("CREATE SCHEMA IF NOT EXISTS signals")
    assert created == "CREATE SCHEMA IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX"
    assert_sandbox_write(created)
    table = prepare_sql("CREATE TABLE IF NOT EXISTS signals.ticket_topics (ticket_id VARCHAR)")
    assert "MCP_DB.SIGNAL_SANDBOX.ticket_topics" in table
    assert_sandbox_write(table)
    preserved = prepare_sql("SELECT 'from sessions' AS label")
    assert preserved == "SELECT 'from sessions' AS label"


def test_sandbox_migration_statements_stay_inside_the_schema():
    statements = load_statements()
    assert len(statements) >= 15
    for statement in statements:
        assert_sandbox_write(statement)
        assert "MCP_DB.SIGNAL_SANDBOX" in statement.upper()


def test_demo_rows_shared_by_duckdb_and_snowflake():
    assert len(group_rows()) == 2960
    assert len(ticket_rows()) == 92
    assert len(deploy_rows()) == 3
    assert len(meta_rows()) == 6
    assert len(golden_rows()) == 70


def test_result_types_are_coerced():
    payload = {
        "result_set": {
            "data": [["2026-11-20 02:00:00.000", "true", "42", "1.5"]],
            "resultSetMetaData": {
                "rowType": [
                    {"name": "TS", "type": "timestamp_ntz", "scale": 9},
                    {"name": "B", "type": "boolean", "scale": 0},
                    {"name": "N", "type": "fixed", "scale": 0},
                    {"name": "D", "type": "real", "scale": 0},
                ]
            },
        }
    }
    row = parse_sql_payload(payload)[0]
    assert row["ts"] == datetime(2026, 11, 20, 2, 0, 0)
    assert row["b"] is True
    assert row["n"] == 42
    assert row["d"] == 1.5


def test_executemany_batches_bound_values():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body["params"]["arguments"]["sql"])
        inner = {"result_set": {"data": [], "resultSetMetaData": {"rowType": []}}}
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": inner})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    warehouse = SnowflakeWarehouse(token="pat-test", url="https://example.test/mcp", http=client)
    warehouse.executemany(
        "INSERT INTO demo_meta (key, value) VALUES (?,?)",
        [("session_weight", "15.625"), ("seeded_payment_method", "upi")],
    )
    assert len(seen) == 1
    assert "INSERT INTO MCP_DB.SIGNAL_SANDBOX.demo_meta" in seen[0]
    assert "'session_weight'" in seen[0]
    assert "'upi'" in seen[0]
    assert "?" not in seen[0]


def test_pat_alias_is_used_when_the_primary_name_is_unset(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_PAT_TOKEN", raising=False)
    monkeypatch.setenv("SNOWFLAKE_PAT", "alias-token")
    warehouse = SnowflakeWarehouse()
    assert warehouse.token == "alias-token"
