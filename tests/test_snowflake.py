from __future__ import annotations

import json

import httpx
import pytest

from signal_service.warehouse.snowflake import (
    SnowflakeWarehouse,
    UnsafeSQL,
    assert_read_sql,
    assert_sandbox_write,
    parse_sql_payload,
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
