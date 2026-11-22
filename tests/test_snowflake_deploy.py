"""Snowflake deploy stays additive and a second run exits 0.

The mock speaks the MCP JSON-RPC shape. It never opens a Snowflake connection.
"""

from __future__ import annotations

import json

import httpx
import pytest

from datetime import datetime

from signal_service.demo.generate import ANOMALY_START, group_rows
from signal_service.demo.snowflake_deploy import expansion_totals
from signal_service.pipeline.ddl import ensure_objects, medallion_statements
from signal_service.warehouse.snowflake import ColumnAlreadyExists, SnowflakeWarehouse, add_column_target

_GROUPS = group_rows()
_EXPECTED = expansion_totals(_GROUPS)
_HOUR = datetime(ANOMALY_START.year, ANOMALY_START.month, ANOMALY_START.day, ANOMALY_START.hour)
_SLICE_VOLUME = sum(int(row[8]) for row in _GROUPS if row[2] == "android" and row[3] == "IN" and row[1] == _HOUR)
_SLICE_CONVERTED = sum(int(row[9]) for row in _GROUPS if row[2] == "android" and row[3] == "IN" and row[1] == _HOUR)
_ADD = "ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS ADD COLUMN IF NOT EXISTS dq_status VARCHAR DEFAULT 'ok'"
_EXISTS = "SQL compilation error: ambiguous column name ''"


def _ok(body: dict, columns: list[str], data: list[list]) -> httpx.Response:
    inner = {
        "result_set": {
            "data": data,
            "resultSetMetaData": {
                "numRows": len(data),
                "rowType": [{"name": name} for name in columns],
            },
        }
    }
    return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": inner})


def _fail(body: dict, text: str) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "jsonrpc": "2.0",
            "id": body["id"],
            "result": {"isError": True, "content": [{"type": "text", "text": text}]},
        },
    )


def _warehouse(handler) -> SnowflakeWarehouse:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return SnowflakeWarehouse(token="pat-test", url="https://example.test/mcp", http=client)


def test_add_column_checks_the_catalog_and_is_not_sent_twice():
    columns: set[tuple[str, str]] = set()
    writes: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        tool = body["params"]["name"]
        sql = body["params"]["arguments"]["sql"]
        upper = " ".join(sql.upper().split())
        if tool == "sql_read_tool":
            assert "INFORMATION_SCHEMA.COLUMNS" in upper
            assert "SIGNAL_SANDBOX" in upper
            data = [[table, column] for table, column in sorted(columns)]
            return _ok(body, ["TABLE_NAME", "COLUMN_NAME"], data)
        target = add_column_target(sql)
        assert target is not None
        key = (target[0].upper(), target[1].upper())
        if key in columns:
            return _fail(body, _EXISTS)
        columns.add(key)
        writes.append(upper)
        return _ok(body, [], [])

    client = httpx.Client(transport=httpx.MockTransport(handler))

    def make() -> SnowflakeWarehouse:
        return SnowflakeWarehouse(token="pat-test", url="https://example.test/mcp", http=client)

    make().execute(_ADD)
    make().execute(_ADD)
    assert writes == [
        "ALTER TABLE MCP_DB.SIGNAL_SANDBOX.SILVER_GROUPS ADD COLUMN IF NOT EXISTS DQ_STATUS VARCHAR DEFAULT 'OK'"
    ]


def test_existing_column_error_is_ignored_and_other_compilation_errors_are_not():
    def empty_catalog(request: httpx.Request, *, message: str) -> httpx.Response:
        body = json.loads(request.content)
        tool = body["params"]["name"]
        if tool == "sql_read_tool":
            return _ok(body, ["TABLE_NAME", "COLUMN_NAME"], [])
        return _fail(body, message)

    ignored = _warehouse(lambda request: empty_catalog(request, message=_EXISTS))
    ignored.execute(_ADD)

    other = _warehouse(
        lambda request: empty_catalog(
            request,
            message="SQL compilation error: ambiguous column name 'DQ_STATUS'",
        )
    )
    with pytest.raises(RuntimeError) as named:
        other.execute(_ADD)
    assert not isinstance(named.value, ColumnAlreadyExists)

    syntax = _warehouse(
        lambda request: empty_catalog(request, message="SQL compilation error: syntax error unexpected DEFAULT")
    )
    with pytest.raises(RuntimeError, match="syntax error") as failed:
        syntax.execute(_ADD)
    assert not isinstance(failed.value, ColumnAlreadyExists)


def test_core_migration_runs_once_per_warehouse():
    class Recording:
        backend = "snowflake"

        def __init__(self):
            self.executed: list[str] = []

        def execute(self, sql, params=None):
            self.executed.append(" ".join(sql.split()))

        def query(self, sql, params=None):
            return [{"n": 0}]

    warehouse = Recording()
    ensure_objects(warehouse)
    first = list(warehouse.executed)
    ensure_objects(warehouse)
    second = warehouse.executed[len(first) :]
    adds = [sql for sql in first if "ADD COLUMN" in sql.upper()]
    assert len(adds) == len([sql for sql in medallion_statements("core") if "ADD COLUMN" in sql.upper()])
    assert adds
    assert any("SILVER_GROUPS" in sql.upper() and "CREATE TABLE" in sql.upper() for sql in first)
    assert not [sql for sql in second if "ADD COLUMN" in sql.upper()]
    assert not [
        sql for sql in second if "CREATE TABLE" in sql.upper() and "SILVER_" in sql.upper()
    ]
    assert not any(" DROP " in f" {sql.upper()} " or " TRUNCATE " in f" {sql.upper()} " for sql in warehouse.executed)


class _Remote:
    def __init__(self):
        self.columns: set[tuple[str, str]] = set()
        self.added: list[tuple[str, str]] = []
        self.writes: list[str] = []
        self.gold_done = False


def _answer_read(remote: _Remote, sql: str) -> tuple[list[str], list[list]]:
    upper = " ".join(sql.upper().split())
    if "INFORMATION_SCHEMA.COLUMNS" in upper:
        return ["TABLE_NAME", "COLUMN_NAME"], [[table, column] for table, column in sorted(remote.columns)]
    if "PIPELINE_RUNS" in upper and "COUNT(" in upper:
        return ["N"], [[1 if remote.gold_done else 0]]
    if "PIPELINE_RUNS" in upper:
        if not remote.gold_done:
            return ["LAYER", "STATUS", "ROWS_IN", "ROWS_OUT", "ROWS_REJECTED", "ERROR", "STARTED_AT"], []
        rows = [
            ["bronze", "success", 1, 1, 0, None, "t1"],
            ["silver", "success", 1, 1, 0, None, "t2"],
            ["gold", "success", 1, 1, 0, None, "t3"],
        ]
        return ["LAYER", "STATUS", "ROWS_IN", "ROWS_OUT", "ROWS_REJECTED", "ERROR", "STARTED_AT"], rows
    if " AS VOLUME" in upper:
        return ["VOLUME", "POSITIVES"], [[_SLICE_VOLUME, _SLICE_CONVERTED]]
    if " AS SESSIONS" in upper:
        return ["SESSIONS", "CONVERTED"], [[_EXPECTED["sessions"], _EXPECTED["converted"]]]
    if " AS PAYMENTS" in upper:
        return ["PAYMENTS", "PAYMENT_SUCCEEDED"], [[_EXPECTED["payments"], _EXPECTED["payment_succeeded"]]]
    if " AS ORDERS" in upper:
        return ["ORDERS", "AOV"], [[_EXPECTED["orders"], 48.0]]
    if " AS TICKETS" in upper:
        return ["TICKETS"], [[92]]
    if " AS TOPICS" in upper:
        return ["TOPICS"], [[73]]
    if "SILVER_REJECTS" in upper:
        return ["N"], [[0]]
    if "DQ_STATUS" in upper or "DQ_SUSPECT_ROWS" in upper or "GOLD_DATA_QUALITY" in upper:
        return ["N"], [[0]]
    if "TICKET_TOPIC" in upper:
        return ["N"], [[73]]
    if "EXTERNAL_EVENT" in upper:
        return ["N"], [[2]]
    if "COUNT(" in upper or " SUM(" in upper:
        return ["N"], [[0]]
    raise AssertionError(f"unhandled read: {upper[:400]}")


def _deploy_handler(remote: _Remote):
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        tool = body["params"]["name"]
        sql = body["params"]["arguments"]["sql"]
        upper = " ".join(sql.upper().split())
        if " DROP " in f" {upper} " or " TRUNCATE " in f" {upper} ":
            raise AssertionError(upper[:200])
        if tool == "sql_read_tool":
            columns, data = _answer_read(remote, sql)
            return _ok(body, columns, data)
        remote.writes.append(upper)
        target = add_column_target(sql)
        if target is not None:
            key = (target[0].upper(), target[1].upper())
            if key in remote.columns:
                return _fail(body, _EXISTS)
            remote.columns.add(key)
            remote.added.append(key)
        if upper.startswith("CALL ") and "LOAD_STAGING" in upper:
            remote.gold_done = True
        return _ok(body, [], [])

    return handler


def _report(text: str) -> dict:
    start = text.find("\n{")
    if start < 0:
        start = text.find("{")
    else:
        start += 1
    return json.loads(text[start:])


def test_second_deploy_is_a_noop_and_exits_zero(monkeypatch, capsys):
    remote = _Remote()
    client = httpx.Client(transport=httpx.MockTransport(_deploy_handler(remote)))

    def factory():
        return SnowflakeWarehouse(token="pat-test", url="https://example.test/mcp", http=client)

    monkeypatch.setenv("SNOWFLAKE_PAT_TOKEN", "primary-token")
    monkeypatch.setenv("SNOWFLAKE_PAT", "fallback-token")
    monkeypatch.setattr("signal_service.cli.SnowflakeWarehouse", factory)
    monkeypatch.setattr(
        "signal_service.orchestration.pipeline.run_pipeline",
        lambda *args, **kwargs: {"case_card": {"case_id": "C-2026-1120-001"}},
    )
    from signal_service.cli import main

    expected = []
    for statement in medallion_statements("core"):
        target = add_column_target(statement)
        if target is not None:
            expected.append((target[0].upper(), target[1].upper()))
    assert ("SILVER_GROUPS", "DQ_STATUS") in expected
    assert ("SILVER_REJECTS", "REMOVED_BY") in expected
    assert ("SILVER_REJECTS", "REMOVED_AT") in expected
    assert ("GOLD_HOURLY_CONVERSION", "DQ_SUSPECT_ROWS") in expected

    assert main(["deploy-snowflake"]) == 0
    first_out = capsys.readouterr().out
    assert _report(first_out)["ingest"]["status"] == "success"
    assert remote.added == expected
    silver_creates = [
        sql
        for sql in remote.writes
        if "CREATE TABLE IF NOT EXISTS" in sql and "SILVER_GROUPS" in sql and "ADD COLUMN" not in sql
    ]
    assert len(silver_creates) == 1
    added_after_first = list(remote.added)

    assert main(["deploy-snowflake"]) == 0
    second_out = capsys.readouterr().out
    report = _report(second_out)
    assert report["ingest"]["status"] == "noop"
    assert report["token_source"] == "SNOWFLAKE_PAT_TOKEN"
    assert "primary-token" not in first_out and "primary-token" not in second_out
    assert "fallback-token" not in first_out and "fallback-token" not in second_out
    assert remote.added == added_after_first
    assert not any(" DROP " in f" {sql} " or " TRUNCATE " in f" {sql} " for sql in remote.writes)
