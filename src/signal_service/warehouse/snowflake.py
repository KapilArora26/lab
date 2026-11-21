"""Optional Snowflake adapter.

Talks to a Snowflake-managed MCP server over JSON-RPC. The token is read from
SNOWFLAKE_PAT_TOKEN and sent as a bearer programmatic access token. Writes are
limited to MCP_DB.SIGNAL_SANDBOX. This module does not open a connection at
import time and does nothing when the token is absent.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any
from uuid import uuid4

import httpx

from signal_service.config import DEFAULT_SNOWFLAKE_MCP_URL, SANDBOX_QUALIFIED

_FORBIDDEN = re.compile(
    r"\b(drop|alter|truncate|undrop|grant|revoke|merge|call|copy|put|get|remove)\b",
    re.IGNORECASE,
)
_DOTTED = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\b")


class UnsafeSQL(ValueError):
    """The statement is outside the sandbox or is not the kind of SQL requested."""


def _strip(sql: str) -> str:
    without_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    without_line = re.sub(r"--.*?$", " ", without_block, flags=re.MULTILINE)
    return without_line.strip().rstrip(";").strip()


def assert_read_sql(sql: str) -> str:
    text = _strip(sql)
    if not text:
        raise UnsafeSQL("empty SQL")
    if ";" in text:
        raise UnsafeSQL("multiple statements are refused")
    if _FORBIDDEN.search(text):
        raise UnsafeSQL("statement contains a forbidden keyword")
    if not re.match(r"(?is)^(select|show|describe|desc|with)\b", text):
        raise UnsafeSQL("read path accepts SELECT, SHOW, DESCRIBE, or WITH")
    return text


def assert_sandbox_write(sql: str, sandbox: str = SANDBOX_QUALIFIED) -> str:
    text = _strip(sql)
    if not text:
        raise UnsafeSQL("empty SQL")
    if ";" in text:
        raise UnsafeSQL("multiple statements are refused")
    if _FORBIDDEN.search(text):
        raise UnsafeSQL("DROP, ALTER, and other destructive statements are refused")
    if not re.match(r"(?is)^(create|insert|update|delete)\b", text):
        raise UnsafeSQL("write path accepts CREATE, INSERT, UPDATE, or DELETE")
    upper_sandbox = sandbox.upper()
    upper = text.upper()
    if re.match(r"(?is)^create\s+schema\b", text):
        expected = f"CREATE SCHEMA IF NOT EXISTS {upper_sandbox}"
        if " ".join(upper.split()) != expected:
            raise UnsafeSQL(f"only {expected} is allowed")
        return text
    if upper_sandbox not in upper:
        raise UnsafeSQL(f"writes must be fully qualified to {sandbox}")
    scrubbed = re.sub(re.escape(upper_sandbox) + r"\.[A-Z0-9_]+", "", upper)
    scrubbed = scrubbed.replace(upper_sandbox, "")
    for match in _DOTTED.finditer(scrubbed):
        raise UnsafeSQL(
            f"statement references {match.group(0)}, outside {sandbox}"
        )
    return text


def parse_sql_payload(payload: Any) -> list[dict]:
    """Accept a raw result_set or a JSON-RPC envelope around one."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    if "result_set" in payload:
        return _rows_from_result_set(payload["result_set"])
    result = payload.get("result")
    if isinstance(result, dict):
        if "result_set" in result:
            return _rows_from_result_set(result["result_set"])
        structured = result.get("structuredContent")
        if isinstance(structured, dict):
            rows = parse_sql_payload(structured)
            if rows:
                return rows
        for block in result.get("content") or []:
            if isinstance(block, dict) and block.get("text"):
                rows = parse_sql_payload(block["text"])
                if rows:
                    return rows
    return []


def _rows_from_result_set(result_set: dict) -> list[dict]:
    metadata = result_set.get("resultSetMetaData") or {}
    names = [str(column.get("name", f"c{index}")) for index, column in enumerate(metadata.get("rowType") or [])]
    rows: list[dict] = []
    for record in result_set.get("data") or []:
        if names and len(names) == len(record):
            rows.append({name.lower(): value for name, value in zip(names, record, strict=True)})
        elif isinstance(record, dict):
            rows.append({str(key).lower(): value for key, value in record.items()})
    return rows


class SnowflakeWarehouse:
    backend = "snowflake"

    def __init__(
        self,
        *,
        token: str | None = None,
        url: str | None = None,
        sandbox: str = SANDBOX_QUALIFIED,
        http: httpx.Client | None = None,
        timeout: float = 60.0,
    ):
        self.token = token if token is not None else os.environ.get("SNOWFLAKE_PAT_TOKEN", "")
        self.url = url or os.environ.get("SNOWFLAKE_MCP_URL", DEFAULT_SNOWFLAKE_MCP_URL)
        self.sandbox = sandbox
        self.timeout = timeout
        self._http = http
        self._owns_client = http is None

    def _client(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(timeout=self.timeout)
        return self._http

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise UnsafeSQL("SNOWFLAKE_PAT_TOKEN is not set")
        return {
            "Authorization": f"Bearer {self.token}",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _call(self, tool: str, sql: str) -> list[dict]:
        body = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": "tools/call",
            "params": {"name": tool, "arguments": {"sql": sql}},
        }
        response = self._client().post(self.url, headers=self._headers(), json=body)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("error"):
            message = payload["error"].get("message", payload["error"])
            raise RuntimeError(f"snowflake mcp error: {message}")
        return parse_sql_payload(payload)

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        if params:
            raise UnsafeSQL("the Snowflake adapter does not bind parameters; pass literal SQL")
        return self._call("sql_read_tool", assert_read_sql(sql))

    def execute(self, sql: str, params: list | None = None) -> None:
        if params:
            raise UnsafeSQL("the Snowflake adapter does not bind parameters; pass literal SQL")
        self._call("sql_write_tool", assert_sandbox_write(sql, self.sandbox))

    def ensure_sandbox(self) -> None:
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {self.sandbox}")

    def close(self) -> None:
        if self._owns_client and self._http is not None:
            self._http.close()
