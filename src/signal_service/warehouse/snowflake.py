"""Optional Snowflake adapter.

Talks to a Snowflake-managed MCP server over JSON-RPC. The token is read from
SNOWFLAKE_PAT_TOKEN, or from SNOWFLAKE_PAT when that name is the one injected
into the environment. Writes are limited to MCP_DB.SIGNAL_SANDBOX. This module
does not open a connection at import time and does nothing when the token is
absent.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

import httpx

from signal_service.config import DEFAULT_SNOWFLAKE_MCP_URL, SANDBOX_QUALIFIED

# ALTER, MERGE, and CALL are not blanket-forbidden. ALTER is accepted only as
# ALTER TASK <sandbox>.<task> RESUME (tasks are created suspended). MERGE and
# CALL are accepted only as sandbox-scoped statements; see assert_sandbox_write.
_FORBIDDEN = re.compile(
    r"\b(drop|truncate|undrop|grant|revoke|copy|put|get|remove)\b",
    re.IGNORECASE,
)
_STRING = re.compile(r"'(?:''|[^'])*'")
_CHAIN = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*(?:\s*\.\s*[A-Za-z_][A-Za-z0-9_]*)+")
_TABLE_REF = re.compile(
    r"(?i)\b(from|join|into|update|table(?:\s+if\s+not\s+exists)?)\s+([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\.)"
)
_BANNED_QUALIFIERS = {"PUBLIC", "INFORMATION_SCHEMA", "SIGNALS", "MCP"}
_KNOWN_TABLES = {
    "GROUPS",
    "SESSIONS",
    "PAYMENTS",
    "ORDERS",
    "SUPPORT_TICKETS",
    "DEPLOYS",
    "DEMO_META",
    "GOLDEN_TICKETS",
    "TICKET_TOPICS",
    "EXTERNAL_EVENTS",
    "SPEND_LOG",
    "MEMORY_CASES",
    "MEMORY_CALENDAR",
    "MEMORY_CAPABILITIES",
    "MEMORY_PREFERENCES",
    "MEMORY_LESSONS",
    "BRONZE_GROUPS",
    "BRONZE_SUPPORT_TICKETS",
    "BRONZE_DEPLOYS",
    "BRONZE_DEMO_META",
    "BRONZE_GOLDEN_TICKETS",
    "BRONZE_EXTERNAL_EVENTS",
    "SILVER_GROUPS",
    "SILVER_SESSIONS",
    "SILVER_PAYMENTS",
    "SILVER_ORDERS",
    "SILVER_SUPPORT_TICKETS",
    "SILVER_DEPLOYS",
    "SILVER_DEMO_META",
    "SILVER_GOLDEN_TICKETS",
    "SILVER_TICKET_TOPICS",
    "SILVER_EXTERNAL_EVENTS",
    "SILVER_SPEND",
    "SILVER_REJECTS",
    "GOLD_HOURLY_CONVERSION",
    "GOLD_REVENUE_AT_RISK",
    "GOLD_CASE_FACTS",
    "GOLD_TICKET_TOPICS",
    "GOLD_EXTERNAL_EVENTS",
    "GOLD_DAILY_SPEND",
    "GOLD_HEALTH",
    "GOLD_PIPELINE_LAG",
    "PIPELINE_RUNS",
}
_RETRY_STATUS = {429, 502, 503, 504}


class UnsafeSQL(ValueError):
    """The statement is outside the sandbox or is not the kind of SQL requested."""


class ResultTruncated(RuntimeError):
    """sql_read_tool returned a partial page. Callers must not merge it."""


class SnowflakeIPBlocked(RuntimeError):
    """Snowflake rejected the call because of a network policy."""


def _mask_strings(sql: str) -> str:
    return _STRING.sub("''", sql)


def _strip(sql: str) -> str:
    """Drop comments that sit outside string literals, then a trailing semicolon."""
    out: list[str] = []
    index = 0
    length = len(sql)
    in_string = False
    while index < length:
        char = sql[index]
        if in_string:
            if char == "'" and index + 1 < length and sql[index + 1] == "'":
                out.append("''")
                index += 2
                continue
            out.append(char)
            if char == "'":
                in_string = False
            index += 1
            continue
        if char == "'":
            in_string = True
            out.append(char)
            index += 1
            continue
        if char == "-" and index + 1 < length and sql[index + 1] == "-":
            index += 2
            while index < length and sql[index] != "\n":
                index += 1
            out.append(" ")
            continue
        if char == "/" and index + 1 < length and sql[index + 1] == "*":
            end = sql.find("*/", index + 2)
            index = length if end < 0 else end + 2
            out.append(" ")
            continue
        out.append(char)
        index += 1
    text = "".join(out).strip()
    if text.endswith(";"):
        text = text[:-1].strip()
    return text


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        stamp = value
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(timezone.utc).replace(tzinfo=None)
        return f"'{stamp.strftime('%Y-%m-%d %H:%M:%S')}'::TIMESTAMP_NTZ"
    if isinstance(value, date):
        return f"'{value.isoformat()}'::DATE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise UnsafeSQL("non-finite number")
        return format(value, ".15g")
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise UnsafeSQL(f"unsupported parameter type {type(value).__name__}")


def bind_params(sql: str, params: list | None) -> str:
    values = list(params or [])
    if not values and "?" not in sql:
        return sql
    out: list[str] = []
    index = 0
    used = 0
    length = len(sql)
    in_string = False
    while index < length:
        char = sql[index]
        if in_string:
            if char == "'" and index + 1 < length and sql[index + 1] == "'":
                out.append("''")
                index += 2
                continue
            out.append(char)
            if char == "'":
                in_string = False
            index += 1
            continue
        if char == "'":
            in_string = True
            out.append(char)
            index += 1
            continue
        if char == "?":
            if used >= len(values):
                raise UnsafeSQL("not enough SQL parameters")
            out.append(_literal(values[used]))
            used += 1
            index += 1
            continue
        out.append(char)
        index += 1
    if used != len(values):
        raise UnsafeSQL("too many SQL parameters")
    return "".join(out)


def _rewrite_code(sql: str, replacer) -> str:
    parts: list[str] = []
    cursor = 0
    for match in _STRING.finditer(sql):
        parts.append(replacer(sql[cursor : match.start()]))
        parts.append(match.group(0))
        cursor = match.end()
    parts.append(replacer(sql[cursor:]))
    return "".join(parts)


def rewrite_for_sandbox(sql: str, sandbox: str = SANDBOX_QUALIFIED) -> str:
    """Map DuckDB names onto the sandbox without touching string literals."""

    def replacer(code: str) -> str:
        code = re.sub(
            r"(?i)\bcreate\s+schema\s+if\s+not\s+exists\s+signals\b",
            f"CREATE SCHEMA IF NOT EXISTS {sandbox}",
            code,
        )
        code = re.sub(
            r"(?i)\bsignals\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)",
            lambda match: f"{sandbox}.{match.group(1)}",
            code,
        )

        def qualify(match: re.Match[str]) -> str:
            name = match.group(2)
            if name.upper() not in _KNOWN_TABLES:
                return match.group(0)
            return f"{match.group(1)} {sandbox}.{name}"

        return _TABLE_REF.sub(qualify, code)

    return _rewrite_code(sql, replacer)


def prepare_sql(sql: str, params: list | None = None, sandbox: str = SANDBOX_QUALIFIED) -> str:
    return rewrite_for_sandbox(bind_params(_strip(sql), params), sandbox)


def _mask_dollar(sql: str) -> str:
    """Hide procedure and task bodies so semicolons inside $$ are not splits."""
    return re.sub(r"\$\$.*?\$\$", "$$", sql, flags=re.DOTALL)


def _top_level_semicolon(sql: str) -> bool:
    masked = _mask_strings(_mask_dollar(sql))
    return ";" in masked


def _chains(sql: str) -> list[list[str]]:
    found: list[list[str]] = []
    for match in _CHAIN.finditer(_mask_strings(sql)):
        parts = [part.upper() for part in re.split(r"\s*\.\s*", match.group(0))]
        found.append(parts)
    return found


def _chain_allowed(chain: list[str], sandbox_parts: list[str]) -> bool:
    if chain == sandbox_parts:
        return True
    if len(chain) == 3 and chain[:2] == sandbox_parts:
        return True
    if len(chain) >= 3:
        return False
    if len(chain) == 2:
        left = chain[0]
        if left == sandbox_parts[0] or left.endswith("_DB") or left in _BANNED_QUALIFIERS:
            return False
        return True
    return True


def _assert_no_foreign_refs(sql: str, sandbox: str) -> None:
    sandbox_parts = sandbox.upper().split(".")
    for chain in _chains(sql):
        if not _chain_allowed(chain, sandbox_parts):
            raise UnsafeSQL(f"statement references {'.'.join(chain)}, outside {sandbox}")


def assert_read_sql(sql: str) -> str:
    text = _strip(sql)
    guarded = _mask_strings(text)
    if not text:
        raise UnsafeSQL("empty SQL")
    if _top_level_semicolon(text):
        raise UnsafeSQL("multiple statements are refused")
    if _FORBIDDEN.search(guarded):
        raise UnsafeSQL("statement contains a forbidden keyword")
    if not re.match(r"(?is)^(select|show|describe|desc|with)\b", text):
        raise UnsafeSQL("read path accepts SELECT, SHOW, DESCRIBE, or WITH")
    return text


def assert_sandbox_write(sql: str, sandbox: str = SANDBOX_QUALIFIED) -> str:
    text = _strip(sql)
    guarded = _mask_strings(text)
    if not text:
        raise UnsafeSQL("empty SQL")
    if _top_level_semicolon(text):
        raise UnsafeSQL("multiple statements are refused")
    if _FORBIDDEN.search(guarded):
        raise UnsafeSQL("DROP, TRUNCATE, GRANT, and other destructive statements are refused")
    if not re.match(r"(?is)^(create|insert|update|delete|merge|call|alter)\b", text):
        raise UnsafeSQL(
            "write path accepts CREATE, INSERT, UPDATE, DELETE, MERGE, CALL, or ALTER TASK RESUME"
        )
    upper_sandbox = sandbox.upper()
    upper = " ".join(text.upper().split())
    if re.match(r"(?is)^create\s+schema\b", text):
        expected = f"CREATE SCHEMA IF NOT EXISTS {upper_sandbox}"
        if upper != expected:
            raise UnsafeSQL(f"only {expected} is allowed")
        return text
    if upper_sandbox not in upper:
        raise UnsafeSQL(f"writes must be fully qualified to {sandbox}")
    _assert_created_object_qualified(text, sandbox)
    _assert_alter_or_call(text, sandbox)
    _assert_no_foreign_refs(text, sandbox)
    return text


def _assert_created_object_qualified(text: str, sandbox: str) -> None:
    match = re.match(
        r"(?is)^create\s+(?:or\s+replace\s+)?(?:stream|task|dynamic\s+table|procedure)\s+"
        r"(?:if\s+not\s+exists\s+)?([A-Za-z0-9_.]+)",
        text,
    )
    if match is None:
        return
    name = match.group(1)
    if not name.upper().startswith(sandbox.upper() + "."):
        raise UnsafeSQL(f"{name} must be created inside {sandbox}")
    if re.match(r"(?is)^create\s+(?:or\s+replace\s+)?procedure\b", text):
        if f"{sandbox.upper()}.LOAD_STAGING" not in " ".join(text.upper().split()):
            raise UnsafeSQL(f"only procedure {sandbox}.LOAD_STAGING may be created")


def _assert_alter_or_call(text: str, sandbox: str) -> None:
    collapsed = " ".join(text.split())
    if re.match(r"(?is)^alter\b", collapsed):
        pattern = rf"(?is)^alter\s+task\s+{re.escape(sandbox)}\.[A-Za-z_][A-Za-z0-9_]*\s+resume$"
        if not re.match(pattern, collapsed):
            raise UnsafeSQL(f"only ALTER TASK {sandbox}.<task> RESUME is allowed")
    if re.match(r"(?is)^call\b", collapsed):
        pattern = rf"(?is)^call\s+{re.escape(sandbox)}\.LOAD_STAGING\s*\("
        if not re.match(pattern, collapsed):
            raise UnsafeSQL(f"only CALL {sandbox}.LOAD_STAGING is allowed")


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1]
    if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return datetime.fromtimestamp(float(text), timezone.utc).replace(tzinfo=None)
    text = re.sub(r"[+-]\d{2}:?\d{2}$", "", text)
    return datetime.fromisoformat(text)


def _coerce(value: Any, type_name: str | None, scale: int | None) -> Any:
    if value is None or not type_name:
        return value
    kind = str(type_name).lower()
    if kind.startswith("timestamp") or kind in {"date", "time"}:
        try:
            return _parse_timestamp(value)
        except ValueError:
            return value
    if kind == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).lower() == "true"
    if kind in {"real", "float", "double", "dec", "decimal"}:
        return float(value)
    if kind == "fixed":
        if scale and int(scale) > 0:
            return float(value)
        text = str(value)
        if "." in text:
            return float(text)
        return int(text)
    return value


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
    columns = list(metadata.get("rowType") or [])
    names = [str(column.get("name", f"c{index}")) for index, column in enumerate(columns)]
    types = [column.get("type") for column in columns]
    scales = [column.get("scale") for column in columns]
    rows: list[dict] = []
    for record in result_set.get("data") or []:
        if names and isinstance(record, list) and len(names) == len(record):
            coerced = {}
            for index, (name, value) in enumerate(zip(names, record, strict=True)):
                type_name = types[index] if index < len(types) else None
                scale = scales[index] if index < len(scales) else None
                coerced[name.lower()] = _coerce(value, type_name, scale)
            rows.append(coerced)
        elif isinstance(record, dict):
            rows.append({str(key).lower(): value for key, value in record.items()})
    return rows


def _looks_like_ip_block(message: str) -> bool:
    text = message.lower()
    if "ip" not in text and "network policy" not in text:
        return False
    markers = (
        "not allowed",
        "blocked",
        "network policy",
        "allow list",
        "allowlist",
        "whitelist",
        "incoming request",
    )
    return any(marker in text for marker in markers)


def _error_text(result: dict) -> str:
    chunks: list[str] = []
    for block in result.get("content") or []:
        if isinstance(block, dict) and block.get("text"):
            chunks.append(str(block["text"]))
    if chunks:
        return " ".join(chunks)
    return str(result.get("message") or "snowflake tool error")


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if text.lstrip("-").isdigit():
        return int(text)
    return None


def _parse_embedded(value: str) -> Any | None:
    text = value.strip()
    if not text or text[0] not in "{[":
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _collect_result_sets(payload: Any) -> list[dict]:
    found: list[dict] = []
    seen: set[int] = set()

    def add(node: dict) -> None:
        if id(node) in seen:
            return
        if "data" in node and ("resultSetMetaData" in node or "numRows" in node):
            seen.add(id(node))
            found.append(node)
        nested = node.get("result_set")
        if isinstance(nested, dict):
            add(nested)

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            add(node)
            for key, value in node.items():
                if key == "data":
                    continue
                if isinstance(value, str):
                    embedded = _parse_embedded(value)
                    if embedded is not None:
                        walk(embedded)
                    continue
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return found


def _truncated_status(payload: Any) -> str | None:
    def walk(node: Any) -> str | None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "data":
                    continue
                found = walk(value)
                if found:
                    return found
            return None
        if isinstance(node, list):
            for item in node:
                found = walk(item)
                if found:
                    return found
            return None
        if isinstance(node, str):
            embedded = _parse_embedded(node)
            if embedded is not None:
                return walk(embedded)
            if "truncat" in node.lower() and len(node) < 800:
                return node
        return None

    return walk(payload)


def assert_result_complete(payload: Any) -> None:
    """Raise when a tool result is a partial page.

    ``sql_read_tool`` has returned ``numRows`` for the full query while ``data``
    held only the first slice, with status text ``Result truncated to fit
    tool-output budget``. Merging that slice drops the tail and still looks
    successful.
    """
    status = _truncated_status(payload)
    if status:
        raise ResultTruncated(status[:500])
    for result_set in _collect_result_sets(payload):
        data = result_set.get("data")
        if not isinstance(data, list):
            continue
        meta = result_set.get("resultSetMetaData") if isinstance(result_set.get("resultSetMetaData"), dict) else {}
        num_rows = _as_int(meta.get("numRows"))
        if num_rows is None:
            num_rows = _as_int(result_set.get("numRows"))
        if num_rows is not None and len(data) < num_rows:
            raise ResultTruncated(f"result page has {len(data)} rows and numRows is {num_rows}")


def token_from_env() -> str:
    return os.environ.get("SNOWFLAKE_PAT_TOKEN") or os.environ.get("SNOWFLAKE_PAT", "")


def token_source() -> str:
    if os.environ.get("SNOWFLAKE_PAT_TOKEN"):
        return "SNOWFLAKE_PAT_TOKEN"
    if os.environ.get("SNOWFLAKE_PAT"):
        return "SNOWFLAKE_PAT"
    return "missing"


class SnowflakeWarehouse:
    backend = "snowflake"

    def __init__(
        self,
        *,
        token: str | None = None,
        url: str | None = None,
        sandbox: str = SANDBOX_QUALIFIED,
        http: httpx.Client | None = None,
        timeout: float = 180.0,
    ):
        self.token = token if token is not None else token_from_env()
        self.url = url or os.environ.get("SNOWFLAKE_MCP_URL", DEFAULT_SNOWFLAKE_MCP_URL)
        self.sandbox = sandbox
        self.timeout = timeout
        self._http = http
        self._owns_client = http is None

    def _client(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(timeout=httpx.Timeout(self.timeout, connect=30.0))
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

    def _redact(self, message: str) -> str:
        redacted = message
        for name in ("SNOWFLAKE_PAT_TOKEN", "SNOWFLAKE_PAT"):
            secret = os.environ.get(name)
            if secret:
                redacted = redacted.replace(secret, "[redacted]")
        if self.token:
            redacted = redacted.replace(self.token, "[redacted]")
        return _mask_strings(redacted)

    def _fail(self, message: str) -> None:
        safe = self._redact(message)[:800]
        if _looks_like_ip_block(safe):
            raise SnowflakeIPBlocked(safe)
        raise RuntimeError(f"snowflake mcp error: {safe}")

    def _post(self, tool: str, sql: str) -> dict:
        body = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": "tools/call",
            "params": {"name": tool, "arguments": {"sql": sql}},
        }
        delay = 1.0
        last_status = None
        for attempt in range(3):
            try:
                response = self._client().post(self.url, headers=self._headers(), json=body)
            except httpx.TransportError:
                if attempt == 2:
                    self._fail("snowflake mcp request failed after retries")
                time.sleep(delay)
                delay *= 2
                continue
            last_status = response.status_code
            if response.status_code in _RETRY_STATUS and attempt < 2:
                time.sleep(delay)
                delay *= 2
                continue
            text = response.text[:2000]
            if _looks_like_ip_block(text):
                self._fail(text)
            try:
                payload = response.json()
            except json.JSONDecodeError:
                self._fail(f"http {response.status_code} non-json response")
            if response.status_code >= 400:
                message = ""
                if isinstance(payload, dict) and payload.get("error"):
                    err = payload["error"]
                    message = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                self._fail(message or f"http {response.status_code}")
            return payload if isinstance(payload, dict) else {}
        self._fail(f"snowflake mcp http {last_status} after retries")
        return {}

    def _call(self, tool: str, sql: str) -> list[dict]:
        payload = self._post(tool, sql)
        assert_result_complete(payload)
        if payload.get("error"):
            err = payload["error"]
            message = err.get("message", err) if isinstance(err, dict) else err
            self._fail(str(message))
        result = payload.get("result")
        if isinstance(result, dict) and result.get("isError"):
            self._fail(_error_text(result))
        return parse_sql_payload(payload)

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        statement = prepare_sql(sql, params, self.sandbox)
        return self._call("sql_read_tool", assert_read_sql(statement))

    def execute(self, sql: str, params: list | None = None) -> None:
        statement = prepare_sql(sql, params, self.sandbox)
        self._call("sql_write_tool", assert_sandbox_write(statement, self.sandbox))

    def executemany(self, sql: str, rows: list[tuple], *, batch_size: int = 200) -> None:
        if not rows:
            return
        template = rewrite_for_sandbox(_strip(sql), self.sandbox)
        match = re.search(r"(?is)^(.*?)\bvalues\s*(\([^;]*\))\s*$", template)
        if match is None:
            for row in rows:
                self.execute(sql, list(row))
            return
        prefix = match.group(1).rstrip() + " VALUES"
        placeholders = match.group(2)
        for start in range(0, len(rows), batch_size):
            chunk = rows[start : start + batch_size]
            values = ", ".join(bind_params(placeholders, list(row)) for row in chunk)
            statement = f"{prefix} {values}"
            self._call("sql_write_tool", assert_sandbox_write(statement, self.sandbox))

    def ensure_sandbox(self) -> None:
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {self.sandbox}")

    def close(self) -> None:
        if self._owns_client and self._http is not None:
            self._http.close()
            self._http = None
