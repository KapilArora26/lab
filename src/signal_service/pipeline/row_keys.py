"""One hash definition for silver ``row_key``.

Snowflake procedures hash with ``SHA2(payload, 256)``. The payload is the
source name, the batch id, and the business columns the procedures already
hash, joined by ``CHR(31)``. Null and empty string stay distinct. This module
builds that SQL expression and the same payload in Python, so a backfill and
a procedure call produce the same key.

``bronze_row_key`` in ``signal_service.hashing`` is the DuckDB load identity.
It is a different encoding. The procedure hash is the one stored by
``LOAD_STAGING`` and ``REMOVE_SUSPECT``.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone

from signal_service.pipeline.segments import canonical_countries, canonical_platforms, sql_in_list

# kind is string, timestamp, or number. The column list is the expression the
# procedures already use, including the shorter ticket, deploy, golden,
# external, and spend hashes.
ROW_KEY_SOURCES: dict[str, tuple[tuple[str, str], ...]] = {
    "bronze_groups": (
        ("group_id", "string"),
        ("hour", "timestamp"),
        ("platform", "string"),
        ("country", "string"),
        ("channel", "string"),
        ("payment_method", "string"),
        ("sku", "string"),
        ("app_version", "string"),
        ("n_sessions", "number"),
        ("n_convert", "number"),
        ("n_attempt", "number"),
    ),
    "bronze_support_tickets": (
        ("ticket_id", "string"),
        ("ts", "timestamp"),
        ("text_redacted", "string"),
    ),
    "bronze_deploys": (("deploy_id", "string"),),
    "bronze_demo_meta": (
        ("key", "string"),
        ("value", "string"),
    ),
    "bronze_golden_tickets": (("example_id", "string"),),
    "bronze_external_events": (("event_id", "string"),),
    "silver_spend": (
        ("logged_at", "timestamp"),
        ("case_id", "string"),
        ("kind", "string"),
    ),
}

SILVER_ROW_KEY_TABLES: tuple[tuple[str, str], ...] = (
    ("silver_groups", "bronze_groups"),
    ("silver_support_tickets", "bronze_support_tickets"),
    ("silver_deploys", "bronze_deploys"),
    ("silver_demo_meta", "bronze_demo_meta"),
    ("silver_golden_tickets", "bronze_golden_tickets"),
    ("silver_external_events", "bronze_external_events"),
    ("silver_spend", "silver_spend"),
)

_ROWKEY = re.compile(r"@@ROWKEY\s+([a-z0-9_]+)(?:\s+([A-Za-z_][A-Za-z0-9_]*))?@@")


def row_key_sql(source: str, qualifier: str = "") -> str:
    """Snowflake ``SHA2`` expression for one source.

    ``qualifier`` is a table alias such as ``src``. The column names are the
    ones on the silver or bronze row the procedures already read.
    """
    fields = ROW_KEY_SOURCES[source]
    prefix = f"{qualifier}." if qualifier else ""
    parts = [f"'{source}'", f"COALESCE({prefix}_batch_id, '')"]
    for column, kind in fields:
        name = f"{prefix}{column}"
        if kind == "timestamp":
            rendered = f"TO_VARCHAR({name}, 'YYYY-MM-DD HH24:MI:SS')"
        elif kind == "number":
            rendered = f"TO_VARCHAR({name})"
        else:
            rendered = name
        parts.append(f"IFF({name} IS NULL, '0', '1' || {rendered})")
    body = " || CHR(31) || ".join(parts)
    return f"SHA2({body}, 256)"


def row_key_payload(source: str, batch_id: str | None, row: dict) -> str:
    """Payload bytes that ``SHA2(payload, 256)`` hashes."""
    fields = ROW_KEY_SOURCES[source]
    parts = [source, "" if batch_id is None else str(batch_id)]
    parts.extend(_part(row.get(column), kind) for column, kind in fields)
    return "\x1f".join(parts)


def procedure_row_key(source: str, batch_id: str | None, row: dict) -> str:
    """Lowercase hex SHA-256, the value Snowflake ``SHA2(..., 256)`` returns."""
    payload = row_key_payload(source, batch_id, row)
    return hashlib.sha256(payload.encode()).hexdigest()


def render_medallion_sql(raw: str) -> str:
    """Fill allowed-value lists and row-key expressions before the file is split."""
    rendered = raw.replace("@@ALLOWED_PLATFORMS@@", sql_in_list(canonical_platforms()))
    rendered = rendered.replace("@@ALLOWED_COUNTRIES@@", sql_in_list(canonical_countries()))

    def replace(match: re.Match[str]) -> str:
        source = match.group(1)
        if source not in ROW_KEY_SOURCES:
            raise ValueError(f"unknown row_key source {source}")
        return row_key_sql(source, match.group(2) or "")

    rendered = _ROWKEY.sub(replace, rendered)
    if "@@" in rendered:
        raise ValueError("medallion SQL still has an unfilled @@ placeholder")
    return rendered


def backfill_row_keys(warehouse) -> dict:
    """Set ``row_key`` where it is null. A second call changes nothing.

    Snowflake runs the same ``UPDATE`` the migration ships. DuckDB computes
    the procedure hash in process. A key that is already set is left alone.
    """
    if getattr(warehouse, "backend", "duckdb") == "snowflake":
        from signal_service.pipeline.ddl import medallion_statements

        statements = medallion_statements("row_key_backfill")
        for statement in statements:
            warehouse.execute(statement)
        return {"backend": "snowflake", "statements": len(statements)}
    updated = 0
    for table, source in SILVER_ROW_KEY_TABLES:
        rows = warehouse.query(f"SELECT rowid AS _rk_rowid, * FROM {table} WHERE row_key IS NULL")
        for row in rows:
            key = procedure_row_key(source, row.get("_batch_id"), row)
            warehouse.execute(
                f"UPDATE {table} SET row_key = ? WHERE rowid = ? AND row_key IS NULL",
                [key, row["_rk_rowid"]],
            )
            updated += 1
    return {"backend": "duckdb", "updated": updated}


def _part(value: object, kind: str) -> str:
    if value is None:
        return "0"
    if kind == "timestamp":
        return "1" + _timestamp(value)
    if kind == "number":
        return "1" + _number(value)
    return "1" + str(value)


def _timestamp(value: object) -> str:
    if isinstance(value, datetime):
        stamped = value
        if stamped.tzinfo is not None:
            stamped = stamped.astimezone(timezone.utc).replace(tzinfo=None)
        return stamped.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip().replace("Z", "")
    if not text:
        return ""
    return datetime.fromisoformat(text[:19]).strftime("%Y-%m-%d %H:%M:%S")


def _number(value: object) -> str:
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)
