"""Move one batch from bronze to silver to gold and write PIPELINE_RUNS."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

from signal_service.safety.redaction import pii_kinds


class PipelineError(RuntimeError):
    def __init__(self, layer: str, message: str):
        super().__init__(message)
        self.layer = layer
        self.message = message


def clock() -> datetime:
    """UTC wall time for lineage columns.

    Stored naive so Snowflake ``TIMESTAMP_NTZ`` and DuckDB ``TIMESTAMP`` keep
    the UTC clock. A session in America/Los_Angeles must not write Pacific
    local time into ``_ingested_at``, ``_loaded_at``, or ``PIPELINE_RUNS``.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def write_run(
    warehouse,
    *,
    batch_id: str,
    layer: str,
    started_at: datetime,
    status: str,
    rows_in: int = 0,
    rows_out: int = 0,
    rows_rejected: int = 0,
    error: str | None = None,
    finished_at: datetime | None = None,
) -> None:
    warehouse.execute(
        """
        INSERT INTO pipeline_runs (
            run_id, batch_id, layer, started_at, finished_at, status,
            rows_in, rows_out, rows_rejected, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(uuid4()),
            batch_id,
            layer,
            started_at,
            finished_at or clock(),
            status,
            int(rows_in),
            int(rows_out),
            int(rows_rejected),
            None if not error else error[:500],
        ],
    )


def _layer(warehouse, batch_id: str, layer: str, fn):
    started = clock()
    try:
        stats = fn()
    except Exception as exc:
        message = str(exc) or exc.__class__.__name__
        write_run(
            warehouse,
            batch_id=batch_id,
            layer=layer,
            started_at=started,
            status="failed",
            error=message,
        )
        raise PipelineError(layer, message) from exc
    rows_in = int(stats.get("rows_in", 0) or 0)
    rows_out = int(stats.get("rows_out", 0) or 0)
    rows_rejected = int(stats.get("rows_rejected", 0) or 0)
    if rows_in != rows_out + rows_rejected:
        message = f"rows_in {rows_in} != rows_out {rows_out} + rows_rejected {rows_rejected}"
        write_run(
            warehouse,
            batch_id=batch_id,
            layer=layer,
            started_at=started,
            status="failed",
            rows_in=rows_in,
            rows_out=rows_out,
            rows_rejected=rows_rejected,
            error=message,
        )
        raise PipelineError(layer, message)
    write_run(
        warehouse,
        batch_id=batch_id,
        layer=layer,
        started_at=started,
        status="success",
        rows_in=rows_in,
        rows_out=rows_out,
        rows_rejected=rows_rejected,
    )
    return stats


def propagate(warehouse, batch_id: str) -> dict:
    silver = _layer(warehouse, batch_id, "silver", lambda: load_silver(warehouse, batch_id, clock()))
    gold = _layer(warehouse, batch_id, "gold", lambda: load_gold(warehouse, batch_id, clock()))
    return {"silver": silver, "gold": gold}


def group_reason(row: dict, seen: set[str]) -> str | None:
    group_id = row.get("group_id")
    if not group_id:
        return "null group_id"
    if group_id in seen:
        return "duplicate group_id"
    seen.add(str(group_id))
    if row.get("hour") is None:
        return "null hour"
    sessions = row.get("n_sessions")
    converted = row.get("n_convert")
    attempts = row.get("n_attempt")
    if sessions is None or converted is None or attempts is None:
        return "null metric"
    sessions = int(sessions)
    converted = int(converted)
    attempts = int(attempts)
    if sessions < 0 or converted < 0 or attempts < 0:
        return "negative_count"
    if converted > sessions:
        return "conversions_gt_sessions"
    if converted > attempts:
        return "conversions_gt_attempts"
    if not row.get("platform") or not row.get("country"):
        return "null segment"
    return None


def ticket_reason(row: dict, seen: set[str]) -> str | None:
    ticket_id = row.get("ticket_id")
    if not ticket_id:
        return "null ticket_id"
    if ticket_id in seen:
        return "duplicate ticket_id"
    seen.add(str(ticket_id))
    text = row.get("text_redacted") or ""
    if not str(text).strip():
        return "null text"
    kinds = pii_kinds(str(text))
    if kinds:
        return "pii:" + ",".join(kinds)
    return None


def _simple_reason(row: dict, key: str, seen: set[str], label: str) -> str | None:
    value = row.get(key)
    if not value:
        return f"null {label}"
    if value in seen:
        return f"duplicate {label}"
    seen.add(str(value))
    return None


def _json_row(row: dict) -> str:
    cleaned: dict = {}
    for key, value in row.items():
        if str(key).startswith("_"):
            continue
        if isinstance(value, datetime):
            value = value.isoformat()
        if isinstance(value, str) and pii_kinds(value):
            value = "[redacted]"
        cleaned[key] = value
    payload = json.dumps(cleaned, default=str, sort_keys=True)
    if pii_kinds(payload):
        cleaned = {key: value for key, value in cleaned.items() if not isinstance(value, str)}
        cleaned["text_redacted"] = "[redacted]"
        payload = json.dumps(cleaned, default=str, sort_keys=True)
    return payload


def _merge(warehouse, table: str, columns: list[str], keys: list[str], rows: list[dict]) -> int:
    if not rows:
        return 0
    written = 0
    step = 80
    for start in range(0, len(rows), step):
        chunk = rows[start : start + step]
        values = ", ".join("(" + ", ".join(["?"] * len(columns)) + ")" for _ in chunk)
        assignments = ", ".join(f"{column} = s.{column}" for column in columns if column not in keys)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"s.{column}" for column in columns)
        on = " AND ".join(f"t.{column} = s.{column}" for column in keys)
        matched = f"WHEN MATCHED THEN UPDATE SET {assignments}" if assignments else ""
        sql = f"""
            MERGE INTO {table} t
            USING (
                SELECT * FROM (VALUES {values}) AS s({insert_cols})
            ) s
            ON {on}
            {matched}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        params: list = []
        for row in chunk:
            params.extend(row[column] for column in columns)
        warehouse.execute(sql, params)
        written += len(chunk)
    return written


def _insert_rejects(warehouse, batch_id: str, loaded_at: datetime, source: str, rejected: list[tuple[dict, str]]) -> None:
    if not rejected:
        return
    warehouse.executemany(
        """
        INSERT INTO silver_rejects (
            reject_id, source_table, row_json, reason, _loaded_at, _batch_id
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                f"{source}:{batch_id}:{index}",
                source,
                _json_row(row),
                reason,
                loaded_at,
                batch_id,
            )
            for index, (row, reason) in enumerate(rejected)
        ],
    )


def _split(rows: list[dict], reason_fn) -> tuple[list[dict], list[tuple[dict, str]]]:
    good: list[dict] = []
    bad: list[tuple[dict, str]] = []
    for row in rows:
        reason = reason_fn(row)
        if reason:
            bad.append((row, reason))
        else:
            good.append(row)
    return good, bad


def load_silver(warehouse, batch_id: str, loaded_at: datetime) -> dict:
    warehouse.execute("DELETE FROM silver_rejects WHERE _batch_id = ?", [batch_id])
    rows_in = 0
    rows_out = 0
    rows_rejected = 0

    groups = warehouse.query("SELECT * FROM bronze_groups WHERE _batch_id = ? ORDER BY _ingested_at, group_id", [batch_id])
    rows_in += len(groups)
    seen: set[str] = set()
    good, bad = _split(groups, lambda row: group_reason(row, seen))
    rows_out += _merge(
        warehouse,
        "silver_groups",
        [
            "group_id",
            "hour",
            "platform",
            "country",
            "channel",
            "payment_method",
            "sku",
            "app_version",
            "n_sessions",
            "n_convert",
            "n_attempt",
            "_loaded_at",
            "_batch_id",
        ],
        ["group_id"],
        [_with_load(row, loaded_at, batch_id, drop=("_ingested_at", "_source")) for row in good],
    )
    _insert_rejects(warehouse, batch_id, loaded_at, "bronze_groups", bad)
    rows_rejected += len(bad)

    tickets = warehouse.query(
        "SELECT * FROM bronze_support_tickets WHERE _batch_id = ? ORDER BY _ingested_at, ticket_id",
        [batch_id],
    )
    rows_in += len(tickets)
    seen = set()
    good, bad = _split(tickets, lambda row: ticket_reason(row, seen))
    rows_out += _merge(
        warehouse,
        "silver_support_tickets",
        [
            "ticket_id",
            "ts",
            "platform",
            "country",
            "channel",
            "payment_method",
            "sku",
            "app_version",
            "product_area",
            "text_redacted",
            "_loaded_at",
            "_batch_id",
        ],
        ["ticket_id"],
        [_with_load(row, loaded_at, batch_id, drop=("_ingested_at", "_source")) for row in good],
    )
    _insert_rejects(warehouse, batch_id, loaded_at, "bronze_support_tickets", bad)
    rows_rejected += len(bad)

    deploys = warehouse.query("SELECT * FROM bronze_deploys WHERE _batch_id = ? ORDER BY deploy_id", [batch_id])
    rows_in += len(deploys)
    seen = set()
    good, bad = _split(deploys, lambda row: _simple_reason(row, "deploy_id", seen, "deploy_id"))
    rows_out += _merge(
        warehouse,
        "silver_deploys",
        ["deploy_id", "deployed_at", "platform", "app_version", "note", "_loaded_at", "_batch_id"],
        ["deploy_id"],
        [_with_load(row, loaded_at, batch_id, drop=("_ingested_at", "_source")) for row in good],
    )
    _insert_rejects(warehouse, batch_id, loaded_at, "bronze_deploys", bad)
    rows_rejected += len(bad)

    meta = warehouse.query("SELECT * FROM bronze_demo_meta WHERE _batch_id = ? ORDER BY key", [batch_id])
    rows_in += len(meta)
    seen = set()
    good, bad = _split(meta, lambda row: _simple_reason(row, "key", seen, "meta key"))
    rows_out += _merge(
        warehouse,
        "silver_demo_meta",
        ["key", "value", "_loaded_at", "_batch_id"],
        ["key"],
        [_with_load(row, loaded_at, batch_id, drop=("_ingested_at", "_source")) for row in good],
    )
    _insert_rejects(warehouse, batch_id, loaded_at, "bronze_demo_meta", bad)
    rows_rejected += len(bad)

    golden = warehouse.query(
        "SELECT * FROM bronze_golden_tickets WHERE _batch_id = ? ORDER BY example_id",
        [batch_id],
    )
    rows_in += len(golden)
    seen = set()

    def golden_reason(row: dict) -> str | None:
        base = _simple_reason(row, "example_id", seen, "example_id")
        if base:
            return base
        kinds = pii_kinds(str(row.get("text_redacted") or ""))
        if kinds:
            return "pii:" + ",".join(kinds)
        return None

    good, bad = _split(golden, golden_reason)
    rows_out += _merge(
        warehouse,
        "silver_golden_tickets",
        [
            "example_id",
            "text_redacted",
            "topic",
            "sentiment",
            "severity",
            "product_area",
            "_loaded_at",
            "_batch_id",
        ],
        ["example_id"],
        [_with_load(row, loaded_at, batch_id, drop=("_ingested_at", "_source")) for row in good],
    )
    _insert_rejects(warehouse, batch_id, loaded_at, "bronze_golden_tickets", bad)
    rows_rejected += len(bad)

    external = warehouse.query(
        "SELECT * FROM bronze_external_events WHERE _batch_id = ? ORDER BY event_id",
        [batch_id],
    )
    rows_in += len(external)
    seen = set()
    good, bad = _split(external, lambda row: _simple_reason(row, "event_id", seen, "event_id"))
    rows_out += _merge(
        warehouse,
        "silver_external_events",
        [
            "event_id",
            "case_id",
            "title",
            "summary",
            "source_url",
            "source_type",
            "relevance",
            "observed_at",
            "metric",
            "provider",
            "_loaded_at",
            "_batch_id",
        ],
        ["event_id"],
        [_with_load(row, loaded_at, batch_id, drop=("_ingested_at", "_source")) for row in good],
    )
    _insert_rejects(warehouse, batch_id, loaded_at, "bronze_external_events", bad)
    rows_rejected += len(bad)
    return {"rows_in": rows_in, "rows_out": rows_out, "rows_rejected": rows_rejected}


def _with_load(row: dict, loaded_at: datetime, batch_id: str, *, drop: tuple[str, ...]) -> dict:
    kept = {key: value for key, value in row.items() if key not in drop}
    kept["_loaded_at"] = loaded_at
    kept["_batch_id"] = batch_id
    return kept


def _meta_float(warehouse, key: str, default: float) -> float:
    try:
        rows = warehouse.query("SELECT value FROM silver_demo_meta WHERE key = ?", [key])
    except Exception:
        return default
    if not rows or rows[0]["value"] is None:
        return default
    return float(rows[0]["value"])


def load_gold(warehouse, batch_id: str, loaded_at: datetime) -> dict:
    grains = warehouse.query(
        """
        SELECT hour, platform, country, payment_method, app_version,
               SUM(n_sessions)::INTEGER AS sessions,
               SUM(n_convert)::INTEGER AS converted
        FROM silver_groups
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 1, 2, 3, 4, 5
        """
    )
    batch_keys = {
        (
            row["hour"],
            row["platform"],
            row["country"],
            row["payment_method"],
            row["app_version"],
        )
        for row in warehouse.query(
            """
            SELECT DISTINCT hour, platform, country, payment_method, app_version
            FROM silver_groups
            WHERE _batch_id = ?
            """,
            [batch_id],
        )
    }
    scored = _score_grains(grains)
    aov = _meta_float(warehouse, "aov_usd", 48.0)
    weight = _meta_float(warehouse, "session_weight", 1.0)
    conversion_rows = []
    revenue_rows = []
    for row in scored:
        key = (row["hour"], row["platform"], row["country"], row["payment_method"], row["app_version"])
        if key not in batch_keys:
            continue
        conversion_rows.append(
            {
                "hour": row["hour"],
                "platform": row["platform"],
                "country": row["country"],
                "payment_method": row["payment_method"],
                "app_version": row["app_version"],
                "sessions": int(row["sessions"]),
                "converted": int(row["converted"]),
                "conversion_rate": row["conversion_rate"],
                "baseline_rate": row["baseline_rate"],
                "seasonal_index": row["seasonal_index"],
                "expected_rate": row["expected_rate"],
                "is_anomaly": row["is_anomaly"],
                "_batch_id": batch_id,
                "_loaded_at": loaded_at,
            }
        )
        lost = row["lost_conversions"]
        revenue_rows.append(
            {
                "hour": row["hour"],
                "platform": row["platform"],
                "country": row["country"],
                "payment_method": row["payment_method"],
                "app_version": row["app_version"],
                "lost_conversions": lost,
                "aov_usd": aov,
                "session_weight": weight,
                "revenue_at_risk_usd": lost * aov * weight,
                "_batch_id": batch_id,
                "_loaded_at": loaded_at,
            }
        )
    for table in ("gold_hourly_conversion", "gold_revenue_at_risk", "gold_daily_spend"):
        warehouse.execute(f"DELETE FROM {table} WHERE _batch_id = ?", [batch_id])
    conversion_columns = [
        "hour",
        "platform",
        "country",
        "payment_method",
        "app_version",
        "sessions",
        "converted",
        "conversion_rate",
        "baseline_rate",
        "seasonal_index",
        "expected_rate",
        "is_anomaly",
        "_batch_id",
        "_loaded_at",
    ]
    _merge(
        warehouse,
        "gold_hourly_conversion",
        conversion_columns,
        ["hour", "platform", "country", "payment_method", "app_version"],
        conversion_rows,
    )
    _merge(
        warehouse,
        "gold_revenue_at_risk",
        [
            "hour",
            "platform",
            "country",
            "payment_method",
            "app_version",
            "lost_conversions",
            "aov_usd",
            "session_weight",
            "revenue_at_risk_usd",
            "_batch_id",
            "_loaded_at",
        ],
        ["hour", "platform", "country", "payment_method", "app_version"],
        revenue_rows,
    )
    spend_rows = _spend_rows(warehouse, batch_id, loaded_at, grains)
    _merge(
        warehouse,
        "gold_daily_spend",
        ["spend_date", "spend_usd", "cap_usd", "headroom_usd", "_batch_id", "_loaded_at"],
        ["spend_date", "_batch_id"],
        spend_rows,
    )
    sync_case_facts(warehouse, batch_id, loaded_at)
    topic_gold = produce_ticket_topics(warehouse, batch_id, loaded_at)
    produced = len(conversion_rows) + len(revenue_rows) + len(spend_rows) + len(topic_gold)
    return {
        "rows_in": produced,
        "rows_out": produced,
        "rows_rejected": 0,
    }


def _score_grains(grains: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in grains:
        key = (row["platform"], row["country"], row["payment_method"], row["app_version"])
        grouped.setdefault(key, []).append(row)
    scored: list[dict] = []
    for items in grouped.values():
        items.sort(key=lambda item: item["hour"])
        history: dict[int, list[float]] = {}
        rates = []
        for item in items:
            sessions = int(item["sessions"] or 0)
            converted = int(item["converted"] or 0)
            rate = converted / sessions if sessions else 0.0
            rates.append(rate)
            item["conversion_rate"] = rate
        mean = sum(rates) / len(rates) if rates else 0.0
        for item in items:
            hour = item["hour"]
            hour_of_day = hour.hour if isinstance(hour, datetime) else datetime.fromisoformat(str(hour)).hour
            prior = history.get(hour_of_day) or []
            baseline = sum(prior) / len(prior) if prior else None
            rate = float(item["conversion_rate"])
            item["baseline_rate"] = baseline
            item["seasonal_index"] = (baseline / mean) if baseline is not None and mean else None
            item["expected_rate"] = baseline
            item["is_anomaly"] = bool(
                baseline is not None and int(item["sessions"]) >= 100 and rate <= baseline * 0.9
            )
            lost = 0.0
            if baseline is not None:
                lost = max(0.0, (baseline - rate) * int(item["sessions"]))
            item["lost_conversions"] = lost
            history.setdefault(hour_of_day, []).append(rate)
            scored.append(item)
    return scored


def _spend_rows(warehouse, batch_id: str, loaded_at: datetime, grains: list[dict]) -> list[dict]:
    from signal_service.config import load_settings

    cap = float(load_settings().daily_spend_cap_usd)
    spent = 0.0
    try:
        rows = warehouse.query("SELECT COALESCE(SUM(estimated_usd), 0) AS spent FROM silver_spend")
        if rows and rows[0]["spent"] is not None:
            spent = float(rows[0]["spent"])
    except Exception:
        spent = 0.0
    spend_date = None
    if grains:
        latest = max(row["hour"] for row in grains)
        if isinstance(latest, datetime):
            spend_date = latest.date()
        else:
            spend_date = datetime.fromisoformat(str(latest)).date()
    if spend_date is None:
        spend_date = loaded_at.date()
    return [
        {
            "spend_date": spend_date,
            "spend_usd": spent,
            "cap_usd": cap,
            "headroom_usd": cap - spent,
            "_batch_id": batch_id,
            "_loaded_at": loaded_at,
        }
    ]


def produce_ticket_topics(warehouse, batch_id: str, loaded_at: datetime | None = None) -> list[dict]:
    """Classify the seeded incident window, then aggregate it into gold.

    The documented window is Android India from 2026-11-20 02:00 through 07:00:
    73 tickets after the three PII rows are rejected. The fallback classifier
    is deterministic and does not call the model. Tickets already stored for
    the demo case are left in place so a later pipeline run does not relabel them.
    """
    from datetime import timedelta

    from signal_service.config import load_settings
    from signal_service.demo.generate import ANOMALY_START, DEMO_CASE_ID, END
    from signal_service.grok.client import MockGrokClient
    from signal_service.signals.tickets import run_ticket_signals

    settings = load_settings()
    run_ticket_signals(
        warehouse,
        MockGrokClient(),
        case_id=DEMO_CASE_ID,
        segment={"platform": "android", "country": "IN"},
        started_at=ANOMALY_START,
        ended_at=END - timedelta(hours=1),
        batch_size=settings.batch_size,
        model=settings.grok_model,
    )
    promoted = promote_topics(warehouse, batch_id, loaded_at or clock())
    return promoted["gold_rows"]


def promote_topics(warehouse, batch_id: str, loaded_at: datetime | None = None) -> dict:
    """Copy classifier rows for this batch into silver and the gold aggregate."""
    loaded_at = loaded_at or clock()
    try:
        topics = warehouse.query("SELECT * FROM signals.ticket_topics")
    except Exception:
        topics = []
    try:
        in_batch = {
            row["ticket_id"]
            for row in warehouse.query(
                "SELECT ticket_id FROM bronze_support_tickets WHERE _batch_id = ?",
                [batch_id],
            )
        }
    except Exception:
        in_batch = set()
    topic_rows = []
    for row in topics:
        if row.get("ticket_id") not in in_batch:
            continue
        topic_rows.append(
            {
                "ticket_id": row["ticket_id"],
                "case_id": row.get("case_id"),
                "classified_at": row.get("classified_at"),
                "topic": row.get("topic"),
                "sentiment": row.get("sentiment"),
                "severity": row.get("severity"),
                "product_area": row.get("product_area"),
                "evidence_snippet": row.get("evidence_snippet"),
                "confidence": row.get("confidence"),
                "model": row.get("model"),
                "provider": row.get("provider"),
                "input_hash": row.get("input_hash"),
                "_loaded_at": loaded_at,
                "_batch_id": batch_id,
            }
        )
    if topic_rows:
        _merge(
            warehouse,
            "silver_ticket_topics",
            list(topic_rows[0].keys()),
            ["ticket_id", "case_id"],
            topic_rows,
        )
    warehouse.execute("DELETE FROM gold_ticket_topics WHERE _batch_id = ?", [batch_id])
    aggregates = []
    if topic_rows:
        aggregates = warehouse.query(
            """
            SELECT topic, sentiment, severity, product_area,
                   COUNT(*)::INTEGER AS ticket_count,
                   AVG(confidence)::DOUBLE AS avg_confidence
            FROM silver_ticket_topics
            WHERE _batch_id = ?
            GROUP BY 1, 2, 3, 4
            """,
            [batch_id],
        )
    gold_rows = [
        {
            "topic": row["topic"],
            "sentiment": row["sentiment"],
            "severity": row["severity"],
            "product_area": row["product_area"],
            "ticket_count": int(row["ticket_count"]),
            "avg_confidence": float(row["avg_confidence"] or 0),
            "_batch_id": batch_id,
            "_loaded_at": loaded_at,
        }
        for row in aggregates
    ]
    if gold_rows:
        _merge(
            warehouse,
            "gold_ticket_topics",
            list(gold_rows[0].keys()),
            ["topic", "sentiment", "severity", "product_area", "_batch_id"],
            gold_rows,
        )
    return {"topics": len(topic_rows), "aggregates": len(gold_rows), "gold_rows": gold_rows}


def mirror_signals(warehouse, batch_id: str, loaded_at: datetime | None = None) -> dict:
    """Copy classifier output into silver, then aggregate the gold topic mart."""
    loaded_at = loaded_at or clock()
    promoted = promote_topics(warehouse, batch_id, loaded_at)
    try:
        events = warehouse.query("SELECT * FROM signals.external_events")
    except Exception:
        events = []
    event_rows = [
        {
            "event_id": row["event_id"],
            "case_id": row.get("case_id"),
            "title": row.get("title"),
            "summary": row.get("summary"),
            "source_url": row.get("source_url"),
            "source_type": row.get("source_type"),
            "relevance": row.get("relevance"),
            "observed_at": row.get("observed_at"),
            "metric": row.get("metric"),
            "provider": row.get("provider"),
            "_loaded_at": loaded_at,
            "_batch_id": batch_id,
        }
        for row in events
    ]
    _merge(
        warehouse,
        "silver_external_events",
        list(event_rows[0].keys()) if event_rows else [],
        ["event_id"],
        event_rows,
    )
    try:
        spend = warehouse.query("SELECT * FROM signals.spend_log")
    except Exception:
        spend = []
    spend_rows = [
        {
            "logged_at": row.get("logged_at"),
            "case_id": row.get("case_id"),
            "kind": row.get("kind"),
            "estimated_usd": row.get("estimated_usd"),
            "input_tokens": row.get("input_tokens"),
            "output_tokens": row.get("output_tokens"),
            "provider": row.get("provider"),
            "_loaded_at": loaded_at,
            "_batch_id": batch_id,
        }
        for row in spend
    ]
    if spend_rows:
        _merge(
            warehouse,
            "silver_spend",
            list(spend_rows[0].keys()),
            ["logged_at", "case_id", "kind"],
            spend_rows,
        )
    return {
        "topics": promoted["topics"],
        "events": len(event_rows),
        "aggregates": promoted["aggregates"],
    }


def sync_case_facts(warehouse, batch_id: str, loaded_at: datetime | None = None) -> int:
    loaded_at = loaded_at or clock()
    try:
        cases = warehouse.query("SELECT * FROM memory_cases")
    except Exception:
        return 0
    written = 0
    for case in cases:
        payload: dict = {}
        raw = case.get("payload_json")
        if raw:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {}
        revenue = payload.get("revenue_at_risk_usd_per_hour")
        warehouse.execute("DELETE FROM gold_case_facts WHERE case_id = ?", [case["case_id"]])
        warehouse.execute(
            """
            INSERT INTO gold_case_facts (
                case_id, metric, segment_json, status, owner, opened_at, started_at,
                severity, revenue_at_risk_usd, _batch_id, _loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                case.get("case_id"),
                case.get("metric"),
                case.get("segment_json"),
                case.get("status"),
                case.get("owner"),
                case.get("opened_at"),
                case.get("started_at"),
                case.get("severity"),
                None if revenue is None else float(revenue),
                batch_id,
                loaded_at,
            ],
        )
        written += 1
    return written
