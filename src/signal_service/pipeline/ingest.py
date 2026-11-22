"""On-demand bronze landing. Propagation runs before the call returns on DuckDB."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from signal_service.config import load_settings
from signal_service.demo.generate import (
    ANOMALY_START,
    OTHER_TEXTS,
    START,
    UPI_TEXTS,
    deploy_rows,
    external_event_rows,
    group_rows,
    meta_rows,
    ticket_rows,
)
from signal_service.pipeline.ddl import ensure_objects
from signal_service.pipeline.mode import resolve_mode
from signal_service.pipeline.propagate import PipelineError, clock, propagate, write_run

SEED_BATCH_ID = "seed-2026-11-08"

BRONZE_TABLES = (
    "bronze_groups",
    "bronze_support_tickets",
    "bronze_deploys",
    "bronze_demo_meta",
    "bronze_golden_tickets",
    "bronze_external_events",
)


class Batch:
    def __init__(
        self,
        batch_id: str,
        source: str,
        groups: list[tuple],
        tickets: list[tuple],
        deploys: list[tuple],
        meta: list[tuple],
        golden: list[tuple],
        external: list[tuple],
    ):
        self.batch_id = batch_id
        self.source = source
        self.groups = groups
        self.tickets = tickets
        self.deploys = deploys
        self.meta = meta
        self.golden = golden
        self.external = external

    def row_count(self) -> int:
        return (
            len(self.groups)
            + len(self.tickets)
            + len(self.deploys)
            + len(self.meta)
            + len(self.golden)
            + len(self.external)
        )


def seed_batch() -> Batch:
    from signal_service.demo.generate import golden_rows

    return Batch(
        batch_id=SEED_BATCH_ID,
        source="generator",
        groups=group_rows(),
        tickets=ticket_rows(),
        deploys=deploy_rows(),
        meta=meta_rows(),
        golden=golden_rows(),
        external=external_event_rows(),
    )


def ingest(
    warehouse,
    *,
    batch: str = "new",
    path: str | None = None,
    hours: int | None = None,
    mode: str | None = None,
    rerun: bool = False,
) -> dict:
    ensure_objects(warehouse)
    if batch == "file":
        payload = load_file_batch(path or "")
    elif batch == "new":
        payload = new_batch(warehouse, hours if hours is not None else load_settings().ingest_hours)
    elif batch == "seed":
        payload = seed_batch()
    else:
        raise ValueError("batch must be 'new', 'file', or 'seed'")
    if rerun:
        return rerun_batch(warehouse, payload.batch_id, mode=mode)
    return ingest_batch(warehouse, payload, mode=mode)


def ingest_seed(warehouse, *, mode: str | None = None, rerun: bool = False) -> dict:
    ensure_objects(warehouse)
    if rerun:
        return rerun_batch(warehouse, SEED_BATCH_ID, mode=mode)
    return ingest_batch(warehouse, seed_batch(), mode=mode)


def rerun_batch(warehouse, batch_id: str, *, mode: str | None = None) -> dict:
    """Reprocess one batch even when a gold run was recorded as success.

    Bronze rows are left in place. This does not ``DROP`` or ``TRUNCATE``.
    Silver rejects for the batch are replaced, and ``MERGE`` inserts keys that
    were missing. Use it when a gold success row is wrong, for example after a
    truncated read. CLI: ``python -m signal_service.cli rerun <batch_id>`` or
    ``python -m signal_service.cli ingest --batch seed --rerun``.
    """
    ensure_objects(warehouse)
    if not _bronze_present(warehouse, batch_id):
        raise ValueError(f"cannot rerun {batch_id}: bronze rows are not present")
    batch = Batch(
        batch_id=batch_id,
        source="rerun",
        groups=[],
        tickets=[],
        deploys=[],
        meta=[],
        golden=[],
        external=[],
    )
    return ingest_batch(warehouse, batch, mode=mode, rerun=True)


def ingest_batch(warehouse, batch: Batch, *, mode: str | None = None, rerun: bool = False) -> dict:
    ensure_objects(warehouse)
    report = resolve_mode(warehouse, mode)
    if rerun and not _bronze_present(warehouse, batch.batch_id):
        raise ValueError(f"cannot rerun {batch.batch_id}: bronze rows are not present")
    if not rerun and _gold_succeeded(warehouse, batch.batch_id):
        from signal_service.pipeline.reconcile import reconciliation_errors

        problems = reconciliation_errors(warehouse, batch.batch_id)
        if problems:
            return {
                "batch_id": batch.batch_id,
                "mode": report.mode,
                "propagation": "noop",
                "status": "failed",
                "noop": False,
                "error": "; ".join(problems),
                "missing_privileges": report.missing_privileges,
                "notes": report.notes,
                "layers": _layers(warehouse, batch.batch_id),
            }
        return {
            "batch_id": batch.batch_id,
            "mode": report.mode,
            "propagation": "noop",
            "status": "noop",
            "noop": True,
            "missing_privileges": report.missing_privileges,
            "notes": report.notes,
            "layers": [],
        }
    if not _bronze_present(warehouse, batch.batch_id):
        started = clock()
        try:
            landed = _land_bronze(warehouse, batch, started)
        except Exception as exc:
            write_run(
                warehouse,
                batch_id=batch.batch_id,
                layer="bronze",
                started_at=started,
                status="failed",
                rows_in=batch.row_count(),
                error=str(exc),
            )
            raise
        write_run(
            warehouse,
            batch_id=batch.batch_id,
            layer="bronze",
            started_at=started,
            status="success",
            rows_in=batch.row_count(),
            rows_out=landed,
            rows_rejected=0,
        )
    propagation = "sync"
    error = None
    status = "success"
    if getattr(warehouse, "backend", "duckdb") == "snowflake":
        propagation = "procedure"
        status, error = _load_staging(warehouse, batch.batch_id, report, rerun)
    elif report.mode == "procedure" and report.procedure_ready:
        propagation = "procedure"
        try:
            warehouse.execute(
                "CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(?)",
                [batch.batch_id],
            )
        except Exception as exc:
            propagation = "sync"
            report.missing_privileges = list(dict.fromkeys([*report.missing_privileges, "CREATE PROCEDURE"]))
            report.notes.append(f"LOAD_STAGING call failed, sync MERGE used: {str(exc)[:200]}")
            status, error = _sync(warehouse, batch.batch_id)
    elif report.mode == "dynamic_table":
        propagation = "dynamic_table"
        status = "landed"
    else:
        status, error = _sync(warehouse, batch.batch_id)
    if status == "success":
        from signal_service.pipeline.reconcile import reconciliation_errors

        problems = reconciliation_errors(warehouse, batch.batch_id)
        if problems:
            status = "failed"
            error = "; ".join(problems)
    return {
        "batch_id": batch.batch_id,
        "mode": report.mode,
        "propagation": propagation,
        "status": status,
        "noop": False,
        "error": error,
        "missing_privileges": report.missing_privileges,
        "notes": report.notes,
        "layers": _layers(warehouse, batch.batch_id),
    }


def _load_staging(warehouse, batch_id: str, report, rerun: bool) -> tuple[str, str | None]:
    """Propagate on the server. Never SELECT bronze rows into this process."""
    if not _ensure_procedure(warehouse, report):
        return (
            "failed",
            "LOAD_STAGING is not available. Snowflake ingest does not pull bronze rows into the client.",
        )
    if rerun:
        statement = "CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(?, ?)"
        params = [batch_id, True]
    else:
        statement = "CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(?)"
        params = [batch_id]
    try:
        warehouse.execute(statement, params)
    except Exception as exc:
        return "failed", str(exc)[:500]
    from signal_service.pipeline.reconcile import latest_layer_status

    failed = latest_layer_status(warehouse, batch_id)
    if failed:
        return "failed", failed
    return "success", None


def _ensure_procedure(warehouse, report) -> bool:
    if report.procedure_ready:
        return True
    from signal_service.pipeline.ddl import medallion_statements

    for statement in medallion_statements("procedure"):
        try:
            warehouse.execute(statement)
        except Exception as exc:
            report.notes.append(f"LOAD_STAGING was not created: {str(exc)[:200]}")
            return False
    report.procedure_ready = True
    return True


def _sync(warehouse, batch_id: str) -> tuple[str, str | None]:
    try:
        propagate(warehouse, batch_id)
    except PipelineError as exc:
        return "failed", exc.message
    return "success", None


def _layers(warehouse, batch_id: str) -> list[dict]:
    rows = warehouse.query(
        """
        SELECT layer, status, rows_in, rows_out, rows_rejected, error, started_at, finished_at
        FROM pipeline_runs
        WHERE batch_id = ?
        ORDER BY started_at
        """,
        [batch_id],
    )
    return rows


def _gold_succeeded(warehouse, batch_id: str) -> bool:
    rows = warehouse.query(
        """
        SELECT COUNT(*)::INTEGER AS n
        FROM pipeline_runs
        WHERE batch_id = ? AND layer = 'gold' AND status = 'success'
        """,
        [batch_id],
    )
    return bool(rows and int(rows[0]["n"]) > 0)


def _bronze_present(warehouse, batch_id: str) -> bool:
    total = 0
    for table in BRONZE_TABLES:
        rows = warehouse.query(f"SELECT COUNT(*)::INTEGER AS n FROM {table} WHERE _batch_id = ?", [batch_id])
        total += int(rows[0]["n"])
    return total > 0


def _land_bronze(warehouse, batch: Batch, ingested_at: datetime) -> int:
    warehouse.executemany(
        """
        INSERT INTO bronze_groups (
            group_id, hour, platform, country, channel, payment_method, sku, app_version,
            n_sessions, n_convert, n_attempt, _ingested_at, _source, _batch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(row) + (ingested_at, batch.source, batch.batch_id) for row in batch.groups],
    )
    warehouse.executemany(
        """
        INSERT INTO bronze_support_tickets (
            ticket_id, ts, platform, country, channel, payment_method, sku, app_version,
            product_area, text_redacted, _ingested_at, _source, _batch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(row) + (ingested_at, batch.source, batch.batch_id) for row in batch.tickets],
    )
    warehouse.executemany(
        """
        INSERT INTO bronze_deploys (
            deploy_id, deployed_at, platform, app_version, note, _ingested_at, _source, _batch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(row) + (ingested_at, batch.source, batch.batch_id) for row in batch.deploys],
    )
    warehouse.executemany(
        """
        INSERT INTO bronze_demo_meta (key, value, _ingested_at, _source, _batch_id)
        VALUES (?, ?, ?, ?, ?)
        """,
        [tuple(row) + (ingested_at, batch.source, batch.batch_id) for row in batch.meta],
    )
    warehouse.executemany(
        """
        INSERT INTO bronze_golden_tickets (
            example_id, text_redacted, topic, sentiment, severity, product_area,
            _ingested_at, _source, _batch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(row) + (ingested_at, batch.source, batch.batch_id) for row in batch.golden],
    )
    warehouse.executemany(
        """
        INSERT INTO bronze_external_events (
            event_id, case_id, title, summary, source_url, source_type, relevance,
            observed_at, metric, provider, _ingested_at, _source, _batch_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(row) + (ingested_at, batch.source, batch.batch_id) for row in batch.external],
    )
    return batch.row_count()


def new_batch(warehouse, hours: int) -> Batch:
    if hours < 1:
        raise ValueError("hours must be at least 1")
    latest = _latest_hour(warehouse)
    start = START if latest is None else latest + timedelta(hours=1)
    hour_list = [start + timedelta(hours=offset) for offset in range(hours)]
    last = hour_list[-1]
    return Batch(
        batch_id=f"batch-{start:%Y%m%d%H}-{last:%Y%m%d%H}",
        source="generator",
        groups=group_rows(hour_list),
        tickets=tickets_for_hours(hour_list, _next_ticket_number(warehouse)),
        deploys=[],
        meta=[],
        golden=[],
        external=[],
    )


def tickets_for_hours(hours: list[datetime], start_number: int) -> list[tuple]:
    tickets: list[tuple] = []
    number = start_number
    for hour in hours:
        if hour >= ANOMALY_START:
            for offset in range(12):
                text = UPI_TEXTS[(number + offset) % len(UPI_TEXTS)]
                tickets.append(
                    (
                        f"t-{number:05d}",
                        hour,
                        "android",
                        "IN",
                        "app" if offset % 2 == 0 else "paid",
                        "upi",
                        "SKU-100" if offset % 2 == 0 else "SKU-200",
                        "4.2.1",
                        "unspecified",
                        text,
                    )
                )
                number += 1
            continue
        text, platform, country, payment, version = OTHER_TEXTS[hour.day % len(OTHER_TEXTS)]
        tickets.append(
            (
                f"t-{number:05d}",
                hour,
                platform,
                country,
                "app",
                payment,
                "SKU-100",
                version,
                "unspecified",
                text,
            )
        )
        number += 1
    return tickets


def load_file_batch(path: str) -> Batch:
    candidate = Path(path)
    if not candidate.is_file():
        raise FileNotFoundError(path)
    payload = json.loads(candidate.read_text(encoding="utf-8"))
    batch_id = str(payload.get("batch_id") or f"file-{candidate.stem}")

    def groups() -> list[tuple]:
        rows = []
        for item in payload.get("groups") or []:
            rows.append(
                (
                    item["group_id"],
                    _parse_ts(item["hour"]),
                    item["platform"],
                    item["country"],
                    item.get("channel"),
                    item.get("payment_method"),
                    item.get("sku"),
                    item.get("app_version"),
                    int(item["n_sessions"]),
                    int(item["n_convert"]),
                    int(item["n_attempt"]),
                )
            )
        return rows

    def tickets() -> list[tuple]:
        rows = []
        for item in payload.get("support_tickets") or []:
            rows.append(
                (
                    item["ticket_id"],
                    _parse_ts(item["ts"]),
                    item.get("platform"),
                    item.get("country"),
                    item.get("channel"),
                    item.get("payment_method"),
                    item.get("sku"),
                    item.get("app_version"),
                    item.get("product_area"),
                    item.get("text_redacted") or item.get("text") or "",
                )
            )
        return rows

    def deploys() -> list[tuple]:
        return [
            (
                item["deploy_id"],
                _parse_ts(item["deployed_at"]),
                item.get("platform"),
                item.get("app_version"),
                item.get("note"),
            )
            for item in payload.get("deploys") or []
        ]

    def meta() -> list[tuple]:
        return [(item["key"], str(item["value"])) for item in payload.get("demo_meta") or []]

    def golden() -> list[tuple]:
        return [
            (
                item["example_id"],
                item["text_redacted"],
                item["topic"],
                item["sentiment"],
                item["severity"],
                item["product_area"],
            )
            for item in payload.get("golden_tickets") or []
        ]

    def external() -> list[tuple]:
        rows = []
        for item in payload.get("external_events") or []:
            observed = item.get("observed_at")
            rows.append(
                (
                    item["event_id"],
                    item.get("case_id"),
                    item.get("title"),
                    item.get("summary"),
                    item.get("source_url"),
                    item.get("source_type"),
                    float(item.get("relevance") or 0),
                    _parse_ts(observed) if observed else None,
                    item.get("metric"),
                    item.get("provider"),
                )
            )
        return rows

    return Batch(
        batch_id=batch_id,
        source="file",
        groups=groups(),
        tickets=tickets(),
        deploys=deploys(),
        meta=meta(),
        golden=golden(),
        external=external(),
    )


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value).replace("Z", "")
    return datetime.fromisoformat(text)


def _latest_hour(warehouse) -> datetime | None:
    try:
        rows = warehouse.query("SELECT MAX(hour) AS hour FROM bronze_groups")
    except Exception:
        return None
    if not rows or rows[0]["hour"] is None:
        return None
    hour = rows[0]["hour"]
    if isinstance(hour, datetime):
        return hour.replace(tzinfo=None)
    return datetime.fromisoformat(str(hour))


def _next_ticket_number(warehouse) -> int:
    try:
        rows = warehouse.query("SELECT ticket_id FROM bronze_support_tickets")
    except Exception:
        return 1
    best = 0
    for row in rows:
        text = str(row["ticket_id"])
        digits = "".join(char for char in text if char.isdigit())
        if digits:
            best = max(best, int(digits))
    return best + 1

