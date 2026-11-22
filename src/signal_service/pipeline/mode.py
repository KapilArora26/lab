"""Choose how bronze reaches gold. DuckDB always merges inside the ingest call."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

from signal_service.config import SANDBOX_QUALIFIED
from signal_service.pipeline.ddl import medallion_statements


@dataclass
class ModeReport:
    mode: str
    missing_privileges: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    procedure_ready: bool = False
    tasks_ready: bool = False

    def as_dict(self) -> dict:
        return {
            "mode": self.mode,
            "missing_privileges": list(self.missing_privileges),
            "notes": list(self.notes),
            "procedure_ready": self.procedure_ready,
            "tasks_ready": self.tasks_ready,
        }


def resolve_mode(warehouse, requested: str | None = None) -> ModeReport:
    choice = (requested or os.environ.get("SIGNAL_PIPELINE_MODE") or "auto").strip().lower()
    if choice not in {"auto", "sync", "task", "procedure", "dynamic_table"}:
        raise ValueError("SIGNAL_PIPELINE_MODE must be auto, sync, task, procedure, or dynamic_table")
    backend = getattr(warehouse, "backend", "duckdb")
    if backend != "snowflake" or choice == "sync":
        note = "DuckDB propagates bronze to silver to gold inside the ingest call."
        if backend == "snowflake" and choice == "sync":
            note = "Synchronous MERGE selected by SIGNAL_PIPELINE_MODE=sync."
        return ModeReport(mode="sync", notes=[note])
    cached = getattr(warehouse, "_pipeline_mode", None)
    if cached is not None and requested is None:
        return cached
    report = install_event_driven(warehouse)
    if choice == "auto":
        selected = report
    elif choice == "task" and report.tasks_ready:
        selected = report
        selected.mode = "task"
    elif choice == "procedure" and report.procedure_ready:
        selected = report
        selected.mode = "procedure"
    elif choice == "dynamic_table" and report.mode == "dynamic_table":
        selected = report
    else:
        report.mode = "sync"
        report.notes.append(f"Requested mode {choice} is not available; using synchronous MERGE.")
        selected = report
    warehouse._pipeline_mode = selected
    return selected


class ProcedureCompileError(RuntimeError):
    """A sandbox procedure did not compile, so tasks were not created."""


def install_event_driven(warehouse) -> ModeReport:
    """Create streams, tasks, and LOAD_STAGING when the role allows it.

    A denied privilege is recorded and skipped. A procedure that fails to
    compile stops the install. Nothing here issues a GRANT.
    """
    missing: list[str] = []
    notes: list[str] = []
    errors: list[str] = []
    streams_ok = True
    for statement in medallion_statements("stream"):
        if not _try(warehouse, statement, missing, errors):
            streams_ok = False
    procedure_ok = True
    procedure_statements = medallion_statements("procedure")
    if not procedure_statements:
        procedure_ok = False
        missing.append("CREATE PROCEDURE")
    for statement in procedure_statements:
        try:
            warehouse.execute(statement)
        except Exception as exc:
            if is_privilege_denial(exc):
                missing.append(privilege_for(statement))
                errors.append(f"{privilege_for(statement)}: {str(exc)[:240]}")
                procedure_ok = False
                continue
            raise ProcedureCompileError(
                "Sandbox procedure failed to compile, so tasks were not created. "
                "LOAD_STAGING must stay LOAD_STAGING(batch VARCHAR). A second argument "
                "overloads the procedure already in the sandbox and Snowflake rejects it. "
                f"{exc}"
            ) from exc
    tasks_ok = False
    if streams_ok and procedure_ok:
        task_statements = medallion_statements("task")
        tasks_ok = bool(task_statements)
        for statement in task_statements:
            if not _try(warehouse, statement, missing, errors):
                tasks_ok = False
        if tasks_ok:
            for statement in medallion_statements("resume"):
                if not _try(warehouse, statement, missing, errors):
                    tasks_ok = False
    elif not streams_ok:
        notes.append("Tasks were not created because a stream could not be created.")
    dynamic = False
    if not tasks_ok and not procedure_ok:
        dynamic = _try_dynamic_tables(warehouse, missing, notes, errors)
    if tasks_ok:
        mode = "task"
    elif procedure_ok:
        mode = "procedure"
    elif dynamic:
        mode = "dynamic_table"
    else:
        mode = "sync"
        notes.append("Synchronous MERGE will run inside the ingest call.")
    for item in errors:
        notes.append(item)
    return ModeReport(
        mode=mode,
        missing_privileges=list(dict.fromkeys(missing)),
        notes=notes,
        procedure_ready=procedure_ok,
        tasks_ready=tasks_ok,
    )


def dynamic_table_sql(warehouse_name: str, *, sandbox: str = SANDBOX_QUALIFIED) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", warehouse_name):
        raise ValueError("SIGNAL_SNOWFLAKE_WAREHOUSE must be a single identifier")
    return f"""
        CREATE OR REPLACE DYNAMIC TABLE {sandbox}.DT_SILVER_GROUPS
            TARGET_LAG = '1 minute'
            WAREHOUSE = {warehouse_name}
        AS
        SELECT
            group_id, hour, platform, country, channel, payment_method, sku, app_version,
            n_sessions, n_convert, n_attempt,
            CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS _loaded_at,
            _batch_id
        FROM {sandbox}.BRONZE_GROUPS
        WHERE group_id IS NOT NULL
          AND n_sessions >= 0
          AND n_convert >= 0
          AND n_attempt >= 0
          AND n_attempt >= n_convert
          AND n_sessions >= n_convert
          AND YEAR(hour) <> 1970
          AND hour >= TO_TIMESTAMP_NTZ('2020-01-01')
          AND hour <= DATEADD('second', 86400, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)
    """.strip()


def privilege_for(sql: str) -> str:
    collapsed = " ".join(sql.lower().split())
    if collapsed.startswith("alter task"):
        return "EXECUTE TASK"
    if collapsed.startswith("create") and " procedure " in collapsed[:180]:
        return "CREATE PROCEDURE"
    if collapsed.startswith("create") and " dynamic table " in collapsed[:180]:
        return "CREATE DYNAMIC TABLE"
    if collapsed.startswith("create") and " stream " in collapsed[:180]:
        return "CREATE STREAM"
    if collapsed.startswith("create") and " task " in collapsed[:180]:
        return "CREATE TASK"
    return "SANDBOX DDL"


def is_privilege_denial(exc: BaseException) -> bool:
    """True only for an authorization failure.

    A SQL compilation error is not a missing privilege. Callers record it on
    the mode report and leave ``missing_privileges`` empty for that statement.
    """
    text = str(exc).lower()
    if any(token in text for token in ("compilation", "syntax error", "unexpected")):
        return False
    markers = (
        "insufficient privilege",
        "access control error",
        "not authorized",
        "authorization failed",
        "permission denied",
    )
    return any(marker in text for marker in markers)


def _try(warehouse, statement: str, missing: list[str], errors: list[str]) -> bool:
    try:
        warehouse.execute(statement)
    except Exception as exc:
        if is_privilege_denial(exc):
            missing.append(privilege_for(statement))
        errors.append(f"{privilege_for(statement)}: {str(exc)[:240]}")
        return False
    return True


def _try_dynamic_tables(warehouse, missing: list[str], notes: list[str], errors: list[str]) -> bool:
    warehouse_name = os.environ.get("SIGNAL_SNOWFLAKE_WAREHOUSE", "").strip()
    if not warehouse_name:
        notes.append(
            "Dynamic tables were not attempted because SIGNAL_SNOWFLAKE_WAREHOUSE is unset. "
            "TARGET_LAG of 1 minute is the alternative when tasks are unavailable."
        )
        return False
    try:
        statement = dynamic_table_sql(warehouse_name)
    except ValueError as exc:
        notes.append(str(exc))
        return False
    return _try(warehouse, statement, missing, errors)
