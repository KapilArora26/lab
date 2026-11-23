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
        try:
            align_user_managed_tasks(warehouse)
            tasks_ok = True
        except WarehouseUsageError:
            raise
        except Exception as exc:
            usage = warehouse_usage_denial(exc)
            if usage:
                raise WarehouseUsageError(usage) from exc
            if is_privilege_denial(exc):
                missing.append("CREATE TASK")
            errors.append(f"CREATE TASK: {str(exc)[:240]}")
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


TASK_WAREHOUSE = "COMPUTE_WH"
# Suspend the producer first. Resume the consumer first so it is ready
# before bronze starts landing new silver rows.
SUSPEND_ORDER = ("TASK_BRONZE_TO_SILVER", "TASK_SILVER_TO_GOLD")
RESUME_ORDER = ("TASK_SILVER_TO_GOLD", "TASK_BRONZE_TO_SILVER")
_SERVERLESS_PARAMS = (
    "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE",
    "TARGET_COMPLETION_INTERVAL",
)
_BRONZE_STREAMS = (
    "STREAM_BRONZE_GROUPS",
    "STREAM_BRONZE_SUPPORT_TICKETS",
    "STREAM_BRONZE_DEPLOYS",
    "STREAM_BRONZE_DEMO_META",
    "STREAM_BRONZE_GOLDEN_TICKETS",
    "STREAM_BRONZE_EXTERNAL_EVENTS",
)


class WarehouseUsageError(RuntimeError):
    """The deploying role cannot use COMPUTE_WH. Deploy stops."""


def warehouse_usage_denial(exc: BaseException) -> str | None:
    """A task statement failed because COMPUTE_WH is not usable."""
    text = str(exc).lower()
    if "compute_wh" not in text:
        return None
    if not any(token in text for token in ("privilege", "not authorized", "usage", "does not exist", "access control")):
        return None
    return (
        "The deploying role lacks USAGE on warehouse COMPUTE_WH. "
        "Deploy does not create or alter the warehouse and does not change grants. "
        f"{exc}"
    )


def align_user_managed_tasks(warehouse, *, sandbox: str = SANDBOX_QUALIFIED) -> None:
    """Point both triggered tasks at COMPUTE_WH.

    Existing tasks are suspended, given the warehouse, and stripped of the
    serverless parameters only when those parameters are set. New tasks are
    created with ``WAREHOUSE = COMPUTE_WH`` and no schedule. Resume runs the
    silver task before the bronze task. ``SHOW TASKS`` must then show
    ``COMPUTE_WH``, a null schedule, and a stream condition.

    ``USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE`` cannot stay set once a
    warehouse is assigned, so that parameter is cleared before
    ``SET WAREHOUSE``. ``TARGET_COMPLETION_INTERVAL`` is cleared after.
    """
    if getattr(warehouse, "query", None) is None:
        for statement in medallion_statements("task"):
            warehouse.execute(statement)
        for statement in medallion_statements("resume"):
            warehouse.execute(statement)
        return
    _assert_compute_warehouse(warehouse)
    existing = _tasks_by_name(warehouse, sandbox)
    for name in SUSPEND_ORDER:
        if name not in existing:
            continue
        warehouse.execute(f"ALTER TASK {sandbox}.{name} SUSPEND")
        set_params = _parameters_set_on_task(warehouse, sandbox, name)
        # Snowflake rejects WAREHOUSE together with the managed size.
        if "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE" in set_params:
            warehouse.execute(
                f"ALTER TASK {sandbox}.{name} UNSET USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE"
            )
        warehouse.execute(f"ALTER TASK {sandbox}.{name} SET WAREHOUSE = {TASK_WAREHOUSE}")
        if "TARGET_COMPLETION_INTERVAL" in set_params:
            warehouse.execute(f"ALTER TASK {sandbox}.{name} UNSET TARGET_COMPLETION_INTERVAL")
    for statement in medallion_statements("task"):
        warehouse.execute(statement)
    for statement in medallion_statements("resume"):
        warehouse.execute(statement)
    shown = _tasks_by_name(warehouse, sandbox)
    errors = task_shape_errors(list(shown.values()))
    if errors:
        raise RuntimeError("SHOW TASKS verification failed: " + "; ".join(errors))


def task_shape_errors(rows: list[dict]) -> list[str]:
    """Checks a ``SHOW TASKS`` result for the user-managed triggered shape."""
    by_name = {}
    for row in rows:
        name = str(row.get("name") or "").upper()
        if name:
            by_name[name] = {str(key).lower(): value for key, value in row.items()}
    errors: list[str] = []
    expected_condition = {
        "TASK_BRONZE_TO_SILVER": _BRONZE_STREAMS,
        "TASK_SILVER_TO_GOLD": ("STREAM_SILVER_GROUPS",),
    }
    for name, streams in expected_condition.items():
        row = by_name.get(name)
        if row is None:
            errors.append(f"{name} is missing from SHOW TASKS")
            continue
        warehouse = str(row.get("warehouse") or "").upper()
        if warehouse != TASK_WAREHOUSE:
            errors.append(f"{name} warehouse is {warehouse or 'NULL'}, expected {TASK_WAREHOUSE}")
        schedule = row.get("schedule")
        if schedule not in (None, ""):
            errors.append(f"{name} schedule is {schedule}, expected NULL")
        condition = str(row.get("condition") or "")
        folded = condition.upper()
        if "SYSTEM$STREAM_HAS_DATA" not in folded:
            errors.append(f"{name} condition is not set")
        for stream in streams:
            if stream not in folded:
                errors.append(f"{name} condition does not include {stream}")
        definition = str(row.get("definition") or "").upper().replace(" ", "")
        if "LOAD_STAGING(NULL)" not in definition:
            errors.append(f"{name} definition does not call LOAD_STAGING(NULL)")
        interval = row.get("target_completion_interval")
        if interval not in (None, ""):
            errors.append(f"{name} target_completion_interval is still set")
    return errors


def _assert_compute_warehouse(warehouse) -> None:
    rows = warehouse.query("SHOW WAREHOUSES LIKE 'COMPUTE_WH'")
    names = set()
    for row in rows:
        lowered = {str(key).lower(): value for key, value in row.items()}
        names.add(str(lowered.get("name") or "").upper())
    if TASK_WAREHOUSE not in names:
        raise WarehouseUsageError(
            "The deploying role lacks USAGE on warehouse COMPUTE_WH. "
            "Deploy does not create or alter the warehouse and does not change grants."
        )


def _tasks_by_name(warehouse, sandbox: str) -> dict[str, dict]:
    rows = warehouse.query(f"SHOW TASKS IN SCHEMA {sandbox}")
    found: dict[str, dict] = {}
    for row in rows:
        lowered = {str(key).lower(): value for key, value in row.items()}
        name = str(lowered.get("name") or "").upper()
        if name in RESUME_ORDER or name in SUSPEND_ORDER:
            found[name] = lowered
    return found


def _parameters_set_on_task(warehouse, sandbox: str, name: str) -> set[str]:
    rows = warehouse.query(f"SHOW PARAMETERS IN TASK {sandbox}.{name}")
    set_on_task: set[str] = set()
    for row in rows:
        lowered = {str(key).lower(): value for key, value in row.items()}
        key = str(lowered.get("key") or "").upper()
        if key not in _SERVERLESS_PARAMS:
            continue
        level = str(lowered.get("level") or "").upper()
        value = lowered.get("value")
        if level == "TASK" and value not in (None, ""):
            set_on_task.add(key)
    return set_on_task


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
