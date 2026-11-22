from __future__ import annotations

from datetime import datetime

from signal_service.config import Settings, load_settings
from signal_service.detection.sentinel import apply_guards, load_series, scan, select_primary
from signal_service.diagnosis.pack import diagnose
from signal_service.narrative.brief import chart_specs, exec_brief, voice_script, weekly_review_page
from signal_service.orchestration.atlas import (
    assign_owner,
    build_case_file,
    quiet_hours_note,
    route_next,
)
from signal_service.orchestration.builder import agent_brief, estimate_batch_cost, write_build_spec
from signal_service.orchestration.memory import Memory
from signal_service.safety.cost import SpendGuard
from signal_service.signals.external import run_external_signals
from signal_service.signals.tickets import load_topics, run_ticket_signals
from signal_service.signals.voice import synthesize_voice


def _guard(settings: Settings) -> SpendGuard:
    return SpendGuard(
        daily_cap_usd=settings.daily_spend_cap_usd,
        input_usd_per_million=settings.grok_input_usd_per_million,
        output_usd_per_million=settings.grok_output_usd_per_million,
        tts_usd_per_million_chars=settings.grok_tts_usd_per_million_chars,
    )


def _search_query(card: dict, pack: dict) -> str:
    parts = [card["metric"], "drop"]
    for key, value in card["segment"].items():
        parts.append(f"{key} {value}")
    for hypothesis in pack["hypotheses"]:
        if hypothesis.get("value"):
            parts.append(str(hypothesis["value"]))
    parts.append(card["started_at"])
    return " ".join(parts)


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", ""))


def run_pipeline(
    warehouse,
    client,
    settings: Settings | None = None,
    *,
    simulate_build_approval: bool = True,
) -> dict:
    settings = settings or load_settings()
    guard = getattr(client, "spend", None) or _guard(settings)
    memory = Memory(warehouse)
    memory.seed_defaults()
    findings = scan(warehouse, settings)
    guarded = apply_guards(
        findings,
        calendar=memory.calendar(),
        open_cases=memory.open_cases(),
        settings=settings,
    )
    primary = select_primary(guarded["alerts"])
    if primary is None:
        return {
            "alerts": guarded["alerts"],
            "suppressed": guarded["suppressed"],
            "duplicates": guarded["duplicates"],
            "error": "no anomaly detected",
        }
    for alert in guarded["alerts"]:
        memory.save_case(alert, status="investigating", owner=assign_owner(alert["metric"]), payload=alert)

    pack_before = diagnose(warehouse, primary, topics=[], external=[])
    spec = write_build_spec(pack_before, primary, guard)
    brief_text = agent_brief(spec, primary)
    approvals = [
        {
            "type": "build",
            "by": "data_owner",
            "status": "simulated_for_demo" if simulate_build_approval else "pending",
            "note": (
                "Local demo continues so the seeded incident can be briefed. "
                "Production Atlas leaves this pending until the data owner replies."
            ),
        },
        {"type": "merge_deploy", "by": "engineer", "status": "pending"},
        {"type": "executive_action", "by": "vp_growth", "status": "pending"},
    ]
    signal_run = None
    external_run = None
    pack = pack_before
    if pack_before.get("capability_gap") and simulate_build_approval:
        signal_run = run_ticket_signals(
            warehouse,
            client,
            case_id=primary["case_id"],
            segment=primary["segment"],
            started_at=_parse(primary["started_at"]),
            ended_at=_parse(primary["ended_at"]),
            batch_size=settings.batch_size,
            model=settings.grok_model,
        )
        external_run = run_external_signals(
            warehouse,
            client,
            case_id=primary["case_id"],
            metric=primary["metric"],
            query=_search_query(primary, pack_before),
        )
        pack = diagnose(
            warehouse,
            primary,
            topics=load_topics(warehouse, primary["case_id"]),
            external=external_run["items"],
        )
        _publish_signals(warehouse, primary["case_id"])

    note = quiet_hours_note(memory.preferences(), primary["severity"], _parse(primary["started_at"]))
    brief = exec_brief(primary, pack, quiet_hours_note=note)
    script = voice_script(primary, pack)
    voice = synthesize_voice(client, script, language="en")
    points, _ = load_series(warehouse, primary["metric"], primary["segment"])
    series = [
        {"ts": point.ts.replace(microsecond=0).isoformat() + "Z", "rate": round(point.value, 4)}
        for point in points[-72:]
    ]
    charts = chart_specs(primary, series, pack.get("slices") or {})
    case_file = build_case_file(primary, pack, approvals)
    memory.update_case(primary["case_id"], status="briefed", payload=case_file)
    from signal_service.pipeline.propagate import sync_case_facts

    sync_case_facts(warehouse, batch_id=f"cases-{primary['case_id']}")
    cases = memory.open_cases()
    week = weekly_review_page(cases, week_label="2026-11-16")
    cost = estimate_batch_cost(
        (signal_run or {}).get("classified", pack_before.get("ticket_count", 0)),
        len(script),
        guard,
    )
    verification = {
        "status": "pending_eval",
        "reasons": ["eval score is attached by the CLI after the golden-set harness runs"],
        "merge": False,
    }
    return {
        "case_card": primary,
        "other_alerts": [alert for alert in guarded["alerts"] if alert["case_id"] != primary["case_id"]],
        "suppressed": guarded["suppressed"],
        "duplicates": guarded["duplicates"],
        "diagnosis_before_signals": {
            "hypotheses": pack_before["hypotheses"],
            "capability_gap": pack_before["capability_gap"],
            "ticket_count": pack_before["ticket_count"],
        },
        "build_spec": spec,
        "agent_brief": brief_text,
        "signal_run": signal_run,
        "external_run": {
            "kept": None if external_run is None else external_run["kept"],
            "provider": None if external_run is None else external_run["provider"],
            "items": [] if external_run is None else external_run["items"],
        },
        "diagnosis_pack": pack,
        "brief": {
            "markdown": brief["markdown"],
            "voice_script": script,
            "voice": {key: value for key, value in voice.items() if key != "audio_bytes"},
            "chart_specs": charts,
            "word_count": len(script.split()),
        },
        "case_file": case_file,
        "weekly_review": week,
        "route_after_diagnosis": route_next(pack),
        "spend": {
            "recorded_usd": round(guard.spent_usd, 6),
            "live_estimate_usd": cost,
            "provider": client.provider,
        },
        "verify_pr": verification,
        "calendar_note": _calendar_note(memory, primary),
    }


def _publish_signals(warehouse, case_id: str) -> None:
    """Land classifier output in silver and gold for the batch that owns the tickets.

    Snowflake runs the MERGE inside ``LOAD_STAGING`` so the client does not
    read the bronze page. DuckDB merges in process.
    """
    from signal_service.pipeline.ingest import SEED_BATCH_ID
    from signal_service.pipeline.propagate import mirror_signals

    batch_id = SEED_BATCH_ID
    try:
        rows = warehouse.query(
            """
            SELECT _batch_id
            FROM bronze_support_tickets
            WHERE ticket_id IN (
                SELECT ticket_id FROM signals.ticket_topics WHERE case_id = ?
            )
            LIMIT 1
            """,
            [case_id],
        )
    except Exception:
        rows = []
    if rows and rows[0].get("_batch_id"):
        batch_id = rows[0]["_batch_id"]
    if getattr(warehouse, "backend", "duckdb") == "snowflake":
        try:
            warehouse.execute("CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(?)", [batch_id])
            return
        except Exception:
            pass
    mirror_signals(warehouse, batch_id)


def _calendar_note(memory: Memory, card: dict) -> str:
    started = _parse(card["started_at"])
    covering = [
        event
        for event in memory.calendar()
        if event["start"] <= started < event["end"]
    ]
    if covering:
        return "A calendar event covers the incident start: " + ", ".join(event["id"] for event in covering)
    return "No promo, freeze, or holiday covers the incident start."
