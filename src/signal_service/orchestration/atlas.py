"""Lead-bot operations. This module does not query sessions, orders, payments, or tickets."""

from __future__ import annotations

from datetime import datetime, timedelta


def assign_owner(metric: str) -> str:
    if metric in {"checkout_conversion", "payment_success_rate"}:
        return "vp_growth"
    return "analytics_lead"


def quiet_hours_note(preferences: dict, severity: str, started_at: datetime) -> str | None:
    window = (preferences.get("vp_growth") or {}).get("quiet_hours")
    if not window or "-" not in window:
        return None
    start_text, end_text = window.split("-", 1)
    start_hour = int(start_text.split(":")[0])
    end_hour = int(end_text.split(":")[0])
    hour = started_at.hour
    if start_hour <= end_hour:
        inside = start_hour <= hour < end_hour
    else:
        inside = hour >= start_hour or hour < end_hour
    if not inside:
        return None
    if severity == "high":
        return (
            "VP Growth is inside quiet hours. High severity overrides that preference, "
            "so the brief is delivered now."
        )
    return "VP Growth is inside quiet hours. The brief is held until the window ends."


def sla_nudges(cases: list[dict], now: datetime, *, ack_minutes: int = 30, escalate_minutes: int = 120) -> list[dict]:
    nudges = []
    for case in cases:
        if case.get("status") in {"closed", "briefed", "awaiting_action"}:
            continue
        opened = case.get("opened_at")
        if isinstance(opened, str):
            opened = datetime.fromisoformat(opened.replace("Z", ""))
        if not isinstance(opened, datetime):
            continue
        age = now - opened
        if case.get("acknowledged_at") is None and age >= timedelta(minutes=ack_minutes):
            nudges.append({"case_id": case["case_id"], "action": "nudge", "age_minutes": int(age.total_seconds() // 60)})
        if age >= timedelta(minutes=escalate_minutes):
            nudges.append({"case_id": case["case_id"], "action": "escalate", "age_minutes": int(age.total_seconds() // 60)})
    return nudges


def freshness_check(
    *,
    latest_classified_at: datetime | None,
    now: datetime,
    error_rate: float,
    spend_usd: float,
    daily_cap_usd: float,
    max_age_hours: int = 26,
) -> dict:
    age_hours = None
    stale = latest_classified_at is None
    if latest_classified_at is not None:
        age_hours = (now - latest_classified_at).total_seconds() / 3600
        stale = age_hours > max_age_hours
    over_budget = spend_usd > daily_cap_usd
    error_breach = error_rate > 0.05
    return {
        "ok": not stale and not over_budget and not error_breach,
        "stale": stale,
        "age_hours": None if age_hours is None else round(age_hours, 2),
        "error_rate": error_rate,
        "spend_usd": spend_usd,
        "over_budget": over_budget,
    }


def build_case_file(card: dict, pack: dict, approvals: list[dict]) -> dict:
    return {
        "case_id": card["case_id"],
        "schema_version": 1,
        "metric": card["metric"],
        "segment": card["segment"],
        "delta_pct": card["delta_pct"],
        "started_at": card["started_at"],
        "revenue_at_risk_usd_per_hour": card["revenue_at_risk_usd_per_hour"],
        "severity": card["severity"],
        "hypotheses": [
            {
                "id": item["id"],
                "claim": item["claim"],
                "confidence": item["confidence"],
                "evidence": item["evidence"],
            }
            for item in pack["hypotheses"]
        ],
        "capability_gap": pack.get("capability_gap"),
        "approvals": approvals,
    }


def route_next(pack: dict) -> str:
    if pack.get("capability_gap"):
        return "builder"
    lead = (pack.get("hypotheses") or [{}])[0]
    if float(lead.get("confidence") or 0) >= 0.7:
        return "narrator"
    return "diagnostician"
