from __future__ import annotations

from datetime import datetime

from signal_service.detection.seasonality import Detection


def revenue_at_risk_per_hour(
    expected_rate: float,
    current_rate: float,
    sessions_per_hour: float,
    aov_usd: float,
    session_weight: float = 1.0,
) -> float:
    lost_rate = max(0.0, expected_rate - current_rate)
    return lost_rate * sessions_per_hour * aov_usd * session_weight


def severity_band(usd_per_hour: float, *, high: float = 5000.0, medium: float = 1000.0) -> str:
    if usd_per_hour >= high:
        return "high"
    if usd_per_hour >= medium:
        return "medium"
    return "low"


def canonical_segment(segment: dict[str, str]) -> str:
    return "|".join(f"{key}={segment[key]}" for key in sorted(segment))


def is_duplicate(metric: str, segment: dict[str, str], open_cases: list[dict]) -> bool:
    key = (metric, canonical_segment(segment))
    for case in open_cases:
        if case.get("status") == "closed":
            continue
        existing = case.get("segment") or {}
        if (case.get("metric"), canonical_segment(existing)) == key:
            return True
    return False


def calendar_suppression(
    calendar: list[dict],
    *,
    metric: str,
    segment: dict[str, str],
    started_at: datetime,
) -> dict | None:
    """Return the promo, freeze, or holiday that covers this alert, if any."""
    for event in calendar:
        start = _as_datetime(event["start"])
        end = _as_datetime(event["end"])
        if not (start <= started_at < end):
            continue
        event_metric = event.get("metric")
        if event_metric and event_metric != metric:
            continue
        event_segment = event.get("segment") or {}
        if any(segment.get(key) != value for key, value in event_segment.items()):
            continue
        if event.get("kind") in {"promo", "freeze", "holiday"}:
            return event
    return None


def _as_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = value.replace("Z", "")
    return datetime.fromisoformat(text)


def case_id_for(started_at: datetime, sequence: int) -> str:
    return f"C-{started_at:%Y-%m%d}-{sequence:03d}"
