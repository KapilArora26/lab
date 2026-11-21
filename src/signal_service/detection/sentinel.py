from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from signal_service.config import Settings
from signal_service.detection.scoring import (
    calendar_suppression,
    case_id_for,
    is_duplicate,
    revenue_at_risk_per_hour,
    severity_band,
)
from signal_service.detection.seasonality import Detection, HourlyPoint, detect_series
from signal_service.hashing import query_hash

SEGMENT_COLUMNS = ("platform", "country", "channel", "payment_method", "sku", "app_version")

WATCHES: list[dict] = [
    {"metric": "checkout_conversion", "segment": {"platform": "android", "country": "IN"}, "tier": 1},
    {"metric": "checkout_conversion", "segment": {"platform": "ios", "country": "IN"}, "tier": 1},
    {"metric": "checkout_conversion", "segment": {"platform": "android", "country": "US"}, "tier": 1},
    {"metric": "checkout_conversion", "segment": {}, "tier": 1},
    {"metric": "payment_success_rate", "segment": {"platform": "android", "country": "IN"}, "tier": 1},
]


@dataclass(frozen=True)
class WatchFinding:
    metric: str
    segment: dict[str, str]
    tier: int
    detection: Detection
    query_hash: str
    aov_usd: float
    session_weight: float


def _filters(segment: dict[str, str]) -> tuple[str, list]:
    unknown = set(segment) - set(SEGMENT_COLUMNS)
    if unknown:
        raise ValueError(f"unsupported segment columns: {sorted(unknown)}")
    clauses = [f"{column} = ?" for column in segment]
    params = [segment[column] for column in segment]
    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(clauses), params


def series_sql(metric: str, segment: dict[str, str]) -> tuple[str, list]:
    where, params = _filters(segment)
    if metric == "checkout_conversion":
        sql = f"""
            SELECT date_trunc('hour', ts) AS bucket,
                   count(*)::DOUBLE AS volume,
                   sum(CASE WHEN converted THEN 1 ELSE 0 END)::DOUBLE AS positives
            FROM sessions
            {where}
            GROUP BY 1
            ORDER BY 1
        """
        return sql, params
    if metric == "payment_success_rate":
        sql = f"""
            SELECT date_trunc('hour', ts) AS bucket,
                   count(*)::DOUBLE AS volume,
                   sum(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END)::DOUBLE AS positives
            FROM payments
            {where}
            GROUP BY 1
            ORDER BY 1
        """
        return sql, params
    raise ValueError(f"unknown metric {metric}")


def load_series(warehouse, metric: str, segment: dict[str, str]) -> tuple[list[HourlyPoint], str]:
    sql, params = series_sql(metric, segment)
    rows = warehouse.query(sql, params)
    points: list[HourlyPoint] = []
    for row in rows:
        volume = float(row["volume"])
        if volume <= 0:
            continue
        bucket = row["bucket"]
        if not isinstance(bucket, datetime):
            bucket = datetime.fromisoformat(str(bucket))
        if bucket.tzinfo is not None:
            bucket = bucket.replace(tzinfo=None)
        points.append(HourlyPoint(ts=bucket, value=float(row["positives"]) / volume, volume=volume))
    return points, query_hash(sql, params)


def _meta_float(warehouse, key: str, default: float) -> float:
    try:
        rows = warehouse.query("SELECT value FROM demo_meta WHERE key = ?", [key])
    except Exception:
        return default
    if not rows:
        return default
    return float(rows[0]["value"])


def _aov(warehouse, segment: dict[str, str]) -> float:
    where, params = _filters(segment)
    sql = f"SELECT avg(amount_usd)::DOUBLE AS aov FROM orders {where}"
    rows = warehouse.query(sql, params)
    if rows and rows[0]["aov"] is not None:
        return float(rows[0]["aov"])
    return _meta_float(warehouse, "aov_usd", 48.0)


def scan(warehouse, settings: Settings | None = None) -> list[WatchFinding]:
    settings = settings or Settings()
    weight = _meta_float(warehouse, "session_weight", 1.0)
    findings: list[WatchFinding] = []
    for watch in WATCHES:
        points, digest = load_series(warehouse, watch["metric"], watch["segment"])
        detection = detect_series(
            points,
            period=settings.period_hours,
            z_threshold=settings.z_threshold,
            min_volume=settings.min_volume,
            min_consecutive=settings.min_consecutive,
            fit_exclude_tail=settings.fit_exclude_tail,
            sigma_floor=settings.sigma_floor,
            direction="drop",
        )
        if detection is None:
            continue
        findings.append(
            WatchFinding(
                metric=watch["metric"],
                segment=dict(watch["segment"]),
                tier=watch["tier"],
                detection=detection,
                query_hash=digest,
                aov_usd=_aov(warehouse, watch["segment"]),
                session_weight=weight,
            )
        )
    return findings


def to_case_card(finding: WatchFinding, sequence: int, settings: Settings | None = None) -> dict:
    settings = settings or Settings()
    detection = finding.detection
    per_hour = revenue_at_risk_per_hour(
        detection.expected_rate,
        detection.current_rate,
        detection.volume,
        finding.aov_usd,
        finding.session_weight,
    )
    band = severity_band(
        per_hour,
        high=settings.high_severity_usd_per_hour,
        medium=settings.medium_severity_usd_per_hour,
    )
    return {
        "case_id": case_id_for(detection.started_at, sequence),
        "schema_version": 1,
        "metric": finding.metric,
        "segment": finding.segment,
        "magnitude": {
            "baseline_rate": round(detection.baseline_rate, 6),
            "expected_rate": round(detection.expected_rate, 6),
            "current_rate": round(detection.current_rate, 6),
            "delta_absolute": round(detection.current_rate - detection.expected_rate, 6),
            "delta_pct": round(detection.delta_pct, 2),
            "z_score": round(detection.z_score, 2),
        },
        "delta_pct": round(detection.delta_pct, 2),
        "started_at": _iso(detection.started_at),
        "ended_at": _iso(detection.ended_at),
        "revenue_at_risk_usd_per_hour": round(per_hour, 2),
        "severity": band,
        "baseline": {
            "comparison": "seasonal_stl_residual",
            "expected_rate": round(detection.expected_rate, 6),
            "history_mean_rate": round(detection.baseline_rate, 6),
            "aov_usd": round(finding.aov_usd, 2),
            "session_weight": finding.session_weight,
        },
        "query_hashes": [finding.query_hash],
        "z_score": round(detection.z_score, 2),
        "volume": round(detection.volume, 2),
        "tier": finding.tier,
        "method": "seasonal_stl_residual_zscore",
        "consecutive_hours": detection.consecutive,
    }


def apply_guards(
    findings: list[WatchFinding],
    *,
    calendar: list[dict],
    open_cases: list[dict],
    settings: Settings | None = None,
) -> dict:
    """Drop suppressed and duplicate alerts. Assign case ids to the rest."""
    settings = settings or Settings()
    alerts: list[dict] = []
    suppressed: list[dict] = []
    duplicates: list[dict] = []
    sequence = 1 + sum(1 for case in open_cases if str(case.get("case_id", "")).startswith("C-"))
    for finding in findings:
        event = calendar_suppression(
            calendar,
            metric=finding.metric,
            segment=finding.segment,
            started_at=finding.detection.started_at,
        )
        card = to_case_card(finding, sequence, settings)
        if event is not None:
            card["suppressed_by"] = event.get("id")
            suppressed.append(card)
            continue
        if is_duplicate(finding.metric, finding.segment, open_cases):
            duplicates.append(card)
            continue
        alerts.append(card)
        sequence += 1
    return {"alerts": alerts, "suppressed": suppressed, "duplicates": duplicates}


def select_primary(alerts: list[dict]) -> dict | None:
    preferred = [
        alert
        for alert in alerts
        if alert["metric"] == "checkout_conversion"
        and alert["segment"].get("platform") == "android"
        and alert["segment"].get("country") == "IN"
    ]
    if preferred:
        return preferred[0]
    if not alerts:
        return None
    return max(alerts, key=lambda alert: alert["revenue_at_risk_usd_per_hour"])


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat() + "Z"
