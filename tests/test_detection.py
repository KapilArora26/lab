from __future__ import annotations

import math
from datetime import datetime, timedelta

import pytest

from signal_service.detection.scoring import (
    calendar_suppression,
    is_duplicate,
    revenue_at_risk_per_hour,
    severity_band,
)
from signal_service.detection.seasonality import HourlyPoint, detect_series


def _points(hours: int, *, drop_hours: int = 0, drop: float = 0.0, volume: float = 500) -> list[HourlyPoint]:
    start = datetime(2026, 1, 1)
    points = []
    for index in range(hours):
        ts = start + timedelta(hours=index)
        value = 0.10 + 0.01 * math.sin(2 * math.pi * ts.hour / 24)
        if drop_hours and index >= hours - drop_hours:
            value -= drop
        points.append(HourlyPoint(ts=ts, value=value, volume=volume))
    return points


def test_seasonal_drop_is_detected():
    points = _points(24 * 14, drop_hours=6, drop=0.02)
    found = detect_series(points)
    assert found is not None
    assert found.started_at == points[-6].ts
    assert found.z_score <= -3
    assert found.delta_pct < -10
    assert found.consecutive == 6


def test_seasonality_alone_is_not_an_anomaly():
    assert detect_series(_points(24 * 14)) is None


def test_low_volume_is_not_an_anomaly():
    points = _points(24 * 14, drop_hours=6, drop=0.02, volume=20)
    assert detect_series(points, min_volume=100) is None


def test_single_hour_blip_needs_persistence():
    points = _points(24 * 14, drop_hours=1, drop=0.03)
    assert detect_series(points, min_consecutive=2) is None


def test_severity_and_revenue():
    per_hour = revenue_at_risk_per_hour(0.10, 0.088, 500, 48, 15.625)
    assert per_hour == pytest.approx(4500)
    assert severity_band(per_hour) == "medium"
    assert severity_band(8261) == "high"
    assert severity_band(100) == "low"


def test_calendar_suppresses_matching_window_only():
    started = datetime(2026, 11, 20, 2)
    promo = {
        "id": "promo",
        "kind": "promo",
        "start": "2026-11-20T00:00:00Z",
        "end": "2026-11-20T06:00:00Z",
        "metric": "checkout_conversion",
        "segment": {},
    }
    assert calendar_suppression([promo], metric="checkout_conversion", segment={"platform": "android"}, started_at=started)
    later = dict(promo, start="2026-11-21T00:00:00Z", end="2026-11-21T06:00:00Z")
    assert calendar_suppression([later], metric="checkout_conversion", segment={}, started_at=started) is None
    ios = {
        "id": "freeze",
        "kind": "freeze",
        "start": "2026-11-20T00:00:00Z",
        "end": "2026-11-20T06:00:00Z",
        "metric": None,
        "segment": {"platform": "ios"},
    }
    assert calendar_suppression([ios], metric="checkout_conversion", segment={"platform": "android", "country": "IN"}, started_at=started) is None


def test_duplicate_open_case_matches_metric_and_segment():
    open_cases = [{"metric": "checkout_conversion", "segment": {"country": "IN", "platform": "android"}, "status": "investigating"}]
    assert is_duplicate("checkout_conversion", {"platform": "android", "country": "IN"}, open_cases)
    assert not is_duplicate("checkout_conversion", {"platform": "ios", "country": "IN"}, open_cases)
    closed = [dict(open_cases[0], status="closed")]
    assert not is_duplicate("checkout_conversion", {"platform": "android", "country": "IN"}, closed)
