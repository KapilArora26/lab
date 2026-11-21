"""Seasonality-aware residual z-scores.

This is an additive STL-style decomposition: a centered moving-average trend,
hour-of-day seasonal indices, and a robust z-score on the residual. The seasonal
model is fit on history before the trailing window so a fresh level shift is not
absorbed into the trend. A small sigma floor keeps a perfectly flat series from
dividing by zero.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class HourlyPoint:
    ts: datetime
    value: float
    volume: float


@dataclass(frozen=True)
class Detection:
    started_at: datetime
    ended_at: datetime
    consecutive: int
    z_score: float
    baseline_rate: float
    current_rate: float
    expected_rate: float
    delta_pct: float
    volume: float


def _moving_average(values: list[float], period: int) -> list[float | None]:
    count = len(values)
    trend: list[float | None] = [None] * count
    if period < 2 or count < period:
        return trend
    half = period // 2
    cumulative = [0.0]
    for value in values:
        cumulative.append(cumulative[-1] + value)
    for index in range(count):
        left = index - half
        right = left + period
        if left < 0 or right > count:
            continue
        trend[index] = (cumulative[right] - cumulative[left]) / period
    return trend


def fit_seasonal(points: list[HourlyPoint], period: int) -> tuple[list[float], float]:
    values = [point.value for point in points]
    trend = _moving_average(values, period)
    buckets: list[list[float]] = [[] for _ in range(period)]
    trend_values: list[float] = []
    for index, point in enumerate(points):
        if trend[index] is None:
            continue
        buckets[point.ts.hour % period].append(values[index] - trend[index])
        trend_values.append(trend[index])
    seasonal: list[float] = []
    for bucket in buckets:
        seasonal.append(sum(bucket) / len(bucket) if bucket else 0.0)
    center = sum(seasonal) / period
    seasonal = [value - center for value in seasonal]
    level = sum(trend_values) / len(trend_values) if trend_values else sum(values) / len(values)
    return seasonal, level


def _robust_center(residuals: list[float], floor: float) -> tuple[float, float]:
    if not residuals:
        return 0.0, floor
    center = statistics.median(residuals)
    mad = statistics.median(abs(value - center) for value in residuals)
    sigma = max(1.4826 * mad, floor)
    return center, sigma


def detect_series(
    points: list[HourlyPoint],
    *,
    period: int = 24,
    z_threshold: float = 3.0,
    min_volume: float = 100.0,
    min_consecutive: int = 2,
    fit_exclude_tail: int = 48,
    sigma_floor: float = 0.002,
    direction: str = "drop",
) -> Detection | None:
    """Return the latest consecutive anomaly that reaches the end of the series."""
    if len(points) < period * 3:
        return None
    ordered = sorted(points, key=lambda point: point.ts)
    tail = min(fit_exclude_tail, max(period, len(ordered) // 3))
    fit = ordered[:-tail]
    if len(fit) < period * 2:
        return None
    seasonal, level = fit_seasonal(fit, period)
    fit_residuals = [
        point.value - (level + seasonal[point.ts.hour % period]) for point in fit
    ]
    center, sigma = _robust_center(fit_residuals, sigma_floor)

    scored: list[tuple[HourlyPoint, float, float]] = []
    for point in ordered[-tail:]:
        expected = level + seasonal[point.ts.hour % period]
        z_score = ((point.value - expected) - center) / sigma
        scored.append((point, z_score, expected))

    run: list[tuple[HourlyPoint, float, float]] = []
    for point, z_score, expected in reversed(scored):
        anomalous = point.volume >= min_volume and (
            (direction == "drop" and z_score <= -z_threshold)
            or (direction == "spike" and z_score >= z_threshold)
        )
        if anomalous:
            run.append((point, z_score, expected))
            continue
        if not run and point.volume < min_volume:
            continue
        break
    run.reverse()
    if len(run) < min_consecutive:
        return None

    volume = sum(point.volume for point, _, _ in run)
    current = sum(point.value * point.volume for point, _, _ in run) / volume
    expected = sum(expected * point.volume for point, _, expected in run) / volume
    if expected == 0:
        return None
    delta_pct = (current - expected) / abs(expected) * 100
    if direction == "drop" and delta_pct >= 0:
        return None
    if direction == "spike" and delta_pct <= 0:
        return None
    fit_volume = sum(point.volume for point in fit)
    baseline = sum(point.value * point.volume for point in fit) / fit_volume
    z_values = [z_score for _, z_score, _ in run]
    return Detection(
        started_at=run[0][0].ts,
        ended_at=run[-1][0].ts,
        consecutive=len(run),
        z_score=sum(z_values) / len(z_values),
        baseline_rate=baseline,
        current_rate=current,
        expected_rate=expected,
        delta_pct=delta_pct,
        volume=volume / len(run),
    )
