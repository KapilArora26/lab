from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from signal_service.orchestration.atlas import (
    build_case_file,
    freshness_check,
    quiet_hours_note,
    route_next,
    sla_nudges,
)
from signal_service.orchestration.builder import verify_pr


def test_atlas_and_narrator_do_not_query_metric_tables():
    root = Path(__file__).resolve().parents[1]
    for relative in ("src/signal_service/orchestration/atlas.py", "src/signal_service/narrative/brief.py"):
        text = (root / relative).read_text(encoding="utf-8").lower()
        assert "from sessions" not in text
        assert "support_tickets" not in text
        assert "select " not in text


def test_quiet_hours_high_severity_overrides_and_sla_nudges():
    preferences = {"vp_growth": {"quiet_hours": "22:00-07:00"}}
    note = quiet_hours_note(preferences, "high", datetime(2026, 11, 20, 2, 15))
    assert note is not None and "overrides" in note
    assert quiet_hours_note(preferences, "low", datetime(2026, 11, 20, 2, 15)).startswith("VP Growth is inside")
    assert quiet_hours_note(preferences, "high", datetime(2026, 11, 20, 15, 0)) is None
    now = datetime(2026, 11, 20, 4, 0)
    nudges = sla_nudges(
        [{"case_id": "C-1", "status": "investigating", "opened_at": now - timedelta(hours=3), "acknowledged_at": None}],
        now,
    )
    assert {item["action"] for item in nudges} == {"nudge", "escalate"}


def test_freshness_and_verify_pr_never_merges():
    now = datetime(2026, 11, 21, 12)
    fresh = freshness_check(
        latest_classified_at=now - timedelta(hours=2),
        now=now,
        error_rate=0.01,
        spend_usd=1,
        daily_cap_usd=25,
    )
    assert fresh["ok"]
    stale = freshness_check(
        latest_classified_at=now - timedelta(hours=48),
        now=now,
        error_rate=0.2,
        spend_usd=30,
        daily_cap_usd=25,
    )
    assert not stale["ok"]
    report = verify_pr(
        {
            "tests_passed": True,
            "eval_topic_accuracy": 0.88,
            "eval_threshold": 0.85,
            "schema_diff": "signals.ticket_topics",
            "cost_estimate_usd": 0.14,
        }
    )
    assert report["status"] == "pass"
    assert report["merge"] is False
    failed = verify_pr({"tests_passed": False, "eval_topic_accuracy": 0.5, "cost_estimate_usd": 1})
    assert failed["status"] == "fail"
    assert failed["merge"] is False


def test_case_file_shape():
    card = {
        "case_id": "C-2026-1120-001",
        "metric": "checkout_conversion",
        "segment": {"platform": "android", "country": "IN"},
        "delta_pct": -11.2,
        "started_at": "2026-11-20T02:00:00Z",
        "revenue_at_risk_usd_per_hour": 8400,
        "severity": "high",
    }
    pack = {
        "hypotheses": [{"id": "H1", "claim": "Payment gateway failures on UPI", "confidence": 0.8, "evidence": ["q_a"]}],
        "capability_gap": None,
    }
    case_file = build_case_file(card, pack, [{"type": "build", "by": "data_owner", "status": "pending"}])
    for key in (
        "case_id",
        "schema_version",
        "metric",
        "segment",
        "delta_pct",
        "started_at",
        "revenue_at_risk_usd_per_hour",
        "severity",
        "hypotheses",
        "capability_gap",
        "approvals",
    ):
        assert key in case_file
    assert route_next({"capability_gap": {"id": "ticket_topics_missing"}, "hypotheses": []}) == "builder"
    assert route_next(pack) == "narrator"
