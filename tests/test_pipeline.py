from __future__ import annotations

from signal_service.grok.client import MockGrokClient
from signal_service.narrative.brief import weekly_review_page
from signal_service.orchestration.pipeline import run_pipeline


def _ids(pack: dict) -> set[str]:
    return {item["id"] for item in pack["evidence"]}


def test_seeded_incident_end_to_end(demo_wh):
    result = run_pipeline(demo_wh, MockGrokClient(), simulate_build_approval=True)
    card = result["case_card"]
    assert card["metric"] == "checkout_conversion"
    assert card["segment"] == {"platform": "android", "country": "IN"}
    assert card["started_at"].startswith("2026-11-20T02:00:00")
    assert -15 <= card["delta_pct"] <= -8
    assert card["severity"] == "high"
    assert card["revenue_at_risk_usd_per_hour"] >= 5000
    assert card["z_score"] <= -3
    assert card["query_hashes"]
    assert "No promo" in result["calendar_note"]

    before = result["diagnosis_before_signals"]
    assert before["capability_gap"]["proposed_module"] == "ticket_signals"
    assert before["hypotheses"][0]["confidence"] < 0.75
    claims = {item["claim"] for item in before["hypotheses"]}
    assert any("4.2.1" in claim for claim in claims)
    assert any("UPI" in claim for claim in claims)

    assert result["signal_run"]["rejected_pii"] == 3
    assert result["signal_run"]["classified"] >= 50
    assert result["signal_run"]["provider"] == "mock"
    assert result["external_run"]["kept"] == 2
    assert result["spend"]["recorded_usd"] == 0
    assert result["spend"]["live_estimate_usd"]["total_usd"] < 1

    pack = result["diagnosis_pack"]
    assert pack["capability_gap"] is None
    lead = pack["hypotheses"][0]
    assert lead["claim"] == "Payment gateway failures on UPI"
    assert 0.78 <= lead["confidence"] <= 0.90
    assert lead["confidence"] > before["hypotheses"][1]["confidence"] or "UPI" in before["hypotheses"][0]["claim"]
    upi_before = next(item for item in before["hypotheses"] if "UPI" in item["claim"])
    assert lead["confidence"] > upi_before["confidence"]
    for hypothesis in pack["hypotheses"]:
        assert set(hypothesis["evidence"]) <= _ids(pack) | set(card["query_hashes"])
    assert result["route_after_diagnosis"] == "narrator"

    brief = result["brief"]
    assert "Payment gateway failures on UPI" in brief["markdown"]
    assert "$" in brief["markdown"]
    for sentence in brief["markdown"].split("\n"):
        if sentence.startswith("- ") and "evidence:" not in sentence:
            raise AssertionError(sentence)
    assert 140 <= brief["word_count"] <= 210
    assert brief["voice"]["audio_path"] is None
    assert brief["chart_specs"][0]["data"]["values"]
    approvals = {item["type"]: item["status"] for item in result["case_file"]["approvals"]}
    assert approvals["build"] == "simulated_for_demo"
    assert approvals["merge_deploy"] == "pending"
    assert approvals["executive_action"] == "pending"
    assert result["case_file"]["capability_gap"] is None
    assert result["build_spec"]["merge"] is False


def test_second_sweep_dedupes_the_open_case(demo_wh):
    first = run_pipeline(demo_wh, MockGrokClient(), simulate_build_approval=True)
    second = run_pipeline(demo_wh, MockGrokClient(), simulate_build_approval=True)
    assert first["case_card"]["case_id"].startswith("C-2026-1120-")
    assert second["error"] == "no anomaly detected"
    assert second["duplicates"]
    assert second["alerts"] == []


def test_weekly_page_uses_only_case_fields():
    page = weekly_review_page(
        [{"case_id": "C-1", "metric": "checkout_conversion", "severity": "high", "status": "briefed", "hypotheses": [{"claim": "Payment gateway failures on UPI"}]}],
        week_label="2026-11-16",
    )
    assert "C-1" in page
    assert "Payment gateway failures on UPI" in page
