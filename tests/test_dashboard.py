"""The dashboard reads gold and the audit table. Chat cannot write."""

from __future__ import annotations

import inspect
from pathlib import Path

from fastapi.testclient import TestClient

from signal_service.api import dashboard
from signal_service.api.app import create_app
from signal_service.grok.client import MockGrokClient
from signal_service.orchestration.pipeline import run_pipeline
from signal_service.pipeline.ingest import SEED_BATCH_ID


def _count(warehouse, sql: str) -> int:
    return int(warehouse.query(sql)[0]["n"])


def test_dashboard_code_does_not_name_earlier_layers():
    text = Path(dashboard.__file__).read_text(encoding="utf-8").lower()
    assert "bronze_" not in text
    assert "silver_" not in text
    assert "ingest" not in inspect.getsource(dashboard.answer)
    assert "ingest" not in inspect.getsource(dashboard.collect)


def test_dashboard_page_and_gold_data(demo_wh):
    client = TestClient(create_app(demo_wh))
    page = client.get("/dashboard")
    assert page.status_code == 200
    html = page.text
    assert "Data quality" in html
    assert "chart.js" in html
    assert "60000" in html
    assert "Load new data" in html
    assert "confirm(" in html
    assert "/v1/ingest" in html
    assert "/v1/dashboard/chat" in html
    data = client.get("/v1/dashboard/data")
    assert data.status_code == 200
    body = data.json()
    assert {row["layer"] for row in body["health"]} == {"bronze", "silver", "gold"}
    assert body["conversion"]
    assert body["anomaly_count"] > 0
    assert body["revenue_at_risk_usd"] > 0
    assert body["spend"][0]["cap_usd"] == 25
    assert body["cases"] == []
    assert "batch_id" in body["lag"][0]
    assert "staging_to_gold_seconds" in body["lag"][0]
    runs = client.get("/v1/dashboard/runs", params={"batch_id": SEED_BATCH_ID})
    assert runs.status_code == 200
    layers = [row["layer"] for row in runs.json()]
    assert layers == ["bronze", "silver", "gold"]


def test_chat_is_read_only_and_grounded(demo_wh):
    before_runs = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM pipeline_runs")
    before_groups = _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups")
    client = TestClient(create_app(demo_wh))
    response = client.post(
        "/v1/dashboard/chat",
        json={"persona": "narrator", "message": "What is at risk?"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["provider"] == "mock"
    assert body["persona"] == "narrator"
    assert "Revenue at risk" in body["reply"]
    assert "$" in body["reply"]
    refused = client.post(
        "/v1/dashboard/chat",
        json={"persona": "narrator", "message": "Email jane.doe@example.com"},
    )
    assert refused.status_code == 400
    unknown = client.post(
        "/v1/dashboard/chat",
        json={"persona": "operator", "message": "Hello"},
    )
    assert unknown.status_code == 400
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM pipeline_runs") == before_runs
    assert _count(demo_wh, "SELECT COUNT(*)::INTEGER AS n FROM bronze_groups") == before_groups
    assert MockGrokClient().provider == "mock"


def test_open_cases_come_from_gold_after_the_pipeline(demo_wh):
    run_pipeline(demo_wh, MockGrokClient(), simulate_build_approval=True)
    client = TestClient(create_app(demo_wh))
    body = client.get("/v1/dashboard/data").json()
    ids = {row["case_id"] for row in body["cases"]}
    assert "C-2026-1120-001" in ids
    assert body["topics"]
    assert any(row["topic"] == "upi_payment_failure" for row in body["topics"])
