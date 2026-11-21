from __future__ import annotations

from fastapi.testclient import TestClient

from signal_service.api.app import create_app


def test_health_and_pipeline(demo_wh):
    client = TestClient(create_app(demo_wh))
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["grok"] == "mock"
    response = client.post("/v1/pipeline/run")
    assert response.status_code == 200
    body = response.json()
    assert body["case_card"]["segment"]["country"] == "IN"
    assert body["diagnosis_pack"]["hypotheses"][0]["claim"] == "Payment gateway failures on UPI"
    evaluation = client.get("/v1/eval")
    assert evaluation.status_code == 200
    assert evaluation.json()["passed"] is True
