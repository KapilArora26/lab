from __future__ import annotations

import json
import logging

import httpx
import pytest

from signal_service.grok.client import LiveGrokClient, MockGrokClient, SchemaError
from signal_service.safety.cost import CircuitBreakerOpen, SpendGuard
from signal_service.safety.redaction import RedactionError


def _guard() -> SpendGuard:
    return SpendGuard(25, 2, 6, 15)


def _classification(ticket_id: str, text: str) -> dict:
    return {
        "id": ticket_id,
        "topic": "upi_payment_failure",
        "sentiment": "negative",
        "severity": "high",
        "product_area": "payments",
        "evidence_snippet": text[:40],
        "confidence": 0.9,
    }


def test_mock_drops_unsourced_external_items_and_rejects_pii():
    client = MockGrokClient(_guard())
    items = client.search("android India UPI checkout conversion drop")
    assert len(items) == 2
    assert all(item["source_url"].startswith("https://") for item in items)
    with pytest.raises(RedactionError):
        client.classify_batch([{"id": "x", "text": "mail me at a@b.com"}])


def test_live_client_retries_then_validates_and_does_not_log_text(caplog):
    text = "UPI payment failed after OTP on Android checkout."
    secret = "super-secret-token-value"
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, json={"error": "busy"})
        body = {
            "choices": [{"message": {"content": json.dumps({"items": [_classification("t1", text)]})}}],
            "usage": {"prompt_tokens": 20, "completion_tokens": 8},
        }
        return httpx.Response(200, json=body)

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.x.ai/v1")
    client = LiveGrokClient(secret, _guard(), http=http, max_retries=3, sleeper=lambda _delay: None)
    with caplog.at_level(logging.INFO):
        rows = client.classify_batch([{"id": "t1", "text": text}])
    assert rows[0]["topic"] == "upi_payment_failure"
    assert calls["n"] == 3
    assert text not in caplog.text
    assert secret not in caplog.text
    assert "input_tokens=20" in caplog.text


def test_schema_failure_is_retried():
    text = "UPI payment failed after OTP."
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            content = json.dumps({"items": []})
        else:
            content = json.dumps({"items": [_classification("t1", text)]})
        body = {"choices": [{"message": {"content": content}}], "usage": {"prompt_tokens": 4, "completion_tokens": 4}}
        return httpx.Response(200, json=body)

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.x.ai/v1")
    client = LiveGrokClient("k", _guard(), http=http, max_retries=2, sleeper=lambda _delay: None)
    rows = client.classify_batch([{"id": "t1", "text": text}])
    assert rows[0]["id"] == "t1"
    assert calls["n"] == 2
    client.max_retries = 1
    calls["n"] = 0

    def always_bad(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        body = {"choices": [{"message": {"content": "{\"items\": []}"}}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}}
        return httpx.Response(200, json=body)

    client.http = httpx.Client(transport=httpx.MockTransport(always_bad), base_url="https://api.x.ai/v1")
    with pytest.raises(SchemaError):
        client.classify_batch([{"id": "t1", "text": text}])


def test_circuit_breaker_does_not_call_the_api():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={})

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.x.ai/v1")
    guard = SpendGuard(0, 2, 6, 15)
    client = LiveGrokClient("k", guard, http=http, max_retries=1)
    with pytest.raises(CircuitBreakerOpen):
        client.classify_batch([{"id": "t1", "text": "UPI payment failed after OTP."}])
    assert calls["n"] == 0
