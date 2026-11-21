from __future__ import annotations

import pytest

from signal_service.safety.cost import CircuitBreakerOpen, SpendGuard
from signal_service.safety.redaction import RedactionError, assert_redacted, pii_kinds


def test_redaction_rejects_contact_and_card_data():
    assert pii_kinds("UPI payment failed after OTP on Android checkout.") == []
    assert_redacted("UPI payment failed after OTP on Android checkout.")
    with pytest.raises(RedactionError):
        assert_redacted("Email me at jane.doe@example.com when it is fixed.")
    with pytest.raises(RedactionError):
        assert_redacted("Call +91 98765 43210 so I can retry.")
    with pytest.raises(RedactionError):
        assert_redacted("Card 4242 4242 4242 4242 was saved.")
    with pytest.raises(RedactionError):
        assert_redacted("SSN 123-45-6789 should never be stored.")


def test_circuit_breaker_halts_before_the_cap():
    guard = SpendGuard(daily_cap_usd=1, input_usd_per_million=2, output_usd_per_million=6, tts_usd_per_million_chars=15)
    guard.authorize(0.4)
    guard.record(0.4, kind="classify", input_tokens=100, output_tokens=20)
    guard.authorize(0.4)
    with pytest.raises(CircuitBreakerOpen):
        guard.authorize(0.7)
    assert guard.halted
    assert guard.spent_usd == pytest.approx(0.4)
