"""Deterministic keyword classifier used when the Grok API is unavailable."""

from __future__ import annotations

from signal_service.grok.schemas import TicketClassification

_UPI = ("fail", "failed", "failure", "otp", "declined")
_CARD = ("card was declined", "card declined", "cvv", "insufficient funds")
_REFUND = ("refund", "money back", "charged twice")
_LOGIN = ("log in", "login", "password", "sign in")
_SHIPPING = ("shipping", "delivery", "tracking", "not arrived", "hasn't arrived")
_PROMO = ("coupon", "promo code", "discount code", "voucher")
_QUALITY = ("damaged", "broken item", "product quality", "defective")
_CHECKOUT = ("checkout", "pay button", "cart stuck", "place order")
_POSITIVE = ("love", "thanks", "great", "wonderful")


def classify_text(ticket_id: str, text: str) -> TicketClassification:
    lowered = text.lower()
    if "upi" in lowered and any(token in lowered for token in _UPI):
        topic, sentiment, severity, area = "upi_payment_failure", "negative", "high", "payments"
    elif any(token in lowered for token in _CARD):
        topic, sentiment, severity, area = "card_decline", "negative", "high", "payments"
    elif any(token in lowered for token in _REFUND):
        topic, sentiment, severity, area = "refund_request", "negative", "medium", "payments"
    elif any(token in lowered for token in _LOGIN):
        topic, sentiment, severity, area = "login_issue", "negative", "medium", "account"
    elif any(token in lowered for token in _SHIPPING):
        topic, sentiment, severity, area = "shipping_delay", "negative", "medium", "shipping"
    elif any(token in lowered for token in _PROMO):
        topic, sentiment, severity, area = "promo_confusion", "neutral", "low", "checkout"
    elif any(token in lowered for token in _QUALITY):
        topic, sentiment, severity, area = "product_quality", "negative", "medium", "catalog"
    elif any(token in lowered for token in _CHECKOUT):
        topic, sentiment, severity, area = "checkout_bug", "negative", "medium", "checkout"
    elif any(token in lowered for token in _POSITIVE):
        topic, sentiment, severity, area = "other", "positive", "low", "other"
    else:
        topic, sentiment, severity, area = "other", "neutral", "low", "other"
    confidence = 0.35 if topic == "other" else 0.66
    return TicketClassification(
        id=ticket_id,
        topic=topic,
        sentiment=sentiment,
        severity=severity,
        product_area=area,
        evidence_snippet=text[:140],
        confidence=confidence,
    )
