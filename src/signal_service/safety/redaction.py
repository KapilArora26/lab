from __future__ import annotations

import re

EMAIL = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE = re.compile(r"(?:\+?\d{1,3}[\s.\-])?\d{5}[\s.\-]\d{5}")
CARD = re.compile(r"\d[\d \-]{11,22}\d")
GOVT_ID = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
UPI_ID = re.compile(r"\b[A-Z0-9.\-]{2,}@[A-Z]{2,64}\b", re.IGNORECASE)


class RedactionError(ValueError):
    """Raised when text still contains a PII pattern."""


def pii_kinds(text: str) -> list[str]:
    found: list[str] = []
    if EMAIL.search(text):
        found.append("email")
    if PHONE.search(text):
        found.append("phone")
    if _has_card(text):
        found.append("card")
    if GOVT_ID.search(text):
        found.append("government_id")
    if UPI_ID.search(text) and "email" not in found:
        found.append("upi_id")
    return found


def _has_card(text: str) -> bool:
    for match in CARD.finditer(text):
        digits = re.sub(r"\D", "", match.group())
        if 13 <= len(digits) <= 19:
            return True
    return False


def assert_redacted(text: str) -> None:
    kinds = pii_kinds(text)
    if kinds:
        raise RedactionError("row failed redaction check: " + ",".join(kinds))
