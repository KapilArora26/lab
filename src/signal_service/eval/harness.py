from __future__ import annotations

import json

from signal_service.grok.schemas import TicketClassification
from signal_service.paths import data_path
from signal_service.signals.fallback import classify_text

TOPIC_THRESHOLD = 0.85
FIELD_THRESHOLD = 0.80
MIN_ROWS = 50
FIELDS = ("topic", "sentiment", "severity", "product_area")


def load_golden(path=None) -> list[dict]:
    source = path or data_path("golden", "tickets.jsonl")
    rows = []
    for line in source.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def run_eval(path=None) -> dict:
    rows = load_golden(path)
    correct = {field: 0 for field in FIELDS}
    schema_errors = 0
    for row in rows:
        prediction = classify_text(row["id"], row["text"])
        try:
            TicketClassification.model_validate(prediction.model_dump())
        except Exception:
            schema_errors += 1
        for field in FIELDS:
            if getattr(prediction, field) == row[field]:
                correct[field] += 1
    count = len(rows)
    accuracy = {field: (correct[field] / count if count else 0.0) for field in FIELDS}
    passed = (
        count >= MIN_ROWS
        and schema_errors == 0
        and accuracy["topic"] >= TOPIC_THRESHOLD
        and all(accuracy[field] >= FIELD_THRESHOLD for field in FIELDS)
    )
    return {
        "n": count,
        "accuracy": {field: round(value, 4) for field, value in accuracy.items()},
        "schema_errors": schema_errors,
        "topic_threshold": TOPIC_THRESHOLD,
        "field_threshold": FIELD_THRESHOLD,
        "passed": passed,
        "classifier": "fallback-keywords",
    }
