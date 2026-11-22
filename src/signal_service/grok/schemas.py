from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, Field

from signal_service.paths import data_path


def load_taxonomy() -> dict:
    return json.loads(data_path("taxonomy.json").read_text(encoding="utf-8"))


class TicketClassification(BaseModel):
    id: str
    topic: str
    sentiment: Literal["negative", "neutral", "positive"]
    severity: Literal["low", "medium", "high"]
    product_area: Literal["checkout", "payments", "shipping", "account", "catalog", "other"]
    evidence_snippet: str = Field(max_length=140)
    confidence: float = Field(ge=0, le=1)


class TicketBatch(BaseModel):
    items: list[TicketClassification]


class ChatReply(BaseModel):
    """Plain-text dashboard reply. The model is not asked to call tools or write data."""

    reply: str = Field(min_length=1, max_length=4000)


class ExternalSignal(BaseModel):
    title: str
    summary: str
    source_url: str
    source_type: Literal["web", "x"]
    relevance: float = Field(ge=0, le=1)
    observed_at: str | None = None
    supports: list[str] = Field(default_factory=list)


class ExternalBatch(BaseModel):
    items: list[ExternalSignal]


def ticket_response_schema() -> dict:
    taxonomy = load_taxonomy()
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["items"],
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "id",
                        "topic",
                        "sentiment",
                        "severity",
                        "product_area",
                        "evidence_snippet",
                        "confidence",
                    ],
                    "properties": {
                        "id": {"type": "string"},
                        "topic": {"type": "string", "enum": taxonomy["topics"]},
                        "sentiment": {"type": "string", "enum": taxonomy["sentiments"]},
                        "severity": {"type": "string", "enum": taxonomy["severities"]},
                        "product_area": {"type": "string", "enum": taxonomy["product_areas"]},
                        "evidence_snippet": {"type": "string", "maxLength": 140},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    },
                },
            }
        },
    }
