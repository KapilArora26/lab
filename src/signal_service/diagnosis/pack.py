from __future__ import annotations

from datetime import datetime, timedelta

from signal_service.diagnosis.ranker import rank_hypotheses
from signal_service.diagnosis.slicer import slice_case


def _started(card: dict) -> datetime:
    return datetime.fromisoformat(card["started_at"].replace("Z", ""))


def count_tickets(warehouse, card: dict) -> int:
    started = _started(card)
    ended = datetime.fromisoformat(card["ended_at"].replace("Z", "")) + timedelta(hours=1)
    clauses = ["ts >= ?", "ts < ?"]
    params: list = [started, ended]
    for column in ("platform", "country"):
        if column in card["segment"]:
            clauses.append(f"{column} = ?")
            params.append(card["segment"][column])
    rows = warehouse.query(
        f"SELECT count(*)::INTEGER AS n FROM silver_support_tickets WHERE {' AND '.join(clauses)}",
        params,
    )
    return int(rows[0]["n"])


def diagnose(warehouse, card: dict, *, topics: list[dict] | None = None, external: list[dict] | None = None) -> dict:
    topics = topics or []
    external = [item for item in (external or []) if item.get("source_url")]
    sliced = slice_case(warehouse, card)
    ranked = rank_hypotheses(sliced, topics, external, _started(card))
    tickets = count_tickets(warehouse, card)
    gap = None
    if tickets >= 10 and len(topics) == 0:
        gap = {
            "id": "ticket_topics_missing",
            "summary": (
                "Support tickets in the incident window have no structured topic signal. "
                "The cause appears to sit in unstructured text."
            ),
            "proposed_module": "ticket_signals",
            "table": "signals.ticket_topics",
            "ticket_count": tickets,
        }
    return {
        "case_id": card["case_id"],
        "schema_version": 1,
        "metric": card["metric"],
        "segment": card["segment"],
        "hypotheses": ranked["hypotheses"],
        "capability_gap": gap,
        "evidence": ranked["evidence"],
        "external_signals": external,
        "ticket_count": tickets,
        "classified_count": len(topics),
        "slices": {
            name: {
                "query_hash": payload["query_hash"],
                "lost_conversions": round(payload["lost_conversions"], 2),
                "top": sorted(payload["values"], key=lambda item: -item["loss_share"])[:4],
            }
            for name, payload in sliced["dimensions"].items()
        },
    }
