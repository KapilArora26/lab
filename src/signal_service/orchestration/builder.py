from __future__ import annotations

import json

from signal_service.safety.cost import SpendGuard


def estimate_batch_cost(ticket_count: int, brief_characters: int, guard: SpendGuard) -> dict:
    classify = guard.estimate_tokens(ticket_count * 500, ticket_count * 120)
    search = guard.estimate_tokens(800, 400)
    speech = brief_characters / 1_000_000 * guard.tts_usd_per_million_chars
    return {
        "classify_usd": round(classify, 6),
        "search_usd": round(search, 6),
        "speech_usd": round(speech, 6),
        "total_usd": round(classify + search + speech, 6),
        "ticket_count": ticket_count,
        "assumptions": "500 input and 120 output tokens per ticket, one search call, TTS at the configured character rate.",
    }


def write_build_spec(pack: dict, card: dict, guard: SpendGuard) -> dict:
    gap = pack.get("capability_gap") or {
        "id": "none",
        "summary": "No capability gap.",
        "proposed_module": None,
        "table": None,
        "ticket_count": pack.get("ticket_count", 0),
    }
    cost = estimate_batch_cost(int(gap.get("ticket_count") or 0), 900, guard)
    return {
        "module": gap.get("proposed_module"),
        "reason": gap.get("summary"),
        "target_table": gap.get("table"),
        "case_id": card["case_id"],
        "taxonomy": "data/taxonomy.json",
        "acceptance": [
            "golden-set topic accuracy >= 0.85",
            "no PII in logs or prompts",
            "daily spend cap enforced",
            "external items without a source URL are dropped",
        ],
        "estimated_cost_usd": cost,
        "data_touched": ["support_tickets.text_redacted", gap.get("table")],
        "merge": False,
    }


def agent_brief(spec: dict, card: dict) -> str:
    redacted_case = {
        "case_id": card["case_id"],
        "metric": card["metric"],
        "segment": card["segment"],
        "delta_pct": card["delta_pct"],
        "started_at": card["started_at"],
        "severity": card["severity"],
    }
    return "\n".join(
        [
            "# Agent brief",
            "",
            "Problem: structured metrics show a checkout drop, and the incident window's support text has no topic signal.",
            "",
            "## Triggering case (redacted)",
            "```json",
            json.dumps(redacted_case, indent=2),
            "```",
            "",
            "## Target",
            f"- Module: {spec.get('module')}",
            f"- Table: {spec.get('target_table')}",
            f"- Taxonomy: {spec.get('taxonomy')}",
            "",
            "## Acceptance",
            *[f"- {item}" for item in spec.get("acceptance") or []],
            "",
            "## Constraints",
            "- Read XAI_API_KEY from the environment. Use structured outputs.",
            "- Redact before send. Reject rows that fail the check. Never log raw text.",
            "- Timeouts, bounded retries, batch size, and the daily spend cap are mandatory.",
            "- Model default is grok-4.7. Endpoints: /v1/chat/completions, /v1/responses, /v1/tts.",
            "- Follow .cursor/skills/add-signal-module and the Grok skills.",
            "",
            f"Estimated cost: {spec.get('estimated_cost_usd', {}).get('total_usd')} USD.",
            "",
        ]
    )


def verify_pr(report: dict) -> dict:
    reasons = []
    if not report.get("tests_passed"):
        reasons.append("tests failed")
    score = report.get("eval_topic_accuracy")
    threshold = float(report.get("eval_threshold", 0.85))
    if score is None or float(score) < threshold:
        reasons.append("eval below threshold")
    if "schema_diff" not in report:
        reasons.append("schema diff missing")
    if "cost_estimate_usd" not in report:
        reasons.append("cost estimate missing")
    return {"status": "pass" if not reasons else "fail", "reasons": reasons, "merge": False}
