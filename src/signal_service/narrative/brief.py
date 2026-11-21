from __future__ import annotations


def _segment_label(segment: dict) -> str:
    if not segment:
        return "all traffic"
    return ", ".join(f"{key} {value}" for key, value in segment.items())


def _cite(ids: list[str]) -> str:
    return ", ".join(ids)


def exec_brief(card: dict, pack: dict, *, quiet_hours_note: str | None = None) -> dict:
    """Build the decision brief. Every sentence carries evidence ids from the pack or card."""
    hypotheses = pack["hypotheses"]
    lead = hypotheses[0]
    card_evidence = card["query_hashes"]
    sentences = [
        {
            "text": (
                f"{card['metric']} for {_segment_label(card['segment'])} is "
                f"{card['delta_pct']:.1f}% versus the seasonal baseline, starting {card['started_at']}."
            ),
            "evidence": card_evidence,
        },
        {
            "text": (
                f"Revenue at risk is ${card['revenue_at_risk_usd_per_hour']:,.0f} per hour "
                f"({card['severity']} severity). Waiting 24 hours costs about "
                f"${card['revenue_at_risk_usd_per_hour'] * 24:,.0f}."
            ),
            "evidence": card_evidence,
        },
        {
            "text": (
                f"{lead['id']} ({lead['confidence']:.2f}): {lead['claim']}."
            ),
            "evidence": lead["evidence"],
        },
    ]
    if len(hypotheses) > 1:
        second = hypotheses[1]
        sentences.append(
            {
                "text": f"{second['id']} ({second['confidence']:.2f}): {second['claim']}.",
                "evidence": second["evidence"],
            }
        )
    if any(item["claim"].startswith("Payment gateway failures on UPI") for item in hypotheses):
        action_evidence = lead["evidence"]
        version_ids = []
        for item in hypotheses:
            if item.get("dimension") == "app_version":
                version_ids = item["evidence"]
        sentences.append(
            {
                "text": (
                    "Suggested action, which a person must approve: route UPI traffic to the backup "
                    "gateway, or roll back the implicated app version. Nothing has been changed."
                ),
                "evidence": action_evidence + version_ids,
            }
        )
    sentences.append(
        {
            "text": f"What would change my mind: {lead['what_would_change_my_mind']}",
            "evidence": lead["evidence"],
        }
    )
    if pack.get("capability_gap"):
        sentences.append(
            {
                "text": f"Capability gap: {pack['capability_gap']['summary']}",
                "evidence": card_evidence,
            }
        )
    if quiet_hours_note:
        sentences.append({"text": quiet_hours_note, "evidence": card_evidence})
    lines = ["# Incident brief", ""]
    for sentence in sentences:
        lines.append(f"- {sentence['text']} (evidence: {_cite(sentence['evidence'])})")
    lines.append("")
    lines.append("## Hypotheses")
    for item in hypotheses:
        lines.append(
            f"- {item['id']} {item['confidence']:.2f} {item['claim']} "
            f"(evidence: {_cite(item['evidence'])})"
        )
    return {"markdown": "\n".join(lines) + "\n", "sentences": sentences}


def voice_script(card: dict, pack: dict) -> str:
    lead = pack["hypotheses"][0]
    second = pack["hypotheses"][1] if len(pack["hypotheses"]) > 1 else None
    third = pack["hypotheses"][2] if len(pack["hypotheses"]) > 2 else None
    day_cost = card["revenue_at_risk_usd_per_hour"] * 24
    evidence_names = ", ".join(lead["evidence"][:3])
    second_sentence = ""
    if second is not None:
        second_sentence = (
            f" A second reading, {second['claim']}, sits at confidence {second['confidence']:.2f} "
            "and does not replace the lead."
        )
    third_sentence = ""
    if third is not None:
        third_sentence = f" {third['id']} at confidence {third['confidence']:.2f}: {third['claim']}."
    metric_name = card["metric"].replace("_", " ")
    script = (
        f"This is the growth brief for case {card['case_id']}. "
        f"{metric_name} on {_segment_label(card['segment'])} is down {abs(card['delta_pct']):.1f} percent "
        f"from the seasonal baseline, starting {card['started_at']}. "
        f"Revenue at risk is about {card['revenue_at_risk_usd_per_hour']:,.0f} dollars per hour, "
        f"so a full day of waiting is about {day_cost:,.0f} dollars. "
        f"Severity is {card['severity']}. "
        f"The leading hypothesis, at confidence {lead['confidence']:.2f}, is {lead['claim']}. "
        f"That reading cites {evidence_names}."
        f"{second_sentence}{third_sentence} "
        "The actions on the table are to route UPI traffic to the backup gateway, "
        "or to roll back the implicated app version. "
        "Neither action has been taken. "
        f"What would change my mind: {lead['what_would_change_my_mind']} "
        "I will check the metric again after you decide."
    )
    return " ".join(script.split())


def chart_specs(card: dict, series: list[dict], slices: dict) -> list[dict]:
    payment_values = (slices.get("payment_method") or {}).get("top") or []
    return [
        {
            "title": f"{card['metric']} {_segment_label(card['segment'])}",
            "mark": "line",
            "encoding": {"x": "ts", "y": "rate"},
            "data": {"values": series},
        },
        {
            "title": "Lost checkouts by payment method",
            "mark": "bar",
            "encoding": {"x": "value", "y": "lost_conversions"},
            "data": {
                "values": [
                    {
                        "value": item["value"],
                        "lost_conversions": round(item["lost_conversions"], 2),
                    }
                    for item in payment_values
                ]
            },
        },
    ]


def weekly_review_page(cases: list[dict], *, week_label: str) -> str:
    lines = [f"# Weekly review {week_label}", "", "| Case | Metric | Severity | Status | Lead hypothesis |", "| --- | --- | --- | --- | --- |"]
    if not cases:
        lines.append("| — | — | — | no cases | — |")
    for case in cases:
        lead = ""
        hypotheses = case.get("hypotheses") or []
        if hypotheses:
            lead = hypotheses[0].get("claim", "")
        lines.append(
            f"| {case.get('case_id', '')} | {case.get('metric', '')} | "
            f"{case.get('severity', '')} | {case.get('status', '')} | {lead} |"
        )
    lines.append("")
    lines.append("Outcomes are recorded only when a person confirms them.")
    return "\n".join(lines) + "\n"
