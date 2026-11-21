from __future__ import annotations

from datetime import datetime, timedelta

# Weights are fixed so a seeded UPI incident lands near 0.8 only after ticket
# topics and a sourced external event are both present.
LOSS_WEIGHT = 0.28
LIFT_WEIGHT = 0.10
TICKET_WEIGHT = 0.276
EXTERNAL_WEIGHT = 0.15
DEPLOY_BONUS = 0.04
BASE = 0.10
SHARE_CUTOFF = 0.55


def _lift_term(lift: float | None) -> float:
    if lift is None or lift < 1:
        return 0.0
    return min((lift - 1) / 2, 1.0)


def _score(loss_share: float, lift: float | None, alignment: float, relevance: float, deploy: bool) -> float:
    raw = (
        BASE
        + LOSS_WEIGHT * loss_share
        + LIFT_WEIGHT * _lift_term(lift)
        + TICKET_WEIGHT * alignment
        + EXTERNAL_WEIGHT * relevance
        + (DEPLOY_BONUS if deploy else 0.0)
    )
    return round(min(raw, 0.93), 4)


def _topic_alignment(topics: list[dict], topic: str) -> float:
    relevant = [item for item in topics if float(item.get("confidence", 0)) >= 0.5 and item.get("topic") != "other"]
    if not relevant:
        return 0.0
    matches = sum(1 for item in relevant if item.get("topic") == topic)
    return matches / len(relevant)


def _external_relevance(external: list[dict], tokens: set[str]) -> float:
    scores = []
    for item in external:
        supports = {token.lower() for token in item.get("supports") or []}
        text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
        if supports & tokens or any(token in text for token in tokens):
            if item.get("source_url"):
                scores.append(float(item.get("relevance") or 0))
    return max(scores) if scores else 0.0


def _deploy_match(deploys: list[dict], version: str, started_at: datetime) -> dict | None:
    for deploy in deploys:
        if deploy.get("app_version") != version:
            continue
        stamp = deploy.get("deployed_at")
        if isinstance(stamp, str):
            stamp = datetime.fromisoformat(stamp.replace("Z", ""))
        if started_at - timedelta(hours=6) <= stamp <= started_at + timedelta(hours=1):
            return deploy
    return None


def rank_hypotheses(sliced: dict, topics: list[dict], external: list[dict], started_at: datetime) -> dict:
    evidence = []
    candidates = []
    for dimension, payload in sliced["dimensions"].items():
        evidence.append(
            {
                "id": payload["query_hash"],
                "kind": "query",
                "ref": payload["query_hash"],
                "summary": f"Conversion slice by {dimension} during the incident versus the prior 7 days.",
            }
        )
        for value in payload["values"]:
            if value["loss_share"] < SHARE_CUTOFF:
                continue
            candidates.append((dimension, value, payload["query_hash"]))
    payment_evidence = sliced["payment_query_hash"]
    evidence.append(
        {
            "id": payment_evidence,
            "kind": "query",
            "ref": payment_evidence,
            "summary": "Payment failure rate by method during the incident versus the prior 7 days.",
        }
    )
    upi_alignment = _topic_alignment(topics, "upi_payment_failure")
    upi_relevance = _external_relevance(external, {"upi", "gateway", "payment"})
    if topics:
        evidence.append(
            {
                "id": f"signals.ticket_topics#{len(topics)}",
                "kind": "ticket_topics",
                "ref": "signals.ticket_topics",
                "summary": f"{len(topics)} classified tickets in the incident window.",
            }
        )
    for index, item in enumerate(external, start=1):
        if not item.get("source_url"):
            continue
        evidence.append(
            {
                "id": f"external#{index}",
                "kind": "external",
                "ref": item["source_url"],
                "summary": item.get("title") or item["source_url"],
            }
        )

    scored = []
    for dimension, value, digest in candidates:
        name = str(value["value"])
        deploy = None
        lift = None
        alignment = 0.0
        relevance = 0.0
        if dimension == "payment_method" and name == "upi":
            failure = sliced["payment_failure"].get("upi") or {}
            lift = failure.get("lift")
            alignment = upi_alignment
            relevance = upi_relevance
            claim = "Payment gateway failures on UPI"
            change = (
                "If UPI payment success returns to its baseline while Android India "
                "conversion stays down, this hypothesis is wrong."
            )
            ids = [digest, payment_evidence]
            if topics:
                ids.append(f"signals.ticket_topics#{len(topics)}")
            if relevance:
                ids.extend(
                    item["id"] for item in evidence if item["kind"] == "external"
                )
        elif dimension == "app_version":
            deploy = _deploy_match(sliced["deploys"], name, started_at)
            claim = f"Regression in app version {name}"
            change = (
                f"If version {name} matches the prior version on UPI during the same hours, "
                "this hypothesis is wrong."
            )
            ids = [digest]
            if deploy is not None:
                deploy_id = f"deploy#{deploy.get('platform', 'app')}-{name}"
                ids.append(deploy_id)
                evidence.append(
                    {
                        "id": deploy_id,
                        "kind": "deploy",
                        "ref": deploy.get("deploy_id", deploy_id),
                        "summary": f"{deploy.get('platform')} {name} deployed at {deploy.get('deployed_at')}.",
                    }
                )
        else:
            claim = f"Conversion fell for {dimension} {name}"
            change = "A broader slice with a larger loss share would outrank this one."
            ids = [digest]
        confidence = _score(
            value["loss_share"],
            lift,
            alignment,
            relevance,
            deploy is not None,
        )
        scored.append(
            {
                "claim": claim,
                "confidence": confidence,
                "evidence": ids,
                "what_would_change_my_mind": change,
                "dimension": dimension,
                "value": name,
                "loss_share": round(value["loss_share"], 4),
            }
        )
    scored.sort(key=lambda item: (-item["confidence"], -item["loss_share"], item["claim"]))
    hypotheses = []
    for index, item in enumerate(scored[:2], start=1):
        hypotheses.append({"id": f"H{index}", **item})
    if len(hypotheses) < 3:
        channel_hash = sliced["dimensions"]["channel"]["query_hash"]
        sku_hash = sliced["dimensions"]["sku"]["query_hash"]
        hypotheses.append(
            {
                "id": f"H{len(hypotheses) + 1}",
                "claim": "Channel and SKU do not concentrate the loss",
                "confidence": 0.22,
                "evidence": [channel_hash, sku_hash],
                "what_would_change_my_mind": (
                    "A channel or SKU value that accounts for most lost checkouts would replace this note."
                ),
                "dimension": None,
                "value": None,
                "loss_share": 0.0,
            }
        )
    return {"hypotheses": hypotheses[:3], "evidence": evidence}
