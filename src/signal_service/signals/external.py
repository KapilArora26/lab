from __future__ import annotations

from datetime import datetime

from signal_service.signals.tables import ensure_signal_tables


def run_external_signals(warehouse, client, *, case_id: str, metric: str, query: str) -> dict:
    ensure_signal_tables(warehouse)
    kept = client.search(query)
    now_rows = []
    for index, item in enumerate(kept, start=1):
        observed = item.get("observed_at")
        observed_dt = None
        if observed:
            observed_dt = datetime.fromisoformat(observed.replace("Z", ""))
        now_rows.append(
            (
                f"ext-{case_id}-{index}",
                case_id,
                item["title"],
                item["summary"],
                item["source_url"],
                item["source_type"],
                float(item["relevance"]),
                observed_dt,
                metric,
                client.provider,
            )
        )
    if now_rows:
        warehouse.executemany(
            "INSERT INTO signals.external_events VALUES (?,?,?,?,?,?,?,?,?,?)",
            now_rows,
        )
    return {"kept": len(kept), "provider": client.provider, "items": kept}


def load_external(warehouse, case_id: str) -> list[dict]:
    ensure_signal_tables(warehouse)
    return warehouse.query(
        """
        SELECT event_id, title, summary, source_url, source_type, relevance, observed_at, provider
        FROM signals.external_events
        WHERE case_id = ?
        ORDER BY relevance DESC
        """,
        [case_id],
    )
