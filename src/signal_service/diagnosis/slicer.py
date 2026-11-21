from __future__ import annotations

from datetime import datetime, timedelta

from signal_service.hashing import query_hash

DIMENSIONS = ("payment_method", "app_version", "channel", "sku")


def _window(card: dict) -> tuple[datetime, datetime]:
    started = datetime.fromisoformat(card["started_at"].replace("Z", ""))
    ended = datetime.fromisoformat(card["ended_at"].replace("Z", ""))
    return started, ended + timedelta(hours=1)


def _where(segment: dict[str, str]) -> tuple[str, list]:
    clauses = [f"{column} = ?" for column in segment]
    params = [segment[column] for column in segment]
    if not clauses:
        return "", []
    return " AND ".join(clauses), params


def _rates(warehouse, dimension: str, segment: dict, start: datetime, end: datetime, table: str) -> tuple[dict, str]:
    if dimension not in DIMENSIONS and dimension != "gateway":
        raise ValueError(dimension)
    segment_sql, segment_params = _where(segment)
    prefix = f"{segment_sql} AND " if segment_sql else ""
    if table == "sessions":
        sql = f"""
            SELECT {dimension} AS value,
                   count(*)::DOUBLE AS volume,
                   sum(CASE WHEN converted THEN 1 ELSE 0 END)::DOUBLE AS positives
            FROM sessions
            WHERE {prefix} ts >= ? AND ts < ?
            GROUP BY 1
        """
        params = segment_params + [start, end]
    else:
        value_column = "method" if dimension == "payment_method" else dimension
        sql = f"""
            SELECT {value_column} AS value,
                   count(*)::DOUBLE AS attempts,
                   sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)::DOUBLE AS fails
            FROM payments
            WHERE {prefix} ts >= ? AND ts < ?
            GROUP BY 1
        """
        params = segment_params + [start, end]
    rows = warehouse.query(sql, params)
    return {row["value"]: row for row in rows}, query_hash(sql, params)


def slice_case(warehouse, card: dict) -> dict:
    started, ended = _window(card)
    history_start = started - timedelta(days=7)
    segment = card["segment"]
    dimensions = {}
    for dimension in DIMENSIONS:
        current, digest = _rates(warehouse, dimension, segment, started, ended, "sessions")
        baseline, _ = _rates(warehouse, dimension, segment, history_start, started, "sessions")
        values = []
        for name, row in current.items():
            base = baseline.get(name)
            base_volume = float(base["volume"]) if base else 0.0
            base_rate = float(base["positives"]) / base_volume if base and base_volume else 0.0
            volume = float(row["volume"])
            rate = float(row["positives"]) / volume if volume else 0.0
            lost = max(0.0, (base_rate - rate) * volume)
            values.append(
                {
                    "dimension": dimension,
                    "value": name,
                    "baseline_rate": base_rate,
                    "current_rate": rate,
                    "current_volume": volume,
                    "lost_conversions": lost,
                }
            )
        total_lost = sum(item["lost_conversions"] for item in values)
        for item in values:
            item["loss_share"] = item["lost_conversions"] / total_lost if total_lost else 0.0
        dimensions[dimension] = {"query_hash": digest, "values": values, "lost_conversions": total_lost}
    pay_current, pay_hash = _rates(warehouse, "payment_method", segment, started, ended, "payments")
    pay_base, _ = _rates(warehouse, "payment_method", segment, history_start, started, "payments")
    failure = {}
    for name, row in pay_current.items():
        attempts = float(row["attempts"])
        current_rate = float(row["fails"]) / attempts if attempts else 0.0
        base = pay_base.get(name)
        base_attempts = float(base["attempts"]) if base else 0.0
        base_rate = float(base["fails"]) / base_attempts if base and base_attempts else 0.0
        failure[name] = {
            "baseline_failure_rate": base_rate,
            "current_failure_rate": current_rate,
            "lift": (current_rate / base_rate) if base_rate else None,
        }
    deploys = warehouse.query(
        "SELECT deploy_id, deployed_at, platform, app_version, note FROM deploys"
    )
    return {
        "dimensions": dimensions,
        "payment_failure": failure,
        "payment_query_hash": pay_hash,
        "deploys": [
            {
                **row,
                "deployed_at": row["deployed_at"].isoformat() + "Z"
                if isinstance(row["deployed_at"], datetime)
                else str(row["deployed_at"]),
            }
            for row in deploys
        ],
    }
