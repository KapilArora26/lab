from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone


def _default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def query_hash(sql: str, params: list | None = None) -> str:
    folded = " ".join(sql.split())
    payload = folded + "|" + json.dumps(params or [], default=_default)
    return "q_" + hashlib.sha256(payload.encode()).hexdigest()[:12]


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


# Business columns only. Load timestamps and dq flags change; the key must not.
ROW_KEY_FIELDS: dict[str, tuple[str, ...]] = {
    "bronze_groups": (
        "group_id",
        "hour",
        "platform",
        "country",
        "channel",
        "payment_method",
        "sku",
        "app_version",
        "n_sessions",
        "n_convert",
        "n_attempt",
    ),
    "bronze_support_tickets": (
        "ticket_id",
        "ts",
        "platform",
        "country",
        "channel",
        "payment_method",
        "sku",
        "app_version",
        "product_area",
        "text_redacted",
    ),
    "bronze_deploys": ("deploy_id", "deployed_at", "platform", "app_version", "note"),
    "bronze_demo_meta": ("key", "value"),
    "bronze_golden_tickets": (
        "example_id",
        "text_redacted",
        "topic",
        "sentiment",
        "severity",
        "product_area",
    ),
    "bronze_external_events": (
        "event_id",
        "case_id",
        "title",
        "summary",
        "source_url",
        "source_type",
        "relevance",
        "observed_at",
        "metric",
        "provider",
    ),
    "silver_spend": (
        "logged_at",
        "case_id",
        "kind",
        "estimated_usd",
        "input_tokens",
        "output_tokens",
        "provider",
    ),
}

DEFAULT_TIMESTAMP_FLOOR = "2020-01-01"
DEFAULT_FUTURE_GRACE_SECONDS = 86400


def _part(value: object) -> str:
    """Null and empty string must not hash to the same token."""
    if value is None:
        return "0"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return "1" + value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return "1" + value.strftime("%Y-%m-%d")
    return "1" + str(value)


def bronze_row_key(source: str, batch_id: str | None, row: dict) -> str:
    """Stable non-null identity for one bronze or spend row.

    The hash is the batch id, the source table, and the business columns.
    A null group_id still produces a key, and it is not the key of a
    different null group_id row.
    """
    fields = ROW_KEY_FIELDS[source]
    parts = [source, "" if batch_id is None else str(batch_id)]
    parts.extend(_part(row.get(field)) for field in fields)
    payload = "\x1f".join(parts)
    digest = hashlib.sha256(payload.encode()).hexdigest()
    # A letter between groups keeps a digit run shorter than a card number.
    chunked = "a".join(digest[index : index + 4] for index in range(0, len(digest), 4))
    return "row" + chunked


def _as_timestamp(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip().replace("Z", "")
    if not text:
        return None
    return datetime.fromisoformat(text[:19])


def timestamp_reason(
    value: object,
    *,
    now: datetime | None = None,
    floor: datetime | None = None,
    grace_seconds: int | None = None,
) -> str | None:
    """Flag a group hour, the session timestamp it becomes, or a spend date.

    Epoch (year 1970), anything before the floor, and anything after now plus
    a small grace window are suspect. The floor defaults to 2020-01-01
    (``SIGNAL_DQ_TIMESTAMP_FLOOR``). The grace defaults to 86400 seconds,
    one day (``SIGNAL_DQ_FUTURE_GRACE_SECONDS``).
    """
    stamped = _as_timestamp(value)
    if stamped is None:
        return None
    if floor is None or grace_seconds is None:
        from signal_service.config import load_settings

        settings = load_settings()
        if floor is None:
            floor = settings.dq_timestamp_floor
        if grace_seconds is None:
            grace_seconds = settings.dq_future_grace_seconds
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    if stamped.year == 1970:
        return "epoch_timestamp"
    if stamped < floor:
        return "timestamp_before_floor"
    if stamped > now + timedelta(seconds=int(grace_seconds)):
        return "future_timestamp"
    return None
