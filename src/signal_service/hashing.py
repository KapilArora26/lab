from __future__ import annotations

import hashlib
import json
from datetime import datetime


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
