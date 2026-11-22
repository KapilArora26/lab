from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime


DEFAULT_SNOWFLAKE_MCP_URL = (
    "https://ulkfcdl-vl97193.snowflakecomputing.com/api/v2/databases/"
    "MCP_DB/schemas/MCP/mcp-servers/CURSOR_MCP_SERVER"
)
SANDBOX_QUALIFIED = "MCP_DB.SIGNAL_SANDBOX"


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    z_threshold: float = 3.0
    min_volume: float = 100.0
    min_consecutive: int = 2
    period_hours: int = 24
    fit_exclude_tail: int = 48
    sigma_floor: float = 0.002
    high_severity_usd_per_hour: float = 5000.0
    medium_severity_usd_per_hour: float = 1000.0
    batch_size: int = 25
    daily_spend_cap_usd: float = 25.0
    grok_timeout_seconds: float = 30.0
    grok_max_retries: int = 3
    grok_model: str = "grok-4.7"
    grok_input_usd_per_million: float = 2.0
    grok_output_usd_per_million: float = 6.0
    grok_tts_usd_per_million_chars: float = 15.0
    voice_id: str = "eve"
    snowflake_mcp_url: str = DEFAULT_SNOWFLAKE_MCP_URL
    snowflake_sandbox: str = SANDBOX_QUALIFIED
    pipeline_mode: str = "auto"
    ingest_hours: int = 6
    dq_timestamp_floor: datetime = datetime(2020, 1, 1)
    dq_future_grace_seconds: int = 86400


def load_settings() -> Settings:
    return Settings(
        z_threshold=_float("SIGNAL_Z_THRESHOLD", 3.0),
        min_volume=_float("SIGNAL_MIN_VOLUME", 100.0),
        min_consecutive=_int("SIGNAL_MIN_CONSECUTIVE", 2),
        batch_size=_int("SIGNAL_BATCH_SIZE", 25),
        daily_spend_cap_usd=_float("SIGNAL_DAILY_SPEND_CAP_USD", 25.0),
        grok_timeout_seconds=_float("GROK_TIMEOUT_SECONDS", 30.0),
        grok_max_retries=_int("GROK_MAX_RETRIES", 3),
        grok_model=os.environ.get("GROK_MODEL", "grok-4.7"),
        grok_input_usd_per_million=_float("GROK_INPUT_USD_PER_MILLION", 2.0),
        grok_output_usd_per_million=_float("GROK_OUTPUT_USD_PER_MILLION", 6.0),
        grok_tts_usd_per_million_chars=_float("GROK_TTS_USD_PER_MILLION_CHARS", 15.0),
        voice_id=os.environ.get("GROK_VOICE_ID", "eve"),
        snowflake_mcp_url=os.environ.get("SNOWFLAKE_MCP_URL", DEFAULT_SNOWFLAKE_MCP_URL),
        snowflake_sandbox=os.environ.get("SNOWFLAKE_SANDBOX", SANDBOX_QUALIFIED),
        pipeline_mode=os.environ.get("SIGNAL_PIPELINE_MODE", "auto"),
        ingest_hours=_int("SIGNAL_INGEST_HOURS", 6),
        dq_timestamp_floor=_day("SIGNAL_DQ_TIMESTAMP_FLOOR", "2020-01-01"),
        dq_future_grace_seconds=_int("SIGNAL_DQ_FUTURE_GRACE_SECONDS", 86400),
    )


def _day(name: str, default: str) -> datetime:
    raw = os.environ.get(name)
    text = default if raw is None or raw == "" else raw.strip()
    return datetime.fromisoformat(text[:10])
