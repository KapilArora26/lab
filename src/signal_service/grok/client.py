from __future__ import annotations

import json
import logging
import time
from typing import Protocol

import httpx

from signal_service.grok.schemas import (
    ChatReply,
    ExternalBatch,
    ExternalSignal,
    TicketBatch,
    ticket_response_schema,
)
from signal_service.hashing import text_hash
from signal_service.paths import data_path
from signal_service.safety.cost import SpendGuard
from signal_service.safety.redaction import assert_redacted
from signal_service.signals.fallback import classify_text

logger = logging.getLogger("signal_service.grok")

RETRY_STATUS = {429, 500, 502, 503, 504}
API_BASE = "https://api.x.ai/v1"


class GrokError(RuntimeError):
    pass


class SchemaError(GrokError):
    pass


class GrokClient(Protocol):
    provider: str

    def classify_batch(self, items: list[dict]) -> list[dict]: ...

    def search(self, query: str) -> list[dict]: ...

    def synthesize(self, text: str, language: str = "en") -> bytes | None: ...

    def chat(self, *, system: str, user: str) -> str: ...


def _extract_json_object(text: str) -> dict:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise SchemaError("model response did not contain a JSON object")
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise SchemaError("model response was not valid JSON") from exc


def _usage(payload: dict) -> tuple[int, int]:
    usage = payload.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
    return input_tokens, output_tokens


def _output_text(payload: dict) -> str:
    chunks: list[str] = []
    for item in payload.get("output") or []:
        if item.get("type") != "message":
            continue
        for part in item.get("content") or []:
            if part.get("type") in {"output_text", "text"} and part.get("text"):
                chunks.append(part["text"])
    if chunks:
        return "\n".join(chunks)
    choices = payload.get("choices") or []
    if choices:
        return str(choices[0].get("message", {}).get("content") or "")
    return ""


def _validated_classifications(body: dict, items: list[dict]) -> list[dict]:
    parsed = TicketBatch.model_validate(_extract_json_object(_output_text(body)))
    by_id = {item["id"]: item["text"] for item in items}
    if len(parsed.items) != len(items):
        raise SchemaError("classification batch size did not match the input")
    results = []
    seen = set()
    for item in parsed.items:
        source = by_id.get(item.id)
        if source is None or item.id in seen:
            raise SchemaError("classification ids did not match the input")
        if not item.evidence_snippet or item.evidence_snippet not in source:
            raise SchemaError("evidence snippet is not a substring of the redacted text")
        seen.add(item.id)
        results.append(item.model_dump())
    return results


def keep_sourced(items: list[ExternalSignal]) -> list[dict]:
    """Drop events without an http(s) source and dedupe by URL."""
    best: dict[str, ExternalSignal] = {}
    for item in items:
        url = (item.source_url or "").strip()
        if not (url.startswith("https://") or url.startswith("http://")):
            continue
        key = url.split("?", 1)[0].rstrip("/")
        current = best.get(key)
        if current is None or item.relevance > current.relevance:
            best[key] = item
    ordered = sorted(best.values(), key=lambda item: item.relevance, reverse=True)[:5]
    return [item.model_dump() for item in ordered]


class LiveGrokClient:
    provider = "grok"

    def __init__(
        self,
        api_key: str,
        spend: SpendGuard,
        *,
        model: str = "grok-4.7",
        timeout: float = 30.0,
        max_retries: int = 3,
        voice_id: str = "eve",
        http: httpx.Client | None = None,
        sleeper=time.sleep,
    ):
        self.api_key = api_key
        self.spend = spend
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.voice_id = voice_id
        self.sleeper = sleeper
        self.http = http or httpx.Client(base_url=API_BASE, timeout=timeout)
        self.owns_client = http is None

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}

    def _post(self, path: str, payload: dict, *, estimated_usd: float) -> httpx.Response:
        self.spend.authorize(estimated_usd)
        delay = 0.5
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            started = time.perf_counter()
            try:
                response = self.http.post(path, json=payload, headers=self._headers())
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                logger.info(
                    "grok_call endpoint=%s status=transport_error attempt=%s",
                    path,
                    attempt,
                )
            else:
                latency_ms = int((time.perf_counter() - started) * 1000)
                logger.info(
                    "grok_call endpoint=%s status=%s latency_ms=%s attempt=%s",
                    path,
                    response.status_code,
                    latency_ms,
                    attempt,
                )
                if response.status_code in RETRY_STATUS:
                    last_error = GrokError(f"retryable status {response.status_code}")
                elif response.status_code >= 400:
                    raise GrokError(f"grok status {response.status_code}")
                else:
                    return response
            if attempt + 1 < self.max_retries:
                self.sleeper(delay)
                delay *= 2
        raise GrokError(f"grok call failed after retries: {last_error}")

    def classify_batch(self, items: list[dict]) -> list[dict]:
        for item in items:
            assert_redacted(item["text"])
        prompt = json.dumps(
            [{"id": item["id"], "text": item["text"]} for item in items],
            ensure_ascii=True,
        )
        estimated_input = max(1, len(prompt) // 4)
        estimated = self.spend.estimate_tokens(estimated_input, 80 * max(1, len(items)))
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Classify each redacted support ticket. Return one item per id. "
                        "evidence_snippet must be a verbatim substring of that ticket, "
                        "at most 140 characters. Do not invent facts."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "ticket_batch",
                    "strict": True,
                    "schema": ticket_response_schema(),
                },
            },
        }
        last_error: Exception | None = None
        delay = 0.5
        for attempt in range(self.max_retries):
            response = self._post("/chat/completions", payload, estimated_usd=estimated)
            body = response.json()
            input_tokens, output_tokens = _usage(body)
            self.spend.record(
                self.spend.estimate_tokens(input_tokens or estimated_input, output_tokens or 80),
                kind="classify",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
            logger.info(
                "grok_usage endpoint=/chat/completions input_tokens=%s output_tokens=%s input_hash=%s",
                input_tokens,
                output_tokens,
                text_hash(prompt)[:16],
            )
            try:
                return _validated_classifications(body, items)
            except SchemaError as exc:
                last_error = exc
                logger.info("grok_schema_retry endpoint=/chat/completions attempt=%s", attempt)
                if attempt + 1 < self.max_retries:
                    self.sleeper(delay)
                    delay *= 2
        raise SchemaError(f"classification failed schema checks: {last_error}")

    def search(self, query: str) -> list[dict]:
        prompt = (
            "Find outside events relevant to this incident. Return JSON "
            '{"items":[{"title","summary","source_url","source_type","relevance","observed_at"}]}. '
            "source_type is web or x. Every item needs a real http(s) source_url. "
            f"Incident: {query}"
        )
        estimated = self.spend.estimate_tokens(max(1, len(prompt) // 4), 400)
        payload = {
            "model": self.model,
            "input": [{"role": "user", "content": prompt}],
            "tools": [{"type": "web_search"}, {"type": "x_search"}],
        }
        response = self._post("/responses", payload, estimated_usd=estimated)
        body = response.json()
        input_tokens, output_tokens = _usage(body)
        self.spend.record(
            self.spend.estimate_tokens(input_tokens or len(prompt) // 4, output_tokens or 200),
            kind="search",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        logger.info(
            "grok_usage endpoint=/responses input_tokens=%s output_tokens=%s input_hash=%s",
            input_tokens,
            output_tokens,
            text_hash(query)[:16],
        )
        parsed = ExternalBatch.model_validate(_extract_json_object(_output_text(body)))
        return keep_sourced(parsed.items)

    def chat(self, *, system: str, user: str) -> str:
        assert_redacted(system)
        assert_redacted(user)
        prompt = system + "\n" + user
        estimated_input = max(1, len(prompt) // 4)
        estimated = self.spend.estimate_tokens(estimated_input, 400)
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        response = self._post("/chat/completions", payload, estimated_usd=estimated)
        body = response.json()
        input_tokens, output_tokens = _usage(body)
        self.spend.record(
            self.spend.estimate_tokens(input_tokens or estimated_input, output_tokens or 200),
            kind="dashboard_chat",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        logger.info(
            "grok_usage endpoint=/chat/completions input_tokens=%s output_tokens=%s input_hash=%s",
            input_tokens,
            output_tokens,
            text_hash(prompt)[:16],
        )
        raw_content = (body.get("choices") or [{}])[0].get("message", {}).get("content")
        content = raw_content.strip() if isinstance(raw_content, str) else ""
        parsed = ChatReply.model_validate({"reply": content})
        assert_redacted(parsed.reply)
        return parsed.reply

    def complete(self, *, system: str, user: str, tools: list[dict] | None = None) -> dict:
        """One chat completion. Returns assistant text and any tool calls."""
        assert_redacted(system)
        assert_redacted(user)
        prompt = system + "\n" + user
        estimated_input = max(1, len(prompt) // 4)
        estimated = self.spend.estimate_tokens(estimated_input, 400)
        payload: dict = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"
        response = self._post("/chat/completions", payload, estimated_usd=estimated)
        body = response.json()
        input_tokens, output_tokens = _usage(body)
        self.spend.record(
            self.spend.estimate_tokens(input_tokens or estimated_input, output_tokens or 200),
            kind="dashboard_chat",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        logger.info(
            "grok_usage endpoint=/chat/completions input_tokens=%s output_tokens=%s input_hash=%s",
            input_tokens,
            output_tokens,
            text_hash(prompt)[:16],
        )
        message = (body.get("choices") or [{}])[0].get("message") or {}
        raw_content = message.get("content")
        content = raw_content.strip() if isinstance(raw_content, str) else ""
        tool_calls = message.get("tool_calls") or []
        if content:
            parsed = ChatReply.model_validate({"reply": content})
            assert_redacted(parsed.reply)
            content = parsed.reply
        elif not tool_calls:
            raise SchemaError("model response did not include a reply")
        return {"content": content, "tool_calls": list(tool_calls)}

    def synthesize(self, text: str, language: str = "en") -> bytes | None:
        estimated = self.spend.estimate_speech(text)
        self.spend.authorize(estimated)
        payload = {"text": text, "voice_id": self.voice_id, "language": language}
        response = self._post("/tts", payload, estimated_usd=0)
        self.spend.record(estimated, kind="tts", input_tokens=len(text), output_tokens=0)
        logger.info(
            "grok_usage endpoint=/tts chars=%s input_hash=%s",
            len(text),
            text_hash(text)[:16],
        )
        return response.content

    def close(self) -> None:
        if self.owns_client:
            self.http.close()


class MockGrokClient:
    provider = "mock"

    def __init__(self, spend: SpendGuard | None = None):
        self.spend = spend

    def classify_batch(self, items: list[dict]) -> list[dict]:
        results = []
        for item in items:
            assert_redacted(item["text"])
            results.append(classify_text(item["id"], item["text"]).model_dump())
        return results

    def search(self, query: str) -> list[dict]:
        lowered = query.lower()
        if not any(token in lowered for token in ("upi", "checkout", "payment", "android")):
            return []
        raw = json.loads(data_path("fixtures", "external_signals.json").read_text(encoding="utf-8"))
        signals = [ExternalSignal.model_validate(item) for item in raw]
        return keep_sourced(signals)

    def chat(self, *, system: str, user: str) -> str:
        assert_redacted(system)
        assert_redacted(user)
        return ""

    def synthesize(self, text: str, language: str = "en") -> bytes | None:
        return None
