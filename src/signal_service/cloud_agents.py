"""Cursor cloud-agent client. Dashboard chat may only open a pull request."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from typing import Any

import httpx
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger("signal_service.cloud_agents")

API_BASE = "https://api.cursor.com"
FIXED_REPO_URL = "https://github.com/KapilArora26/lab"
FIXED_STARTING_REF = "main"
MAX_LAUNCHES_PER_HOUR = 3
LAUNCH_WINDOW_SECONDS = 3600.0
DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
TASK_PREAMBLE = (
    "You may only open a pull request. "
    "Never merge. Never push to main. Never deploy. Never touch Snowflake. Never print secrets. "
    "The repository and the branch are fixed by the server. Do not change them."
)
DISABLED_MESSAGE = "Cloud agent launch is disabled."
MISSING_KEY_MESSAGE = "Cloud agent launch is disabled. CURSOR_API_KEY is not set."
CONFIRM_MESSAGE = (
    "Confirm the cloud agent launch in the dashboard before it starts. "
    "The agent will only open a pull request."
)
BLOCKED_MESSAGE = (
    "That task was rejected. Cloud agents cannot merge, deploy, drop, grant, "
    "change a role, or exfiltrate secrets or tokens."
)
RATE_LIMIT_MESSAGE = "Cloud agent launch is limited to 3 launches per hour."

_AGENT_ID = re.compile(r"^bc-[A-Za-z0-9_-]{8,80}$")
_RUN_ID = re.compile(r"^run-[A-Za-z0-9_-]{8,80}$")
_PR_URL = re.compile(r"^https://github\.com/KapilArora26/lab/pull/\d+$")
_BLOCKED = (
    re.compile(r"\bmerg(?:e|ed|es|ing)\b", re.IGNORECASE),
    re.compile(r"\bdeploy(?:ed|ing|s|ment|ments)?\b", re.IGNORECASE),
    re.compile(r"\bdrop(?:ped|ping|s)?\b", re.IGNORECASE),
    re.compile(r"\bgrant(?:ed|ing|s)?\b", re.IGNORECASE),
    re.compile(r"\broles?\b", re.IGNORECASE),
    re.compile(
        r"\b(?:"
        r"exfiltrat\w*"
        r"|(?:print|show|reveal|dump|echo|leak|steal|extract|export|paste|display|cat)\w*"
        r".{0,60}(?:secrets?|tokens?|api[_ ]?keys?|credentials?|passwords?)"
        r"|(?:secrets?|tokens?|api[_ ]?keys?|credentials?|passwords?)"
        r".{0,60}(?:print|show|reveal|dump|echo|leak|steal|extract|export|paste|display)\w*"
        r")\b",
        re.IGNORECASE | re.DOTALL,
    ),
)
_RUN_STATUSES = {
    "CREATING",
    "RUNNING",
    "WAITING_FOR_BACKGROUND_WORK",
    "FINISHED",
    "ERROR",
    "CANCELLED",
    "EXPIRED",
}
_AGENT_STATUSES = {"ACTIVE", "IDLE", "ARCHIVED"}
_launch_times: list[float] = []
_lock = threading.Lock()

TOOL_SYSTEM = (
    "You can call launch_cloud_agent when the user asks to open a pull request or change this "
    "repository. You can call cloud_agent_status with an agent id to check that launch. "
    "You cannot merge, deploy, choose a repository, or choose a branch. "
    "A launch does not start until the user confirms it in the dashboard."
)

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "launch_cloud_agent",
            "description": (
                "Launch a Cursor cloud agent that opens a pull request on the fixed repository. "
                "Call this when the user asks to open a pull request or fix something in the repo. "
                "You cannot choose the repository, branch, or merge behavior. "
                "The user must confirm in the dashboard before the agent starts."
            ),
            "parameters": {
                "type": "object",
                "additionalProperties": False,
                "required": ["task"],
                "properties": {
                    "task": {
                        "type": "string",
                        "description": "The change the pull request should make.",
                    }
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cloud_agent_status",
            "description": (
                "Report the status of a Cursor cloud agent and the pull request URL when one is ready."
            ),
            "parameters": {
                "type": "object",
                "additionalProperties": False,
                "required": ["agent_id"],
                "properties": {"agent_id": {"type": "string"}},
            },
        },
    },
]


class LaunchArgs(BaseModel):
    task: str = Field(min_length=1, max_length=2000)


class StatusArgs(BaseModel):
    agent_id: str = Field(min_length=1, max_length=80)


def launch_enabled() -> bool:
    return os.environ.get("ENABLE_AGENT_LAUNCH", "").strip().lower() in {"1", "true", "yes", "on"}


def redact_secrets(text: str) -> str:
    """Remove API keys from text that might be returned or logged."""
    redacted = text
    for name in ("CURSOR_API_KEY", "XAI_API_KEY"):
        raw = os.environ.get(name) or ""
        for candidate in {raw, raw.strip()}:
            if len(candidate) >= 8:
                redacted = redacted.replace(candidate, "[redacted]")
    return re.sub(r"(?i)Bearer\s+\S+", "Bearer [redacted]", redacted)


def task_blocked(task: str) -> bool:
    return any(pattern.search(task) for pattern in _BLOCKED)


def reset_rate_limit() -> None:
    with _lock:
        _launch_times.clear()


def _cursor_key() -> str:
    return os.environ.get("CURSOR_API_KEY", "").strip()


def _disabled() -> dict[str, str]:
    if not launch_enabled():
        return {"status": "disabled", "message": DISABLED_MESSAGE}
    if not _cursor_key():
        return {"status": "disabled", "message": MISSING_KEY_MESSAGE}
    return {}


def _scrub(value: Any) -> Any:
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, dict):
        return {str(key): _scrub(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    return value


def _error(message: str) -> dict[str, str]:
    cleaned = redact_secrets(message).strip() or "Cloud agent request failed."
    return {"status": "error", "message": cleaned}


def _allowlisted_status(value: Any, allowed: set[str]) -> str:
    text = str(value or "").strip().upper()
    return text if text in allowed else "UNKNOWN"


def _take_slot(now: float) -> bool:
    with _lock:
        cutoff = now - LAUNCH_WINDOW_SECONDS
        _launch_times[:] = [stamp for stamp in _launch_times if stamp > cutoff]
        if len(_launch_times) >= MAX_LAUNCHES_PER_HOUR:
            return False
        _launch_times.append(now)
        return True


def _prepare_task(task: str) -> dict[str, str] | str:
    cleaned = redact_secrets(str(task or "")).strip()
    try:
        return LaunchArgs.model_validate({"task": cleaned}).task
    except ValidationError:
        return _error("A task is required.")


class CloudAgentClient:
    """HTTP client for the Cursor Cloud Agents API. The key stays in memory only."""

    def __init__(self, *, http: httpx.Client | None = None, timeout: httpx.Timeout | None = None):
        self._http = http
        self._owns = False
        self._timeout = timeout or DEFAULT_TIMEOUT

    def __repr__(self) -> str:
        return "CloudAgentClient()"

    def close(self) -> None:
        if self._owns and self._http is not None:
            self._http.close()
            self._owns = False

    def _client(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(base_url=API_BASE, timeout=self._timeout)
            self._owns = True
        return self._http

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {_cursor_key()}"}

    def _send(self, method: str, path: str, *, json_body: dict | None = None) -> httpx.Response | dict:
        try:
            response = self._client().request(method, path, json=json_body, headers=self._headers())
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            logger.info("cursor_agent method=%s outcome=transport_error", method)
            return _error(redact_secrets(str(exc)) or "Cloud agent request failed.")
        logger.info("cursor_agent method=%s http_status=%s", method, response.status_code)
        return response

    def launch(self, task: str, *, confirmed: bool, now: float) -> dict:
        prepared = _prepare_task(task)
        if isinstance(prepared, dict):
            return _scrub(prepared)
        blocked = _guard(prepared, confirmed=confirmed, now=now)
        if blocked is not None:
            return _scrub(blocked)
        payload = {
            "prompt": {"text": f"{TASK_PREAMBLE}\n\nTask:\n{prepared}"},
            "repos": [{"url": FIXED_REPO_URL, "startingRef": FIXED_STARTING_REF}],
            "autoCreatePR": True,
            "workOnCurrentBranch": False,
        }
        sent = self._send("POST", "/v1/agents", json_body=payload)
        if isinstance(sent, dict):
            return sent
        if sent.status_code >= 400:
            return _failure(sent)
        try:
            body = sent.json()
        except json.JSONDecodeError:
            return _error("Cloud agent response was not JSON.")
        if not isinstance(body, dict):
            return _error("Cloud agent response was not an object.")
        return _scrub(_launch_result(body))

    def status(self, agent_id: str) -> dict:
        disabled = _disabled()
        if disabled:
            return disabled
        if not _AGENT_ID.fullmatch(agent_id):
            return _error("The agent id is not valid.")
        sent = self._send("GET", f"/v1/agents/{agent_id}")
        if isinstance(sent, dict):
            return sent
        if sent.status_code >= 400:
            return _failure(sent)
        try:
            agent = sent.json()
        except json.JSONDecodeError:
            return _error("Cloud agent response was not JSON.")
        if not isinstance(agent, dict):
            return _error("Cloud agent response was not an object.")
        run: dict = {}
        run_id = str(agent.get("latestRunId") or "")
        if _RUN_ID.fullmatch(run_id):
            run_response = self._send("GET", f"/v1/agents/{agent_id}/runs/{run_id}")
            if isinstance(run_response, dict):
                return run_response
            if run_response.status_code < 400:
                try:
                    parsed = run_response.json()
                except json.JSONDecodeError:
                    parsed = {}
                if isinstance(parsed, dict):
                    run = parsed
        return _scrub(_status_result(agent_id, agent, run))

    def retire(self, agent_id: str, run_id: str | None = None) -> dict:
        """Cancel the run and delete the agent. Dashboard chat cannot call this."""
        if not _cursor_key():
            return {"status": "disabled", "message": MISSING_KEY_MESSAGE}
        if not _AGENT_ID.fullmatch(agent_id):
            return _error("The agent id is not valid.")
        cancelled = False
        if run_id and _RUN_ID.fullmatch(run_id):
            sent = self._send("POST", f"/v1/agents/{agent_id}/runs/{run_id}/cancel")
            if isinstance(sent, dict):
                return sent
            cancelled = sent.status_code < 400 or sent.status_code == 409
            if sent.status_code >= 400 and sent.status_code != 409:
                logger.info("cursor_agent method=POST outcome=cancel_failed http_status=%s", sent.status_code)
        deleted = self._send("DELETE", f"/v1/agents/{agent_id}")
        if isinstance(deleted, dict):
            return deleted
        if deleted.status_code >= 400:
            return _failure(deleted)
        return _scrub(
            {
                "status": "retired",
                "agent_id": agent_id,
                "cancelled": cancelled,
                "deleted": True,
                "message": "Cloud agent was stopped and deleted.",
            }
        )


def _guard(task: str, *, confirmed: bool, now: float) -> dict | None:
    disabled = _disabled()
    if disabled:
        return disabled
    if task_blocked(task):
        logger.info("cursor_agent method=POST outcome=blocked")
        return {"status": "blocked", "message": BLOCKED_MESSAGE}
    if not confirmed:
        return {"status": "confirmation_required", "message": CONFIRM_MESSAGE, "task": task}
    if not _take_slot(now):
        logger.info("cursor_agent method=POST outcome=rate_limited")
        return {"status": "rate_limited", "message": RATE_LIMIT_MESSAGE}
    return None


def _failure(response: httpx.Response) -> dict[str, str]:
    detail = redact_secrets(response.text[:500]).strip()
    message = f"Cloud agent request failed (HTTP {response.status_code})."
    if detail:
        message = f"{message} {detail}"
    return _error(message)


def _agent_url(agent_id: str) -> str:
    return f"https://cursor.com/agents/{agent_id}"


def _launch_result(body: dict) -> dict:
    agent = body.get("agent") if isinstance(body.get("agent"), dict) else {}
    run = body.get("run") if isinstance(body.get("run"), dict) else {}
    agent_id = str(agent.get("id") or "")
    if not _AGENT_ID.fullmatch(agent_id):
        return _error("Cloud agent response did not include an agent id.")
    run_id = str(run.get("id") or "")
    if not _RUN_ID.fullmatch(run_id):
        run_id = ""
    run_status = _allowlisted_status(run.get("status"), _RUN_STATUSES)
    url = _agent_url(agent_id)
    return {
        "status": "launched",
        "agent_id": agent_id,
        "agent_url": url,
        "run_id": run_id,
        "run_status": run_status,
        "agent_status": _allowlisted_status(agent.get("status"), _AGENT_STATUSES),
        "message": f"Cloud agent launched. Status: {run_status}. Agent: {url}",
    }


def _pr_url(run: dict) -> str | None:
    git = run.get("git") if isinstance(run.get("git"), dict) else {}
    branches = git.get("branches") if isinstance(git.get("branches"), list) else []
    for branch in branches:
        if not isinstance(branch, dict):
            continue
        candidate = str(branch.get("prUrl") or "")
        if _PR_URL.fullmatch(candidate):
            return candidate
    return None


def _status_result(agent_id: str, agent: dict, run: dict) -> dict:
    run_status = _allowlisted_status(run.get("status"), _RUN_STATUSES) if run else "UNKNOWN"
    agent_status = _allowlisted_status(agent.get("status"), _AGENT_STATUSES)
    pr_url = _pr_url(run)
    url = _agent_url(agent_id)
    if pr_url:
        message = f"Cloud agent {agent_id} is {run_status}. Pull request: {pr_url}"
    else:
        shown = run_status if run else agent_status
        message = f"Cloud agent {agent_id} is {shown}. The pull request is not ready yet."
    return {
        "status": "ok",
        "agent_id": agent_id,
        "agent_url": url,
        "agent_status": agent_status,
        "run_status": run_status if run else None,
        "pr_url": pr_url,
        "message": message,
    }


def _with_client(http: httpx.Client | None) -> CloudAgentClient:
    return CloudAgentClient(http=http)


def launch_cloud_agent(
    task: str,
    *,
    confirmed: bool = False,
    http: httpx.Client | None = None,
    now: float | None = None,
) -> dict:
    client = _with_client(http)
    try:
        return client.launch(task, confirmed=confirmed, now=time.time() if now is None else now)
    finally:
        client.close()


def cloud_agent_status(agent_id: str, *, http: httpx.Client | None = None) -> dict:
    client = _with_client(http)
    try:
        return client.status(str(agent_id or ""))
    finally:
        client.close()


def retire_agent(agent_id: str, run_id: str | None = None, *, http: httpx.Client | None = None) -> dict:
    """Stop a launch this process started. Not a dashboard chat tool."""
    client = _with_client(http)
    try:
        return client.retire(agent_id, run_id)
    finally:
        client.close()


def _parse_arguments(raw: Any) -> dict:
    if isinstance(raw, dict):
        arguments = raw
    elif not raw:
        arguments = {}
    else:
        parsed = json.loads(str(raw))
        if not isinstance(parsed, dict):
            raise ValueError("tool arguments must be an object")
        arguments = parsed
    return arguments


def execute_tool_call(call: dict) -> dict:
    """Run one model tool call. A model cannot confirm a launch or pick the repo."""
    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    name = str(function.get("name") or "")
    try:
        arguments = _parse_arguments(function.get("arguments"))
    except (ValueError, json.JSONDecodeError):
        return _error("The tool arguments were not valid JSON.")
    if name == "launch_cloud_agent":
        try:
            task = LaunchArgs.model_validate({"task": arguments.get("task")}).task
        except ValidationError:
            return _error("launch_cloud_agent requires a task.")
        return launch_cloud_agent(task, confirmed=False)
    if name == "cloud_agent_status":
        try:
            agent_id = StatusArgs.model_validate({"agent_id": arguments.get("agent_id")}).agent_id
        except ValidationError:
            return _error("cloud_agent_status requires an agent id.")
        return cloud_agent_status(agent_id)
    return _error("That tool is not available.")
