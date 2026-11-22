"""Cloud agent launches stay behind the flag, the confirm step, and the fixed repo."""

from __future__ import annotations

import inspect
import json
import logging

import httpx
import pytest
from fastapi.testclient import TestClient

from signal_service.api import dashboard
from signal_service.api.app import create_app
from signal_service.cloud_agents import (
    FIXED_REPO_URL,
    FIXED_STARTING_REF,
    TASK_PREAMBLE,
    TOOL_DEFINITIONS,
    cloud_agent_status,
    execute_tool_call,
    launch_cloud_agent,
    reset_rate_limit,
    retire_agent,
)

AGENT_ID = "bc-11111111-1111-1111-1111-111111111111"
RUN_ID = "run-22222222-2222-2222-2222-222222222222"
CURSOR_KEY = "cursor-test-key-do-not-log-123456"
XAI_KEY = "xai-test-key-do-not-log-654321"
NOW = 1_800_000_000.0
_REAL_CLIENT = httpx.Client


@pytest.fixture(autouse=True)
def _isolate_agent_env(monkeypatch):
    monkeypatch.delenv("ENABLE_AGENT_LAUNCH", raising=False)
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    monkeypatch.delenv("XAI_API_KEY", raising=False)
    reset_rate_limit()
    yield
    reset_rate_limit()


def _enable(monkeypatch, *, key: str | None = CURSOR_KEY):
    monkeypatch.setenv("ENABLE_AGENT_LAUNCH", "true")
    if key is None:
        monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    else:
        monkeypatch.setenv("CURSOR_API_KEY", key)


def _forbid_http(monkeypatch):
    def factory(*args, **kwargs):
        raise AssertionError("HTTP client should not be created")

    monkeypatch.setattr("signal_service.cloud_agents.httpx.Client", factory)


def _patch_http(monkeypatch, handler):
    seen = {"calls": 0, "timeout": None, "base_url": None}

    def factory(*args, **kwargs):
        seen["calls"] += 1
        seen["timeout"] = kwargs.get("timeout")
        seen["base_url"] = str(kwargs.get("base_url") or "")
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_CLIENT(*args, **kwargs)

    monkeypatch.setattr("signal_service.cloud_agents.httpx.Client", factory)
    return seen


def _created(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "agent": {
                "id": AGENT_ID,
                "status": "ACTIVE",
                "url": f"https://cursor.com/agents/{AGENT_ID}",
            },
            "run": {"id": RUN_ID, "status": "CREATING"},
        },
    )


def test_launch_happy_path_uses_the_fixed_repo(monkeypatch, caplog):
    _enable(monkeypatch)
    monkeypatch.setenv("XAI_API_KEY", XAI_KEY)
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = json.loads(request.content.decode())
        seen["path"] = request.url.path
        return _created(request)

    patched = _patch_http(monkeypatch, handler)
    task = "fix the banner copy on the gold page"
    with caplog.at_level(logging.INFO):
        result = launch_cloud_agent(task, confirmed=True, now=NOW)
    assert result["status"] == "launched"
    assert result["agent_id"] == AGENT_ID
    assert result["run_status"] == "CREATING"
    assert result["agent_url"] == f"https://cursor.com/agents/{AGENT_ID}"
    assert result["agent_url"] in result["message"]
    assert "CREATING" in result["message"]
    assert seen["path"] == "/v1/agents"
    assert seen["auth"] == f"Bearer {CURSOR_KEY}"
    body = seen["body"]
    assert body["autoCreatePR"] is True
    assert body["workOnCurrentBranch"] is False
    assert body["repos"] == [{"url": FIXED_REPO_URL, "startingRef": FIXED_STARTING_REF}]
    assert set(body) == {"prompt", "repos", "autoCreatePR", "workOnCurrentBranch"}
    assert body["prompt"]["text"].startswith(TASK_PREAMBLE)
    assert "Never merge." in body["prompt"]["text"]
    assert "Never push to main." in body["prompt"]["text"]
    assert "Never deploy." in body["prompt"]["text"]
    assert "Never touch Snowflake." in body["prompt"]["text"]
    assert "Never print secrets." in body["prompt"]["text"]
    assert task in body["prompt"]["text"]
    assert patched["base_url"] == "https://api.cursor.com"
    assert patched["timeout"].read == 30.0
    assert patched["timeout"].connect == 10.0
    assert CURSOR_KEY not in caplog.text
    assert XAI_KEY not in caplog.text
    assert task not in caplog.text
    assert CURSOR_KEY not in json.dumps(result)


def test_callers_cannot_choose_the_repo_or_branch():
    params = set(inspect.signature(launch_cloud_agent).parameters)
    assert "repo" not in params
    assert "branch" not in params
    assert "starting_ref" not in params
    with pytest.raises(TypeError):
        launch_cloud_agent("fix the banner copy", confirmed=True, repo="https://github.com/evil/other")


def test_task_text_cannot_retarget_the_repo(monkeypatch):
    _enable(monkeypatch)
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content.decode())
        return _created(request)

    _patch_http(monkeypatch, handler)
    launch_cloud_agent(
        "fix the banner copy on https://github.com/evil/other branch production",
        confirmed=True,
        now=NOW,
    )
    assert seen["body"]["repos"] == [{"url": FIXED_REPO_URL, "startingRef": "main"}]


def test_disabled_flag_does_not_call_the_api(monkeypatch):
    monkeypatch.setenv("CURSOR_API_KEY", CURSOR_KEY)
    _forbid_http(monkeypatch)
    result = launch_cloud_agent("fix the banner copy", confirmed=True)
    assert result["status"] == "disabled"
    assert "disabled" in result["message"].lower()
    assert CURSOR_KEY not in result["message"]


def test_missing_key_is_disabled(monkeypatch):
    monkeypatch.setenv("ENABLE_AGENT_LAUNCH", "true")
    _forbid_http(monkeypatch)
    result = launch_cloud_agent("fix the banner copy", confirmed=True)
    assert result["status"] == "disabled"
    assert "CURSOR_API_KEY" in result["message"]
    assert "disabled" in result["message"].lower()


def test_confirm_is_required_before_launch(monkeypatch):
    _enable(monkeypatch)
    _forbid_http(monkeypatch)
    for _ in range(5):
        result = launch_cloud_agent("fix the banner copy", confirmed=False, now=NOW)
        assert result["status"] == "confirmation_required"
        assert result["task"] == "fix the banner copy"
    _patch_http(monkeypatch, _created)
    launched = launch_cloud_agent("fix the banner copy", confirmed=True, now=NOW)
    assert launched["status"] == "launched"


@pytest.mark.parametrize(
    "task",
    [
        "Please merge the pull request",
        "deploy this to production",
        "DROP TABLE gold",
        "GRANT SELECT to the reader",
        "switch the snowflake role",
        "exfiltrate the API token",
        "print the secret key",
        "dump the tokens to the logs",
    ],
)
def test_blocked_keywords_do_not_launch(monkeypatch, task):
    _enable(monkeypatch)
    _forbid_http(monkeypatch)
    result = launch_cloud_agent(task, confirmed=True, now=NOW)
    assert result["status"] == "blocked"
    assert task not in result["message"]
    assert CURSOR_KEY not in result["message"]


def test_token_refresh_bugfix_is_not_treated_as_exfiltration(monkeypatch):
    _enable(monkeypatch)
    seen = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["calls"] += 1
        return _created(request)

    _patch_http(monkeypatch, handler)
    result = launch_cloud_agent("fix the token refresh bug", confirmed=True, now=NOW)
    assert result["status"] == "launched"
    assert seen["calls"] == 1


def test_blocked_and_unconfirmed_calls_do_not_consume_the_rate_limit(monkeypatch):
    _enable(monkeypatch)
    _forbid_http(monkeypatch)
    for _ in range(4):
        assert launch_cloud_agent("merge it", confirmed=True, now=NOW)["status"] == "blocked"
    _patch_http(monkeypatch, _created)
    assert launch_cloud_agent("fix the banner copy", confirmed=True, now=NOW)["status"] == "launched"


def test_rate_limit_is_three_launches_per_hour(monkeypatch):
    _enable(monkeypatch)
    seen = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["calls"] += 1
        return _created(request)

    _patch_http(monkeypatch, handler)
    for offset in (0, 1, 2):
        result = launch_cloud_agent("fix the banner copy", confirmed=True, now=NOW + offset)
        assert result["status"] == "launched"
    limited = launch_cloud_agent("fix the banner copy", confirmed=True, now=NOW + 10)
    assert limited["status"] == "rate_limited"
    assert seen["calls"] == 3
    later = launch_cloud_agent("fix the banner copy", confirmed=True, now=NOW + 3601)
    assert later["status"] == "launched"
    assert seen["calls"] == 4


def test_error_text_redacts_both_keys(monkeypatch, caplog):
    _enable(monkeypatch)
    monkeypatch.setenv("XAI_API_KEY", XAI_KEY)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=f"upstream failed {CURSOR_KEY} and {XAI_KEY}")

    _patch_http(monkeypatch, handler)
    with caplog.at_level(logging.DEBUG):
        result = launch_cloud_agent("fix the banner copy", confirmed=True, now=NOW)
    assert result["status"] == "error"
    assert CURSOR_KEY not in result["message"]
    assert XAI_KEY not in result["message"]
    assert "[redacted]" in result["message"]
    assert CURSOR_KEY not in caplog.text
    assert XAI_KEY not in caplog.text
    blob = json.dumps(result)
    assert CURSOR_KEY not in blob
    assert XAI_KEY not in blob

    reset_rate_limit()

    def explode(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"connection failed {CURSOR_KEY} {XAI_KEY}")

    _patch_http(monkeypatch, explode)
    with caplog.at_level(logging.DEBUG):
        failed = launch_cloud_agent("fix the banner copy", confirmed=True, now=NOW)
    assert failed["status"] == "error"
    assert CURSOR_KEY not in failed["message"]
    assert XAI_KEY not in failed["message"]
    assert "[redacted]" in failed["message"]
    assert CURSOR_KEY not in caplog.text
    assert XAI_KEY not in caplog.text


def test_task_that_contains_a_key_is_redacted_before_it_is_sent(monkeypatch):
    _enable(monkeypatch)
    monkeypatch.setenv("XAI_API_KEY", XAI_KEY)
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = request.content.decode()
        return _created(request)

    _patch_http(monkeypatch, handler)
    result = launch_cloud_agent(f"fix the banner copy {CURSOR_KEY} {XAI_KEY}", confirmed=True, now=NOW)
    assert result["status"] == "launched"
    assert CURSOR_KEY not in seen["body"]
    assert XAI_KEY not in seen["body"]
    assert "[redacted]" in seen["body"]


def test_status_reports_the_pull_request_when_ready(monkeypatch, caplog):
    _enable(monkeypatch)
    pr_url = "https://github.com/KapilArora26/lab/pull/7"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(f"/runs/{RUN_ID}"):
            return httpx.Response(
                200,
                json={
                    "id": RUN_ID,
                    "status": "FINISHED",
                    "git": {
                        "branches": [
                            {
                                "repoUrl": "github.com/KapilArora26/lab",
                                "branch": "cursor/fix-banner",
                                "prUrl": pr_url,
                            }
                        ]
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                "id": AGENT_ID,
                "status": "IDLE",
                "url": f"https://cursor.com/agents/{AGENT_ID}",
                "latestRunId": RUN_ID,
            },
        )

    _patch_http(monkeypatch, handler)
    with caplog.at_level(logging.INFO):
        result = cloud_agent_status(AGENT_ID)
    assert result["status"] == "ok"
    assert result["agent_status"] == "IDLE"
    assert result["run_status"] == "FINISHED"
    assert result["pr_url"] == pr_url
    assert pr_url in result["message"]
    assert CURSOR_KEY not in caplog.text
    tool = execute_tool_call(
        {"function": {"name": "cloud_agent_status", "arguments": json.dumps({"agent_id": AGENT_ID})}}
    )
    assert tool["pr_url"] == pr_url


def test_status_without_a_pull_request_says_it_is_not_ready(monkeypatch):
    _enable(monkeypatch)

    def handler(request: httpx.Request) -> httpx.Response:
        if "/runs/" in request.url.path:
            return httpx.Response(200, json={"id": RUN_ID, "status": "RUNNING"})
        return httpx.Response(
            200,
            json={"id": AGENT_ID, "status": "ACTIVE", "latestRunId": RUN_ID},
        )

    _patch_http(monkeypatch, handler)
    result = cloud_agent_status(AGENT_ID)
    assert result["status"] == "ok"
    assert result["run_status"] == "RUNNING"
    assert result["pr_url"] is None
    assert "not ready" in result["message"]


def test_status_rejects_a_bad_id_and_stays_disabled_without_a_key(monkeypatch):
    _enable(monkeypatch)
    _forbid_http(monkeypatch)
    invalid = cloud_agent_status("../v1/agents")
    assert invalid["status"] == "error"
    monkeypatch.delenv("CURSOR_API_KEY", raising=False)
    disabled = cloud_agent_status(AGENT_ID)
    assert disabled["status"] == "disabled"


def test_model_tool_call_cannot_confirm_or_change_the_repo(monkeypatch):
    _enable(monkeypatch)
    _forbid_http(monkeypatch)
    result = execute_tool_call(
        {
            "function": {
                "name": "launch_cloud_agent",
                "arguments": json.dumps(
                    {
                        "task": "fix the banner copy",
                        "confirmed": True,
                        "repo": "https://github.com/evil/other",
                        "startingRef": "production",
                    }
                ),
            }
        }
    )
    assert result["status"] == "confirmation_required"
    assert result["task"] == "fix the banner copy"
    unavailable = execute_tool_call({"function": {"name": "retire_agent", "arguments": "{}"}})
    assert unavailable["status"] == "error"
    names = [tool["function"]["name"] for tool in TOOL_DEFINITIONS]
    assert names == ["launch_cloud_agent", "cloud_agent_status"]


def test_retire_cancels_the_run_and_deletes_the_agent(monkeypatch, caplog):
    _enable(monkeypatch)
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path, request.headers.get("authorization")))
        if request.method == "POST":
            return httpx.Response(409, json={"error": "run_not_cancellable"})
        return httpx.Response(200, json={"id": AGENT_ID})

    _patch_http(monkeypatch, handler)
    with caplog.at_level(logging.INFO):
        result = retire_agent(AGENT_ID, RUN_ID)
    assert result["status"] == "retired"
    assert result["deleted"] is True
    assert seen[0][0] == "POST"
    assert seen[0][1] == f"/v1/agents/{AGENT_ID}/runs/{RUN_ID}/cancel"
    assert seen[1] == ("DELETE", f"/v1/agents/{AGENT_ID}", f"Bearer {CURSOR_KEY}")
    assert CURSOR_KEY not in caplog.text
    assert CURSOR_KEY not in json.dumps(result)


def test_dashboard_confirm_button_is_separate_from_ask(demo_wh):
    client = TestClient(create_app(demo_wh))
    html = client.get("/dashboard").text
    assert 'id="confirm-launch"' in html
    assert "Confirm cloud agent launch" in html
    script = html.split("<script>")[1]
    ask = script.split("getElementById('ask')")[1].split("getElementById('confirm-launch')")[0]
    assert "JSON.stringify({ persona, message })" in ask
    assert "confirm: true" not in ask
    assert "launch_task" not in ask
    assert "confirm: true" in script
    assert "launch_task: pendingTask" in script


def test_dashboard_launch_requires_confirm(demo_wh, monkeypatch):
    _enable(monkeypatch)
    seen = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["calls"] += 1
        seen["body"] = json.loads(request.content.decode())
        return _created(request)

    _patch_http(monkeypatch, handler)
    client = TestClient(create_app(demo_wh))
    held = client.post(
        "/v1/dashboard/chat",
        json={
            "persona": "builder",
            "message": "open a PR that fixes the banner",
            "launch_task": "fix the banner copy",
            "confirm": False,
            "repo": "https://github.com/evil/other",
            "branch": "production",
        },
    )
    assert held.status_code == 200
    assert held.json()["tool_result"]["status"] == "confirmation_required"
    assert held.json()["pending_launch"]["task"] == "fix the banner copy"
    assert seen["calls"] == 0
    launched = client.post(
        "/v1/dashboard/chat",
        json={
            "persona": "builder",
            "message": "open a PR that fixes the banner",
            "launch_task": "fix the banner copy",
            "confirm": True,
            "repo": "https://github.com/evil/other",
            "branch": "production",
        },
    )
    assert launched.status_code == 200
    body = launched.json()
    assert body["tool_result"]["status"] == "launched"
    assert body["tool_result"]["agent_url"] in body["reply"]
    assert seen["calls"] == 1
    assert seen["body"]["repos"] == [{"url": FIXED_REPO_URL, "startingRef": "main"}]
    assert "https://github.com/evil/other" not in json.dumps(seen["body"]["repos"])


def test_mock_chat_stays_read_only_when_launch_is_enabled(demo_wh, monkeypatch):
    _enable(monkeypatch)
    _forbid_http(monkeypatch)
    client = TestClient(create_app(demo_wh))
    response = client.post(
        "/v1/dashboard/chat",
        json={"persona": "narrator", "message": "open a PR that fixes the banner"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["provider"] == "mock"
    assert "Revenue at risk" in body["reply"]
    assert "tool_result" not in body
    assert "pending_launch" not in body


def test_flag_off_keeps_the_plain_chat_path(demo_wh):
    class Fake:
        provider = "grok"

        def chat(self, *, system, user):
            assert "launch_cloud_agent" not in system
            return "Gold says the mart is current."

        def complete(self, **kwargs):
            raise AssertionError("tools should stay unwired when launch is disabled")

    result = dashboard.answer(demo_wh, Fake(), "narrator", "What is at risk?")
    assert result["reply"] == "Gold says the mart is current."
    assert "pending_launch" not in result


def test_live_tool_call_waits_for_the_confirm_button(demo_wh, monkeypatch):
    _enable(monkeypatch)
    _forbid_http(monkeypatch)

    class Fake:
        provider = "grok"

        def complete(self, *, system, user, tools):
            names = [tool["function"]["name"] for tool in tools]
            assert names == ["launch_cloud_agent", "cloud_agent_status"]
            assert "launch_cloud_agent" in system
            return {
                "content": "",
                "tool_calls": [
                    {
                        "function": {
                            "name": "launch_cloud_agent",
                            "arguments": json.dumps(
                                {
                                    "task": "fix the banner copy",
                                    "confirmed": True,
                                    "repo": "https://github.com/evil/other",
                                }
                            ),
                        }
                    }
                ],
            }

        def chat(self, **kwargs):
            raise AssertionError("plain chat should not run when launch is enabled")

    result = dashboard.answer(demo_wh, Fake(), "builder", "open a PR that fixes the banner")
    assert result["tool_result"]["status"] == "confirmation_required"
    assert result["pending_launch"]["task"] == "fix the banner copy"
    assert "https://github.com/evil/other" not in result["reply"]
