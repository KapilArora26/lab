# Signal-to-Decision

Signal-to-Decision is the code half of a multi-bot business-intelligence system. A KPI drops overnight. This service notices it with a seasonal statistical rule, slices the loss, classifies redacted support text, attaches sourced outside events, and writes a decision brief. People still approve anything that spends money, changes production, or goes to an executive.

The Grok Bots (Atlas, Sentinel, Diagnostician, Builder, Narrator) are configured outside this repo. Their personas, skills, routines, and memory shapes are in [`bots/`](bots/) so they can be pasted into Grok Bot.

## Architecture

```text
warehouse (DuckDB, or Snowflake reads)
        |
        v
   Sentinel scan          statistical only, no model
        |  Case Card
        v
   Atlas lifecycle        cases, calendar, approvals, no metric SQL
        |
        +---- Diagnostician: slicer, ranker, evidence
        |            | capability gap
        |            v
        |         Builder spec and agent brief
        |            |
        |            v
        |     ticket_signals + external_signals + voice_brief
        |            |  Grok API, or the mock client
        |            v
        +---- Narrator: brief, voice script, charts, weekly page
```

Detection is a rule. The model is used only after a case exists, and only for unstructured text, live search, and speech.

| Piece | Where it lives |
| --- | --- |
| Seasonal residual detector, severity, de-duplication, calendar suppression | `src/signal_service/detection` |
| Dimension slicer, hypothesis ranker, Diagnosis Pack | `src/signal_service/diagnosis` |
| Ticket classifier, external search, voice | `src/signal_service/signals` and `src/signal_service/grok` |
| Case registry, SLA, freshness, build spec, PR check | `src/signal_service/orchestration` |
| Brief, voice script, charts, weekly page | `src/signal_service/narrative` |
| HTTP API | `src/signal_service/api` |
| House rules and skills for the Cursor agent | `.cursor/rules`, `.cursor/skills` |
| Bot personas, routines, connectors, memory | `bots/` |

## Demo incident

The generator builds 12 days of hourly sessions, orders, payments, a deploy feed, and support tickets. On 2026-11-20 at 02:00 UTC, Android checkout conversion in India drops. The lost checkouts sit on UPI and app version 4.2.1, which deployed at 01:40 UTC. Ticket text says the UPI payment failed after OTP. A few rows still contain email, phone, or card numbers; the classifier rejects them.

Stored session counts are downsampled. `demo_meta.session_weight` (15.625) and a $48 average order value scale revenue at risk back up. The seasonal baseline, not a flat average, is the comparison.

## Setup

Python 3.11 or newer.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

No API key is required. Without `XAI_API_KEY` the service uses the keyword fallback and the checked-in external-signal fixture. Without `SNOWFLAKE_PAT_TOKEN` it uses DuckDB only.

## Run locally

```bash
python -m signal_service.cli generate --out data/warehouse/demo.duckdb
python -m signal_service.cli eval
python -m signal_service.cli run-demo --db data/warehouse/demo.duckdb --out artifacts/demo-run.json
python -m signal_service.cli serve --db data/warehouse/demo.duckdb
```

`run-demo` writes `artifacts/demo-run.json` and `artifacts/demo-run.md` with the Case Card, the Diagnosis Pack before and after signals, and the brief. The build approval in that run is marked `simulated_for_demo` so the seeded incident can be briefed. Merge and the executive action stay `pending`.

HTTP:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | Process and warehouse check |
| POST | `/v1/pipeline/run` | Detect, diagnose, classify, brief |
| GET | `/v1/eval` | Golden-set score |

## Tests and eval

```bash
python -m pytest
python -m signal_service.cli eval
```

CI is `.github/workflows/ci.yml`. It installs the package and runs both commands. The golden set is `data/golden/tickets.jsonl` (70 labeled rows). The fallback classifier must reach topic accuracy of at least 0.85 and 0.80 on sentiment, severity, and product area. Eight paraphrases omit the fallback keywords on purpose, so a keyword tweak that only memorizes the easy rows can still fail.

The mocked Grok client is what CI uses for retries, schema checks, redaction, and the spend cap. Live calls are not required.

## Grok API

Read the key from `XAI_API_KEY`. Defaults:

| Use | Endpoint | Notes |
| --- | --- | --- |
| Ticket classification | `POST https://api.x.ai/v1/chat/completions` | `response_format.type = json_schema`, model `GROK_MODEL` (default `grok-4.7`) |
| Web and X search | `POST https://api.x.ai/v1/responses` | tools `web_search` and `x_search`. Items without an http(s) URL are dropped |
| Voice | `POST https://api.x.ai/v1/tts` | `voice_id` from `GROK_VOICE_ID` (default `eve`), `language` default `en` |

Timeouts, exponential backoff, and the daily spend cap are in `signal_service.grok.client` and `signal_service.safety.cost`. Logs record token counts and an input hash. They do not record ticket text or the key.

Planning rates for the circuit breaker (override with env vars; they are not an invoice): input $2 / million tokens, output $6 / million tokens, speech $15 / million characters. The seeded batch estimates at about $0.14. A mock run records $0.

## Warehouse

DuckDB is the default. The file is created by `generate` and is gitignored.

Snowflake is optional. The adapter in `src/signal_service/warehouse/snowflake.py` posts JSON-RPC `tools/call` to the MCP server:

`https://ulkfcdl-vl97193.snowflakecomputing.com/api/v2/databases/MCP_DB/schemas/MCP/mcp-servers/CURSOR_MCP_SERVER`

| Env var | Purpose |
| --- | --- |
| `SNOWFLAKE_PAT_TOKEN` | Bearer programmatic access token. Required for any live call. |
| `SNOWFLAKE_MCP_URL` | Overrides the URL above. |
| `SNOWFLAKE_SANDBOX` | Defaults to `MCP_DB.SIGNAL_SANDBOX`. |

Headers are `Authorization: Bearer <token>` and `X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN`. Reads may be `SELECT`, `SHOW`, `DESCRIBE`, or `WITH`. Writes must be `CREATE`, `INSERT`, `UPDATE`, or `DELETE` fully qualified to the sandbox. `DROP`, `ALTER`, `TRUNCATE`, and any other schema are refused in code. The token was not present in the build environment, so no sandbox objects were created.

Snowflake accounts often enforce an IP allowlist. Allowlist the host that will call the adapter. The agent that built this repo saw public IP `54.147.134.223`. That address is the build VM, not a permanent production egress.

## Deploy

This environment had no Render, Fly, or Railway credentials, and no new account was created. The service is ready to run as a container.

```bash
docker build -t signal-service .
docker run --rm -p 8000:8000 signal-service
```

[`render.yaml`](render.yaml) describes a Render web service with health check `/health`. The first request waits while the demo warehouse is generated.

What you need to deploy:

1. A Render account (or any host that runs this Dockerfile). No key from this repo will create one.
2. Optional `XAI_API_KEY` if you want live classification, search, and voice. Leave it empty to keep the mock path.
3. Optional `SNOWFLAKE_PAT_TOKEN` plus an allowlisted IP if you want the Snowflake adapter. Leave it empty to keep DuckDB.

There is no live URL until those credentials exist.

## What you configure in Grok Bot

Nothing in `bots/` runs on its own. Create five bots and paste the matching file:

| Bot | File | Data access |
| --- | --- | --- |
| Atlas, lead | `bots/atlas.md` | None. Case registry, calendar, preferences, lessons. |
| Sentinel | `bots/sentinel.md` | Read-only metrics. Routine every 15 minutes. |
| Diagnostician | `bots/diagnostician.md` | Read-only metrics, deploys, `signals.*`. |
| Builder | `bots/builder.md` | Cursor cloud agent and git PR creation. No merge. |
| Narrator | `bots/narrator.md` | The Diagnosis Pack only. |

Also paste `bots/routines.md`, `bots/connectors.md`, and the JSON in `bots/memory-schemas.json`. Bots hand each other the case file in `bots/handoff-contract.schema.json`.

Human gates, which the code will not skip:

| Gate | Who | Status in the demo run |
| --- | --- | --- |
| Build the ticket signal | Data owner | `simulated_for_demo` so the local pipeline can finish |
| Merge and deploy | Engineer | `pending` |
| Executive action | VP Growth | `pending` |

## Environment

| Variable | Default | Role |
| --- | --- | --- |
| `XAI_API_KEY` | unset | Live Grok calls. Unset uses the mock. |
| `GROK_MODEL` | `grok-4.7` | Chat and responses model. |
| `GROK_VOICE_ID` | `eve` | TTS voice. |
| `GROK_TIMEOUT_SECONDS` | `30` | HTTP timeout. |
| `GROK_MAX_RETRIES` | `3` | Retries on 429, 5xx, and schema failure. |
| `SIGNAL_BATCH_SIZE` | `25` | Ticket batch size. |
| `SIGNAL_DAILY_SPEND_CAP_USD` | `25` | Circuit breaker. |
| `SIGNAL_Z_THRESHOLD` | `3` | Residual z-score. |
| `SIGNAL_MIN_VOLUME` | `100` | Minimum sessions in an hour. |
| `SIGNAL_MIN_CONSECUTIVE` | `2` | Hours the drop must persist. |
| `SIGNAL_DUCKDB_PATH` | `data/warehouse/demo.duckdb` | Warehouse file for the API. |
| `SNOWFLAKE_PAT_TOKEN` | unset | Snowflake MCP token. |
| `SNOWFLAKE_MCP_URL` | the MCP URL above | Snowflake MCP endpoint. |
| `SNOWFLAKE_SANDBOX` | `MCP_DB.SIGNAL_SANDBOX` | Only schema writes may touch. |

## Schema

DuckDB signal tables, also listed for Snowflake under `MCP_DB.SIGNAL_SANDBOX` in [`sql/schema.sql`](sql/schema.sql):

- `signals.ticket_topics` — topic, sentiment, severity, product area, evidence snippet, confidence, model, provider, input hash
- `signals.external_events` — title, summary, source URL, source type, relevance
- `signals.spend_log` — kind, estimated USD, token counts
