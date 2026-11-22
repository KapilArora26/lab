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
| HTTP API and dashboard chat | `src/signal_service/api` |
| Cloud agent launch from dashboard chat | `src/signal_service/cloud_agents.py` |
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

On a Mac or Linux laptop, from the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
cp .env.example .env
set -a
source .env
set +a
python -m uvicorn signal_service.api.app:create_app --factory --host 127.0.0.1 --port 8000
```

Open [http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard).

`.env.example` lists `SNOWFLAKE_PAT_TOKEN`, `XAI_API_KEY`, `CURSOR_API_KEY`, and `ENABLE_AGENT_LAUNCH` with placeholder values only. Copy it to `.env` and replace the placeholders locally. `.env` is gitignored. Leave `ENABLE_AGENT_LAUNCH` as `false` until you want the dashboard to launch a cloud agent.

The first dashboard request builds `data/warehouse/demo.duckdb` when that file is missing. DuckDB does not call Snowflake. If you point this process at Snowflake, the laptop's public IP must be allowed by the Snowflake network rule.

The same warehouse can be exercised without the dashboard:

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
| POST | `/v1/ingest` | Land a batch in bronze and propagate it |
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
| `SNOWFLAKE_PAT_TOKEN` | Bearer programmatic access token. Required for any live call. `SNOWFLAKE_PAT` is accepted when the first name is unset. |
| `SNOWFLAKE_MCP_URL` | Overrides the URL above. |
| `SNOWFLAKE_SANDBOX` | Defaults to `MCP_DB.SIGNAL_SANDBOX`. |

Headers are `Authorization: Bearer <token>` and `X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN`. Reads may be `SELECT`, `SHOW`, `DESCRIBE`, or `WITH`. Writes must be fully qualified to the sandbox. The adapter allows `CREATE` (including `STREAM`, `TASK`, `DYNAMIC TABLE`, and `PROCEDURE`), `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING`, and `ALTER TASK ... RESUME`. `DROP`, `TRUNCATE`, `GRANT`, any other `ALTER`, and any other schema are refused in code. The adapter binds `?` placeholders and rewrites `signals.*` plus the demo table names onto the sandbox, so the same pipeline runs on either warehouse.

## Medallion pipeline

Landing is on demand. Nothing is scheduled. `python -m signal_service.cli ingest` or `POST /v1/ingest` appends a batch to bronze. Silver and gold then update before that call returns when the warehouse is DuckDB, and on Snowflake when the role can create `LOAD_STAGING`. Streams and tasks are the automatic path for inserts that do not go through the CLI. Each layer reads only the layer before it. The dashboard reads gold.

```text
ingest (CLI or POST /v1/ingest)     on demand, never a schedule
        |
        v
   BRONZE_*   raw landing, PII allowed
              _ingested_at, _source, _batch_id
        |
        |  DuckDB sync MERGE; Snowflake CALL LOAD_STAGING (tasks use the same call)
        v
   SILVER_*   typed, deduplicated, redacted
              _loaded_at, _batch_id, SILVER_REJECTS
        |
        v
   GOLD_*     conversion, revenue at risk, cases, topics, spend
              _loaded_at, _batch_id
        |
        +-- GOLD_PIPELINE_LAG   bronze_ingested_at, silver_loaded_at,
        |                       gold_loaded_at, staging_to_gold_seconds
        +-- GOLD_HEALTH
        +-- PIPELINE_RUNS       one row per layer step
```

```bash
python -m signal_service.cli ingest --batch new --db data/warehouse/demo.duckdb
python -m signal_service.cli ingest --batch file data/batches/example.json
```

`--batch new` asks the generator for the next hours after the latest bronze timestamp (`SIGNAL_INGEST_HOURS`, default 6). The same `_batch_id` is a no-op. Raw PII stays in `BRONZE_SUPPORT_TICKETS`. Silver tickets are the rows that pass `assert_redacted`; the others land in `SILVER_REJECTS` with the text replaced by `[redacted]`.

`SIGNAL_PIPELINE_MODE` is `auto`, `sync`, `task`, `procedure`, or `dynamic_table`. DuckDB is always `sync`. On Snowflake, `auto` creates table streams, serverless triggered tasks, and `MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(batch VARCHAR)` when the role allows it. The tasks have no `SCHEDULE`. `TASK_BRONZE_TO_SILVER` runs when any bronze stream has data (`STREAM_BRONZE_GROUPS`, `STREAM_BRONZE_SUPPORT_TICKETS`, `STREAM_BRONZE_DEPLOYS`, `STREAM_BRONZE_DEMO_META`, `STREAM_BRONZE_GOLDEN_TICKETS`, `STREAM_BRONZE_EXTERNAL_EVENTS`). `TASK_SILVER_TO_GOLD` runs when `STREAM_SILVER_GROUPS` has data. Both omit `WAREHOUSE`. `TARGET_COMPLETION_INTERVAL = '1 MINUTE'` is the serverless completion target Snowflake requires on a triggered task, and `OVERLAP_POLICY = NO_OVERLAP` keeps `ALLOW_OVERLAPPING_EXECUTION` false. No account parameter is set. Snowflake's own minimum gap between trigger runs stays 30 seconds. `RERUN_STAGING(batch VARCHAR)` is the only reprocess entry point, so the existing one-argument procedure is replaced instead of overloaded. Each task body is a single statement, `CALL MCP_DB.SIGNAL_SANDBOX.LOAD_STAGING(NULL)`. The procedure writes `PIPELINE_RUNS`. The only `ALTER` in the migration is `ALTER TASK ... RESUME`, because a new task is suspended. A denied privilege is recorded and that statement is skipped. A SQL compilation error is recorded too, and it is not labeled as a missing privilege. Snowflake ingest calls `LOAD_STAGING` and does not read bronze rows into the client. If the read tool truncates a result page, the adapter raises instead of merging the slice. Dynamic tables at `TARGET_LAG = '1 minute'` are attempted only when `SIGNAL_SNOWFLAKE_WAREHOUSE` is set and tasks could not be created. The ingest response reports `mode`, `propagation`, and `missing_privileges`.

`python -m signal_service.cli rerun <batch_id>` calls `RERUN_STAGING` on Snowflake and reprocesses a batch whose gold row was marked success. It does not `DROP` or `TRUNCATE`. `ingest --batch seed --rerun` is the same path for the seed batch. If `LOAD_STAGING` or `RERUN_STAGING` fails to compile, install stops and the command exits non-zero. Tasks are not created after that failure. Deploy fails when layer counts do not reconcile.

A monitor reads:

```sql
SELECT * FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS ORDER BY started_at DESC;
SELECT * FROM MCP_DB.SIGNAL_SANDBOX.GOLD_PIPELINE_LAG;
```

`deploy-snowflake` applies `sql/snowflake/001_sandbox.sql` and `sql/snowflake/002_medallion.sql`, loads the seed batch through ingest, and checks GROUPS 2,960, SESSIONS 429,200, PAYMENTS 51,800, ORDERS 42,692, SUPPORT_TICKETS 92, and TICKET_TOPICS 73.

## Dashboard

`GET /dashboard` is a server-rendered page. Chart.js loads from a CDN and the page refreshes from `GET /v1/dashboard/data` every 60 seconds. That JSON, and the page itself, read `GOLD_*` objects. Layer progress is the audit table, not an earlier layer. Chat is `POST /v1/dashboard/chat`. It uses the same Grok client, the key from `XAI_API_KEY`, the redaction check, and the daily spend cap. With no key it answers from the gold summary with the mock client. The chosen persona is one of `bots/atlas.md`, `sentinel.md`, `diagnostician.md`, `builder.md`, or `narrator.md`. Chat cannot land a batch or write a warehouse row. When `ENABLE_AGENT_LAUNCH` is on, a confirmed request can launch a cloud agent that opens a pull request. See [Cloud agent pull requests](#cloud-agent-pull-requests).

```text
GET /dashboard
    |
    +-- gold health, conversion vs baseline, revenue at risk
    +-- open cases, ticket topics, spend vs cap
    +-- PIPELINE_RUNS  (freshness and the load-data progress)
    |
    +-- Load new data --> confirm --> POST /v1/ingest
    |                         |
    |                         +-- poll PIPELINE_RUNS for that batch
    |
    +-- Ask --> POST /v1/dashboard/chat --> gold summary + persona
                (read only; mock when XAI_API_KEY is unset)
                |
                +-- launch_cloud_agent --> Confirm cloud agent launch
                    --> POST confirm=true --> pull request on the fixed repo
```

```bash
python -m signal_service.cli serve --db data/warehouse/demo.duckdb
```

Open `http://127.0.0.1:8000/dashboard`. The monitor query is the same one the page polls:

```sql
SELECT * FROM MCP_DB.SIGNAL_SANDBOX.PIPELINE_RUNS ORDER BY started_at DESC;
```

## Cloud agent pull requests

Dashboard chat can ask Grok to launch a Cursor cloud agent. The server calls `POST https://api.cursor.com/v1/agents` with `Authorization: Bearer` and the key from `CURSOR_API_KEY`. The repository is fixed to `https://github.com/KapilArora26/lab` and the starting ref is `main`. The request sets `autoCreatePR` and leaves `workOnCurrentBranch` false, so the agent opens a pull request from a new branch. Chat cannot change the repository or the branch.

`ENABLE_AGENT_LAUNCH` defaults to off. With the flag off, or with `CURSOR_API_KEY` unset, the tool replies that launch is disabled and the read-only chat path is unchanged. With the flag on, Grok may call `launch_cloud_agent(task)`. The page then shows **Confirm cloud agent launch**. Nothing is sent to Cursor until that button is pressed. `cloud_agent_status(agent_id)` reads `GET /v1/agents/{id}` and the latest run, and it includes the pull request URL when the run has one.

Every task is prefixed with a fixed preamble: never merge, never push to main, never deploy, never touch Snowflake, never print secrets. Tasks that mention merge, deploy, drop, grant, role, or secret or token exfiltration are rejected. The process allows 3 launches per hour. Keys are read from the environment only. They are not written to logs, replies, or error text.

## Deploy to Snowflake

The migrations are [`sql/snowflake/001_sandbox.sql`](sql/snowflake/001_sandbox.sql) and [`sql/snowflake/002_medallion.sql`](sql/snowflake/002_medallion.sql). They create only `MCP_DB.SIGNAL_SANDBOX`. `sessions`, `payments`, and `orders` are views over silver groups. They expand each group with `ARRAY_GENERATE_RANGE`, which is the same 0-based range the DuckDB generator materializes.

```bash
python -m signal_service.cli deploy-snowflake
python -m signal_service.cli run-demo --backend snowflake --out artifacts/snowflake-run.json
```

`deploy-snowflake` applies both migrations, installs streams, triggered tasks, and `LOAD_STAGING` when the role allows, and loads the seed batch through ingest. `TASK_BRONZE_TO_SILVER` and `TASK_SILVER_TO_GOLD` are created with no `SCHEDULE`. They fire from `WHEN SYSTEM$STREAM_HAS_DATA(...)`: the bronze task on `STREAM_BRONZE_GROUPS`, `STREAM_BRONZE_SUPPORT_TICKETS`, `STREAM_BRONZE_DEPLOYS`, `STREAM_BRONZE_DEMO_META`, `STREAM_BRONZE_GOLDEN_TICKETS`, and `STREAM_BRONZE_EXTERNAL_EVENTS`, and the silver task on `STREAM_SILVER_GROUPS`. Every one of those streams is `ON TABLE`. Table streams are a supported trigger source, and creating the stream enables change tracking on the table. This migration does not add a stream on a view, a hybrid table, or an external table. A view stream would need `CHANGE_TRACKING` on the view. The tasks stay serverless (`USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'`, `TARGET_COMPLETION_INTERVAL = '1 MINUTE'`, no warehouse) with `OVERLAP_POLICY = NO_OVERLAP`. It does not delete rows. A second deploy is a no-op when that batch already reconciled. If a gold success row does not match the table counts, deploy fails and the batch can be reprocessed with `rerun`. It then checks the expansion counts and classifies the seeded incident. `run-demo --backend snowflake` runs detection, diagnosis, ticket classification, external signals, and the brief against that warehouse. Without `XAI_API_KEY` it uses the mock client, same as the DuckDB demo. The adapter reads `SNOWFLAKE_PAT_TOKEN`, or `SNOWFLAKE_PAT` when that is the name present in the environment.

The Snowflake run matched the local DuckDB demo: case `C-2026-1120-001`, Android checkout conversion in India, and H1 “Payment gateway failures on UPI” at confidence 0.80 (0.8018 before the brief rounds it). The case card, both diagnosis packs, the signal counts (73 classified, 3 rejected), the brief, and the voice script were the same.

Objects left in `MCP_DB.SIGNAL_SANDBOX` after that run:

| Object | Kind | Rows |
| --- | --- | --- |
| `GROUPS` | table | 2,960 |
| `SUPPORT_TICKETS` | table | 92 |
| `DEPLOYS` | table | 3 |
| `DEMO_META` | table | 6 |
| `GOLDEN_TICKETS` | table | 70 |
| `SESSIONS` | view | 429,200 |
| `PAYMENTS` | view | 51,800 |
| `ORDERS` | view | 42,692 |
| `TICKET_TOPICS` | table | 73 |
| `EXTERNAL_EVENTS` | table | 2 |
| `SPEND_LOG` | table | 0 |
| `MEMORY_CASES` | table | 3 |
| `MEMORY_CALENDAR` | table | 2 |
| `MEMORY_CAPABILITIES` | table | 3 |
| `MEMORY_PREFERENCES` | table | 3 |
| `MEMORY_LESSONS` | table | 0 |

`SPEND_LOG` stays empty because this path records spend on the case file. `MEMORY_LESSONS` stays empty until a person records an outcome. The mock run recorded $0. The live-call estimate for the same batch is about $0.14.

Snowflake accounts often enforce an IP allowlist. This cloud agent does not have one stable public address. During the load, Snowflake login history for the programmatic token recorded `3.209.66.12`, `13.219.159.123`, `3.228.178.73`, and `52.44.113.131`. `https://api.ipify.org` returned `13.219.159.123` and, on one check, `54.236.146.182`. An earlier build VM saw `54.147.134.223`. Allowlist the host that will call the adapter. None of these addresses is a permanent production egress.

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
| `CURSOR_API_KEY` | unset | Cursor Cloud Agents API key. Unset disables launch. |
| `ENABLE_AGENT_LAUNCH` | off | When `true`, dashboard chat may launch a cloud agent. |
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
| `SNOWFLAKE_PAT_TOKEN` | unset | Snowflake MCP token. `SNOWFLAKE_PAT` is the fallback name. |
| `SNOWFLAKE_MCP_URL` | the MCP URL above | Snowflake MCP endpoint. |
| `SNOWFLAKE_SANDBOX` | `MCP_DB.SIGNAL_SANDBOX` | Only schema writes may touch. |
| `SIGNAL_PIPELINE_MODE` | `auto` | `sync`, `task`, `procedure`, or `dynamic_table`. |
| `SIGNAL_INGEST_HOURS` | `6` | Hours generated by `ingest --batch new`. |
| `SIGNAL_SNOWFLAKE_WAREHOUSE` | unset | Warehouse name for the dynamic-table fallback. |

## Schema

DuckDB signal tables, also listed for Snowflake under `MCP_DB.SIGNAL_SANDBOX` in [`sql/schema.sql`](sql/schema.sql):

- `signals.ticket_topics` — topic, sentiment, severity, product area, evidence snippet, confidence, model, provider, input hash
- `signals.external_events` — title, summary, source URL, source type, relevance
- `signals.spend_log` — kind, estimated USD, token counts
