# Connectors

Each bot gets the smallest scope that does its job. Atlas gets no data connector.

| Connector | Bot | Scope in this repo | Setup outside the repo |
| --- | --- | --- | --- |
| Warehouse, read-only | Sentinel, Diagnostician | DuckDB file from `python -m signal_service.cli generate`. Optional Snowflake reads through the MCP adapter. | Point the bot at the semantic metrics only. Do not grant raw tables or PII columns. |
| Warehouse, sandbox write | Builder, signal-service job | `signals.*` in DuckDB. On Snowflake, only `MCP_DB.SIGNAL_SANDBOX`, and only when `SNOWFLAKE_PAT_TOKEN` is set. | Allowlist the runner IP. Create the programmatic access token. Do not grant `DROP` or `ALTER` on other schemas. |
| Deploy feed | Diagnostician | `deploys` table in the demo warehouse. | Replace with the real release feed, still read-only. |
| Support text | signal-service `ticket_signals` | `support_tickets.text_redacted`. Rows that fail redaction are counted and skipped. | Export redacted text only. |
| Git host and CI | Builder | The agent opens a PR. `verify_pr` reads status. Merge stays false. | Grant the agent branch and PR creation. Do not grant merge. |
| Cursor cloud agent | Builder | Launch and watch a run. The repo rules in `.cursor/rules` and skills in `.cursor/skills` are the house standard. | Connect the Cursor account the bot is allowed to launch. |
| Chat | Atlas, Narrator | Not implemented as a network client. The case thread is the audit log once the bot is connected to #bi-war-room. | Bind the bots to that channel only. |
| Grok API | signal-service | `XAI_API_KEY`. Mock client when unset. | Store the key as an environment variable on the host. |

Snowflake MCP URL (override with `SNOWFLAKE_MCP_URL`):

`https://ulkfcdl-vl97193.snowflakecomputing.com/api/v2/databases/MCP_DB/schemas/MCP/mcp-servers/CURSOR_MCP_SERVER`

Headers: `Authorization: Bearer $SNOWFLAKE_PAT_TOKEN` and `X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN`. Tools: `sql_read_tool`, `sql_write_tool`.
