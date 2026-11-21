---
name: add-signal-module
description: Add a new signal-service module (interface, schema, writer, tests, eval, registration). Use when building ticket_signals, external_signals, voice_brief, or a sibling such as review_signals.
---

# Add a signal module

Follow `.cursor/rules` while doing this. Detection stays statistical; only new unstructured or outside-world signals belong here.

## Steps

1. Name the module `signal_service/signals/<name>.py`. It exposes one function `run_<name>(warehouse, client, request) -> RunStats`.
2. Add a Pydantic result model in `signal_service/models.py`. Fields are typed. No free-form blobs.
3. Add the warehouse table under the `signals` schema (DuckDB) and the matching `CREATE TABLE IF NOT EXISTS MCP_DB.SIGNAL_SANDBOX.<table>` statement in `sql/schema.sql`. Writers use the warehouse port, not a new connection.
4. Read inputs through the warehouse. Run `assert_redacted` before any text is sent to Grok. Reject rows that fail.
5. Call Grok only through `signal_service.grok.client`. Structured outputs for classification, the responses API for search, `/v1/tts` for audio.
6. Check `SpendGuard.authorize` before each batch. Persist token usage and estimated cost, not the prompt.
7. Add unit tests with the mock client, including an empty batch, a PII rejection, and a schema-invalid model response.
8. If the module classifies text, add at least 50 labeled rows to `data/golden/` and make `python -m signal_service.cli eval` enforce the threshold.
9. Register the capability in `signal_service/orchestration/capabilities.py` with the module name, table, owning PR field, and eval threshold.
10. Document the new env vars in the README. Do not add a secret default.

## Done when

- `pytest` passes with no API key.
- The eval gate passes.
- A demo case can cite the new table by id.
