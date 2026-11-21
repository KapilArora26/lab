---
name: grok-structured-classifier
description: Classify redacted text with the Grok API into a fixed taxonomy using structured outputs. Use when changing ticket_signals or adding another classifier.
---

# Grok structured classifier

## Pattern

1. Define the taxonomy in `data/taxonomy.json`. The Pydantic model uses those values as literals. Do not accept free-text topics.
2. Build the chat-completions request:

```json
{
  "model": "grok-4.7",
  "messages": [
    {"role": "system", "content": "Classify the redacted support text. Return only the schema."},
    {"role": "user", "content": "<json array of {id, text}>"}
  ],
  "response_format": {
    "type": "json_schema",
    "json_schema": {"name": "ticket_batch", "strict": true, "schema": "<TicketBatch schema>"}
  }
}
```

3. Endpoint: `POST https://api.x.ai/v1/chat/completions`. Key: `XAI_API_KEY`.
4. Parse `choices[0].message.content` as JSON and validate with Pydantic. Require one output item per input id.
5. `evidence_snippet` must be a substring of that item's redacted text and at most 140 characters. Drop the item if it is not.
6. Confidence below 0.5 does not count as support in the hypothesis ranker.
7. The fallback classifier in `signal_service/signals/fallback.py` is used when the key is missing or the circuit breaker is open. It must stay keyword-based and deterministic. Do not "fix" the golden set by copying model output into the labels.
8. Log `input_hash` (SHA-256 of the redacted text), token usage, and latency. The log line must not contain the text.

## Confidence handling

- Store the model confidence on the row.
- The diagnosis ranker uses topic counts among rows with confidence >= 0.5.
- A batch that fails schema validation is retried. After the last retry, mark the batch failed and continue the case with `provider_status: degraded`.
