---
name: grok-live-search-wrapper
description: Wrap Grok live web and X search for external signals, with source capture and dedupe. Use when changing external_signals.
---

# Grok live search wrapper

## Pattern

1. Build a query from the case metric, segment, and the leading slice (payment method, app version). Do not include ticket text.
2. Call `POST https://api.x.ai/v1/responses` with model `grok-4.7` and tools `{"type": "web_search"}` and `{"type": "x_search"}`.
3. Ask for JSON objects with `title`, `summary`, `source_url`, `source_type` (`web` or `x`), `relevance` (0 to 1), and `observed_at`.
4. Validate with `ExternalSignal`. Drop any item whose `source_url` is missing or not http(s). Hallucinated events without a source never reach the Diagnosis Pack.
5. Deduplicate by canonical URL (strip trailing slash and query string). Keep the higher relevance.
6. Cap at 5 items. Sort by relevance descending.
7. When `XAI_API_KEY` is absent, return `data/fixtures/external_signals.json` filtered by the same URL rule. Set `provider: mock`.
8. Citations in the Diagnosis Pack use ids `external#<n>` plus the source URL. The narrator may quote the title and the URL and nothing beyond the stored summary.
