---
name: write-eval-set
description: Build and run the golden-set eval harness for a signal-service classifier. Use when adding labeled examples or changing the eval gate.
---

# Write an eval set

## Building the set

1. Put labeled JSONL at `data/golden/<task>.jsonl`. One object per line.
2. Required fields for tickets: `id`, `text`, `topic`, `sentiment`, `severity`, `product_area`.
3. At least 50 rows. Cover every taxonomy topic, plus paraphrases that do not use the fallback keywords. Those paraphrases are how the gate stays honest.
4. Texts are already redacted. Do not paste real customer messages. Do not include emails, phones, or card numbers.
5. Labels are written by a person. Never generate labels by calling the classifier you are scoring.

## Running it

```bash
python -m signal_service.cli eval
```

The harness scores the fallback classifier and checks that a mocked Grok client returns schema-valid rows. Thresholds live in `signal_service/eval/harness.py`. CI fails when the fallback topic accuracy drops below 0.85.

## Reporting

Copy the printed accuracy, the row count, and the pass/fail line into the pull request under "Eval results". If you add rows, say how many and which topic they cover.
