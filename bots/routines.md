# Routines

These are Grok Bot routines. The repo implements the functions they call. A person creates the schedule in Grok Bot; nothing here runs on a timer by itself.

| Routine | Bot | Schedule | What it calls |
| --- | --- | --- | --- |
| Sentinel sweep | Sentinel | Every 15 minutes for tier-1 KPIs. Hourly for tier-2. | `POST /v1/pipeline/run` or `signal_service.detection.sentinel.scan` |
| Case SLA watchdog | Atlas | Every 30 minutes | `sla_nudges`: nudge when a case has no acknowledgement for 30 minutes, escalate at 120 minutes |
| Signal freshness | Atlas | Daily | `freshness_check`: latest `signals.ticket_topics` row younger than 26 hours, Grok error rate at or under 5 percent, spend at or under `SIGNAL_DAILY_SPEND_CAP_USD` |
| Weekly review | Narrator | Monday 07:00 | `weekly_review_page` over the case registry |
| Memory hygiene | Atlas | Weekly | Close stale cases. Ask for an outcome. `record_outcome` writes a lesson fingerprint and sets status closed |

Shadow mode for a rollout: for the first two weeks the sweep posts only to the analyst channel. Executives are added after the weekly review shows acceptable precision.
