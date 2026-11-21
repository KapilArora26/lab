CAPABILITIES = [
    {
        "name": "ticket_signals",
        "table": "signals.ticket_topics",
        "eval_threshold": 0.85,
        "description": "Classifies redacted support text into the fixed taxonomy.",
    },
    {
        "name": "external_signals",
        "table": "signals.external_events",
        "eval_threshold": None,
        "description": "Sourced web and X events for an incident window.",
    },
    {
        "name": "voice_brief",
        "table": None,
        "eval_threshold": None,
        "description": "Speaks the narrator script with the Grok text-to-speech API.",
    },
]
