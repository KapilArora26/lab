"""Decide whether a stream-triggered load may process a batch again.

``TASK_SILVER_TO_GOLD`` calls ``LOAD_STAGING(NULL)``. The procedure used to
rewrite ``SILVER_GROUPS`` in the same transaction that read
``STREAM_SILVER_GROUPS``. Snowflake advances a stream offset to the
transaction start, so those writes showed up as new stream rows after
commit. The task body still returned successfully, and a failed gold audit
left the batch eligible, so the task ran again every 30 seconds and inserted
another failed ``PIPELINE_RUNS`` pair.

A batch that already has a gold success or a gold failure is consumed and
not loaded again. ``RERUN_STAGING`` is the explicit retry. The procedure
also drains ``STREAM_SILVER_GROUPS`` in a second transaction so the silver
writes from the load do not wake the task.
"""

from __future__ import annotations

TERMINAL_GOLD = frozenset({"success", "failed"})


def stream_load_action(gold_status: str | None) -> str:
    """Return ``load`` or ``consume``.

    ``consume`` means the stream is read and committed and no new audit row
    is written. ``load`` means this is the one pass that records silver and
    gold for the batch.
    """
    if gold_status in TERMINAL_GOLD:
        return "consume"
    return "load"
