"""Example TaskFloww task handlers.

A handler is a plain function taking the task ``payload`` (a dict) and returning
a JSON-serializable result (or None). Map it to a task name in the config:

    tasks:
      - name: send_email
        handler: examples.tasks:send_email
        queue: tasks.default

Run the worker from the `worker/` directory so ``examples`` is importable.
"""
from __future__ import annotations

import logging

log = logging.getLogger("taskfloww.examples")


def send_email(payload: dict) -> dict:
    """Pretend to send an email; echo a small result."""
    to = payload.get("to", "unknown")
    log.info("sending email to %s", to)
    return {"sent": True, "to": to}


def generate_report(payload: dict) -> dict:
    """Pretend to generate a report."""
    month = payload.get("month", "n/a")
    log.info("generating report for %s", month)
    return {"report": f"report-{month}", "rows": 0}


def always_fails(payload: dict) -> dict:
    """A handler that always raises — useful to exercise retries/DLQ."""
    raise RuntimeError("intentional failure for testing")
