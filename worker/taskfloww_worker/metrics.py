"""Prometheus metrics for the TaskFloww worker.

Exposed at ``/metrics`` on ``metrics.worker_port`` when metrics are enabled.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

TASKS_PROCESSED = Counter(
    "taskfloww_worker_tasks_processed_total",
    "Tasks processed by the worker.",
    ["task_name", "status"],
)

TASK_DURATION = Histogram(
    "taskfloww_worker_task_duration_seconds",
    "Task handler execution duration.",
    ["task_name"],
)

TASKS_IN_FLIGHT = Gauge(
    "taskfloww_worker_tasks_in_flight",
    "Tasks currently executing in this worker.",
)

HEARTBEATS_SENT = Counter(
    "taskfloww_worker_heartbeats_sent_total",
    "Heartbeat messages published.",
)

DUPLICATE_DELIVERIES = Counter(
    "taskfloww_worker_duplicate_deliveries_total",
    "Duplicate deliveries skipped by the idempotency guard.",
)


def start_metrics_server(port: int) -> None:
    """Start the Prometheus exposition HTTP server (background thread)."""
    start_http_server(port)
