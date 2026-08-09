from prometheus_client import generate_latest

from taskfloww_worker import metrics


def test_worker_metrics_defined_and_increment():
    metrics.TASKS_PROCESSED.labels(task_name="send_email", status="succeeded").inc()
    metrics.TASKS_PROCESSED.labels(task_name="send_email", status="failed").inc()
    metrics.HEARTBEATS_SENT.inc()
    metrics.DUPLICATE_DELIVERIES.inc()
    metrics.TASKS_IN_FLIGHT.inc()
    metrics.TASKS_IN_FLIGHT.dec()
    metrics.TASK_DURATION.labels(task_name="send_email").observe(0.01)

    out = generate_latest().decode()
    for name in [
        "taskfloww_worker_tasks_processed_total",
        "taskfloww_worker_heartbeats_sent_total",
        "taskfloww_worker_tasks_in_flight",
        "taskfloww_worker_task_duration_seconds",
        "taskfloww_worker_duplicate_deliveries_total",
    ]:
        assert name in out, f"expected metric {name!r} in exposition"
