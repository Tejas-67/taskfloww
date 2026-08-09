"""Fault-tolerance unit tests for the worker execution path.

These drive ``_execute`` (runs in the pool) → ``_finish`` (marshalled to the IO
thread) with fakes standing in for the pika channel/connection, proving:
  * a successful handler publishes a *succeeded* Result and acks the delivery;
  * a raising handler publishes a *failed* Result (with the error) and STILL
    acks — the orchestrator, not the broker, owns retries (at-least-once);
  * an unknown task name fails cleanly instead of hanging the message;
  * a redelivery *after* completion is ignored (idempotent — dedupe via the
    ``_processed`` set, not just the in-flight set).
"""
import json
import textwrap

from taskfloww_worker import messages
from taskfloww_worker.config import load_config
from taskfloww_worker.registry import Registry
from taskfloww_worker.worker import Worker

CONFIG = textwrap.dedent(
    """
    broker:
      uri: amqp://localhost:5672/
    queues:
      definitions:
        - name: tasks.default
          routing_key: priority.default
      dead_letter:
        name: tasks.dlq
    tasks:
      - name: send_email
        handler: examples.tasks:send_email
        queue: tasks.default
    """
)


class _FakeChannel:
    """Captures publishes and acks; runs on the (simulated) IO thread."""

    def __init__(self):
        self.published = []  # list of (routing_key, decoded-body dict)
        self.acked = []

    def basic_publish(self, exchange=None, routing_key=None, body=None, properties=None):
        self.published.append((routing_key, json.loads(body)))

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)


class _SyncConn:
    """add_callback_threadsafe runs the callback inline (as the IO thread would)."""

    def add_callback_threadsafe(self, cb):
        cb()


def _worker(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(CONFIG)
    cfg = load_config(str(p), environ={})
    w = Worker(cfg, Registry(), worker_id="w-test")
    w._ch = _FakeChannel()
    w._conn = _SyncConn()
    return w


def _msg(execution_id: str, task_name: str = "send_email") -> messages.TaskMessage:
    return messages.TaskMessage.from_bytes(json.dumps({
        "task_id": "task-" + execution_id, "execution_id": execution_id,
        "task_name": task_name, "payload": {"k": "v"}, "attempt": 1,
    }).encode())


def test_success_publishes_succeeded_result_and_acks(tmp_path):
    w = _worker(tmp_path)
    w.registry.register("send_email", lambda payload: {"echo": payload["k"]})

    w._execute(_msg("exec-ok"), delivery_tag=11)

    assert w._ch.acked == [11]
    assert len(w._ch.published) == 1
    _, body = w._ch.published[0]
    assert body["status"] == "succeeded"
    assert body["execution_id"] == "exec-ok"
    assert body["result"] == {"echo": "v"}
    assert "error" not in body
    # completed executions are remembered for idempotent redelivery handling
    assert "exec-ok" in w._processed
    assert "exec-ok" not in w._in_flight


def test_handler_failure_publishes_failed_result_and_still_acks(tmp_path):
    w = _worker(tmp_path)

    def boom(payload):
        raise ValueError("kaboom")

    w.registry.register("send_email", boom)

    w._execute(_msg("exec-fail"), delivery_tag=22)

    # Acked so the broker doesn't redeliver forever; the orchestrator drives retry.
    assert w._ch.acked == [22]
    _, body = w._ch.published[0]
    assert body["status"] == "failed"
    assert "ValueError" in body["error"]
    assert "kaboom" in body["error"]


def test_unknown_task_fails_cleanly(tmp_path):
    w = _worker(tmp_path)  # no handler registered at all

    w._execute(_msg("exec-unknown", task_name="does_not_exist"), delivery_tag=33)

    assert w._ch.acked == [33]
    _, body = w._ch.published[0]
    assert body["status"] == "failed"
    assert "no handler registered" in body["error"]


def test_redelivery_after_completion_is_ignored(tmp_path):
    """At-least-once: a duplicate that arrives AFTER completion must not re-run."""
    w = _worker(tmp_path)
    calls = []
    w.registry.register("send_email", lambda payload: calls.append(1))

    # First delivery runs to completion (marks exec-dup processed).
    w._execute(_msg("exec-dup"), delivery_tag=41)
    assert len(calls) == 1

    # Redelivery of the same execution id — _on_message must ack without resubmitting.
    class _Method:
        def __init__(self, tag):
            self.delivery_tag = tag

    class _Pool:
        def __init__(self):
            self.submitted = []

        def submit(self, fn, *args):
            self.submitted.append(args)
            raise AssertionError("completed execution must not be resubmitted")

    w._pool = _Pool()
    body = json.dumps({
        "task_id": "task-exec-dup", "execution_id": "exec-dup",
        "task_name": "send_email", "payload": {"k": "v"}, "attempt": 1,
    }).encode()
    w._on_message(w._ch, _Method(42), None, body)

    assert len(calls) == 1  # handler not called again
    assert 42 in w._ch.acked
    assert w._pool.submitted == []
