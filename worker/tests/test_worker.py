"""Unit tests for the worker's non-IO logic (queue resolution + dedupe)."""
import textwrap

from taskfloww_worker.config import load_config
from taskfloww_worker.registry import Registry
from taskfloww_worker.worker import Worker

CONFIG = textwrap.dedent(
    """
    broker:
      uri: amqp://guest:guest@localhost:5672/
    queues:
      default_exchange: taskfloww.direct
      dead_letter_exchange: taskfloww.dlx
      definitions:
        - name: tasks.high
          routing_key: priority.high
        - name: tasks.default
          routing_key: priority.default
      dead_letter:
        name: tasks.dlq
    tasks:
      - name: send_email
        handler: examples.tasks:send_email
        queue: tasks.default
      - name: generate_report
        handler: examples.tasks:generate_report
        queue: tasks.high
    """
)


def _worker(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(CONFIG)
    cfg = load_config(str(p), environ={})
    reg = Registry()
    reg.load_from_config(cfg)
    return Worker(cfg, reg, worker_id="w-test")


class _FakeChannel:
    def __init__(self):
        self.acked = []

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)


class _FakeMethod:
    def __init__(self, tag):
        self.delivery_tag = tag


class _FakePool:
    def __init__(self):
        self.submitted = []

    def submit(self, fn, *args):
        self.submitted.append(args)


def test_resolve_queues(tmp_path):
    w = _worker(tmp_path)
    assert w._queues == ["tasks.default", "tasks.high"]


def _msg(execution_id: str) -> bytes:
    import json
    return json.dumps({
        "task_id": "t-" + execution_id, "execution_id": execution_id,
        "task_name": "send_email", "payload": {}, "attempt": 1,
    }).encode()


def test_dedupe_duplicate_delivery(tmp_path):
    w = _worker(tmp_path)
    ch = _FakeChannel()
    w._pool = _FakePool()

    # first delivery of exec-1 → submitted to pool, not acked yet
    w._on_message(ch, _FakeMethod(1), None, _msg("exec-1"))
    assert len(w._pool.submitted) == 1
    assert ch.acked == []

    # duplicate delivery of exec-1 while still in-flight → acked, not resubmitted
    w._on_message(ch, _FakeMethod(2), None, _msg("exec-1"))
    assert len(w._pool.submitted) == 1
    assert ch.acked == [2]


def test_malformed_message_is_acked(tmp_path):
    w = _worker(tmp_path)
    ch = _FakeChannel()
    w._pool = _FakePool()
    w._on_message(ch, _FakeMethod(5), None, b"{not json")
    assert ch.acked == [5]
    assert w._pool.submitted == []
