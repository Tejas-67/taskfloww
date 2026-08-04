import json

from taskfloww_worker import messages


def test_task_message_from_bytes():
    body = json.dumps({
        "task_id": "t1", "execution_id": "e1", "task_name": "send_email",
        "payload": {"to": "a@b.com"}, "attempt": 2, "max_retries": 5, "priority": 7,
        "enqueued_at": "2026-01-01T00:00:00Z",
    }).encode()
    m = messages.TaskMessage.from_bytes(body)
    assert m.task_id == "t1" and m.execution_id == "e1" and m.task_name == "send_email"
    assert m.payload == {"to": "a@b.com"} and m.attempt == 2 and m.max_retries == 5 and m.priority == 7


def test_task_message_defaults_missing_payload():
    body = json.dumps({"task_id": "t", "execution_id": "e", "task_name": "x"}).encode()
    m = messages.TaskMessage.from_bytes(body)
    assert m.payload == {} and m.attempt == 0


def test_result_message_success():
    b = messages.result_message(execution_id="e1", task_id="t1", task_name="send_email",
                                worker_id="w1", attempt=1, success=True, result={"ok": True})
    d = json.loads(b)
    assert d["type"] == "result" and d["status"] == "succeeded"
    assert d["execution_id"] == "e1" and d["result"] == {"ok": True}
    assert "error" not in d  # omitted when empty
    assert "finished_at" in d


def test_result_message_failure():
    b = messages.result_message(execution_id="e2", task_id="t2", task_name="x",
                                worker_id="w1", attempt=3, success=False, error="boom")
    d = json.loads(b)
    assert d["status"] == "failed" and d["error"] == "boom"
    assert "result" not in d  # omitted when None


def test_result_message_non_serializable_result_coerced():
    class Weird:
        def __str__(self):
            return "weird"
    b = messages.result_message(execution_id="e", task_id="t", task_name="x",
                                worker_id="w", attempt=1, success=True, result=Weird())
    d = json.loads(b)
    assert d["result"] == {"value": "weird"}


def test_heartbeat_message():
    b = messages.heartbeat_message(worker_id="w1", hostname="h", pid=9,
                                   queues=["tasks.default"], in_flight=[("e1", "t1"), ("e2", "t2")])
    d = json.loads(b)
    assert d["type"] == "heartbeat" and d["worker_id"] == "w1" and d["pid"] == 9
    assert d["in_flight"] == [{"execution_id": "e1", "task_id": "t1"},
                              {"execution_id": "e2", "task_id": "t2"}]
