"""Wire contract between the orchestrator and workers.

Mirrors ``orchestrator/internal/message`` (Task inbound; Result/Heartbeat
outbound). Keep the JSON field names in sync with the Go structs.
"""
from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from typing import Any, Iterable

TYPE_RESULT = "result"
TYPE_HEARTBEAT = "heartbeat"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@dataclass
class TaskMessage:
    """A unit of work delivered to the worker (mirrors message.Task)."""

    task_id: str
    execution_id: str
    task_name: str
    payload: dict
    attempt: int
    max_retries: int
    priority: int
    enqueued_at: str

    @classmethod
    def from_bytes(cls, body: bytes) -> "TaskMessage":
        d = json.loads(body)
        return cls(
            task_id=d["task_id"],
            execution_id=d["execution_id"],
            task_name=d["task_name"],
            payload=d.get("payload") or {},
            attempt=int(d.get("attempt", 0)),
            max_retries=int(d.get("max_retries", 0)),
            priority=int(d.get("priority", 0)),
            enqueued_at=d.get("enqueued_at", ""),
        )


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return {"value": str(value)}


def result_message(
    *,
    execution_id: str,
    task_id: str,
    task_name: str,
    worker_id: str,
    attempt: int,
    success: bool,
    result: Any = None,
    error: str = "",
) -> bytes:
    """Build a Result control message."""
    body: dict[str, Any] = {
        "type": TYPE_RESULT,
        "execution_id": execution_id,
        "task_id": task_id,
        "task_name": task_name,
        "worker_id": worker_id,
        "attempt": attempt,
        "status": STATUS_SUCCEEDED if success else STATUS_FAILED,
        "finished_at": _now_iso(),
    }
    if result is not None:
        body["result"] = _json_safe(result)
    if error:
        body["error"] = error
    return json.dumps(body).encode()


def heartbeat_message(
    *,
    worker_id: str,
    hostname: str,
    pid: int,
    queues: list[str],
    in_flight: Iterable[tuple[str, str]],
) -> bytes:
    """Build a Heartbeat control message. in_flight is (execution_id, task_id) pairs."""
    body = {
        "type": TYPE_HEARTBEAT,
        "worker_id": worker_id,
        "hostname": hostname,
        "pid": pid,
        "queues": queues,
        "in_flight": [{"execution_id": e, "task_id": t} for e, t in in_flight],
        "timestamp": _now_iso(),
    }
    return json.dumps(body).encode()
