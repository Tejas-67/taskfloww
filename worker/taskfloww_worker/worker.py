"""The TaskFloww worker run loop.

Rabbit-only (ADR-0002/B1): consumes work from the priority queues, runs the
mapped Python function in a thread pool, and publishes a result + periodic
heartbeats to the control exchange. Never touches PostgreSQL.

Concurrency model: pika's IO runs on the main thread; handlers run in a
ThreadPoolExecutor (so a slow task never blocks heartbeats). Result publishes,
acks, and heartbeats are marshalled back onto the IO thread via
``add_callback_threadsafe`` (pika channels are not thread-safe).
"""
from __future__ import annotations

import collections
import concurrent.futures
import functools
import logging
import os
import socket
import threading
from typing import Optional

import pika

from taskfloww_worker import messages
from taskfloww_worker.config import Config
from taskfloww_worker.registry import Registry

log = logging.getLogger("taskfloww.worker")

_PROCESSED_MAX = 10000


class Worker:
    """Consumes tasks and reports results/heartbeats for one worker process."""

    def __init__(self, config: Config, registry: Registry, worker_id: Optional[str] = None) -> None:
        self.cfg = config
        self.registry = registry
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.worker_id = worker_id or f"worker-{self.hostname}-{self.pid}"

        self._conn: Optional[pika.BlockingConnection] = None
        self._ch = None
        self._pool: Optional[concurrent.futures.ThreadPoolExecutor] = None

        self._lock = threading.Lock()
        self._in_flight: dict[str, dict] = {}  # execution_id -> {task_id, delivery_tag}
        self._processed: "collections.OrderedDict[str, bool]" = collections.OrderedDict()
        self._stopping = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        self._queues = self._resolve_queues()

    # -- queue resolution ---------------------------------------------------

    def _resolve_queues(self) -> list[str]:
        """The set of queues to consume: those referenced by configured tasks."""
        default = self.cfg.queues.definitions[0].name if self.cfg.queues.definitions else None
        qs: set[str] = set()
        for t in self.cfg.tasks:
            q = t.queue or default
            if q:
                qs.add(q)
        if not qs and default:
            qs.add(default)
        return sorted(q for q in qs if q)

    # -- lifecycle ----------------------------------------------------------

    def run(self) -> None:
        params = pika.URLParameters(self.cfg.broker.uri)
        params.client_properties = {"connection_name": self.cfg.broker.connection_name}
        self._conn = pika.BlockingConnection(params)
        self._ch = self._conn.channel()
        self._ch.basic_qos(prefetch_count=self.cfg.broker.prefetch)
        # Control exchange is declared by the orchestrator too; declaring is idempotent.
        self._ch.exchange_declare(self.cfg.control.exchange, exchange_type="direct", durable=True)

        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, self.cfg.broker.prefetch))
        for q in self._queues:
            self._ch.basic_consume(queue=q, on_message_callback=self._on_message, auto_ack=False)
        self._start_heartbeat()

        log.info(
            "worker %s started; tasks=%s queues=%s prefetch=%d",
            self.worker_id, self.registry.names(), self._queues, self.cfg.broker.prefetch,
        )
        try:
            self._ch.start_consuming()
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        if self._stopping.is_set():
            return
        self._stopping.set()
        log.info("worker %s stopping", self.worker_id)
        if self._conn is not None and self._conn.is_open:
            try:
                self._conn.add_callback_threadsafe(self._ch.stop_consuming)
            except Exception:  # pragma: no cover - best effort
                pass
        if self._pool is not None:
            self._pool.shutdown(wait=True)
        if self._conn is not None and self._conn.is_open:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover
                pass

    # -- message handling ---------------------------------------------------

    def _on_message(self, ch, method, properties, body) -> None:
        try:
            msg = messages.TaskMessage.from_bytes(body)
        except Exception as e:  # malformed → drop (ack) so it doesn't loop
            log.error("dropping malformed task message: %s", e)
            ch.basic_ack(method.delivery_tag)
            return

        with self._lock:
            if msg.execution_id in self._processed or msg.execution_id in self._in_flight:
                log.info("duplicate delivery execution_id=%s ignored", msg.execution_id)
                ch.basic_ack(method.delivery_tag)
                return
            self._in_flight[msg.execution_id] = {"task_id": msg.task_id, "delivery_tag": method.delivery_tag}

        self._pool.submit(self._execute, msg, method.delivery_tag)

    def _execute(self, msg: messages.TaskMessage, delivery_tag: int) -> None:
        fn = self.registry.get(msg.task_name)
        success, result, error = True, None, ""
        if fn is None:
            success, error = False, f"no handler registered for task {msg.task_name!r}"
        else:
            try:
                result = fn(msg.payload)
            except Exception as e:  # user handler failed
                success, error = False, f"{type(e).__name__}: {e}"
                log.exception("task %s (execution %s) failed", msg.task_name, msg.execution_id)
        # Marshal result publish + ack back onto the IO thread.
        self._conn.add_callback_threadsafe(
            functools.partial(self._finish, msg, delivery_tag, success, result, error)
        )

    def _finish(self, msg, delivery_tag, success, result, error) -> None:
        try:
            body = messages.result_message(
                execution_id=msg.execution_id, task_id=msg.task_id, task_name=msg.task_name,
                worker_id=self.worker_id, attempt=msg.attempt, success=success, result=result, error=error,
            )
            self._ch.basic_publish(
                exchange=self.cfg.control.exchange,
                routing_key=self.cfg.control.result_routing_key,
                body=body,
                properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
            )
            self._ch.basic_ack(delivery_tag)
        except Exception as e:  # pragma: no cover - broker hiccup
            log.error("failed to publish result/ack for %s: %s", msg.execution_id, e)
        finally:
            with self._lock:
                self._in_flight.pop(msg.execution_id, None)
                self._processed[msg.execution_id] = True
                while len(self._processed) > _PROCESSED_MAX:
                    self._processed.popitem(last=False)
        log.info("finished execution=%s task=%s success=%s", msg.execution_id, msg.task_name, success)

    # -- heartbeats ---------------------------------------------------------

    def _start_heartbeat(self) -> None:
        interval = max(1, self.cfg.heartbeat.interval_seconds)

        def loop() -> None:
            while not self._stopping.wait(interval):
                if self._conn is None or not self._conn.is_open:
                    return
                try:
                    self._conn.add_callback_threadsafe(self._publish_heartbeat)
                except Exception:  # pragma: no cover
                    return

        self._hb_thread = threading.Thread(target=loop, name="taskfloww-heartbeat", daemon=True)
        self._hb_thread.start()

    def _publish_heartbeat(self) -> None:
        with self._lock:
            in_flight = [(e, d["task_id"]) for e, d in self._in_flight.items()]
        body = messages.heartbeat_message(
            worker_id=self.worker_id, hostname=self.hostname, pid=self.pid,
            queues=self._queues, in_flight=in_flight,
        )
        try:
            self._ch.basic_publish(
                exchange=self.cfg.control.exchange,
                routing_key=self.cfg.control.heartbeat_routing_key,
                body=body,
                properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
            )
        except Exception as e:  # pragma: no cover
            log.debug("heartbeat publish failed: %s", e)
