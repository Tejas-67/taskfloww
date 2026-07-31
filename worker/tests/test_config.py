"""Tests for the plug-and-play worker config loader."""
from __future__ import annotations

import textwrap

import pytest

from taskfloww_worker.config import ConfigError, load_config, redact_uri

VALID = textwrap.dedent(
    """
    app:
      name: taskfloww
      environment: development
    logging:
      level: info
      format: json
    broker:
      uri: ${TEST_BROKER_URI:-amqp://guest:guest@localhost:5672/}
    queues:
      default_exchange: taskfloww.direct
      dead_letter_exchange: taskfloww.dlx
      definitions:
        - name: tasks.default
          routing_key: priority.default
          max_priority: 10
      dead_letter:
        name: tasks.dlq
        routing_key: dead
    # orchestrator-only sections should be ignored by the worker loader:
    database:
      uri: postgres://u:p@localhost:5432/db
    scheduler:
      dispatch_batch_size: 100
    tasks:
      - name: send_email
        handler: myapp.tasks:send_email
        queue: tasks.default
        max_retries: 3
    """
)


def _write(tmp_path, body: str) -> str:
    p = tmp_path / "config.yaml"
    p.write_text(body)
    return str(p)


def test_loads_and_fills_defaults(tmp_path):
    cfg = load_config(_write(tmp_path, VALID), environ={})
    assert cfg.broker.prefetch == 32  # default
    assert cfg.heartbeat.lease_ttl_seconds == 60  # default
    assert cfg.retry.backoff.strategy == "exponential"  # default
    assert cfg.queues.definitions[0].name == "tasks.default"
    # orchestrator-only sections are ignored, not errors
    assert not hasattr(cfg, "database")


def test_interpolation_default_and_override(tmp_path):
    path = _write(tmp_path, VALID)
    # unset -> default from ${:-...}
    cfg = load_config(path, environ={})
    assert cfg.broker.uri == "amqp://guest:guest@localhost:5672/"
    # set -> uses env value
    cfg2 = load_config(path, environ={"TEST_BROKER_URI": "amqp://real:pw@broker:5672/"})
    assert cfg2.broker.uri == "amqp://real:pw@broker:5672/"


def test_env_override(tmp_path):
    path = _write(tmp_path, VALID)
    cfg = load_config(path, environ={"TASKFLOWW_BROKER__PREFETCH": "64", "TASKFLOWW_LOGGING__LEVEL": "debug"})
    assert cfg.broker.prefetch == 64  # str coerced to int
    assert cfg.logging.level == "debug"


def test_handler_map_and_effective_retries(tmp_path):
    cfg = load_config(_write(tmp_path, VALID), environ={})
    assert cfg.handler_map() == {"send_email": "myapp.tasks:send_email"}
    t = cfg.task_by_name("send_email")
    assert t is not None and t.effective_max_retries(cfg.retry.max_retries) == 3


def test_missing_file():
    with pytest.raises(ConfigError, match="not found"):
        load_config("/no/such/config.yaml", environ={})


def test_validation_aggregates_problems(tmp_path):
    bad = textwrap.dedent(
        """
        logging:
          level: verbose
        broker:
          uri: amqp://localhost
        queues:
          default_exchange: x
          dead_letter_exchange: y
          definitions:
            - name: q1
              routing_key: rk
          dead_letter:
            name: dlq
        heartbeat:
          interval_seconds: 30
          lease_ttl_seconds: 10
          reaper_interval_seconds: 15
        tasks:
          - name: t1
            handler: not-a-valid-handler
            queue: missing.queue
        """
    )
    with pytest.raises(ConfigError) as ei:
        load_config(_write(tmp_path, bad), environ={})
    msg = str(ei.value)
    # several independent field/sub-model problems reported at once
    assert "logging.level" in msg
    assert "lease_ttl_seconds" in msg
    assert "handler" in msg


def test_queue_reference_must_exist(tmp_path):
    # otherwise-valid config, but the task points at an undefined queue
    bad = textwrap.dedent(
        """
        broker:
          uri: amqp://guest:guest@localhost:5672/
        queues:
          default_exchange: taskfloww.direct
          dead_letter_exchange: taskfloww.dlx
          definitions:
            - name: tasks.default
              routing_key: priority.default
          dead_letter:
            name: tasks.dlq
        tasks:
          - name: send_email
            handler: myapp.tasks:send_email
            queue: nope.queue
        """
    )
    with pytest.raises(ConfigError, match="does not match any"):
        load_config(_write(tmp_path, bad), environ={})


def test_redact_uri():
    assert redact_uri("amqp://guest:secret@host:5672/") == "amqp://guest:****@host:5672/"
    assert redact_uri("amqp://host:5672/") == "amqp://host:5672/"  # no creds
    assert redact_uri("") == ""
