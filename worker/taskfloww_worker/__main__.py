"""Entrypoint for the TaskFloww worker.

Phase 2: loads the plug-and-play config (path via -c/--config or
TASKFLOWW_CONFIG), configures structured JSON logging from it, and logs a
startup summary of the task→function mappings it would serve. Invalid config
fails fast. The RabbitMQ consume loop, heartbeat/result publishers, and task
registry land in Phase 4 (see ../docs/ROADMAP.md).

Run with:  python -m taskfloww_worker [-c config.yaml]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

from taskfloww_worker import __version__
from taskfloww_worker.config import Config, ConfigError, load_config, redact_uri


class JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter (replaced by structlog in Phase 5)."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}


def configure_logging(level: str = "info", fmt: str = "json") -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "text":
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    else:
        handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(_LEVELS.get(level, logging.INFO))
    return logging.getLogger("taskfloww.worker")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="taskfloww-worker")
    parser.add_argument(
        "-c",
        "--config",
        default=os.environ.get("TASKFLOWW_CONFIG", "config/config.example.yaml"),
        help="path to the TaskFloww config file (or set TASKFLOWW_CONFIG)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # Bootstrap logger for pre-config errors.
    boot = configure_logging()
    try:
        cfg: Config = load_config(args.config)
    except ConfigError as e:
        boot.error("failed to load configuration from %s:\n%s", args.config, e)
        return 1

    log = configure_logging(cfg.logging.level, cfg.logging.format)
    log.info(
        "taskfloww worker starting version=%s environment=%s broker=%s prefetch=%d "
        "tasks=%d lease_ttl=%ds heartbeat=%ds",
        __version__,
        cfg.app.environment,
        redact_uri(cfg.broker.uri),
        cfg.broker.prefetch,
        len(cfg.tasks),
        cfg.heartbeat.lease_ttl_seconds,
        cfg.heartbeat.interval_seconds,
    )
    for name, handler in cfg.handler_map().items():
        log.info("registered task mapping: %s -> %s", name, handler)
    log.info("consume loop lands in phase 4")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
