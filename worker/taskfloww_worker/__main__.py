"""Entrypoint for the TaskFloww worker.

Loads the plug-and-play config, imports+registers the task handlers it maps,
and runs the consume loop. Run from a directory where your handler modules are
importable (the CWD is added to sys.path).

    python -m taskfloww_worker -c config.yaml
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from taskfloww_worker import __version__
from taskfloww_worker.config import Config, ConfigError, load_config, redact_uri
from taskfloww_worker.registry import Registry
from taskfloww_worker.worker import Worker


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
        "-c", "--config",
        default=os.environ.get("TASKFLOWW_CONFIG", "config/config.example.yaml"),
        help="path to the TaskFloww config file (or set TASKFLOWW_CONFIG)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    boot = configure_logging()
    try:
        cfg: Config = load_config(args.config)
    except ConfigError as e:
        boot.error("failed to load configuration from %s:\n%s", args.config, e)
        return 1

    log = configure_logging(cfg.logging.level, cfg.logging.format)

    # Make user handler modules importable relative to the working directory.
    sys.path.insert(0, os.getcwd())
    registry = Registry()
    try:
        registry.load_from_config(cfg)
    except Exception as e:
        log.error("failed to import task handlers: %s", e)
        return 1

    log.info(
        "taskfloww worker starting version=%s broker=%s tasks=%s",
        __version__, redact_uri(cfg.broker.uri), registry.names(),
    )

    worker = Worker(cfg, registry, worker_id=os.environ.get("TASKFLOWW_WORKER_ID"))

    def _handle_signal(signum, _frame):
        log.info("received signal %s, shutting down", signum)
        worker.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        worker.run()
    except Exception as e:
        log.error("worker crashed: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
