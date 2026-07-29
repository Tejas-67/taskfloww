"""Entrypoint for the TaskFloww worker.

Phase 0 scaffold: configures structured (JSON) logging and prints a startup
banner. The RabbitMQ consume loop, heartbeat/result publishers, and task
registry are implemented in Phase 4 (see ../docs/ROADMAP.md).

Run with:  python -m taskfloww_worker   (or the `taskfloww-worker` console script)
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from taskfloww_worker import __version__


class JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter.

    Replaced by structlog in Phase 5; kept dependency-free here so the scaffold
    runs on a bare interpreter.
    """

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


def configure_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    return logging.getLogger("taskfloww.worker")


def main() -> int:
    log = configure_logging()
    log.info("taskfloww worker starting (phase 0 scaffold) version=%s", __version__)
    log.info("no tasks registered yet — consume loop lands in phase 4")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
