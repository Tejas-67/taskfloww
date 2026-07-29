"""TaskFloww — thin, plug-and-play Python worker SDK.

Phase 0 scaffold. Later phases add the task registry (decorator + config-path
mapping), the RabbitMQ consume loop with prefetch backpressure, periodic
heartbeat and result publishers, an idempotency guard, and structured logging
(see ../docs/ROADMAP.md).

Workers talk **only** to RabbitMQ — never directly to PostgreSQL (ADR-0002 / B1).
"""

__version__ = "0.0.0"
__all__ = ["__version__"]
