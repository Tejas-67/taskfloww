"""Plug-and-play configuration for the TaskFloww worker.

One YAML file drives both the Go orchestrator and the Python workers. This
module loads the worker-relevant sections, validates them with pydantic (so
mistakes fail fast with clear messages), and supports:

* ``${VAR:-default}`` interpolation inside YAML values, and
* environment overrides via ``TASKFLOWW_`` (nested keys joined by ``__``,
  e.g. ``TASKFLOWW_BROKER__PREFETCH=64`` overrides ``broker.prefetch``).

Orchestrator-only sections (``database``, ``server``, ``scheduler``) present in
the shared file are ignored here (``extra="ignore"``).

Design note: we use a plain pydantic ``BaseModel`` plus an explicit env overlay
rather than ``pydantic-settings``. The primary source is a YAML file with
shell-style interpolation; an explicit overlay makes the file→env precedence
obvious and keeps validation errors clean.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

ENV_PREFIX = "TASKFLOWW_"
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

_LOG_LEVELS = {"debug", "info", "warn", "error"}
_LOG_FORMATS = {"json", "text"}
_BACKOFF_KINDS = {"exponential", "fixed"}


class ConfigError(ValueError):
    """Raised when configuration is missing or invalid (fails fast)."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore")


class App(_Model):
    name: str = "taskfloww"
    environment: str = "development"


class Logging(_Model):
    level: str = "info"
    format: str = "json"

    @field_validator("level")
    @classmethod
    def _level(cls, v: str) -> str:
        if v not in _LOG_LEVELS:
            raise ValueError(f"must be one of {sorted(_LOG_LEVELS)}")
        return v

    @field_validator("format")
    @classmethod
    def _format(cls, v: str) -> str:
        if v not in _LOG_FORMATS:
            raise ValueError(f"must be one of {sorted(_LOG_FORMATS)}")
        return v


class Metrics(_Model):
    enabled: bool = True
    path: str = "/metrics"
    worker_port: int = 9100


class Broker(_Model):
    uri: str
    prefetch: int = 32
    connection_name: str = "taskfloww"

    @field_validator("prefetch")
    @classmethod
    def _prefetch(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be > 0")
        return v


class QueueDef(_Model):
    name: str
    routing_key: str
    max_priority: int = 10


class DeadLetter(_Model):
    name: str
    routing_key: str = "dead"


class Queues(_Model):
    default_exchange: str = "taskfloww.direct"
    dead_letter_exchange: str = "taskfloww.dlx"
    definitions: list[QueueDef] = Field(default_factory=list)
    dead_letter: DeadLetter

    @model_validator(mode="after")
    def _check(self) -> "Queues":
        if not self.definitions:
            raise ValueError("must contain at least one queue definition")
        names = [d.name for d in self.definitions]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ValueError(f"duplicate queue names: {dupes}")
        return self


class Backoff(_Model):
    strategy: str = "exponential"
    base_seconds: float = 2.0
    multiplier: float = 2.0
    max_seconds: float = 300.0
    jitter: bool = True

    @model_validator(mode="after")
    def _check(self) -> "Backoff":
        if self.strategy not in _BACKOFF_KINDS:
            raise ValueError(f"strategy must be one of {sorted(_BACKOFF_KINDS)}")
        if self.base_seconds <= 0:
            raise ValueError("base_seconds must be > 0")
        if self.strategy == "exponential" and self.multiplier < 1:
            raise ValueError("multiplier must be >= 1 for exponential")
        if self.max_seconds < self.base_seconds:
            raise ValueError("max_seconds must be >= base_seconds")
        return self


class Retry(_Model):
    max_retries: int = 5
    backoff: Backoff = Field(default_factory=Backoff)

    @field_validator("max_retries")
    @classmethod
    def _max_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError("must be >= 0")
        return v


class Heartbeat(_Model):
    interval_seconds: int = 10
    lease_ttl_seconds: int = 60
    reaper_interval_seconds: int = 15

    @model_validator(mode="after")
    def _invariants(self) -> "Heartbeat":
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be > 0")
        if self.lease_ttl_seconds <= self.interval_seconds:
            raise ValueError("lease_ttl_seconds must be > interval_seconds to avoid false re-queues")
        if self.reaper_interval_seconds <= 0:
            raise ValueError("reaper_interval_seconds must be > 0")
        if self.reaper_interval_seconds > self.lease_ttl_seconds:
            raise ValueError("reaper_interval_seconds must be <= lease_ttl_seconds")
        return self


class TaskMapping(_Model):
    name: str
    handler: str
    queue: Optional[str] = None
    max_retries: Optional[int] = None

    @field_validator("handler")
    @classmethod
    def _handler(cls, v: str) -> str:
        mod, sep, fn = v.partition(":")
        if not sep or not mod or not fn:
            raise ValueError(f'{v!r} must be "module:function"')
        return v

    @field_validator("max_retries")
    @classmethod
    def _max_retries(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError("must be >= 0")
        return v

    def effective_max_retries(self, default: int) -> int:
        """Return this task's max retries, inheriting ``default`` when unset."""
        return default if self.max_retries is None else self.max_retries


class Config(_Model):
    app: App = Field(default_factory=App)
    logging: Logging = Field(default_factory=Logging)
    metrics: Metrics = Field(default_factory=Metrics)
    broker: Broker
    queues: Queues
    retry: Retry = Field(default_factory=Retry)
    heartbeat: Heartbeat = Field(default_factory=Heartbeat)
    tasks: list[TaskMapping] = Field(default_factory=list)

    @model_validator(mode="after")
    def _cross_checks(self) -> "Config":
        queue_names = {d.name for d in self.queues.definitions}
        seen: set[str] = set()
        for t in self.tasks:
            if t.name in seen:
                raise ValueError(f"duplicate task name {t.name!r}")
            seen.add(t.name)
            if t.queue and t.queue not in queue_names:
                raise ValueError(
                    f"tasks[{t.name!r}].queue {t.queue!r} does not match any queues.definitions[].name"
                )
        return self

    def task_by_name(self, name: str) -> Optional[TaskMapping]:
        return next((t for t in self.tasks if t.name == name), None)

    def handler_map(self) -> dict[str, str]:
        """task-name → handler-path, for building the worker registry."""
        return {t.name: t.handler for t in self.tasks}


# --- loading ---------------------------------------------------------------

def _expand_env(text: str, environ: Mapping[str, str]) -> str:
    """Expand ${VAR} / ${VAR:-default} using ``environ``."""

    def repl(m: re.Match[str]) -> str:
        name, default = m.group(1), m.group(2)
        value = environ.get(name)
        if value:
            return value
        return default if default is not None else ""

    return _ENV_REF.sub(repl, text)


def _apply_env_overrides(data: dict[str, Any], environ: Mapping[str, str]) -> dict[str, Any]:
    """Overlay TASKFLOWW_ env vars onto the parsed dict (env wins over file)."""
    for key, val in environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = key[len(ENV_PREFIX):].lower().split("__")
        if not path or path == [""]:
            continue
        node: dict[str, Any] = data
        for part in path[:-1]:
            nxt = node.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                node[part] = nxt
            node = nxt
        node[path[-1]] = val
    return data


def _format_errors(path: str, exc: ValidationError) -> str:
    lines = [f"invalid configuration in {path} ({exc.error_count()} problem(s)):"]
    for err in exc.errors():
        loc = ".".join(str(x) for x in err["loc"]) or "(root)"
        lines.append(f"  - {loc}: {err['msg']}")
    return "\n".join(lines)


def load_config(path: str | os.PathLike[str], environ: Optional[Mapping[str, str]] = None) -> Config:
    """Load, interpolate, env-override, and validate the config at ``path``.

    Raises :class:`ConfigError` (with a readable, aggregated message) on any
    problem.
    """
    environ = os.environ if environ is None else environ
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"config file not found: {path}")

    text = _expand_env(p.read_text(), environ)
    try:
        raw = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:  # pragma: no cover - passthrough
        raise ConfigError(f"invalid YAML in {path}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"config root must be a mapping in {path}")

    merged = _apply_env_overrides(raw, environ)
    try:
        return Config.model_validate(merged)
    except ValidationError as e:
        raise ConfigError(_format_errors(str(path), e)) from e


def redact_uri(raw: str) -> str:
    """Mask the password in a URI-style DSN for safe logging."""
    if not raw:
        return raw
    sep = raw.find("://")
    if sep < 0:
        return raw
    rest = raw[sep + 3:]
    at = rest.find("@")
    if at < 0:
        return raw
    userinfo = rest[:at]
    colon = userinfo.find(":")
    if colon < 0:
        return raw
    return raw[: sep + 3] + userinfo[:colon] + ":****" + rest[at:]
