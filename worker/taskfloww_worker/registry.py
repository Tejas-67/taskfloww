"""Task registry — the plug-and-play core on the worker side.

A user writes a plain function ``def handler(payload: dict) -> Any`` and maps it
to a task name in the config (``handler: "module:function"``). The worker
imports and registers each mapping at startup. The ``@task`` decorator is
optional sugar.
"""
from __future__ import annotations

import importlib
from typing import Any, Callable, Optional

Handler = Callable[[dict], Any]


class Registry:
    """Maps task names to handler callables."""

    def __init__(self) -> None:
        self._fns: dict[str, Handler] = {}

    def register(self, name: str, fn: Handler) -> None:
        if not callable(fn):
            raise TypeError(f"handler for {name!r} is not callable")
        self._fns[name] = fn

    def task(self, name: str) -> Callable[[Handler], Handler]:
        """Decorator: register the decorated function under ``name``."""

        def decorator(fn: Handler) -> Handler:
            self.register(name, fn)
            return fn

        return decorator

    def get(self, name: str) -> Optional[Handler]:
        return self._fns.get(name)

    def names(self) -> list[str]:
        return sorted(self._fns)

    def load_from_config(self, config) -> None:
        """Import and register every ``name -> "module:function"`` mapping."""
        for name, path in config.handler_map().items():
            mod_name, sep, fn_name = path.partition(":")
            if not sep or not mod_name or not fn_name:
                raise ValueError(f'task {name!r} handler {path!r} must be "module:function"')
            module = importlib.import_module(mod_name)
            try:
                fn = getattr(module, fn_name)
            except AttributeError as e:
                raise ValueError(f"handler {path!r} not found: {e}") from e
            self.register(name, fn)


# Convenience global registry for the @task decorator.
default_registry = Registry()


def task(name: str) -> Callable[[Handler], Handler]:
    return default_registry.task(name)
