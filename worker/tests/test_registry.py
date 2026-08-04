import pytest

from taskfloww_worker.registry import Registry, default_registry, task


def test_register_and_get():
    r = Registry()
    r.register("a", lambda p: p)
    assert r.get("a") is not None
    assert r.get("missing") is None
    assert r.names() == ["a"]


def test_decorator():
    r = Registry()

    @r.task("greet")
    def greet(payload):
        return "hi"

    assert r.get("greet") is greet


def test_register_non_callable():
    r = Registry()
    with pytest.raises(TypeError):
        r.register("x", 123)


class _FakeConfig:
    """Minimal object exposing handler_map() like config.Config."""

    def __init__(self, mapping):
        self._m = mapping

    def handler_map(self):
        return self._m


def test_load_from_config_imports_handlers():
    r = Registry()
    r.load_from_config(_FakeConfig({"send_email": "examples.tasks:send_email"}))
    fn = r.get("send_email")
    assert fn is not None
    assert fn({"to": "x@y.com"}) == {"sent": True, "to": "x@y.com"}


def test_load_from_config_bad_path():
    r = Registry()
    with pytest.raises(ValueError):
        r.load_from_config(_FakeConfig({"t": "no_colon_here"}))


def test_load_from_config_missing_attr():
    r = Registry()
    with pytest.raises(ValueError):
        r.load_from_config(_FakeConfig({"t": "examples.tasks:does_not_exist"}))


def test_global_task_decorator():
    @task("global_one")
    def _h(payload):
        return 1

    assert default_registry.get("global_one") is _h
