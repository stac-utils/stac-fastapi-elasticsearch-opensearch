import importlib
import sys
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize(
    "module_name,settings_class_name",
    [
        ("stac_fastapi.elasticsearch.app", "ElasticsearchSettings"),
        ("stac_fastapi.opensearch.app", "OpensearchSettings"),
    ],
)
def test_run_uses_env_overrides(monkeypatch, module_name, settings_class_name):
    module = importlib.import_module(module_name)

    class DummySettings:
        app_host = "settings-host"
        app_port = "7001"
        reload = True

    captured: dict[str, object] = {}

    def fake_run(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=fake_run))
    monkeypatch.setattr(module, settings_class_name, DummySettings)
    monkeypatch.setenv("APP_HOST", "  env-host  ")
    monkeypatch.setenv("APP_PORT", "9205")
    monkeypatch.setenv("RELOAD", "0")

    module.run()

    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["host"] == "env-host"
    assert kwargs["port"] == 9205
    assert kwargs["reload"] is False


@pytest.mark.parametrize(
    "module_name,settings_class_name",
    [
        ("stac_fastapi.elasticsearch.app", "ElasticsearchSettings"),
        ("stac_fastapi.opensearch.app", "OpensearchSettings"),
    ],
)
def test_run_falls_back_to_safe_defaults(monkeypatch, module_name, settings_class_name):
    module = importlib.import_module(module_name)

    class DummySettings:
        app_host = ""
        app_port = "not-a-number"
        reload = "invalid"

    captured: dict[str, object] = {}

    def fake_run(*args, **kwargs):
        captured["kwargs"] = kwargs

    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=fake_run))
    monkeypatch.setattr(module, settings_class_name, DummySettings)
    monkeypatch.delenv("APP_HOST", raising=False)
    monkeypatch.delenv("APP_PORT", raising=False)
    monkeypatch.delenv("RELOAD", raising=False)

    module.run()

    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["host"] == "0.0.0.0"
    assert kwargs["port"] == 8000
    assert kwargs["reload"] is True
