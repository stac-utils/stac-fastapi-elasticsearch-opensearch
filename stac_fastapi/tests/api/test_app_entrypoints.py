import importlib
import os
import runpy
from unittest.mock import Mock

import pytest
from fastapi import FastAPI


@pytest.mark.parametrize("backend", ["elasticsearch", "opensearch"])
@pytest.mark.parametrize("invoke_as_module", [False, True])
@pytest.mark.parametrize(
    "dotenv, environment, expected",
    [
        (None, {}, ("0.0.0.0", 8000, True)),
        ("", {}, ("0.0.0.0", 8000, True)),
        (
            "APP_HOST=127.0.0.2\nAPP_PORT=8766\nRELOAD=false\n",
            {},
            ("127.0.0.2", 8766, False),
        ),
        (
            None,
            {"APP_HOST": "127.0.0.1", "APP_PORT": "8765", "RELOAD": "false"},
            ("127.0.0.1", 8765, False),
        ),
        (
            "APP_HOST=127.0.0.2\nAPP_PORT=8766\nRELOAD=false\n",
            {"APP_HOST": "127.0.0.1", "APP_PORT": "8765", "RELOAD": "true"},
            ("127.0.0.1", 8765, True),
        ),
        (
            "APP_HOST=127.0.0.2\nAPP_PORT=8766\nRELOAD=false\n",
            {"APP_PORT": "8765"},
            ("127.0.0.2", 8765, False),
        ),
        ("APP_PORT=8766\n", {}, ("0.0.0.0", 8766, True)),
        ("APP_PORT=invalid\nRELOAD=invalid\n", {}, ("0.0.0.0", 8000, True)),
        (
            None,
            {"APP_PORT": "invalid", "RELOAD": "invalid"},
            ("0.0.0.0", 8000, True),
        ),
        (
            "APP_PORT=8766\nRELOAD=false\n",
            {"APP_PORT": "invalid", "RELOAD": "invalid"},
            ("0.0.0.0", 8000, True),
        ),
        (
            "APP_PORT=invalid\nRELOAD=invalid\n",
            {"APP_PORT": "8765", "RELOAD": "false"},
            ("0.0.0.0", 8765, False),
        ),
        (
            "APP_PORT=8766\nRELOAD=false\n",
            {"APP_PORT": "", "RELOAD": ""},
            ("0.0.0.0", 8000, True),
        ),
        (None, {"APP_HOST": ""}, ("0.0.0.0", 8000, True)),
        (None, {"APP_PORT": "0"}, ("0.0.0.0", 8000, True)),
        (None, {"APP_PORT": "65536"}, ("0.0.0.0", 8000, True)),
        *[
            (f"RELOAD={value}\n", {}, ("0.0.0.0", 8000, expected))
            for value, expected in [
                ("TRUE", True),
                ("1", True),
                ("yes", True),
                ("Y", True),
                ("FALSE", False),
                ("0", False),
                ("no", False),
                ("N", False),
            ]
        ],
    ],
)
@pytest.mark.filterwarnings("ignore:.*found in sys.modules.*:RuntimeWarning")
def test_backend_entrypoint_settings(
    monkeypatch, tmp_path, backend, invoke_as_module, dotenv, environment, expected
):
    """Launchers preserve dotenv settings and allow process overrides without mutation."""
    monkeypatch.chdir(tmp_path)
    if dotenv is not None:
        (tmp_path / ".env").write_text(dotenv)
    for key in ("APP_HOST", "APP_PORT", "RELOAD"):
        monkeypatch.delenv(key, raising=False)
    for key, value in environment.items():
        monkeypatch.setenv(key, value)

    name = f"stac_fastapi.{backend}.app"
    module = pytest.importorskip(name)
    uvicorn = pytest.importorskip("uvicorn")
    run = Mock()
    monkeypatch.setattr(uvicorn, "run", run)
    original_environment = dict(os.environ)

    if invoke_as_module:
        runpy.run_module(name, run_name="__main__")
    else:
        module.run()

    host, port, reload = expected
    run.assert_called_once_with(
        f"{name}:create_app",
        factory=True,
        host=host,
        port=port,
        log_level="info",
        reload=reload,
    )
    assert dict(os.environ) == original_environment


@pytest.mark.parametrize("backend", ["elasticsearch", "opensearch"])
def test_backend_uvicorn_factory_returns_app(backend):
    """The factory referenced by both Uvicorn launch paths remains callable."""
    pytest.importorskip(f"stac_fastapi.{backend}.app")
    module = importlib.import_module(f"stac_fastapi.{backend}.app")

    assert isinstance(module.create_app(), FastAPI)
