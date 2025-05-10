import importlib

import pytest


def get_settings_class():
    """
    Try to import ElasticsearchSettings or OpenSearchSettings, whichever is available.
    Returns a tuple: (settings_class, config_module)
    """
    try:
        config = importlib.import_module("stac_fastapi.elasticsearch.config")
        importlib.reload(config)
        return config.ElasticsearchSettings, config
    except ModuleNotFoundError:
        try:
            config = importlib.import_module("stac_fastapi.opensearch.config")
            importlib.reload(config)
            return config.OpensearchSettings, config
        except ModuleNotFoundError:
            pytest.skip(
                "Neither Elasticsearch nor OpenSearch config module is available."
            )


def test_enable_direct_response_true(monkeypatch):
    """Test that ENABLE_DIRECT_RESPONSE env var enables direct response config."""
    monkeypatch.setenv("ENABLE_DIRECT_RESPONSE", "true")
    settings_class, _ = get_settings_class()
    settings = settings_class()
    assert settings.enable_direct_response is True


def test_enable_direct_response_false(monkeypatch):
    """Test that ENABLE_DIRECT_RESPONSE env var disables direct response config."""
    monkeypatch.setenv("ENABLE_DIRECT_RESPONSE", "false")
    settings_class, _ = get_settings_class()
    settings = settings_class()
    assert settings.enable_direct_response is False


def test_database_refresh_true(monkeypatch):
    """Test that DATABASE_REFRESH env var enables database refresh."""
    monkeypatch.setenv("DATABASE_REFRESH", "true")
    settings_class, _ = get_settings_class()
    settings = settings_class()
    assert settings.database_refresh == "true"


def test_database_refresh_false(monkeypatch):
    """Test that DATABASE_REFRESH env var disables database refresh."""
    monkeypatch.setenv("DATABASE_REFRESH", "false")
    settings_class, _ = get_settings_class()
    settings = settings_class()
    assert settings.database_refresh == "false"


def test_database_refresh_wait_for(monkeypatch):
    """Test that DATABASE_REFRESH env var sets database refresh to 'wait_for'."""
    monkeypatch.setenv("DATABASE_REFRESH", "wait_for")
    settings_class, _ = get_settings_class()
    settings = settings_class()
    assert settings.database_refresh == "wait_for"
