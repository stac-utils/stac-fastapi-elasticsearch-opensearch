"""Unit tests for resolving ITEMS_INDEX_STRATEGY and the deprecated flag."""

import pytest

from stac_fastapi.core.utilities import items_index_strategy


@pytest.fixture
def env(monkeypatch):
    monkeypatch.delenv("ITEMS_INDEX_STRATEGY", raising=False)
    monkeypatch.delenv("ENABLE_DATETIME_INDEX_FILTERING", raising=False)

    def set_env(strategy=None, legacy=None):
        if strategy is not None:
            monkeypatch.setenv("ITEMS_INDEX_STRATEGY", strategy)
        if legacy is not None:
            monkeypatch.setenv("ENABLE_DATETIME_INDEX_FILTERING", legacy)

    return set_env


@pytest.mark.parametrize(
    "strategy, legacy, expected",
    [
        (None, None, "simple"),
        ("", None, "simple"),
        ("  ", "true", "datetime"),
        (None, "true", "datetime"),
        (None, "false", "simple"),
        (None, "bogus", "simple"),
        ("simple", None, "simple"),
        (" Datetime ", None, "datetime"),
        ("datetime", "true", "datetime"),
        ("simple", "false", "simple"),
        ("simple", "", "simple"),
        ("datetime", "bogus", "datetime"),
    ],
)
def test_resolves_strategy(env, strategy, legacy, expected):
    env(strategy, legacy)
    assert items_index_strategy() == expected


def test_rejects_unknown_strategy(env):
    env("monthly")
    with pytest.raises(ValueError, match="simple, datetime"):
        items_index_strategy()


@pytest.mark.parametrize(
    "strategy, legacy", [("simple", "true"), ("datetime", "false"), ("datetime", "0")]
)
def test_rejects_legacy_flag_that_disagrees(env, strategy, legacy):
    env(strategy, legacy)
    with pytest.raises(ValueError, match="ENABLE_DATETIME_INDEX_FILTERING"):
        items_index_strategy()
