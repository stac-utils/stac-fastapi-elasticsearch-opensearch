"""Unit tests for resolving ITEMS_INDEX_STRATEGY and the deprecated flag."""

import logging

import pytest

from stac_fastapi.core.utilities import (
    _warn_legacy_index_flag,
    items_index_is_partitioned,
    items_index_strategy,
)


@pytest.fixture
def env(monkeypatch):
    _warn_legacy_index_flag.cache_clear()
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


@pytest.mark.parametrize("strategy, expected", [("simple", False), ("datetime", True)])
def test_partitioned_only_for_datetime(env, strategy, expected):
    env(strategy)
    assert items_index_is_partitioned() is expected


def legacy_warnings(caplog):
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "ENABLE_DATETIME_INDEX_FILTERING" in r.getMessage()
    ]


@pytest.mark.parametrize("strategy", [None, "datetime"])
def test_warns_once_when_legacy_flag_is_set(env, caplog, strategy):
    env(strategy, "true")
    for _ in range(3):
        items_index_strategy()
    warnings = legacy_warnings(caplog)
    assert len(warnings) == 1
    assert "deprecated" in warnings[0]
    assert "not a boolean" not in warnings[0]


@pytest.mark.parametrize("strategy", [None, "simple"])
def test_warns_once_about_unrecognized_legacy_value(env, caplog, strategy):
    env(strategy, "typo")
    for _ in range(3):
        items_index_strategy()
    warnings = legacy_warnings(caplog)
    assert len(warnings) == 1
    assert "typo" in warnings[0] and "not a boolean" in warnings[0]


@pytest.mark.parametrize("legacy", [None, ""])
def test_no_warning_without_legacy_flag(env, caplog, legacy):
    env("simple", legacy)
    items_index_strategy()
    assert legacy_warnings(caplog) == []
