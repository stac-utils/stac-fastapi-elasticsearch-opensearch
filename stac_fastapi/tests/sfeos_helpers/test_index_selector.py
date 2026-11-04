"""Unit tests for datetime-based index selection filters."""

import pytest

from stac_fastapi.sfeos_helpers.search_engine.selection.selectors import (
    DatetimeBasedIndexSelector,
)

RANGE = "2020-02-10T00:00:00Z/2020-02-12T00:00:00Z"


@pytest.fixture
def selector(monkeypatch):
    monkeypatch.setenv("USE_DATETIME", "false")
    # Bypass the process-wide singleton: parse_datetime_filters needs no client.
    return object.__new__(DatetimeBasedIndexSelector)


@pytest.mark.parametrize(
    "datetime_search",
    [RANGE, {"gte": "2020-02-10T00:00:00Z", "lte": "2020-02-12T00:00:00Z"}],
)
def test_parse_datetime_filters_accepts_string_and_dict(selector, datetime_search):
    filters = selector.parse_datetime_filters(datetime_search, for_insertion=False)

    assert filters["start_datetime"] == {"gte": "2020-02-10T00:00:00Z", "lte": None}
    assert filters["end_datetime"] == {"gte": None, "lte": "2020-02-12T00:00:00Z"}
    assert filters["datetime"] == {"gte": None, "lte": None}


def test_parse_datetime_filters_accepts_none(selector):
    filters = selector.parse_datetime_filters(None, for_insertion=False)

    assert filters["start_datetime"] == {"gte": None, "lte": None}
    assert filters["end_datetime"] == {"gte": None, "lte": None}
