"""Datetime intervals outside the date_nanos range (1970 to 2262), #967."""

import pytest

from stac_fastapi.core.datetime_utils import format_datetime_range
from stac_fastapi.sfeos_helpers.database import (
    apply_collections_datetime_filter_shared,
    return_date,
)

MIN = "1970-01-01T00:00:00+00:00"
MAX = "2262-04-11T23:47:16.854775807Z"
EMPTY = {"gte": None, "lte": None, "empty": True}


@pytest.mark.parametrize(
    "nanos,interval,expected",
    [
        (
            "true",
            "1950-01-01T00:00:00Z",
            "1950-01-01T00:00:00.0Z/1950-01-01T00:00:00.0Z",
        ),
        (
            "true",
            "1950-01-01T00:00:00Z/1975-01-01T00:00:00Z",
            "1950-01-01T00:00:00.0Z/1975-01-01T00:00:00.0Z",
        ),
        ("true", "../1960-01-01T00:00:00Z", "../1960-01-01T00:00:00.0Z"),
        ("true", "2300-01-01T00:00:00Z/..", "2300-01-01T00:00:00.0Z/.."),
        (
            "true",
            "2020-01-01T00:00:00.000000001Z/..",
            "2020-01-01T00:00:00.000000001Z/..",
        ),
        (
            "true",
            "2020-01-01T05:30:00.123456789+05:30",
            "2020-01-01T00:00:00.123456789Z/2020-01-01T00:00:00.123456789Z",
        ),
        (
            "true",
            "2020-01-01T00:00:00.1234567890Z/..",
            "2020-01-01T00:00:00.123456789Z/..",
        ),
        ("false", "../1960-01-01T00:00:00Z", "../1960-01-01T00:00:00.000Z"),
    ],
)
def test_format_datetime_range_keeps_out_of_range_bounds_and_open_ends(
    monkeypatch, nanos, interval, expected
):
    monkeypatch.setenv("USE_DATETIME_NANOS", nanos)
    assert format_datetime_range(interval) == expected


@pytest.mark.parametrize(
    "interval,expected",
    [
        ("1950-01-01T00:00:00Z", EMPTY),
        ("1950-01-01T00:00:00Z/1960-01-01T00:00:00Z", EMPTY),
        ("../1960-01-01T00:00:00Z", EMPTY),
        ("2300-01-01T00:00:00Z/..", EMPTY),
        ("2300-01-01T00:00:00Z", EMPTY),
        (
            "1950-01-01T00:00:00Z/1975-01-01T00:00:00Z",
            {"gte": MIN, "lte": "1975-01-01T00:00:00Z"},
        ),
        (
            "2000-01-01T00:00:00Z/2300-01-01T00:00:00Z",
            {"gte": "2000-01-01T00:00:00Z", "lte": MAX},
        ),
        ("../1975-01-01T00:00:00Z", {"gte": MIN, "lte": "1975-01-01T00:00:00Z"}),
        ("2000-01-01T00:00:00Z/..", {"gte": "2000-01-01T00:00:00Z", "lte": MAX}),
        (
            "1975-01-01T00:00:00Z",
            {"gte": "1975-01-01T00:00:00Z", "lte": "1975-01-01T00:00:00Z"},
        ),
        (f"2000-01-01T00:00:00Z/{MAX}", {"gte": "2000-01-01T00:00:00Z", "lte": MAX}),
        (
            "2000-01-01T00:00:00Z/2262-04-11T23:47:16.854775808Z",
            {"gte": "2000-01-01T00:00:00Z", "lte": MAX},
        ),
        (
            "2000-01-01T00:00:00Z/2262-04-12T01:47:16.8547759+02:00",
            {"gte": "2000-01-01T00:00:00Z", "lte": MAX},
        ),
        (f"{MAX}/..", {"gte": MAX, "lte": MAX}),
        ("2262-04-11T23:47:16.854775808Z/..", EMPTY),
        ("1950-01-01", {"gte": "1950-01-01", "lte": "1950-01-01"}),
        ("..", {"gte": None, "lte": None}),
        (None, {"gte": None, "lte": None}),
    ],
)
def test_return_date_nanos_empties_or_clamps_out_of_range_intervals(
    monkeypatch, interval, expected
):
    monkeypatch.setenv("USE_DATETIME_NANOS", "true")
    assert return_date(interval) == expected


@pytest.mark.parametrize(
    "interval,expected",
    [
        (
            "1950-01-01T00:00:00Z/1960-01-01T00:00:00Z",
            {"gte": "1950-01-01T00:00:00Z", "lte": "1960-01-01T00:00:00Z"},
        ),
        (
            "../1960-01-01T00:00:00Z",
            {"gte": "0001-01-01T00:00:00Z", "lte": "1960-01-01T00:00:00Z"},
        ),
        (
            "2300-01-01T00:00:00Z",
            {"gte": "2300-01-01T00:00:00Z", "lte": "2300-01-01T00:00:00Z"},
        ),
    ],
)
def test_return_date_without_nanos_does_not_clamp(monkeypatch, interval, expected):
    monkeypatch.setenv("USE_DATETIME_NANOS", "false")
    assert return_date(interval) == expected


def _interval_range(lte=None, gte=None):
    bound = {"lte": lte} if lte else {"gte": gte}
    return {"range": {"extent.temporal.interval": bound}}


@pytest.mark.parametrize(
    "interval,must",
    [
        ("../1700-01-01T00:00:00Z", [_interval_range(lte="1700-01-01T00:00:00Z")]),
        ("1700-01-01T00:00:00Z/..", [_interval_range(gte="1700-01-01T00:00:00Z")]),
        ("../..", [{"exists": {"field": "extent.temporal.interval"}}]),
    ],
)
def test_collection_datetime_filter_leaves_open_ends_unbounded(interval, must):
    assert apply_collections_datetime_filter_shared(interval) == {
        "bool": {"must": must}
    }
