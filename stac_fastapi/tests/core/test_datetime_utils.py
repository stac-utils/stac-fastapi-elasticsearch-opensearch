"""Unit tests for datetime range normalization."""

import pytest
from fastapi import HTTPException

from stac_fastapi.core.datetime_utils import format_datetime_range

OUT_OF_UTC_RANGE = ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"]


@pytest.mark.parametrize("use_nanos", ["true", "false"])
@pytest.mark.parametrize("value", OUT_OF_UTC_RANGE)
@pytest.mark.parametrize("shape", ["{v}", "{v}/..", "../{v}"])
def test_format_datetime_range_rejects_bound_outside_utc_range(
    monkeypatch, use_nanos, value, shape
):
    monkeypatch.setenv("USE_DATETIME_NANOS", use_nanos)

    with pytest.raises(HTTPException) as exc_info:
        format_datetime_range(shape.format(v=value))

    assert exc_info.value.status_code == 400
    assert value in exc_info.value.detail


@pytest.mark.parametrize(
    "use_nanos, expected",
    [
        ("true", "2020-01-01T00:00:00.0Z/2020-01-01T00:00:00.0Z"),
        ("false", "2020-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"),
    ],
)
def test_format_datetime_range_converts_offset_to_utc(monkeypatch, use_nanos, expected):
    monkeypatch.setenv("USE_DATETIME_NANOS", use_nanos)

    assert format_datetime_range("2020-01-01T01:00:00+01:00") == expected
