"""Datetime bounds that can't be represented in UTC are rejected with 400."""

import pytest

pytestmark = pytest.mark.asyncio

OUT_OF_UTC_RANGE = ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"]
ROUTES = [
    ("/search", {}),
    ("/collections", {}),
    ("/aggregate", {"aggregations": "total_count"}),
]


@pytest.mark.parametrize("value", OUT_OF_UTC_RANGE)
@pytest.mark.parametrize("path, params", ROUTES)
async def test_get_datetime_outside_utc_range_returns_400(
    app_client, value, path, params
):
    response = await app_client.get(path, params={**params, "datetime": value})

    assert response.status_code == 400
    assert "outside the range that can be represented in UTC" in response.text
