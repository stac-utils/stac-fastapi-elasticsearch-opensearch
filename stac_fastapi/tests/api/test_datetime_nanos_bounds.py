"""Dates outside 1970 to 2262 with the default USE_DATETIME_NANOS=true (#967)."""

import uuid
from copy import deepcopy

import pytest
import pytest_asyncio

from stac_fastapi.core.utilities import get_bool_env

from ..conftest import MockRequest, delete_collections_and_items


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _item(base: dict, item_id: str, dt: str | None, start: str, end: str) -> dict:
    item = deepcopy(base)
    item["id"] = item_id
    item["properties"].update(datetime=dt, start_datetime=start, end_datetime=end)
    return item


@pytest.fixture(params=["true", "false"], ids=["use_datetime", "start_end_only"])
def use_datetime(request, monkeypatch):
    monkeypatch.setenv("USE_DATETIME", request.param)


async def _seed(client, load_test_data, with_span: bool = True) -> str:
    """A 1970-01-01 item and a datetime:null item spanning the whole nanos range."""
    collection = load_test_data("test_collection.json")
    collection["id"] = _uid("nanos-bounds")
    resp = await client.post("/collections", json=collection)
    assert resp.status_code == 201, resp.text

    base = load_test_data("test_item.json")
    base["collection"] = collection["id"]
    epoch = "1970-01-01T00:00:00Z"
    items = [_item(base, "dt-1970", epoch, epoch, epoch)]
    if with_span:
        items.append(_item(base, "span", None, epoch, "2262-01-01T00:00:00Z"))
    for item in items:
        resp = await client.post(f"/collections/{collection['id']}/items", json=item)
        assert resp.status_code == 201, resp.text
    return collection["id"]


@pytest_asyncio.fixture()
async def seeded(catalogs_app_client, txn_client, load_test_data):
    if get_bool_env("ENABLE_DATETIME_INDEX_FILTERING"):
        pytest.skip("seeds a datetime:null item, which index filtering rejects")
    try:
        yield await _seed(catalogs_app_client, load_test_data)
    finally:
        await delete_collections_and_items(txn_client)


@pytest_asyncio.fixture()
async def seeded_with_index_filtering(
    use_datetime, datetime_filtering_env, app_client, txn_client, load_test_data
):
    """Index filtering needs a datetime with USE_DATETIME=true, so the span item is left out there."""
    with_span = get_bool_env("USE_DATETIME") is False
    try:
        yield await _seed(app_client, load_test_data, with_span), with_span
    finally:
        await delete_collections_and_items(txn_client)


def _ids(resp) -> set[str]:
    assert resp.status_code == 200, resp.text
    return {f["id"] for f in resp.json()["features"]}


OUT_OF_RANGE = [
    "1950-01-01T00:00:00Z",
    "1950-01-01T00:00:00Z/1960-01-01T00:00:00Z",
    "../1960-01-01T00:00:00Z",
    "2300-01-01T00:00:00Z/..",
]
INTERVALS = [(interval, set()) for interval in OUT_OF_RANGE] + [
    ("1950-01-01T00:00:00Z/1975-01-01T00:00:00Z", {"dt-1970", "span"})
]


async def _assert_item_searches(client, cid, interval, expected):
    params = {"collections": cid, "datetime": interval}
    assert _ids(await client.get("/search", params=params)) == expected
    body = {"collections": [cid], "datetime": interval}
    assert _ids(await client.post("/search", json=body)) == expected
    resp = await client.get(f"/collections/{cid}/items", params={"datetime": interval})
    assert _ids(resp) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("interval,expected", INTERVALS)
async def test_search_outside_nanos_range_matches_nothing(
    catalogs_app_client, seeded, use_datetime, interval, expected
):
    await _assert_item_searches(catalogs_app_client, seeded, interval, expected)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize("interval,expected", INTERVALS)
async def test_search_outside_nanos_range_with_index_filtering(
    app_client, seeded_with_index_filtering, interval, expected
):
    cid, with_span = seeded_with_index_filtering
    if not with_span:
        expected = expected - {"span"}
    await _assert_item_searches(app_client, cid, interval, expected)


@pytest.mark.asyncio
async def test_lower_bound_keeps_nanoseconds(catalogs_app_client, seeded, use_datetime):
    """A bound 1 ns after the 1970-01-01 item excludes it."""
    interval = "1970-01-01T00:00:00.000000001Z/.."
    params = {"collections": seeded, "datetime": interval}
    assert _ids(await catalogs_app_client.get("/search", params=params)) == {"span"}
    body = {"collections": [seeded], "datetime": interval}
    assert _ids(await catalogs_app_client.post("/search", json=body)) == {"span"}


@pytest.mark.asyncio
async def test_open_upper_bound_reaches_the_last_storable_nanosecond(
    catalogs_app_client, txn_client, seeded, load_test_data
):
    # The API parses datetimes to microseconds, so store the item through the database.
    last = "2262-04-11T23:47:16.854775807Z"
    item = load_test_data("test_item.json")
    item.update(id="dt-max", collection=seeded)
    item["properties"].update(datetime=last, start_datetime=last, end_datetime=last)
    item = await txn_client.database.async_prep_create_item(
        item=item, base_url=str(MockRequest.base_url)
    )
    await txn_client.database.create_item(item, refresh=True)

    params = {"collections": seeded, "datetime": "2262-04-11T00:00:00Z/.."}
    resp = await catalogs_app_client.get("/search", params=params)
    found = {f["id"]: f for f in resp.json()["features"]}
    assert found["dt-max"]["properties"]["datetime"] == last


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval,expected",
    [
        ("1950-01-01T00:00:00Z", set()),
        ("1950-01-01T00:00:00Z/1975-01-01T00:00:00Z", {"dt-1970", "span"}),
    ],
)
async def test_catalog_collection_items_outside_nanos_range(
    catalogs_app_client, load_test_data, seeded, interval, expected
):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = _uid("cat")
    resp = await catalogs_app_client.post("/catalogs", json=catalog)
    assert resp.status_code == 201, resp.text
    resp = await catalogs_app_client.post(
        f"/catalogs/{catalog['id']}/collections", json={"id": seeded}
    )
    assert resp.status_code == 200, resp.text

    resp = await catalogs_app_client.get(
        f"/catalogs/{catalog['id']}/collections/{seeded}/items",
        params={"datetime": interval},
    )
    assert _ids(resp) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval,expected",
    [
        ("1955-01-01T00:00:00Z", {"1950s"}),
        ("../1700-01-01T00:00:00Z", {"1600s"}),
        ("2500-01-01T00:00:00Z/..", {"3000s"}),
    ],
)
async def test_collection_search_outside_nanos_range(
    catalogs_app_client, txn_client, load_test_data, interval, expected
):
    prefix = _uid("extent")
    try:
        for name, extent in [
            ("1600s", ["1600-01-01T00:00:00Z", "1650-01-01T00:00:00Z"]),
            ("1950s", ["1950-01-01T00:00:00Z", "1960-01-01T00:00:00Z"]),
            ("1980s", ["1980-01-01T00:00:00Z", "1990-01-01T00:00:00Z"]),
            ("3000s", ["3000-01-01T00:00:00Z", "3050-01-01T00:00:00Z"]),
        ]:
            collection = load_test_data("test_collection.json")
            collection["id"] = f"{prefix}-{name}"
            collection["extent"]["temporal"]["interval"] = [extent]
            resp = await catalogs_app_client.post("/collections", json=collection)
            assert resp.status_code == 201, resp.text

        params = {"datetime": interval, "limit": 100}
        for resp in (
            await catalogs_app_client.get("/collections", params=params),
            await catalogs_app_client.get("/collections-search", params=params),
            await catalogs_app_client.post("/collections-search", json=params),
        ):
            assert resp.status_code == 200, resp.text
            found = {
                c["id"]
                for c in resp.json()["collections"]
                if c["id"].startswith(prefix)
            }
            assert found == {f"{prefix}-{name}" for name in expected}
    finally:
        await delete_collections_and_items(txn_client)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval,expected",
    [
        ("1950-01-01T00:00:00Z", 0),
        ("../1960-01-01T00:00:00Z", 0),
        ("1950-01-01T00:00:00Z/1975-01-01T00:00:00Z", 2),
    ],
)
async def test_aggregate_outside_nanos_range(
    catalogs_app_client, seeded, interval, expected
):
    params = {
        "collections": seeded,
        "datetime": interval,
        "aggregations": "total_count",
    }
    get = await catalogs_app_client.get("/aggregate", params=params)
    post = await catalogs_app_client.post(
        "/aggregate",
        json={
            "collections": [seeded],
            "datetime": interval,
            "aggregations": ["total_count"],
        },
    )
    for resp in (get, post):
        assert resp.status_code == 200, resp.text
        aggs = {a["name"]: a["value"] for a in resp.json()["aggregations"]}
        assert aggs["total_count"] == expected


def test_empty_interval_is_a_filter_that_keeps_later_filters(txn_client):
    """Aggregation applies datetime before collections; both must stay in the query."""
    db = txn_client.database
    search, _ = db.apply_datetime_filter(db.make_search(), "1950-01-01T00:00:00Z")
    search = db.apply_collections_filter(search, ["c1"])
    filters = search.to_dict()["query"]["bool"]["filter"]
    assert {"match_none": {}} in filters
    assert {"terms": {"collection": ["c1"]}} in filters
