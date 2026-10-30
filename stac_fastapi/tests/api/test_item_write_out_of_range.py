"""Single-item writes the engine rejects return 400 (#967)."""

import uuid
from copy import deepcopy

import pytest
import pytest_asyncio
from fastapi import HTTPException

from ..conftest import MockRequest, delete_collections_and_items

OUT_OF_NANOS_RANGE = ["1950-01-01T00:00:00Z", "2300-01-01T00:00:00Z"]


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _item(base: dict, item_id: str, collection_id: str, dt: str) -> dict:
    item = deepcopy(base)
    item.update(id=item_id, collection=collection_id)
    item["properties"].update(datetime=dt, start_datetime=dt, end_datetime=dt)
    return item


async def _seed(client, load_test_data) -> str:
    """A collection with a 1970-01-01 item, which either USE_DATETIME mode can index."""
    collection = load_test_data("test_collection.json")
    collection["id"] = _uid("write-400")
    resp = await client.post("/collections", json=collection)
    assert resp.status_code == 201, resp.text

    item = _item(
        load_test_data("test_item.json"),
        "dt-1970",
        collection["id"],
        "1970-01-01T00:00:00Z",
    )
    resp = await client.post(f"/collections/{collection['id']}/items", json=item)
    assert resp.status_code == 201, resp.text
    return collection["id"]


@pytest_asyncio.fixture()
async def seeded(app_client, txn_client, load_test_data):
    try:
        yield await _seed(app_client, load_test_data)
    finally:
        await delete_collections_and_items(txn_client)


@pytest.fixture(params=["true", "false"], ids=["use_datetime", "start_end_only"])
def use_datetime(request, monkeypatch):
    monkeypatch.setenv("USE_DATETIME", request.param)


@pytest_asyncio.fixture()
async def seeded_with_index_filtering(
    use_datetime, datetime_filtering_env, app_client, txn_client, load_test_data
):
    try:
        yield await _seed(app_client, load_test_data)
    finally:
        await delete_collections_and_items(txn_client)


def _assert_400_names_value(resp, dt: str):
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert "properties." in detail and dt in detail


@pytest.mark.asyncio
@pytest.mark.parametrize("dt", OUT_OF_NANOS_RANGE)
async def test_item_write_outside_nanos_range_returns_400(
    app_client, seeded, load_test_data, dt
):
    item = _item(load_test_data("test_item.json"), "dt-1970", seeded, dt)

    post = await app_client.post(
        f"/collections/{seeded}/items", json={**item, "id": _uid("new")}
    )
    put = await app_client.put(f"/collections/{seeded}/items/dt-1970", json=item)

    for resp in (post, put):
        _assert_400_names_value(resp, dt)


@pytest.mark.asyncio
@pytest.mark.parametrize("dt", OUT_OF_NANOS_RANGE)
async def test_item_patch_outside_nanos_range_returns_400_with_validator(
    app_client, seeded, monkeypatch, dt
):
    """With the STAC validator on, PATCH writes the merged item through create_item."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    resp = await app_client.patch(
        f"/collections/{seeded}/items/dt-1970",
        json={"properties": {"datetime": dt, "start_datetime": dt, "end_datetime": dt}},
        headers={"Content-Type": "application/merge-patch+json"},
    )
    _assert_400_names_value(resp, dt)


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_item_post_outside_nanos_range_with_index_filtering_returns_400(
    app_client, seeded_with_index_filtering, load_test_data
):
    cid = seeded_with_index_filtering
    dt = "1950-01-01T00:00:00Z"
    item = _item(load_test_data("test_item.json"), _uid("old"), cid, dt)
    resp = await app_client.post(f"/collections/{cid}/items", json=item)
    _assert_400_names_value(resp, dt)


@pytest.mark.asyncio
async def test_create_item_outside_nanos_range_raises_400(txn_client, ctx):
    item = deepcopy(ctx.item)
    item["id"] = _uid("old")
    item["properties"]["datetime"] = "1950-01-01T00:00:00Z"
    item = await txn_client.database.async_prep_create_item(
        item=item, base_url=str(MockRequest.base_url)
    )

    with pytest.raises(HTTPException) as exc:
        await txn_client.database.create_item(item)
    assert exc.value.status_code == 400
