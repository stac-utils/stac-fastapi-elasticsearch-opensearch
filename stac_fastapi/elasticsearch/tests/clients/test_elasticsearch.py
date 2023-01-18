import uuid
from copy import deepcopy
from typing import Callable

import pytest
from stac_pydantic import Item

from stac_fastapi.extensions.third_party.bulk_transactions import Items
from stac_fastapi.types.errors import ConflictError, NotFoundError

from ..conftest import MockRequest, create_item


async def test_create_collection(app_client, ctx, core_client, txn_client):
    in_coll = deepcopy(ctx.collection)
    in_coll["id"] = str(uuid.uuid4())
    await txn_client.create_collection(in_coll, request=MockRequest)
    got_coll = await core_client.get_collection(in_coll["id"], request=MockRequest)
    assert got_coll["id"] == in_coll["id"]
    await txn_client.delete_collection(in_coll["id"])


async def test_create_collection_already_exists(app_client, ctx, txn_client):
    data = deepcopy(ctx.collection)

    # change id to avoid elasticsearch duplicate key error
    data["_id"] = str(uuid.uuid4())

    with pytest.raises(ConflictError):
        await txn_client.create_collection(data, request=MockRequest)

    await txn_client.delete_collection(data["id"])


async def test_update_collection(
    core_client,
    txn_client,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")

    await txn_client.create_collection(data, request=MockRequest)
    data["keywords"].append("new keyword")
    await txn_client.update_collection(data, request=MockRequest)

    coll = await core_client.get_collection(data["id"], request=MockRequest)
    assert "new keyword" in coll["keywords"]

    await txn_client.delete_collection(data["id"])


async def test_delete_collection(
    core_client,
    txn_client,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    await txn_client.create_collection(data, request=MockRequest)

    await txn_client.delete_collection(data["id"])

    with pytest.raises(NotFoundError):
        await core_client.get_collection(data["id"], request=MockRequest)


async def test_get_collection(
    core_client,
    txn_client,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    await txn_client.create_collection(data, request=MockRequest)
    coll = await core_client.get_collection(data["id"], request=MockRequest)
    assert coll["id"] == data["id"]

    await txn_client.delete_collection(data["id"])


async def test_get_item(app_client, ctx, core_client):
    got_item = await core_client.get_item(
        item_id=ctx.item["id"],
        collection_id=ctx.item["collection"],
        request=MockRequest,
    )
    assert got_item["id"] == ctx.item["id"]
    assert got_item["collection"] == ctx.item["collection"]


async def test_get_collection_items(app_client, ctx, core_client, txn_client):
    coll = ctx.collection
    num_of_items_to_create = 5
    for _ in range(num_of_items_to_create):
        item = deepcopy(ctx.item)
        item["id"] = str(uuid.uuid4())
        await txn_client.create_item(
            collection_id=item["collection"],
            item=item,
            request=MockRequest,
            refresh=True,
        )

    fc = await core_client.item_collection(coll["id"], request=MockRequest())
    assert len(fc["features"]) == num_of_items_to_create + 1  # ctx.item

    for item in fc["features"]:
        assert item["collection"] == coll["id"]


async def test_create_item(ctx, core_client, txn_client):
    resp = await core_client.get_item(
        ctx.item["id"], ctx.item["collection"], request=MockRequest
    )
    assert Item(**ctx.item).dict(
        exclude={"links": ..., "properties": {"created", "updated"}}
    ) == Item(**resp).dict(exclude={"links": ..., "properties": {"created", "updated"}})


async def test_create_item_already_exists(ctx, txn_client):
    with pytest.raises(ConflictError):
        await txn_client.create_item(
            collection_id=ctx.item["collection"],
            item=ctx.item,
            request=MockRequest,
            refresh=True,
        )


async def test_update_item(ctx, core_client, txn_client):
    ctx.item["properties"]["foo"] = "bar"
    collection_id = ctx.item["collection"]
    item_id = ctx.item["id"]
    await txn_client.update_item(
        collection_id=collection_id, item_id=item_id, item=ctx.item, request=MockRequest
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )
    assert updated_item["properties"]["foo"] == "bar"


async def test_update_geometry(ctx, core_client, txn_client):
    new_coordinates = [
        [
            [142.15052873427666, -33.82243006904891],
            [140.1000346138806, -34.257132625788756],
            [139.5776607193635, -32.514709769700254],
            [141.6262528041627, -32.08081674221862],
            [142.15052873427666, -33.82243006904891],
        ]
    ]

    ctx.item["geometry"]["coordinates"] = new_coordinates
    collection_id = ctx.item["collection"]
    item_id = ctx.item["id"]
    await txn_client.update_item(
        collection_id=collection_id, item_id=item_id, item=ctx.item, request=MockRequest
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )
    assert updated_item["geometry"]["coordinates"] == new_coordinates


async def test_delete_item(ctx, core_client, txn_client):
    await txn_client.delete_item(ctx.item["id"], ctx.item["collection"])

    with pytest.raises(NotFoundError):
        await core_client.get_item(
            ctx.item["id"], ctx.item["collection"], request=MockRequest
        )


async def test_bulk_item_insert(ctx, core_client, txn_client, bulk_txn_client):
    items = {}
    for _ in range(10):
        _item = deepcopy(ctx.item)
        _item["id"] = str(uuid.uuid4())
        items[_item["id"]] = _item

    # fc = es_core.item_collection(coll["id"], request=MockStarletteRequest)
    # assert len(fc["features"]) == 0

    bulk_txn_client.bulk_item_insert(Items(items=items), refresh=True)

    fc = await core_client.item_collection(ctx.collection["id"], request=MockRequest())
    assert len(fc["features"]) >= 10

    # for item in items:
    #     es_transactions.delete_item(
    #         item["id"], item["collection"], request=MockStarletteRequest
    #     )


async def test_feature_collection_insert(
    core_client,
    txn_client,
    ctx,
):
    features = []
    for _ in range(10):
        _item = deepcopy(ctx.item)
        _item["id"] = str(uuid.uuid4())
        features.append(_item)

    feature_collection = {"type": "FeatureCollection", "features": features}

    await create_item(txn_client, feature_collection)

    fc = await core_client.item_collection(ctx.collection["id"], request=MockRequest())
    assert len(fc["features"]) >= 10


async def test_landing_page_no_collection_title(ctx, core_client, txn_client, app):
    ctx.collection["id"] = "new_id"
    del ctx.collection["title"]
    await txn_client.create_collection(ctx.collection, request=MockRequest)

    landing_page = await core_client.landing_page(request=MockRequest(app=app))
    for link in landing_page["links"]:
        if link["href"].split("/")[-1] == ctx.collection["id"]:
            assert link["title"]
