import uuid
from copy import deepcopy
from typing import Callable

import pytest
from stac_pydantic import Item, api

from stac_fastapi.types.errors import ConflictError, NotFoundError

from ..conftest import MockRequest


@pytest.mark.asyncio
async def test_create_collection(app_client, ctx, core_client, txn_client):
    in_coll = deepcopy(ctx.collection)
    in_coll["id"] = str(uuid.uuid4())
    await txn_client.create_collection(api.Collection(**in_coll), request=MockRequest)
    got_coll = await core_client.get_collection(in_coll["id"], request=MockRequest)
    assert got_coll["id"] == in_coll["id"]
    await txn_client.delete_collection(in_coll["id"])


@pytest.mark.asyncio
async def test_create_collection_already_exists(app_client, ctx, txn_client):
    data = deepcopy(ctx.collection)

    # change id to avoid elasticsearch duplicate key error
    data["_id"] = str(uuid.uuid4())

    with pytest.raises(ConflictError):
        await txn_client.create_collection(api.Collection(**data), request=MockRequest)

    await txn_client.delete_collection(data["id"])


@pytest.mark.asyncio
async def test_update_collection(
    core_client,
    txn_client,
    load_test_data: Callable,
):
    collection_data = load_test_data("test_collection.json")
    item_data = load_test_data("test_item.json")

    await txn_client.create_collection(
        api.Collection(**collection_data), request=MockRequest
    )
    await txn_client.create_item(
        collection_id=collection_data["id"],
        item=api.Item(**item_data),
        request=MockRequest,
        refresh=True,
    )

    collection_data["keywords"].append("new keyword")
    await txn_client.update_collection(
        collection_data["id"], api.Collection(**collection_data), request=MockRequest
    )

    coll = await core_client.get_collection(collection_data["id"], request=MockRequest)
    assert "new keyword" in coll["keywords"]

    item = await core_client.get_item(
        item_id=item_data["id"],
        collection_id=collection_data["id"],
        request=MockRequest,
    )
    assert item["id"] == item_data["id"]
    assert item["collection"] == item_data["collection"]

    await txn_client.delete_collection(collection_data["id"])


@pytest.mark.skip(reason="Can not update collection id anymore?")
@pytest.mark.asyncio
async def test_update_collection_id(
    core_client,
    txn_client,
    load_test_data: Callable,
):
    collection_data = load_test_data("test_collection.json")
    item_data = load_test_data("test_item.json")
    new_collection_id = "new-test-collection"

    await txn_client.create_collection(
        api.Collection(**collection_data), request=MockRequest
    )
    await txn_client.create_item(
        collection_id=collection_data["id"],
        item=api.Item(**item_data),
        request=MockRequest,
        refresh=True,
    )

    old_collection_id = collection_data["id"]
    collection_data["id"] = new_collection_id

    await txn_client.update_collection(
        collection_id=collection_data["id"],
        collection=api.Collection(**collection_data),
        request=MockRequest(
            query_params={
                "collection_id": old_collection_id,
                "limit": "10",
            }
        ),
        refresh=True,
    )

    with pytest.raises(NotFoundError):
        await core_client.get_collection(old_collection_id, request=MockRequest)

    coll = await core_client.get_collection(collection_data["id"], request=MockRequest)
    assert coll["id"] == new_collection_id

    with pytest.raises(NotFoundError):
        await core_client.get_item(
            item_id=item_data["id"],
            collection_id=old_collection_id,
            request=MockRequest,
        )

    item = await core_client.get_item(
        item_id=item_data["id"],
        collection_id=collection_data["id"],
        request=MockRequest,
        refresh=True,
    )

    assert item["id"] == item_data["id"]
    assert item["collection"] == new_collection_id

    await txn_client.delete_collection(collection_data["id"])


@pytest.mark.asyncio
async def test_delete_collection(
    core_client,
    txn_client,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    await txn_client.create_collection(api.Collection(**data), request=MockRequest)

    await txn_client.delete_collection(data["id"])

    with pytest.raises(NotFoundError):
        await core_client.get_collection(data["id"], request=MockRequest)


@pytest.mark.asyncio
async def test_get_collection(
    core_client,
    txn_client,
    load_test_data: Callable,
):
    data = load_test_data("test_collection.json")
    await txn_client.create_collection(api.Collection(**data), request=MockRequest)
    coll = await core_client.get_collection(data["id"], request=MockRequest)
    assert coll["id"] == data["id"]

    await txn_client.delete_collection(data["id"])


@pytest.mark.asyncio
async def test_get_item(app_client, ctx, core_client):
    got_item = await core_client.get_item(
        item_id=ctx.item["id"],
        collection_id=ctx.item["collection"],
        request=MockRequest,
    )
    assert got_item["id"] == ctx.item["id"]
    assert got_item["collection"] == ctx.item["collection"]


@pytest.mark.asyncio
async def test_get_collection_items(app_client, ctx, core_client, txn_client):
    coll = ctx.collection
    num_of_items_to_create = 5
    for _ in range(num_of_items_to_create):
        item = deepcopy(ctx.item)
        item["id"] = str(uuid.uuid4())
        await txn_client.create_item(
            collection_id=item["collection"],
            item=api.Item(**item),
            request=MockRequest,
            refresh=True,
        )

    fc = await core_client.item_collection(coll["id"], request=MockRequest())
    assert len(fc["features"]) == num_of_items_to_create + 1  # ctx.item

    for item in fc["features"]:
        assert item["collection"] == coll["id"]


@pytest.mark.asyncio
async def test_create_item(ctx, core_client, txn_client):
    resp = await core_client.get_item(
        ctx.item["id"], ctx.item["collection"], request=MockRequest
    )
    assert Item(**ctx.item).model_dump(
        exclude={"links": ..., "properties": {"created", "updated"}}
    ) == Item(**resp).model_dump(
        exclude={"links": ..., "properties": {"created", "updated"}}
    )


@pytest.mark.asyncio
async def test_create_item_already_exists(ctx, txn_client):
    with pytest.raises(ConflictError):
        await txn_client.create_item(
            collection_id=ctx.item["collection"],
            item=api.Item(**ctx.item),
            request=MockRequest,
            refresh=True,
        )


@pytest.mark.asyncio
async def test_update_item(ctx, core_client, txn_client):
    item = ctx.item
    item["properties"]["foo"] = "bar"
    collection_id = item["collection"]
    item_id = item["id"]
    await txn_client.update_item(
        collection_id=collection_id,
        item_id=item_id,
        item=api.Item(**item),
        request=MockRequest,
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )
    assert updated_item["properties"]["foo"] == "bar"


@pytest.mark.asyncio
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
        collection_id=collection_id,
        item_id=item_id,
        item=api.Item(**ctx.item),
        request=MockRequest,
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )
    assert updated_item["geometry"]["coordinates"] == new_coordinates


@pytest.mark.asyncio
async def test_delete_item(ctx, core_client, txn_client):
    await txn_client.delete_item(ctx.item["id"], ctx.item["collection"])

    with pytest.raises(NotFoundError):
        await core_client.get_item(
            ctx.item["id"], ctx.item["collection"], request=MockRequest
        )


@pytest.mark.asyncio
async def test_landing_page_no_collection_title(ctx, core_client, txn_client, app):
    ctx.collection["id"] = "new_id"
    del ctx.collection["title"]
    await txn_client.create_collection(
        api.Collection(**ctx.collection), request=MockRequest
    )

    landing_page = await core_client.landing_page(request=MockRequest(app=app))
    for link in landing_page["links"]:
        if link["href"].split("/")[-1] == ctx.collection["id"]:
            assert link["title"]
