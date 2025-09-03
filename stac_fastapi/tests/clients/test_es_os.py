import uuid
from copy import deepcopy
from typing import Callable

import pytest
from fastapi import HTTPException
from stac_pydantic import Item, api

from stac_fastapi.extensions.core.transaction.request import (
    PatchAddReplaceTest,
    PatchMoveCopy,
    PatchRemove,
)
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
async def test_merge_patch_item_add(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch={"properties": {"foo": "bar", "ext:hello": "world"}},
        request=MockRequest(headers={"content-type": "application/json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )
    assert updated_item["properties"]["foo"] == "bar"
    assert updated_item["properties"]["ext:hello"] == "world"


@pytest.mark.asyncio
async def test_merge_patch_item_remove(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch={"properties": {"gsd": None, "proj:epsg": None}},
        request=MockRequest(headers={"content-type": "application/merge-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )
    assert "gsd" not in updated_item["properties"]
    assert "proj:epsg" not in updated_item["properties"]


@pytest.mark.asyncio
async def test_merge_patch_create_nest(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch={"properties": {"new": {"nest": "foo"}}},
        request=MockRequest(headers={"content-type": "application/merge-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )
    assert "new" in updated_item["properties"]
    assert "nest" in updated_item["properties"]["new"]
    assert updated_item["properties"]["new"]["nest"] == "foo"


@pytest.mark.asyncio
async def test_json_patch_item_add(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/foo", "value": "bar"}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/ext:hello", "value": "world"}
        ),
        PatchAddReplaceTest.model_validate(
            {
                "op": "add",
                "path": "/properties/eo:bands/1",
                "value": {
                    "gsd": 10,
                    "name": "FB",
                    "common_name": "fake_band",
                    "center_wavelength": 3.45,
                    "full_width_half_max": 1.23,
                },
            }
        ),
    ]

    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )

    assert updated_item["properties"]["foo"] == "bar"
    assert updated_item["properties"]["ext:hello"] == "world"
    assert (
        len(updated_item["properties"]["eo:bands"])
        == len(ctx.item["properties"]["eo:bands"]) + 1
    )
    assert updated_item["properties"]["eo:bands"][1] == {
        "gsd": 10,
        "name": "FB",
        "common_name": "fake_band",
        "center_wavelength": 3.45,
        "full_width_half_max": 1.23,
    }


@pytest.mark.asyncio
async def test_json_patch_item_replace(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/properties/gsd", "value": 100}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/properties/proj:epsg", "value": 12345}
        ),
        PatchAddReplaceTest.model_validate(
            {
                "op": "replace",
                "path": "/properties/eo:bands/1",
                "value": {
                    "gsd": 10,
                    "name": "FB",
                    "common_name": "fake_band",
                    "center_wavelength": 3.45,
                    "full_width_half_max": 1.23,
                },
            }
        ),
    ]

    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )

    assert updated_item["properties"]["gsd"] == 100
    assert updated_item["properties"]["proj:epsg"] == 12345
    assert len(updated_item["properties"]["eo:bands"]) == len(
        ctx.item["properties"]["eo:bands"]
    )
    assert updated_item["properties"]["eo:bands"][1] == {
        "gsd": 10,
        "name": "FB",
        "common_name": "fake_band",
        "center_wavelength": 3.45,
        "full_width_half_max": 1.23,
    }


@pytest.mark.asyncio
async def test_json_patch_item_test(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "test", "path": "/properties/gsd", "value": 15}
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "test", "path": "/properties/proj:epsg", "value": 32756}
        ),
        PatchAddReplaceTest.model_validate(
            {
                "op": "test",
                "path": "/properties/eo:bands/1",
                "value": item["properties"]["eo:bands"][1],
            }
        ),
    ]

    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )

    assert updated_item["properties"]["gsd"] == 15
    assert updated_item["properties"]["proj:epsg"] == 32756
    assert (
        updated_item["properties"]["eo:bands"][1] == item["properties"]["eo:bands"][1]
    )


@pytest.mark.asyncio
async def test_json_patch_item_move(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "move", "path": "/properties/foo", "from": "/properties/gsd"}
        ),
        PatchMoveCopy.model_validate(
            {"op": "move", "path": "/properties/bar", "from": "/properties/proj:epsg"}
        ),
        PatchMoveCopy.model_validate(
            {
                "op": "move",
                "path": "/properties/eo:bands/0",
                "from": "/properties/eo:bands/1",
            }
        ),
    ]

    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )

    assert updated_item["properties"]["foo"] == 15
    assert "gsd" not in updated_item["properties"]
    assert updated_item["properties"]["bar"] == 32756
    assert "proj:epsg" not in updated_item["properties"]
    assert len(updated_item["properties"]["eo:bands"]) == len(
        ctx.item["properties"]["eo:bands"]
    )
    assert (
        updated_item["properties"]["eo:bands"][0]
        == ctx.item["properties"]["eo:bands"][1]
    )
    assert (
        updated_item["properties"]["eo:bands"][1]
        != ctx.item["properties"]["eo:bands"][1]
    )


@pytest.mark.asyncio
async def test_json_patch_item_copy(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "copy", "path": "/properties/foo", "from": "/properties/gsd"}
        ),
        PatchMoveCopy.model_validate(
            {"op": "copy", "path": "/properties/bar", "from": "/properties/proj:epsg"}
        ),
        PatchMoveCopy.model_validate(
            {
                "op": "copy",
                "path": "/properties/eo:bands/0",
                "from": "/properties/eo:bands/1",
            }
        ),
    ]

    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )

    assert updated_item["properties"]["foo"] == updated_item["properties"]["gsd"]
    assert updated_item["properties"]["bar"] == updated_item["properties"]["proj:epsg"]
    assert len(updated_item["properties"]["eo:bands"]) == len(
        ctx.item["properties"]["eo:bands"]
    )
    assert (
        updated_item["properties"]["eo:bands"][0]
        == ctx.item["properties"]["eo:bands"][1]
    )


@pytest.mark.asyncio
async def test_json_patch_item_remove(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchRemove.model_validate({"op": "remove", "path": "/properties/gsd"}),
        PatchRemove.model_validate({"op": "remove", "path": "/properties/proj:epsg"}),
        PatchRemove.model_validate({"op": "remove", "path": "/properties/eo:bands/1"}),
    ]

    await txn_client.patch_item(
        collection_id=collection_id,
        item_id=item_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_item = await core_client.get_item(
        item_id, collection_id, request=MockRequest
    )

    assert "gsd" not in updated_item["properties"]
    assert "proj:epsg" not in updated_item["properties"]
    assert (
        len(updated_item["properties"]["eo:bands"])
        == len(ctx.item["properties"]["eo:bands"]) - 1
    )
    assert (
        updated_item["properties"]["eo:bands"]
        == ctx.item["properties"]["eo:bands"][:1]
        + ctx.item["properties"]["eo:bands"][2:]
    )


@pytest.mark.asyncio
async def test_json_patch_add_with_bad_nest(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/properties/bad/nest", "value": "foo"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_item(
            collection_id=collection_id,
            item_id=item_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_item_test_wrong_value(ctx, core_client, txn_client):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "test", "path": "/properties/platform", "value": "landsat-9"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_item(
            collection_id=collection_id,
            item_id=item_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_item_replace_property_does_not_exists(
    ctx, core_client, txn_client
):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/properties/foo", "value": "landsat-9"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_item(
            collection_id=collection_id,
            item_id=item_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_item_remove_property_does_not_exists(
    ctx, core_client, txn_client
):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchRemove.model_validate({"op": "remove", "path": "/properties/foo"}),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_item(
            collection_id=collection_id,
            item_id=item_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_item_move_property_does_not_exists(
    ctx, core_client, txn_client
):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "move", "path": "/properties/bar", "from": "/properties/foo"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_item(
            collection_id=collection_id,
            item_id=item_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_item_copy_property_does_not_exists(
    ctx, core_client, txn_client
):
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "copy", "path": "/properties/bar", "from": "/properties/foo"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_item(
            collection_id=collection_id,
            item_id=item_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


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


@pytest.mark.asyncio
async def test_merge_patch_collection_add(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]

    await txn_client.patch_collection(
        collection_id=collection_id,
        patch={"summaries": {"foo": "bar", "hello": "world"}},
        request=MockRequest(headers={"content-type": "application/json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )
    assert updated_collection["summaries"]["foo"] == "bar"
    assert updated_collection["summaries"]["hello"] == "world"


@pytest.mark.asyncio
async def test_merge_patch_collection_remove(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    await txn_client.patch_collection(
        collection_id=collection_id,
        patch={"summaries": {"gsd": None}},
        request=MockRequest(headers={"content-type": "application/merge-patch+json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )
    assert "gsd" not in updated_collection["summaries"]


@pytest.mark.asyncio
async def test_json_patch_collection_add(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/summaries/foo", "value": "bar"},
        ),
        PatchAddReplaceTest.model_validate(
            {"op": "add", "path": "/summaries/gsd/1", "value": 100},
        ),
    ]

    await txn_client.patch_collection(
        collection_id=collection_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )

    assert updated_collection["summaries"]["foo"] == "bar"
    assert updated_collection["summaries"]["gsd"] == [30, 100]


@pytest.mark.asyncio
async def test_json_patch_collection_replace(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/summaries/gsd", "value": [100]}
        ),
    ]

    await txn_client.patch_collection(
        collection_id=collection_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )

    assert updated_collection["summaries"]["gsd"] == [100]


@pytest.mark.asyncio
async def test_json_patch_collection_test(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "test", "path": "/summaries/gsd", "value": [30]}
        ),
    ]

    await txn_client.patch_collection(
        collection_id=collection_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )

    assert updated_collection["summaries"]["gsd"] == [30]


@pytest.mark.asyncio
async def test_json_patch_collection_move(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "move", "path": "/summaries/bar", "from": "/summaries/gsd"}
        ),
    ]

    await txn_client.patch_collection(
        collection_id=collection_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )

    assert updated_collection["summaries"]["bar"] == [30]
    assert "gsd" not in updated_collection["summaries"]


@pytest.mark.asyncio
async def test_json_patch_collection_copy(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "copy", "path": "/summaries/foo", "from": "/summaries/gsd"}
        ),
    ]

    await txn_client.patch_collection(
        collection_id=collection_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )

    assert (
        updated_collection["summaries"]["foo"] == updated_collection["summaries"]["gsd"]
    )


@pytest.mark.asyncio
async def test_json_patch_collection_remove(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchRemove.model_validate({"op": "remove", "path": "/summaries/gsd"}),
    ]

    await txn_client.patch_collection(
        collection_id=collection_id,
        patch=operations,
        request=MockRequest(headers={"content-type": "application/json-patch+json"}),
    )

    updated_collection = await core_client.get_collection(
        collection_id, request=MockRequest
    )

    assert "gsd" not in updated_collection["summaries"]


@pytest.mark.asyncio
async def test_json_patch_collection_test_wrong_value(ctx, core_client, txn_client):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "test", "path": "/summaries/platform", "value": "landsat-9"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_collection(
            collection_id=collection_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_collection_replace_property_does_not_exists(
    ctx, core_client, txn_client
):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchAddReplaceTest.model_validate(
            {"op": "replace", "path": "/summaries/foo", "value": "landsat-9"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_collection(
            collection_id=collection_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_collection_remove_property_does_not_exists(
    ctx, core_client, txn_client
):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchRemove.model_validate({"op": "remove", "path": "/summaries/foo"}),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_collection(
            collection_id=collection_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_collection_move_property_does_not_exists(
    ctx, core_client, txn_client
):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "move", "path": "/summaries/bar", "from": "/summaries/foo"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_collection(
            collection_id=collection_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )


@pytest.mark.asyncio
async def test_json_patch_collection_copy_property_does_not_exists(
    ctx, core_client, txn_client
):
    collection = ctx.collection
    collection_id = collection["id"]
    operations = [
        PatchMoveCopy.model_validate(
            {"op": "copy", "path": "/summaries/bar", "from": "/summaries/foo"}
        ),
    ]

    with pytest.raises(HTTPException):

        await txn_client.patch_collection(
            collection_id=collection_id,
            patch=operations,
            request=MockRequest(
                headers={"content-type": "application/json-patch+json"}
            ),
        )
