import copy
import os
import uuid
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient
from stac_pydantic import api

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

from ..conftest import (
    build_test_app,
    create_collection,
    delete_collections_and_items,
    refresh_indices,
)

CORE_COLLECTION_PROPS = [
    "id",
    "type",
    "stac_extensions",
    "stac_version",
    "title",
    "description",
    "keywords",
    "license",
    "providers",
    "summaries",
    "extent",
    "links",
    "assets",
]


@pytest.mark.asyncio
async def test_create_and_delete_collection(app_client, load_test_data):
    """Test creation and deletion of a collection"""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test"

    resp = await app_client.post("/collections", json=test_collection)
    assert resp.status_code == 201

    resp = await app_client.delete(f"/collections/{test_collection['id']}")
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_create_collection_transactions_extension(load_test_data):
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test"

    os.environ["ENABLE_TRANSACTIONS_EXTENSIONS"] = "false"
    app_disabled = build_test_app()
    async with AsyncClient(
        transport=ASGITransport(app=app_disabled), base_url="http://test"
    ) as client:
        resp = await client.post("/collections", json=test_collection)
        assert resp.status_code in (
            404,
            405,
            501,
        ), f"Expected failure, got {resp.status_code}"

    os.environ["ENABLE_TRANSACTIONS_EXTENSIONS"] = "true"
    app_enabled = build_test_app()
    async with AsyncClient(
        transport=ASGITransport(app=app_enabled), base_url="http://test"
    ) as client:
        resp = await client.post("/collections", json=test_collection)
        assert resp.status_code == 201
        resp = await client.delete(f"/collections/{test_collection['id']}")
        assert resp.status_code == 204

    del os.environ["ENABLE_TRANSACTIONS_EXTENSIONS"]


@pytest.mark.asyncio
async def test_create_collection_conflict(app_client, ctx):
    """Test creation of a collection which already exists"""
    # This collection ID is created in the fixture, so this should be a conflict
    resp = await app_client.post("/collections", json=ctx.collection)
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_delete_missing_collection(app_client):
    """Test deletion of a collection which does not exist"""
    resp = await app_client.delete("/collections/missing-collection")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_collection_already_exists(ctx, app_client, load_test_data):
    """Test updating a collection which already exists"""
    collection = load_test_data("test_collection.json")
    collection["keywords"].append("test")
    resp = await app_client.put(f"/collections/{ctx.collection['id']}", json=collection)
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{collection['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert "test" in resp_json["keywords"]


@pytest.mark.asyncio
async def test_update_new_collection(app_client, load_test_data):
    """Test updating a collection which does not exist (same as creation)"""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "new-test-collection"

    resp = await app_client.put(
        f"/collections/{test_collection['id']}", json=test_collection
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_collection_not_found(app_client):
    """Test read a collection which does not exist"""
    resp = await app_client.get("/collections/does-not-exist")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_returns_valid_collection(ctx, app_client):
    """Test validates fetched collection with jsonschema"""
    resp = await app_client.put(
        f"/collections/{ctx.collection['id']}", json=ctx.collection
    )
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{ctx.collection['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()

    assert resp_json == api.Collection(**resp_json).model_dump(mode="json")


@pytest.mark.asyncio
async def test_collection_extensions_post(ctx, app_client):
    """Test that extensions can be used to define additional top-level properties"""
    collection = ctx.collection
    collection.get("stac_extensions", []).append(
        "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json"
    )
    test_asset = {"title": "test", "description": "test", "type": "test"}
    ctx.collection["item_assets"] = {"test": test_asset}
    ctx.collection["id"] = "test-item-assets"
    resp = await app_client.post("/collections", json=ctx.collection)

    assert resp.status_code == 201
    assert resp.json().get("item_assets", {}).get("test") == test_asset


@pytest.mark.asyncio
async def test_collection_extensions_put(ctx, app_client):
    """Test that extensions can be used to define additional top-level properties"""
    ctx.collection.get("stac_extensions", []).append(
        "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json"
    )
    test_asset = {"title": "test", "description": "test", "type": "test"}
    ctx.collection["item_assets"] = {"test": test_asset}
    resp = await app_client.put(
        f"/collections/{ctx.collection['id']}", json=ctx.collection
    )

    assert resp.status_code == 200
    assert resp.json().get("item_assets", {}).get("test") == test_asset


@pytest.mark.skip(reason="stac pydantic in stac fastapi 3 doesn't allow this.")
@pytest.mark.asyncio
async def test_collection_defaults(app_client):
    """Test that properties omitted by client are populated w/ default values"""
    minimal_coll = {"id": str(uuid.uuid4())}
    resp = await app_client.post("/collections", json=minimal_coll)

    assert resp.status_code == 200
    resp_json = resp.json()
    for prop in CORE_COLLECTION_PROPS:
        assert prop in resp_json.keys()


@pytest.mark.asyncio
async def test_pagination_collection(app_client, ctx, txn_client):
    """Test collection pagination links"""

    # Clear existing collections if necessary
    await delete_collections_and_items(txn_client)

    # Ingest 6 collections
    ids = set()
    for _ in range(6):
        ctx.collection["id"] = str(uuid.uuid4())
        await create_collection(txn_client, collection=ctx.collection)
        ids.add(ctx.collection["id"])

    await refresh_indices(txn_client)

    # Paginate through all 6 collections with a limit of 1
    collection_ids = set()
    page = await app_client.get("/collections", params={"limit": 1})
    while True:
        page_data = page.json()
        assert (
            len(page_data["collections"]) <= 1
        )  # Each page should have 1 or 0 collections
        collection_ids.update(coll["id"] for coll in page_data["collections"])

        next_link = next(
            (link for link in page_data["links"] if link["rel"] == "next"), None
        )
        if not next_link:
            break  # No more pages

        href = next_link["href"][len("http://test-server") :]
        page = await app_client.get(href)

    # Confirm we have paginated through all collections
    assert collection_ids == ids


@pytest.mark.asyncio
async def test_links_collection(app_client, ctx, txn_client):
    await delete_collections_and_items(txn_client)
    collection = copy.deepcopy(ctx.collection)
    collection["links"].append(
        {"href": "https://landsat.usgs.gov/", "rel": "license", "type": "text/html"}
    )
    await create_collection(txn_client, collection=collection)
    response = await app_client.get(f"/collections/{collection['id']}")
    assert (
        len([link for link in response.json()["links"] if link["rel"] == "license"])
        == 1
    )


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.asyncio
async def test_patch_missing_collection_returns_404(app_client, monkeypatch, validator):
    """Test PATCH on a missing collection returns 404 instead of 500."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    resp = await app_client.patch(
        f"/collections/missing-collection-{uuid.uuid4()}",
        json={"title": "does not exist"},
        headers={"Content-Type": "application/merge-patch+json"},
    )
    assert resp.status_code == 404


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.asyncio
async def test_patch_collection_accepts_parameterised_media_type(
    app_client, ctx, monkeypatch, validator
):
    """Test an upper-case, parameterised patch media type is accepted."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    resp = await app_client.patch(
        f"/collections/{ctx.collection['id']}",
        json={"title": "cased"},
        headers={"Content-Type": "application/MERGE-PATCH+JSON; charset=utf-8"},
    )
    assert resp.status_code == 200
    assert resp.json()["title"] == "cased"


@pytest.mark.parametrize(
    "patch,content_type",
    [
        ([{"op": "replace", "path": "/id", "value": "renamed"}], "json-patch"),
        ([{"op": "add", "path": "collection", "value": "renamed"}], "json-patch"),
        ([{"op": "replace", "path": "collection", "value": "renamed"}], "json-patch"),
        ([{"op": "move", "from": "/id", "path": "/title"}], "json-patch"),
        ([{"op": "remove", "path": "/id"}], "json-patch"),
        ([{"op": "replace", "path": "", "value": {"id": "renamed"}}], "json-patch"),
        ({"id": "renamed"}, "merge-patch"),
    ],
)
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.asyncio
async def test_patch_collection_rejects_id_changes(
    app_client, ctx, txn_client, monkeypatch, validator, patch, content_type
):
    """Test a patch that changes the collection id returns 400 and changes nothing."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    before = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=ctx.collection["id"]
    )
    item_index = index_alias_by_collection_id(ctx.collection["id"])
    item_id = mk_item_id(ctx.item["id"], ctx.collection["id"])
    item_before = await txn_client.database.client.get(index=item_index, id=item_id)
    if isinstance(patch, list):
        patch = [{"op": "add", "path": "/title", "value": "must not persist"}] + patch
    else:
        patch = {**patch, "title": "must not persist"}
    lookup = AsyncMock(side_effect=AssertionError("Invalid identity reached lookup"))
    with monkeypatch.context() as guarded:
        guarded.setattr(type(txn_client.database), "find_collection", lookup)
        resp = await app_client.patch(
            f"/collections/{ctx.collection['id']}",
            json=patch,
            headers={"Content-Type": f"application/{content_type}+json"},
        )
    assert resp.status_code == 400
    lookup.assert_not_awaited()
    after = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=ctx.collection["id"]
    )
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]
    item_after = await txn_client.database.client.get(index=item_index, id=item_id)
    assert item_after["_source"] == item_before["_source"]
    assert item_after["_version"] == item_before["_version"]
    assert item_after["_source"]["collection"] == ctx.collection["id"]
    assert not await txn_client.database.client.exists(
        index=index_alias_by_collection_id("renamed"),
        id=mk_item_id(ctx.item["id"], "renamed"),
    )

    stored = await app_client.get(f"/collections/{ctx.collection['id']}")
    assert stored.status_code == 200
    assert stored.json()["id"] == ctx.collection["id"]
    assert (await app_client.get("/collections/renamed")).status_code == 404


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "patch,content_type",
    [
        ([{"op": "replace", "path": "/type", "value": "Catalog"}], "json-patch"),
        ([{"op": "add", "path": "/type", "value": "Catalog"}], "json-patch"),
        ([{"op": "replace", "path": "/type", "value": None}], "json-patch"),
        ([{"op": "remove", "path": "/type"}], "json-patch"),
        ([{"op": "copy", "from": "/description", "path": "/type"}], "json-patch"),
        ([{"op": "move", "from": "/description", "path": "/type"}], "json-patch"),
        ([{"op": "move", "from": "/type", "path": "/title"}], "json-patch"),
        ("replace-root-type", "json-patch"),
        ("remove-root-type", "json-patch"),
        ({"type": "Catalog"}, "merge-patch"),
        ({"type": None}, "merge-patch"),
    ],
)
@pytest.mark.asyncio
async def test_patch_collection_rejects_type_changes_before_writing(
    app_client, ctx, txn_client, monkeypatch, validator, patch, content_type
):
    """Reject type changes without persisting any operation in the request."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    collection_id = ctx.collection["id"]
    url = f"/collections/{collection_id}"
    before = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=collection_id
    )
    if isinstance(patch, str):
        replacement = copy.deepcopy(ctx.collection)
        if patch == "replace-root-type":
            replacement["type"] = "Catalog"
        else:
            del replacement["type"]
        patch = [{"op": "replace", "path": "", "value": replacement}]
    if isinstance(patch, list):
        patch = [{"op": "add", "path": "/title", "value": "must not persist"}] + patch
    else:
        patch = {**patch, "title": "must not persist"}

    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": f"application/{content_type}+json"}
    )

    assert response.status_code == 400
    after = await txn_client.database.client.get(
        index=COLLECTIONS_INDEX, id=collection_id
    )
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]
    readable = await app_client.get(url)
    assert readable.status_code == 200
    assert readable.json()["type"] == "Collection"


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "patch,content_type",
    [
        ([{"op": "replace", "path": "/type", "value": "Collection"}], "json-patch"),
        ([{"op": "add", "path": "/type", "value": "Collection"}], "json-patch"),
        ([{"op": "copy", "from": "/type", "path": "/title"}], "json-patch"),
        ({"type": "Collection"}, "merge-patch"),
    ],
)
@pytest.mark.asyncio
async def test_patch_collection_preserving_type_succeeds(
    app_client, ctx, monkeypatch, validator, patch, content_type
):
    """Allow unchanged type values and copies of type into unrelated metadata."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = f"/collections/{ctx.collection['id']}"
    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": f"application/{content_type}+json"}
    )
    assert response.status_code == 200
    stored = await app_client.get(url)
    assert stored.status_code == 200
    assert stored.json()["type"] == "Collection"
    if isinstance(patch, list) and patch[0]["op"] == "copy":
        assert stored.json()["title"] == "Collection"


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.asyncio
async def test_patch_collection_field_is_not_a_rename_alias(
    app_client, ctx, txn_client, monkeypatch, validator
):
    """A JSON Pointer to collection metadata does not rename the resource."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    collection_id = ctx.collection["id"]
    item_index = index_alias_by_collection_id(collection_id)
    item_id = mk_item_id(ctx.item["id"], collection_id)
    item_before = await txn_client.database.client.get(index=item_index, id=item_id)
    for operation, value in [("add", "custom metadata"), ("replace", "new metadata")]:
        response = await app_client.patch(
            f"/collections/{collection_id}",
            json=[{"op": operation, "path": "/collection", "value": value}],
            headers={"Content-Type": "application/json-patch+json"},
        )
        assert response.status_code == 200
        stored = await txn_client.database.client.get(
            index=COLLECTIONS_INDEX, id=collection_id
        )
        assert stored["_source"]["id"] == collection_id
        assert stored["_source"]["collection"] == value
    item_after = await txn_client.database.client.get(index=item_index, id=item_id)
    assert item_after["_source"] == item_before["_source"]
    assert item_after["_version"] == item_before["_version"]
