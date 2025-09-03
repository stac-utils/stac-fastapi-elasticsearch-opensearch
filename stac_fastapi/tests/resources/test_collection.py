import copy
import os
import uuid

import pytest
from httpx import AsyncClient
from stac_pydantic import api

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
    async with AsyncClient(app=app_disabled, base_url="http://test") as client:
        resp = await client.post("/collections", json=test_collection)
        assert resp.status_code in (
            404,
            405,
            501,
        ), f"Expected failure, got {resp.status_code}"

    os.environ["ENABLE_TRANSACTIONS_EXTENSIONS"] = "true"
    app_enabled = build_test_app()
    async with AsyncClient(app=app_enabled, base_url="http://test") as client:
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
