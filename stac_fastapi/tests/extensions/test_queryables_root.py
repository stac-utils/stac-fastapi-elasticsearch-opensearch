import json
import time
import uuid
from typing import Callable, Dict

import pytest
from httpx import AsyncClient

import stac_fastapi.sfeos_helpers.filter.client as filter_client_module
from stac_fastapi.core.extensions.filter import DEFAULT_QUERYABLES


@pytest.mark.asyncio
async def test_root_queryables_default(app_client: AsyncClient):
    """Test that the root queryables endpoint returns the default schema."""
    resp = await app_client.get("/queryables")
    assert resp.status_code == 200
    queryables = resp.json()
    assert queryables["properties"] == DEFAULT_QUERYABLES


@pytest.mark.asyncio
async def test_root_queryables_config_file(
    app_client: AsyncClient, monkeypatch: pytest.MonkeyPatch, tmp_path
):
    """Test that the root queryables endpoint returns the config file contents if defined."""
    # Create a dummy config file
    config_file = tmp_path / "queryables_config.json"
    dummy_queryables = {"dummy_field": {"title": "Dummy Field", "type": "string"}}
    config_data = {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "$id": "https://example.com/queryables.json",
        "type": "object",
        "title": "Queryables config",
        "properties": dummy_queryables,
        "additionalProperties": False,
    }
    with open(config_file, "w") as f:
        json.dump(config_data, f)

    monkeypatch.setenv("STAC_QUERYABLES_CONFIG", str(config_file))

    resp = await app_client.get("/queryables")
    assert resp.status_code == 200
    queryables = resp.json()
    assert queryables["properties"] == dummy_queryables


@pytest.mark.asyncio
async def test_root_queryables_union(
    app_client: AsyncClient,
    load_test_data: Callable[[str], Dict],
    monkeypatch: pytest.MonkeyPatch,
):
    """Test that the root queryables endpoint returns merged queryables when union flag is True.

    Queryables are now generated on-the-fly from item index mappings, so we need to add items
    with custom fields to each collection to test the union behavior.
    """
    monkeypatch.setenv("ROOT_QUERYABLES_UNION", "true")
    monkeypatch.setenv("DATABASE_REFRESH", "true")
    # Small TTL to prevent cache from test collision
    monkeypatch.setenv("QUERYABLES_CACHE_TTL", "0")

    # Create two collections with distinct custom fields
    collection_1 = load_test_data("test_collection.json")
    collection_1["id"] = f"union-test-1-{uuid.uuid4()}"
    r = await app_client.post("/collections", json=collection_1)
    r.raise_for_status()

    collection_2 = load_test_data("test_collection.json")
    collection_2["id"] = f"union-test-2-{uuid.uuid4()}"
    r = await app_client.post("/collections", json=collection_2)
    r.raise_for_status()

    # Add items with custom fields to each collection so they appear in queryables
    item_1 = load_test_data("test_item.json")
    item_1["id"] = f"item-1-{uuid.uuid4()}"
    item_1["collection"] = collection_1["id"]
    item_1["properties"]["field1"] = "test_value"
    r = await app_client.post(f"/collections/{collection_1['id']}/items", json=item_1)
    r.raise_for_status()

    item_2 = load_test_data("test_item.json")
    item_2["id"] = f"item-2-{uuid.uuid4()}"
    item_2["collection"] = collection_2["id"]
    item_2["properties"]["field2"] = 42
    r = await app_client.post(f"/collections/{collection_2['id']}/items", json=item_2)
    r.raise_for_status()

    # Act
    resp = await app_client.get("/queryables")
    assert resp.status_code == 200
    queryables = resp.json()

    # Assert
    properties = queryables.get("properties", {})

    # Both field1 and field2 should be present in the properties alongside baseline queryables
    assert "field1" in properties
    assert "field2" in properties
    assert properties["field1"]["type"] == "string"
    assert properties["field2"]["type"] == "number"

    # Baseline queryable id should also be there
    assert "id" in properties

    # Clean up
    r = await app_client.delete(f"/collections/{collection_1['id']}")
    r.raise_for_status()
    r = await app_client.delete(f"/collections/{collection_2['id']}")
    r.raise_for_status()


@pytest.mark.asyncio
async def test_root_queryables_union_served_from_cache(
    app_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
):
    """Test that root /queryables union result is served from cache within TTL.

    Seeds the global cache with a known sentinel value and a fresh timestamp,
    then verifies the endpoint returns that value without hitting the database.
    """
    monkeypatch.setenv("ROOT_QUERYABLES_UNION", "true")
    monkeypatch.setenv("QUERYABLES_CACHE_TTL", "3600")

    sentinel = {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "$id": "https://example.com/queryables",
        "type": "object",
        "title": "Queryables",
        "properties": {
            "cached_sentinel_field": {
                "type": "string",
                "title": "Cached Sentinel Field",
            }
        },
        "additionalProperties": False,
    }
    monkeypatch.setattr(filter_client_module, "_GLOBAL_QUERYABLES_CACHE", sentinel)
    monkeypatch.setattr(
        filter_client_module, "_GLOBAL_QUERYABLES_LAST_UPDATED", time.time()
    )

    resp = await app_client.get("/queryables")
    assert resp.status_code == 200
    assert resp.json() == sentinel


@pytest.mark.asyncio
async def test_root_queryables_union_cache_refreshed_after_ttl(
    app_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
):
    """Test that the cache is bypassed and refreshed after TTL expires."""
    monkeypatch.setenv("ROOT_QUERYABLES_UNION", "true")
    monkeypatch.setenv("QUERYABLES_CACHE_TTL", "3600")

    stale_sentinel = {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "$id": "https://example.com/queryables",
        "type": "object",
        "title": "Queryables",
        "properties": {"stale_field": {"type": "string"}},
        "additionalProperties": False,
    }
    # Seed an expired cache entry
    monkeypatch.setattr(
        filter_client_module, "_GLOBAL_QUERYABLES_CACHE", stale_sentinel
    )
    monkeypatch.setattr(filter_client_module, "_GLOBAL_QUERYABLES_LAST_UPDATED", 0.0)

    resp = await app_client.get("/queryables")
    assert resp.status_code == 200
    # The stale sentinel should NOT be returned because the TTL has expired
    assert "stale_field" not in resp.json().get("properties", {})
