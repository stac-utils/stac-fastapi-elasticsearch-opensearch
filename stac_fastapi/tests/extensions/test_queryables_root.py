import json
import uuid
from typing import Callable, Dict

import pytest
from httpx import AsyncClient

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
    """Test that the root queryables endpoint returns merged queryables when union flag is True."""
    monkeypatch.setenv("ROOT_QUERYABLES_UNION", "true")
    monkeypatch.setenv("DATABASE_REFRESH", "true")
    # Small TTL to prevent cache from test collision
    monkeypatch.setenv("QUERYABLES_CACHE_TTL", "0")

    # Create two collections with distinct queryables
    collection_1 = load_test_data("test_collection.json")
    collection_1["id"] = f"union-test-1-{uuid.uuid4()}"
    # Setting an arbitrary queryables dict in the collection JSON (simulating database extraction logic)
    collection_1["queryables"] = {"properties": {"field1": {"type": "string"}}}
    r = await app_client.post("/collections", json=collection_1)
    r.raise_for_status()

    collection_2 = load_test_data("test_collection.json")
    collection_2["id"] = f"union-test-2-{uuid.uuid4()}"
    collection_2["queryables"] = {"properties": {"field2": {"type": "number"}}}
    r = await app_client.post("/collections", json=collection_2)
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

    # Baseline queryable id should also be there
    assert "id" in properties

    # Clean up
    r = await app_client.delete(f"/collections/{collection_1['id']}")
    r.raise_for_status()
    r = await app_client.delete(f"/collections/{collection_2['id']}")
    r.raise_for_status()
