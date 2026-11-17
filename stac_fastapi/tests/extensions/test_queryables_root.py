import json
import time
import uuid
from copy import deepcopy
from typing import Callable, Dict
from urllib.parse import quote

import pytest
from httpx import AsyncClient

import stac_fastapi.sfeos_helpers.filter.client as filter_client_module
from stac_fastapi.core.extensions.filter import DEFAULT_QUERYABLES
from stac_fastapi.sfeos_helpers.database import (
    index_alias_by_collection_id,
    resolve_aliases_shared,
)
from stac_fastapi.sfeos_helpers.filter import EsAsyncBaseFiltersClient
from stac_fastapi.sfeos_helpers.mappings import SHARED_ITEMS_INDEX

from ..conftest import delete_collections_and_items, refresh_indices


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


async def add_collections(app_client, test_collection, test_item, count=3):
    """Add collections whose items differ in platform (an enum) and extra fields."""
    collection_ids = [f"rebuild-{n}-{uuid.uuid4().hex[:8]}" for n in range(count)]
    for n, collection_id in enumerate(collection_ids):
        collection = deepcopy(test_collection)
        collection["id"] = collection_id
        r = await app_client.post("/collections", json=collection)
        assert r.status_code == 201, r.text
        item = deepcopy(test_item)
        item["id"] = f"item-{n}"
        item["collection"] = collection_id
        item["properties"]["platform"] = f"platform-{n}"
        item["properties"][f"field_{n}"] = n
        item["properties"]["hidden_field"] = "x"
        r = await app_client.post(f"/collections/{collection_id}/items", json=item)
        assert r.status_code == 201, r.text
    return collection_ids


def count_calls(monkeypatch, target, name):
    calls = []
    method = getattr(target, name)

    async def spy(*args, **kwargs):
        calls.append(kwargs)
        return await method(*args, **kwargs)

    monkeypatch.setattr(target, name, spy)
    return calls


def canonical(schemas):
    return sorted(json.dumps(s, sort_keys=True) for s in schemas)


@pytest.mark.asyncio
async def test_shared_rebuild_reads_the_shared_mapping_once(
    app_client, txn_client, shared_index_env, test_collection, test_item, monkeypatch
):
    collection_ids = await add_collections(app_client, test_collection, test_item)
    await refresh_indices(txn_client)
    database = txn_client.database
    get_mapping = count_calls(monkeypatch, database.client.indices, "get_mapping")
    resolve = count_calls(monkeypatch, database.client.indices, "resolve_index")
    search = count_calls(monkeypatch, database.client, "search")

    queryables = await database.get_all_collection_queryables()

    assert len(get_mapping) == 1
    assert get_mapping[0]["index"] == SHARED_ITEMS_INDEX
    assert len(resolve) == 1
    # One collection-ID scroll, then one enum aggregation per collection.
    assert len(search) == 1 + len(collection_ids)
    assert sorted(q["properties"]["platform"]["enum"][0] for q in queryables) == [
        "platform-0",
        "platform-1",
        "platform-2",
    ]

    item = deepcopy(test_item)
    item["id"] = "item-new-field"
    item["collection"] = collection_ids[0]
    item["properties"]["new_field"] = "x"
    r = await app_client.post(f"/collections/{collection_ids[0]}/items", json=item)
    assert r.status_code == 201, r.text
    get_mapping.clear()

    queryables = await database.get_all_collection_queryables()

    assert len(get_mapping) == 1
    assert all("new_field" in q["properties"] for q in queryables)


async def assert_rebuild_matches_per_collection_queryables(
    app_client, txn_client, test_collection, test_item, monkeypatch
):
    monkeypatch.setenv("EXCLUDED_FROM_QUERYABLES", "properties.hidden_field")
    collection_ids = await add_collections(app_client, test_collection, test_item)
    await refresh_indices(txn_client)
    database = txn_client.database
    filters_client = EsAsyncBaseFiltersClient(
        database=database, settings=database.async_settings
    )

    rebuilt = await database.get_all_collection_queryables()
    per_collection = [
        await filters_client.get_queryables(collection_id=cid) for cid in collection_ids
    ]

    assert canonical(rebuilt) == canonical(per_collection)
    assert all("hidden_field" not in q["properties"] for q in rebuilt)
    assert {tuple(q["properties"]["platform"]["enum"]) for q in rebuilt} == {
        ("platform-0",),
        ("platform-1",),
        ("platform-2",),
    }


@pytest.mark.asyncio
async def test_shared_rebuild_matches_per_collection_queryables(
    app_client, txn_client, shared_index_env, test_collection, test_item, monkeypatch
):
    await assert_rebuild_matches_per_collection_queryables(
        app_client, txn_client, test_collection, test_item, monkeypatch
    )


@pytest.mark.asyncio
async def test_separate_rebuild_matches_per_collection_queryables(
    app_client, txn_client, test_collection, test_item, monkeypatch
):
    await delete_collections_and_items(txn_client)
    try:
        await assert_rebuild_matches_per_collection_queryables(
            app_client, txn_client, test_collection, test_item, monkeypatch
        )
    finally:
        await delete_collections_and_items(txn_client)


@pytest.mark.asyncio
async def test_rebuild_skips_a_collection_without_its_alias(
    app_client, txn_client, shared_index_env, test_collection, test_item
):
    collection_ids = await add_collections(app_client, test_collection, test_item)
    await refresh_indices(txn_client)
    database = txn_client.database
    await database.client.indices.delete_alias(
        index=SHARED_ITEMS_INDEX, name=index_alias_by_collection_id(collection_ids[0])
    )

    queryables = await database.get_all_collection_queryables()

    assert sorted(q["properties"]["platform"]["enum"][0] for q in queryables) == [
        "platform-1",
        "platform-2",
    ]


@pytest.mark.asyncio
async def test_rebuild_reads_a_multi_index_alias_by_its_name(
    app_client, txn_client, test_collection, test_item, monkeypatch
):
    await delete_collections_and_items(txn_client)
    try:
        (collection_id,) = await add_collections(
            app_client, test_collection, test_item, count=1
        )
        database = txn_client.database
        alias = index_alias_by_collection_id(collection_id)
        await database.client.indices.create(index=f"{alias}_extra_partition")
        await database.client.indices.put_alias(
            index=f"{alias}_extra_partition", name=alias
        )
        get_mapping = count_calls(monkeypatch, database.client.indices, "get_mapping")

        queryables = await database.get_all_collection_queryables()

        assert [call["index"] for call in get_mapping] == [alias]
        assert len(queryables) == 1
    finally:
        await delete_collections_and_items(txn_client)


@pytest.mark.asyncio
async def test_rebuild_skips_a_collection_whose_index_has_no_mapping(
    app_client, txn_client, test_collection, test_item
):
    client = txn_client.database.client
    unmapped = f"unmapped-{uuid.uuid4().hex}"
    await delete_collections_and_items(txn_client)
    try:
        collection_ids = await add_collections(
            app_client, test_collection, test_item, count=2
        )
        await refresh_indices(txn_client)
        alias = index_alias_by_collection_id(collection_ids[0])
        await client.indices.create(index=unmapped)
        await client.indices.update_aliases(
            body={
                "actions": [
                    {"remove": {"index": f"{alias}*", "alias": alias}},
                    {"add": {"index": unmapped, "alias": alias}},
                ]
            }
        )

        queryables = await txn_client.database.get_all_collection_queryables()

        assert [q["properties"]["platform"]["enum"] for q in queryables] == [
            ["platform-1"]
        ]
    finally:
        await client.indices.delete(index=f"{unmapped}*")
        await delete_collections_and_items(txn_client)


class FakeIndices:
    def __init__(self, known):
        self.known = known
        self.calls = []

    async def resolve_index(self, name, **kwargs):
        self.calls.append((name, kwargs))
        names = name.split(",")
        return {
            "indices": [{"name": n} for n in names if n in self.known["indices"]],
            "aliases": [
                {"name": n, "indices": self.known["aliases"][n]}
                for n in names
                if n in self.known["aliases"]
            ],
        }


class FakeOpenSearch:
    def __init__(self, known):
        self.indices = FakeIndices(known)


class FakeElasticsearch(FakeOpenSearch):
    def options(self, **kwargs):
        return self


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client_class, expected_kwargs",
    [
        (FakeElasticsearch, {"ignore_unavailable": True}),
        (FakeOpenSearch, {"params": {"ignore_unavailable": "true"}}),
    ],
)
async def test_resolve_aliases_batches_and_drops_missing_names(
    client_class, expected_kwargs
):
    # Each "é" is six characters once percent-encoded into the URL.
    aliases = [f"items_collection_{n:064d}" for n in range(100)] + [
        f"items_{'é' * 40}{n:02d}" for n in range(20)
    ]
    client = client_class(
        {
            "indices": {"items_concrete"},
            "aliases": {alias: ["b", "a"] for alias in aliases[1:]},
        }
    )

    backing = await resolve_aliases_shared(
        client, aliases + ["items_concrete", aliases[1]]
    )

    assert backing == {
        **{alias: ("a", "b") for alias in aliases[1:]},
        "items_concrete": ("items_concrete",),
    }
    calls = client.indices.calls
    assert len(calls) == 5
    assert all(len(quote(name, safe=",")) <= 3000 for name, _ in calls)
    assert all(kwargs == expected_kwargs for _, kwargs in calls)
    assert [n for name, _ in calls for n in name.split(",")] == aliases + [
        "items_concrete"
    ]
