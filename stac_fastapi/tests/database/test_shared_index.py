"""ITEMS_INDEX_STRATEGY=shared: every collection's items in one index."""

import hashlib
import os
from copy import deepcopy

import pytest
from fastapi import HTTPException

from stac_fastapi.sfeos_helpers.database import ensure_generation_mappings
from stac_fastapi.sfeos_helpers.database import index as index_module
from stac_fastapi.sfeos_helpers.database import (
    index_alias_by_collection_id,
    reconcile_shared_alias,
    shared_alias_filter,
)
from stac_fastapi.sfeos_helpers.database.utils import ConnectionTimeout
from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_ITEMS_MAPPINGS,
    ES_ITEMS_SETTINGS,
    SHARED_ITEMS_INDEX,
)
from stac_fastapi.sfeos_helpers.search_engine import IndexInsertionFactory
from stac_fastapi.sfeos_helpers.search_engine import factory as factory_module
from stac_fastapi.types.errors import ConflictError

from ..conftest import rebuilt_index_strategy

OPENSEARCH = os.getenv("BACKEND", "elasticsearch").lower() == "opensearch"
CANONICAL_ID = "fao-gismgr:ASIS:raster:mapsets:ASI-D"
MERGE_PATCH = {"Content-Type": "application/merge-patch+json"}


def injected_timeout():
    # The retry matches whichever client's class utils imported; their
    # constructors differ.
    try:
        return ConnectionTimeout("TIMEOUT", "injected timeout", None)
    except TypeError:
        return ConnectionTimeout("injected timeout")


def collection_with(test_collection, collection_id):
    collection = deepcopy(test_collection)
    collection["id"] = collection_id
    return collection


def item_with(test_item, collection_id, item_id="shared-item", title=None):
    item = deepcopy(test_item)
    item["id"] = item_id
    item["collection"] = collection_id
    item["properties"]["title"] = title or collection_id
    return item


async def add_collection(app_client, test_collection, test_item, collection_id):
    response = await app_client.post(
        "/collections", json=collection_with(test_collection, collection_id)
    )
    assert response.status_code == 201, response.text
    response = await app_client.post(
        f"/collections/{collection_id}/items",
        json=item_with(test_item, collection_id),
    )
    assert response.status_code == 201, response.text


async def count(client, collection_id):
    await client.indices.refresh(index=SHARED_ITEMS_INDEX)
    response = await client.count(
        index=SHARED_ITEMS_INDEX,
        body={"query": {"term": {"collection": collection_id}}},
    )
    return response["count"]


async def collection_exists(client, collection_id):
    return bool(await client.exists(index=COLLECTIONS_INDEX, id=collection_id))


async def stored_generation(client, collection_id):
    doc = await client.get(index=COLLECTIONS_INDEX, id=collection_id)
    return doc["_source"]["collection_generation"]


async def alias_filter(client, collection_id):
    alias = index_alias_by_collection_id(collection_id)
    aliases = await client.indices.get_alias(name=alias)
    return aliases[SHARED_ITEMS_INDEX]["aliases"][alias]["filter"]


async def touch_collection(client, collection_id):
    doc = await client.get(index=COLLECTIONS_INDEX, id=collection_id)
    source = {**doc["_source"], "title": f"changed {doc['_seq_no']}"}
    await client.index(
        index=COLLECTIONS_INDEX, id=collection_id, body=source, refresh=True
    )


@pytest.fixture
def database(txn_client, shared_index_env):
    return txn_client.database


@pytest.fixture(
    params=[
        ("shared-a", "shared-b"),
        ("a:b", "ab"),
        (CANONICAL_ID, CANONICAL_ID + "-2"),
    ]
)
def collection_ids(request):
    return request.param


def test_alias_strategy_changes_preserve_public_ids(monkeypatch):
    for strategy in ("shared", "simple", "datetime", "shared"):
        monkeypatch.setenv("ITEMS_INDEX_STRATEGY", strategy)
        expected = (
            "items_collection_" + hashlib.sha256(CANONICAL_ID.encode()).hexdigest()
            if strategy == "shared"
            else "items_" + CANONICAL_ID.replace(":", "")
        )
        assert index_alias_by_collection_id(CANONICAL_ID) == expected
        assert index_alias_by_collection_id("*") == "items_*"


@pytest.mark.asyncio
async def test_collections_share_one_index_with_filtered_aliases(
    app_client, database, test_collection, test_item, collection_ids
):
    for collection_id in collection_ids:
        await add_collection(app_client, test_collection, test_item, collection_id)

    client = database.client
    assert set(await client.indices.get(index="items_*")) == {SHARED_ITEMS_INDEX}
    for collection_id in collection_ids:
        generation = await stored_generation(client, collection_id)
        assert await alias_filter(client, collection_id) == shared_alias_filter(
            collection_id, generation
        )


@pytest.mark.asyncio
async def test_same_item_id_in_two_collections_stays_separate(
    app_client, database, test_collection, test_item, collection_ids
):
    a, b = collection_ids
    for collection_id in collection_ids:
        await add_collection(app_client, test_collection, test_item, collection_id)
    url_a = f"/collections/{a}/items/shared-item"
    url_b = f"/collections/{b}/items/shared-item"

    assert (await app_client.get(url_a)).json()["properties"]["title"] == a

    replacement = item_with(test_item, a, title="put")
    assert (await app_client.put(url_a, json=replacement)).status_code == 200
    response = await app_client.patch(
        url_a, json={"properties": {"title": "patched"}}, headers=MERGE_PATCH
    )
    assert response.status_code == 200, response.text
    assert (await app_client.get(url_a)).json()["properties"]["title"] == "patched"
    assert (await app_client.get(url_b)).json()["properties"]["title"] == b

    assert (await app_client.delete(url_a)).status_code == 204
    assert (await app_client.get(url_a)).status_code == 404
    assert (await app_client.get(url_b)).status_code == 200


@pytest.mark.asyncio
async def test_listing_and_search_are_scoped_and_bulk_lands_in_its_collection(
    app_client, database, test_collection, test_item, collection_ids
):
    a, b = collection_ids
    for collection_id in collection_ids:
        await add_collection(app_client, test_collection, test_item, collection_id)
    bulk = {
        "type": "FeatureCollection",
        "features": [item_with(test_item, b, f"bulk-{n}", "bulk") for n in range(3)],
    }
    response = await app_client.post(f"/collections/{b}/items", json=bulk)
    assert response.status_code == 201, response.text

    listing = (await app_client.get(f"/collections/{a}/items")).json()
    assert [f["id"] for f in listing["features"]] == ["shared-item"]
    assert (
        len((await app_client.get(f"/collections/{b}/items")).json()["features"]) == 4
    )

    scoped = await app_client.post("/search", json={"collections": [a]})
    assert [f["collection"] for f in scoped.json()["features"]] == [a]

    everything = await app_client.post("/search", json={"ids": ["shared-item"]})
    assert sorted(f["collection"] for f in everything.json()["features"]) == sorted(
        collection_ids
    )
    queryables = await app_client.get(f"/collections/{a}/queryables")
    assert queryables.status_code == 200, queryables.text
    assert "title" in queryables.json()["properties"]
    page = await app_client.get(f"/collections/{b}/items", params={"limit": 2})
    next_link = next(link for link in page.json()["links"] if link["rel"] == "next")
    next_page = await app_client.get(next_link["href"])
    features = page.json()["features"] + next_page.json()["features"]
    assert len({feature["id"] for feature in features}) == 4
    assert {feature["collection"] for feature in features} == {b}
    assert await count(database.client, a) == 1
    assert await count(database.client, b) == 4
    response = await app_client.post(
        f"/collections/{a}/items",
        json={
            "type": "FeatureCollection",
            "features": [item_with(test_item, a, "bulk-a")],
        },
    )
    assert response.status_code == 201, response.text
    assert (await app_client.get(f"/collections/{a}/items/bulk-a")).json()[
        "collection"
    ] == a
    assert await count(database.client, a) == 2


@pytest.mark.asyncio
async def test_delete_removes_unrefreshed_items_and_keeps_other_collections(
    app_client, database, test_collection, test_item, collection_ids
):
    a, b = collection_ids
    for collection_id in collection_ids:
        await add_collection(app_client, test_collection, test_item, collection_id)
    indices = database.client.indices
    prior = await indices.get_settings(
        index=SHARED_ITEMS_INDEX, name="index.refresh_interval"
    )
    interval = prior.get(SHARED_ITEMS_INDEX, {}).get("settings", {}).get("index", {})
    await indices.put_settings(
        index=SHARED_ITEMS_INDEX, body={"refresh_interval": "-1"}
    )
    try:
        # A plain write: create_item's existence lookup would refresh the index.
        await database.client.index(
            index=index_alias_by_collection_id(a),
            id=f"unrefreshed|{a}",
            body={
                **item_with(test_item, a, "unrefreshed"),
                "collection_generation": await stored_generation(database.client, a),
            },
            refresh="false",
        )

        assert (await app_client.delete(f"/collections/{a}")).status_code == 204
    finally:
        await indices.put_settings(
            index=SHARED_ITEMS_INDEX,
            body={"refresh_interval": interval.get("refresh_interval")},
        )

    assert await count(database.client, a) == 0
    assert await count(database.client, b) == 1
    assert not await indices.exists_alias(name=index_alias_by_collection_id(a))
    await add_collection(app_client, test_collection, test_item, a)
    assert await count(database.client, a) == 1
    assert await count(database.client, b) == 1


@pytest.mark.asyncio
async def test_delete_removes_a_legacy_per_collection_index(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-b")
    client = database.client
    await client.index(
        index=COLLECTIONS_INDEX,
        id="legacy",
        body=collection_with(test_collection, "legacy"),
        refresh=True,
    )
    await client.indices.create(
        index="items_legacy_old", body={"aliases": {"items_legacy": {}}}
    )

    assert (await app_client.delete("/collections/legacy")).status_code == 204

    assert not await client.indices.exists(index="items_legacy_old")
    assert await count(client, "shared-b") == 1


@pytest.mark.asyncio
async def test_change_during_legacy_cleanup_keeps_the_index(
    app_client, database, monkeypatch, test_collection, test_item
):
    client = database.client
    await client.index(
        index=COLLECTIONS_INDEX,
        id="legacy",
        body=collection_with(test_collection, "legacy"),
        refresh=True,
    )
    await client.indices.create(
        index="items_legacy_old", body={"aliases": {"items_legacy": {}}}
    )
    indices_class = type(client.indices)
    delete_alias = indices_class.delete_alias

    async def delete_alias_then_change(self, *args, **kwargs):
        response = await delete_alias(self, *args, **kwargs)
        await touch_collection(client, "legacy")
        return response

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "delete_alias", delete_alias_then_change)
        try:
            await database.delete_collection("legacy")
        except ConflictError:
            pass

    if await collection_exists(client, "legacy"):
        assert await client.indices.exists(index="items_legacy_old")


@pytest.mark.asyncio
async def test_delete_succeeds_when_the_shared_index_is_missing(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    await database.client.indices.delete(index=SHARED_ITEMS_INDEX)

    assert (await app_client.delete("/collections/shared-a")).status_code == 204
    assert not await collection_exists(database.client, "shared-a")


@pytest.mark.asyncio
async def test_stale_expected_version_is_a_conflict(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    _, old = await database.find_collection_with_version("shared-a")
    await touch_collection(database.client, "shared-a")

    with pytest.raises(ConflictError):
        await database.delete_collection("shared-a", pinned=old)

    alias = index_alias_by_collection_id("shared-a")
    assert await database.client.indices.exists_alias(name=alias)
    assert await count(database.client, "shared-a") == 1


@pytest.mark.asyncio
async def test_change_before_cleanup_starts_is_a_conflict(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    indices_class = type(client.indices)
    resolve_index = indices_class.resolve_index

    async def resolve_then_change(self, *args, **kwargs):
        resolved = await resolve_index(self, *args, **kwargs)
        await touch_collection(client, "shared-a")
        return resolved

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "resolve_index", resolve_then_change)
        with pytest.raises(ConflictError):
            await database.delete_collection("shared-a")

    alias = index_alias_by_collection_id("shared-a")
    assert await client.indices.exists_alias(name=alias)
    assert await count(client, "shared-a") == 1


@pytest.mark.asyncio
async def test_change_before_delete_by_query_is_a_conflict(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    indices_class = type(client.indices)
    delete_alias = indices_class.delete_alias

    async def delete_alias_then_change(self, *args, **kwargs):
        response = await delete_alias(self, *args, **kwargs)
        await touch_collection(client, "shared-a")
        return response

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "delete_alias", delete_alias_then_change)
        with pytest.raises(ConflictError):
            await database.delete_collection("shared-a")

    assert await count(client, "shared-a") == 1
    assert await collection_exists(client, "shared-a")
    generation = await stored_generation(client, "shared-a")
    assert await alias_filter(client, "shared-a") == shared_alias_filter(
        "shared-a", generation
    )


@pytest.mark.asyncio
async def test_overlapping_delete_keeps_the_items_of_a_recreated_collection(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    delete_by_query = type(client).delete_by_query
    paused = False

    async def overlap_first_call(self, *args, **kwargs):
        # Delete A passed its last version check; B, a re-create and a new item
        # all finish before A's delete_by_query runs.
        nonlocal paused
        if not paused:
            paused = True
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            await add_collection(app_client, test_collection, test_item, "shared-a")
        return await delete_by_query(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete_by_query", overlap_first_call)
        response = await app_client.delete("/collections/shared-a")

    assert paused
    assert response.status_code == 409, response.text
    assert await collection_exists(client, "shared-a")
    assert await count(client, "shared-a") == 1
    item = await app_client.get("/collections/shared-a/items/shared-item")
    assert item.status_code == 200, item.text


@pytest.mark.asyncio
async def test_overlapping_delete_restores_the_alias_of_a_recreated_collection(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    indices_class = type(client.indices)
    delete_alias = indices_class.delete_alias
    paused = False

    async def overlap_first_call(self, *args, **kwargs):
        # Delete A checked the alias owner; its delete_alias now removes the
        # alias of the collection created again meanwhile.
        nonlocal paused
        if not paused:
            paused = True
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            await add_collection(app_client, test_collection, test_item, "shared-a")
        return await delete_alias(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "delete_alias", overlap_first_call)
        response = await app_client.delete("/collections/shared-a")

    assert paused
    assert response.status_code == 409, response.text
    generation = await stored_generation(client, "shared-a")
    assert await alias_filter(client, "shared-a") == shared_alias_filter(
        "shared-a", generation
    )
    item = await app_client.get("/collections/shared-a/items/shared-item")
    assert item.status_code == 200, item.text


@pytest.mark.asyncio
async def test_change_after_cleanup_restores_the_alias_with_its_generation(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    generation = await stored_generation(client, "shared-a")
    delete_by_query = type(client).delete_by_query

    async def delete_then_put(self, *args, **kwargs):
        response = await delete_by_query(self, *args, **kwargs)
        # After the cleanup's last check, so the final document delete conflicts.
        put = await app_client.put(
            "/collections/shared-a",
            json={**collection_with(test_collection, "shared-a"), "title": "put"},
        )
        assert put.status_code == 200, put.text
        return response

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete_by_query", delete_then_put)
        response = await app_client.delete("/collections/shared-a")

    assert response.status_code == 409, response.text
    assert await stored_generation(client, "shared-a") == generation
    assert await alias_filter(client, "shared-a") == shared_alias_filter(
        "shared-a", generation
    )


@pytest.mark.asyncio
async def test_create_paused_before_its_alias_ends_on_the_recreated_generation(
    app_client, database, monkeypatch, test_collection, test_item
):
    client = database.client
    indices_class = type(client.indices)
    put_alias = indices_class.put_alias
    paused = False

    async def recreate_before_first_put(self, *args, **kwargs):
        nonlocal paused
        if not paused:
            paused = True
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            response = await app_client.post(
                "/collections", json=collection_with(test_collection, "shared-a")
            )
            assert response.status_code == 201, response.text
        return await put_alias(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "put_alias", recreate_before_first_put)
        response = await app_client.post(
            "/collections", json=collection_with(test_collection, "shared-a")
        )

    assert paused
    assert response.status_code == 201, response.text
    generation = await stored_generation(client, "shared-a")
    assert await alias_filter(client, "shared-a") == shared_alias_filter(
        "shared-a", generation
    )
    response = await app_client.post(
        "/collections/shared-a/items", json=item_with(test_item, "shared-a")
    )
    assert response.status_code == 201, response.text
    item = await app_client.get("/collections/shared-a/items/shared-item")
    assert item.status_code == 200, item.text


@pytest.mark.asyncio
async def test_restore_paused_before_its_alias_ends_on_the_recreated_generation(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    indices_class = type(client.indices)
    delete_alias = indices_class.delete_alias
    put_alias = indices_class.put_alias
    touched = paused = False

    async def delete_alias_then_change(self, *args, **kwargs):
        nonlocal touched
        response = await delete_alias(self, *args, **kwargs)
        if not touched:
            touched = True
            await touch_collection(client, "shared-a")
        return response

    async def recreate_before_first_put(self, *args, **kwargs):
        # The 409's restore read the old generation; the id is deleted,
        # created again and given an item before its put lands.
        nonlocal paused
        if not paused:
            paused = True
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            await add_collection(app_client, test_collection, test_item, "shared-a")
        return await put_alias(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "delete_alias", delete_alias_then_change)
        patch.setattr(indices_class, "put_alias", recreate_before_first_put)
        response = await app_client.delete("/collections/shared-a")

    assert touched and paused
    assert response.status_code == 409, response.text
    generation = await stored_generation(client, "shared-a")
    assert await alias_filter(client, "shared-a") == shared_alias_filter(
        "shared-a", generation
    )
    item = await app_client.get("/collections/shared-a/items/shared-item")
    assert item.status_code == 200, item.text


@pytest.mark.asyncio
async def test_timeout_after_the_alias_is_removed_restores_it(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    delete_by_query = type(client).delete_by_query
    injected = False

    async def change_then_time_out(self, *args, **kwargs):
        # A replay cannot see the alias the first attempt removed.
        nonlocal injected
        if not injected:
            injected = True
            await touch_collection(client, "shared-a")
            raise injected_timeout()
        return await delete_by_query(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete_by_query", change_then_time_out)
        response = await app_client.delete("/collections/shared-a")

    assert injected
    assert response.status_code == 409, response.text
    alias = index_alias_by_collection_id("shared-a")
    assert await client.indices.exists_alias(name=alias)
    assert await count(client, "shared-a") == 1


@pytest.mark.asyncio
async def test_alias_restored_during_cleanup_is_not_deleted_as_an_index(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    delete_by_query = type(client).delete_by_query

    async def delete_then_restore(self, *args, **kwargs):
        response = await delete_by_query(self, *args, **kwargs)
        # The 409 of an overlapping delete puts the alias back.
        await reconcile_shared_alias(client, "shared-a")
        return response

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete_by_query", delete_then_restore)
        response = await app_client.delete("/collections/shared-a")

    assert response.status_code == 409, response.text
    assert await collection_exists(client, "shared-a")
    assert await client.indices.exists_alias(
        name=index_alias_by_collection_id("shared-a")
    )


@pytest.mark.asyncio
async def test_stale_generation_item_is_cleared_by_an_item_delete(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-b")
    response = await app_client.post(
        "/collections", json=collection_with(test_collection, "shared-a")
    )
    assert response.status_code == 201, response.text
    client = database.client
    # Left by an earlier creation of the id, e.g. a write queued before its delete.
    await client.index(
        index=SHARED_ITEMS_INDEX,
        id="shared-item|shared-a",
        body={**item_with(test_item, "shared-a"), "collection_generation": "stale"},
        refresh=True,
    )
    url = "/collections/shared-a/items/shared-item"
    assert (await app_client.get(url)).status_code == 404
    response = await app_client.post(
        "/collections/shared-a/items", json=item_with(test_item, "shared-a")
    )
    assert response.status_code == 409, response.text

    assert (await app_client.delete(url)).status_code == 204
    response = await app_client.post(
        "/collections/shared-a/items", json=item_with(test_item, "shared-a")
    )
    assert response.status_code == 201, response.text
    assert (await app_client.get(url)).status_code == 200


async def index_stale(client, test_item, item_id, generation=None):
    # Left by an earlier creation of the id, e.g. a write queued before its delete.
    body = item_with(test_item, "shared-a", item_id)
    if generation:
        body["collection_generation"] = generation
    await client.index(
        index=SHARED_ITEMS_INDEX, id=f"{item_id}|shared-a", body=body, refresh=True
    )


def is_discovery(kwargs):
    body = kwargs.get("body") or {}
    return kwargs.get("index") == SHARED_ITEMS_INDEX and "aggs" in body


@pytest.mark.asyncio
async def test_collection_delete_removes_items_left_by_earlier_generations(
    app_client, database, test_collection, test_item
):
    for collection_id in ("shared-a", "shared-b"):
        await add_collection(app_client, test_collection, test_item, collection_id)
    client = database.client
    await index_stale(client, test_item, "stale", "stale")
    await index_stale(client, test_item, "untagged")
    search = {"ids": ["stale", "untagged"]}
    response = await app_client.post("/search", json=search)
    assert len(response.json()["features"]) == 2, response.text

    assert (await app_client.delete("/collections/shared-a")).status_code == 204

    assert await count(client, "shared-a") == 0
    assert await count(client, "shared-b") == 1
    response = await app_client.post("/search", json=search)
    assert response.json()["features"] == [], response.text


@pytest.mark.asyncio
async def test_collection_delete_removes_more_generations_than_one_batch(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    await index_stale(client, test_item, "stale-1", "stale-1")
    await index_stale(client, test_item, "stale-2", "stale-2")
    monkeypatch.setattr(index_module, "GENERATION_DISCOVERY_BATCH_SIZE", 1)

    assert (await app_client.delete("/collections/shared-a")).status_code == 204

    assert await count(client, "shared-a") == 0


PARTIAL_DISCOVERY = {
    "_shards": {"total": 1, "successful": 0, "failed": 1},
    "aggregations": {"generations": {"buckets": [], "sum_other_doc_count": 0}},
}
ALL_SHARDS = {"total": 1, "successful": 1, "failed": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "partial, touch",
    [
        (PARTIAL_DISCOVERY, False),
        ({**PARTIAL_DISCOVERY, "timed_out": True, "_shards": ALL_SHARDS}, False),
        # An unavailable OpenSearch shard is neither successful nor failed.
        ({**PARTIAL_DISCOVERY, "_shards": {**ALL_SHARDS, "total": 2}}, False),
        (PARTIAL_DISCOVERY, True),
    ],
)
async def test_incomplete_discovery_keeps_the_collection(
    app_client, database, monkeypatch, test_collection, test_item, partial, touch
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    search = type(client).search
    injected = False

    async def partial_first_discovery(self, *args, **kwargs):
        nonlocal injected
        if not injected and is_discovery(kwargs):
            injected = True
            if touch:
                await touch_collection(client, "shared-a")
            return partial
        return await search(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "search", partial_first_discovery)
        response = await app_client.delete("/collections/shared-a")

    assert injected
    assert await count(client, "shared-a") == 1
    if touch:
        assert response.status_code == 409, response.text
        generation = await stored_generation(client, "shared-a")
        assert await alias_filter(client, "shared-a") == shared_alias_filter(
            "shared-a", generation
        )
        return
    assert response.status_code == 500, response.text
    assert "retry DELETE" in response.json()["detail"]
    assert await collection_exists(client, "shared-a")
    assert (await app_client.delete("/collections/shared-a")).status_code == 204
    assert await count(client, "shared-a") == 0


@pytest.mark.asyncio
async def test_recreate_before_discovery_keeps_the_new_generation(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    search = type(client).search
    paused = False

    async def recreate_before_first_discovery(self, *args, **kwargs):
        # The outer delete removed the alias; the id is deleted, created again
        # and given an item before its discovery search runs.
        nonlocal paused
        if not paused and is_discovery(kwargs):
            paused = True
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            await add_collection(app_client, test_collection, test_item, "shared-a")
        return await search(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "search", recreate_before_first_discovery)
        response = await app_client.delete("/collections/shared-a")

    assert paused
    assert response.status_code == 409, response.text
    assert await count(client, "shared-a") == 1
    item = await app_client.get("/collections/shared-a/items/shared-item")
    assert item.status_code == 200, item.text
    generation = await stored_generation(client, "shared-a")
    assert await alias_filter(client, "shared-a") == shared_alias_filter(
        "shared-a", generation
    )


async def rebuild_collections_index(client):
    name = f"{COLLECTIONS_INDEX}-000001"
    await client.indices.delete(index=name)
    await client.indices.create(index=name, body={"aliases": {COLLECTIONS_INDEX: {}}})
    await ensure_generation_mappings(client)


@pytest.mark.asyncio
async def test_recreate_on_a_rebuilt_collections_index_keeps_the_new_generation(
    app_client, database, monkeypatch, test_collection, test_item
):
    client = database.client
    await rebuild_collections_index(client)
    await add_collection(app_client, test_collection, test_item, "shared-a")
    search = type(client).search
    paused = False

    async def rebuild_before_first_discovery(self, *args, **kwargs):
        # The re-created collection is the first document of a fresh index of the
        # same name, so it repeats the version the delete started from.
        nonlocal paused
        if not paused and is_discovery(kwargs):
            paused = True
            await rebuild_collections_index(client)
            response = await app_client.post(
                "/collections", json=collection_with(test_collection, "shared-a")
            )
            assert response.status_code == 201, response.text
            response = await app_client.post(
                "/collections/shared-a/items",
                json=item_with(test_item, "shared-a", "new-item"),
            )
            assert response.status_code == 201, response.text
        return await search(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "search", rebuild_before_first_discovery)
        response = await app_client.delete("/collections/shared-a")

    assert paused
    assert response.status_code == 409, response.text
    item = await app_client.get("/collections/shared-a/items/new-item")
    assert item.status_code == 200, item.text


@pytest.mark.asyncio
async def test_delete_without_generation_doc_values_falls_back(
    app_client, database, test_collection, test_item
):
    client = database.client
    mappings = deepcopy(ES_ITEMS_MAPPINGS)
    mappings["properties"]["collection_generation"] = {
        "type": "keyword",
        "doc_values": False,
    }
    # A hand-created shared index whose generations can't be aggregated.
    await client.indices.create(
        index=SHARED_ITEMS_INDEX,
        body={"mappings": mappings, "settings": ES_ITEMS_SETTINGS},
    )
    await add_collection(app_client, test_collection, test_item, "shared-a")
    await add_collection(app_client, test_collection, test_item, "shared-b")
    await index_stale(client, test_item, "stale", "stale")

    assert (await app_client.delete("/collections/shared-a")).status_code == 204

    assert not await client.exists(index=SHARED_ITEMS_INDEX, id="shared-item|shared-a")
    # Only the current generation's items go.
    assert await client.exists(index=SHARED_ITEMS_INDEX, id="stale|shared-a")
    assert await count(client, "shared-b") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shards, touch",
    [
        ({"total": 2, "successful": 1, "failed": 1}, False),
        # An unavailable primary: no copy refreshed, and no failure listed.
        ({"total": 1, "successful": 0, "failed": 0}, False),
        ({"total": 2, "successful": 1, "failed": 1}, True),
    ],
)
async def test_refresh_shard_failure_keeps_the_collection(
    app_client, database, monkeypatch, test_collection, test_item, shards, touch
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    indices_class = type(client.indices)
    refresh = indices_class.refresh
    injected = False

    async def fail_first_shared_refresh(self, *args, **kwargs):
        nonlocal injected
        response = await refresh(self, *args, **kwargs)
        if not injected and kwargs.get("index") == SHARED_ITEMS_INDEX:
            injected = True
            if touch:
                await touch_collection(client, "shared-a")
            return {"_shards": shards}
        return response

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "refresh", fail_first_shared_refresh)
        response = await app_client.delete("/collections/shared-a")

    assert injected
    assert response.status_code == (409 if touch else 500), response.text
    assert await collection_exists(client, "shared-a")
    assert await count(client, "shared-a") == 1
    if touch:
        generation = await stored_generation(client, "shared-a")
        assert await alias_filter(client, "shared-a") == shared_alias_filter(
            "shared-a", generation
        )


@pytest.mark.asyncio
async def test_collection_without_a_generation_keeps_its_term_alias(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-b")
    client = database.client
    # Created before generations existed.
    await client.index(
        index=COLLECTIONS_INDEX,
        id="shared-a",
        body=collection_with(test_collection, "shared-a"),
        refresh=True,
    )
    await client.indices.put_alias(
        index=SHARED_ITEMS_INDEX,
        name=index_alias_by_collection_id("shared-a"),
        body={"filter": {"term": {"collection": "shared-a"}}},
    )
    response = await app_client.post(
        "/collections/shared-a/items", json=item_with(test_item, "shared-a")
    )
    assert response.status_code == 201, response.text
    stored = await client.get(index=SHARED_ITEMS_INDEX, id="shared-item|shared-a")
    assert "collection_generation" not in stored["_source"]

    assert (await app_client.delete("/collections/shared-a")).status_code == 204
    assert await count(client, "shared-a") == 0
    assert await count(client, "shared-b") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["true", "false"])
async def test_client_sent_generation_is_ignored_or_refused(
    app_client, database, monkeypatch, test_collection, test_item, validator
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    client = database.client
    sent = {"collection_generation": "client"}
    response = await app_client.post(
        "/collections", json={**collection_with(test_collection, "shared-a"), **sent}
    )
    assert response.status_code == 201, response.text
    generation = await stored_generation(client, "shared-a")
    assert generation != "client"
    response = await app_client.post(
        "/collections/shared-a/items", json={**item_with(test_item, "shared-a"), **sent}
    )
    assert response.status_code == 201, response.text

    collection_url = "/collections/shared-a"
    item_url = "/collections/shared-a/items/shared-item"
    response = await app_client.put(
        collection_url, json={**collection_with(test_collection, "shared-a"), **sent}
    )
    assert response.status_code == 200, response.text
    response = await app_client.put(
        item_url, json={**item_with(test_item, "shared-a"), **sent}
    )
    assert response.status_code == 200, response.text
    for url in (collection_url, item_url):
        response = await app_client.patch(url, json=sent, headers=MERGE_PATCH)
        assert response.status_code == 200, response.text
        for op in (
            {"op": "add", "path": "/collection_generation", "value": "client"},
            {"op": "remove", "path": "/collection_generation"},
        ):
            response = await app_client.patch(
                url,
                json=[op],
                headers={"Content-Type": "application/json-patch+json"},
            )
            assert response.status_code == 400, response.text

    assert await stored_generation(client, "shared-a") == generation
    stored = await client.get(index=SHARED_ITEMS_INDEX, id="shared-item|shared-a")
    assert stored["_source"]["collection_generation"] == generation


@pytest.mark.asyncio
async def test_default_database_update_keeps_the_generation(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    generation = await stored_generation(client, "shared-a")

    await database.update_collection(
        "shared-a",
        {
            **collection_with(test_collection, "shared-a"),
            "title": "replaced",
            "collection_generation": "caller",
        },
        refresh=True,
    )

    assert await stored_generation(client, "shared-a") == generation
    response = await app_client.post(
        "/collections/shared-a/items",
        json=item_with(test_item, "shared-a", "after-update"),
    )
    assert response.status_code == 201, response.text
    item = await app_client.get("/collections/shared-a/items/after-update")
    assert item.status_code == 200, item.text
    assert (await app_client.delete("/collections/shared-a")).status_code == 204
    assert await count(client, "shared-a") == 0


@pytest.mark.asyncio
async def test_writes_to_a_missing_shared_alias_create_no_index(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    alias = index_alias_by_collection_id("shared-a")
    # As while the collection is created or deleted, or after a failed delete.
    await client.indices.delete_alias(index=SHARED_ITEMS_INDEX, name=alias)

    response = await app_client.post(
        "/collections/shared-a/items", json=item_with(test_item, "shared-a", "one")
    )
    assert response.status_code == 409, response.text
    response = await app_client.post(
        "/collections/shared-a/items",
        json={
            "type": "FeatureCollection",
            "features": [item_with(test_item, "shared-a", "two")],
        },
    )
    assert response.status_code == 400, response.text
    items = [
        await database.async_prep_create_item(
            item=item_with(test_item, "shared-a", name), base_url="http://test/"
        )
        for name in ("three", "four")
    ]
    _, errors = await database.bulk_async("shared-a", items[:1], refresh=True)
    assert [e["create"]["status"] for e in errors] == [404]
    _, errors = database.bulk_sync("shared-a", items[1:], refresh=True)
    assert [e["create"]["status"] for e in errors] == [404]

    assert not await client.indices.exists(index=alias)
    assert await count(client, "shared-a") == 1


@pytest.mark.asyncio
async def test_late_stray_index_delete_spares_a_recreated_collection(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    stray = index_alias_by_collection_id("shared-a")
    # A raw write left an index under the alias name after the alias went away.
    await client.indices.delete_alias(index=SHARED_ITEMS_INDEX, name=stray)
    await client.index(
        index=stray,
        id="late|shared-a",
        body=item_with(test_item, "shared-a", "late"),
        refresh=True,
    )
    indices_class = type(client.indices)
    delete = indices_class.delete
    put_alias = indices_class.put_alias
    in_gap = []
    paused = False

    async def write_before_first_put(self, *args, **kwargs):
        # The re-create wrote its document but not its alias yet.
        if not in_gap:
            in_gap.append(
                await app_client.post(
                    "/collections/shared-a/items",
                    json=item_with(test_item, "shared-a", "in-gap"),
                )
            )
        return await put_alias(self, *args, **kwargs)

    async def recreate_before_first_delete(self, *args, **kwargs):
        # Delete A passed its last version check before removing the stray index.
        nonlocal paused
        if kwargs.get("index") == stray and not paused:
            paused = True
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            with monkeypatch.context() as gap:
                gap.setattr(indices_class, "put_alias", write_before_first_put)
                response = await app_client.post(
                    "/collections", json=collection_with(test_collection, "shared-a")
                )
            assert response.status_code == 201, response.text
        return await delete(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "delete", recreate_before_first_delete)
        response = await app_client.delete("/collections/shared-a")

    assert paused and in_gap
    assert in_gap[0].status_code == 409, in_gap[0].text
    assert response.status_code == 409, response.text
    assert await client.indices.exists(index=SHARED_ITEMS_INDEX)
    generation = await stored_generation(client, "shared-a")
    assert await alias_filter(client, "shared-a") == shared_alias_filter(
        "shared-a", generation
    )
    response = await app_client.post(
        "/collections/shared-a/items", json=item_with(test_item, "shared-a", "after")
    )
    assert response.status_code == 201, response.text
    item = await app_client.get("/collections/shared-a/items/after")
    assert item.status_code == 200, item.text


@pytest.mark.asyncio
async def test_alias_put_back_before_the_stray_delete_is_a_conflict(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    stray = index_alias_by_collection_id("shared-a")
    await client.indices.delete_alias(index=SHARED_ITEMS_INDEX, name=stray)
    await client.index(
        index=stray,
        id="late|shared-a",
        body=item_with(test_item, "shared-a", "late"),
        refresh=True,
    )
    indices_class = type(client.indices)
    delete = indices_class.delete
    paused = False

    async def restore_before_first_delete(self, *args, **kwargs):
        # Another delete removed the stray index and its 409 put the alias back,
        # leaving the collection document unchanged.
        nonlocal paused
        if kwargs.get("index") == stray and not paused:
            paused = True
            await delete(self, index=stray)
            await reconcile_shared_alias(client, "shared-a")
        return await delete(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(indices_class, "delete", restore_before_first_delete)
        response = await app_client.delete("/collections/shared-a")

    assert paused
    assert response.status_code == 409, response.text
    assert await collection_exists(client, "shared-a")
    assert await client.indices.exists_alias(name=stray)


@pytest.mark.asyncio
async def test_generation_is_not_exposed(
    app_client, database, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    responses = [
        (await app_client.get("/collections/shared-a")).json(),
        (await app_client.get("/collections")).json(),
        (await app_client.get("/collections/shared-a/items/shared-item")).json(),
        (await app_client.get("/collections/shared-a/items")).json(),
        (await app_client.post("/search", json={"collections": ["shared-a"]})).json(),
    ]
    for path in ("queryables", "sortables"):
        for url in (f"/{path}", f"/collections/shared-a/{path}"):
            response = await app_client.get(url)
            assert response.status_code == 200, response.text
            responses.append(response.json())
    for body in responses:
        assert "collection_generation" not in str(body)
    response = await app_client.post(
        "/search", json={"sortby": [{"field": "collection_generation"}]}
    )
    assert response.status_code == 400, response.text


@pytest.mark.asyncio
async def test_generation_mappings_are_added_to_existing_strict_indexes(
    database, monkeypatch
):
    client = database.client
    names = ("strict-collections-test", "strict-items-test")
    for name in names:
        await client.indices.create(
            index=name,
            body={
                "mappings": {
                    "dynamic": "strict",
                    "properties": {"id": {"type": "keyword"}},
                }
            },
        )
    try:
        monkeypatch.setattr(index_module, "COLLECTIONS_INDEX", names[0])
        monkeypatch.setattr(index_module, "SHARED_ITEMS_INDEX", names[1])
        await ensure_generation_mappings(client)
        await ensure_generation_mappings(client)
        for name in names:
            await client.index(
                index=name,
                id="a",
                body={"id": "a", "collection_generation": "g"},
                refresh=True,
            )
        monkeypatch.setattr(index_module, "SHARED_ITEMS_INDEX", "missing-items-test")
        await ensure_generation_mappings(client)
    finally:
        await client.indices.delete(index=",".join(names))


@pytest.mark.asyncio
async def test_change_during_cleanup_fails_the_final_delete(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    delete_by_query = type(client).delete_by_query

    stray = index_alias_by_collection_id("shared-a")

    async def delete_then_change(self, *args, **kwargs):
        response = await delete_by_query(self, *args, **kwargs)
        await touch_collection(client, "shared-a")
        # Written after the change, to the index the removed alias leaves behind.
        await client.index(
            index=stray,
            id="late|shared-a",
            body=item_with(test_item, "shared-a", "late"),
            refresh=True,
        )
        return response

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete_by_query", delete_then_change)
        with pytest.raises(ConflictError):
            await database.delete_collection("shared-a")

    assert await collection_exists(client, "shared-a")
    assert (await client.count(index=stray))["count"] == 1
    # The restore leaves a stray index under the alias name to the retried DELETE.
    assert not await client.indices.exists_alias(name=stray)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "responses, status",
    [
        ([{"deleted": 0, "version_conflicts": 1, "failures": []}], 204),
        ([{"deleted": 0, "version_conflicts": 1, "failures": []}] * 2, 500),
        (
            [{"deleted": 0, "version_conflicts": 0, "failures": [], "timed_out": True}],
            500,
        ),
        (
            [{"deleted": 0, "version_conflicts": 0, "failures": [], "canceled": "x"}],
            500,
        ),
        (
            [{"deleted": 0, "failures": [], "slices": [{}, {"canceled": "x"}]}],
            500,
        ),
        ([{"status": 404, "error": {"type": "search_context_missing_exception"}}], 500),
    ],
)
async def test_unfinished_delete_by_query_keeps_the_collection(
    app_client, database, monkeypatch, test_collection, test_item, responses, status
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    delete_by_query = type(client).delete_by_query
    queued = list(responses)

    async def scripted(self, *args, **kwargs):
        if queued:
            return queued.pop(0)
        return await delete_by_query(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete_by_query", scripted)
        response = await app_client.delete("/collections/shared-a")

    assert response.status_code == status, response.text
    if status == 500:
        assert "retry DELETE" in response.json()["detail"]
        assert await collection_exists(client, "shared-a")
        assert (await app_client.delete("/collections/shared-a")).status_code == 204
    assert not await collection_exists(client, "shared-a")
    assert await count(client, "shared-a") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("if_match", [False, True])
@pytest.mark.parametrize("recreate", [True, False])
async def test_replayed_delete_never_adopts_a_recreated_collection(
    app_client, database, monkeypatch, test_collection, test_item, if_match, recreate
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    headers = {}
    if if_match:
        etag = (await app_client.get("/collections/shared-a")).headers["etag"]
        headers["If-Match"] = etag
    client = database.client
    delete = type(client).delete
    injected = False

    async def commit_then_time_out(self, *args, **kwargs):
        nonlocal injected
        response = await delete(self, *args, **kwargs)
        if not injected and kwargs.get("id") == "shared-a":
            injected = True
            if recreate:
                await add_collection(app_client, test_collection, test_item, "shared-a")
            raise injected_timeout()
        return response

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete", commit_then_time_out)
        response = await app_client.delete("/collections/shared-a", headers=headers)

    assert injected
    if recreate:
        assert response.status_code == 409, response.text
        assert await collection_exists(client, "shared-a")
        alias = index_alias_by_collection_id("shared-a")
        assert await client.indices.exists_alias(name=alias)
        assert await count(client, "shared-a") == 1
    else:
        assert response.status_code == 404, response.text


@pytest.mark.asyncio
async def test_simple_strategy_replay_of_a_shared_layout_cleanup(
    app, app_client, txn_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    delete_by_query = type(client).delete_by_query
    injected = False

    async def time_out_once(self, *args, **kwargs):
        nonlocal injected
        if not injected:
            injected = True
            raise injected_timeout()
        return await delete_by_query(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setenv("ITEMS_INDEX_STRATEGY", "simple")
        async with rebuilt_index_strategy(txn_client, app):
            patch.setattr(type(client), "delete_by_query", time_out_once)
            response = await app_client.delete("/collections/shared-a")

    assert injected
    if OPENSEARCH:
        # Only Elasticsearch retries delete_item_index; the replay of the whole
        # delete finds the collection gone. An unsupported migration.
        assert response.status_code == 404, response.text
        assert await count(client, "shared-a") == 1
    else:
        assert response.status_code == 204, response.text
        assert await count(client, "shared-a") == 0


@pytest.mark.asyncio
async def test_retried_delete_removes_a_stray_index_from_a_failed_cleanup(
    app_client, database, monkeypatch, test_collection, test_item
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client

    async def failing(self, *args, **kwargs):
        return {"deleted": 0, "version_conflicts": 0, "failures": [{"cause": "x"}]}

    with monkeypatch.context() as patch:
        patch.setattr(type(client), "delete_by_query", failing)
        assert (await app_client.delete("/collections/shared-a")).status_code == 500

    assert await collection_exists(client, "shared-a")
    response = await app_client.post(
        "/collections", json=collection_with(test_collection, "shared-a")
    )
    assert response.status_code == 409
    stray = index_alias_by_collection_id("shared-a")
    response = await app_client.post(
        "/collections/shared-a/items",
        json=item_with(test_item, "shared-a", "stray"),
    )
    assert response.status_code == 409, response.text
    assert not await client.indices.exists(index=stray)
    # A raw write to the alias name still creates the index.
    await client.index(
        index=stray,
        id="stray|shared-a",
        body=item_with(test_item, "shared-a", "stray"),
        refresh=True,
    )

    assert (await app_client.delete("/collections/shared-a")).status_code == 204
    assert not await client.indices.exists(index=stray)
    response = await app_client.post(
        "/collections", json=collection_with(test_collection, "shared-a")
    )
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["shared", "simple", "datetime"])
async def test_delete_removes_an_index_created_by_a_write_during_cleanup(
    app,
    app_client,
    txn_client,
    database,
    monkeypatch,
    test_collection,
    test_item,
    strategy,
):
    await add_collection(app_client, test_collection, test_item, "shared-a")
    client = database.client
    delete_by_query = type(client).delete_by_query
    stray = index_alias_by_collection_id("shared-a")

    async def write_then_delete(self, *args, **kwargs):
        # The alias is already gone, so this write auto-creates `stray`.
        await client.index(
            index=stray,
            id="late|shared-a",
            body=item_with(test_item, "shared-a", "late"),
            refresh=True,
        )
        return await delete_by_query(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setenv("ITEMS_INDEX_STRATEGY", strategy)
        async with rebuilt_index_strategy(txn_client, app):
            if strategy != "shared":
                await client.indices.delete_alias(index=SHARED_ITEMS_INDEX, name=stray)
                stray = index_alias_by_collection_id("shared-a")
            patch.setattr(type(client), "delete_by_query", write_then_delete)
            assert (await app_client.delete("/collections/shared-a")).status_code == 204

    assert not await client.indices.exists(index=stray)
    response = await app_client.post(
        "/collections", json=collection_with(test_collection, "shared-a")
    )
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
async def test_simple_strategy_deletes_shared_layout_collections(
    app, app_client, txn_client, database, monkeypatch, test_collection, test_item
):
    for collection_id in ("shared-a", "shared-b"):
        await add_collection(app_client, test_collection, test_item, collection_id)
    client = database.client
    # The document goes first here, so another generation may be a re-created one.
    await index_stale(client, test_item, "other", "other")

    with monkeypatch.context() as patch:
        patch.setenv("ITEMS_INDEX_STRATEGY", "simple")
        async with rebuilt_index_strategy(txn_client, app):
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            assert await client.indices.exists(index=SHARED_ITEMS_INDEX)
            assert not await client.exists(
                index=SHARED_ITEMS_INDEX, id="shared-item|shared-a"
            )
            assert await client.exists(index=SHARED_ITEMS_INDEX, id="other|shared-a")

            # Under simple, "shared" is an ordinary id whose alias is the index.
            await add_collection(app_client, test_collection, test_item, "shared")
            assert await count(client, "shared") == 1
            assert (await app_client.delete("/collections/shared")).status_code == 204

    assert await client.indices.exists(index=SHARED_ITEMS_INDEX)
    assert await count(client, "shared") == 0
    assert await count(client, "shared-b") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["simple", "datetime"])
@pytest.mark.parametrize("missing_alias", [False, True])
@pytest.mark.parametrize("foreign_kind", ["alias", "index"])
async def test_colon_delete_requires_shared_strategy_and_preserves_legacy_names(
    app,
    app_client,
    txn_client,
    database,
    monkeypatch,
    test_collection,
    test_item,
    strategy,
    missing_alias,
    foreign_kind,
):
    client = database.client
    for collection_id in ("a:b", "ab"):
        await add_collection(app_client, test_collection, test_item, collection_id)
    alias = index_alias_by_collection_id("a:b")
    if missing_alias:
        await client.indices.delete_alias(index=SHARED_ITEMS_INDEX, name=alias)
    foreign_index = "items_foreign" if foreign_kind == "alias" else "items_ab"
    await client.index(
        index=foreign_index, id="foreign", body={"value": 1}, refresh=True
    )
    if foreign_kind == "alias":
        await client.indices.put_alias(index=foreign_index, name="items_ab")

    with monkeypatch.context() as patch:
        patch.setenv("ITEMS_INDEX_STRATEGY", strategy)
        async with rebuilt_index_strategy(txn_client, app):
            response = await app_client.delete("/collections/a:b")
    assert response.status_code == 409, response.text
    assert "ITEMS_INDEX_STRATEGY=shared" in response.text
    assert await collection_exists(client, "a:b")
    assert await collection_exists(client, "ab")
    assert await count(client, "a:b") == 1
    assert await count(client, "ab") == 1
    assert bool(await client.indices.exists_alias(name=alias)) is not missing_alias

    assert (await app_client.delete("/collections/a:b")).status_code == 204
    assert not await collection_exists(client, "a:b")
    assert await count(client, "a:b") == 0
    assert await count(client, "ab") == 1
    assert await collection_exists(client, "ab")
    assert (await client.count(index="items_ab"))["count"] == 1
    assert await client.indices.exists(index=foreign_index)


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["simple", "datetime"])
@pytest.mark.parametrize("shared_name_alias", [False, True])
async def test_legacy_colon_delete_without_shared_physical_index(
    app,
    app_client,
    txn_client,
    database,
    monkeypatch,
    test_collection,
    test_item,
    strategy,
    shared_name_alias,
):
    client = database.client
    if shared_name_alias:
        await client.indices.create(
            index="items_foreign", body={"aliases": {SHARED_ITEMS_INDEX: {}}}
        )
    with monkeypatch.context() as patch:
        patch.setenv("ITEMS_INDEX_STRATEGY", strategy)
        async with rebuilt_index_strategy(txn_client, app):
            await add_collection(app_client, test_collection, test_item, "a:b")
            assert (await app_client.delete("/collections/a:b")).status_code == 204
    assert not await collection_exists(client, "a:b")
    assert not await client.indices.exists(index="items_ab")
    if shared_name_alias:
        assert await client.indices.exists_alias(name=SHARED_ITEMS_INDEX)
        assert await client.indices.exists(index="items_foreign")


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["legacy", "shared-b", None])
async def test_legacy_shared_alias_requires_its_exact_collection_filter(
    app_client,
    database,
    test_collection,
    test_item,
    owner,
):
    await add_collection(app_client, test_collection, test_item, "shared-b")
    client = database.client
    await client.index(
        index=COLLECTIONS_INDEX,
        id="legacy",
        body=collection_with(test_collection, "legacy"),
        refresh=True,
    )
    await client.index(
        index=SHARED_ITEMS_INDEX,
        id="shared-item|legacy",
        body=item_with(test_item, "legacy"),
        refresh=True,
    )
    await client.indices.put_alias(
        index=SHARED_ITEMS_INDEX,
        name="items_legacy",
        body={"filter": {"term": {"collection": owner}}} if owner else {},
    )
    response = await app_client.delete("/collections/legacy")
    assert response.status_code == (204 if owner == "legacy" else 409), response.text
    assert await collection_exists(client, "legacy") is (owner != "legacy")
    assert bool(await client.indices.exists_alias(name="items_legacy")) is (
        owner != "legacy"
    )
    assert await count(client, "legacy") == (0 if owner == "legacy" else 1)
    assert await count(client, "shared-b") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("collection_id", ["a b", "a|b", "shared"])
async def test_ids_the_shared_index_cannot_alias_are_rejected(
    app_client, database, test_collection, collection_id
):
    response = await app_client.post(
        "/collections", json=collection_with(test_collection, collection_id)
    )
    assert response.status_code == 400, response.text
    assert not await collection_exists(database.client, collection_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["shared", "simple", "datetime"])
@pytest.mark.parametrize("suffix", ["", "#"])
async def test_ids_shaped_like_a_shared_alias_are_rejected(
    app,
    app_client,
    txn_client,
    database,
    monkeypatch,
    test_collection,
    strategy,
    suffix,
):
    # Its alias would be the shared alias of collection "a".
    collection_id = f"collection_{hashlib.sha256(b'a').hexdigest()}{suffix}"
    with monkeypatch.context() as patch:
        patch.setenv("ITEMS_INDEX_STRATEGY", strategy)
        async with rebuilt_index_strategy(txn_client, app):
            response = await app_client.post(
                "/collections", json=collection_with(test_collection, collection_id)
            )
    assert response.status_code == 400, response.text
    assert not await collection_exists(database.client, collection_id)


@pytest.mark.asyncio
async def test_rename_is_refused(app_client, database, test_collection, test_item):
    await add_collection(app_client, test_collection, test_item, "shared-a")

    with pytest.raises(HTTPException) as exc_info:
        await database.update_collection(
            "shared-a", collection_with(test_collection, "shared-renamed")
        )
    assert exc_info.value.status_code == 400

    assert not await collection_exists(database.client, "shared-renamed")
    alias = index_alias_by_collection_id("shared-renamed")
    assert not await database.client.indices.exists_alias(name=alias)


@pytest.mark.parametrize(
    "attribute, value",
    [
        ("ITEMS_ALIAS_PREFIX", "other_"),
        (
            "ES_ITEMS_MAPPINGS",
            {"properties": {"collection": {"type": "keyword", "normalizer": "x"}}},
        ),
        (
            "ES_ITEMS_MAPPINGS",
            {
                "properties": {
                    "collection": {"type": "keyword"},
                    "collection_generation": {"type": "text"},
                }
            },
        ),
    ],
)
def test_shared_strategy_refuses_unsupported_settings(monkeypatch, attribute, value):
    monkeypatch.delenv("ENABLE_DATETIME_INDEX_FILTERING", raising=False)
    monkeypatch.setenv("ITEMS_INDEX_STRATEGY", "shared")
    monkeypatch.setattr(factory_module, attribute, value)

    with pytest.raises(ValueError, match="ITEMS_INDEX_STRATEGY=shared"):
        IndexInsertionFactory.create_insertion_strategy(None)


@pytest.mark.asyncio
@pytest.mark.parametrize("replaced", [False, True])
async def test_failed_index_creation_rolls_back_the_collection(
    txn_client, monkeypatch, test_collection, replaced
):
    database = txn_client.database
    collection = collection_with(test_collection, "rollback")
    replacement = {**collection, "title": "replacement"}

    async def fail(client, collection_id):
        if replaced:
            await database.client.index(
                index=COLLECTIONS_INDEX, id="rollback", body=replacement, refresh=True
            )
        raise RuntimeError("injected")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(database.async_index_inserter, "create_simple_index", fail)
            with pytest.raises(RuntimeError, match="injected"):
                await database.create_collection(collection, refresh=True)

        if replaced:
            stored = await database.client.get(index=COLLECTIONS_INDEX, id="rollback")
            assert stored["_source"]["title"] == "replacement"
        else:
            assert not await collection_exists(database.client, "rollback")
    finally:
        if hasattr(database.client, "options"):
            await database.client.options(ignore_status=[404]).delete(
                index=COLLECTIONS_INDEX, id="rollback", refresh=True
            )
        else:
            await database.client.delete(
                index=COLLECTIONS_INDEX, id="rollback", refresh=True, ignore=[404]
            )


@pytest.mark.asyncio
async def test_deleting_collection_star_keeps_other_collections_indexes(
    txn_client, test_collection
):
    # Default strategy: the alias of collection "*" is the pattern `items_*`.
    database = txn_client.database
    await database.create_collection(
        collection_with(test_collection, "keep-me"), refresh=True
    )
    await database.create_collection(
        collection_with(test_collection, "*"), refresh=True
    )
    alias = index_alias_by_collection_id("keep-me")
    try:
        await database.delete_collection("*", refresh=True)

        assert await database.client.indices.exists_alias(name=alias)
        assert not await collection_exists(database.client, "*")
    finally:
        await database.delete_collection("keep-me", refresh=True)
