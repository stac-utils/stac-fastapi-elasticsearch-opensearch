"""ITEMS_INDEX_STRATEGY=shared: every collection's items in one index."""

import os
from copy import deepcopy

import pytest
from fastapi import HTTPException

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.database.utils import ConnectionTimeout
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX, SHARED_ITEMS_INDEX
from stac_fastapi.sfeos_helpers.search_engine import IndexInsertionFactory
from stac_fastapi.sfeos_helpers.search_engine import factory as factory_module
from stac_fastapi.types.errors import ConflictError

from ..conftest import rebuilt_index_strategy

OPENSEARCH = os.getenv("BACKEND", "elasticsearch").lower() == "opensearch"
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


async def touch_collection(client, collection_id):
    doc = await client.get(index=COLLECTIONS_INDEX, id=collection_id)
    source = {**doc["_source"], "title": f"changed {doc['_seq_no']}"}
    await client.index(
        index=COLLECTIONS_INDEX, id=collection_id, body=source, refresh=True
    )


@pytest.fixture
def database(txn_client, shared_index_env):
    return txn_client.database


@pytest.mark.asyncio
async def test_collections_share_one_index_with_filtered_aliases(
    app_client, database, test_collection, test_item
):
    for collection_id in ("shared-a", "shared-b"):
        await add_collection(app_client, test_collection, test_item, collection_id)

    client = database.client
    assert set(await client.indices.get(index="items_*")) == {SHARED_ITEMS_INDEX}
    for collection_id in ("shared-a", "shared-b"):
        alias = index_alias_by_collection_id(collection_id)
        aliases = await client.indices.get_alias(name=alias)
        assert aliases[SHARED_ITEMS_INDEX]["aliases"][alias]["filter"] == {
            "term": {"collection": collection_id}
        }


@pytest.mark.asyncio
async def test_same_item_id_in_two_collections_stays_separate(
    app_client, database, test_collection, test_item
):
    for collection_id in ("shared-a", "shared-b"):
        await add_collection(app_client, test_collection, test_item, collection_id)
    url_a = "/collections/shared-a/items/shared-item"
    url_b = "/collections/shared-b/items/shared-item"

    assert (await app_client.get(url_a)).json()["properties"]["title"] == "shared-a"

    replacement = item_with(test_item, "shared-a", title="put")
    assert (await app_client.put(url_a, json=replacement)).status_code == 200
    response = await app_client.patch(
        url_a, json={"properties": {"title": "patched"}}, headers=MERGE_PATCH
    )
    assert response.status_code == 200, response.text
    assert (await app_client.get(url_a)).json()["properties"]["title"] == "patched"
    assert (await app_client.get(url_b)).json()["properties"]["title"] == "shared-b"

    assert (await app_client.delete(url_a)).status_code == 204
    assert (await app_client.get(url_a)).status_code == 404
    assert (await app_client.get(url_b)).status_code == 200


@pytest.mark.asyncio
async def test_listing_and_search_are_scoped_and_bulk_lands_in_its_collection(
    app_client, database, test_collection, test_item
):
    for collection_id in ("shared-a", "shared-b"):
        await add_collection(app_client, test_collection, test_item, collection_id)
    bulk = {
        "type": "FeatureCollection",
        "features": [
            item_with(test_item, "shared-b", f"bulk-{n}", "bulk") for n in range(3)
        ],
    }
    response = await app_client.post("/collections/shared-b/items", json=bulk)
    assert response.status_code == 201, response.text

    listing = (await app_client.get("/collections/shared-a/items")).json()
    assert [f["id"] for f in listing["features"]] == ["shared-item"]
    assert (
        len((await app_client.get("/collections/shared-b/items")).json()["features"])
        == 4
    )

    scoped = await app_client.post("/search", json={"collections": ["shared-a"]})
    assert [f["collection"] for f in scoped.json()["features"]] == ["shared-a"]

    everything = await app_client.post("/search", json={"ids": ["shared-item"]})
    assert sorted(f["collection"] for f in everything.json()["features"]) == [
        "shared-a",
        "shared-b",
    ]
    assert await count(database.client, "shared-a") == 1
    assert await count(database.client, "shared-b") == 4


@pytest.mark.asyncio
async def test_delete_removes_unrefreshed_items_and_keeps_other_collections(
    app_client, database, test_collection, test_item
):
    for collection_id in ("shared-a", "shared-b"):
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
            index=index_alias_by_collection_id("shared-a"),
            id="unrefreshed|shared-a",
            body=item_with(test_item, "shared-a", "unrefreshed"),
            refresh="false",
        )

        assert (await app_client.delete("/collections/shared-a")).status_code == 204
    finally:
        await indices.put_settings(
            index=SHARED_ITEMS_INDEX,
            body={"refresh_interval": interval.get("refresh_interval")},
        )

    assert await count(database.client, "shared-a") == 0
    assert await count(database.client, "shared-b") == 1
    assert not await indices.exists_alias(name=index_alias_by_collection_id("shared-a"))


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
    response = await app_client.post(
        "/collections/shared-a/items",
        json=item_with(test_item, "shared-a", "stray"),
    )
    assert response.status_code == 201, response.text
    stray = index_alias_by_collection_id("shared-a")
    assert await client.indices.exists(index=stray)

    assert (await app_client.delete("/collections/shared-a")).status_code == 204
    assert not await client.indices.exists(index=stray)
    response = await app_client.post(
        "/collections", json=collection_with(test_collection, "shared-a")
    )
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
async def test_delete_removes_an_index_created_by_a_write_during_cleanup(
    app_client, database, monkeypatch, test_collection, test_item
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

    with monkeypatch.context() as patch:
        patch.setenv("ITEMS_INDEX_STRATEGY", "simple")
        async with rebuilt_index_strategy(txn_client, app):
            assert (await app_client.delete("/collections/shared-a")).status_code == 204
            assert await client.indices.exists(index=SHARED_ITEMS_INDEX)
            assert await count(client, "shared-a") == 0

            # Under simple, "shared" is an ordinary id whose alias is the index.
            await add_collection(app_client, test_collection, test_item, "shared")
            assert await count(client, "shared") == 1
            assert (await app_client.delete("/collections/shared")).status_code == 204

    assert await client.indices.exists(index=SHARED_ITEMS_INDEX)
    assert await count(client, "shared") == 0
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
