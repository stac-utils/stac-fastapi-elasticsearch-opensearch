"""Creation races exercise the exact database injected into the HTTP app."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import quote
from uuid import uuid4

import pytest
import pytest_asyncio
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from stac_fastapi.core.etag import DocumentVersion
from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX, ITEMS_INDEX_PREFIX
from stac_fastapi.sfeos_helpers.search_engine.selection.selectors import (
    DatetimeBasedIndexSelector,
)
from stac_fastapi.types.errors import ConflictError

from ..conftest import (
    AsyncSettings,
    DatabaseLogic,
    create_collection_index,
    create_index_templates,
    instantiate_api,
)


@pytest_asyncio.fixture
async def owned_collection_app(request, monkeypatch):
    datetime = getattr(request, "param", False)
    with monkeypatch.context() as patch:
        patch.setenv("ENABLE_DATETIME_INDEX_FILTERING", str(datetime).lower())
        patch.setattr(DatetimeBasedIndexSelector, "_instance", None)
        database = DatabaseLogic()
        api = instantiate_api(
            settings=AsyncSettings(enable_catalogs_route=True), database_logic=database
        )
        api.app.router.dependencies = []
        await create_index_templates()
        await create_collection_index()
        prefix = f"atomic-{uuid4().hex}"
        try:
            async with AsyncClient(
                transport=ASGITransport(app=api.app), base_url="http://test-server"
            ) as client:
                yield SimpleNamespace(
                    database=database, client=client, prefix=prefix, datetime=datetime
                )
        finally:
            # Delete only this fixture's resources, even when an assertion fails.
            # delete_by_query only sees refreshed documents (DATABASE_REFRESH=false tests).
            await database.client.indices.refresh(index=COLLECTIONS_INDEX)
            await database.client.delete_by_query(
                index=COLLECTIONS_INDEX,
                body={"query": {"prefix": {"id": prefix}}},
                refresh=True,
            )
            await database.client.indices.delete(
                index=f"{ITEMS_INDEX_PREFIX}{prefix}*", ignore_unavailable=True
            )
            if datetime:
                cache = database.async_index_selector.cache_manager
                if cache._redis is not None:
                    await cache._redis.aclose()
            await database.client.close()
            database.sync_client.close()


async def race(app, collection, monkeypatch, scoped=False):
    database, client = app.database, app.client
    collection["id"] = app.prefix
    paths = ["/collections", "/collections"]
    parents = [f"{app.prefix}-parent-{i}" for i in range(2)]
    if scoped:
        for parent in parents:
            response = await client.post(
                "/catalogs",
                json={
                    "id": parent,
                    "type": "Catalog",
                    "stac_version": "1.0.0",
                    "description": parent,
                    "links": [],
                },
            )
            assert response.status_code == 201, response.text
        paths = [f"/catalogs/{parent}/collections" for parent in parents]

    bodies = [dict(deepcopy(collection), description=f"creator-{i}") for i in range(2)]
    barrier = asyncio.Barrier(2)
    index = database.client.index
    writes = 0

    async def synchronized_index(*args, **kwargs):
        nonlocal writes
        # One shot, keyed by ID (including baseline writes without op_type).
        if kwargs.get("id") == collection["id"] and writes < 2:
            writes += 1
            await asyncio.wait_for(barrier.wait(), timeout=15)
        return await index(*args, **kwargs)

    provision = AsyncMock(wraps=database.async_index_inserter.create_simple_index)
    with monkeypatch.context() as patch:
        patch.setattr(database.client, "index", synchronized_index)
        patch.setattr(database.async_index_inserter, "create_simple_index", provision)
        responses = await asyncio.wait_for(
            asyncio.gather(
                *(client.post(path, json=body) for path, body in zip(paths, bodies))
            ),
            timeout=30,
        )

    assert writes == 2
    assert sorted(response.status_code for response in responses) == [201, 409], [
        response.text for response in responses
    ]
    winner = next(
        i for i, response in enumerate(responses) if response.status_code == 201
    )
    stored = await database.find_collection(collection["id"])
    assert stored["description"] == bodies[winner]["description"]
    assert provision.await_count == (0 if app.datetime else 1)
    if not app.datetime:
        provision.assert_awaited_once_with(database.client, collection["id"])
    if scoped:
        assert stored["parent_ids"] == [parents[winner]]
        before = await database.client.get(index=COLLECTIONS_INDEX, id=app.prefix)
        # A full-body retry remains a conflict under #868, without any mutation.
        retry = await client.post(paths[1 - winner], json=bodies[1 - winner])
        assert retry.status_code == 409, retry.text
        assert "Warning" not in retry.headers
        assert (
            await database.client.get(index=COLLECTIONS_INDEX, id=app.prefix)
        ) == before
        # Explicit ID-only linking adds membership while preserving winner content.
        retry = await client.post(paths[1 - winner], json={"id": app.prefix})
        assert retry.status_code == 200, retry.text
        assert "Warning" not in retry.headers
        linked = await database.find_collection(collection["id"])
        assert set(linked.pop("parent_ids")) == set(parents)
        stored.pop("parent_ids")
        assert linked == stored


@pytest.mark.asyncio
async def test_root_creation_race(owned_collection_app, test_collection, monkeypatch):
    await race(owned_collection_app, test_collection, monkeypatch)


@pytest.mark.asyncio
async def test_scoped_creation_race(owned_collection_app, test_collection, monkeypatch):
    await race(owned_collection_app, test_collection, monkeypatch, scoped=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("owned_collection_app", [True], indirect=True)
async def test_datetime_creation_race(
    owned_collection_app, test_collection, monkeypatch
):
    assert (
        not owned_collection_app.database.async_index_inserter.should_create_collection_index()
    )
    assert (
        owned_collection_app.database.async_index_selector.alias_loader.client
        is owned_collection_app.database.client
    )
    await race(owned_collection_app, test_collection, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["Collection", "Catalog"])
async def test_existing_document_conflict(
    owned_collection_app, test_collection, monkeypatch, kind
):
    app = owned_collection_app
    test_collection["id"] = app.prefix
    existing = dict(deepcopy(test_collection), type=kind, description="original")
    if kind == "Collection":
        await app.database.create_collection(existing, refresh=True)
    else:
        await app.database.create_catalog(existing, refresh=True)
    before = await app.database.client.get(index=COLLECTIONS_INDEX, id=app.prefix)
    provision = AsyncMock()
    monkeypatch.setattr(
        app.database.async_index_inserter, "create_simple_index", provision
    )
    with pytest.raises(ConflictError, match=f"Collection {app.prefix} already exists"):
        await app.database.create_collection(test_collection, refresh=True)
    after = await app.database.client.get(index=COLLECTIONS_INDEX, id=app.prefix)
    assert after == before
    provision.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refresh,expected", [(True, "true"), (False, "false"), ("wait_for", "wait_for")]
)
async def test_create_arguments(
    owned_collection_app, test_collection, monkeypatch, refresh, expected
):
    app = owned_collection_app
    test_collection["id"] = app.prefix
    monkeypatch.delenv("DATABASE_REFRESH", raising=False)
    index = AsyncMock(
        return_value={"_index": COLLECTIONS_INDEX, "_seq_no": 0, "_primary_term": 1}
    )
    exists = AsyncMock(
        side_effect=AssertionError("creation must use an authoritative write")
    )
    provision = AsyncMock()
    monkeypatch.setattr(app.database.client, "index", index)
    monkeypatch.setattr(app.database.client, "exists", exists)
    monkeypatch.setattr(
        app.database.async_index_inserter, "create_simple_index", provision
    )
    result = await app.database.create_collection(test_collection, refresh=refresh)
    assert result == DocumentVersion(index=COLLECTIONS_INDEX, seq_no=0, primary_term=1)
    kwargs = index.call_args.kwargs
    assert kwargs["op_type"] == "create"
    assert kwargs["refresh"] == expected
    assert kwargs["index"] == COLLECTIONS_INDEX
    assert kwargs["id"] == app.prefix
    payload_key = (
        "body" if "opensearch" in type(app.database).__module__ else "document"
    )
    assert kwargs[payload_key] is test_collection
    assert "bbox_shape" in test_collection
    provision.assert_awaited_once_with(app.database.client, app.prefix)


@pytest.mark.asyncio
async def test_unexpected_write_failure(
    owned_collection_app, test_collection, monkeypatch
):
    app = owned_collection_app
    test_collection["id"] = app.prefix
    error = RuntimeError("unexpected write failure")
    provision = AsyncMock()
    monkeypatch.setattr(app.database.client, "index", AsyncMock(side_effect=error))
    monkeypatch.setattr(
        app.database.async_index_inserter, "create_simple_index", provision
    )
    with pytest.raises(RuntimeError) as caught:
        await app.database.create_collection(test_collection)
    assert caught.value is error
    provision.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [True, False])
async def test_rename(
    owned_collection_app, test_collection, test_item, monkeypatch, conflict
):
    app = owned_collection_app
    source = dict(deepcopy(test_collection), id=f"{app.prefix}-source")
    destination = dict(deepcopy(test_collection), id=f"{app.prefix}-destination")
    for collection in [source, destination] if conflict else [source]:
        response = await app.client.post("/collections", json=collection)
        assert response.status_code == 201, response.text
        item = dict(deepcopy(test_item), collection=collection["id"])
        response = await app.client.post(
            f"/collections/{collection['id']}/items", json=item
        )
        assert response.status_code == 201, response.text

    async def snapshot():
        documents = [
            await app.database.client.get(index=COLLECTIONS_INDEX, id=c["id"])
            for c in [source, destination]
        ]
        items = await app.database.client.search(
            index=f"{ITEMS_INDEX_PREFIX}{app.prefix}*",
            body={"query": {"match_all": {}}, "version": True},
        )
        return documents, sorted(items["hits"]["hits"], key=lambda hit: hit["_id"])

    if conflict:
        before = await snapshot()
        reindex = AsyncMock(wraps=app.database.client.reindex)
        delete = AsyncMock(wraps=app.database.delete_collection)
        monkeypatch.setattr(app.database.client, "reindex", reindex)
        monkeypatch.setattr(app.database, "delete_collection", delete)
        with pytest.raises(ConflictError):
            await app.database.update_collection(
                source["id"], destination, refresh=True
            )
        assert await snapshot() == before
        reindex.assert_not_awaited()
        delete.assert_not_awaited()
    else:
        await app.database.update_collection(source["id"], destination, refresh=True)
        assert not await app.database.client.exists(
            index=COLLECTIONS_INDEX, id=source["id"]
        )
        assert (await app.database.find_collection(destination["id"]))[
            "id"
        ] == destination["id"]
        response = await app.client.get(
            f"/collections/{destination['id']}/items/{test_item['id']}"
        )
        assert response.status_code == 200, response.text
        assert response.json()["collection"] == destination["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["-b", "-b'c"])
async def test_rename_keeps_item_ids(
    owned_collection_app, test_collection, test_item, suffix
):
    app = owned_collection_app
    source = f"{app.prefix}-a"
    destination = f"{app.prefix}{suffix}"
    response = await app.client.post(
        "/collections", json=dict(deepcopy(test_collection), id=source)
    )
    assert response.status_code == 201, response.text
    # Each id contains a collection id, so rewriting it inside _id would misplace or collide items.
    item_ids = [source, f"{destination}{source}", f"{destination}{destination}"]
    for item_id in item_ids:
        item = dict(deepcopy(test_item), id=item_id, collection=source)
        response = await app.client.post(f"/collections/{source}/items", json=item)
        assert response.status_code == 201, response.text

    await app.database.update_collection(
        source, dict(deepcopy(test_collection), id=destination), refresh=True
    )

    for item_id in item_ids:
        response = await app.client.get(
            f"/collections/{quote(destination, safe='')}/items/{quote(item_id, safe='')}"
        )
        assert response.status_code == 200, response.text
        assert response.json()["id"] == item_id
        assert response.json()["collection"] == destination


@pytest.mark.asyncio
async def test_rename_keeps_unrefreshed_items(
    owned_collection_app, test_collection, test_item, monkeypatch
):
    app = owned_collection_app
    # validate_refresh lets DATABASE_REFRESH (true in conftest) override refresh=False.
    monkeypatch.setenv("DATABASE_REFRESH", "false")
    source = dict(deepcopy(test_collection), id=f"{app.prefix}-a")
    destination = dict(deepcopy(test_collection), id=f"{app.prefix}-b")
    response = await app.client.post("/collections", json=source)
    assert response.status_code == 201, response.text
    # Stop background refreshes, so only the rename itself can make the items visible.
    await app.database.client.indices.put_settings(
        index=index_alias_by_collection_id(source["id"]),
        body={"refresh_interval": "-1"},
    )
    item_ids = [f"unrefreshed-{i}" for i in range(3)]
    for item_id in item_ids:
        item = dict(deepcopy(test_item), id=item_id, collection=source["id"])
        item = await app.database.async_prep_create_item(item, "http://test-server")
        await app.database.create_item(item, refresh=False)

    await app.database.update_collection(source["id"], destination, refresh=False)
    await app.database.client.indices.refresh(
        index=index_alias_by_collection_id(destination["id"])
    )

    for item_id in item_ids:
        response = await app.client.get(
            f"/collections/{destination['id']}/items/{item_id}"
        )
        assert response.status_code == 200, response.text
    response = await app.client.get(f"/collections/{destination['id']}/items")
    assert response.status_code == 200, response.text
    assert len(response.json()["features"]) == len(item_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize("owned_collection_app", [True], indirect=True)
async def test_rename_is_refused_under_datetime_strategy(
    owned_collection_app, test_collection, test_item
):
    app = owned_collection_app
    source = dict(deepcopy(test_collection), id=f"{app.prefix}-a")
    destination = dict(deepcopy(test_collection), id=f"{app.prefix}-b")
    response = await app.client.post("/collections", json=source)
    assert response.status_code == 201, response.text
    item = dict(deepcopy(test_item), collection=source["id"])
    response = await app.client.post(f"/collections/{source['id']}/items", json=item)
    assert response.status_code == 201, response.text

    with pytest.raises(HTTPException) as caught:
        await app.database.update_collection(source["id"], destination, refresh=True)
    assert caught.value.status_code == 400

    response = await app.client.get(f"/collections/{source['id']}")
    assert response.status_code == 200, response.text
    response = await app.client.get(
        f"/collections/{source['id']}/items/{test_item['id']}"
    )
    assert response.status_code == 200, response.text
    assert not await app.database.client.exists(
        index=COLLECTIONS_INDEX, id=destination["id"]
    )
    indices = await app.database.client.indices.get(
        index=f"{ITEMS_INDEX_PREFIX}{destination['id']}*", allow_no_indices=True
    )
    assert not list(indices)
