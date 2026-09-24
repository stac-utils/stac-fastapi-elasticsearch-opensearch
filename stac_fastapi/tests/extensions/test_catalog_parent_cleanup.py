"""Raw-storage and failure contracts for prospective catalog parent cleanup."""

import copy
import logging
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from stac_fastapi.sfeos_helpers.database import catalogs
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX
from stac_fastapi.types.errors import ConflictError


def update_response(**overrides):
    return (
        dict(
            total=1,
            updated=1,
            deleted=0,
            noops=0,
            version_conflicts=0,
            batches=1,
            timed_out=False,
            failures=[],
        )
        | overrides
    )


def count_response(**overrides):
    return {
        "count": 0,
        "_shards": {"total": 1, "successful": 1, "failed": 0},
    } | overrides


def fake_client():
    return SimpleNamespace(
        indices=SimpleNamespace(
            refresh=AsyncMock(
                return_value={"_shards": {"total": 2, "successful": 1, "failed": 0}}
            )
        ),
        update_by_query=AsyncMock(return_value=update_response()),
        count=AsyncMock(return_value=count_response()),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
async def test_cleanup_shared_request_contract(wrapped):
    client = fake_client()
    if wrapped:
        for method in [client.indices.refresh, client.update_by_query, client.count]:
            method.return_value = SimpleNamespace(body=method.return_value)
    await catalogs.unlink_catalog_children_shared(client, "parent")
    args = client.update_by_query.call_args.kwargs
    assert args["index"] == COLLECTIONS_INDEX
    assert args["scroll_size"] == 500
    assert args["conflicts"] == "proceed"
    assert args["wait_for_completion"] is True
    assert args["refresh"] is True
    assert args["body"]["script"]["params"] == {"parent_id": "parent"}
    assert args["body"]["query"] == {
        "bool": {
            "filter": [
                {"term": {"parent_ids": "parent"}},
                {"terms": {"type": ["Catalog", "Collection"]}},
            ]
        }
    }
    client.count.assert_awaited_once_with(
        index=COLLECTIONS_INDEX, body={"query": args["body"]["query"]}
    )
    client.indices.refresh.assert_awaited_once_with(index=COLLECTIONS_INDEX)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        None,
        {},
        {"task": "task-id"},
        update_response(timed_out=True),
        update_response(timed_out=None),
        update_response(total=True),
        update_response(updated=-1),
        update_response(total=2),
        update_response(batches=0),
        update_response(deleted=1),
        update_response(failures=None),
        update_response(terminated_early=True),
        update_response(
            failures=[{"status": 500, "cause": {"type": "script_exception"}}]
        ),
        update_response(failures=[{"status": 409, "cause": {"type": "unrelated"}}]),
        update_response(
            failures=[
                {"status": 409, "cause": {"type": "version_conflict_engine_exception"}}
            ]
        ),
        *[
            {key: value for key, value in update_response().items() if key != missing}
            for missing in update_response()
        ],
    ],
)
async def test_cleanup_rejects_failed_or_incomplete_updates(response):
    client = fake_client()
    client.update_by_query.return_value = response
    with pytest.raises(RuntimeError):
        await catalogs.unlink_catalog_children_shared(client, "parent")
    client.count.assert_not_awaited()
    assert client.update_by_query.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        None,
        {},
        {"count": 0},
        count_response(count=True),
        count_response(count=-1),
        count_response(timed_out=True),
        count_response(terminated_early=True),
        count_response(_shards={"total": 2, "successful": 1, "failed": 0}),
        count_response(_shards={"total": 2, "successful": 1, "failed": 1}),
        count_response(_shards={"total": 0, "successful": 0, "failed": 0}),
        count_response(_shards={"total": 1, "successful": 1}),
        count_response(_shards={"total": 1, "successful": 1, "failed": False}),
    ],
)
async def test_cleanup_rejects_unverified_counts(response):
    client = fake_client()
    client.count.return_value = response
    with pytest.raises(RuntimeError):
        await catalogs.unlink_catalog_children_shared(client, "parent")
    assert client.update_by_query.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["refresh", "update_by_query", "count"])
async def test_cleanup_propagates_transport_errors(method):
    client = fake_client()
    mocked = client.indices.refresh if method == "refresh" else getattr(client, method)
    mocked.side_effect = RuntimeError("backend failed")
    with pytest.raises(RuntimeError, match="backend failed"):
        await catalogs.unlink_catalog_children_shared(client, "parent")


@pytest.mark.asyncio
async def test_cleanup_rejects_refresh_shard_failures():
    client = fake_client()
    client.indices.refresh.return_value = {
        "_shards": {"total": 2, "successful": 1, "failed": 1}
    }
    with pytest.raises(RuntimeError):
        await catalogs.unlink_catalog_children_shared(client, "parent")
    client.update_by_query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
@pytest.mark.parametrize("exhausted", [False, True])
async def test_cleanup_retries_conflicts_and_remaining_edges(conflict, exhausted):
    client = fake_client()
    update = (
        update_response(updated=0, version_conflicts=1)
        if conflict
        else update_response()
    )
    client.update_by_query.side_effect = [
        update,
        update,
        update if exhausted else update_response(),
    ]
    client.count.side_effect = [
        count_response(count=1),
        count_response(count=1),
        count_response(count=1 if exhausted else 0),
    ]
    if exhausted:
        with pytest.raises(ConflictError):
            await catalogs.unlink_catalog_children_shared(client, "parent")
    else:
        await catalogs.unlink_catalog_children_shared(client, "parent")
    assert client.indices.refresh.await_count == 3
    assert client.update_by_query.await_count == 3
    assert client.count.await_count == 3


@pytest.mark.asyncio
async def test_cleanup_accepts_conflict_failure_then_retries():
    client = fake_client()
    client.update_by_query.side_effect = [
        update_response(
            updated=0,
            version_conflicts=1,
            failures=[
                {"status": 409, "cause": {"type": "version_conflict_engine_exception"}}
            ],
        ),
        update_response(),
    ]
    await catalogs.unlink_catalog_children_shared(client, "parent")
    assert client.update_by_query.await_count == 2


@pytest.mark.asyncio
async def test_delete_cleans_direct_edges_without_data_loss(
    catalogs_app_client, txn_client, ctx, load_test_data, monkeypatch
):
    client = txn_client.database.client
    parent_id = "cleanup-" + str(uuid.uuid4())
    parent = load_test_data("test_catalog.json") | {"id": parent_id}
    assert (await catalogs_app_client.post("/catalogs", json=parent)).status_code == 201
    monkeypatch.setattr(catalogs, "_PARENT_CLEANUP_BATCH_SIZE", 2)
    originals = {}
    expected = {}
    for kind in ["Catalog", "Collection"]:
        for suffix, parents in [
            ("single", [parent_id]),
            ("multi", ["first", parent_id, "last"]),
            ("duplicate", [parent_id, "first", parent_id, "last", parent_id]),
            ("empty", []),
            ("missing", None),
            ("unrelated", ["first"]),
        ]:
            resource_id = parent_id + kind + suffix
            doc = load_test_data(
                "test_catalog.json" if kind == "Catalog" else "test_collection.json"
            )
            doc.update(
                id=resource_id,
                type=kind,
                title="Keep this title",
                custom={"retained": [1, 2]},
            )
            if parents is not None:
                doc["parent_ids"] = parents
            else:
                doc.pop("parent_ids", None)
            originals[resource_id] = doc
            expected[resource_id] = copy.deepcopy(doc)
            if parents is not None:
                expected[resource_id]["parent_ids"] = [
                    p for p in parents if p != parent_id
                ]
    child_id = parent_id + "Catalogsingle"
    grandchild_id = parent_id + "grandchild"
    originals[grandchild_id] = load_test_data("test_catalog.json") | {
        "id": grandchild_id,
        "parent_ids": [child_id],
    }
    expected[grandchild_id] = copy.deepcopy(originals[grandchild_id])
    # Historical typeless/other-type documents are outside this prospective cleanup.
    for kind in [None, "Other"]:
        resource_id = parent_id + str(kind)
        doc = {"id": resource_id, "parent_ids": [parent_id]}
        if kind:
            doc["type"] = kind
        originals[resource_id] = doc
        expected[resource_id] = copy.deepcopy(doc)
    item_before = (
        await catalogs_app_client.get(
            f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
        )
    ).json()
    collection_before = (
        await client.get(index=COLLECTIONS_INDEX, id=ctx.collection["id"])
    )["_source"]
    originals[ctx.collection["id"]] = collection_before | {"parent_ids": [parent_id]}
    expected[ctx.collection["id"]] = collection_before | {"parent_ids": []}
    settings = await client.indices.get_settings(index=COLLECTIONS_INDEX)
    settings_body = dict(getattr(settings, "body", settings))
    index_settings = next(iter(settings_body.values()))["settings"]["index"]
    original_refresh = index_settings.get("refresh_interval")
    # Disable periodic refresh so the pre-selection refresh must expose acknowledged writes.
    await client.indices.put_settings(
        index=COLLECTIONS_INDEX, body={"index": {"refresh_interval": "-1"}}
    )
    real_update = client.update_by_query
    batch_responses = []

    async def record_update(**kwargs):
        result = await real_update(**kwargs)
        batch_responses.append(dict(getattr(result, "body", result)))
        return result

    try:
        for resource_id, doc in originals.items():
            await client.index(
                index=COLLECTIONS_INDEX, id=resource_id, body=doc, refresh=False
            )
        with patch.object(
            client, "update_by_query", new_callable=AsyncMock, side_effect=record_update
        ) as update:
            await txn_client.database.delete_catalog(parent_id, refresh=True)
        assert (
            await catalogs_app_client.get(f"/catalogs/{parent_id}")
        ).status_code == 404
        assert update.call_args.kwargs["scroll_size"] == 2
        assert batch_responses[0]["batches"] >= 4
        for resource_id, doc in expected.items():
            assert (await client.get(index=COLLECTIONS_INDEX, id=resource_id))[
                "_source"
            ] == doc
        assert (
            await catalogs_app_client.get(
                f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
            )
        ).json() == item_before
        root_child = (await catalogs_app_client.get(f"/catalogs/{child_id}")).json()
        assert any(
            link["rel"] == "parent" and link["href"].rstrip("/") == "http://test-server"
            for link in root_child["links"]
        )
        assert (
            await catalogs_app_client.post("/catalogs", json=parent)
        ).status_code == 201
        children = await catalogs_app_client.get(f"/catalogs/{parent_id}/children")
        assert children.status_code == 200
        # Only the deliberately unsupported historical documents remain linked.
        assert {child["id"] for child in children.json()["children"]} == {
            parent_id + "None",
            parent_id + "Other",
        }
        for path in ["catalogs", "collections"]:
            response = await catalogs_app_client.get(f"/catalogs/{parent_id}/{path}")
            assert response.status_code == 200
            assert response.json()[path] == []
    finally:
        await client.indices.put_settings(
            index=COLLECTIONS_INDEX,
            body={"index": {"refresh_interval": original_refresh}},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["timeout", "partial", "malformed", "count", "conflict", "remaining"]
)
async def test_failed_cleanup_retains_parent_and_retry_preserves_updates(
    catalogs_app_client, txn_client, load_test_data, failure, caplog
):
    client = txn_client.database.client
    parent_id = "retry-" + str(uuid.uuid4())
    parent = load_test_data("test_catalog.json") | {"id": parent_id}
    assert (await catalogs_app_client.post("/catalogs", json=parent)).status_code == 201
    children = [parent_id + str(i) for i in range(2)]
    for child in children:
        await client.index(
            index=COLLECTIONS_INDEX,
            id=child,
            body={
                "id": child,
                "type": "Catalog",
                "parent_ids": [parent_id, "other"],
                "title": "Original",
            },
            refresh=True,
        )
    real_update = client.update_by_query

    async def partial_update(**kwargs):
        # One real successful field update precedes the injected partial response.
        body = copy.deepcopy(kwargs["body"])
        body["query"]["bool"]["filter"].append({"term": {"id": children[0]}})
        result = await real_update(**(kwargs | {"body": body}))
        result = dict(getattr(result, "body", result))
        if failure == "timeout":
            result["timed_out"] = True
        if failure == "partial":
            result["failures"] = [
                {"status": 500, "cause": {"type": "script_exception"}}
            ]
        if failure == "malformed":
            result.pop("total")
        if failure == "conflict":
            result.update(total=result["updated"] + 1, version_conflicts=1, batches=1)
        return result

    caplog.set_level(logging.ERROR)
    with patch.object(
        client, "update_by_query", new_callable=AsyncMock, side_effect=partial_update
    ), patch.object(
        client, "delete", new_callable=AsyncMock, wraps=client.delete
    ) as delete:
        if failure == "count":
            with patch.object(
                client,
                "count",
                new_callable=AsyncMock,
                return_value=count_response(
                    _shards={"total": 2, "successful": 1, "failed": 1}
                ),
            ):
                with pytest.raises(RuntimeError):
                    await txn_client.database.delete_catalog(parent_id, refresh=True)
        else:
            error = (
                ConflictError if failure in ["conflict", "remaining"] else RuntimeError
            )
            with pytest.raises(error):
                await txn_client.database.delete_catalog(parent_id, refresh=True)
        delete.assert_not_awaited()
    assert (await client.get(index=COLLECTIONS_INDEX, id=parent_id))["_source"][
        "type"
    ] == "Catalog"
    assert (await client.get(index=COLLECTIONS_INDEX, id=children[0]))["_source"][
        "parent_ids"
    ] == ["other"]
    assert (await client.get(index=COLLECTIONS_INDEX, id=children[1]))["_source"][
        "parent_ids"
    ] == [parent_id, "other"]
    assert any(
        "Error deleting catalog" in r.message and r.exc_info for r in caplog.records
    ) is (failure not in ["conflict", "remaining"])
    await client.update(
        index=COLLECTIONS_INDEX,
        id=children[0],
        body={"doc": {"title": "Updated after partial cleanup"}},
        refresh=True,
    )
    assert (
        await catalogs_app_client.delete(f"/catalogs/{parent_id}")
    ).status_code == 204
    for child in children:
        assert (await client.get(index=COLLECTIONS_INDEX, id=child))["_source"][
            "parent_ids"
        ] == ["other"]
    assert (await client.get(index=COLLECTIONS_INDEX, id=children[0]))["_source"][
        "title"
    ] == "Updated after partial cleanup"


@pytest.mark.asyncio
async def test_invalid_catalog_ids_never_start_cleanup(
    catalogs_app_client, txn_client, ctx
):
    with patch(
        f"{type(txn_client.database).__module__}.unlink_catalog_children_shared",
        new_callable=AsyncMock,
    ) as cleanup:
        for resource_id in ["missing-" + str(uuid.uuid4()), ctx.collection["id"]]:
            assert (
                await catalogs_app_client.delete(f"/catalogs/{resource_id}")
            ).status_code == 404
        cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_scalar_parent_list_fails_safely(
    catalogs_app_client, txn_client, load_test_data
):
    client = txn_client.database.client
    parent_id = "scalar-" + str(uuid.uuid4())
    parent = load_test_data("test_catalog.json") | {"id": parent_id}
    assert (await catalogs_app_client.post("/catalogs", json=parent)).status_code == 201
    await client.index(
        index=COLLECTIONS_INDEX,
        id=parent_id + "child",
        body={"type": "Catalog", "parent_ids": parent_id},
        refresh=True,
    )
    with pytest.raises(Exception):
        await txn_client.database.delete_catalog(parent_id, refresh=True)
    assert (await txn_client.database.find_catalog(parent_id))["id"] == parent_id


@pytest.mark.asyncio
async def test_cleanup_exhaustion_returns_409(
    catalogs_app_client, txn_client, load_test_data
):
    parent_id = "conflict-" + str(uuid.uuid4())
    parent = load_test_data("test_catalog.json") | {"id": parent_id}
    assert (await catalogs_app_client.post("/catalogs", json=parent)).status_code == 201
    # Patch the client class because the API owns a distinct database instance.
    with patch.object(
        type(txn_client.database.client),
        "update_by_query",
        new_callable=AsyncMock,
        return_value=update_response(updated=0, version_conflicts=1),
    ) as update:
        response = await catalogs_app_client.delete(f"/catalogs/{parent_id}")
    assert response.status_code == 409
    assert update.await_count == 3
    assert (await txn_client.database.find_catalog(parent_id))["id"] == parent_id
    assert (
        await catalogs_app_client.delete(f"/catalogs/{parent_id}")
    ).status_code == 204
