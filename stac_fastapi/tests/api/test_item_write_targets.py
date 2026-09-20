"""Transaction identity, searchable target, and public visibility regressions."""

import uuid
from copy import deepcopy
from unittest.mock import AsyncMock, Mock

import pytest

from stac_fastapi.core.core import BulkTransactionsClient, TransactionsClient
from stac_fastapi.core.redis_utils import AsyncRedisQueueManager
from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id
from stac_fastapi.sfeos_helpers.search_engine import DatetimeIndexInserter

from ..conftest import refresh_indices

pytestmark = [pytest.mark.asyncio, pytest.mark.datetime_filtering]


@pytest.fixture(autouse=True)
def datetime_mode(request, monkeypatch):
    params = getattr(getattr(request.node, "callspec", None), "params", {})
    if params.get("field") in ("start_datetime", "end_datetime"):
        monkeypatch.setenv("USE_DATETIME", "false")


async def stored(txn_client, item):
    return await txn_client.database.client.get(
        index=index_alias_by_collection_id(item["collection"]),
        id=mk_item_id(item["id"], item["collection"]),
    )


def state(document):
    return document["_source"], document["_version"]


async def send_patch(client, url, body, media):
    return await client.patch(
        url, json=body, headers={"Content-Type": f"application/{media}-patch+json"}
    )


def forbid_pipeline(monkeypatch, txn_client):
    """Fail if rejection reaches preprocessing, validation, queueing, or a write."""
    mocks = []
    for owner, name, factory in [
        (BulkTransactionsClient, "preprocess_item", Mock),
        (TransactionsClient, "_validate_single_item", AsyncMock),
        (TransactionsClient, "_apply_and_validate_patch", AsyncMock),
        (txn_client.database, "create_item", AsyncMock),
        (txn_client.database, "bulk_async", AsyncMock),
    ]:
        mock = factory(side_effect=AssertionError(f"unexpected {name}"))
        monkeypatch.setattr(owner, name, mock)
        mocks.append(mock)
    queue = AsyncMock(side_effect=AssertionError("unexpected enqueue"))
    monkeypatch.setattr("stac_fastapi.core.utilities.queue_items_if_enabled", queue)
    return mocks + [queue]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("field", ["id", "collection"])
async def test_put_identity_mismatch_is_rejected_before_lookup(
    app_client, txn_client, ctx, monkeypatch, validator, field
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    item = deepcopy(ctx.item)
    item[field] = f"different-{uuid.uuid4()}"
    before = await stored(txn_client, ctx.item)
    lookup = AsyncMock(side_effect=AssertionError("identity must precede lookup"))
    with monkeypatch.context() as guard:
        guard.setattr(txn_client.database, "get_item_for_write", lookup)
        calls = forbid_pipeline(guard, txn_client)
        response = await app_client.put(
            f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}",
            json=item,
        )
    assert response.status_code == 400
    lookup.assert_not_called()
    for call in calls:
        call.assert_not_called()
    assert state(await stored(txn_client, ctx.item)) == state(before)
    url = f"/collections/{item['collection']}/items/{item['id']}"
    assert (await app_client.get(url)).status_code == 404


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("queue", ["false", "true"])
@pytest.mark.parametrize("method", ["put", "merge", "json"])
@pytest.mark.parametrize("exists", [False, True])
async def test_missing_target_stops_pipeline(
    app_client, txn_client, ctx, monkeypatch, validator, queue, method, exists
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", queue)
    item = deepcopy(ctx.item)
    item["id"] = f"missing-{uuid.uuid4()}"
    if not exists:
        item["collection"] = f"missing-{uuid.uuid4()}"
    url = f"/collections/{item['collection']}/items/{item['id']}"
    before = await stored(txn_client, ctx.item)
    with monkeypatch.context() as guard:
        calls = forbid_pipeline(guard, txn_client)
        if method == "put":
            response = await app_client.put(url, json=item)
        else:
            patch = {"properties": {"title": "missing"}}
            if method == "json":
                patch = [{"op": "add", "path": "/properties/title", "value": "missing"}]
            response = await send_patch(app_client, url, patch, method)
    assert response.status_code == 404
    for call in calls:
        call.assert_not_called()
    assert (await app_client.get(url)).status_code == 404
    assert state(await stored(txn_client, ctx.item)) == state(before)


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("collection_value", ["absent", None, "matching"])
@pytest.mark.parametrize("method", ["put", "post", "batch"])
async def test_uri_collection_population(
    app_client, txn_client, ctx, monkeypatch, validator, collection_value, method
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    item = deepcopy(ctx.item)
    if method != "put":
        item["id"] = str(uuid.uuid4())
    if collection_value == "absent":
        item.pop("collection")
    elif collection_value is None:
        item["collection"] = None
    item["properties"]["title"] = "collection from URI"
    url = f"/collections/{ctx.collection['id']}/items"
    features = (
        [item, {**deepcopy(item), "id": str(uuid.uuid4())}]
        if method == "batch"
        else [item]
    )
    if method == "put":
        response = await app_client.put(f"{url}/{item['id']}", json=item)
    else:
        body = (
            item
            if method == "post"
            else {"type": "FeatureCollection", "features": features}
        )
        response = await app_client.post(url, json=body)
    assert response.status_code == (200 if method == "put" else 201), response.text
    await refresh_indices(txn_client)
    for feature in features:
        result = await app_client.get(f"{url}/{feature['id']}")
        assert result.status_code == 200
        assert result.json()["collection"] == ctx.collection["id"]
        assert result.json()["properties"]["title"] == "collection from URI"


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("queue", ["false", "true"])
@pytest.mark.parametrize("batch", [False, True])
async def test_post_preflights_all_collection_identities(
    app_client, txn_client, ctx, monkeypatch, validator, queue, batch
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", queue)
    valid = deepcopy(ctx.item)
    valid["id"] = f"valid-{uuid.uuid4()}"
    invalid = deepcopy(valid)
    invalid["id"] = f"invalid-{uuid.uuid4()}"
    invalid["collection"] = f"other-{uuid.uuid4()}"
    body = (
        {"type": "FeatureCollection", "features": [valid, invalid]}
        if batch
        else invalid
    )
    before = await stored(txn_client, ctx.item)
    with monkeypatch.context() as guard:
        calls = forbid_pipeline(guard, txn_client)
        response = await app_client.post(
            f"/collections/{ctx.collection['id']}/items",
            json=body,
        )
    assert response.status_code == 400
    for call in calls:
        call.assert_not_called()
    for item in (valid, invalid):
        url = f"/collections/{ctx.collection['id']}/items/{item['id']}"
        assert (await app_client.get(url)).status_code == 404
    assert state(await stored(txn_client, ctx.item)) == state(before)


async def test_put_requires_searchable_target_after_unrefreshed_create(
    app_client, txn_client, ctx, monkeypatch
):
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "false")
    monkeypatch.setenv("DATABASE_REFRESH", "false")
    item = deepcopy(ctx.item)
    item["id"] = f"unrefreshed-{uuid.uuid4()}"
    url = f"/collections/{item['collection']}/items/{item['id']}"
    alias = index_alias_by_collection_id(ctx.collection["id"])
    indices = txn_client.database.client.indices
    concrete_indices = list((await indices.get_settings(index=alias)).keys())
    prior = await indices.get_settings(index=alias, name="index.refresh_interval")
    for concrete in concrete_indices:
        await indices.put_settings(index=concrete, body={"refresh_interval": "-1"})
    try:
        await txn_client.database.create_item(item=item, refresh=False)
        before = await stored(txn_client, item)
        assert (await app_client.put(url, json=item)).status_code == 404
        assert state(await stored(txn_client, item)) == state(before)
        await indices.refresh(index=alias)
        item["properties"]["title"] = "visible after refresh"
        response = await app_client.put(url, json=item)
        assert response.status_code == 200
        after = await stored(txn_client, item)
        assert after["_source"]["properties"]["title"] == "visible after refresh"
    finally:
        for concrete in concrete_indices:
            settings = prior.get(concrete, {}).get("settings", {}).get("index", {})
            interval = settings.get("refresh_interval")
            await indices.put_settings(
                index=concrete, body={"refresh_interval": interval}
            )
        assert (
            await indices.get_settings(index=alias, name="index.refresh_interval")
            == prior
        )
        await refresh_indices(txn_client)


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("format", ["merge", "json"])
@pytest.mark.parametrize("unhide", ["put", "patch"])
async def test_hidden_item_accepts_put_and_patch(
    app_client,
    catalogs_app_client,
    txn_client,
    ctx,
    monkeypatch,
    validator,
    format,
    unhide,
):
    client = catalogs_app_client
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    monkeypatch.setenv("HIDE_ITEM_PATH", "properties._private.hidden")
    hidden = deepcopy(ctx.item)
    hidden["properties"].update(title="original", _private={"hidden": True})
    url = f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
    assert (await app_client.put(url, json=hidden)).status_code == 200
    hidden["properties"]["title"] = "hidden PUT"
    assert (await app_client.put(url, json=hidden)).status_code == 200
    catalog = {
        "id": str(uuid.uuid4()),
        "type": "Catalog",
        "stac_version": "1.0.0",
        "description": "visibility",
        "links": [],
    }
    assert (await client.post("/catalogs", json=catalog)).status_code == 201
    assert (
        await client.post(
            f"/catalogs/{catalog['id']}/collections",
            json={"id": ctx.collection["id"]},
        )
    ).status_code == 200
    try:
        patch = {"properties": {**hidden["properties"], "title": "hidden PATCH"}}
        if format == "json":
            patch = [
                {"op": "replace", "path": "/properties/title", "value": "hidden PATCH"}
            ]
        response = await send_patch(app_client, url, patch, format)
        assert response.status_code == 200
        result = await txn_client.database.get_item_for_write(
            ctx.collection["id"], ctx.item["id"]
        )
        assert result["properties"]["title"] == "hidden PATCH"
        assert result["properties"]["_private"]["hidden"] is True
        for endpoint in (url, f"/catalogs/{catalog['id']}{url}"):
            assert (await client.get(endpoint)).status_code == 404
        for endpoint in ("/search", f"/collections/{ctx.collection['id']}/items"):
            result = (
                await app_client.get(
                    endpoint, params={"collections": ctx.collection["id"]}
                )
            ).json()
            assert result["features"] == []
            assert result["numberMatched"] == result["numberReturned"] == 0
        hidden["properties"]["_private"]["hidden"] = False
        if unhide == "put":
            response = await app_client.put(url, json=hidden)
        else:
            patch = {"properties": hidden["properties"]}
            if format == "json":
                patch = [
                    {
                        "op": "replace",
                        "path": "/properties/_private/hidden",
                        "value": False,
                    }
                ]
            response = await send_patch(app_client, url, patch, format)
        assert response.status_code == 200
        assert (await app_client.get(url)).json()["properties"]["_private"][
            "hidden"
        ] is False
        assert (await client.get(f"/catalogs/{catalog['id']}{url}")).status_code == 200
        result = (
            await app_client.get(
                "/search", params={"collections": ctx.collection["id"]}
            )
        ).json()
        assert [item["id"] for item in result["features"]] == [ctx.item["id"]]
        assert result["numberMatched"] == result["numberReturned"] == 1
    finally:
        assert (await client.delete(f"/catalogs/{catalog['id']}")).status_code == 204


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("strict", ["false", "true"])
async def test_hidden_put_queue_ack_preserves_storage_until_worker(
    app_client, txn_client, ctx, monkeypatch, validator, strict
):
    monkeypatch.setenv("HIDE_ITEM_PATH", "properties._private.hidden")
    item = deepcopy(ctx.item)
    item["properties"]["_private"] = {"hidden": True}
    url = f"/collections/{item['collection']}/items/{item['id']}"
    assert (await app_client.put(url, json=item)).status_code == 200
    before = await stored(txn_client, item)
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "true")
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    monkeypatch.setenv("VALIDATE_BEFORE_QUEUE", strict)
    item["properties"]["title"] = "queued hidden update"
    manager = await AsyncRedisQueueManager.create()
    try:
        response = await app_client.put(url, json=item)
        assert response.status_code == 202, response.text
        assert response.json()["id"] == item["id"]
        assert response.json()["properties"]["title"] == "queued hidden update"
        pending = await manager.get_pending_items(item["collection"])
        assert len(pending) == 1
        assert pending[0]["properties"]["title"] == "queued hidden update"
        assert pending[0]["properties"]["_private"]["hidden"] is True
        assert state(await stored(txn_client, item)) == state(before)
        assert (await app_client.get(url)).status_code == 404
    finally:
        await manager.remove_item(item["collection"], item["id"])
        await manager.close()


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("queue", ["false", "true"])
@pytest.mark.parametrize("field", ["datetime", "start_datetime", "end_datetime"])
@pytest.mark.parametrize("method", ["put", "merge", "json"])
async def test_write_datetime_immutability_before_queue(
    app_client, txn_client, ctx, monkeypatch, validator, queue, field, method
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", queue)
    monkeypatch.setenv("VALIDATE_BEFORE_QUEUE", "false")
    item = deepcopy(ctx.item)
    before = await stored(txn_client, item)
    item["properties"][field] = item["properties"][field].replace("T12:", "T13:")
    assert item["properties"][field] != ctx.item["properties"].get(field)
    manager = await AsyncRedisQueueManager.create()
    try:
        url = f"/collections/{item['collection']}/items/{item['id']}"
        queued = queue == "true" and method == "put"
        if method == "put":
            response = await app_client.put(url, json=item)
        else:
            patch = {"properties": item["properties"]}
            if method == "json":
                patch = [
                    {
                        "op": "replace",
                        "path": f"/properties/{field}",
                        "value": item["properties"][field],
                    }
                ]
            response = await send_patch(app_client, url, patch, method)
        immutable = isinstance(
            txn_client.database.async_index_inserter, DatetimeIndexInserter
        )
        assert response.status_code == (
            400 if immutable else 202 if queued else 200
        ), response.text
        pending = await manager.get_pending_items(item["collection"])
        after = await stored(txn_client, item)
        if immutable:
            assert "not yet supported" in str(response.json()["detail"])
            assert pending == []
        elif queued:
            assert len(pending) == 1
            assert pending[0]["properties"][field] == item["properties"][field]
        else:
            assert after["_source"]["properties"][field] == item["properties"][field]
        if immutable or queued:
            assert state(after) == state(before)
    finally:
        await manager.remove_item(item["collection"], item["id"])
        await manager.close()


@pytest.mark.parametrize("field", ["datetime", "start_datetime", "end_datetime"])
@pytest.mark.parametrize(
    "case,validator",
    [
        (case, validator)
        for case in (
            "remove copy move move-source replace-properties remove-properties "
            "same-replace same-copy same-move same-properties same-merge copy-out "
            "ordered-copy same-ordered-copy later-copy reset-copy repeat-copy alias-copy bad-scalar bad-null"
        ).split()
        for validator in (
            ("false",)
            if case.startswith("bad") or case == "same-merge"
            else ("false", "true")
        )
    ],
)
async def test_json_patch_datetime_effects(
    app_client, txn_client, ctx, monkeypatch, validator, field, case
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    before = await stored(txn_client, ctx.item)
    properties = deepcopy(ctx.item["properties"])
    path, scratch = f"/properties/{field}", "/properties/date_source"
    other = (
        "/properties/datetime" if field != "datetime" else "/properties/start_datetime"
    )
    same = case.startswith("same-") or case == "copy-out"
    value = properties[field] if same else properties[field].replace("T12:", "T13:")
    patch = [{"op": "replace", "path": path, "value": value}]
    if case == "remove":
        patch = [{"op": "remove", "path": path}]
    elif case in ("copy", "move", "same-copy", "same-move", "copy-out", "move-source"):
        op = "move" if "move" in case else "copy"
        destination = scratch if case in ("copy-out", "move-source") else path
        origin = path if same or case == "move-source" else other
        patch = [{"op": op, "path": destination, "from": origin}]
        if case == "move":
            patch.insert(0, {"op": "copy", "path": scratch, "from": origin})
            patch[-1]["from"] = scratch
    elif "properties" in case:
        properties[field] = value
        properties["title"] = "ancestor metadata update"
        patch = [{"op": "replace", "path": "/properties", "value": properties}]
        if case == "remove-properties":
            patch = [{"op": "remove", "path": "/properties"}]
    elif "ordered" in case or case in ("later-copy", "repeat-copy"):
        patch = [
            {"op": "copy", "path": scratch, "from": path if not same else other},
            {"op": "replace", "path": scratch, "value": value},
            {"op": "copy", "path": path, "from": scratch},
        ]
    if case == "repeat-copy":
        patch.insert(2, deepcopy(patch[0]))
    if case in ("reset-copy", "alias-copy"):
        scratch = "/properties/_private"
        obj = {"value": properties[field]}
        patch = [
            {"op": "add", "path": scratch, "value": obj},
            {"op": "replace", "path": scratch + "/value", "value": value},
            {"op": "add", "path": scratch, "value": obj},
            {"op": "copy", "path": path, "from": scratch + "/value"},
        ]
    if case == "alias-copy":
        patch[2]["path"] += ":"
        patch[-1]["from"] = scratch + ":/value"
    if case == "later-copy":
        patch[0] = {"op": "add", "path": scratch, "value": properties[field]}
        patch[1], patch[2] = patch[2], patch[1]
    malformed = case.startswith("bad")
    if malformed:
        bad = None if case == "bad-null" else "x"
        patch = [
            {"op": "add", "path": scratch, "value": bad},
            {"op": "remove", "path": scratch + "/x"},
        ]
    media = "merge" if case == "same-merge" else "json"
    if media == "merge":
        patch = {"properties": {}}
    url = f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    response = await send_patch(app_client, url, patch, media)
    immutable = isinstance(
        txn_client.database.async_index_inserter, DatetimeIndexInserter
    )
    removed = case in ("remove", "move-source", "remove-properties")
    if case in ("later-copy", "reset-copy", "repeat-copy", "alias-copy"):
        same = validator == "true"
    rejected = malformed or (
        not same and (immutable or (removed and validator == "true"))
    )
    assert response.status_code == (400 if rejected else 200), response.text
    after = await stored(txn_client, ctx.item)
    if rejected:
        assert state(after) == state(before)
        if immutable and validator == "false" and not malformed:
            assert "not yet supported" in str(response.json()["detail"])
    else:
        result = after["_source"].get("properties", {})
        if removed:
            assert field not in result
        else:
            expected = properties[field] if same else value
            if case in ("copy", "move"):
                expected = ctx.item["properties"][other.rsplit("/", 1)[1]]
            assert result[field] == expected
        if "properties" in case and not removed:
            assert result["title"] == "ancestor metadata update"
        if case in ("copy-out", "move-source"):
            assert result["date_source"] == ctx.item["properties"][field]
        if case not in ("same-copy", "same-move", "same-replace", "same-merge"):
            assert after["_version"] > before["_version"]


@pytest.mark.parametrize("field", ["datetime", "start_datetime", "end_datetime"])
async def test_datetime_copy_checks_live_source_atomically(
    app_client, txn_client, ctx, monkeypatch, field
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    url = f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    original = ctx.item["properties"][field]
    changed = original.replace("T12:", "T13:")
    patch = [{"op": "add", "path": "/properties/date_source", "value": original}]
    assert (await send_patch(app_client, url, patch, "json")).status_code == 200
    client_type = type(txn_client.database.client)
    update = client_type.update
    concurrent = []

    async def race(client, **kwargs):
        await update(
            client,
            index=kwargs["index"],
            id=kwargs["id"],
            body={"doc": {"properties": {"date_source": changed}}},
            refresh=True,
        )
        concurrent.append(await stored(txn_client, ctx.item))
        return await update(client, **kwargs)

    monkeypatch.setattr(client_type, "update", race)
    patch = [
        {
            "op": "copy",
            "path": f"/properties/{field}",
            "from": "/properties/date_source",
        }
    ]
    response = await send_patch(app_client, url, patch, "json")
    after = await stored(txn_client, ctx.item)
    assert len(concurrent) == 1, response.text
    if isinstance(txn_client.database.async_index_inserter, DatetimeIndexInserter):
        assert response.status_code == 400, response.text
        assert "not yet supported" in str(response.json()["detail"])
        assert state(after) == state(concurrent[0])
    else:
        assert response.status_code == 200, response.text
        assert after["_source"]["properties"][field] == changed
        assert after["_version"] == concurrent[0]["_version"] + 1
