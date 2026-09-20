"""PATCH resource identity and type guard regressions."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from stac_fastapi.core.core import patch_changes_field
from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

from ..conftest import create_collection, create_item

pytestmark = [pytest.mark.datetime_filtering, pytest.mark.asyncio]
FIELDS = [
    ("item", "id"),
    ("item", "collection"),
    ("collection", "id"),
    ("collection", "type"),
]


def _target(ctx, resource):
    if resource == "collection":
        return f"/collections/{ctx.collection['id']}", dict(
            index=COLLECTIONS_INDEX, id=ctx.collection["id"]
        )
    return (
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}",
        dict(
            index=index_alias_by_collection_id(ctx.item["collection"]),
            id=mk_item_id(ctx.item["id"], ctx.item["collection"]),
        ),
    )


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource,field", FIELDS)
@pytest.mark.parametrize(
    "operation",
    [
        "add",
        "replace",
        "null",
        "remove",
        "move-from",
        "copy-to",
        "move-to",
        "root",
        "root-remove",
        "merge",
        "merge-null",
    ],
)
async def test_patch_rejects_protected_changes(
    app_client, ctx, txn_client, monkeypatch, validator, resource, field, operation
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    _, item_document = _target(ctx, "item")
    before = await txn_client.database.client.get(**document)
    item_before = await txn_client.database.client.get(**item_document)
    replacement = deepcopy(getattr(ctx, resource))
    value = "Catalog" if field == "type" else "renamed"
    metadata = "/properties/title" if resource == "item" else "/title"
    content_type = "json-patch"
    if operation.startswith("merge"):
        content_type = "merge-patch"
        patch = {
            field: None if operation == "merge-null" else value,
            "title": "must not persist",
        }
    else:
        op = {"op": operation, "path": f"/{field}", "value": value}
        if operation == "null":
            op.update(op="replace", value=None)
        elif operation == "move-from":
            op = {"op": "move", "from": f"/{field}", "path": metadata}
        elif operation in ("copy-to", "move-to"):
            op = {
                "op": operation.split("-")[0],
                "from": "/stac_version",
                "path": f"/{field}",
            }
        elif operation.startswith("root"):
            replacement.pop(
                field
            ) if operation == "root-remove" else replacement.update({field: value})
            op = {"op": "replace", "path": "", "value": replacement}
        patch = [{"op": "add", "path": metadata, "value": "must not persist"}, op]
    lookup = AsyncMock(side_effect=AssertionError("Invalid identity reached lookup"))
    with monkeypatch.context() as guarded:
        guarded.setattr(type(txn_client.database), "find_collection", lookup)
        response = await app_client.patch(
            url,
            json=patch,
            headers={"Content-Type": f"application/{content_type}+json"},
        )
    assert response.status_code == 400, response.text
    lookup.assert_not_awaited()
    for target, original in ((document, before), (item_document, item_before)):
        after = await txn_client.database.client.get(**target)
        assert after["_source"] == original["_source"]
        assert after["_version"] == original["_version"]
    stored = await app_client.get(url)
    assert stored.status_code == 200
    assert stored.json()[field] == getattr(ctx, resource)[field]
    assert (await app_client.get("/collections/renamed")).status_code == 404
    assert (
        await app_client.get(f"/collections/{ctx.collection['id']}/items/renamed")
    ).status_code == 404
    assert not await txn_client.database.client.exists(
        index=index_alias_by_collection_id("renamed"),
        id=mk_item_id(ctx.item["id"], "renamed"),
    )


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource,field", FIELDS)
@pytest.mark.parametrize("operation", ["add", "replace", "copy", "merge"])
async def test_patch_preserving_protected_fields_succeeds(
    app_client, ctx, txn_client, monkeypatch, validator, resource, field, operation
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    original = getattr(ctx, resource)
    value = original[field]
    if operation == "merge":
        patch, content_type = {field: value}, "merge-patch"
    else:
        op = {"op": operation, "path": f"/{field}", "value": value}
        if operation == "copy":
            op = {
                "op": "copy",
                "from": f"/{field}",
                "path": "/properties/original" if resource == "item" else "/title",
            }
        patch, content_type = [op], "json-patch"
    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": f"application/{content_type}+json"}
    )
    assert response.status_code == 200, response.text
    stored = (await txn_client.database.client.get(**document))["_source"]
    for protected in ["id", "collection"] if resource == "item" else ["id", "type"]:
        assert stored[protected] == original[protected]
    if operation == "copy":
        assert (
            stored["properties"]["original"] if resource == "item" else stored["title"]
        ) == value


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("content_type", ["merge-patch", "json-patch"])
@pytest.mark.parametrize("target_type", [None, "Catalog", "Feature"])
async def test_patch_missing_or_wrong_type_collection(
    app_client, ctx, txn_client, monkeypatch, validator, content_type, target_type
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    document = dict(index=COLLECTIONS_INDEX, id="wrong-patch-target")
    if target_type:
        await txn_client.database.client.index(
            **document, body={"id": document["id"], "type": target_type}, refresh=True
        )
        before = await txn_client.database.client.get(**document)
    patch = (
        {"title": "must not persist"}
        if content_type == "merge-patch"
        else [{"op": "add", "path": "/title", "value": "must not persist"}]
    )
    response = await app_client.patch(
        f"/collections/{document['id']}",
        json=patch,
        headers={"Content-Type": f"application/{content_type}+json"},
    )
    assert response.status_code == 404, response.text
    if target_type:
        after = await txn_client.database.client.get(**document)
        assert after["_source"] == before["_source"]
        assert after["_version"] == before["_version"]
    else:
        assert not await txn_client.database.client.exists(**document)


@pytest.mark.parametrize("validator", ["false", "true"])
async def test_collection_metadata_is_not_a_rename(
    app_client, ctx, txn_client, monkeypatch, validator
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, "collection")
    _, item_document = _target(ctx, "item")
    before = await txn_client.database.client.get(**item_document)
    for operation, value in [("add", "custom metadata"), ("replace", "new metadata")]:
        response = await app_client.patch(
            url,
            json=[{"op": operation, "path": "/collection", "value": value}],
            headers={"Content-Type": "application/json-patch+json"},
        )
        assert response.status_code == 200, response.text
        stored = (await txn_client.database.client.get(**document))["_source"]
        assert stored["id"] == ctx.collection["id"]
        assert stored["collection"] == value
    after = await txn_client.database.client.get(**item_document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "operation,path",
    [("add", "collection"), ("replace", "collection"), ("replace", "/t:ype")],
)
async def test_patch_rejects_backend_aliases(
    app_client, ctx, txn_client, monkeypatch, validator, operation, path
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, "collection")
    before = await txn_client.database.client.get(**document)
    response = await app_client.patch(
        url,
        json=[{"op": operation, "path": path, "value": "renamed"}],
        headers={"Content-Type": "application/json-patch+json"},
    )
    assert response.status_code == 400
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]
    assert patch_changes_field(
        [{"op": "replace", "path": "/t:ype", "value": "Catalog"}], "type", "Collection"
    )


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource,field", FIELDS[:3])
@pytest.mark.parametrize("format", ["json-patch", "merge-patch"])
async def test_patch_cannot_overwrite_existing_alternate(
    app_client, ctx, txn_client, monkeypatch, validator, resource, field, format
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    alternate_collection = {**deepcopy(ctx.collection), "id": "alternate"}
    await create_collection(txn_client, alternate_collection)
    alternate_item = deepcopy(ctx.item)
    alternate_item[field if resource == "item" else "collection"] = "alternate"
    await create_item(txn_client, alternate_item)
    documents = [
        _target(ctx, resource)[1],
        dict(index=COLLECTIONS_INDEX, id="alternate"),
        dict(
            index=index_alias_by_collection_id(alternate_item["collection"]),
            id=mk_item_id(alternate_item["id"], alternate_item["collection"]),
        ),
    ]
    before = [
        await txn_client.database.client.get(**document) for document in documents
    ]
    patch = (
        {field: "alternate"}
        if format == "merge-patch"
        else [{"op": "replace", "path": f"/{field}", "value": "alternate"}]
    )
    response = await app_client.patch(
        _target(ctx, resource)[0],
        json=patch,
        headers={"Content-Type": f"application/{format}+json"},
    )
    assert response.status_code == 400
    for document, original in zip(documents, before):
        after = await txn_client.database.client.get(**document)
        assert after["_source"] == original["_source"]
        assert after["_version"] == original["_version"]
