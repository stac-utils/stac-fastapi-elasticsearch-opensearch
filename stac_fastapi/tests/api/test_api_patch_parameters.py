"""HTTP PATCH regressions for independent script values and atomic failures."""

import pytest

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX, ITEMS_INDEX_PREFIX


async def _stored_document(txn_client, resource, collection_id):
    if resource == "collection":
        document = await txn_client.database.client.get(
            index=COLLECTIONS_INDEX, id=collection_id
        )
    else:
        result = await txn_client.database.client.search(
            index=f"{ITEMS_INDEX_PREFIX}*",
            body={"query": {"match_all": {}}, "version": True},
        )
        assert len(result["hits"]["hits"]) == 1
        document = result["hits"]["hits"][0]
    return document["_source"], document["_version"]


@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize("patch_kind", ["json", "merge"])
async def test_http_patch_preserves_colliding_values(
    app_client, ctx, txn_client, monkeypatch, validator, resource, patch_kind
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    collection_id = ctx.collection["id"]
    path = f"/collections/{collection_id}"
    expected_type = "Feature" if resource == "item" else "Collection"
    fields = {"type": expected_type, "t:ype": "distinct-type"}
    if resource == "item":
        path += f'/items/{ctx.item["id"]}'
        fields.update({"name": "plain", "n:ame": "colon"})
        pairs = [
            ("/type", expected_type),
            ("/t:ype", "distinct-type"),
            ("/name", "plain"),
            ("/n:ame", "colon"),
        ]
    else:
        fields.update(
            {
                "id": collection_id,
                "i:d": "distinct-id",
                "name": "plain",
                "n:ame": "colon",
            }
        )
        pairs = [(f"/{key}", value) for key, value in fields.items()]
    if patch_kind == "merge":
        nest = "properties" if resource == "item" else "summaries"
        fields[nest] = {"name": ["plain"], "n:ame": ["colon"]}
        if resource == "item":
            fields[nest].update(ctx.item["properties"])
    patch = (
        [{"op": "add", "path": key, "value": value} for key, value in pairs]
        if patch_kind == "json"
        else fields
    )

    response = await app_client.patch(
        path,
        json=patch,
        headers={"Content-Type": f"application/{patch_kind}-patch+json"},
    )

    assert response.status_code == 200, response.text
    source, _ = await _stored_document(txn_client, resource, collection_id)
    assert source["type"] == expected_type
    assert source["id"] == (ctx.item["id"] if resource == "item" else collection_id)
    if patch_kind == "json":
        assert source["t:ype"] == "distinct-type"
    ordinary = source[nest] if patch_kind == "merge" else source
    assert ordinary["name"] == (["plain"] if patch_kind == "merge" else "plain")
    assert ordinary["n:ame"] == (["colon"] if patch_kind == "merge" else "colon")
    if resource == "collection":
        assert source["id"] == collection_id
        if patch_kind == "json":
            assert source["i:d"] == "distinct-id"


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["item", "collection"])
async def test_http_failed_test_preserves_source_and_version(
    app_client, ctx, txn_client, monkeypatch, resource
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    collection_id = ctx.collection["id"]
    path = f"/collections/{collection_id}"
    value_path = "/description"
    if resource == "item":
        path += f'/items/{ctx.item["id"]}'
        value_path = "/properties/name"
    before = await _stored_document(txn_client, resource, collection_id)

    response = await app_client.patch(
        path,
        json=[
            {"op": "add", "path": value_path, "value": "must-not-persist"},
            {"op": "test", "path": "/type", "value": "wrong-type"},
        ],
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert response.status_code == 400
    assert await _stored_document(txn_client, resource, collection_id) == before
