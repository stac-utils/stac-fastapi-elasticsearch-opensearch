"""PATCH media types, recursive merges and atomic invalid-operation regressions."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

pytestmark = [pytest.mark.datetime_filtering, pytest.mark.asyncio]


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
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize("format", ["json-patch", "merge-patch"])
@pytest.mark.parametrize(
    "header",
    [
        "application/{format}+json; charset=utf-8",
        "Application/{format}+JSON; charset=UTF-8",
    ],
)
async def test_patch_accepts_case_insensitive_parameterised_media_type(
    app_client, ctx, txn_client, monkeypatch, validator, resource, format, header
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    path = "/properties/title" if resource == "item" else "/title"
    patch = (
        [{"op": "add", "path": path, "value": "patched"}]
        if format == "json-patch"
        else (
            {"properties": {"title": "patched"}}
            if resource == "item"
            else {"title": "patched"}
        )
    )
    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": header.format(format=format.upper())}
    )
    assert response.status_code == 200, response.text
    stored = (await txn_client.database.client.get(**document))["_source"]
    for result in (response.json(), stored):
        assert (result["properties"] if resource == "item" else result)[
            "title"
        ] == "patched"


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize("initial", ["missing", "scalar", "object"])
@pytest.mark.parametrize("mode", ["nested", "empty", "null-only"])
async def test_merge_patch_recurses_preserves_siblings_and_removes_null(
    app_client, ctx, txn_client, monkeypatch, validator, resource, initial, mode
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    # This extension field stores arbitrary JSON; do not constrain its shape
    # through dynamic search mappings while exercising scalar-to-object merges.
    mapping = {"properties": {"custom": {"type": "object", "enabled": False}}}
    field = "properties" if resource == "item" else "summaries"
    mapping = {"properties": {field: mapping}}
    await txn_client.database.client.indices.put_mapping(
        index=document["index"], body=mapping
    )
    seed = deepcopy(getattr(ctx, resource))
    container = seed[field]
    container["custom_sibling"] = ["untouched"]
    container["custom_remove"] = ["remove me"]
    if initial != "missing":
        container["custom"] = (
            ("scalar" if resource == "item" else ["scalar"])
            if initial == "scalar"
            else {
                "keep": "sibling",
                "remove": "old",
                "nested": {"keep": True, "change": "old"},
            }
        )
    assert (await app_client.put(url, json=seed)).status_code == 200
    changes = {
        "custom": {
            "remove": None,
            "absent": None,
            "nested": {"change": "new", "absent": None},
        },
        "custom_remove": None,
    }
    if mode != "nested":
        changes["custom"] = {} if mode == "empty" else {"remove": None}
    patch = {field: changes}
    response = await app_client.patch(
        url, json=patch, headers={"Content-Type": "application/merge-patch+json"}
    )
    assert response.status_code == 200, response.text
    expected = deepcopy(container["custom"]) if initial == "object" else {}
    if mode != "empty":
        expected.pop("remove", None)
    if mode == "nested":
        expected.setdefault("nested", {})["change"] = "new"
    stored = (await txn_client.database.client.get(**document))["_source"]
    for result in (response.json(), stored):
        merged = result[field]
        assert merged["custom"] == expected
        assert merged["custom_sibling"] == ["untouched"]
        assert "custom_remove" not in merged
        if resource == "item":
            assert merged["datetime"] == seed["properties"]["datetime"]
        else:
            assert result["extent"] == seed["extent"]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
async def test_merge_patch_preserves_literal_member_names(
    app_client, ctx, txn_client, monkeypatch, validator, resource
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    field = "properties" if resource == "item" else "summaries"
    await txn_client.database.client.indices.put_mapping(
        index=document["index"],
        body={
            "properties": {
                field: {"properties": {"custom": {"type": "object", "enabled": False}}}
            }
        },
    )
    seed = deepcopy(getattr(ctx, resource))
    sibling = {"sibling": ["untouched"]}
    seed[field]["custom"] = deepcopy(sibling)
    assert (await app_client.put(url, json=seed)).status_code == 200
    keys = ["0", "01", "-1", "a/b", "~1", "quote'\"", "back\\slash", ""]
    added = {key: {"keep": True, key: index} for index, key in enumerate(keys)}
    expected = {**sibling, **added}
    for changes, wanted in [
        (added, expected),
        ({key: {} for key in keys}, expected),
        ({key: None for key in keys}, sibling),
    ]:
        response = await app_client.patch(
            url,
            json={field: {"custom": changes}},
            headers={"Content-Type": "application/merge-patch+json"},
        )
        assert response.status_code == 200, response.text
        stored = (await txn_client.database.client.get(**document))["_source"]
        for result in (response.json(), stored):
            assert result[field]["custom"] == wanted
            assert "a" not in result[field]["custom"]


@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("resource", ["item", "collection"])
@pytest.mark.parametrize("operation", ["remove", "replace"])
@pytest.mark.parametrize("member", ["absent", "scalar/0"])
async def test_invalid_json_patch_is_400_without_mutation(
    app_client, ctx, txn_client, monkeypatch, validator, resource, operation, member
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url, document = _target(ctx, resource)
    seed = deepcopy(getattr(ctx, resource))
    container = seed["properties"] if resource == "item" else seed
    container["scalar"] = "existing"
    assert (await app_client.put(url, json=seed)).status_code == 200
    before = await txn_client.database.client.get(**document)
    prefix = "/properties" if resource == "item" else ""
    response = await app_client.patch(
        url,
        json=[
            {"op": "add", "path": prefix + "/title", "value": "must not persist"},
            {"op": operation, "path": prefix + "/" + member, "value": 1},
        ],
        headers={"Content-Type": "application/json-patch+json"},
    )
    assert response.status_code == 400, response.text
    after = await txn_client.database.client.get(**document)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


async def test_validation_typeerror_is_not_reclassified(txn_client, ctx, monkeypatch):
    validation = AsyncMock(side_effect=TypeError("validation defect"))
    monkeypatch.setattr(txn_client, "_validate_single_item", validation)
    with pytest.raises(TypeError, match="validation defect"):
        await txn_client._apply_and_validate_patch(
            ctx.item,
            [{"op": "add", "path": "/properties/title", "value": "patched"}],
            "application/json-patch+json",
        )
    validation.assert_awaited_once()
