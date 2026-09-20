"""PATCH resource identity and type guard regressions."""

import pytest

from stac_fastapi.core.core import patch_changes_field
from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id, mk_item_id

ITEM_IDENTITY_PATCHES = [
    [{"op": "replace", "path": "/id", "value": "renamed"}],
    [{"op": "remove", "path": "/id"}],
    [{"op": "move", "from": "/id", "path": "/properties/moved_id"}],
    [{"op": "replace", "path": "", "value": {"id": "renamed"}}],
]


async def _stored_item(txn_client, item):
    """Read the raw item for no-mutation assertions."""
    return await txn_client.database.client.get(
        index=index_alias_by_collection_id(item["collection"]),
        id=mk_item_id(item["id"], item["collection"]),
    )


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize("operations", ITEM_IDENTITY_PATCHES)
async def test_item_patch_rejects_identity_changes(
    app_client, ctx, txn_client, monkeypatch, validator, operations
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    before = await _stored_item(txn_client, ctx.item)
    response = await app_client.patch(
        url,
        json=operations,
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert response.status_code == 400
    after = await _stored_item(txn_client, ctx.item)
    assert after["_source"] == before["_source"]
    assert after["_version"] == before["_version"]


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
async def test_item_patch_rejects_merge_identity_changes(
    app_client, ctx, monkeypatch, validator
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    response = await app_client.patch(
        url,
        json={"id": "renamed"},
        headers={"Content-Type": "application/merge-patch+json"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "operations",
    [
        [{"op": "replace", "path": "/id", "value": "renamed"}],
        [{"op": "replace", "path": "", "value": {"id": "renamed"}}],
        [{"op": "replace", "path": "/type", "value": "Catalog"}],
        [{"op": "replace", "path": "/t:ype", "value": "Catalog"}],
        [{"op": "replace", "path": "collection", "value": "renamed"}],
    ],
)
async def test_collection_patch_rejects_identity_type_and_legacy_alias(
    app_client, ctx, monkeypatch, validator, operations
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    url = f"/collections/{ctx.collection['id']}"
    response = await app_client.patch(
        url,
        json=operations,
        headers={"Content-Type": "application/json-patch+json"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
async def test_patch_missing_collection_target_returns_404(
    app_client, monkeypatch, validator
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    response = await app_client.patch(
        "/collections/missing-patch-collection",
        json={"title": "not found"},
        headers={"Content-Type": "application/merge-patch+json"},
    )
    assert response.status_code == 404


def test_patch_guard_rejects_backend_path_alias_for_protected_field():
    operations = [{"op": "replace", "path": "/t:ype", "value": "Catalog"}]

    assert patch_changes_field(operations, "type", "Collection")
