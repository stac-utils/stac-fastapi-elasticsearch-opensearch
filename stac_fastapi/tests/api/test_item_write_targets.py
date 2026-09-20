"""Regression coverage for transaction target identity and write visibility."""

import uuid
from copy import deepcopy

import pytest

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id

from ..conftest import refresh_indices


@pytest.mark.asyncio
async def test_put_missing_item_in_existing_collection_returns_404(app_client, ctx):
    item = deepcopy(ctx.item)
    item["id"] = f"missing-{uuid.uuid4()}"

    response = await app_client.put(
        f"/collections/{ctx.collection['id']}/items/{item['id']}", json=item
    )

    assert response.status_code == 404
    assert (
        await app_client.get(f"/collections/{ctx.collection['id']}/items/{item['id']}")
    ).status_code == 404


@pytest.mark.asyncio
async def test_put_identity_mismatch_is_rejected_before_lookup(app_client, ctx):
    item = deepcopy(ctx.item)
    item["id"] = f"different-{uuid.uuid4()}"
    item_url = f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
    original = (await app_client.get(item_url)).json()

    response = await app_client.put(
        f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}", json=item
    )

    assert response.status_code == 400
    assert (
        await app_client.get(
            f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
        )
    ).json() == original


@pytest.mark.asyncio
async def test_put_without_collection_populates_uri_value(app_client, ctx):
    item = deepcopy(ctx.item)
    item.pop("collection")
    item["properties"]["title"] = "collection from URI"

    response = await app_client.put(
        f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}", json=item
    )

    assert response.status_code == 200
    assert response.json()["collection"] == ctx.collection["id"]


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_put_requires_searchable_target_after_unrefreshed_create(
    app_client, txn_client, ctx, monkeypatch
):
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "false")
    monkeypatch.setenv("DATABASE_REFRESH", "false")
    item = deepcopy(ctx.item)
    item["id"] = f"unrefreshed-{uuid.uuid4()}"
    item_url = f"/collections/{ctx.collection['id']}/items/{item['id']}"
    index = index_alias_by_collection_id(ctx.collection["id"])
    await txn_client.database.client.indices.put_settings(
        index=index, body={"refresh_interval": "-1"}
    )
    try:
        await txn_client.database.create_item(item=item, refresh=False)
        assert (await app_client.put(item_url, json=item)).status_code == 404

        await txn_client.database.client.indices.refresh(index=index)
        item["properties"]["title"] = "visible after refresh"
        response = await app_client.put(item_url, json=item)
        assert response.status_code == 200
    finally:
        await txn_client.database.client.indices.put_settings(
            index=index, body={"refresh_interval": "1s"}
        )
        await refresh_indices(txn_client)


@pytest.mark.asyncio
async def test_feature_collection_preflights_all_collection_identities(app_client, ctx):
    valid = deepcopy(ctx.item)
    valid["id"] = f"valid-{uuid.uuid4()}"
    invalid = deepcopy(valid)
    invalid["id"] = f"invalid-{uuid.uuid4()}"
    invalid["collection"] = f"other-{uuid.uuid4()}"

    response = await app_client.post(
        f"/collections/{ctx.collection['id']}/items",
        json={"type": "FeatureCollection", "features": [valid, invalid]},
    )

    assert response.status_code == 400
    for item in (valid, invalid):
        assert (
            await app_client.get(
                f"/collections/{ctx.collection['id']}/items/{item['id']}"
            )
        ).status_code == 404


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    ("content_type", "patch"),
    [
        (
            "application/merge-patch+json",
            {"properties": {"title": "updated while hidden"}},
        ),
        (
            "application/json-patch+json",
            [
                {
                    "op": "replace",
                    "path": "/properties/title",
                    "value": "updated while hidden",
                }
            ],
        ),
    ],
)
async def test_hidden_item_accepts_put_and_patch(
    app_client,
    txn_client,
    ctx,
    monkeypatch,
    validator,
    content_type,
    patch,
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    monkeypatch.setenv("HIDE_ITEM_PATH", "properties._private.hidden")

    hidden = deepcopy(ctx.item)
    hidden["properties"]["title"] = "original title"
    hidden["properties"]["_private"] = {"hidden": True}
    put_response = await app_client.put(
        f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}", json=hidden
    )
    assert put_response.status_code == 200

    item_url = f"/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
    assert (await app_client.get(item_url)).status_code == 404
    assert (await app_client.get("/search", params={"ids": ctx.item["id"]})).json()[
        "features"
    ] == []

    if content_type == "application/merge-patch+json":
        # Keep this test independent of the sibling recursive-merge semantics fix.
        patch = {"properties": {**hidden["properties"], **patch["properties"]}}
    patch_response = await app_client.patch(
        item_url, json=patch, headers={"Content-Type": content_type}
    )
    assert patch_response.status_code == 200
    assert patch_response.json()["id"] == ctx.item["id"]
    stored = await txn_client.database.get_item_for_write(
        ctx.collection["id"], ctx.item["id"]
    )
    assert stored["properties"]["title"] == "updated while hidden"
    assert stored["properties"]["_private"]["hidden"] is True

    visible = deepcopy(hidden)
    visible["properties"]["_private"]["hidden"] = False
    unhide_response = await app_client.put(item_url, json=visible)
    assert unhide_response.status_code == 200
    await refresh_indices(txn_client)
    visible_response = await app_client.get(item_url)
    assert visible_response.status_code == 200
    assert visible_response.json()["properties"]["_private"]["hidden"] is False
