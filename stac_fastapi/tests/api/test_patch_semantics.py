"""PATCH media-type and invalid-operation regressions."""

from copy import deepcopy

import pytest


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
@pytest.mark.parametrize("validator", ["false", "true"])
@pytest.mark.parametrize(
    "content_type",
    [
        "application/json-patch+json; charset=utf-8",
        "Application/JSON-PATCH+JSON; charset=UTF-8",
    ],
)
async def test_patch_accepts_case_insensitive_parameterised_media_type(
    app_client, ctx, monkeypatch, validator, content_type
):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", validator)
    response = await app_client.patch(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}",
        json=[{"op": "add", "path": "/properties/title", "value": "patched"}],
        headers={"Content-Type": content_type},
    )

    assert response.status_code == 200
    assert response.json()["properties"]["title"] == "patched"


@pytest.mark.datetime_filtering
@pytest.mark.asyncio
async def test_invalid_json_patch_typeerror_is_400(app_client, ctx, monkeypatch):
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    item = deepcopy(ctx.item)
    item["properties"]["title"] = "existing"
    await app_client.put(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}", json=item
    )
    before = await app_client.get(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )
    response = await app_client.patch(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}",
        json=[{"op": "remove", "path": "/properties/title/0"}],
        headers={"Content-Type": "application/json-patch+json"},
    )

    assert response.status_code == 400
    after = await app_client.get(
        f"/collections/{ctx.item['collection']}/items/{ctx.item['id']}"
    )
    assert after.json() == before.json()
