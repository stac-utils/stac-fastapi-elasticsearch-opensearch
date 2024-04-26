import os

import pytest


@pytest.mark.asyncio
async def test_get_search_not_authenticated(app_client_basic_auth, ctx):
    """Test public endpoint search without authentication"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()
    params = {"id": ctx.item["id"]}

    response = await app_client_basic_auth.get("/search", params=params)

    assert response.status_code == 200
    assert response.json()["features"][0]["geometry"] == ctx.item["geometry"]


@pytest.mark.asyncio
async def test_post_search_authenticated(app_client_basic_auth, ctx):
    """Test protected post search with reader auhtentication"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()
    params = {"id": ctx.item["id"]}
    headers = {"Authorization": "Basic cmVhZGVyOnJlYWRlcg=="}

    response = await app_client_basic_auth.post("/search", json=params, headers=headers)

    assert response.status_code == 200
    assert response.json()["features"][0]["geometry"] == ctx.item["geometry"]


@pytest.mark.asyncio
async def test_delete_resource_insufficient_permissions(app_client_basic_auth):
    """Test protected delete collection with reader auhtentication"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()
    headers = {
        "Authorization": "Basic cmVhZGVyOnJlYWRlcg=="
    }  # Assuming this is a valid authorization token

    response = await app_client_basic_auth.delete(
        "/collections/test-collection", headers=headers
    )

    assert (
        response.status_code == 403
    )  # Expecting a 403 status code for insufficient permissions
    assert response.json() == {
        "detail": "Insufficient permissions for [DELETE /collections/test-collection]"
    }
