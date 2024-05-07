import os

import pytest


@pytest.mark.asyncio
async def test_get_search_not_authenticated(app_client_basic_auth, ctx):
    """Test public endpoint [GET /search] without authentication"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()
    params = {"id": ctx.item["id"]}

    response = await app_client_basic_auth.get("/search", params=params)

    assert response.status_code == 200, response
    assert response.json()["features"][0]["geometry"] == ctx.item["geometry"]


@pytest.mark.asyncio
async def test_post_search_authenticated(app_client_basic_auth, ctx):
    """Test protected endpoint [POST /search] with reader auhtentication"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()
    params = {"id": ctx.item["id"]}
    headers = {"Authorization": "Basic cmVhZGVyOnJlYWRlcg=="}

    response = await app_client_basic_auth.post("/search", json=params, headers=headers)

    assert response.status_code == 200, response
    assert response.json()["features"][0]["geometry"] == ctx.item["geometry"]


@pytest.mark.asyncio
async def test_delete_resource_anonymous(
    app_client_basic_auth,
):
    """Test protected endpoint [DELETE /collections/{collection_id}] without auhtentication"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()

    response = await app_client_basic_auth.delete("/collections/test-collection")

    assert response.status_code == 401
    assert response.json() == {"detail": "Not authenticated"}


@pytest.mark.asyncio
async def test_delete_resource_invalid_credentials(app_client_basic_auth, ctx):
    """Test protected endpoint [DELETE /collections/{collection_id}] with invalid credentials"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()

    headers = {"Authorization": "Basic YWRtaW46cGFzc3dvcmQ="}

    response = await app_client_basic_auth.delete(
        f"/collections/{ctx.collection['id']}", headers=headers
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Incorrect username or password"}


@pytest.mark.asyncio
async def test_delete_resource_insufficient_permissions(app_client_basic_auth, ctx):
    """Test protected endpoint [DELETE /collections/{collection_id}] with reader user which has insufficient permissions"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()

    headers = {"Authorization": "Basic cmVhZGVyOnJlYWRlcg=="}

    response = await app_client_basic_auth.delete(
        f"/collections/{ctx.collection['id']}", headers=headers
    )

    assert response.status_code == 403
    assert response.json() == {
        "detail": "Insufficient permissions for [DELETE /collections/test-collection]"
    }


@pytest.mark.asyncio
async def test_delete_resource_sufficient_permissions(app_client_basic_auth, ctx):
    """Test protected endpoint [DELETE /collections/{collection_id}] with admin user which has sufficient permissions"""
    if not os.getenv("BASIC_AUTH"):
        pytest.skip()

    headers = {"Authorization": "Basic YWRtaW46YWRtaW4="}

    response = await app_client_basic_auth.delete(
        f"/collections/{ctx.collection['id']}", headers=headers
    )

    assert response.status_code == 204
