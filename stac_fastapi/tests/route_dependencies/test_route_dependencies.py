import pytest


@pytest.mark.asyncio
async def test_not_authenticated(route_dependencies_client, ctx):
    """Test public endpoint [GET /search] without authentication"""
    params = {"id": ctx.item["id"]}

    response = await route_dependencies_client.get("/search", params=params)

    assert response.status_code == 401, response


@pytest.mark.asyncio
async def test_authenticated(route_dependencies_client, ctx):
    """Test protected endpoint [POST /search] with reader auhtentication"""

    params = {"id": ctx.item["id"]}

    response = await route_dependencies_client.post(
        "/search",
        json=params,
        auth=("bob", "dobbs"),
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 200, response
    assert len(response.json()["features"]) == 1
