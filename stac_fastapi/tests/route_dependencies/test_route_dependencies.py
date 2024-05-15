import pytest


@pytest.mark.asyncio
async def test_not_authenticated(route_dependencies_client):
    """Test protected endpoint [GET /collections] without permissions"""

    response = await route_dependencies_client.get("/collections")

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_authenticated(route_dependencies_client):
    """Test protected endpoint [GET /collections] with permissions"""

    response = await route_dependencies_client.get(
        "/collections",
        auth=("bob", "dobbs"),
    )

    assert response.status_code == 200
    assert len(response.json()["collections"]) == 1
