import logging

import pytest
from httpx import AsyncClient
from slowapi.errors import RateLimitExceeded

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_tile_returns_mvt(app_client: AsyncClient, ctx):
    # url = "/collections/test-collection/tiles/19/84000/197301.mvt"
    url = "/collections/test-collection/tiles/5/29/19.mvt"
    response = await app_client.get(url)
    logger.info(f"Tile response status: {response.status_code}")
    assert response.status_code == 200
    # MVT is binary, but should not be empty
    assert response.content, "Tile endpoint did not return any data"


@pytest.mark.asyncio
async def test_stac_tile_returns_mvt(app_client: AsyncClient, ctx):
    # url = "/collections/test-collection/tiles/19/84000/197301.mvt"
    url = "/data/tiles/5/29/19.mvt"
    response = await app_client.get(url)
    logger.info(f"Tile response status: {response.status_code}")
    assert response.status_code == 200
    # MVT is binary, but should not be empty
    assert response.content, "Tile endpoint did not return any data"


@pytest.mark.asyncio
async def test_tilejson_returns_json(app_client: AsyncClient, ctx):
    url = "/collections/test-collection/tiles/tilejson.json"
    response = await app_client.get(url)
    logger.info(f"TileJSON response status: {response.status_code}")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data, "Missing 'version' key in response"
    assert "bounds" in data, "Missing 'bounds' key in response"
    assert "tiles" in data, "Missing 'tiles' key in response"


@pytest.mark.asyncio
async def test_clear_tile_cache(app_client: AsyncClient, ctx):
    url = "/admin/tiles/vector/cache/clear"
    response = await app_client.post(url)
    logger.info(f"Clear cache response status: {response.status_code}")
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Tile cache cleared successfully"
