"""Fields projection with and without response models (#865)."""

import uuid
from functools import cache

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ..conftest import AsyncSettings, instantiate_api

CATALOG_SEARCH_PATH = "/catalogs/{catalog_id}/search"

ROUTES = [
    ("GET", "/search"),
    ("POST", "/search"),
    ("GET", "/collections/{collection_id}/items"),
    ("GET", CATALOG_SEARCH_PATH),
    ("POST", CATALOG_SEARCH_PATH),
]
FIELDS = {
    "include": ("id", {"include": ["id"]}),
    "exclude": ("-geometry", {"exclude": ["geometry"]}),
}


@cache
def _app(response_models: bool):
    api = instantiate_api(
        settings=AsyncSettings(
            enable_catalogs_route=True, enable_response_models=response_models
        )
    )
    api.app.router.dependencies = []
    return api.app


@pytest_asyncio.fixture
async def seeded(catalogs_app_client, load_test_data):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"fields-catalog-{uuid.uuid4()}"
    resp = await catalogs_app_client.post("/catalogs", json=catalog)
    assert resp.status_code == 201

    collection = load_test_data("test_collection.json")
    collection["id"] = f"fields-collection-{uuid.uuid4()}"
    resp = await catalogs_app_client.post(
        f"/catalogs/{catalog['id']}/collections", json=collection
    )
    assert resp.status_code == 201

    item = load_test_data("test_item.json")
    item["id"] = f"fields-item-{uuid.uuid4()}"
    item["collection"] = collection["id"]
    resp = await catalogs_app_client.post(
        f"/collections/{collection['id']}/items", json=item
    )
    assert resp.status_code == 201
    return {"catalog_id": catalog["id"], "collection_id": collection["id"]}


async def _request(response_models, method, path, seeded, fields=None):
    url = path.format(**seeded)
    async with AsyncClient(
        transport=ASGITransport(app=_app(response_models)),
        base_url="http://test-server",
    ) as client:
        if method == "GET":
            params = {"collections": seeded["collection_id"]}
            if fields:
                params["fields"] = fields[0]
            return await client.get(url, params=params)
        body = {"collections": [seeded["collection_id"]]}
        if fields:
            body["fields"] = fields[1]
        return await client.post(url, json=body)


@pytest.mark.asyncio
@pytest.mark.parametrize("projection", FIELDS)
@pytest.mark.parametrize("method,path", ROUTES)
async def test_projected_search_returns_requested_fields_with_any_response_model_setting(
    seeded, method, path, projection
):
    bodies = []
    for response_models in (False, True):
        resp = await _request(response_models, method, path, seeded, FIELDS[projection])
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/geo+json"
        features = resp.json()["features"]
        assert len(features) == 1
        if projection == "include":
            assert set(features[0]) == {"id"}
        else:
            assert "geometry" not in features[0]
            assert features[0]["type"] == "Feature"
        bodies.append(resp.json())

    assert bodies[0] == bodies[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("response_models", [False, True])
@pytest.mark.parametrize(
    "method,path",
    [route for route in ROUTES if route[1] in ("/search", CATALOG_SEARCH_PATH)],
)
async def test_search_without_fields_returns_full_items(
    seeded, method, path, response_models
):
    resp = await _request(response_models, method, path, seeded)

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/geo+json"
    body = resp.json()
    (feature,) = body["features"]
    assert {"type", "geometry", "properties", "assets"} <= set(feature)
    if not response_models:
        assert body["numberReturned"] == 1


@pytest.mark.parametrize("response_models", [False, True])
def test_catalog_search_response_model_follows_settings(response_models):
    operations = _app(response_models).openapi()["paths"][CATALOG_SEARCH_PATH]

    for method in ("get", "post"):
        content = operations[method]["responses"]["200"]["content"]
        assert bool(content["application/geo+json"]["schema"]) == response_models
