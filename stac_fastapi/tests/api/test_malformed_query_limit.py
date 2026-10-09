"""HTTP regressions for the ``limit`` chosen by search routes."""

import asyncio
import uuid
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.asyncio

DETAIL = "Invalid limit parameter: must be a positive integer"
ROUTES = ("global", "catalog", "collections")


@pytest_asyncio.fixture
async def limit_scope(catalogs_app_client, load_test_data):
    """Seed a catalog with a collection and item so every route reaches the core."""
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"limit-{uuid.uuid4()}"
    collection = load_test_data("test_collection.json")
    collection["id"] = f"limit-{uuid.uuid4()}"
    item = load_test_data("test_item.json")
    item.update(id=f"limit-{uuid.uuid4()}", collection=collection["id"])
    catalog_path = f"/catalogs/{catalog['id']}"
    collection_path = f"/collections/{collection['id']}"
    for path, body in (
        ("/catalogs", catalog),
        (f"{catalog_path}/collections", collection),
        (f"{collection_path}/items", item),
    ):
        response = await catalogs_app_client.post(path, json=body)
        assert response.status_code == 201, response.text
    routes = {
        "global": "/search",
        "catalog": f"{catalog_path}/search",
        "collections": "/collections-search",
    }
    try:
        for _ in range(50):
            control = await catalogs_app_client.post(
                routes["catalog"], json={"ids": [item["id"]]}
            )
            assert control.status_code == 200, control.text
            if control.json()["features"]:
                break
            await asyncio.sleep(0.1)
        assert [f["id"] for f in control.json()["features"]] == [item["id"]]
        yield routes
    finally:
        response = await catalogs_app_client.delete(collection_path)
        assert response.status_code == 204, response.text
        response = await catalogs_app_client.delete(catalog_path)
        assert response.status_code == 204, response.text


@pytest_asyncio.fixture
async def client(catalogs_app, catalogs_app_client):
    """Serve app errors as responses; the second fixture creates the indices."""
    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as c:
        yield c


def results(response):
    body = response.json()
    return body["collections"] if "collections" in body else body["features"]


@pytest.fixture
def forwarded_limit(txn_client, monkeypatch):
    """Replace the backend searches and return the limit a route forwarded."""
    database = type(txn_client.database)
    mocks = {
        "execute_search": AsyncMock(return_value=([], 0, None)),
        "get_all_collections": AsyncMock(return_value=([], None, 0)),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(database, name, mock)

    def limit_for(route):
        mock = mocks[
            "get_all_collections" if route == "collections" else "execute_search"
        ]
        mock.assert_called_once()
        return mock.call_args.kwargs["limit"]

    return limit_for


@pytest.mark.parametrize("value", ("bad", "1.5", "0", "-1"))
@pytest.mark.parametrize("route", ROUTES)
async def test_malformed_query_limit_returns_400(client, limit_scope, route, value):
    response = await client.post(limit_scope[route], params={"limit": value}, json={})
    assert response.status_code == 400, response.text
    assert response.json() == {"detail": DETAIL}


@pytest.mark.parametrize("value", (0, -1))
async def test_collections_search_body_limit_below_one_returns_400(
    client, limit_scope, value
):
    response = await client.post(limit_scope["collections"], json={"limit": value})
    assert response.status_code == 400, response.text
    assert response.json() == {"detail": DETAIL}


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "params,body", [({"limit": "1"}, {}), ({"limit": "bad"}, {"limit": 1})]
)
async def test_valid_limit_is_applied(client, limit_scope, route, params, body):
    """A valid query limit applies, and a body limit still beats the query string."""
    response = await client.post(limit_scope[route], params=params, json=body)
    assert response.status_code == 200, response.text
    assert len(results(response)) == 1


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "params,body,expected",
    [
        ({"limit": "20000"}, {}, 10000),
        ({"limit": ""}, {}, 7),
        ({}, {"limit": 20000}, 10000),
    ],
)
async def test_forwarded_limit(
    client, limit_scope, forwarded_limit, monkeypatch, route, params, body, expected
):
    """Values over 10000 are cropped and an empty query limit keeps the default."""
    for name in ("STAC_GLOBAL_ITEM_MAX_LIMIT", "STAC_GLOBAL_COLLECTION_MAX_LIMIT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("STAC_DEFAULT_ITEM_LIMIT", "7")
    monkeypatch.setenv("STAC_DEFAULT_COLLECTION_LIMIT", "7")
    response = await client.post(limit_scope[route], params=params, json=body)
    assert response.status_code == 200, response.text
    assert forwarded_limit(route) == expected


@pytest.mark.parametrize(
    "route,path",
    [
        ("collections", "/collections-search"),
        ("collections", "/collections"),
        ("global", "/search"),
    ],
)
async def test_get_limit_above_maximum_is_cropped(
    client, forwarded_limit, monkeypatch, route, path
):
    for name in ("STAC_GLOBAL_ITEM_MAX_LIMIT", "STAC_GLOBAL_COLLECTION_MAX_LIMIT"):
        monkeypatch.delenv(name, raising=False)
    response = await client.get(path, params={"limit": "20000"})
    assert response.status_code == 200, response.text
    assert forwarded_limit(route) == 10000


async def test_catalog_scope_precedes_limit_parsing(client, load_test_data):
    """An empty catalog returns before the query limit is read."""
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"empty-{uuid.uuid4()}"
    path = f"/catalogs/{catalog['id']}"
    response = await client.post("/catalogs", json=catalog)
    assert response.status_code == 201, response.text
    try:
        response = await client.post(f"{path}/search", params={"limit": "bad"}, json={})
        assert response.status_code == 200, response.text
        assert response.json()["features"] == []
    finally:
        assert (await client.delete(path)).status_code == 204
