"""HTTP regressions for malformed item search pagination tokens."""

import asyncio
import sys
import uuid
from base64 import urlsafe_b64encode
from urllib.parse import parse_qs, urlparse

import orjson
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from stac_fastapi.sfeos_helpers.search_engine.selection.selectors import (
    DatetimeBasedIndexSelector,
    UnfilteredIndexSelector,
)

from ..conftest import DatabaseLogic

pytestmark = [pytest.mark.asyncio, pytest.mark.datetime_filtering]

INVALID = {"detail": "Invalid pagination token."}


def encode(value):
    return urlsafe_b64encode(orjson.dumps(value)).decode()


MALFORMED_TOKENS = {
    "base64": "!!!!",
    "padding": "a",
    "not-json": "bm90LWpzb24=",
    "null": encode(None),
    "object": encode({}),
    "string": encode("x"),
    "empty-list": encode([]),
    "short-list": encode([1]),
    "long-list": encode(["x", "y", "z", "w", "v"]),
}
ROUTES = (
    ("global", "GET"),
    ("global", "POST"),
    ("catalog", "GET"),
    ("catalog", "POST"),
    ("items", "GET"),
    ("catalog_items", "GET"),
)


def next_token(page):
    link = next(link for link in page["links"] if link["rel"] == "next")
    if "body" in link:
        return link["body"]["token"]
    return parse_qs(urlparse(link["href"]).query)["token"][0]


async def request(client, method, path, params):
    if method == "GET":
        return await client.get(path, params=params)
    return await client.post(path, json=params)


@pytest_asyncio.fixture
async def token_scope(catalogs_app_client, load_test_data):
    """Seed a populated catalog scope with two items that paginate at limit=1."""
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"token-{uuid.uuid4()}"
    collection = load_test_data("test_collection.json")
    collection["id"] = f"token-{uuid.uuid4()}"
    catalog_path = f"/catalogs/{catalog['id']}"
    collection_path = f"/collections/{collection['id']}"
    items = []
    for day in (1, 2):
        item = load_test_data("test_item.json")
        item.update(id=f"token-{day}-{uuid.uuid4()}", collection=collection["id"])
        item["properties"]["datetime"] = f"2020-02-1{day}T00:00:00Z"
        items.append(item)
    for path, body in (
        ("/catalogs", catalog),
        (f"{catalog_path}/collections", collection),
        *((f"{collection_path}/items", item) for item in items),
    ):
        response = await catalogs_app_client.post(path, json=body)
        assert response.status_code == 201, response.text
    routes = {
        "global": "/search",
        "catalog": f"{catalog_path}/search",
        "items": f"{collection_path}/items",
        "catalog_items": f"{catalog_path}{collection_path}/items",
    }
    try:
        for _ in range(50):
            control = await catalogs_app_client.get(routes["catalog"])
            assert control.status_code == 200, control.text
            if len(control.json()["features"]) == len(items):
                break
            await asyncio.sleep(0.1)
        assert len(control.json()["features"]) == len(items)
        yield routes, collection["id"]
    finally:
        response = await catalogs_app_client.delete(collection_path)
        assert response.status_code == 204, response.text
        response = await catalogs_app_client.delete(catalog_path)
        assert response.status_code == 204, response.text


@pytest.mark.parametrize("route,method", ROUTES)
@pytest.mark.parametrize("token", MALFORMED_TOKENS.values(), ids=MALFORMED_TOKENS)
async def test_malformed_item_token_rejected(
    catalogs_app, token_scope, route, method, token
):
    """Every item search route rejects a malformed token with a client error."""
    routes, _ = token_scope
    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        response = await request(client, method, routes[route], {"token": token})
    assert response.status_code == 400, response.text
    assert response.json() == INVALID


@pytest.mark.parametrize(
    "body",
    (
        {},
        {
            "filter-lang": "cql2-json",
            "filter": {"op": "=", "args": [{"property": "id"}, "x"]},
        },
    ),
    ids=("select", "cql2"),
)
async def test_malformed_token_skips_index_resolution(catalogs_app, monkeypatch, body):
    """A malformed token is rejected before any index selection runs."""

    async def unexpected(*args, **kwargs):
        raise AssertionError("unexpected index resolution")

    for selector in (DatetimeBasedIndexSelector, UnfilteredIndexSelector):
        monkeypatch.setattr(selector, "select_indexes", unexpected)
    monkeypatch.setattr(
        sys.modules[DatabaseLogic.__module__], "resolve_cql2_indexes", unexpected
    )
    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        control = await client.post("/search", json=body)
        response = await client.post("/search", json={**body, "token": "!!!!"})
    assert control.status_code == 500
    assert response.status_code == 400, response.text
    assert response.json() == INVALID


@pytest.mark.parametrize(
    "route,method", (("global", "GET"), ("global", "POST"), ("items", "GET"))
)
async def test_token_length_follows_sortby(
    catalogs_app, catalogs_app_client, token_scope, route, method
):
    """Tokens are checked against the requested sort, and sortby tokens still page."""
    routes, collection_id = token_scope
    base = {"limit": 1}
    if route == "global":
        base["collections"] = [collection_id] if method == "POST" else collection_id
    sortby = [{"field": "id", "direction": "asc"}] if method == "POST" else "id"
    first = await request(catalogs_app_client, method, routes[route], base)
    assert first.status_code == 200, first.text
    default_token = next_token(first.json())

    sorted_params = {**base, "sortby": sortby}
    sorted_first = await request(
        catalogs_app_client, method, routes[route], sorted_params
    )
    assert sorted_first.status_code == 200, sorted_first.text
    sorted_next = await request(
        catalogs_app_client,
        method,
        routes[route],
        {**sorted_params, "token": next_token(sorted_first.json())},
    )
    assert sorted_next.status_code == 200, sorted_next.text
    assert [f["id"] for f in sorted_next.json()["features"]] != [
        f["id"] for f in sorted_first.json()["features"]
    ]

    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        response = await request(
            client, method, routes[route], {**sorted_params, "token": default_token}
        )
    assert response.status_code == 400, response.text
    assert response.json() == INVALID


@pytest.mark.parametrize(
    "route,method",
    (("global", "GET"), ("global", "POST"), ("items", "GET"), ("catalog_items", "GET")),
)
async def test_valid_token_round_trips(catalogs_app_client, token_scope, route, method):
    """A real next token returns the following page unchanged."""
    routes, collection_id = token_scope
    params = {"limit": 1}
    if route == "global":
        params["collections"] = [collection_id] if method == "POST" else collection_id
    first = await request(catalogs_app_client, method, routes[route], params)
    assert first.status_code == 200, first.text
    second = await request(
        catalogs_app_client,
        method,
        routes[route],
        {**params, "token": next_token(first.json())},
    )
    assert second.status_code == 200, second.text
    first_ids = [f["id"] for f in first.json()["features"]]
    second_ids = [f["id"] for f in second.json()["features"]]
    assert len(first_ids) == len(second_ids) == 1
    assert first_ids != second_ids


async def test_collection_cursor_contract_preserved(catalogs_app_client):
    """Collection pagination keeps accepting arbitrary cursor strings."""
    response = await catalogs_app_client.get("/collections", params={"token": "!!!!"})
    assert response.status_code == 200, response.text
