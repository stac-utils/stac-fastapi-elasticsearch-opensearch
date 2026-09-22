"""HTTP regressions for raw GET search parsing shared by delegated routes."""

import asyncio
import json
import uuid
from unittest.mock import AsyncMock
from urllib.parse import quote_plus

import orjson
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from stac_fastapi.core import core

pytestmark = [pytest.mark.asyncio, pytest.mark.datetime_filtering]

MALFORMED = [
    ({"query": "{"}, "Invalid query parameter: expected valid JSON."),
    (
        {"filter": "{", "filter-lang": "cql2-json"},
        "Invalid filter parameter: expected valid CQL2 JSON.",
    ),
    *[
        (
            {"filter": expression, "filter-lang": "cql2-text"},
            "Invalid filter parameter: expected valid CQL2 text.",
        )
        for expression in ("id =", "id = @")
    ],
]
ROUTES = ("global", "catalog", "items")


@pytest_asyncio.fixture
async def search_scope(catalogs_app_client, load_test_data):
    """Seed a populated scope and verify the real backend before parsing probes."""
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"parse-{uuid.uuid4()}"
    collection = load_test_data("test_collection.json")
    collection["id"] = f"parse-{uuid.uuid4()}"
    item = load_test_data("test_item.json")
    item.update(id=f"banks-{uuid.uuid4()}", collection=collection["id"])
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
        "items": f"{collection_path}/items",
    }
    try:
        for _ in range(50):
            control = await catalogs_app_client.get(
                routes["catalog"], params={"ids": item["id"]}
            )
            assert control.status_code == 200, control.text
            if control.json()["features"]:
                break
            await asyncio.sleep(0.1)
        assert [f["id"] for f in control.json()["features"]] == [item["id"]]
        yield routes, item
    finally:
        response = await catalogs_app_client.delete(collection_path)
        assert response.status_code == 204, response.text
        response = await catalogs_app_client.delete(catalog_path)
        assert response.status_code == 204, response.text


@pytest.mark.parametrize(
    "route,params,detail",
    [(route, params, detail) for route in ROUTES for params, detail in MALFORMED]
    + [
        (
            route,
            {"intersects": "{"},
            "Invalid intersects parameter: expected valid JSON.",
        )
        for route in ("global", "catalog")
    ],
)
async def test_malformed_search_parameters(
    catalogs_app, search_scope, monkeypatch, route, params, detail
):
    """Reject syntax errors before downstream search on every accepting route."""
    routes, _ = search_scope
    downstream = AsyncMock(side_effect=AssertionError("unexpected downstream search"))
    monkeypatch.setattr(core.CoreClient, "post_search", downstream)
    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        response = await client.get(routes[route], params=params)
    downstream.assert_not_called()
    assert response.status_code == 400, response.text
    assert response.json() == {"detail": detail}


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("kind", ("query", "json", "text", "default", "intersects"))
async def test_valid_search_neighbors(catalogs_app_client, search_scope, route, kind):
    """Valid encoded filters still return the stored item through delegated routes."""
    routes, item = search_scope
    params = {"ids": item["id"]}
    if kind == "query":
        params["query"] = json.dumps(
            {"platform": {"eq": item["properties"]["platform"]}}
        )
    elif kind == "json":
        params.update(
            {
                "filter-lang": "cql2-json",
                "filter": json.dumps(
                    {"op": "like", "args": [{"property": "id"}, "%banks%"]}
                ),
            }
        )
    elif kind in ("text", "default"):
        params["filter"] = "id LIKE '%banks%'"
        if kind == "text":
            params["filter-lang"] = "cql2-text"
    else:
        # Preserve the historical extra decoding of intersects, and its omission
        # from the item-listing request model.
        params["intersects"] = quote_plus(json.dumps(item["geometry"]))
    response = await catalogs_app_client.get(routes[route], params=params)
    assert response.status_code == 200, response.text
    features = response.json()["features"]
    assert [feature["id"] for feature in features] == [item["id"]]
    assert features[0]["geometry"] == item["geometry"]
    assert {key: features[0]["properties"][key] for key in item["properties"]} == item[
        "properties"
    ]


async def test_listing_ignores_intersects(catalogs_app_client, search_scope):
    """Item listing continues to ignore an unaccepted intersects parameter."""
    routes, item = search_scope
    response = await catalogs_app_client.get(
        routes["items"], params={"intersects": "{"}
    )
    assert response.status_code == 200
    assert [f["id"] for f in response.json()["features"]] == [item["id"]]


@pytest.mark.parametrize("route", ("global", "catalog"))
async def test_bbox_intersects_validation(catalogs_app_client, search_scope, route):
    """Valid JSON still reaches the existing mutually exclusive geometry check."""
    routes, item = search_scope
    response = await catalogs_app_client.get(
        routes[route],
        params={"bbox": "-180,-90,180,90", "intersects": json.dumps(item["geometry"])},
    )
    assert response.status_code == 400
    assert "Invalid parameters provided:" in response.json()["detail"]
    assert "intersects" in response.json()["detail"]


async def test_catalog_scope_precedes_parsing(
    catalogs_app_client, search_scope, load_test_data
):
    """Preserve scoped rejection and the early return for an empty catalog."""
    routes, _ = search_scope
    response = await catalogs_app_client.get(
        routes["catalog"], params={"collections": "outside-scope", "query": "{"}
    )
    assert response.status_code == 403
    assert "outside the scope" in response.json()["detail"]
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = f"empty-{uuid.uuid4()}"
    path = f"/catalogs/{catalog['id']}"
    response = await catalogs_app_client.post("/catalogs", json=catalog)
    assert response.status_code == 201
    try:
        response = await catalogs_app_client.get(
            f"{path}/search", params={"query": "{"}
        )
        assert response.status_code == 200
        assert response.json()["features"] == []
    finally:
        assert (await catalogs_app_client.delete(path)).status_code == 204


@pytest.mark.parametrize(
    "target,error",
    [
        ("parse_cql2_text", RuntimeError("parser defect")),
        ("to_cql2", ValueError("serialization defect")),
        ("post_search", orjson.JSONDecodeError("downstream JSON", "x", 0)),
        ("post_search", ValueError("downstream value")),
        ("execute_search", RuntimeError("backend unavailable")),
    ],
)
async def test_unrelated_failures_propagate(
    catalogs_app,
    catalogs_app_client,
    search_scope,
    txn_client,
    monkeypatch,
    target,
    error,
):
    """Parsing handlers must never turn server failures into client errors."""
    routes, item = search_scope
    if target == "execute_search":
        monkeypatch.setattr(
            type(txn_client.database), target, AsyncMock(side_effect=error)
        )
    elif target == "post_search":
        monkeypatch.setattr(core.CoreClient, target, AsyncMock(side_effect=error))
    else:

        def fail(*args, **kwargs):
            raise error

        monkeypatch.setattr(core, target, fail)
    params = {"filter": f"id = '{item['id']}'"}
    with pytest.raises(type(error)) as raised:
        await catalogs_app_client.get(routes["global"], params=params)
    assert raised.value is error
    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        response = await client.get(routes["global"], params=params)
    assert response.status_code == 500


async def test_generated_json_failure_propagates(
    catalogs_app_client, search_scope, monkeypatch
):
    """JSON generated from an AST is not untrusted raw client JSON."""
    routes, _ = search_scope
    monkeypatch.setattr(core, "to_cql2", lambda ast: "{")
    with pytest.raises(orjson.JSONDecodeError):
        await catalogs_app_client.get(routes["global"], params={"filter": "id = 'a'"})


async def test_invalid_literal_is_not_a_syntax_error(catalogs_app, search_scope):
    """Grammar-valid literal validation remains outside the syntax-only boundary."""
    routes, _ = search_scope
    async with AsyncClient(
        transport=ASGITransport(app=catalogs_app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        response = await client.get(
            routes["global"],
            params={
                "filter": "datetime = DATE('2000-19-39')",
                "filter-lang": "cql2-text",
            },
        )
    assert response.status_code == 500
