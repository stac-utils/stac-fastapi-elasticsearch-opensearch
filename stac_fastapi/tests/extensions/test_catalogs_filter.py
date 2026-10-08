"""CQL2 `filter` on the catalog listing routes.

The Multi-Tenant Catalogs routes that list resources accept the same `filter`,
`filter-lang`, and `filter-crs` parameters as the global routes they mirror:
collection filters on `/catalogs`, `/catalogs/{id}/collections`,
`/catalogs/{id}/catalogs`, and `/catalogs/{id}/children`, and item filters on
`/catalogs/{id}/collections/{collection_id}/items`.
"""

import json
import os
import uuid
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest
from httpx import ASGITransport, AsyncClient

from ..conftest import build_test_app_with_catalogs


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def _catalog(client, load_test_data, catalog_id: str, parent: str | None = None):
    catalog = load_test_data("test_catalog.json")
    catalog["id"] = catalog_id
    path = f"/catalogs/{parent}/catalogs" if parent else "/catalogs"
    resp = await client.post(path, json=catalog)
    assert resp.status_code == 201, resp.text
    return catalog


async def _collection(client, ctx, catalog_id: str, collection_id: str):
    collection = {**ctx.collection, "id": collection_id}
    resp = await client.post(f"/catalogs/{catalog_id}/collections", json=collection)
    assert resp.status_code == 201, resp.text
    return collection


def _ids(resp, key: str) -> list[str]:
    assert resp.status_code == 200, resp.text
    return sorted(entry["id"] for entry in resp.json()[key])


@pytest.mark.asyncio
async def test_catalogs_list_applies_filter(catalogs_app_client, load_test_data):
    """GET /catalogs keeps only the catalogs that match the filter."""
    keep, drop = _uid("keep-cat"), _uid("drop-cat")
    await _catalog(catalogs_app_client, load_test_data, keep)
    await _catalog(catalogs_app_client, load_test_data, drop)

    resp = await catalogs_app_client.get(
        "/catalogs", params={"filter": f"id = '{keep}'", "limit": 100}
    )
    assert _ids(resp, "catalogs") == [keep]
    assert resp.json()["numberMatched"] == 1


@pytest.mark.asyncio
async def test_catalog_collections_apply_filter(
    catalogs_app_client, load_test_data, ctx
):
    """GET /catalogs/{id}/collections keeps only the matching collections."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    keep, drop = _uid("keep-col"), _uid("drop-col")
    await _collection(catalogs_app_client, ctx, cat, keep)
    await _collection(catalogs_app_client, ctx, cat, drop)

    resp = await catalogs_app_client.get(
        f"/catalogs/{cat}/collections", params={"filter": f"id = '{keep}'"}
    )
    assert _ids(resp, "collections") == [keep]
    assert resp.json()["numberMatched"] == 1


@pytest.mark.asyncio
async def test_catalog_collections_accept_cql2_json(
    catalogs_app_client, load_test_data, ctx
):
    """The filter can be given as CQL2 JSON."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    keep, drop = _uid("keep-col"), _uid("drop-col")
    await _collection(catalogs_app_client, ctx, cat, keep)
    await _collection(catalogs_app_client, ctx, cat, drop)

    cql2_json = {"op": "=", "args": [{"property": "id"}, keep]}
    resp = await catalogs_app_client.get(
        f"/catalogs/{cat}/collections",
        params={"filter": json.dumps(cql2_json), "filter-lang": "cql2-json"},
    )
    assert _ids(resp, "collections") == [keep]


@pytest.mark.asyncio
async def test_sub_catalogs_apply_filter(catalogs_app_client, load_test_data):
    """GET /catalogs/{id}/catalogs keeps only the matching sub-catalogs."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    keep, drop = _uid("keep-sub"), _uid("drop-sub")
    await _catalog(catalogs_app_client, load_test_data, keep, parent=cat)
    await _catalog(catalogs_app_client, load_test_data, drop, parent=cat)

    resp = await catalogs_app_client.get(
        f"/catalogs/{cat}/catalogs", params={"filter": f"id = '{keep}'"}
    )
    assert _ids(resp, "catalogs") == [keep]


@pytest.mark.asyncio
async def test_catalog_children_apply_filter(catalogs_app_client, load_test_data, ctx):
    """GET /catalogs/{id}/children keeps only the matching catalogs and collections."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    sub_keep, sub_drop = _uid("keep-sub"), _uid("drop-sub")
    col_keep, col_drop = _uid("keep-col"), _uid("drop-col")
    await _catalog(catalogs_app_client, load_test_data, sub_keep, parent=cat)
    await _catalog(catalogs_app_client, load_test_data, sub_drop, parent=cat)
    await _collection(catalogs_app_client, ctx, cat, col_keep)
    await _collection(catalogs_app_client, ctx, cat, col_drop)

    resp = await catalogs_app_client.get(
        f"/catalogs/{cat}/children",
        params={"filter": f"id IN ('{sub_keep}', '{col_keep}')"},
    )
    assert _ids(resp, "children") == sorted([sub_keep, col_keep])


@pytest.mark.asyncio
async def test_catalog_collection_items_apply_filter(
    catalogs_app_client, load_test_data, ctx
):
    """GET /catalogs/{id}/collections/{collection_id}/items keeps only the matching items."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    resp = await catalogs_app_client.post(
        f"/catalogs/{cat}/collections", json={"id": ctx.collection["id"]}
    )
    assert resp.status_code == 200, resp.text
    other = {**ctx.item, "id": _uid("other-item")}
    resp = await catalogs_app_client.post(
        f"/collections/{ctx.collection['id']}/items", json=other
    )
    assert resp.status_code == 201, resp.text

    resp = await catalogs_app_client.get(
        f"/catalogs/{cat}/collections/{ctx.collection['id']}/items",
        params={"filter": f"id = '{ctx.item['id']}'"},
    )
    assert _ids(resp, "features") == [ctx.item["id"]]


@pytest.mark.asyncio
async def test_next_link_keeps_the_filter(catalogs_app_client, load_test_data, ctx):
    """A paged, filtered listing keeps the filter on its next link."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    prefix = _uid("match")
    for i in range(3):
        await _collection(catalogs_app_client, ctx, cat, f"{prefix}-{i}")
    await _collection(catalogs_app_client, ctx, cat, _uid("other"))

    seen: list[str] = []
    params: dict | None = {"filter": f"id LIKE '{prefix}-%'", "limit": 2}
    url = f"/catalogs/{cat}/collections"
    for _ in range(3):
        resp = await catalogs_app_client.get(url, params=params)
        seen += _ids(resp, "collections")
        assert resp.json()["numberMatched"] == 3
        nxt = [link for link in resp.json()["links"] if link["rel"] == "next"]
        if not nxt:
            break
        query = parse_qs(urlparse(nxt[0]["href"]).query)
        assert query["filter"] == [f"id LIKE '{prefix}-%'"]
        url, params = nxt[0]["href"], None
    assert sorted(seen) == [f"{prefix}-{i}" for i in range(3)]


@pytest.mark.asyncio
async def test_next_link_keeps_other_parameters(
    catalogs_app_client, load_test_data, ctx
):
    """Next links keep every query parameter, such as `type` on children."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    await _catalog(catalogs_app_client, load_test_data, _uid("sub"), parent=cat)
    cols = sorted([_uid("col"), _uid("col")])
    for col in cols:
        await _collection(catalogs_app_client, ctx, cat, col)

    resp = await catalogs_app_client.get(
        f"/catalogs/{cat}/children", params={"type": "Collection", "limit": 1}
    )
    first = _ids(resp, "children")
    nxt = [link for link in resp.json()["links"] if link["rel"] == "next"]
    assert len(nxt) == 1
    assert parse_qs(urlparse(nxt[0]["href"]).query)["type"] == ["Collection"]
    resp = await catalogs_app_client.get(nxt[0]["href"])
    assert sorted(first + _ids(resp, "children")) == cols


@pytest.mark.asyncio
async def test_unsupported_filter_lang_is_rejected(catalogs_app_client):
    """A filter language other than CQL2 text or JSON is a 400."""
    resp = await catalogs_app_client.get(
        "/catalogs", params={"filter": "id = 'x'", "filter-lang": "ecql"}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_invalid_filter_is_rejected(catalogs_app_client, load_test_data):
    """A filter that does not parse is a 400, not an unfiltered listing."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    resp = await catalogs_app_client.get(
        f"/catalogs/{cat}/collections", params={"filter": "id = = 'x'"}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cql2_json",
    [
        {"op": "in", "args": [{"property": "id"}, "not-a-list"]},
        {"op": "between", "args": [{"property": "id"}, 1]},
    ],
)
@pytest.mark.parametrize(
    "path",
    [
        "/catalogs",
        "/catalogs/{catalog_id}/collections",
        "/catalogs/{catalog_id}/catalogs",
        "/catalogs/{catalog_id}/children",
        "/catalogs/{catalog_id}/collections/{collection_id}/items",
    ],
)
async def test_filter_that_fails_translation_is_rejected(
    catalogs_app_client, load_test_data, ctx, path, cql2_json
):
    """A filter that parses but can't be translated to a query is a 400, as on /search."""
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    resp = await catalogs_app_client.post(
        f"/catalogs/{cat}/collections", json={"id": ctx.collection["id"]}
    )
    assert resp.status_code == 200, resp.text

    resp = await catalogs_app_client.get(
        path.format(catalog_id=cat, collection_id=ctx.collection["id"]),
        params={"filter": json.dumps(cql2_json), "filter-lang": "cql2-json"},
    )
    assert resp.status_code == 400, resp.text
    assert "Error with cql2 filter" in resp.text


@pytest.mark.asyncio
async def test_item_filter_checks_the_queryables(
    catalogs_app_client, load_test_data, ctx
):
    """With VALIDATE_QUERYABLES, an item filter on a field that is not queryable is a 400, as on /search."""
    # The queryables cache reads the setting when the app is built.
    with mock.patch.dict(os.environ, {"VALIDATE_QUERYABLES": "true"}):
        app = build_test_app_with_catalogs()
    cat = _uid("cat")
    await _catalog(catalogs_app_client, load_test_data, cat)
    resp = await catalogs_app_client.post(
        f"/catalogs/{cat}/collections", json={"id": ctx.collection["id"]}
    )
    assert resp.status_code == 200, resp.text
    path = f"/catalogs/{cat}/collections/{ctx.collection['id']}/items"

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test-server"
    ) as client:
        queryable = await client.get(
            path, params={"filter": f"id = '{ctx.item['id']}'"}
        )
        not_queryable = await client.get(path, params={"filter": "invalid_param = 'x'"})

    assert _ids(queryable, "features") == [ctx.item["id"]]
    assert not_queryable.status_code == 400, not_queryable.text
    assert "Invalid query fields: invalid_param" in not_queryable.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/catalogs",
        "/catalogs/{catalog_id}/collections",
        "/catalogs/{catalog_id}/catalogs",
        "/catalogs/{catalog_id}/children",
        "/catalogs/{catalog_id}/collections/{collection_id}/items",
    ],
)
async def test_openapi_lists_the_filter_parameters(catalogs_app_client, path):
    """The filter parameters are declared on each listing route."""
    resp = await catalogs_app_client.get("/api")
    assert resp.status_code == 200
    params = {p["name"] for p in resp.json()["paths"][path]["get"]["parameters"]}
    assert {"filter", "filter-lang"} <= params
