"""Invalid date, timestamp and interval literals in CQL2 JSON filters get 400."""

import copy
import json
import sys
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from stac_fastapi.core.extensions.filter import CQL2FilterError, check_cql2_literals

BAD_LITERALS = [
    {"date": "2000-19-39"},
    {"timestamp": "2000-19-39T00:00:00Z"},
    {"timestamp": 1},
    {"timestamp": "2020-01-01T00:00:00+25:00"},
    {"timestamp": "2020-01-01T00:00:00"},
    {"timestamp": "2020-01-01 00:00:00"},
]
BAD_INTERVAL = {"interval": ["2000-19-39", ".."]}

GLOBAL_ROUTES = [
    ("GET", "/search", "cql2-json"),
    ("GET", "/collections", "cql2-json"),
    ("GET", "/collections-search", "cql2-json"),
    ("GET", "/aggregate", "cql2-json"),
    ("POST", "/search", "cql2-json"),
    ("POST", "/collections-search", "cql2-json"),
    ("POST", "/aggregate", "cql2-json"),
    ("GET", "/collections", None),
    ("GET", "/collections-search", None),
]
LISTING_PATHS = [
    "/catalogs",
    "/catalogs/{catalog_id}/collections",
    "/catalogs/{catalog_id}/catalogs",
    "/catalogs/{catalog_id}/children",
    "/catalogs/{catalog_id}/collections/{collection_id}/items",
]
CATALOG_ROUTES = (
    [
        ("GET", path, lang)
        for path in LISTING_PATHS
        for lang in ("cql2-json", None, "cql2-text")
    ]
    + [("GET", "/catalogs/{catalog_id}/search", "cql2-json")]
    + [("POST", "/catalogs/{catalog_id}/search", lang) for lang in ("cql2-json", None)]
)


def _literal_id(literal):
    return "%s=%s" % next(iter(literal.items()))


def _filter(literal, op="="):
    return {"op": op, "args": [{"property": "datetime"}, literal]}


async def _send(client, method, path, cql2, lang):
    if method == "GET":
        params = {"filter": cql2 if isinstance(cql2, str) else json.dumps(cql2)}
        if lang:
            params["filter-lang"] = lang
        if path.endswith("/aggregate"):
            params["aggregations"] = "total_count"
        return await client.get(path, params=params)
    body = {"filter": cql2}
    if lang:
        body["filter-lang"] = lang
    if path.endswith("/aggregate"):
        body["aggregations"] = ["total_count"]
    return await client.post(path, json=body)


def _assert_rejected(resp, value):
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert f"({value!r})" in detail, detail
    assert "is not an RFC 3339" in detail, detail
    assert "400:" not in detail, detail


async def _catalog(client, load_test_data):
    catalog = {
        **load_test_data("test_catalog.json"),
        "id": f"cat-{uuid.uuid4().hex[:12]}",
    }
    resp = await client.post("/catalogs", json=catalog)
    assert resp.status_code == 201, resp.text
    return catalog["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,lang", GLOBAL_ROUTES)
@pytest.mark.parametrize("literal", BAD_LITERALS, ids=_literal_id)
async def test_invalid_literal_on_global_routes(app, method, path, lang, literal):
    """A date or timestamp that is not an RFC 3339 date or date-time gets 400."""
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        resp = await _send(client, method, path, _filter(literal), lang)

    _assert_rejected(resp, next(iter(literal.values())))


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,lang", GLOBAL_ROUTES)
async def test_invalid_interval_bound_on_global_routes(app, method, path, lang):
    """A bad interval bound gets 400 naming it, before the unsupported operator."""
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        resp = await _send(
            client, method, path, _filter(BAD_INTERVAL, "t_intersects"), lang
        )

    _assert_rejected(resp, "2000-19-39")


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,lang", CATALOG_ROUTES)
async def test_invalid_literal_on_catalog_routes(
    catalogs_app_client, load_test_data, ctx, method, path, lang
):
    """Catalog routes reject the same literals, whatever filter-lang they get."""
    cat = await _catalog(catalogs_app_client, load_test_data)
    resp = await catalogs_app_client.post(
        f"/catalogs/{cat}/collections", json={"id": ctx.collection["id"]}
    )
    assert resp.status_code == 200, resp.text
    path = path.format(catalog_id=cat, collection_id=ctx.collection["id"])

    for literal in BAD_LITERALS:
        resp = await _send(catalogs_app_client, method, path, _filter(literal), lang)
        _assert_rejected(resp, next(iter(literal.values())))


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("literal", BAD_LITERALS, ids=_literal_id)
async def test_invalid_literal_on_empty_catalog_search(
    catalogs_app_client, load_test_data, method, literal
):
    """A catalog with no collections checks the filter before it returns no items."""
    cat = await _catalog(catalogs_app_client, load_test_data)

    resp = await _send(
        catalogs_app_client,
        method,
        f"/catalogs/{cat}/search",
        _filter(literal),
        "cql2-json",
    )

    _assert_rejected(resp, next(iter(literal.values())))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cql2,lang,detail",
    [
        pytest.param(
            "datetime >= TIMESTAMP('2000-19-39T00:00:00Z')",
            None,
            "is not an RFC 3339 date-time",
            id="bad-text-timestamp",
        ),
        pytest.param(
            _filter({"timestamp": "2020-01-01T00:00:00Z"}),
            None,
            "expected valid CQL2 text",
            id="json-without-filter-lang",
        ),
        pytest.param(
            "true",
            "cql2-json",
            "expected a CQL2 expression with an operator",
            id="json-not-an-object",
        ),
    ],
)
async def test_empty_catalog_search_get_reads_filter_like_get_search(
    catalogs_app_client, load_test_data, cql2, lang, detail
):
    """GET search on a catalog with no collections reads the filter as /search does."""
    cat = await _catalog(catalogs_app_client, load_test_data)

    resp = await _send(
        catalogs_app_client, "GET", f"/catalogs/{cat}/search", cql2, lang
    )

    assert resp.status_code == 400, resp.text
    assert detail in resp.json()["detail"], resp.text


def test_check_cql2_literals_accepts_valid_filter():
    """Valid literals, open bounds and property bounds pass, and nothing changes."""
    cql2 = {
        "op": "and",
        "args": [
            _filter({"date": "2020-01-01"}),
            _filter({"timestamp": "2020-01-01T00:00:00.5+05:30"}, ">="),
            _filter({"interval": ["..", "2020-01-01T00:00:00Z"]}, "t_intersects"),
            _filter(
                {"interval": [{"property": "start_datetime"}, ".."]}, "t_intersects"
            ),
            {"op": "=", "args": [{"property": "eo:cloud_cover"}, 1.0]},
        ],
    }
    expected = copy.deepcopy(cql2)

    assert check_cql2_literals(cql2) is None
    assert cql2 == expected
    assert isinstance(cql2["args"][-1]["args"][1], float)


@pytest.mark.parametrize("literal", BAD_LITERALS + [BAD_INTERVAL], ids=_literal_id)
def test_check_cql2_literals_rejects_invalid_literal(literal):
    """Each invalid literal raises CQL2FilterError, however deep it sits."""
    cql2 = {"op": "not", "args": [_filter(literal, "t_intersects")]}

    with pytest.raises(CQL2FilterError, match="is not an RFC 3339"):
        check_cql2_literals(cql2)


def test_check_cql2_literals_walks_past_the_recursion_limit():
    """A filter nested deeper than Python recurses is still walked, not a RecursionError."""
    cql2 = _filter({"timestamp": "2000-19-39T00:00:00Z"})
    for _ in range(sys.getrecursionlimit() + 100):
        cql2 = {"op": "not", "args": [cql2]}

    with pytest.raises(CQL2FilterError, match="is not an RFC 3339"):
        check_cql2_literals(cql2)
