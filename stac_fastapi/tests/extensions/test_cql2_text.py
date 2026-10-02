"""CQL2 text filters on GET routes: the CQL2 JSON they become, and the results."""

import copy
import json
from unittest.mock import AsyncMock

import orjson
import pytest
from httpx import ASGITransport, AsyncClient

from ..conftest import create_collection, create_item, refresh_indices


def _eq(name, value):
    return {"op": "=", "args": [{"property": name}, value]}


# CQL2 text and the CQL2 JSON it stands for (OGC 21-065r2).
TEXT_TO_JSON = [
    pytest.param(
        "NOT (id = 'a' OR id = 'b')",
        {"op": "not", "args": [{"op": "or", "args": [_eq("id", "a"), _eq("id", "b")]}]},
        id="not-before-a-group",
    ),
    pytest.param(
        "id NOT LIKE 'a%'",
        {"op": "not", "args": [{"op": "like", "args": [{"property": "id"}, "a%"]}]},
        id="not-like",
    ),
    pytest.param(
        "id NOT IN ('a', 'b')",
        {"op": "not", "args": [{"op": "in", "args": [{"property": "id"}, ["a", "b"]]}]},
        id="not-in",
    ),
    pytest.param(
        "gsd NOT BETWEEN 10 AND 20",
        {
            "op": "not",
            "args": [{"op": "between", "args": [{"property": "gsd"}, 10, 20]}],
        },
        id="not-between",
    ),
    pytest.param(
        "id = 'a' OR id = 'b' AND id = 'c'",
        {
            "op": "or",
            "args": [
                _eq("id", "a"),
                {"op": "and", "args": [_eq("id", "b"), _eq("id", "c")]},
            ],
        },
        id="and-binds-tighter-than-or",
    ),
    pytest.param(
        "A_OVERLAPS(tags, ('a', 'b'))",
        {"op": "a_overlaps", "args": [{"property": "tags"}, ["a", "b"]]},
        id="a-overlaps",
    ),
    pytest.param(
        "A_CONTAINS(tags, ('a'))",
        {"op": "a_contains", "args": [{"property": "tags"}, ["a"]]},
        id="a-contains",
    ),
    pytest.param(
        "A_EQUALS(tags, ('a', 'b'))",
        {"op": "a_equals", "args": [{"property": "tags"}, ["a", "b"]]},
        id="a-equals",
    ),
    pytest.param(
        "A_CONTAINEDBY(tags, ('a', 'b'))",
        {"op": "a_containedBy", "args": [{"property": "tags"}, ["a", "b"]]},
        id="a-containedby",
    ),
    pytest.param(
        "gsd = 15 AND eo:cloud_cover < 10.5",
        {
            "op": "and",
            "args": [
                _eq("gsd", 15),
                {"op": "<", "args": [{"property": "eo:cloud_cover"}, 10.5]},
            ],
        },
        id="whole-numbers-stay-integers",
    ),
    pytest.param(
        "datetime >= TIMESTAMP('2020-02-12T00:00:00Z')",
        {
            "op": ">=",
            "args": [{"property": "datetime"}, {"timestamp": "2020-02-12T00:00:00Z"}],
        },
        id="timestamp",
    ),
    pytest.param(
        "datetime >= DATE('2020-02-12')",
        {"op": ">=", "args": [{"property": "datetime"}, {"date": "2020-02-12"}]},
        id="date",
    ),
]


def _same_json(actual, expected):
    """Compare as JSON text, so 15 and 15.0 differ."""
    if isinstance(actual, str):
        actual = orjson.loads(actual)
    option = orjson.OPT_SORT_KEYS
    return orjson.dumps(actual, option=option) == orjson.dumps(expected, option=option)


@pytest.mark.asyncio
@pytest.mark.parametrize("filter_lang", ["cql2-text", None])
@pytest.mark.parametrize("text,expected", TEXT_TO_JSON)
async def test_search_get_cql2_text_to_json(
    app, app_client, monkeypatch, text, expected, filter_lang
):
    """GET /search hands the translator the CQL2 JSON form of a CQL2 text filter."""
    database = app.state.app_config["client"].database
    apply = AsyncMock(side_effect=lambda search, _filter: (search, None))
    monkeypatch.setattr(database, "apply_cql2_filter", apply)
    params = {"filter": text}
    if filter_lang:
        params["filter-lang"] = filter_lang

    resp = await app_client.get("/search", params=params)

    assert resp.status_code == 200, resp.text
    apply.assert_awaited_once()
    assert _same_json(apply.await_args.args[1], expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("filter_lang", ["cql2-text", None])
@pytest.mark.parametrize("text,expected", TEXT_TO_JSON)
async def test_collections_get_cql2_text_to_json(
    app, app_client, monkeypatch, text, expected, filter_lang
):
    """GET /collections hands the database the CQL2 JSON form of a CQL2 text filter."""
    database = app.state.app_config["client"].database
    get_all = AsyncMock(return_value=([], None, 0))
    monkeypatch.setattr(database, "get_all_collections", get_all)
    params = {"filter": text}
    if filter_lang:
        params["filter-lang"] = filter_lang

    resp = await app_client.get("/collections", params=params)

    assert resp.status_code == 200, resp.text
    get_all.assert_awaited_once()
    assert _same_json(get_all.await_args.kwargs["filter"], expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("text,expected", TEXT_TO_JSON)
async def test_aggregate_get_cql2_text_to_json(
    app, app_client, monkeypatch, text, expected
):
    """GET /aggregate hands the translator the CQL2 JSON form of a CQL2 text filter."""
    database = app.state.app_config["client"].database
    apply = AsyncMock(side_effect=lambda search, _filter: (search, None))
    monkeypatch.setattr(database, "apply_cql2_filter", apply)

    resp = await app_client.get(
        "/aggregate",
        params={
            "aggregations": "total_count",
            "filter-lang": "cql2-text",
            "filter": text,
        },
    )

    assert resp.status_code == 200, resp.text
    apply.assert_awaited_once()
    assert _same_json(apply.await_args.args[1], expected)


async def _add_items(txn_client, ctx, item_ids, **properties):
    for item_id in item_ids:
        item = copy.deepcopy(ctx.item)
        item["id"] = item_id
        item["properties"].update(properties.get(item_id, {}))
        await create_item(txn_client, item)
    await refresh_indices(txn_client)


async def _search_ids(app_client, ctx, text):
    resp = await app_client.get(
        "/search",
        params={
            "collections": ctx.collection["id"],
            "filter-lang": "cql2-text",
            "filter": text,
        },
    )
    assert resp.status_code == 200, resp.text
    return {f["id"] for f in resp.json()["features"]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "negated",
    [
        "id NOT LIKE 'negated-skip-%'",
        "id NOT IN ('negated-skip-1', 'negated-skip-2')",
        "gsd NOT BETWEEN 20 AND 40",
    ],
)
async def test_search_cql2_text_negated_predicate(app_client, txn_client, ctx, negated):
    """NOT LIKE, NOT IN and NOT BETWEEN leave out the items they match."""
    await _add_items(
        txn_client,
        ctx,
        ["negated-keep-1", "negated-skip-1", "negated-skip-2"],
        **{"negated-skip-1": {"gsd": 30}, "negated-skip-2": {"gsd": 30}},
    )

    ids = await _search_ids(app_client, ctx, f"{negated} AND id LIKE 'negated-%'")

    assert ids == {"negated-keep-1"}


@pytest.mark.asyncio
async def test_search_cql2_text_not_before_a_group(app_client, txn_client, ctx):
    """NOT may stand before a parenthesized condition (booleanFactor)."""
    await _add_items(txn_client, ctx, ["group-a", "group-b-1", "group-c"])

    ids = await _search_ids(
        app_client,
        ctx,
        "NOT (id = 'group-a' OR id LIKE 'group-b%') AND id LIKE 'group-%'",
    )

    assert ids == {"group-c"}


@pytest.mark.asyncio
async def test_search_cql2_text_and_binds_tighter_than_or(app_client, txn_client, ctx):
    """`a OR b AND c` means `a OR (b AND c)`."""
    await _add_items(txn_client, ctx, ["prec-a", "prec-b", "prec-c"])

    ids = await _search_ids(
        app_client, ctx, "id = 'prec-a' OR id = 'prec-b' AND id = 'prec-c'"
    )

    assert ids == {"prec-a"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "text,expected",
    [
        pytest.param(
            "id NOT LIKE 'cql2text-skip%' AND id LIKE 'cql2text-%'",
            {"cql2text-keep", "cql2text-other"},
            id="not-like",
        ),
        pytest.param(
            "id NOT IN ('cql2text-skip') AND id LIKE 'cql2text-%'",
            {"cql2text-keep", "cql2text-other"},
            id="not-in",
        ),
        pytest.param(
            "NOT (id = 'cql2text-keep' OR id = 'cql2text-skip') "
            "AND id LIKE 'cql2text-%'",
            {"cql2text-other"},
            id="not-before-a-group",
        ),
        pytest.param(
            "id = 'cql2text-keep' OR id = 'cql2text-skip' AND id = 'cql2text-other'",
            {"cql2text-keep"},
            id="and-binds-tighter-than-or",
        ),
    ],
)
async def test_collections_cql2_text(
    app_client, txn_client, ctx, load_test_data, text, expected
):
    """GET /collections reads NOT and AND/OR in CQL2 text as CQL2 defines them."""
    for collection_id in ("cql2text-keep", "cql2text-skip", "cql2text-other"):
        collection = copy.deepcopy(load_test_data("test_collection.json"))
        collection["id"] = collection_id
        await create_collection(txn_client, collection)
    await refresh_indices(txn_client)

    resp = await app_client.get(
        "/collections", params={"filter-lang": "cql2-text", "filter": text}
    )

    assert resp.status_code == 200, resp.text
    assert {c["id"] for c in resp.json()["collections"]} == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "filter_lang,text",
    [("cql2-text", "TRUE"), ("cql2-text", "FALSE"), ("cql2-json", "false")],
)
async def test_collections_filter_without_operator(app, filter_lang, text):
    """A filter that is only a boolean is refused with 400, not ignored or a 500."""
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        resp = await client.get(
            "/collections", params={"filter-lang": filter_lang, "filter": text}
        )

    assert resp.status_code == 400, resp.text


async def _get(app, route, text):
    params = {"filter-lang": "cql2-text", "filter": text}
    if route == "/aggregate":
        params["aggregations"] = "total_count"
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        return await client.get(route, params=params)


ROUTES = ["/search", "/collections", "/aggregate"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "route,literal",
    [
        (route, literal)
        for route in ROUTES
        for literal in (
            "DATE('nope')",
            "DATE('2020-01-01T00:00:00Z')",
            "TIMESTAMP('nope')",
            "TIMESTAMP('2020-01-01')",
        )
    ]
    + [("/collections", "DATE('2000-19-39')")],
)
async def test_invalid_temporal_literal(app, app_client, route, literal):
    """A DATE that is not an RFC 3339 date, or a TIMESTAMP that is not an RFC 3339
    date-time, gets 400."""
    resp = await _get(app, route, f"datetime >= {literal}")

    assert resp.status_code == 400, resp.text


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "text,op",
    [
        (
            "T_INTERSECTS(datetime, INTERVAL('2020-01-01T00:00:00Z', '..'))",
            "t_intersects",
        ),
        (
            "id = 'a' AND T_BEFORE(datetime, TIMESTAMP('2030-01-01T00:00:00Z'))",
            "t_before",
        ),
    ],
)
async def test_unsupported_operator_is_named(app, app_client, route, text, op):
    """An operator the translator does not support gets 400 naming it."""
    resp = await _get(app, route, text)

    assert resp.status_code == 400, resp.text
    assert op in resp.json()["detail"], resp.text


@pytest.mark.asyncio
async def test_unsupported_operator_is_named_in_cql2_json(app, app_client):
    """The same holds for CQL2 JSON."""
    cql2_json = {
        "op": "t_intersects",
        "args": [{"property": "datetime"}, {"interval": ["2020-01-01", ".."]}],
    }
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test-server",
    ) as client:
        resp = await client.post("/search", json={"filter": cql2_json})

    assert resp.status_code == 400, resp.text
    assert "t_intersects" in resp.json()["detail"], resp.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "date,expected",
    [("2020-02-12", {"dated"}), ("2020-02-13", set())],
)
async def test_search_cql2_date_literal(app_client, txn_client, ctx, date, expected):
    """A CQL2 date literal compares as a date, in CQL2 text and in CQL2 JSON."""
    await _add_items(
        txn_client, ctx, ["dated"], dated={"created": "2020-02-12T12:30:22Z"}
    )
    id_filter = {"op": "=", "args": [{"property": "id"}, "dated"]}
    date_filter = {"op": ">=", "args": [{"property": "created"}, {"date": date}]}

    text_ids = await _search_ids(
        app_client, ctx, f"id = 'dated' AND created >= DATE('{date}')"
    )
    resp = await app_client.post(
        "/search",
        json={
            "collections": [ctx.collection["id"]],
            "filter-lang": "cql2-json",
            "filter": {"op": "and", "args": [id_filter, date_filter]},
        },
    )

    assert text_ids == expected
    assert resp.status_code == 200, resp.text
    assert {f["id"] for f in resp.json()["features"]} == expected


@pytest.mark.asyncio
async def test_search_cql2_text_numbers_match_json(app_client, ctx):
    """A whole number in CQL2 text selects the same items as in CQL2 JSON."""
    row = int(ctx.item["properties"]["landsat:row"])
    id_filter = {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]}
    row_filter = {"op": "=", "args": [{"property": "landsat:row"}, row]}

    text_resp = await app_client.get(
        "/search",
        params={
            "filter-lang": "cql2-text",
            "filter": f"id = '{ctx.item['id']}' AND landsat:row = {row}",
        },
    )
    json_resp = await app_client.get(
        "/search",
        params={
            "filter-lang": "cql2-json",
            "filter": json.dumps({"op": "and", "args": [id_filter, row_filter]}),
        },
    )

    for resp in (text_resp, json_resp):
        assert resp.status_code == 200, resp.text
        assert len(resp.json()["features"]) == 1
