"""HTTP coverage of aggregation parsing and manual GET model validation."""

from unittest.mock import AsyncMock
from urllib.parse import quote_plus

import orjson
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from lark.exceptions import UnexpectedInput
from pydantic import ValidationError

from stac_fastapi.sfeos_helpers.aggregation import client as aggregation

pytestmark = pytest.mark.asyncio
ROUTES = ["/aggregate", "/collections/{collection_id}/aggregate"]
GEOMETRY = {"type": "Point", "coordinates": [151, -33]}
BBOX = [149, -35, 153, -31]
CASES = [
    pytest.param(
        {"filter": "{", "filter-lang": "cql2-json"},
        "Invalid filter parameter: expected valid CQL2 JSON.",
        orjson.JSONDecodeError,
        id="json-filter",
    ),
    pytest.param(
        {"filter": "id =", "filter-lang": "cql2-text"},
        "Invalid filter parameter: expected valid CQL2 text.",
        UnexpectedInput,
        id="text-incomplete",
    ),
    pytest.param(
        {"filter": "id = @", "filter-lang": "cql2-text"},
        "Invalid filter parameter: expected valid CQL2 text.",
        UnexpectedInput,
        id="text-token",
    ),
    pytest.param(
        {"intersects": "{"},
        "Invalid intersects parameter: expected valid JSON.",
        orjson.JSONDecodeError,
        id="intersects-json",
    ),
    pytest.param(
        {
            "bbox": ",".join(map(str, BBOX)),
            "intersects": orjson.dumps(GEOMETRY).decode(),
        },
        "Invalid aggregation parameters.",
        ValidationError,
        id="model-bbox-intersects",
    ),
]


@pytest.fixture
def test_item(load_test_data):
    item = load_test_data("test_item.json")
    item["id"] = "river-banks-test-item"
    return item

@pytest_asyncio.fixture(scope="session")
async def aggregation_http(app):
    """Use a dedicated client that exposes server error responses."""
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test-server",
        ) as client:
            yield client


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("params,detail,original_error", CASES)
async def test_malformed_get(
    aggregation_http, app, ctx, monkeypatch, route, params, detail, original_error
):
    execute = AsyncMock(side_effect=AssertionError("aggregation must not execute"))
    monkeypatch.setattr(app.state.app_config["client"].database, "aggregate", execute)
    url = route.format(collection_id=ctx.collection["id"])
    params = {"aggregations": "total_count", **params}
    response = await aggregation_http.get(url, params=params)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test-server"
    ) as raising:
        if response.status_code == 500:
            # The unchanged baseline must fail the status assertion below,
            # after establishing that the intended original exception occurred.
            with pytest.raises(original_error):
                await raising.get(url, params=params)
        else:
            paired = await raising.get(url, params=params)
            assert paired.status_code == 400
            assert paired.json() == {"detail": detail}
    execute.assert_not_awaited()
    assert response.status_code == 400, response.text
    assert response.json() == {"detail": detail}


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "kind", ["empty", "json", "text", "like", "bbox", "intersects"]
)
async def test_valid_controls(aggregation_http, ctx, route, kind):
    url = route.format(collection_id=ctx.collection["id"])
    expression = {"op": "=", "args": [{"property": "id"}, ctx.item["id"]]}
    params = {"aggregations": "total_count"}
    body = {"aggregations": ["total_count"]}
    expected = 1
    if kind == "json":
        params.update(
            {"filter": orjson.dumps(expression).decode(), "filter-lang": "cql2-json"}
        )
        body.update({"filter": expression, "filter-lang": "cql2-json"})
    elif kind == "text":
        params["filter"] = f"id = '{ctx.item['id']}'"
        body.update({"filter": expression, "filter-lang": "cql2-json"})
    elif kind == "like":
        expression = {"op": "like", "args": [{"property": "id"}, "%banks%"]}
        params.update(
            {"filter": orjson.dumps(expression).decode(), "filter-lang": "cql2-json"}
        )
        body.update({"filter": expression, "filter-lang": "cql2-json"})
    elif kind == "bbox":
        params["bbox"] = ",".join(map(str, BBOX))
        body["bbox"] = BBOX
    elif kind == "intersects":
        # Preserve the existing additional unquote_plus on intersects.
        params["intersects"] = quote_plus(orjson.dumps(ctx.item["geometry"]).decode())
        body["intersects"] = ctx.item["geometry"]
    get = await aggregation_http.get(url, params=params)
    post = await aggregation_http.post(url, json=body)
    for response in (get, post):
        assert response.status_code == 200, response.text
        assert response.json()["aggregations"][0]["value"] == expected
    assert get.json()["aggregations"] == post.json()["aggregations"]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "settings,detail",
    [
        ({"datetime_frequency_interval": "day"}, None),
        ({"geometry_geohash_grid_frequency_precision": 5}, None),
        (
            {"geometry_geohash_grid_frequency_precision": 55},
            "Invalid precision value. Must be between 1 and 12",
        ),
        (
            {"datetime_frequency_interval": "invalid"},
            "Invalid datetime interval. Must be one of "
            "['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second']",
        ),
    ],
)
async def test_precision_interval_controls(
    aggregation_http, ctx, route, settings, detail
):
    url = route.format(collection_id=ctx.collection["id"])
    get = await aggregation_http.get(
        url, params={"aggregations": "total_count,datetime_frequency", **settings}
    )
    post = await aggregation_http.post(
        url, json={"aggregations": ["total_count", "datetime_frequency"], **settings}
    )
    for response in (get, post):
        assert response.status_code == (400 if detail else 200), response.text
        if detail:
            assert response.json() == {"detail": detail}
        else:
            values = {agg["name"]: agg for agg in response.json()["aggregations"]}
            assert values["total_count"]["value"] == 1
            assert values["datetime_frequency"]["buckets"][0]["frequency"] == 1
    assert (
        get.json()["detail" if detail else "aggregations"]
        == post.json()["detail" if detail else "aggregations"]
    )


@pytest.mark.parametrize("route", ROUTES)
async def test_existing_errors(aggregation_http, ctx, route):
    url = route.format(collection_id=ctx.collection["id"])
    response = await aggregation_http.get(url)
    assert response.status_code == 400
    assert response.json() == {
        "detail": "No 'aggregations' found. Use '/aggregations' to return available aggregations"
    }
    body = {"aggregations": ["total_count"], "bbox": BBOX, "intersects": GEOMETRY}
    response = await aggregation_http.post(url, json=body)
    assert response.status_code == 400
    assert response.json() == {
        "detail": [
            {
                "type": "value_error",
                "loc": ["body"],
                "msg": "Value error, intersects and bbox parameters are mutually exclusive",
                "input": body,
                "ctx": {"error": {}},
            }
        ],
        "body": body,
    }
    response = await aggregation_http.get(
        url,
        params={
            "aggregations": "total_count",
            "filter-lang": "cql-json",
            "filter": "{}",
        },
    )
    assert response.status_code == 400
    assert response.json()["detail"][0]["type"] == "literal_error"
    assert response.json()["detail"][0]["loc"] == ["query", "filter-lang"]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "stage",
    [
        "parser",
        "serializer",
        "generated-json",
        "json",
        "value",
        "validation",
        "backend",
    ],
)
async def test_server_failures_propagate(
    aggregation_http, app, ctx, monkeypatch, route, stage
):
    error = RuntimeError("injected aggregation failure")
    if stage == "json":
        error = orjson.JSONDecodeError("injected JSON failure", "{", 0)
    elif stage == "value":
        error = ValueError("injected value failure")
    elif stage == "validation":
        with pytest.raises(ValidationError) as caught:
            aggregation.EsAggregationExtensionPostRequest(
                bbox=BBOX, intersects=GEOMETRY
            )
        error = caught.value

    def fail(*args, **kwargs):
        raise error

    params = {"aggregations": "total_count"}
    if stage in {"parser", "serializer", "generated-json"}:
        params["filter"] = f"id = '{ctx.item['id']}'"
        if stage == "parser":
            monkeypatch.setattr(aggregation, "parse_cql2_text", fail)
        elif stage == "serializer":
            monkeypatch.setattr(aggregation, "to_cql2", fail)
        else:
            monkeypatch.setattr(aggregation, "to_cql2", lambda ast: "{")
            error = orjson.JSONDecodeError("generated", "{", 0)
    else:
        monkeypatch.setattr(
            app.state.app_config["client"].database,
            "aggregate",
            AsyncMock(side_effect=error),
        )
    url = route.format(collection_id=ctx.collection["id"])
    response = await aggregation_http.get(url, params=params)
    assert response.status_code == 500, response.text
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test-server"
    ) as raising:
        with pytest.raises(type(error)) as caught:
            await raising.get(url, params=params)
    if stage != "generated-json":
        assert caught.value is error


@pytest.mark.parametrize("route", ROUTES)
async def test_existing_apply_filter_guard(
    aggregation_http, app, ctx, monkeypatch, route
):
    monkeypatch.setattr(
        app.state.app_config["client"].database,
        "apply_cql2_filter",
        AsyncMock(side_effect=RuntimeError("existing apply guard")),
    )
    response = await aggregation_http.get(
        route.format(collection_id=ctx.collection["id"]),
        params={"aggregations": "total_count", "filter": f"id = '{ctx.item['id']}'"},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Error with cql2 filter: existing apply guard"}
