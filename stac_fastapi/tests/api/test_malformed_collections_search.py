"""HTTP regression coverage for manual POST collections-search validation."""

import json
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from stac_fastapi.core.core import CoreClient
from stac_fastapi.core.extensions.collections_search import CollectionsSearchRequest

pytestmark = pytest.mark.asyncio
URL = "/collections-search"
ERROR = {"detail": "Invalid collections search parameters."}
MALFORMED = [
    {"limit": "bad"},
    {"bbox": "bad"},
    {"datetime": 5},
    {"query": {}},
    {"q": 5},
    {"token": 5},
    {"filter": []},
    {"fields": "bad"},
    {"sortby": "bad"},
    {"datetime": "bad"},
]


@pytest.fixture
def test_collection(load_test_data):
    collection = load_test_data("test_collection.json")
    collection["title"] = "Collection auditneedle"
    collection["extent"]["temporal"]["interval"] = [
        ["1990-01-01T00:00:00Z", "2030-01-01T00:00:00Z"]
    ]
    return collection


@pytest_asyncio.fixture(scope="session")
async def collections_http(app, app_client):
    """Expose server errors without changing the shared raising client."""
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test-server",
        ) as client:
            yield client


@pytest.mark.parametrize("body", MALFORMED)
async def test_malformed_post(collections_http, app_client, monkeypatch, body):
    forward = AsyncMock(side_effect=AssertionError("must reject before forwarding"))
    monkeypatch.setattr(CoreClient, "post_all_collections", forward)
    response = await collections_http.post(URL, json=body)
    if response.status_code == 500:
        # Establish the original failure before the unchanged baseline fails below.
        with pytest.raises(ValidationError):
            await app_client.post(URL, json=body)
    else:
        paired = await app_client.post(URL, json=body)
        assert paired.status_code == 400
        assert paired.json() == ERROR
    forward.assert_not_awaited()
    assert response.status_code == 400, response.text
    assert response.json() == ERROR


@pytest.mark.parametrize("body", ["bad", 5, []])
async def test_top_level_admission(collections_http, app_client, body):
    expected = {
        "detail": [
            {
                "type": "dict_type",
                "loc": ["body"],
                "msg": "Input should be a valid dictionary",
                "input": body,
            }
        ],
        "body": body,
    }
    for client in (collections_http, app_client):
        response = await client.post(URL, json=body)
        assert response.status_code == 400
        assert response.json() == expected


@pytest.mark.parametrize("content", [None, b"", b"null"])
async def test_missing_body(collections_http, app_client, content):
    expected = {
        "detail": [
            {"type": "missing", "loc": ["body"], "msg": "Field required", "input": None}
        ],
        "body": None,
    }
    for client in (collections_http, app_client):
        kwargs = {} if content is None else {"content": content}
        response = await client.post(
            URL, headers={"content-type": "application/json"}, **kwargs
        )
        assert response.status_code == 400
        assert response.json() == expected


@pytest.mark.parametrize(
    "kind",
    [
        "empty",
        "json",
        "json-alias",
        "text",
        "text-alias",
        "query",
        "q",
        "q-list",
        "limit",
        "precedence",
        "bad-query-limit",
        "bbox",
        "instant",
        "open-start",
        "open-end",
        "sort-asc",
        "sort-desc",
        "include",
        "exclude",
    ],
)
async def test_populated_controls(collections_http, ctx, kind):
    collection_id = ctx.collection["id"]
    expression = {"op": "=", "args": [{"property": "id"}, collection_id]}
    body, params = {}, {}
    if kind.startswith(("json", "text")):
        language = "text" if kind.startswith("text") else "json"
        alias = "filter-lang" if kind.endswith("alias") else "filter_lang"
        body = {
            "filter": f"id = '{collection_id}'" if language == "text" else expression,
            alias: f"cql2-{language}",
        }
    elif kind == "query":
        body = {"query": json.dumps({"id": {"eq": collection_id}})}
    elif kind in {"q", "q-list"}:
        body = {"q": "auditneedle" if kind == "q" else ["auditneedle"]}
    elif kind in {"limit", "precedence", "bad-query-limit"}:
        body = {"limit": "1" if kind == "limit" else 1}
        params = {"limit": "bad" if kind == "bad-query-limit" else "2"}
    elif kind == "bbox":
        body = {"bbox": [-180, -90, 180, 90]}
    elif kind in {"instant", "open-start", "open-end"}:
        value = "2000-01-01T00:00:00Z"
        body = {
            "datetime": {
                "instant": value,
                "open-start": f"../{value}",
                "open-end": f"{value}/..",
            }[kind]
        }
    elif kind.startswith("sort"):
        body = {"sortby": [{"field": "id", "direction": kind.split("-")[1]}]}
    elif kind == "include":
        body = {"fields": {"include": ["id", "title"]}}
    elif kind == "exclude":
        body = {"fields": {"exclude": ["description"]}}
    response = await collections_http.post(URL, json=body, params=params)
    assert response.status_code == 200, response.text
    collections = response.json()["collections"]
    assert len(collections) == 1
    assert collections[0]["id"] == collection_id
    assert collections[0]["title"] == ctx.collection["title"]
    if kind in {"include", "exclude"}:
        assert "description" not in collections[0]
    if kind == "empty":
        get = await collections_http.get(URL)
        assert get.status_code == 200
        assert get.json()["collections"] == collections


@pytest.mark.parametrize(
    "fields,error",
    [
        ({"include": "bad"}, ValidationError),
        ({"include": 5}, ValidationError),
        (None, AttributeError),
    ],
)
async def test_later_field_errors(collections_http, app_client, fields, error):
    body = {"fields": fields}
    CollectionsSearchRequest.model_validate(body)
    response = await collections_http.post(URL, json=body)
    assert response.status_code == 500
    with pytest.raises(error):
        await app_client.post(URL, json=body)


@pytest.mark.parametrize(
    "stage", ["model-runtime", "validation", "value", "json", "runtime"]
)
async def test_unrelated_failures_propagate(
    collections_http, app_client, monkeypatch, stage
):
    error = RuntimeError("injected collections search failure")
    if stage == "validation":
        with pytest.raises(ValidationError) as caught:
            CollectionsSearchRequest.model_validate({"limit": "bad"})
        error = caught.value
    elif stage == "value":
        error = ValueError("injected value failure")
    elif stage == "json":
        error = json.JSONDecodeError("injected JSON failure", "{", 0)
    if stage == "model-runtime":

        def fail(*args, **kwargs):
            raise error

        monkeypatch.setattr(CollectionsSearchRequest, "model_validate", fail)
    else:
        monkeypatch.setattr(
            CoreClient, "post_all_collections", AsyncMock(side_effect=error)
        )
    response = await collections_http.post(URL, json={})
    assert response.status_code == 500
    with pytest.raises(type(error)) as caught:
        await app_client.post(URL, json={})
    assert caught.value is error
