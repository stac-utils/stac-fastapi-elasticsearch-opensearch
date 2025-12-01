import json
import os
from unittest import mock

import pytest

if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
    from stac_fastapi.opensearch.app import app_config
else:
    from stac_fastapi.elasticsearch.app import app_config


def get_core_client():
    if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
        from stac_fastapi.opensearch.app import app_config
    else:
        from stac_fastapi.elasticsearch.app import app_config
    return app_config["client"]


def reload_queryables_settings():
    client = get_core_client()
    if hasattr(client, "queryables_cache"):
        client.queryables_cache.reload_settings()


@pytest.fixture(autouse=True)
def enable_validation():

    client = app_config["client"]
    with mock.patch.dict(os.environ, {"VALIDATE_QUERYABLES": "true"}):
        client.queryables_cache.reload_settings()
        yield
    client.queryables_cache.reload_settings()


@pytest.mark.asyncio
async def test_search_post_query_valid_param(app_client, ctx):
    """Test POST /search with a valid query parameter"""
    query = {"query": {"eo:cloud_cover": {"lt": 10}}}
    resp = await app_client.post("/search", json=query)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_post_query_invalid_param(app_client, ctx):
    """Test POST /search with an invalid query parameter"""
    query = {"query": {"invalid_param": {"eq": "test"}}}
    resp = await app_client.post("/search", json=query)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert "Invalid query fields: invalid_param" in resp_json["detail"]


@pytest.mark.asyncio
async def test_item_collection_get_filter_valid_param(app_client, ctx):
    """Test GET /collections/{collection_id}/items with a valid filter parameter"""
    collection_id = ctx.item["collection"]
    filter_body = {
        "op": "<",
        "args": [{"property": "eo:cloud_cover"}, 10],
    }
    params = {
        "filter-lang": "cql2-json",
        "filter": json.dumps(filter_body),
    }
    resp = await app_client.get(f"/collections/{collection_id}/items", params=params)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_item_collection_get_filter_invalid_param(app_client, ctx):
    """Test GET /collections/{collection_id}/items with an invalid filter parameter"""
    collection_id = ctx.item["collection"]
    filter_body = {
        "op": "=",
        "args": [{"property": "invalid_param"}, "test"],
    }
    params = {
        "filter-lang": "cql2-json",
        "filter": json.dumps(filter_body),
    }
    resp = await app_client.get(f"/collections/{collection_id}/items", params=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert "Invalid query fields: invalid_param" in resp_json["detail"]


@pytest.mark.asyncio
async def test_validate_queryables_excluded(app_client, ctx):
    """Test that excluded queryables are rejected when validation is enabled."""

    excluded_field = "eo:cloud_cover"
    client = app_config["client"]

    with mock.patch.dict(
        os.environ,
        {
            "VALIDATE_QUERYABLES": "true",
            "EXCLUDED_FROM_QUERYABLES": excluded_field,
            "QUERYABLES_CACHE_TTL": "0",
        },
    ):
        client.queryables_cache.reload_settings()

        query = {"query": {excluded_field: {"lt": 10}}}
        resp = await app_client.post("/search", json=query)
        assert resp.status_code == 400
        assert "Invalid query fields" in resp.json()["detail"]
        assert excluded_field in resp.json()["detail"]

        query = {"query": {"id": {"eq": "test-item"}}}
        resp = await app_client.post("/search", json=query)
        assert resp.status_code == 200

    client.queryables_cache.reload_settings()
