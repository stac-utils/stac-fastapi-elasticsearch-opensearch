import json
import os
from unittest import mock

import pytest

from stac_fastapi.sfeos_helpers.queryables import reload_queryables_settings


@pytest.fixture(autouse=True)
def enable_validation():
    with mock.patch.dict(os.environ, {"VALIDATE_QUERYABLES": "true"}):
        reload_queryables_settings()
        yield
    reload_queryables_settings()


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

    with mock.patch.dict(
        os.environ,
        {
            "VALIDATE_QUERYABLES": "true",
            "EXCLUDED_FROM_QUERYABLES": excluded_field,
            "QUERYABLES_CACHE_TTL": "0",
        },
    ):
        reload_queryables_settings()

        query = {"query": {excluded_field: {"lt": 10}}}
        resp = await app_client.post("/search", json=query)
        assert resp.status_code == 400
        assert "Invalid query fields" in resp.json()["detail"]
        assert excluded_field in resp.json()["detail"]

        query = {"query": {"id": {"eq": "test-item"}}}
        resp = await app_client.post("/search", json=query)
        assert resp.status_code == 200

    reload_queryables_settings()
