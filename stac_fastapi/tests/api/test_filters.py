"""Tests for STAC API filter extension."""

import json

import pytest


@pytest.mark.asyncio
async def test_filter_by_id(app_client, ctx):
    """Test filtering items by ID using the filter parameter."""
    # Get the test item and collection from the context
    item = ctx.item
    collection_id = item["collection"]
    item_id = item["id"]

    # Create a filter to match the item by ID
    filter_body = {"op": "=", "args": [{"property": "id"}, item_id]}

    # Make the request with the filter
    params = [("filter", json.dumps(filter_body)), ("filter-lang", "cql2-json")]

    resp = await app_client.get(
        f"/collections/{collection_id}/items",
        params=params,
    )

    # Verify the response
    assert resp.status_code == 200
    resp_json = resp.json()

    # Should find exactly one matching item
    assert len(resp_json["features"]) == 1
    assert resp_json["features"][0]["id"] == item_id
    assert resp_json["features"][0]["collection"] == collection_id


@pytest.mark.asyncio
async def test_filter_by_nonexistent_id(app_client, ctx):
    """Test filtering with a non-existent ID returns no results."""
    collection_id = ctx.item["collection"]

    # Create a filter with a non-existent ID
    filter_body = {
        "op": "=",
        "args": [{"property": "id"}, "this-id-does-not-exist-12345"],
    }

    # Make the request with the filter
    params = [("filter", json.dumps(filter_body)), ("filter-lang", "cql2-json")]

    resp = await app_client.get(
        f"/collections/{collection_id}/items",
        params=params,
    )

    # Verify the response
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0
