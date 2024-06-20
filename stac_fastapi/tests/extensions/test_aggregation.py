import os

import pytest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.asyncio
async def test_aggregation_extension_landing_page_link(app_client, ctx):
    """Test if the `aggregations` and `aggregate` links are included in the landing page"""
    resp = await app_client.get("/")
    assert resp.status_code == 200

    resp_json = resp.json()
    keys = [link["rel"] for link in resp_json["links"]]

    assert "aggregations" in keys
    assert "aggregate" in keys


@pytest.mark.asyncio
async def test_aggregation_extension_collection_link(app_client, load_test_data):
    """Test if the `aggregations` and `aggregate` links are included in the collection links"""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test"

    resp = await app_client.post("/collections", json=test_collection)
    assert resp.status_code == 201

    resp = await app_client.get(f"/collections/{test_collection['id']}")
    resp_json = resp.json()
    keys = [link["rel"] for link in resp_json["links"]]
    assert "aggregations" in keys
    assert "aggregate" in keys

    resp = await app_client.delete(f"/collections/{test_collection['id']}")
    assert resp.status_code == 204
