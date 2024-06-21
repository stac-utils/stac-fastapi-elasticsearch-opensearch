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


@pytest.mark.asyncio
async def test_get_catalog_aggregations(app_client):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.get("/aggregations")

    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 4


@pytest.mark.asyncio
async def test_post_catalog_aggregations(app_client):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.post("/aggregations")

    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 4


@pytest.mark.asyncio
async def test_get_collection_aggregations(app_client, load_test_data):
    # there's one item that can match, so one of these queries should match it and the other shouldn't

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test"

    resp = await app_client.post("/collections", json=test_collection)
    assert resp.status_code == 201

    resp = await app_client.get(f"/collections/{test_collection['id']}/aggregations")
    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 12

    resp = await app_client.delete(f"/collections/{test_collection['id']}")
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_post_collection_aggregations(app_client, load_test_data):
    # there's one item that can match, so one of these queries should match it and the other shouldn't

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test"

    resp = await app_client.post("/collections", json=test_collection)
    assert resp.status_code == 201

    resp = await app_client.post(f"/collections/{test_collection['id']}/aggregations")
    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 12

    resp = await app_client.delete(f"/collections/{test_collection['id']}")
    assert resp.status_code == 204


# @pytest.mark.asyncio
# async def test_aggregate_filter_extension_gte_get(app_client, ctx):
#     # there's one item that can match, so one of these queries should match it and the other shouldn't
#     resp = await app_client.get(
#         '/aggregate?aggregations=grid_geohex_frequency,total_count&grid_geohex_frequency_precision=2&filter={"op":"<=","args":[{"property": "properties.proj:epsg"},32756]}'
#     )

#     assert resp.status_code == 200
#     assert resp.json()["aggregations"][0]["value"] == 1

#     resp = await app_client.get(
#         '/aggregate?aggregations=grid_geohex_frequency,total_count&grid_geohex_frequency_precision=2&filter={"op":">","args":[{"property": "properties.proj:epsg"},32756]}'
#     )

#     assert resp.status_code == 200
#     assert resp.json()["aggregations"][0]["value"] == 0
