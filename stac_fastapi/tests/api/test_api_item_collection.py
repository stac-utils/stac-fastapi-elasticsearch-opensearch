import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timedelta

import pytest

from ..conftest import create_collection, create_item


@pytest.mark.asyncio
async def test_global_item_max_limit_set(app_client, txn_client, load_test_data):
    """Test with global max limit set for items, expect cap the ?limit parameter"""
    os.environ["STAC_GLOBAL_ITEM_MAX_LIMIT"] = "5"

    test_collection = load_test_data("test_collection.json")
    test_collection_id = "test-collection-for-items"
    test_collection["id"] = test_collection_id
    await create_collection(txn_client, test_collection)

    item = load_test_data("test_item.json")
    item["collection"] = test_collection_id

    for i in range(10):
        test_item = item.copy()
        test_item["id"] = f"test-item-{i}"
        await create_item(txn_client, test_item)

    resp = await app_client.get(f"/collections/{test_collection_id}/items?limit=10")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 5

    resp = await app_client.get(f"/search?collections={test_collection_id}&limit=10")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 5

    del os.environ["STAC_GLOBAL_ITEM_MAX_LIMIT"]


@pytest.mark.asyncio
async def test_default_item_limit_without_limit_parameter_set(
    app_client, txn_client, load_test_data
):
    """Test default item limit set, should use default when no limit provided"""
    os.environ["STAC_DEFAULT_ITEM_LIMIT"] = "10"

    test_collection = load_test_data("test_collection.json")
    test_collection_id = "test-collection-items"
    test_collection["id"] = test_collection_id
    await create_collection(txn_client, test_collection)

    item = load_test_data("test_item.json")
    item["collection"] = test_collection_id

    for i in range(15):
        test_item = item.copy()
        test_item["id"] = f"test-item-{i}"
        await create_item(txn_client, test_item)

    resp = await app_client.get(f"/collections/{test_collection_id}/items")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10

    resp = await app_client.get(f"/search?collections={test_collection_id}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10

    # Also test POST search to compare
    search_body = {"collections": [test_collection_id]}
    resp = await app_client.post("/search", json=search_body)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10

    del os.environ["STAC_DEFAULT_ITEM_LIMIT"]


@pytest.mark.asyncio
async def test_item_collection_sort_desc(app_client, txn_client, ctx):
    """Verify GET /collections/{collectionId}/items honors descending sort on properties.datetime."""
    first_item = ctx.item

    # Create a second item in the same collection with an earlier datetime
    second_item = dict(first_item)
    second_item["id"] = "another-item-for-collection-sort-desc"
    another_item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    ) - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    await create_item(txn_client, second_item)

    # Descending sort: the original (newer) item should come first
    resp = await app_client.get(
        f"/collections/{first_item['collection']}/items",
        params=[("sortby", "-properties.datetime")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


@pytest.mark.asyncio
async def test_item_collection_sort_asc(app_client, txn_client, ctx):
    """Verify GET /collections/{collectionId}/items honors ascending sort on properties.datetime."""
    first_item = ctx.item

    # Create a second item in the same collection with an earlier datetime
    second_item = dict(first_item)
    second_item["id"] = "another-item-for-collection-sort-asc"
    another_item_date = datetime.strptime(
        first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    ) - timedelta(days=1)
    second_item["properties"]["datetime"] = another_item_date.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    await create_item(txn_client, second_item)

    # Ascending sort: the older item should come first
    resp = await app_client.get(
        f"/collections/{first_item['collection']}/items",
        params=[("sortby", "+properties.datetime")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == second_item["id"]
    assert resp_json["features"][1]["id"] == first_item["id"]

    # Also verify bare field (no +) sorts ascending by default
    resp = await app_client.get(
        f"/collections/{first_item['collection']}/items",
        params=[("sortby", "properties.datetime")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == second_item["id"]
    assert resp_json["features"][1]["id"] == first_item["id"]


@pytest.mark.asyncio
async def test_item_collection_query(app_client, txn_client, ctx):
    """Simple query parameter test on the Item Collection route.

    Creates an item with a unique property and ensures it can be retrieved
    using the 'query' parameter on GET /collections/{collection_id}/items.
    """
    unique_val = str(uuid.uuid4())
    test_item = deepcopy(ctx.item)
    test_item["id"] = f"query-basic-{unique_val}"
    # Add a property to filter on
    test_item.setdefault("properties", {})["test_query_key"] = unique_val

    await create_item(txn_client, test_item)

    # Provide the query parameter as a JSON string without adding new imports
    query_param = f'{{"test_query_key": {{"eq": "{unique_val}"}}}}'

    resp = await app_client.get(
        f"/collections/{test_item['collection']}/items",
        params=[("query", query_param)],
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    ids = [f["id"] for f in resp_json["features"]]
    assert test_item["id"] in ids


@pytest.mark.asyncio
async def test_item_collection_filter_by_id(app_client, ctx):
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
async def test_item_collection_filter_by_nonexistent_id(app_client, ctx, txn_client):
    """Test filtering with a non-existent ID returns no results."""
    # Get the test collection and item from context
    collection_id = ctx.collection["id"]
    item_id = ctx.item["id"]

    # First, verify the item exists
    resp = await app_client.get(f"/collections/{collection_id}/items/{item_id}")
    assert resp.status_code == 200

    # Create a non-existent ID
    non_existent_id = f"non-existent-{str(uuid.uuid4())}"

    # Create a filter with the non-existent ID using CQL2-JSON syntax
    filter_body = {"op": "=", "args": [{"property": "id"}, non_existent_id]}

    # URL-encode the filter JSON
    import urllib.parse

    encoded_filter = urllib.parse.quote(json.dumps(filter_body))

    # Make the request with URL-encoded filter in the query string
    url = f"/collections/{collection_id}/items?filter-lang=cql2-json&filter={encoded_filter}"
    resp = await app_client.get(url)

    # Verify the response
    assert resp.status_code == 200
    resp_json = resp.json()
    assert (
        len(resp_json["features"]) == 0
    ), f"Expected no items with ID {non_existent_id}, but found {len(resp_json['features'])} matches"


@pytest.mark.asyncio
async def test_item_collection_fields_extension(app_client, ctx, txn_client):
    resp = await app_client.get(
        "/collections/test-collection/items",
        params={"fields": "+properties.datetime"},
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert list(resp_json["features"][0]["properties"]) == ["datetime"]


@pytest.mark.asyncio
async def test_item_collection_fields_extension_no_properties_get(
    app_client, ctx, txn_client
):
    resp = await app_client.get(
        "/collections/test-collection/items", params={"fields": "-properties"}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert "properties" not in resp_json["features"][0]


@pytest.mark.asyncio
async def test_item_collection_fields_extension_no_null_fields(
    app_client, ctx, txn_client
):
    resp = await app_client.get("/collections/test-collection/items")
    assert resp.status_code == 200
    resp_json = resp.json()
    # check if no null fields: https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/166
    for feature in resp_json["features"]:
        # assert "bbox" not in feature["geometry"]
        for link in feature["links"]:
            assert all(a not in link or link[a] is not None for a in ("title", "asset"))
        for asset in feature["assets"]:
            assert all(
                a not in asset or asset[a] is not None
                for a in ("start_datetime", "created")
            )


@pytest.mark.asyncio
async def test_item_collection_fields_extension_return_all_properties(
    app_client, ctx, txn_client, load_test_data
):
    item = load_test_data("test_item.json")
    resp = await app_client.get(
        "/collections/test-collection/items",
        params={"collections": ["test-collection"], "fields": "properties"},
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    feature = resp_json["features"][0]
    assert len(feature["properties"]) >= len(item["properties"])
    for expected_prop, expected_value in item["properties"].items():
        if expected_prop in (
            "datetime",
            "start_datetime",
            "end_datetime",
            "created",
            "updated",
        ):
            assert feature["properties"][expected_prop][0:19] == expected_value[0:19]
        else:
            assert feature["properties"][expected_prop] == expected_value
