import json
import uuid

import pytest

from ..conftest import create_collection, refresh_indices


@pytest.mark.asyncio
async def test_collections_sort_id_asc(app_client, txn_client, load_test_data):
    """Verify GET /collections honors ascending sort on id."""
    # Create multiple collections with different ids
    base_collection = load_test_data("test_collection.json")

    # Create collections with ids in a specific order to test sorting
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"asc-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-c", f"{test_prefix}-a", f"{test_prefix}-b"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    # Test ascending sort by id
    resp = await app_client.get(
        "/collections",
        params=[("sortby", "+id")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones we created for this test
    test_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Collections should be sorted alphabetically by id
    sorted_ids = sorted(collection_ids)
    assert len(test_collections) == len(collection_ids)
    for i, expected_id in enumerate(sorted_ids):
        assert test_collections[i]["id"] == expected_id


@pytest.mark.asyncio
async def test_collections_sort_id_desc(app_client, txn_client, load_test_data):
    """Verify GET /collections honors descending sort on id."""
    # Create multiple collections with different ids
    base_collection = load_test_data("test_collection.json")

    # Create collections with ids in a specific order to test sorting
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"desc-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-c", f"{test_prefix}-a", f"{test_prefix}-b"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    # Test descending sort by id
    resp = await app_client.get(
        "/collections",
        params=[("sortby", "-id")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones we created for this test
    test_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Collections should be sorted in reverse alphabetical order by id
    sorted_ids = sorted(collection_ids, reverse=True)
    assert len(test_collections) == len(collection_ids)
    for i, expected_id in enumerate(sorted_ids):
        assert test_collections[i]["id"] == expected_id


@pytest.mark.asyncio
async def test_collections_fields(app_client, txn_client, load_test_data):
    """Verify GET /collections honors the fields parameter."""
    # Create multiple collections with different ids
    base_collection = load_test_data("test_collection.json")

    # Create collections with ids in a specific order to test fields
    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"fields-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-a", f"{test_prefix}-b", f"{test_prefix}-c"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        test_collection["description"] = f"Description for collection {i}"
        await create_collection(txn_client, test_collection)

    # Test include fields parameter
    resp = await app_client.get(
        "/collections",
        params=[("fields", "id"), ("fields", "title")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Check if collections exist in the response
    assert "collections" in resp_json, "No collections in response"

    # Filter collections to only include the ones we created for this test
    test_collections = []
    for c in resp_json["collections"]:
        if "id" in c and c["id"].startswith(test_prefix):
            test_collections.append(c)

    # Filter collections to only include the ones we created for this test
    test_collections = []
    for c in resp_json["collections"]:
        if "id" in c and c["id"].startswith(test_prefix):
            test_collections.append(c)

    # Collections should only have id and title fields
    for collection in test_collections:
        assert "id" in collection
        assert "title" in collection
        assert "description" not in collection
        assert "links" in collection  # links are always included

    # Test exclude fields parameter
    resp = await app_client.get(
        "/collections",
        params=[("fields", "-description")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Check if collections exist in the response
    assert (
        "collections" in resp_json
    ), "No collections in response for exclude fields test"

    # Filter collections to only include the ones we created for this test
    test_collections = []
    for c in resp_json["collections"]:
        if "id" in c and c["id"].startswith(test_prefix):
            test_collections.append(c)

    # Collections should have all fields except description
    for collection in test_collections:
        assert "id" in collection
        assert "title" in collection
        assert "description" not in collection
        assert "links" in collection


@pytest.mark.asyncio
async def test_collections_free_text_search_get(app_client, txn_client, load_test_data):
    """Verify GET /collections honors the q parameter for free text search."""
    # Create multiple collections with different content
    base_collection = load_test_data("test_collection.json")

    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"q-get-{uuid.uuid4().hex[:8]}"

    test_collections = [
        {
            "id": f"{test_prefix}-sentinel",
            "title": "Sentinel-2 Collection",
            "description": "Collection of Sentinel-2 data",
            "summaries": {"platform": ["sentinel-2a", "sentinel-2b"]},
        },
        {
            "id": f"{test_prefix}-landsat",
            "title": "Landsat Collection",
            "description": "Collection of Landsat data",
            "summaries": {"platform": ["landsat-8", "landsat-9"]},
        },
        {
            "id": f"{test_prefix}-modis",
            "title": "MODIS Collection",
            "description": "Collection of MODIS data",
            "summaries": {"platform": ["terra", "aqua"]},
        },
    ]

    for i, coll in enumerate(test_collections):
        test_collection = base_collection.copy()
        test_collection["id"] = coll["id"]
        test_collection["title"] = coll["title"]
        test_collection["description"] = coll["description"]
        test_collection["summaries"] = coll["summaries"]
        await create_collection(txn_client, test_collection)

    # Test free text search for "sentinel"
    resp = await app_client.get(
        "/collections",
        params=[("q", "sentinel")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones we created for this test
    found_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Should only find the sentinel collection
    assert len(found_collections) == 1
    assert found_collections[0]["id"] == f"{test_prefix}-sentinel"

    # Test free text search for "landsat"
    resp = await app_client.get(
        "/collections",
        params=[("q", "modis")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Filter collections to only include the ones we created for this test
    found_collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Should only find the landsat collection
    assert len(found_collections) == 1
    assert found_collections[0]["id"] == f"{test_prefix}-modis"


@pytest.mark.asyncio
async def test_collections_filter_search(app_client, txn_client, load_test_data):
    """Verify GET /collections honors the filter parameter for structured search."""
    # Create multiple collections with different content
    base_collection = load_test_data("test_collection.json")

    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"filter-{uuid.uuid4().hex[:8]}"

    # Create collections with different content to test structured filter
    test_collections = [
        {
            "id": f"{test_prefix}-sentinel",
            "title": "Sentinel-2 Collection",
            "description": "Collection of Sentinel-2 data",
            "summaries": {"platform": ["sentinel-2a", "sentinel-2b"]},
        },
        {
            "id": f"{test_prefix}-landsat",
            "title": "Landsat Collection",
            "description": "Collection of Landsat data",
            "summaries": {"platform": ["landsat-8", "landsat-9"]},
        },
        {
            "id": f"{test_prefix}-modis",
            "title": "MODIS Collection",
            "description": "Collection of MODIS data",
            "summaries": {"platform": ["terra", "aqua"]},
        },
    ]

    for i, coll in enumerate(test_collections):
        test_collection = base_collection.copy()
        test_collection["id"] = coll["id"]
        test_collection["title"] = coll["title"]
        test_collection["description"] = coll["description"]
        test_collection["summaries"] = coll["summaries"]
        await create_collection(txn_client, test_collection)

    await refresh_indices(txn_client)

    # Use the ID of the first test collection for the filter
    test_collection_id = test_collections[0]["id"]

    # Create a simple filter for exact ID match using CQL2-JSON
    filter_expr = {"op": "=", "args": [{"property": "id"}, test_collection_id]}

    # Convert to JSON string for URL parameter
    filter_json = json.dumps(filter_expr)

    # Use CQL2-JSON format with explicit filter-lang
    resp = await app_client.get(
        f"/collections?filter={filter_json}&filter-lang=cql2-json",
    )

    assert resp.status_code == 200
    resp_json = resp.json()

    # Should find exactly one collection with the specified ID
    found_collections = [
        c for c in resp_json["collections"] if c["id"] == test_collection_id
    ]

    assert (
        len(found_collections) == 1
    ), f"Expected 1 collection with ID {test_collection_id}, found {len(found_collections)}"
    assert found_collections[0]["id"] == test_collection_id

    # Test 2: CQL2-text format with LIKE operator for more advanced filtering
    # Use a filter that will match the test collection ID we created
    filter_text = f"id LIKE '%{test_collection_id.split('-')[-1]}%'"

    resp = await app_client.get(
        f"/collections?filter={filter_text}&filter-lang=cql2-text",
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # Should find the test collection we created
    found_collections = [
        c for c in resp_json["collections"] if c["id"] == test_collection_id
    ]
    assert (
        len(found_collections) >= 1
    ), f"Expected at least 1 collection with ID {test_collection_id} using LIKE filter"
