import uuid

import pytest

from ..conftest import create_collection


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
