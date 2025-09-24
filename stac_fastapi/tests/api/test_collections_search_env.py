"""Test the ENABLE_COLLECTIONS_SEARCH environment variable."""

import os
import uuid
from unittest import mock

import pytest

from ..conftest import create_collection, refresh_indices


@pytest.mark.asyncio
@mock.patch.dict(os.environ, {"ENABLE_COLLECTIONS_SEARCH": "false"})
async def test_collections_search_disabled(app_client, txn_client, load_test_data):
    """Test that collection search extensions are disabled when ENABLE_COLLECTIONS_SEARCH=false."""
    # Create multiple collections with different ids to test sorting
    base_collection = load_test_data("test_collection.json")

    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"disabled-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-c", f"{test_prefix}-a", f"{test_prefix}-b"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    # Refresh indices to ensure collections are searchable
    await refresh_indices(txn_client)

    # When collection search is disabled, sortby parameter should be ignored
    resp = await app_client.get(
        "/collections",
        params=[("sortby", "+id")],
    )
    assert resp.status_code == 200

    # Verify that results are NOT sorted by id (should be in insertion order or default order)
    resp_json = resp.json()
    collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Extract the ids in the order they were returned
    returned_ids = [c["id"] for c in collections]

    # If sorting was working, they would be in alphabetical order: a, b, c
    # But since sorting is disabled, they should be in a different order
    # We can't guarantee the exact order, but we can check they're not in alphabetical order
    sorted_ids = sorted(returned_ids)
    assert (
        returned_ids != sorted_ids or len(collections) < 2
    ), "Collections appear to be sorted despite ENABLE_COLLECTIONS_SEARCH=false"

    # Fields parameter should also be ignored
    resp = await app_client.get(
        "/collections",
        params=[("fields", "id")],  # Request only id field
    )
    assert resp.status_code == 200

    # Verify that all fields are still returned, not just id
    resp_json = resp.json()
    for collection in resp_json["collections"]:
        if collection["id"].startswith(test_prefix):
            # If fields filtering was working, only id would be present
            # Since it's disabled, other fields like title should still be present
            assert (
                "title" in collection
            ), "Fields filtering appears to be working despite ENABLE_COLLECTIONS_SEARCH=false"


@pytest.mark.asyncio
@mock.patch.dict(os.environ, {"ENABLE_COLLECTIONS_SEARCH": "true"})
async def test_collections_search_enabled(app_client, txn_client, load_test_data):
    """Test that collection search extensions work when ENABLE_COLLECTIONS_SEARCH=true."""
    # Create multiple collections with different ids to test sorting
    base_collection = load_test_data("test_collection.json")

    # Use unique prefixes to avoid conflicts between tests
    test_prefix = f"enabled-{uuid.uuid4().hex[:8]}"
    collection_ids = [f"{test_prefix}-c", f"{test_prefix}-a", f"{test_prefix}-b"]

    for i, coll_id in enumerate(collection_ids):
        test_collection = base_collection.copy()
        test_collection["id"] = coll_id
        test_collection["title"] = f"Test Collection {i}"
        await create_collection(txn_client, test_collection)

    # Refresh indices to ensure collections are searchable
    await refresh_indices(txn_client)

    # Test that sortby parameter works - sort by id ascending
    resp = await app_client.get(
        "/collections",
        params=[("sortby", "+id")],
    )
    assert resp.status_code == 200

    # Verify that results are sorted by id in ascending order
    resp_json = resp.json()
    collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Extract the ids in the order they were returned
    returned_ids = [c["id"] for c in collections]

    # Verify they're in ascending order
    assert returned_ids == sorted(
        returned_ids
    ), "Collections are not sorted by id ascending"

    # Test that sortby parameter works - sort by id descending
    resp = await app_client.get(
        "/collections",
        params=[("sortby", "-id")],
    )
    assert resp.status_code == 200

    # Verify that results are sorted by id in descending order
    resp_json = resp.json()
    collections = [
        c for c in resp_json["collections"] if c["id"].startswith(test_prefix)
    ]

    # Extract the ids in the order they were returned
    returned_ids = [c["id"] for c in collections]

    # Verify they're in descending order
    assert returned_ids == sorted(
        returned_ids, reverse=True
    ), "Collections are not sorted by id descending"

    # Test that fields parameter works - request only id field
    resp = await app_client.get(
        "/collections",
        params=[("fields", "id")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # When fields=id is specified, collections should only have id field
    for collection in resp_json["collections"]:
        if collection["id"].startswith(test_prefix):
            assert "id" in collection, "id field is missing"
            assert (
                "title" not in collection
            ), "title field should be excluded when fields=id"

    # Test that fields parameter works - request multiple fields
    resp = await app_client.get(
        "/collections",
        params=[("fields", "id,title")],
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    # When fields=id,title is specified, collections should have both fields but not others
    for collection in resp_json["collections"]:
        if collection["id"].startswith(test_prefix):
            assert "id" in collection, "id field is missing"
            assert "title" in collection, "title field is missing"
            assert (
                "description" not in collection
            ), "description field should be excluded when fields=id,title"
