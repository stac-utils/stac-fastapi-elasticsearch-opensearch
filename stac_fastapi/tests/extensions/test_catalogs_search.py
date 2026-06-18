"""Tests for Catalogs Search Extension functionality."""

import uuid

import pytest


@pytest.mark.asyncio
async def test_catalog_search_get_empty_catalog(catalogs_app_client, load_test_data):
    """Test GET search on a catalog with no collections returns empty results."""
    # Create a catalog with no collections
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"empty-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Search within the empty catalog
    search_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}/search")
    assert search_resp.status_code == 200

    search_result = search_resp.json()
    assert search_result["type"] == "FeatureCollection"
    assert search_result["features"] == []


@pytest.mark.asyncio
async def test_catalog_search_post_empty_catalog(catalogs_app_client, load_test_data):
    """Test POST search on a catalog with no collections returns empty results."""
    # Create a catalog with no collections
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"empty-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Search within the empty catalog
    search_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/search", json={}
    )
    assert search_resp.status_code == 200

    search_result = search_resp.json()
    assert search_result["type"] == "FeatureCollection"
    assert search_result["features"] == []


@pytest.mark.asyncio
async def test_catalog_search_scopes_to_descendant_collections(
    catalogs_app_client, load_test_data
):
    """Test that catalog search only returns items from descendant collections."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create collection under parent
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Create an item in the collection (use global collections endpoint)
    test_item = load_test_data("test_item.json")
    item_id = f"test-item-{uuid.uuid4()}"
    test_item["id"] = item_id
    test_item["collection"] = collection_id

    item_resp = await catalogs_app_client.post(
        f"/collections/{collection_id}/items",
        json=test_item,
    )
    assert item_resp.status_code == 201

    # Search within the catalog
    search_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/search")
    assert search_resp.status_code == 200

    search_result = search_resp.json()
    assert len(search_result["features"]) == 1
    assert search_result["features"][0]["id"] == item_id


@pytest.mark.asyncio
async def test_catalog_search_rejects_out_of_scope_collections(
    catalogs_app_client, load_test_data
):
    """Test that searching for out-of-scope collections returns 403."""
    # Create two separate catalogs
    catalog1 = load_test_data("test_catalog.json")
    catalog1_id = f"catalog-1-{uuid.uuid4()}"
    catalog1["id"] = catalog1_id

    resp1 = await catalogs_app_client.post("/catalogs", json=catalog1)
    assert resp1.status_code == 201

    catalog2 = load_test_data("test_catalog.json")
    catalog2_id = f"catalog-2-{uuid.uuid4()}"
    catalog2["id"] = catalog2_id

    resp2 = await catalogs_app_client.post("/catalogs", json=catalog2)
    assert resp2.status_code == 201

    # Create collection under catalog1
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog1_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Try to search for catalog1's collection from catalog2
    search_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog2_id}/search",
        json={"collections": [collection_id]},
    )
    assert search_resp.status_code == 403

    error = search_resp.json()
    assert "outside the scope" in error.get("detail", "")


@pytest.mark.asyncio
async def test_catalog_search_with_multi_level_hierarchy(
    catalogs_app_client, load_test_data
):
    """Test search across multi-level catalog hierarchy."""
    # Create root catalog
    root_catalog = load_test_data("test_catalog.json")
    root_id = f"root-catalog-{uuid.uuid4()}"
    root_catalog["id"] = root_id

    root_resp = await catalogs_app_client.post("/catalogs", json=root_catalog)
    assert root_resp.status_code == 201

    # Create sub-catalog under root
    sub_catalog = load_test_data("test_catalog.json")
    sub_id = f"sub-catalog-{uuid.uuid4()}"
    sub_catalog["id"] = sub_id

    sub_resp = await catalogs_app_client.post(
        f"/catalogs/{root_id}/catalogs", json=sub_catalog
    )
    assert sub_resp.status_code == 201

    # Create collection under sub-catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{sub_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Create item in collection (use global collections endpoint)
    test_item = load_test_data("test_item.json")
    item_id = f"test-item-{uuid.uuid4()}"
    test_item["id"] = item_id
    test_item["collection"] = collection_id

    item_resp = await catalogs_app_client.post(
        f"/collections/{collection_id}/items",
        json=test_item,
    )
    assert item_resp.status_code == 201

    # Search from root should find item in nested collection
    search_resp = await catalogs_app_client.get(f"/catalogs/{root_id}/search")
    assert search_resp.status_code == 200

    search_result = search_resp.json()
    assert len(search_result["features"]) == 1
    assert search_result["features"][0]["id"] == item_id


@pytest.mark.asyncio
async def test_catalog_search_get_with_limit(catalogs_app_client, load_test_data):
    """Test GET search with limit parameter."""
    # Create catalog with collection
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create collection
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Create multiple items (use global collections endpoint)
    for i in range(5):
        test_item = load_test_data("test_item.json")
        test_item["id"] = f"test-item-{i}-{uuid.uuid4()}"
        test_item["collection"] = collection_id

        item_resp = await catalogs_app_client.post(
            f"/collections/{collection_id}/items",
            json=test_item,
        )
        assert item_resp.status_code == 201

    # Search with limit
    search_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/search?limit=2")
    assert search_resp.status_code == 200

    search_result = search_resp.json()
    assert len(search_result["features"]) <= 2


@pytest.mark.asyncio
async def test_catalog_search_post_with_limit(catalogs_app_client, load_test_data):
    """Test POST search with limit parameter."""
    # Create catalog with collection
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create collection
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Create multiple items (use global collections endpoint)
    for i in range(5):
        test_item = load_test_data("test_item.json")
        test_item["id"] = f"test-item-{i}-{uuid.uuid4()}"
        test_item["collection"] = collection_id

        item_resp = await catalogs_app_client.post(
            f"/collections/{collection_id}/items",
            json=test_item,
        )
        assert item_resp.status_code == 201

    # Search with limit
    search_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/search", json={"limit": 2}
    )
    assert search_resp.status_code == 200

    search_result = search_resp.json()
    assert len(search_result["features"]) <= 2


@pytest.mark.asyncio
async def test_catalog_search_nonexistent_catalog(catalogs_app_client):
    """Test search on nonexistent catalog returns 404."""
    search_resp = await catalogs_app_client.get("/catalogs/nonexistent-catalog/search")
    assert search_resp.status_code == 404


@pytest.mark.asyncio
async def test_catalog_search_post_nonexistent_catalog(catalogs_app_client):
    """Test POST search on nonexistent catalog returns 404."""
    search_resp = await catalogs_app_client.post(
        "/catalogs/nonexistent-catalog/search", json={}
    )
    assert search_resp.status_code == 404
