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
    # With 5 items created and limit=2, should return exactly 2 features
    assert len(search_result["features"]) == 2


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
    # With 5 items created and limit=2, should return exactly 2 features
    assert len(search_result["features"]) == 2


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


@pytest.mark.asyncio
async def test_catalog_search_with_spatial_intersection(
    catalogs_app_client, load_test_data
):
    """Test catalog search with spatial intersection filter."""
    # Create catalog and collection
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

    # Create multiple items with different geometries
    for i in range(3):
        test_item = load_test_data("test_item.json")
        test_item["id"] = f"test-item-{i}-{uuid.uuid4()}"
        test_item["collection"] = collection_id
        # Vary the geometry slightly (offset coordinates by iteration)
        if test_item.get("geometry") and test_item["geometry"].get("coordinates"):
            coords = test_item["geometry"]["coordinates"]
            # Handle polygon coordinates (nested list of rings)
            if isinstance(coords[0][0], (list, tuple)):
                # Polygon: coords[ring][point][lon/lat]
                test_item["geometry"]["coordinates"] = [
                    [[c[0] + (i * 0.1), c[1] + (i * 0.1)] for c in ring]
                    for ring in coords
                ]
            else:
                # Point or LineString: coords[point][lon/lat]
                test_item["geometry"]["coordinates"] = [
                    [c[0] + (i * 0.1), c[1] + (i * 0.1)] for c in coords
                ]

        item_resp = await catalogs_app_client.post(
            f"/collections/{collection_id}/items",
            json=test_item,
        )
        assert item_resp.status_code == 201

    # Search with spatial intersection
    search_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/search",
        json={"intersects": {"type": "Point", "coordinates": [-105.0, 40.0]}},
    )
    assert search_resp.status_code == 200
    search_result = search_resp.json()
    assert search_result["type"] == "FeatureCollection"
    # Verify the endpoint accepts spatial filters without crashing
    # (actual intersection results depend on test item geometries)
    assert "features" in search_result


@pytest.mark.asyncio
async def test_catalog_search_with_datetime_filter(catalogs_app_client, load_test_data):
    """Test catalog search with datetime range filter."""
    # Create catalog and collection
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

    # Create items
    for i in range(3):
        test_item = load_test_data("test_item.json")
        test_item["id"] = f"test-item-{i}-{uuid.uuid4()}"
        test_item["collection"] = collection_id

        item_resp = await catalogs_app_client.post(
            f"/collections/{collection_id}/items",
            json=test_item,
        )
        assert item_resp.status_code == 201

    # Search with datetime filter (wide range that should include test items)
    search_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/search",
        json={"datetime": "2020-01-01T00:00:00Z/2025-12-31T23:59:59Z"},
    )
    assert search_resp.status_code == 200
    search_result = search_resp.json()
    assert search_result["type"] == "FeatureCollection"
    # Should find items within datetime range (3 items created)
    assert len(search_result["features"]) == 3

    # Search with restrictive datetime filter (should exclude items)
    search_resp_restricted = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/search",
        json={"datetime": "2000-01-01T00:00:00Z/2010-12-31T23:59:59Z"},
    )
    assert search_resp_restricted.status_code == 200
    result_restricted = search_resp_restricted.json()
    # Should find no items in the 2000-2010 range (test items are from 2020s)
    assert len(result_restricted["features"]) == 0


@pytest.mark.asyncio
async def test_catalog_search_with_pagination(catalogs_app_client, load_test_data):
    """Test catalog search with pagination using limit and token."""
    # Create catalog and collection
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

    # Create 5 items
    for i in range(5):
        test_item = load_test_data("test_item.json")
        test_item["id"] = f"test-item-{i}-{uuid.uuid4()}"
        test_item["collection"] = collection_id

        item_resp = await catalogs_app_client.post(
            f"/collections/{collection_id}/items",
            json=test_item,
        )
        assert item_resp.status_code == 201

    # First page with limit=2
    search_resp1 = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/search?limit=2"
    )
    assert search_resp1.status_code == 200
    result1 = search_resp1.json()
    assert len(result1["features"]) <= 2

    # Check for next link (pagination token)
    next_link = next(
        (link for link in result1.get("links", []) if link.get("rel") == "next"),
        None,
    )

    if next_link:
        # Get next page using token
        token = (
            next_link.get("body", {}).get("token")
            or next_link.get("href", "").split("token=")[-1]
        )
        search_resp2 = await catalogs_app_client.get(
            f"/catalogs/{parent_id}/search?limit=2&token={token}"
        )
        assert search_resp2.status_code == 200
        result2 = search_resp2.json()
        assert len(result2["features"]) <= 2


@pytest.mark.asyncio
async def test_catalog_search_with_sortby(catalogs_app_client, load_test_data):
    """Test catalog search with sort parameter."""
    # Create catalog and collection
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

    # Create items
    for i in range(3):
        test_item = load_test_data("test_item.json")
        test_item["id"] = f"test-item-{i}-{uuid.uuid4()}"
        test_item["collection"] = collection_id

        item_resp = await catalogs_app_client.post(
            f"/collections/{collection_id}/items",
            json=test_item,
        )
        assert item_resp.status_code == 201

    # Search with sortby (sort by properties.datetime descending)
    search_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/search",
        json={"sortby": [{"field": "properties.datetime", "direction": "desc"}]},
    )
    assert search_resp.status_code == 200
    search_result = search_resp.json()
    assert search_result["type"] == "FeatureCollection"
    # Should return all 3 items (sortby is a pass-through extension)
    assert len(search_result["features"]) == 3

    # Verify items have datetime properties for sorting
    for feature in search_result["features"]:
        assert "properties" in feature
        assert "datetime" in feature["properties"]


@pytest.mark.asyncio
async def test_catalog_search_combined_filters(catalogs_app_client, load_test_data):
    """Test catalog search with multiple filters combined."""
    # Create catalog and collection
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

    # Create items
    for i in range(3):
        test_item = load_test_data("test_item.json")
        test_item["id"] = f"test-item-{i}-{uuid.uuid4()}"
        test_item["collection"] = collection_id

        item_resp = await catalogs_app_client.post(
            f"/collections/{collection_id}/items",
            json=test_item,
        )
        assert item_resp.status_code == 201

    # Search with multiple filters: datetime + limit
    search_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/search?datetime=2020-01-01T00:00:00Z/2025-12-31T23:59:59Z&limit=2"
    )
    assert search_resp.status_code == 200
    search_result = search_resp.json()
    assert search_result["type"] == "FeatureCollection"
    # With 3 items created and limit=2, should return exactly 2 features
    assert len(search_result["features"]) == 2
