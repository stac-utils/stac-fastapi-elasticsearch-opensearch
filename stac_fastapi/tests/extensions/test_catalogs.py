import uuid

import pytest


@pytest.mark.asyncio
async def test_get_root_catalog(catalogs_app_client, load_test_data):
    """Test getting the catalogs list."""
    resp = await catalogs_app_client.get("/catalogs")
    assert resp.status_code == 200

    catalogs_response = resp.json()
    assert "catalogs" in catalogs_response
    assert "links" in catalogs_response
    assert "numberReturned" in catalogs_response

    # Check for required pagination links
    links = catalogs_response["links"]
    link_rels = [link["rel"] for link in links]
    assert "self" in link_rels
    assert "root" in link_rels
    assert "parent" in link_rels


@pytest.mark.asyncio
async def test_create_catalog(catalogs_app_client, load_test_data):
    """Test creating a new catalog."""
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert resp.status_code == 201

    created_catalog = resp.json()
    assert created_catalog["id"] == test_catalog["id"]
    assert created_catalog["type"] == "Catalog"
    assert created_catalog["title"] == test_catalog["title"]


@pytest.mark.asyncio
async def test_get_catalog(catalogs_app_client, load_test_data):
    """Test getting a specific catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Now get it back
    resp = await catalogs_app_client.get(f"/catalogs/{test_catalog['id']}")
    assert resp.status_code == 200

    catalog = resp.json()
    assert catalog["id"] == test_catalog["id"]
    assert catalog["title"] == test_catalog["title"]
    assert catalog["description"] == test_catalog["description"]


@pytest.mark.asyncio
async def test_get_nonexistent_catalog(catalogs_app_client):
    """Test getting a catalog that doesn't exist."""
    resp = await catalogs_app_client.get("/catalogs/nonexistent-catalog")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_collections(catalogs_app_client, load_test_data, ctx):
    """Test getting collections linked from a catalog."""
    # First create a catalog with a link to the test collection
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    # Update the catalog links to point to the actual test collection
    for link in test_catalog["links"]:
        if link["rel"] == "child":
            link["href"] = f"http://test-server/collections/{ctx.collection['id']}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Now get collections from the catalog
    resp = await catalogs_app_client.get(f"/catalogs/{test_catalog['id']}/collections")
    assert resp.status_code == 200

    collections_response = resp.json()
    assert "collections" in collections_response
    assert "links" in collections_response

    # Should contain the test collection
    collection_ids = [col["id"] for col in collections_response["collections"]]
    assert ctx.collection["id"] in collection_ids


@pytest.mark.asyncio
async def test_get_catalog_collections_nonexistent_catalog(catalogs_app_client):
    """Test getting collections from a catalog that doesn't exist."""
    resp = await catalogs_app_client.get("/catalogs/nonexistent-catalog/collections")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_root_catalog_with_multiple_catalogs(catalogs_app_client, load_test_data):
    """Test that catalogs response includes multiple catalog objects."""
    # Create multiple catalogs
    catalog_ids = []
    for i in range(3):
        test_catalog = load_test_data("test_catalog.json")
        test_catalog["id"] = f"test-catalog-{uuid.uuid4()}-{i}"
        test_catalog["title"] = f"Test Catalog {i}"

        resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
        assert resp.status_code == 201
        catalog_ids.append(test_catalog["id"])

    # Get catalogs list
    resp = await catalogs_app_client.get("/catalogs")
    assert resp.status_code == 200

    catalogs_response = resp.json()
    returned_catalogs = catalogs_response["catalogs"]
    returned_ids = [catalog["id"] for catalog in returned_catalogs]

    # Should have all created catalogs in the response
    for catalog_id in catalog_ids:
        assert catalog_id in returned_ids


@pytest.mark.asyncio
async def test_get_catalog_collection(catalogs_app_client, load_test_data, ctx):
    """Test getting a specific collection from a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Get a specific collection through the catalog route
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections/{ctx.collection['id']}"
    )
    assert resp.status_code == 200

    collection = resp.json()
    assert collection["id"] == ctx.collection["id"]
    assert collection["type"] == "Collection"
    assert "links" in collection


@pytest.mark.asyncio
async def test_get_catalog_collection_nonexistent_catalog(catalogs_app_client, ctx):
    """Test getting a collection from a catalog that doesn't exist."""
    resp = await catalogs_app_client.get(
        f"/catalogs/nonexistent-catalog/collections/{ctx.collection['id']}"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_collection_nonexistent_collection(
    catalogs_app_client, load_test_data
):
    """Test getting a collection that doesn't exist from a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Try to get a nonexistent collection
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections/nonexistent-collection"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_collection_items(catalogs_app_client, load_test_data, ctx):
    """Test getting items from a collection in a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Get items from a collection through the catalog route
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections/{ctx.collection['id']}/items"
    )
    assert resp.status_code == 200

    items_response = resp.json()
    assert items_response["type"] == "FeatureCollection"
    assert "features" in items_response
    assert "links" in items_response
    # Should contain the test item
    assert len(items_response["features"]) > 0


@pytest.mark.asyncio
async def test_get_catalog_collection_items_nonexistent_catalog(
    catalogs_app_client, ctx
):
    """Test getting items from a collection in a catalog that doesn't exist."""
    resp = await catalogs_app_client.get(
        f"/catalogs/nonexistent-catalog/collections/{ctx.collection['id']}/items"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_collection_items_nonexistent_collection(
    catalogs_app_client, load_test_data
):
    """Test getting items from a collection that doesn't exist in a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Try to get items from a nonexistent collection
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections/nonexistent-collection/items"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_collection_item(catalogs_app_client, load_test_data, ctx):
    """Test getting a specific item from a collection in a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Get a specific item through the catalog route
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
    )
    assert resp.status_code == 200

    item = resp.json()
    assert item["id"] == ctx.item["id"]
    assert item["type"] == "Feature"
    assert "properties" in item
    assert "geometry" in item


@pytest.mark.asyncio
async def test_get_catalog_collection_item_nonexistent_catalog(
    catalogs_app_client, ctx
):
    """Test getting an item from a collection in a catalog that doesn't exist."""
    resp = await catalogs_app_client.get(
        f"/catalogs/nonexistent-catalog/collections/{ctx.collection['id']}/items/{ctx.item['id']}"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_collection_item_nonexistent_collection(
    catalogs_app_client, load_test_data, ctx
):
    """Test getting an item from a collection that doesn't exist in a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Try to get an item from a nonexistent collection
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections/nonexistent-collection/items/{ctx.item['id']}"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_collection_item_nonexistent_item(
    catalogs_app_client, load_test_data, ctx
):
    """Test getting an item that doesn't exist from a collection in a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Try to get a nonexistent item
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections/{ctx.collection['id']}/items/nonexistent-item"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_catalogs_pagination_limit(catalogs_app_client, load_test_data):
    """Test that pagination limit parameter works for catalogs endpoint."""
    # Create multiple catalogs
    catalog_ids = []
    for i in range(5):
        test_catalog = load_test_data("test_catalog.json")
        test_catalog["id"] = f"test-catalog-{uuid.uuid4()}-{i}"
        test_catalog["title"] = f"Test Catalog {i}"

        resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
        assert resp.status_code == 201
        catalog_ids.append(test_catalog["id"])

    # Test with limit=2
    resp = await catalogs_app_client.get("/catalogs?limit=2")
    assert resp.status_code == 200

    catalogs_response = resp.json()
    returned_catalogs = catalogs_response["catalogs"]

    # Should only return 2 catalogs
    assert len(returned_catalogs) == 2
    assert catalogs_response["numberReturned"] == 2


@pytest.mark.asyncio
async def test_catalogs_pagination_default_limit(catalogs_app_client, load_test_data):
    """Test that pagination uses default limit when no limit parameter is provided."""
    # Create multiple catalogs
    catalog_ids = []
    for i in range(15):
        test_catalog = load_test_data("test_catalog.json")
        test_catalog["id"] = f"test-catalog-{uuid.uuid4()}-{i}"
        test_catalog["title"] = f"Test Catalog {i}"

        resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
        assert resp.status_code == 201
        catalog_ids.append(test_catalog["id"])

    # Test without limit parameter (should default to 10)
    resp = await catalogs_app_client.get("/catalogs")
    assert resp.status_code == 200

    catalogs_response = resp.json()
    returned_catalogs = catalogs_response["catalogs"]

    # Should return default limit of 10 catalogs
    assert len(returned_catalogs) == 10
    assert catalogs_response["numberReturned"] == 10


@pytest.mark.asyncio
async def test_catalogs_pagination_limit_validation(catalogs_app_client):
    """Test that pagination limit parameter validation works."""
    # Test with limit=0 (should be invalid)
    resp = await catalogs_app_client.get("/catalogs?limit=0")
    assert resp.status_code == 400  # Validation error returns 400 for Query parameters


@pytest.mark.asyncio
async def test_catalogs_pagination_token_parameter(catalogs_app_client, load_test_data):
    """Test that pagination token parameter is accepted (even if token is invalid)."""
    # Create a catalog first
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert resp.status_code == 201

    # Test with token parameter (even if invalid, should be accepted)
    resp = await catalogs_app_client.get("/catalogs?token=invalid_token")
    assert resp.status_code == 200

    catalogs_response = resp.json()
    assert "catalogs" in catalogs_response
    assert "links" in catalogs_response
