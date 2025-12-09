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
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Add the existing collection to the catalog
    add_resp = await catalogs_app_client.post(
        f"/catalogs/{test_catalog['id']}/collections", json=ctx.collection
    )
    assert add_resp.status_code == 201

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

    # Add the existing collection to the catalog
    add_resp = await catalogs_app_client.post(
        f"/catalogs/{test_catalog['id']}/collections", json=ctx.collection
    )
    assert add_resp.status_code == 201

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


@pytest.mark.asyncio
async def test_create_catalog_collection(catalogs_app_client, load_test_data, ctx):
    """Test creating a collection within a catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    # Remove placeholder collection links so we start with a clean catalog
    test_catalog["links"] = [
        link for link in test_catalog.get("links", []) if link.get("rel") != "child"
    ]

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201
    catalog_id = test_catalog["id"]

    # Create a new collection within the catalog
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-{uuid.uuid4()}"

    resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert resp.status_code == 201

    created_collection = resp.json()
    assert created_collection["id"] == test_collection["id"]
    assert created_collection["type"] == "Collection"

    # Verify the collection was created by getting it directly
    get_resp = await catalogs_app_client.get(f"/collections/{test_collection['id']}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == test_collection["id"]

    # Verify the collection has a catalog link to the catalog
    collection_data = get_resp.json()
    collection_links = collection_data.get("links", [])
    catalog_link = None
    for link in collection_links:
        if link.get("rel") == "catalog" and f"/catalogs/{catalog_id}" in link.get(
            "href", ""
        ):
            catalog_link = link
            break

    assert (
        catalog_link is not None
    ), f"Collection should have catalog link to /catalogs/{catalog_id}"
    assert catalog_link["type"] == "application/json"
    assert catalog_link["href"].endswith(f"/catalogs/{catalog_id}")

    # Verify the catalog has a child link to the collection
    catalog_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert catalog_resp.status_code == 200
    catalog_data = catalog_resp.json()
    catalog_links = catalog_data.get("links", [])
    collection_child_link = None
    for link in catalog_links:
        if link.get(
            "rel"
        ) == "child" and f"/collections/{test_collection['id']}" in link.get(
            "href", ""
        ):
            collection_child_link = link
            break

    assert (
        collection_child_link is not None
    ), f"Catalog should have child link to collection /collections/{test_collection['id']}"
    assert collection_child_link["type"] == "application/json"
    assert collection_child_link["href"].endswith(
        f"/collections/{test_collection['id']}"
    )

    # Verify the catalog now includes the collection in its collections endpoint
    catalog_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}/collections")
    assert catalog_resp.status_code == 200

    collections_response = catalog_resp.json()
    collection_ids = [col["id"] for col in collections_response["collections"]]
    assert test_collection["id"] in collection_ids


@pytest.mark.asyncio
async def test_create_catalog_collection_nonexistent_catalog(
    catalogs_app_client, load_test_data
):
    """Test creating a collection in a catalog that doesn't exist."""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-{uuid.uuid4()}"

    resp = await catalogs_app_client.post(
        "/catalogs/nonexistent-catalog/collections", json=test_collection
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_catalog(catalogs_app_client, load_test_data):
    """Test deleting a catalog without cascade."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"
    test_catalog["links"] = [
        link for link in test_catalog.get("links", []) if link.get("rel") != "child"
    ]

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201
    catalog_id = test_catalog["id"]

    # Verify catalog exists
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 200

    # Delete the catalog
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{catalog_id}")
    assert delete_resp.status_code == 204

    # Verify catalog is deleted
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_catalog_cascade(catalogs_app_client, load_test_data):
    """Test deleting a catalog with cascade delete of collections."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"
    test_catalog["links"] = [
        link for link in test_catalog.get("links", []) if link.get("rel") != "child"
    ]

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201
    catalog_id = test_catalog["id"]

    # Create a collection in the catalog
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-{uuid.uuid4()}"

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201
    collection_id = test_collection["id"]

    # Verify collection exists
    get_coll_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_coll_resp.status_code == 200

    # Delete the catalog with cascade=true
    delete_resp = await catalogs_app_client.delete(
        f"/catalogs/{catalog_id}?cascade=true"
    )
    assert delete_resp.status_code == 204

    # Verify catalog is deleted
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 404

    # Verify collection is also deleted (cascade delete)
    get_coll_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_coll_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_catalog_no_cascade(catalogs_app_client, load_test_data):
    """Test deleting a catalog without cascade (collections remain)."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"
    test_catalog["links"] = [
        link for link in test_catalog.get("links", []) if link.get("rel") != "child"
    ]

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201
    catalog_id = test_catalog["id"]

    # Create a collection in the catalog
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-{uuid.uuid4()}"

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201
    collection_id = test_collection["id"]

    # Delete the catalog with cascade=false (default)
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{catalog_id}")
    assert delete_resp.status_code == 204

    # Verify catalog is deleted
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 404

    # Verify collection still exists (no cascade delete)
    get_coll_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_coll_resp.status_code == 200

    # Verify the catalog link was removed from the collection
    collection_data = get_coll_resp.json()
    collection_links = collection_data.get("links", [])
    catalog_link = None
    for link in collection_links:
        if link.get("rel") == "catalog" and catalog_id in link.get("href", ""):
            catalog_link = link
            break

    assert (
        catalog_link is None
    ), "Collection should not have catalog link after catalog deletion"


@pytest.mark.asyncio
async def test_delete_catalog_removes_parent_ids_from_collections(
    catalogs_app_client, load_test_data
):
    """Test that deleting a catalog removes its ID from child collections' parent_ids."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id
    test_catalog["links"] = [
        link for link in test_catalog.get("links", []) if link.get("rel") != "child"
    ]

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create 3 collections in the catalog
    collection_ids = []
    for i in range(3):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"test-collection-{uuid.uuid4()}-{i}"
        test_collection["id"] = collection_id

        create_resp = await catalogs_app_client.post(
            f"/catalogs/{catalog_id}/collections", json=test_collection
        )
        assert create_resp.status_code == 201
        collection_ids.append(collection_id)

    # Verify all collections have the catalog in their parent_ids
    # (indirectly verified by checking they're accessible via the catalog endpoint)
    get_collections_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections"
    )
    assert get_collections_resp.status_code == 200
    collections_response = get_collections_resp.json()
    returned_ids = [col["id"] for col in collections_response["collections"]]
    for collection_id in collection_ids:
        assert collection_id in returned_ids

    # Delete the catalog without cascade
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{catalog_id}")
    assert delete_resp.status_code == 204

    # Verify all collections still exist
    for collection_id in collection_ids:
        get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
        assert get_resp.status_code == 200

    # Verify collections are no longer accessible via the deleted catalog
    # (This indirectly verifies parent_ids was updated)
    for collection_id in collection_ids:
        get_from_catalog_resp = await catalogs_app_client.get(
            f"/catalogs/{catalog_id}/collections/{collection_id}"
        )
        assert get_from_catalog_resp.status_code == 404


@pytest.mark.asyncio
async def test_create_catalog_collection_adds_parent_id(
    catalogs_app_client, load_test_data
):
    """Test that creating a collection in a catalog adds the catalog to parent_ids."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create a new collection through the catalog endpoint
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert create_resp.status_code == 201

    created_collection = create_resp.json()
    assert created_collection["id"] == collection_id

    # Verify the collection has the catalog in parent_ids by getting it directly
    get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_resp.status_code == 200

    collection_data = get_resp.json()
    # parent_ids should be in the collection data (from database)
    # We can verify it exists by checking the catalog link
    catalog_link = None
    for link in collection_data.get("links", []):
        if link.get("rel") == "catalog" and catalog_id in link.get("href", ""):
            catalog_link = link
            break

    assert catalog_link is not None, "Collection should have catalog link"


@pytest.mark.asyncio
async def test_add_existing_collection_to_catalog(
    catalogs_app_client, load_test_data, ctx
):
    """Test adding an existing collection to a catalog adds parent_id."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Add existing collection to the catalog
    add_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=ctx.collection
    )
    assert add_resp.status_code == 201

    # Verify we can get the collection through the catalog endpoint
    get_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections/{ctx.collection['id']}"
    )
    assert get_resp.status_code == 200


@pytest.mark.asyncio
async def test_collection_with_multiple_parent_catalogs(
    catalogs_app_client, load_test_data
):
    """Test that a collection can have multiple parent catalogs."""
    # Create two catalogs
    catalog_ids = []
    for i in range(2):
        test_catalog = load_test_data("test_catalog.json")
        catalog_id = f"test-catalog-{uuid.uuid4()}-{i}"
        test_catalog["id"] = catalog_id

        catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
        assert catalog_resp.status_code == 201
        catalog_ids.append(catalog_id)

    # Create a collection in the first catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_ids[0]}/collections", json=test_collection
    )
    assert create_resp.status_code == 201

    # Add the same collection to the second catalog
    add_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_ids[1]}/collections", json=test_collection
    )
    assert add_resp.status_code == 201

    # Verify we can get the collection from both catalogs
    for catalog_id in catalog_ids:
        get_resp = await catalogs_app_client.get(
            f"/catalogs/{catalog_id}/collections/{collection_id}"
        )
        assert get_resp.status_code == 200


@pytest.mark.asyncio
async def test_get_catalog_collections_uses_parent_ids(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog_collections queries by parent_ids."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create multiple collections in the catalog
    collection_ids = []
    for i in range(3):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"test-collection-{uuid.uuid4()}-{i}"
        test_collection["id"] = collection_id

        create_resp = await catalogs_app_client.post(
            f"/catalogs/{catalog_id}/collections", json=test_collection
        )
        assert create_resp.status_code == 201
        collection_ids.append(collection_id)

    # Get all collections from the catalog
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}/collections")
    assert get_resp.status_code == 200

    collections_response = get_resp.json()
    returned_ids = [col["id"] for col in collections_response["collections"]]

    # All created collections should be returned
    for collection_id in collection_ids:
        assert collection_id in returned_ids


@pytest.mark.asyncio
async def test_delete_collection_from_catalog_single_parent(
    catalogs_app_client, load_test_data
):
    """Test deleting a collection from a catalog when it's the only parent."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create a collection in the catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert create_resp.status_code == 201

    # Delete the collection from the catalog
    delete_resp = await catalogs_app_client.delete(
        f"/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert delete_resp.status_code == 204

    # Verify the collection is completely deleted
    get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_collection_from_catalog_multiple_parents(
    catalogs_app_client, load_test_data
):
    """Test deleting a collection from a catalog when it has multiple parents."""
    # Create two catalogs
    catalog_ids = []
    for i in range(2):
        test_catalog = load_test_data("test_catalog.json")
        catalog_id = f"test-catalog-{uuid.uuid4()}-{i}"
        test_catalog["id"] = catalog_id

        catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
        assert catalog_resp.status_code == 201
        catalog_ids.append(catalog_id)

    # Create a collection in the first catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_ids[0]}/collections", json=test_collection
    )
    assert create_resp.status_code == 201

    # Add the collection to the second catalog
    add_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_ids[1]}/collections", json=test_collection
    )
    assert add_resp.status_code == 201

    # Delete the collection from the first catalog
    delete_resp = await catalogs_app_client.delete(
        f"/catalogs/{catalog_ids[0]}/collections/{collection_id}"
    )
    assert delete_resp.status_code == 204

    # Verify the collection still exists
    get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_resp.status_code == 200

    # Verify we can still get it from the second catalog
    get_from_catalog_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_ids[1]}/collections/{collection_id}"
    )
    assert get_from_catalog_resp.status_code == 200

    # Verify we cannot get it from the first catalog anymore
    get_from_deleted_catalog_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_ids[0]}/collections/{collection_id}"
    )
    assert get_from_deleted_catalog_resp.status_code == 404


@pytest.mark.asyncio
async def test_get_collection_not_in_catalog_returns_404(
    catalogs_app_client, load_test_data, ctx
):
    """Test that getting a collection from a catalog it doesn't belong to returns 404."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Try to get a collection that's not in this catalog
    get_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections/{ctx.collection['id']}"
    )
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_collection_not_in_catalog_returns_404(
    catalogs_app_client, load_test_data, ctx
):
    """Test that deleting a collection from a catalog it doesn't belong to returns 404."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Try to delete a collection that's not in this catalog
    delete_resp = await catalogs_app_client.delete(
        f"/catalogs/{catalog_id}/collections/{ctx.collection['id']}"
    )
    assert delete_resp.status_code == 404


@pytest.mark.asyncio
async def test_catalog_links_contain_all_collections(
    catalogs_app_client, load_test_data
):
    """Test that a catalog's links contain all 3 collections added to it."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id
    # Remove any placeholder child links
    test_catalog["links"] = [
        link for link in test_catalog.get("links", []) if link.get("rel") != "child"
    ]

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create 3 collections in the catalog
    collection_ids = []
    for i in range(3):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"test-collection-{uuid.uuid4()}-{i}"
        test_collection["id"] = collection_id

        create_resp = await catalogs_app_client.post(
            f"/catalogs/{catalog_id}/collections", json=test_collection
        )
        assert create_resp.status_code == 201
        collection_ids.append(collection_id)

    # Get the catalog and verify all 3 collections are in its links
    catalog_get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert catalog_get_resp.status_code == 200

    catalog_data = catalog_get_resp.json()
    catalog_links = catalog_data.get("links", [])

    # Extract all child links (collection links)
    child_links = [link for link in catalog_links if link.get("rel") == "child"]

    # Verify we have exactly 3 child links
    assert (
        len(child_links) == 3
    ), f"Catalog should have 3 child links, but has {len(child_links)}"

    # Verify each collection ID is in the child links
    child_hrefs = [link.get("href", "") for link in child_links]
    for collection_id in collection_ids:
        collection_href = f"/collections/{collection_id}"
        assert any(
            collection_href in href for href in child_hrefs
        ), f"Collection {collection_id} missing from catalog links. Found links: {child_hrefs}"


@pytest.mark.asyncio
async def test_delete_catalog_no_cascade_orphans_collections(
    catalogs_app_client, load_test_data
):
    """Test that deleting a catalog without cascade makes collections root-level orphans."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id
    test_catalog["links"] = [
        link for link in test_catalog.get("links", []) if link.get("rel") != "child"
    ]

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create a collection in the catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert create_resp.status_code == 201

    # Delete the catalog without cascade (default behavior)
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{catalog_id}")
    assert delete_resp.status_code == 204

    # Verify the collection still exists
    get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_resp.status_code == 200

    # Verify the collection is now a root-level orphan (accessible from /collections)
    collections_resp = await catalogs_app_client.get("/collections")
    assert collections_resp.status_code == 200
    collections_data = collections_resp.json()
    collection_ids = [col["id"] for col in collections_data.get("collections", [])]
    assert (
        collection_id in collection_ids
    ), "Orphaned collection should appear in root /collections endpoint"

    # Verify the collection no longer has a catalog link to the deleted catalog
    collection_data = get_resp.json()
    collection_links = collection_data.get("links", [])
    catalog_link = None
    for link in collection_links:
        if link.get("rel") == "catalog" and catalog_id in link.get("href", ""):
            catalog_link = link
            break

    assert (
        catalog_link is None
    ), "Orphaned collection should not have link to deleted catalog"


@pytest.mark.asyncio
async def test_delete_catalog_no_cascade_multi_parent_collection(
    catalogs_app_client, load_test_data
):
    """Test that deleting a catalog without cascade preserves collections with other parents."""
    # Create two catalogs
    catalog_ids = []
    for i in range(2):
        test_catalog = load_test_data("test_catalog.json")
        catalog_id = f"test-catalog-{uuid.uuid4()}-{i}"
        test_catalog["id"] = catalog_id

        catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
        assert catalog_resp.status_code == 201
        catalog_ids.append(catalog_id)

    # Create a collection in the first catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_ids[0]}/collections", json=test_collection
    )
    assert create_resp.status_code == 201

    # Add the collection to the second catalog
    add_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_ids[1]}/collections", json=test_collection
    )
    assert add_resp.status_code == 201

    # Delete the first catalog without cascade
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{catalog_ids[0]}")
    assert delete_resp.status_code == 204

    # Verify the collection still exists
    get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_resp.status_code == 200

    # Verify the collection is still accessible from the second catalog
    get_from_catalog_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_ids[1]}/collections/{collection_id}"
    )
    assert get_from_catalog_resp.status_code == 200

    # Verify the collection is NOT accessible from the deleted catalog
    get_from_deleted_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_ids[0]}/collections/{collection_id}"
    )
    assert get_from_deleted_resp.status_code == 404


@pytest.mark.asyncio
async def test_parent_ids_not_exposed_to_client(catalogs_app_client, load_test_data):
    """Test that parent_ids field is not exposed in API responses."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create a collection in the catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert create_resp.status_code == 201

    # Verify parent_ids is not in the creation response
    created_collection = create_resp.json()
    assert (
        "parent_ids" not in created_collection
    ), "parent_ids should not be exposed in API response"

    # Verify parent_ids is not in the get response
    get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_resp.status_code == 200
    collection_data = get_resp.json()
    assert (
        "parent_ids" not in collection_data
    ), "parent_ids should not be exposed in API response"

    # Verify parent_ids is not in the catalog collection endpoint response
    catalog_collections_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections"
    )
    assert catalog_collections_resp.status_code == 200
    collections_data = catalog_collections_resp.json()
    for collection in collections_data.get("collections", []):
        assert (
            "parent_ids" not in collection
        ), "parent_ids should not be exposed in API response"
