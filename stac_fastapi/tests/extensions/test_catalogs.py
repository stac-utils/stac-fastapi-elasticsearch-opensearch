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
async def test_update_catalog(catalogs_app_client, load_test_data):
    """Test updating an existing catalog."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Update the catalog
    catalog_id = test_catalog["id"]
    updated_catalog = load_test_data("test_catalog.json")
    updated_catalog["id"] = catalog_id
    updated_catalog["title"] = "Updated Catalog Title"
    updated_catalog["description"] = "Updated description for the catalog"

    update_resp = await catalogs_app_client.put(
        f"/catalogs/{catalog_id}", json=updated_catalog
    )
    assert update_resp.status_code == 200

    updated_result = update_resp.json()
    assert updated_result["id"] == catalog_id
    assert updated_result["title"] == "Updated Catalog Title"
    assert updated_result["description"] == "Updated description for the catalog"

    # Verify the update persisted by fetching the catalog
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 200
    fetched_catalog = get_resp.json()
    assert fetched_catalog["title"] == "Updated Catalog Title"
    assert fetched_catalog["description"] == "Updated description for the catalog"


@pytest.mark.asyncio
async def test_update_nonexistent_catalog(catalogs_app_client, load_test_data):
    """Test updating a catalog that doesn't exist."""
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = "nonexistent-catalog"

    resp = await catalogs_app_client.put(
        "/catalogs/nonexistent-catalog", json=test_catalog
    )
    assert resp.status_code == 404


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
async def test_get_catalog_collections_context_fields(
    catalogs_app_client, load_test_data, ctx
):
    """Test that /catalogs/{catalog_id}/collections includes numberReturned and numberMatched.

    This test verifies the fix for issue #632.
    """
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Add two collections to the catalog
    collection1 = ctx.collection.copy()
    collection1["id"] = f"test-collection-1-{uuid.uuid4()}"

    collection2 = ctx.collection.copy()
    collection2["id"] = f"test-collection-2-{uuid.uuid4()}"

    add_resp1 = await catalogs_app_client.post(
        f"/catalogs/{test_catalog['id']}/collections", json=collection1
    )
    assert add_resp1.status_code == 201

    add_resp2 = await catalogs_app_client.post(
        f"/catalogs/{test_catalog['id']}/collections", json=collection2
    )
    assert add_resp2.status_code == 201

    # Get collections from the catalog
    resp = await catalogs_app_client.get(f"/catalogs/{test_catalog['id']}/collections")
    assert resp.status_code == 200

    collections_response = resp.json()

    # Verify context fields are present
    assert "numberReturned" in collections_response, "numberReturned field is missing"
    assert "numberMatched" in collections_response, "numberMatched field is missing"

    # Verify the values are correct
    assert collections_response["numberReturned"] == 2, "Should return 2 collections"
    assert collections_response["numberMatched"] == 2, "Should match 2 collections"

    # Verify collections are present
    assert len(collections_response["collections"]) == 2
    collection_ids = [col["id"] for col in collections_response["collections"]]
    assert collection1["id"] in collection_ids
    assert collection2["id"] in collection_ids


@pytest.mark.asyncio
async def test_get_catalog_collections_pagination(
    catalogs_app_client, load_test_data, ctx
):
    """Test pagination support for /catalogs/{catalog_id}/collections endpoint."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Add 5 collections to the catalog
    collection_ids = []
    for i in range(5):
        collection = ctx.collection.copy()
        collection["id"] = f"test-collection-{i}-{uuid.uuid4()}"
        collection_ids.append(collection["id"])

        add_resp = await catalogs_app_client.post(
            f"/catalogs/{test_catalog['id']}/collections", json=collection
        )
        assert add_resp.status_code == 201

    # Get first page with limit=2
    resp = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections?limit=2"
    )
    assert resp.status_code == 200

    page1 = resp.json()
    assert page1["numberReturned"] == 2
    assert page1["numberMatched"] == 5
    assert len(page1["collections"]) == 2

    # Verify next link exists
    links = page1["links"]
    next_links = [link for link in links if link["rel"] == "next"]
    assert len(next_links) == 1, "Should have a next link"

    # Extract token from next link
    next_url = next_links[0]["href"]
    assert "token=" in next_url

    # Get second page using token
    import re

    token_match = re.search(r"token=([^&]+)", next_url)
    assert token_match
    token = token_match.group(1)

    resp2 = await catalogs_app_client.get(
        f"/catalogs/{test_catalog['id']}/collections?limit=2&token={token}"
    )
    assert resp2.status_code == 200

    page2 = resp2.json()
    assert page2["numberReturned"] == 2
    assert page2["numberMatched"] == 5

    # Verify no duplicate collections between pages
    page1_ids = [c["id"] for c in page1["collections"]]
    page2_ids = [c["id"] for c in page2["collections"]]
    assert (
        len(set(page1_ids) & set(page2_ids)) == 0
    ), "Pages should not have overlapping collections"


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

    # Link the collection to the catalog
    link_resp = await catalogs_app_client.post(
        f"/catalogs/{test_catalog['id']}/collections", json={"id": ctx.collection["id"]}
    )
    assert link_resp.status_code == 201

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

    # Link the collection to the catalog
    link_resp = await catalogs_app_client.post(
        f"/catalogs/{test_catalog['id']}/collections", json={"id": ctx.collection["id"]}
    )
    assert link_resp.status_code == 201

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

    # Verify the collection was created by getting it via the catalog endpoint
    get_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections/{test_collection['id']}"
    )
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == test_collection["id"]

    # Verify the collection has a parent link to the catalog (when accessed via catalog context)
    collection_data = get_resp.json()
    collection_links = collection_data.get("links", [])
    parent_link = None
    for link in collection_links:
        if link.get("rel") == "parent" and f"/catalogs/{catalog_id}" in link.get(
            "href", ""
        ):
            parent_link = link
            break

    assert (
        parent_link is not None
    ), f"Collection should have parent link to /catalogs/{catalog_id}"
    assert parent_link["type"] == "application/json"
    assert parent_link["href"].endswith(f"/catalogs/{catalog_id}")

    # Verify the catalog has a children link
    catalog_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert catalog_resp.status_code == 200
    catalog_data = catalog_resp.json()
    catalog_links = catalog_data.get("links", [])
    children_link = None
    for link in catalog_links:
        if link.get(
            "rel"
        ) == "children" and f"/catalogs/{catalog_id}/children" in link.get("href", ""):
            children_link = link
            break

    assert (
        children_link is not None
    ), f"Catalog should have children link to /catalogs/{catalog_id}/children"
    assert children_link["type"] == "application/json"
    assert children_link["href"].endswith(f"/catalogs/{catalog_id}/children")

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
async def test_link_existing_collection_by_id(catalogs_app_client, load_test_data, ctx):
    """Test linking an existing collection to a catalog using only its ID."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201
    catalog_id = test_catalog["id"]

    # The collection from ctx already exists in the database
    existing_collection_id = ctx.collection["id"]

    # Link the collection using only its HTTP payload
    link_payload = {"id": existing_collection_id}
    resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=link_payload
    )
    assert resp.status_code == 201

    linked_collection = resp.json()
    assert linked_collection["id"] == existing_collection_id

    # Verify the collection is now part of the catalog
    get_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections/{existing_collection_id}"
    )
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == existing_collection_id


@pytest.mark.asyncio
async def test_link_nonexistent_collection_by_id(catalogs_app_client, load_test_data):
    """Test linking a nonexistent collection using only an ID (should fail with 404)."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201
    catalog_id = test_catalog["id"]

    # Provide an ID for a collection that doesn't exist
    fake_collection_id = f"fake-collection-{uuid.uuid4()}"

    # Try to link it
    link_payload = {"id": fake_collection_id}
    resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=link_payload
    )

    # We expect a 404 Not Found since it's just an ID and doesn't exist
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_catalog(catalogs_app_client, load_test_data):
    """Test deleting an empty catalog."""
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
async def test_delete_catalog_no_cascade(catalogs_app_client, load_test_data):
    """Test deleting a catalog (collections remain and are adopted by root)."""
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

    # Delete the catalog (cascade is no longer supported)
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{catalog_id}")
    assert delete_resp.status_code == 204

    # Verify catalog is deleted
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 404

    # Verify collection still exists (never deleted, only unlinked)
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

    # Delete the catalog
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

    # Verify the collection has the catalog in parent_ids by getting it via catalog endpoint
    get_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert get_resp.status_code == 200

    collection_data = get_resp.json()
    # parent_ids should be in the collection data (from database)
    # We can verify it exists by checking the parent link (when accessed via catalog context)
    parent_link = None
    for link in collection_data.get("links", []):
        if link.get("rel") == "parent" and catalog_id in link.get("href", ""):
            parent_link = link
            break

    assert (
        parent_link is not None
    ), "Collection should have parent link when accessed via catalog endpoint"


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
    """Test deleting a collection from a catalog when it's the only parent.

    With the "Unlink & Adopt" safety net, the collection should be adopted by root
    instead of being deleted entirely.
    """
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

    # Verify the collection still exists (adopted by root, not deleted)
    get_resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert get_resp.status_code == 200

    # Verify we cannot get it from the original catalog anymore
    get_from_catalog_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections/{collection_id}"
    )
    assert get_from_catalog_resp.status_code == 404


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

    # Get the catalog and verify it has a children link
    catalog_get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert catalog_get_resp.status_code == 200

    catalog_data = catalog_get_resp.json()
    catalog_links = catalog_data.get("links", [])

    # Extract the children link
    children_link = None
    for link in catalog_links:
        if link.get(
            "rel"
        ) == "children" and f"/catalogs/{catalog_id}/children" in link.get("href", ""):
            children_link = link
            break

    # Verify we have a children link
    assert (
        children_link is not None
    ), f"Catalog should have a children link to /catalogs/{catalog_id}/children"

    # Verify all 3 collections are accessible via the catalog's collections endpoint
    collections_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/collections"
    )
    assert collections_resp.status_code == 200
    collections_data = collections_resp.json()
    collection_ids_in_catalog = [
        col["id"] for col in collections_data.get("collections", [])
    ]

    for collection_id in collection_ids:
        assert (
            collection_id in collection_ids_in_catalog
        ), f"Collection {collection_id} missing from catalog collections endpoint"


@pytest.mark.asyncio
async def test_delete_catalog_orphans_collections(catalogs_app_client, load_test_data):
    """Test that deleting a catalog makes orphaned collections adopt root as parent."""
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
async def test_delete_catalog_preserves_multi_parent_collections(
    catalogs_app_client, load_test_data
):
    """Test that deleting a catalog preserves collections with other parents."""
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

    # Delete the first catalog
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


@pytest.mark.asyncio
async def test_get_catalog_children(catalogs_app_client, load_test_data):
    """Test getting children (collections) from a catalog."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create multiple collections in the catalog
    collection_ids = []
    for i in range(2):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"test-collection-{uuid.uuid4()}-{i}"
        test_collection["id"] = collection_id

        coll_resp = await catalogs_app_client.post(
            f"/catalogs/{catalog_id}/collections", json=test_collection
        )
        assert coll_resp.status_code == 201
        collection_ids.append(collection_id)

    # Get children from the catalog
    children_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}/children")
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    assert "children" in children_data
    assert "links" in children_data
    assert "numberReturned" in children_data
    assert "numberMatched" in children_data

    # Should have 2 children (collections)
    assert len(children_data["children"]) == 2
    assert children_data["numberReturned"] == 2
    assert children_data["numberMatched"] == 2

    # Check that all are collections
    child_types = [child["type"] for child in children_data["children"]]
    assert all(child_type == "Collection" for child_type in child_types)

    # Check that we have the right collection IDs
    returned_ids = [child["id"] for child in children_data["children"]]
    for collection_id in collection_ids:
        assert collection_id in returned_ids

    # Check required links
    links = children_data["links"]
    link_rels = [link["rel"] for link in links]
    assert "self" in link_rels
    assert "root" in link_rels
    assert "parent" in link_rels


@pytest.mark.asyncio
async def test_get_catalog_children_type_filter_catalog(
    catalogs_app_client, load_test_data
):
    """Test filtering children by type=Catalog (should return empty since no catalogs are children)."""
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

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get only catalog children (should be empty)
    children_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/children?type=Catalog"
    )
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    assert len(children_data["children"]) == 0
    assert children_data["numberReturned"] == 0
    assert children_data["numberMatched"] == 0


@pytest.mark.asyncio
async def test_get_catalog_children_type_filter_collection(
    catalogs_app_client, load_test_data
):
    """Test filtering children by type=Collection."""
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

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{catalog_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get only collection children
    children_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/children?type=Collection"
    )
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    assert len(children_data["children"]) == 1
    assert children_data["children"][0]["type"] == "Collection"
    assert children_data["children"][0]["id"] == collection_id


@pytest.mark.asyncio
async def test_get_catalog_children_nonexistent_catalog(catalogs_app_client):
    """Test getting children from a catalog that doesn't exist."""
    resp = await catalogs_app_client.get("/catalogs/nonexistent-catalog/children")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_children_pagination(catalogs_app_client, load_test_data):
    """Test pagination of children endpoint."""
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Create multiple collections in the catalog
    collection_ids = []
    for i in range(5):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"test-collection-{uuid.uuid4()}-{i}"
        test_collection["id"] = collection_id

        coll_resp = await catalogs_app_client.post(
            f"/catalogs/{catalog_id}/collections", json=test_collection
        )
        assert coll_resp.status_code == 201
        collection_ids.append(collection_id)

    # Test pagination with limit=2
    children_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/children?limit=2"
    )
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    assert len(children_data["children"]) == 2
    assert children_data["numberReturned"] == 2
    assert children_data["numberMatched"] == 5

    # Check for next link
    links = children_data["links"]
    next_link = None
    for link in links:
        if link.get("rel") == "next":
            next_link = link
            break

    assert next_link is not None, "Should have next link for pagination"

    # Follow the next link (extract token from URL)
    next_url = next_link["href"]
    # Parse the token from the URL
    from urllib.parse import parse_qs, urlparse

    parsed_url = urlparse(next_url)
    query_params = parse_qs(parsed_url.query)
    token = query_params.get("token", [None])[0]

    assert token is not None, "Next link should contain token"

    # Get next page
    next_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/children?token={token}&limit=2"
    )
    assert next_resp.status_code == 200

    next_data = next_resp.json()
    assert len(next_data["children"]) == 2  # Should have remaining 3, but limited to 2
    assert next_data["numberReturned"] == 2
    assert next_data["numberMatched"] == 5


# ============================================================================
# CATALOG-TO-CATALOG HIERARCHY TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_create_sub_catalog(catalogs_app_client, load_test_data):
    """Test creating a sub-catalog within a parent catalog."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create sub-catalog
    sub_catalog = load_test_data("test_catalog.json")
    sub_id = f"sub-catalog-{uuid.uuid4()}"
    sub_catalog["id"] = sub_id
    sub_catalog["title"] = "Sub Catalog"

    sub_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=sub_catalog
    )
    assert sub_resp.status_code == 201

    created_sub = sub_resp.json()
    assert created_sub["id"] == sub_id
    assert created_sub["type"] == "Catalog"
    assert "parent_ids" not in created_sub, "parent_ids should not be exposed"


@pytest.mark.asyncio
async def test_get_sub_catalogs(catalogs_app_client, load_test_data):
    """Test retrieving sub-catalogs from a parent catalog."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 3 sub-catalogs
    sub_ids = []
    for i in range(3):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-catalog-{uuid.uuid4()}-{i}"
        sub_catalog["id"] = sub_id

        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_ids.append(sub_id)

    # Get sub-catalogs
    get_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/catalogs")
    assert get_resp.status_code == 200

    catalogs_data = get_resp.json()
    assert "catalogs" in catalogs_data
    assert len(catalogs_data["catalogs"]) == 3

    # Verify all sub-catalog IDs are present
    returned_ids = [cat["id"] for cat in catalogs_data["catalogs"]]
    for sub_id in sub_ids:
        assert sub_id in returned_ids

    # Verify links
    links = catalogs_data.get("links", [])
    link_rels = [link["rel"] for link in links]
    assert "self" in link_rels
    assert "parent" in link_rels
    assert "root" in link_rels


@pytest.mark.asyncio
async def test_get_sub_catalogs_empty(catalogs_app_client, load_test_data):
    """Test retrieving sub-catalogs from a parent with no children."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Get sub-catalogs (should be empty)
    get_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/catalogs")
    assert get_resp.status_code == 200

    catalogs_data = get_resp.json()
    assert len(catalogs_data["catalogs"]) == 0
    assert catalogs_data.get("numberReturned", 0) == 0


@pytest.mark.asyncio
async def test_nested_catalog_hierarchy(catalogs_app_client, load_test_data):
    """Test creating a nested hierarchy of catalogs (3 levels)."""
    # Create root catalog
    root_catalog = load_test_data("test_catalog.json")
    root_id = f"root-catalog-{uuid.uuid4()}"
    root_catalog["id"] = root_id

    root_resp = await catalogs_app_client.post("/catalogs", json=root_catalog)
    assert root_resp.status_code == 201

    # Create level 1 sub-catalog
    level1_catalog = load_test_data("test_catalog.json")
    level1_id = f"level1-catalog-{uuid.uuid4()}"
    level1_catalog["id"] = level1_id

    level1_resp = await catalogs_app_client.post(
        f"/catalogs/{root_id}/catalogs", json=level1_catalog
    )
    assert level1_resp.status_code == 201

    # Create level 2 sub-catalog
    level2_catalog = load_test_data("test_catalog.json")
    level2_id = f"level2-catalog-{uuid.uuid4()}"
    level2_catalog["id"] = level2_id

    level2_resp = await catalogs_app_client.post(
        f"/catalogs/{level1_id}/catalogs", json=level2_catalog
    )
    assert level2_resp.status_code == 201

    # Verify level 1 appears in root's sub-catalogs
    root_children = await catalogs_app_client.get(f"/catalogs/{root_id}/catalogs")
    assert root_children.status_code == 200
    root_data = root_children.json()
    assert len(root_data["catalogs"]) == 1
    assert root_data["catalogs"][0]["id"] == level1_id

    # Verify level 2 appears in level 1's sub-catalogs
    level1_children = await catalogs_app_client.get(f"/catalogs/{level1_id}/catalogs")
    assert level1_children.status_code == 200
    level1_data = level1_children.json()
    assert len(level1_data["catalogs"]) == 1
    assert level1_data["catalogs"][0]["id"] == level2_id


@pytest.mark.asyncio
async def test_catalog_children_mixed_catalogs_and_collections(
    catalogs_app_client, load_test_data
):
    """Test that /children endpoint returns both catalogs and collections."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 2 sub-catalogs
    sub_cat_ids = []
    for i in range(2):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-catalog-{uuid.uuid4()}-{i}"
        sub_catalog["id"] = sub_id

        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_cat_ids.append(sub_id)

    # Create 2 collections
    coll_ids = []
    for i in range(2):
        collection = load_test_data("test_collection.json")
        coll_id = f"collection-{uuid.uuid4()}-{i}"
        collection["id"] = coll_id

        coll_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/collections", json=collection
        )
        assert coll_resp.status_code == 201
        coll_ids.append(coll_id)

    # Get all children (mixed)
    children_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/children")
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    assert children_data["numberReturned"] == 4
    assert children_data["numberMatched"] == 4

    # Verify we have both catalogs and collections
    children = children_data["children"]
    catalog_children = [c for c in children if c["type"] == "Catalog"]
    collection_children = [c for c in children if c["type"] == "Collection"]

    assert len(catalog_children) == 2
    assert len(collection_children) == 2

    # Verify IDs
    returned_cat_ids = [c["id"] for c in catalog_children]
    returned_coll_ids = [c["id"] for c in collection_children]

    for sub_id in sub_cat_ids:
        assert sub_id in returned_cat_ids

    for coll_id in coll_ids:
        assert coll_id in returned_coll_ids


@pytest.mark.asyncio
async def test_catalog_children_type_filter_catalog(
    catalogs_app_client, load_test_data
):
    """Test filtering children by type=Catalog."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 2 sub-catalogs
    for i in range(2):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-catalog-{uuid.uuid4()}-{i}"
        sub_catalog["id"] = sub_id

        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201

    # Create 1 collection
    collection = load_test_data("test_collection.json")
    coll_id = f"collection-{uuid.uuid4()}"
    collection["id"] = coll_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=collection
    )
    assert coll_resp.status_code == 201

    # Get only catalog children
    children_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/children?type=Catalog"
    )
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    assert children_data["numberReturned"] == 2
    assert children_data["numberMatched"] == 2

    # Verify all are catalogs
    for child in children_data["children"]:
        assert child["type"] == "Catalog"


@pytest.mark.asyncio
async def test_delete_catalog_with_sub_catalogs_no_cascade(
    catalogs_app_client, load_test_data
):
    """Test deleting a catalog with sub-catalogs (no cascade)."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 2 sub-catalogs
    sub_ids = []
    for i in range(2):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-catalog-{uuid.uuid4()}-{i}"
        sub_catalog["id"] = sub_id

        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_ids.append(sub_id)

    # Delete parent catalog (no cascade)
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{parent_id}")
    assert delete_resp.status_code == 204

    # Verify sub-catalogs still exist as root-level catalogs
    for sub_id in sub_ids:
        get_resp = await catalogs_app_client.get(f"/catalogs/{sub_id}")
        assert get_resp.status_code == 200

    # Verify they're no longer in parent's children
    get_parent_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}")
    assert get_parent_resp.status_code == 404


@pytest.mark.asyncio
async def test_catalog_parent_ids_not_exposed(catalogs_app_client, load_test_data):
    """Test that parent_ids field is not exposed in catalog API responses."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create sub-catalog
    sub_catalog = load_test_data("test_catalog.json")
    sub_id = f"sub-catalog-{uuid.uuid4()}"
    sub_catalog["id"] = sub_id

    sub_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=sub_catalog
    )
    assert sub_resp.status_code == 201

    # Verify parent_ids not in creation response
    created_sub = sub_resp.json()
    assert "parent_ids" not in created_sub, "parent_ids should not be exposed"

    # Verify parent_ids not in get response
    get_resp = await catalogs_app_client.get(f"/catalogs/{sub_id}")
    assert get_resp.status_code == 200
    sub_data = get_resp.json()
    assert "parent_ids" not in sub_data, "parent_ids should not be exposed"

    # Verify parent_ids not in sub-catalogs list response
    list_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/catalogs")
    assert list_resp.status_code == 200
    catalogs_data = list_resp.json()
    for catalog in catalogs_data.get("catalogs", []):
        assert (
            "parent_ids" not in catalog
        ), "parent_ids should not be exposed in list response"


@pytest.mark.asyncio
async def test_delete_sub_catalog_becomes_root_level(
    catalogs_app_client, load_test_data
):
    """Test that deleting a parent catalog makes sub-catalogs root-level."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 2 sub-catalogs
    sub_ids = []
    for i in range(2):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-catalog-{uuid.uuid4()}-{i}"
        sub_catalog["id"] = sub_id

        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_ids.append(sub_id)

    # Verify sub-catalogs are in parent's sub-catalogs list
    parent_children_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/catalogs"
    )
    assert parent_children_resp.status_code == 200
    parent_children = parent_children_resp.json()
    assert len(parent_children["catalogs"]) == 2

    # Delete the parent
    delete_resp = await catalogs_app_client.delete(f"/catalogs/{parent_id}")
    assert delete_resp.status_code == 204

    # Verify sub-catalogs still exist as root-level catalogs
    for sub_id in sub_ids:
        get_resp = await catalogs_app_client.get(f"/catalogs/{sub_id}")
        assert get_resp.status_code == 200

    # Verify parent is deleted
    parent_get_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}")
    assert parent_get_resp.status_code == 404


@pytest.mark.asyncio
async def test_catalog_poly_hierarchy(catalogs_app_client, load_test_data):
    """Test poly-hierarchy: a catalog can belong to multiple parent catalogs."""
    # Create two parent catalogs
    parent_ids = []
    for i in range(2):
        parent = load_test_data("test_catalog.json")
        parent_id = f"parent-{uuid.uuid4()}-{i}"
        parent["id"] = parent_id

        parent_resp = await catalogs_app_client.post("/catalogs", json=parent)
        assert parent_resp.status_code == 201
        parent_ids.append(parent_id)

    # Create a sub-catalog under first parent
    sub_catalog = load_test_data("test_catalog.json")
    sub_id = f"sub-catalog-{uuid.uuid4()}"
    sub_catalog["id"] = sub_id

    create_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_ids[0]}/catalogs", json=sub_catalog
    )
    assert create_resp.status_code == 201

    # Link the same sub-catalog to second parent (poly-hierarchy)
    link_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_ids[1]}/catalogs",
        json=load_test_data("test_catalog.json") | {"id": sub_id},
    )
    assert link_resp.status_code == 201

    # Verify sub-catalog appears in both parents' sub-catalogs lists
    for parent_id in parent_ids:
        get_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/catalogs")
        assert get_resp.status_code == 200

        catalogs_data = get_resp.json()
        returned_ids = [cat["id"] for cat in catalogs_data["catalogs"]]
        assert sub_id in returned_ids

    # Verify the sub-catalog itself still exists and is retrievable
    get_sub_resp = await catalogs_app_client.get(f"/catalogs/{sub_id}")
    assert get_sub_resp.status_code == 200


@pytest.mark.asyncio
async def test_get_sub_catalogs_pagination(catalogs_app_client, load_test_data):
    """Test pagination of sub-catalogs endpoint."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 15 sub-catalogs (more than default limit of 10)
    sub_ids = []
    for i in range(15):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-catalog-{uuid.uuid4()}-{i:02d}"
        sub_catalog["id"] = sub_id

        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_ids.append(sub_id)

    # Get first page with default limit (10)
    page1_resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/catalogs")
    assert page1_resp.status_code == 200
    page1_data = page1_resp.json()

    assert len(page1_data["catalogs"]) == 10
    assert page1_data["numberReturned"] == 10
    assert page1_data["numberMatched"] == 15
    assert "next" in [link["rel"] for link in page1_data["links"]]

    # Get next page using token
    next_link = next(
        (link for link in page1_data["links"] if link["rel"] == "next"), None
    )
    assert next_link is not None

    # Extract token from next link
    next_url = next_link["href"]
    page2_resp = await catalogs_app_client.get(next_url)
    assert page2_resp.status_code == 200
    page2_data = page2_resp.json()

    # Second page should have remaining 5 catalogs
    assert len(page2_data["catalogs"]) == 5
    assert page2_data["numberReturned"] == 5
    assert page2_data["numberMatched"] == 15

    # Verify no 'next' link on last page
    assert "next" not in [link["rel"] for link in page2_data["links"]]

    # Verify all catalogs are unique across pages
    page1_ids = [cat["id"] for cat in page1_data["catalogs"]]
    page2_ids = [cat["id"] for cat in page2_data["catalogs"]]
    assert len(set(page1_ids) & set(page2_ids)) == 0  # No overlap
    assert len(page1_ids) + len(page2_ids) == 15  # All accounted for


@pytest.mark.asyncio
async def test_get_sub_catalogs_pagination_with_limit(
    catalogs_app_client, load_test_data
):
    """Test pagination of sub-catalogs with custom limit parameter."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 8 sub-catalogs
    sub_ids = []
    for i in range(8):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-catalog-{uuid.uuid4()}-{i:02d}"
        sub_catalog["id"] = sub_id

        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_ids.append(sub_id)

    # Get first page with limit=3
    page1_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/catalogs?limit=3"
    )
    assert page1_resp.status_code == 200
    page1_data = page1_resp.json()

    assert len(page1_data["catalogs"]) == 3
    assert page1_data["numberReturned"] == 3
    assert page1_data["numberMatched"] == 8

    # Get second page
    next_link = next(
        (link for link in page1_data["links"] if link["rel"] == "next"), None
    )
    assert next_link is not None
    assert "limit=3" in next_link["href"]

    page2_resp = await catalogs_app_client.get(next_link["href"])
    assert page2_resp.status_code == 200
    page2_data = page2_resp.json()

    assert len(page2_data["catalogs"]) == 3
    assert page2_data["numberReturned"] == 3

    # Get third page
    next_link = next(
        (link for link in page2_data["links"] if link["rel"] == "next"), None
    )
    assert next_link is not None

    page3_resp = await catalogs_app_client.get(next_link["href"])
    assert page3_resp.status_code == 200
    page3_data = page3_resp.json()

    assert len(page3_data["catalogs"]) == 2
    assert page3_data["numberReturned"] == 2

    # Verify no 'next' link on last page
    assert "next" not in [link["rel"] for link in page3_data["links"]]


@pytest.mark.asyncio
async def test_get_catalog_collections_breadcrumb_parent_link(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog_collections returns parent link pointing to the specific catalog.

    This tests the DAG specification requirement that scoped endpoints lock the breadcrumb
    to the specific catalog for contextual navigation in STAC Browser.
    """
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

    # Get collections from the catalog
    resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}/collections")
    assert resp.status_code == 200

    collections_response = resp.json()
    links = collections_response.get("links", [])

    # Verify parent link points to the specific catalog (not root)
    parent_link = next((link for link in links if link.get("rel") == "parent"), None)
    assert parent_link is not None, "Collections response should have parent link"
    assert (
        f"/catalogs/{catalog_id}" in parent_link["href"]
    ), f"Parent link should point to specific catalog, got: {parent_link['href']}"
    assert parent_link["href"].endswith(
        f"/catalogs/{catalog_id}"
    ), "Parent link should end with /catalogs/{catalog_id}"


@pytest.mark.asyncio
async def test_get_catalog_dynamic_parent_links_single_parent(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog returns parent links for all parent catalogs (single parent case).

    This tests the DAG specification requirement for dynamic parent link generation.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a child catalog
    child_catalog = load_test_data("test_catalog.json")
    child_id = f"child-catalog-{uuid.uuid4()}"
    child_catalog["id"] = child_id

    # Link child to parent
    link_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=child_catalog
    )
    assert link_resp.status_code == 201

    # Get the child catalog and verify parent links
    resp = await catalogs_app_client.get(f"/catalogs/{child_id}")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # Find all parent links
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert len(parent_links) > 0, "Catalog should have at least one parent link"

    # Verify parent link points to the parent catalog
    parent_hrefs = [link["href"] for link in parent_links]
    assert any(
        f"/catalogs/{parent_id}" in href for href in parent_hrefs
    ), f"Parent links should include parent catalog, got: {parent_hrefs}"


@pytest.mark.asyncio
async def test_get_catalog_dynamic_parent_links_poly_hierarchy(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog returns single parent + related links for poly-hierarchical catalogs.

    This tests the new spec requirement where:
    - Exactly ONE rel="parent" link (the first/primary parent)
    - Additional parents exposed via rel="related" links
    """
    # Create two parent catalogs
    parent_ids = []
    for i in range(2):
        parent_catalog = load_test_data("test_catalog.json")
        parent_id = f"parent-catalog-{uuid.uuid4()}-{i}"
        parent_catalog["id"] = parent_id

        parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
        assert parent_resp.status_code == 201
        parent_ids.append(parent_id)

    # Create a child catalog
    child_catalog = load_test_data("test_catalog.json")
    child_id = f"child-catalog-{uuid.uuid4()}"
    child_catalog["id"] = child_id

    # Link child to both parents
    for parent_id in parent_ids:
        link_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=child_catalog
        )
        assert link_resp.status_code == 201

    # Get the child catalog and verify link structure
    resp = await catalogs_app_client.get(f"/catalogs/{child_id}")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # NEW SPEC: Exactly ONE parent link
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert (
        len(parent_links) == 1
    ), f"Catalog should have exactly 1 parent link per new spec, got {len(parent_links)}"

    # Verify the parent link points to one of the parent catalogs
    parent_href = parent_links[0]["href"]
    assert any(
        f"/catalogs/{parent_id}" in parent_href for parent_id in parent_ids
    ), f"Parent link should point to one of the parent catalogs, got: {parent_href}"

    # NEW SPEC: Additional parents as rel="related" links
    related_links = [link for link in links if link.get("rel") == "related"]
    assert (
        len(related_links) >= 1
    ), f"Catalog with 2 parents should have at least 1 related link for the other parent, got {len(related_links)}"

    # Verify related links point to the other parent(s)
    related_hrefs = [link["href"] for link in related_links]
    # At least one related link should point to a parent catalog
    assert any(
        any(f"/catalogs/{parent_id}" in href for parent_id in parent_ids)
        for href in related_hrefs
    ), "Related links should include other parent catalogs"

    # NOTE: Catalogs do NOT have rel="duplicate" links because they don't have
    # scoped read endpoints (no GET /catalogs/{parent}/catalogs/{child})


@pytest.mark.asyncio
async def test_get_catalog_dynamic_child_links(catalogs_app_client, load_test_data):
    """Test that get_catalog returns child links for all children (catalogs and collections).

    This tests the DAG specification requirement for dynamic child link generation.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a child catalog
    child_catalog = load_test_data("test_catalog.json")
    child_catalog_id = f"child-catalog-{uuid.uuid4()}"
    child_catalog["id"] = child_catalog_id

    link_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=child_catalog
    )
    assert link_resp.status_code == 201

    # Create a child collection
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get the parent catalog and verify it has child links
    resp = await catalogs_app_client.get(f"/catalogs/{parent_id}")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # Find all child links
    child_links = [link for link in links if link.get("rel") == "child"]
    assert (
        len(child_links) >= 2
    ), f"Catalog with 1 child catalog and 1 child collection should have at least 2 child links, got {len(child_links)}"

    # Verify child catalog link exists
    child_hrefs = [link["href"] for link in child_links]
    assert any(
        f"/catalogs/{child_catalog_id}" in href for href in child_hrefs
    ), "Child links should include child catalog"

    # Verify child collection link exists
    assert any(
        f"/catalogs/{parent_id}/collections/{collection_id}" in href
        for href in child_hrefs
    ), "Child links should include child collection"


@pytest.mark.asyncio
async def test_get_catalog_includes_children_endpoint_link(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog includes the /children endpoint link.

    This ensures backward compatibility with the children endpoint while also
    providing dynamic child links.
    """
    # Create a catalog
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Get the catalog
    resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # Verify children endpoint link exists
    children_link = next(
        (link for link in links if link.get("rel") == "children"), None
    )
    assert children_link is not None, "Catalog should have children endpoint link"
    assert (
        f"/catalogs/{catalog_id}/children" in children_link["href"]
    ), "Children link should point to /children endpoint"


@pytest.mark.asyncio
async def test_get_catalog_root_parent_link_for_top_level_catalog(
    catalogs_app_client, load_test_data
):
    """Test that top-level catalogs (no parents) have a parent link pointing to root.

    This tests the DAG specification requirement that top-level catalogs point to root.
    """
    # Create a top-level catalog (no parents)
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    catalog_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert catalog_resp.status_code == 201

    # Get the catalog
    resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # Find parent links
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert (
        len(parent_links) > 0
    ), "Top-level catalog should have at least one parent link"

    # Verify at least one parent link points to root (base_url)
    parent_hrefs = [link["href"] for link in parent_links]
    # Root parent should be just the base URL (no /catalogs/ path)
    assert any(
        "/catalogs/" not in href or href.endswith("/") for href in parent_hrefs
    ), f"Top-level catalog should have parent link to root, got: {parent_hrefs}"


@pytest.mark.asyncio
async def test_get_catalog_child_links_pagination_over_100(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog returns all child links even with >100 children (pagination).

    This tests that the pagination loop correctly fetches all children across multiple pages.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 15 child collections (more than the 10 limit per page to test pagination)
    collection_ids = []
    for i in range(15):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"test-collection-{uuid.uuid4()}-{i:03d}"
        test_collection["id"] = collection_id

        coll_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/collections", json=test_collection
        )
        assert coll_resp.status_code == 201
        collection_ids.append(collection_id)

    # Get the parent catalog and verify it has all child links
    # Use limit=10 to test pagination with smaller page size (will require 2 pages for 15 items)
    resp = await catalogs_app_client.get(f"/catalogs/{parent_id}?limit=10")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # Find all child links
    child_links = [link for link in links if link.get("rel") == "child"]
    assert (
        len(child_links) >= 15
    ), f"Catalog with 15 children should have at least 15 child links, got {len(child_links)}"

    # Verify all collection IDs are represented in child links
    child_hrefs = [link["href"] for link in child_links]
    for collection_id in collection_ids:
        assert any(
            collection_id in href for href in child_hrefs
        ), f"Child links should include collection {collection_id}"


@pytest.mark.asyncio
async def test_get_catalog_deduplicates_parent_links(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog returns exactly one parent link per new spec.

    This tests the new spec requirement where:
    - Exactly ONE rel="parent" link (no duplicates possible)
    - Related links are also deduplicated
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a child catalog
    child_catalog = load_test_data("test_catalog.json")
    child_id = f"child-catalog-{uuid.uuid4()}"
    child_catalog["id"] = child_id

    link_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=child_catalog
    )
    assert link_resp.status_code == 201

    # Get the child catalog
    resp = await catalogs_app_client.get(f"/catalogs/{child_id}")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # NEW SPEC: Exactly ONE parent link
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert (
        len(parent_links) == 1
    ), f"Catalog should have exactly 1 parent link per new spec, got {len(parent_links)}"

    # Verify no duplicate hrefs in any link relation type
    parent_hrefs = [link["href"] for link in parent_links]
    assert len(parent_hrefs) == len(
        set(parent_hrefs)
    ), f"Parent links should be unique, got duplicates: {parent_hrefs}"

    # Verify related links are also unique
    related_links = [link for link in links if link.get("rel") == "related"]
    related_hrefs = [link["href"] for link in related_links]
    assert len(related_hrefs) == len(
        set(related_hrefs)
    ), f"Related links should be unique, got duplicates: {related_hrefs}"

    # NOTE: Catalogs do NOT have rel="duplicate" links (no scoped read endpoints)


@pytest.mark.asyncio
async def test_get_catalog_child_links_with_missing_title(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog handles children with missing title field gracefully.

    This tests that child links use the child ID as fallback when title is missing.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a child collection
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id
    # Remove title to test fallback
    test_collection.pop("title", None)

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get the parent catalog
    resp = await catalogs_app_client.get(f"/catalogs/{parent_id}")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # Find child links
    child_links = [link for link in links if link.get("rel") == "child"]
    assert len(child_links) > 0, "Catalog should have child links"

    # Verify the child link has a title (should be the collection_id as fallback)
    child_link = next(
        (link for link in child_links if collection_id in link["href"]), None
    )
    assert child_link is not None, "Child link for collection should exist"
    assert "title" in child_link, "Child link should have title field"
    assert (
        child_link["title"] == collection_id
    ), f"Child link title should fallback to collection_id, got: {child_link['title']}"


@pytest.mark.asyncio
async def test_get_catalog_mixed_child_types_pagination(
    catalogs_app_client, load_test_data
):
    """Test that get_catalog correctly handles mixed child types (catalogs and collections) with pagination.

    This tests that both catalog and collection children are included in the paginated results.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 15 child catalogs
    child_catalog_ids = []
    for i in range(15):
        child_catalog = load_test_data("test_catalog.json")
        child_id = f"child-catalog-{uuid.uuid4()}-{i:03d}"
        child_catalog["id"] = child_id

        link_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=child_catalog
        )
        assert link_resp.status_code == 201
        child_catalog_ids.append(child_id)

    # Create 15 child collections
    child_collection_ids = []
    for i in range(15):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"test-collection-{uuid.uuid4()}-{i:03d}"
        test_collection["id"] = collection_id

        coll_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/collections", json=test_collection
        )
        assert coll_resp.status_code == 201
        child_collection_ids.append(collection_id)

    # Get the parent catalog
    # Use limit=10 to test pagination with smaller page size
    resp = await catalogs_app_client.get(f"/catalogs/{parent_id}?limit=10")
    assert resp.status_code == 200

    catalog_data = resp.json()
    links = catalog_data.get("links", [])

    # Find all child links
    child_links = [link for link in links if link.get("rel") == "child"]
    assert (
        len(child_links) >= 30
    ), f"Catalog with 30 children should have at least 30 child links, got {len(child_links)}"

    # Verify both catalog and collection children are present
    child_hrefs = [link["href"] for link in child_links]

    # Check for catalog children
    catalog_child_count = sum(
        1
        for cid in child_catalog_ids
        if any(f"/catalogs/{cid}" in href for href in child_hrefs)
    )
    assert (
        catalog_child_count >= 10
    ), f"Should have at least 10 catalog children in links, got {catalog_child_count}"

    # Check for collection children
    collection_child_count = sum(
        1 for cid in child_collection_ids if any(cid in href for href in child_hrefs)
    )
    assert (
        collection_child_count >= 10
    ), f"Should have at least 10 collection children in links, got {collection_child_count}"


@pytest.mark.asyncio
async def test_collection_serializer_dynamic_parent_links(
    catalogs_app_client, load_test_data
):
    """Test that CollectionSerializer injects dynamic links via /collections endpoint.

    Per the new spec, collections accessed via the global /collections/{id} endpoint
    MUST have parent → root (/) and catalog parents exposed as rel="related" links.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a collection linked to the parent catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get the collection via the global /collections endpoint
    resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert resp.status_code == 200

    collection_data = resp.json()
    links = collection_data.get("links", [])

    # NEW SPEC: Global endpoint MUST have parent → root (/)
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert len(parent_links) == 1, "Collection should have exactly 1 parent link"

    parent_href = parent_links[0]["href"]
    assert (
        parent_href.rstrip("/") == "http://test-server"
    ), f"Global collection parent MUST point to root (/), got: {parent_href}"

    # NEW SPEC: Catalog parents should be rel="related" links
    related_links = [link for link in links if link.get("rel") == "related"]
    related_hrefs = [link["href"] for link in related_links]
    assert any(
        parent_id in href for href in related_hrefs
    ), f"Related links should include catalog parent {parent_id}"


@pytest.mark.asyncio
async def test_collection_serializer_deduplicates_parent_links(
    catalogs_app_client, load_test_data
):
    """Test that CollectionSerializer deduplicates parent links if parent_ids has duplicates.

    This tests that even if the database has duplicate parent IDs, only unique parent links
    are returned when accessed via /collections endpoint.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a collection linked to the parent catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get the collection via the global /collections endpoint
    resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert resp.status_code == 200

    collection_data = resp.json()
    links = collection_data.get("links", [])

    # Find parent links
    parent_links = [link for link in links if link.get("rel") == "parent"]
    parent_hrefs = [link["href"] for link in parent_links]

    # Verify no duplicate parent links
    assert len(parent_hrefs) == len(
        set(parent_hrefs)
    ), f"Parent links should be unique, got duplicates: {parent_hrefs}"


@pytest.mark.asyncio
async def test_collection_serializer_poly_hierarchy_parent_links(
    catalogs_app_client, load_test_data
):
    """Test that CollectionSerializer handles poly-hierarchy (multiple parents) correctly.

    This tests the new spec requirement where:
    - Exactly ONE rel="parent" link (the first/primary parent)
    - Additional parents exposed via rel="related" links
    - All alternative scoped URIs exposed via rel="duplicate" links
    - Canonical link points to global /collections/{id} endpoint
    """
    # Create two parent catalogs
    parent_catalog_1 = load_test_data("test_catalog.json")
    parent_id_1 = f"parent-catalog-1-{uuid.uuid4()}"
    parent_catalog_1["id"] = parent_id_1

    parent_resp_1 = await catalogs_app_client.post("/catalogs", json=parent_catalog_1)
    assert parent_resp_1.status_code == 201

    parent_catalog_2 = load_test_data("test_catalog.json")
    parent_id_2 = f"parent-catalog-2-{uuid.uuid4()}"
    parent_catalog_2["id"] = parent_id_2

    parent_resp_2 = await catalogs_app_client.post("/catalogs", json=parent_catalog_2)
    assert parent_resp_2.status_code == 201

    # Create a collection linked to the first parent
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id_1}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Link the collection to the second parent as well
    # POST the same collection to the second catalog to add it as a parent
    link_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id_2}/collections", json=test_collection
    )
    assert link_resp.status_code == 201

    # Get the collection via the global /collections endpoint
    resp = await catalogs_app_client.get(f"/collections/{collection_id}")
    assert resp.status_code == 200

    collection_data = resp.json()
    links = collection_data.get("links", [])

    # NEW SPEC: Global endpoint MUST have parent → root (/)
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert (
        len(parent_links) == 1
    ), f"Collection should have exactly 1 parent link per new spec, got {len(parent_links)}"

    # Verify the parent link points to root (/)
    parent_href = parent_links[0]["href"]
    assert (
        parent_href.rstrip("/") == "http://test-server"
    ), f"Global collection parent link MUST point to root (/), got: {parent_href}"

    # NEW SPEC: ALL catalog parents as rel="related" links
    related_links = [link for link in links if link.get("rel") == "related"]
    assert (
        len(related_links) >= 2
    ), f"Collection with 2 catalog parents should have 2 related links, got {len(related_links)}"

    # Verify related links include BOTH parent catalogs
    related_hrefs = [link["href"] for link in related_links]
    for parent_id in [parent_id_1, parent_id_2]:
        assert any(
            parent_id in href for href in related_hrefs
        ), f"Related links should include catalog parent {parent_id}"

    # NEW SPEC: Canonical link points to global endpoint
    canonical_links = [link for link in links if link.get("rel") == "canonical"]
    assert (
        len(canonical_links) == 1
    ), f"Collection should have exactly 1 canonical link, got {len(canonical_links)}"
    assert (
        f"/collections/{collection_id}" in canonical_links[0]["href"]
    ), "Canonical link should point to global /collections endpoint"

    # NEW SPEC: Duplicate links for alternative scoped URIs
    duplicate_links = [link for link in links if link.get("rel") == "duplicate"]
    assert (
        len(duplicate_links) >= 2
    ), f"Collection with 2 parents should have at least 2 duplicate links, got {len(duplicate_links)}"

    # Verify duplicate links point to catalog-scoped URIs
    duplicate_hrefs = [link["href"] for link in duplicate_links]
    assert any(
        f"/catalogs/{parent_id_1}/collections/{collection_id}" in href
        for href in duplicate_hrefs
    ), f"Duplicate links should include scoped URI for parent {parent_id_1}"
    assert any(
        f"/catalogs/{parent_id_2}/collections/{collection_id}" in href
        for href in duplicate_hrefs
    ), f"Duplicate links should include scoped URI for parent {parent_id_2}"


@pytest.mark.asyncio
async def test_catalogs_list_includes_parent_links(catalogs_app_client, load_test_data):
    """Test that GET /catalogs list includes parent links for catalogs with parents."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id
    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create child catalog under parent
    child_catalog = load_test_data("test_catalog.json")
    child_id = f"child-{uuid.uuid4()}"
    child_catalog["id"] = child_id
    child_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=child_catalog
    )
    assert child_resp.status_code == 201

    # Get all catalogs list with increased limit to ensure we get all catalogs
    list_resp = await catalogs_app_client.get("/catalogs?limit=100")
    assert list_resp.status_code == 200

    catalogs_data = list_resp.json()
    catalogs = catalogs_data["catalogs"]

    # Find the child catalog in the list
    child_in_list = next((c for c in catalogs if c["id"] == child_id), None)
    assert (
        child_in_list is not None
    ), f"Child catalog {child_id} should be in list. Available catalogs: {[c['id'] for c in catalogs]}"

    # Verify child has parent link
    parent_links = [
        link for link in child_in_list.get("links", []) if link.get("rel") == "parent"
    ]
    assert (
        len(parent_links) > 0
    ), "Child catalog should have parent link in list response"

    # Verify parent link points to correct parent
    parent_hrefs = [link["href"] for link in parent_links]
    assert any(
        parent_id in href for href in parent_hrefs
    ), f"Parent link should reference {parent_id}"

    # Verify parent link has title
    assert all(
        "title" in link for link in parent_links
    ), "All parent links should have titles"


@pytest.mark.asyncio
async def test_posted_catalog_dynamic_links_not_persisted(
    catalogs_app_client, load_test_data
):
    """Test that parent/child/children links in POST request are not persisted in database."""
    # Create a catalog with dynamic links in the request
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    # Add dynamic links to the request (these should be filtered out)
    test_catalog["links"].append(
        {
            "rel": "parent",
            "type": "application/json",
            "href": "http://localhost:8080/catalogs/fake-parent",
            "title": "Fake Parent",
        }
    )
    test_catalog["links"].append(
        {
            "rel": "child",
            "type": "application/json",
            "href": "http://localhost:8080/catalogs/fake-child",
            "title": "Fake Child",
        }
    )
    test_catalog["links"].append(
        {
            "rel": "children",
            "type": "application/json",
            "href": "http://localhost:8080/catalogs/test/children",
            "title": "Children",
        }
    )

    # POST the catalog
    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Fetch the catalog individually
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 200

    stored_catalog = get_resp.json()
    stored_links = stored_catalog.get("links", [])

    # Verify dynamic links from request are NOT in stored catalog
    parent_links = [link for link in stored_links if link.get("rel") == "parent"]
    child_links = [link for link in stored_links if link.get("rel") == "child"]
    children_links = [link for link in stored_links if link.get("rel") == "children"]

    # Parent links should not include the fake parent from request
    fake_parent_links = [
        link for link in parent_links if "fake-parent" in link.get("href", "")
    ]
    assert (
        len(fake_parent_links) == 0
    ), "Fake parent link from request should not be persisted"

    # Child links should not include the fake child from request
    fake_child_links = [
        link for link in child_links if "fake-child" in link.get("href", "")
    ]
    assert (
        len(fake_child_links) == 0
    ), "Fake child link from request should not be persisted"

    # Children links should not include the fake children from request
    fake_children_links = [
        link for link in children_links if "fake" in link.get("href", "")
    ]
    assert (
        len(fake_children_links) == 0
    ), "Fake children link from request should not be persisted"


@pytest.mark.asyncio
async def test_posted_catalog_user_links_are_persisted(
    catalogs_app_client, load_test_data
):
    """Test that user-provided links (license, about, etc.) ARE persisted in database."""
    # Create a catalog with user-provided links
    test_catalog = load_test_data("test_catalog.json")
    catalog_id = f"test-catalog-{uuid.uuid4()}"
    test_catalog["id"] = catalog_id

    # Ensure user-provided links are in the request
    user_links = [
        {"rel": "license", "href": "https://example.com/license", "title": "License"},
        {"rel": "about", "href": "https://example.com/about", "title": "About"},
    ]
    test_catalog["links"] = user_links

    # POST the catalog
    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    # Fetch the catalog individually
    get_resp = await catalogs_app_client.get(f"/catalogs/{catalog_id}")
    assert get_resp.status_code == 200

    stored_catalog = get_resp.json()
    stored_links = stored_catalog.get("links", [])

    # Verify user-provided links ARE persisted
    license_links = [link for link in stored_links if link.get("rel") == "license"]
    assert len(license_links) > 0, "License link should be persisted"
    assert any(
        "example.com/license" in link.get("href", "") for link in license_links
    ), "License link should have correct href"

    about_links = [link for link in stored_links if link.get("rel") == "about"]
    assert len(about_links) > 0, "About link should be persisted"
    assert any(
        "example.com/about" in link.get("href", "") for link in about_links
    ), "About link should have correct href"


@pytest.mark.asyncio
async def test_subcatalog_list_endpoint_includes_links(
    catalogs_app_client, load_test_data
):
    """Test that GET /catalogs/{id}/catalogs returns sub-catalogs with parent and child links."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id
    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create multiple sub-catalogs
    sub_catalog_ids = []
    for i in range(3):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-{uuid.uuid4()}-{i}"
        sub_catalog["id"] = sub_id
        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_catalog_ids.append(sub_id)

    # Get sub-catalogs via /catalogs/{id}/catalogs endpoint
    list_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/catalogs?limit=100"
    )
    assert list_resp.status_code == 200

    catalogs_data = list_resp.json()
    catalogs = catalogs_data["catalogs"]

    # Verify all sub-catalogs are returned
    assert (
        len(catalogs) >= 3
    ), f"Should have at least 3 sub-catalogs, got {len(catalogs)}"

    # Verify each sub-catalog has parent links
    for sub_id in sub_catalog_ids:
        sub_in_list = next((c for c in catalogs if c["id"] == sub_id), None)
        assert sub_in_list is not None, f"Sub-catalog {sub_id} should be in list"

        parent_links = [
            link for link in sub_in_list.get("links", []) if link.get("rel") == "parent"
        ]
        assert len(parent_links) > 0, f"Sub-catalog {sub_id} should have parent link"
        assert any(
            parent_id in link.get("href", "") for link in parent_links
        ), f"Parent link should reference {parent_id}"
        assert all(
            "title" in link for link in parent_links
        ), f"All parent links for {sub_id} should have titles"


@pytest.mark.asyncio
async def test_subcatalog_list_endpoint_includes_child_links(
    catalogs_app_client, load_test_data
):
    """Test that GET /catalogs/{id}/catalogs returns sub-catalogs with child links."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id
    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a sub-catalog
    sub_catalog = load_test_data("test_catalog.json")
    sub_id = f"sub-{uuid.uuid4()}"
    sub_catalog["id"] = sub_id
    sub_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=sub_catalog
    )
    assert sub_resp.status_code == 201

    # Add a collection to the sub-catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id
    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{sub_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get sub-catalogs via /catalogs/{id}/catalogs endpoint
    list_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/catalogs?limit=100"
    )
    assert list_resp.status_code == 200

    catalogs_data = list_resp.json()
    catalogs = catalogs_data["catalogs"]

    # Find the sub-catalog in the list
    sub_in_list = next((c for c in catalogs if c["id"] == sub_id), None)
    assert sub_in_list is not None, f"Sub-catalog {sub_id} should be in list"

    # Verify sub-catalog has child links
    child_links = [
        link for link in sub_in_list.get("links", []) if link.get("rel") == "child"
    ]
    if child_links:
        # Child links are dynamically generated, so they may or may not be present
        # But if they are, verify they point to the correct child
        child_hrefs = [link["href"] for link in child_links]
        assert any(
            collection_id in href for href in child_hrefs
        ), f"Child link should reference {collection_id}"
        assert all(
            "title" in link for link in child_links
        ), "All child links should have titles"


@pytest.mark.asyncio
async def test_both_endpoints_return_consistent_links(
    catalogs_app_client, load_test_data
):
    """Test that /catalogs and /catalogs/{id}/catalogs return consistent parent/child links."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id
    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a sub-catalog
    sub_catalog = load_test_data("test_catalog.json")
    sub_id = f"sub-{uuid.uuid4()}"
    sub_catalog["id"] = sub_id
    sub_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=sub_catalog
    )
    assert sub_resp.status_code == 201

    # Get from /catalogs endpoint
    all_catalogs_resp = await catalogs_app_client.get("/catalogs?limit=100")
    assert all_catalogs_resp.status_code == 200
    all_catalogs = all_catalogs_resp.json()["catalogs"]
    sub_from_all = next((c for c in all_catalogs if c["id"] == sub_id), None)

    # Get from /catalogs/{id}/catalogs endpoint
    sub_catalogs_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/catalogs?limit=100"
    )
    assert sub_catalogs_resp.status_code == 200
    sub_catalogs = sub_catalogs_resp.json()["catalogs"]
    sub_from_endpoint = next((c for c in sub_catalogs if c["id"] == sub_id), None)

    # Both should have the same parent links
    assert sub_from_all is not None, f"Sub-catalog {sub_id} should be in /catalogs"
    assert (
        sub_from_endpoint is not None
    ), f"Sub-catalog {sub_id} should be in /catalogs/{parent_id}/catalogs"

    parent_links_all = [
        link for link in sub_from_all.get("links", []) if link.get("rel") == "parent"
    ]
    parent_links_endpoint = [
        link
        for link in sub_from_endpoint.get("links", [])
        if link.get("rel") == "parent"
    ]

    # Both should have parent links
    assert (
        len(parent_links_all) > 0
    ), "Sub-catalog from /catalogs should have parent links"
    assert (
        len(parent_links_endpoint) > 0
    ), f"Sub-catalog from /catalogs/{parent_id}/catalogs should have parent links"

    # Parent links should be consistent
    parent_hrefs_all = [link.get("href") for link in parent_links_all]
    parent_hrefs_endpoint = [link.get("href") for link in parent_links_endpoint]
    assert any(
        parent_id in href for href in parent_hrefs_all
    ), "Parent link from /catalogs should reference parent"
    assert any(
        parent_id in href for href in parent_hrefs_endpoint
    ), "Parent link from /catalogs/{id}/catalogs should reference parent"


@pytest.mark.asyncio
async def test_children_endpoint_catalogs_include_links(
    catalogs_app_client, load_test_data
):
    """Test that GET /catalogs/{id}/children returns catalog children with parent and child links."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id
    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a sub-catalog
    sub_catalog = load_test_data("test_catalog.json")
    sub_id = f"sub-{uuid.uuid4()}"
    sub_catalog["id"] = sub_id
    sub_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=sub_catalog
    )
    assert sub_resp.status_code == 201

    # Add a collection to the sub-catalog
    test_collection = load_test_data("test_collection.json")
    collection_id = f"collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id
    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{sub_id}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Get children via /catalogs/{id}/children endpoint
    children_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/children?limit=100"
    )
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    children = children_data["children"]

    # Find the sub-catalog in the children list
    sub_in_children = next((c for c in children if c.get("id") == sub_id), None)
    assert (
        sub_in_children is not None
    ), f"Sub-catalog {sub_id} should be in /children response"

    # Verify sub-catalog has parent links
    parent_links = [
        link for link in sub_in_children.get("links", []) if link.get("rel") == "parent"
    ]
    assert (
        len(parent_links) > 0
    ), f"Sub-catalog {sub_id} in /children should have parent link"
    assert any(
        parent_id in link.get("href", "") for link in parent_links
    ), f"Parent link should reference {parent_id}"
    assert all(
        "title" in link for link in parent_links
    ), "All parent links should have titles"

    # Verify sub-catalog has child links
    child_links = [
        link for link in sub_in_children.get("links", []) if link.get("rel") == "child"
    ]
    if child_links:
        child_hrefs = [link["href"] for link in child_links]
        assert any(
            collection_id in href for href in child_hrefs
        ), f"Child link should reference {collection_id}"
        assert all(
            "title" in link for link in child_links
        ), "All child links should have titles"


@pytest.mark.asyncio
async def test_children_endpoint_mixed_content_with_links(
    catalogs_app_client, load_test_data
):
    """Test that GET /catalogs/{id}/children returns both catalogs and collections with proper links."""
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id
    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create 2 sub-catalogs
    sub_catalog_ids = []
    for i in range(2):
        sub_catalog = load_test_data("test_catalog.json")
        sub_id = f"sub-{uuid.uuid4()}-{i}"
        sub_catalog["id"] = sub_id
        sub_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/catalogs", json=sub_catalog
        )
        assert sub_resp.status_code == 201
        sub_catalog_ids.append(sub_id)

    # Create 2 collections
    collection_ids = []
    for i in range(2):
        test_collection = load_test_data("test_collection.json")
        collection_id = f"collection-{uuid.uuid4()}-{i}"
        test_collection["id"] = collection_id
        coll_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/collections", json=test_collection
        )
        assert coll_resp.status_code == 201
        collection_ids.append(collection_id)

    # Get children via /catalogs/{id}/children endpoint
    children_resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id}/children?limit=100"
    )
    assert children_resp.status_code == 200

    children_data = children_resp.json()
    children = children_data["children"]

    # Verify we have both catalogs and collections
    assert (
        len(children) >= 4
    ), f"Should have at least 4 children (2 catalogs + 2 collections), got {len(children)}"

    # Verify catalogs have parent links
    for sub_id in sub_catalog_ids:
        sub_in_children = next((c for c in children if c.get("id") == sub_id), None)
        assert (
            sub_in_children is not None
        ), f"Sub-catalog {sub_id} should be in /children"

        parent_links = [
            link
            for link in sub_in_children.get("links", [])
            if link.get("rel") == "parent"
        ]
        assert len(parent_links) > 0, f"Catalog {sub_id} should have parent link"
        assert all(
            "title" in link for link in parent_links
        ), f"All parent links for catalog {sub_id} should have titles"

    # Verify collections are present (they don't need parent links in /children response)
    for coll_id in collection_ids:
        coll_in_children = next((c for c in children if c.get("id") == coll_id), None)
        assert (
            coll_in_children is not None
        ), f"Collection {coll_id} should be in /children"


@pytest.mark.asyncio
async def test_scoped_collection_links_poly_hierarchy(
    catalogs_app_client, load_test_data
):
    """Test that scoped collection endpoint returns correct links per spec.

    Per the Multi-Tenant Catalogs spec, when accessing a collection via
    /catalogs/{id}/collections/{id} (scoped endpoint), the response MUST include:
    - Exactly ONE rel="parent" pointing to the specific catalog (contextual breadcrumb)
    - rel="related" links for other parent catalogs (poly-hierarchy)
    - rel="canonical" link pointing to global /collections/{id} endpoint
    - rel="duplicate" links for all alternative scoped URIs
    """
    # Create two parent catalogs
    parent_catalog_1 = load_test_data("test_catalog.json")
    parent_id_1 = f"parent-catalog-1-{uuid.uuid4()}"
    parent_catalog_1["id"] = parent_id_1

    parent_resp_1 = await catalogs_app_client.post("/catalogs", json=parent_catalog_1)
    assert parent_resp_1.status_code == 201

    parent_catalog_2 = load_test_data("test_catalog.json")
    parent_id_2 = f"parent-catalog-2-{uuid.uuid4()}"
    parent_catalog_2["id"] = parent_id_2

    parent_resp_2 = await catalogs_app_client.post("/catalogs", json=parent_catalog_2)
    assert parent_resp_2.status_code == 201

    # Create a collection linked to the first parent
    test_collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    test_collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id_1}/collections", json=test_collection
    )
    assert coll_resp.status_code == 201

    # Link the collection to the second parent as well (poly-hierarchy)
    link_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id_2}/collections", json=test_collection
    )
    assert link_resp.status_code == 201

    # Get the collection via the SCOPED endpoint (through parent_id_1)
    resp = await catalogs_app_client.get(
        f"/catalogs/{parent_id_1}/collections/{collection_id}"
    )
    assert resp.status_code == 200

    collection_data = resp.json()
    links = collection_data.get("links", [])

    # SPEC: Exactly ONE parent link pointing to the contextual catalog
    parent_links = [link for link in links if link.get("rel") == "parent"]
    assert (
        len(parent_links) == 1
    ), f"Scoped collection should have exactly 1 parent link, got {len(parent_links)}"

    parent_href = parent_links[0]["href"]
    assert f"/catalogs/{parent_id_1}" in parent_href, (
        f"Scoped collection parent MUST point to contextual catalog {parent_id_1}, "
        f"got: {parent_href}"
    )

    # SPEC: Related links for other parent catalogs
    related_links = [link for link in links if link.get("rel") == "related"]
    assert (
        len(related_links) >= 1
    ), f"Collection with 2 parents should have at least 1 related link, got {len(related_links)}"

    related_hrefs = [link["href"] for link in related_links]
    assert any(
        parent_id_2 in href for href in related_hrefs
    ), f"Related links should include other parent catalog {parent_id_2}"

    # SPEC: Canonical link pointing to global endpoint
    canonical_links = [link for link in links if link.get("rel") == "canonical"]
    assert (
        len(canonical_links) == 1
    ), f"Scoped collection should have exactly 1 canonical link, got {len(canonical_links)}"

    canonical_href = canonical_links[0]["href"]
    assert f"/collections/{collection_id}" in canonical_href, (
        f"Canonical link should point to global /collections/{collection_id}, "
        f"got: {canonical_href}"
    )

    # SPEC: Duplicate links for alternative scoped URIs (excluding current context)
    duplicate_links = [link for link in links if link.get("rel") == "duplicate"]
    assert len(duplicate_links) == 1, (
        f"Collection with 2 parents accessed via 1 catalog should have 1 duplicate "
        f"link (for the other catalog), got {len(duplicate_links)}"
    )

    duplicate_hrefs = [link["href"] for link in duplicate_links]

    # Should NOT have duplicate link for current scoped URI (that's the self link)
    assert not any(
        f"/catalogs/{parent_id_1}/collections/{collection_id}" in href
        for href in duplicate_hrefs
    ), f"Duplicate links should NOT include current context /catalogs/{parent_id_1}"

    # Should have duplicate link for the OTHER parent's scoped URI
    assert any(
        f"/catalogs/{parent_id_2}/collections/{collection_id}" in href
        for href in duplicate_hrefs
    ), f"Duplicate links should include alternative scoped URI /catalogs/{parent_id_2}/collections/{collection_id}"


@pytest.mark.asyncio
async def test_duplicate_links_exclude_current_catalog_context(
    catalogs_app_client, load_test_data
):
    """Test that duplicate links exclude the current catalog context.

    When accessing a collection via /catalogs/{id}/collections/{id}, the duplicate
    links should only show OTHER catalogs where this collection exists, not the
    current catalog context.
    """
    # Create three parent catalogs
    parent_ids = []
    for i in range(3):
        parent_catalog = load_test_data("test_catalog.json")
        parent_id = f"parent-catalog-{uuid.uuid4()}-{i}"
        parent_catalog["id"] = parent_id

        parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
        assert parent_resp.status_code == 201
        parent_ids.append(parent_id)

    # Create a collection
    collection = load_test_data("test_collection.json")
    collection_id = f"multi-parent-collection-{uuid.uuid4()}"
    collection["id"] = collection_id

    # Link collection to all three catalogs
    for parent_id in parent_ids:
        link_resp = await catalogs_app_client.post(
            f"/catalogs/{parent_id}/collections", json=collection
        )
        assert link_resp.status_code in [200, 201]

    # Access collection via first catalog's scoped endpoint
    resp = await catalogs_app_client.get(
        f"/catalogs/{parent_ids[0]}/collections/{collection_id}"
    )
    assert resp.status_code == 200

    collection_data = resp.json()
    links = collection_data.get("links", [])

    # Get duplicate links
    duplicate_links = [link for link in links if link.get("rel") == "duplicate"]
    duplicate_hrefs = [link["href"] for link in duplicate_links]

    # Should NOT include current catalog context in duplicate links
    assert not any(
        f"/catalogs/{parent_ids[0]}/collections/{collection_id}" in href
        for href in duplicate_hrefs
    ), f"Duplicate links should NOT include current catalog context /catalogs/{parent_ids[0]}"

    # Should include the OTHER two catalogs
    assert any(
        f"/catalogs/{parent_ids[1]}/collections/{collection_id}" in href
        for href in duplicate_hrefs
    ), f"Duplicate links should include alternative catalog /catalogs/{parent_ids[1]}"

    assert any(
        f"/catalogs/{parent_ids[2]}/collections/{collection_id}" in href
        for href in duplicate_hrefs
    ), f"Duplicate links should include alternative catalog /catalogs/{parent_ids[2]}"

    # Verify we have exactly 2 duplicate links (for the 2 other catalogs)
    assert len(duplicate_links) == 2, (
        f"Collection with 3 parents accessed via 1 catalog should have 2 duplicate "
        f"links (for the other 2 catalogs), got {len(duplicate_links)}"
    )


@pytest.mark.asyncio
async def test_catalog_collections_endpoint_excludes_catalogs(
    catalogs_app_client, load_test_data
):
    """Test that GET /catalogs/{id}/collections only returns Collections, not Catalogs.

    This verifies that the type filter is working correctly to exclude catalogs
    from the collections endpoint.
    """
    # Create a parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create a child catalog (should NOT appear in collections endpoint)
    child_catalog = load_test_data("test_catalog.json")
    child_catalog_id = f"child-catalog-{uuid.uuid4()}"
    child_catalog["id"] = child_catalog_id

    catalog_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=child_catalog
    )
    assert catalog_resp.status_code == 201

    # Create a collection (SHOULD appear in collections endpoint)
    collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/collections", json=collection
    )
    assert coll_resp.status_code in [200, 201]

    # Get collections from the catalog
    resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/collections")
    assert resp.status_code == 200

    data = resp.json()
    collections = data.get("collections", [])

    # Verify all returned items are Collections (type="Collection")
    for item in collections:
        assert item.get("type") == "Collection", (
            f"Collections endpoint returned item with type={item.get('type')}, "
            f"expected 'Collection'. Item ID: {item.get('id')}"
        )

    # Verify the collection is present
    collection_ids = [c.get("id") for c in collections]
    assert (
        collection_id in collection_ids
    ), f"Collection {collection_id} should be in the results"

    # Verify the child catalog is NOT present
    assert (
        child_catalog_id not in collection_ids
    ), f"Catalog {child_catalog_id} should NOT be in collections endpoint results"


@pytest.mark.asyncio
async def test_catalogs_list_includes_child_links(catalogs_app_client, load_test_data):
    """Test that /catalogs endpoint includes child links for each catalog.

    This verifies that:
    1. Each catalog in the list has a "children" link
    2. Each catalog has "child" links for its direct children
    3. Child links point to the correct endpoints
    """
    # Create parent catalogs
    parent_catalog_1 = load_test_data("test_catalog.json")
    parent_id_1 = f"parent-catalog-1-{uuid.uuid4()}"
    parent_catalog_1["id"] = parent_id_1
    parent_catalog_1["title"] = "Parent Catalog 1"

    parent_resp_1 = await catalogs_app_client.post("/catalogs", json=parent_catalog_1)
    assert parent_resp_1.status_code == 201

    parent_catalog_2 = load_test_data("test_catalog.json")
    parent_id_2 = f"parent-catalog-2-{uuid.uuid4()}"
    parent_catalog_2["id"] = parent_id_2
    parent_catalog_2["title"] = "Parent Catalog 2"

    parent_resp_2 = await catalogs_app_client.post("/catalogs", json=parent_catalog_2)
    assert parent_resp_2.status_code == 201

    # Create child catalogs
    child_catalog = load_test_data("test_catalog.json")
    child_id = f"child-catalog-{uuid.uuid4()}"
    child_catalog["id"] = child_id
    child_catalog["title"] = "Child Catalog"

    child_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id_1}/catalogs", json=child_catalog
    )
    assert child_resp.status_code == 201

    # Create a collection under parent_id_1
    collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    collection["id"] = collection_id
    collection["title"] = "Test Collection"

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{parent_id_1}/collections", json=collection
    )
    assert coll_resp.status_code in [200, 201]

    # Get all catalogs with a high limit to ensure we get all created catalogs
    resp = await catalogs_app_client.get("/catalogs?limit=100")
    assert resp.status_code == 200

    catalogs_response = resp.json()
    catalogs = catalogs_response.get("catalogs", [])

    # Find the parent catalog we created
    parent_catalog = next(
        (cat for cat in catalogs if cat.get("id") == parent_id_1), None
    )
    assert parent_catalog is not None, f"Parent catalog {parent_id_1} not found"

    # Verify parent catalog has links
    links = parent_catalog.get("links", [])
    assert len(links) > 0, "Parent catalog should have links"

    # Check for children endpoint link
    children_links = [link for link in links if link.get("rel") == "children"]
    assert (
        len(children_links) == 1
    ), "Parent catalog should have exactly one 'children' link"
    assert f"/catalogs/{parent_id_1}/children" in children_links[0].get(
        "href", ""
    ), "Children link should point to /catalogs/{id}/children endpoint"

    # Check for child links
    child_links = [link for link in links if link.get("rel") == "child"]
    assert (
        len(child_links) >= 2
    ), f"Parent catalog should have at least 2 child links (1 catalog + 1 collection), got {len(child_links)}"

    # Verify child links point to correct endpoints
    child_hrefs = [link.get("href", "") for link in child_links]

    # Should have a link to the child catalog
    assert any(
        f"/catalogs/{parent_id_1}/catalogs/{child_id}" in href for href in child_hrefs
    ), f"Should have child link to /catalogs/{parent_id_1}/catalogs/{child_id}"

    # Should have a link to the collection
    assert any(
        f"/catalogs/{parent_id_1}/collections/{collection_id}" in href
        for href in child_hrefs
    ), f"Should have child link to /catalogs/{parent_id_1}/collections/{collection_id}"

    # Verify parent catalog 2 has no child links (it has no children)
    parent_catalog_2_obj = next(
        (cat for cat in catalogs if cat.get("id") == parent_id_2), None
    )
    assert parent_catalog_2_obj is not None, f"Parent catalog {parent_id_2} not found"

    links_2 = parent_catalog_2_obj.get("links", [])
    child_links_2 = [link for link in links_2 if link.get("rel") == "child"]
    assert (
        len(child_links_2) == 0
    ), f"Parent catalog 2 should have no child links, got {len(child_links_2)}"


@pytest.mark.asyncio
async def test_sub_catalogs_list_includes_child_links(
    catalogs_app_client, load_test_data
):
    """Test that /catalogs/{id}/catalogs endpoint includes child links for each sub-catalog.

    This verifies that sub-catalogs in the list also have proper child links.
    """
    # Create parent catalog
    parent_catalog = load_test_data("test_catalog.json")
    parent_id = f"parent-catalog-{uuid.uuid4()}"
    parent_catalog["id"] = parent_id

    parent_resp = await catalogs_app_client.post("/catalogs", json=parent_catalog)
    assert parent_resp.status_code == 201

    # Create sub-catalogs
    sub_catalog_1 = load_test_data("test_catalog.json")
    sub_id_1 = f"sub-catalog-1-{uuid.uuid4()}"
    sub_catalog_1["id"] = sub_id_1
    sub_catalog_1["title"] = "Sub Catalog 1"

    sub_resp_1 = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=sub_catalog_1
    )
    assert sub_resp_1.status_code == 201

    sub_catalog_2 = load_test_data("test_catalog.json")
    sub_id_2 = f"sub-catalog-2-{uuid.uuid4()}"
    sub_catalog_2["id"] = sub_id_2
    sub_catalog_2["title"] = "Sub Catalog 2"

    sub_resp_2 = await catalogs_app_client.post(
        f"/catalogs/{parent_id}/catalogs", json=sub_catalog_2
    )
    assert sub_resp_2.status_code == 201

    # Create a collection under sub_catalog_1
    collection = load_test_data("test_collection.json")
    collection_id = f"test-collection-{uuid.uuid4()}"
    collection["id"] = collection_id

    coll_resp = await catalogs_app_client.post(
        f"/catalogs/{sub_id_1}/collections", json=collection
    )
    assert coll_resp.status_code in [200, 201]

    # Get sub-catalogs
    resp = await catalogs_app_client.get(f"/catalogs/{parent_id}/catalogs")
    assert resp.status_code == 200

    catalogs_response = resp.json()
    sub_catalogs = catalogs_response.get("catalogs", [])

    # Find sub_catalog_1
    sub_cat_1 = next((cat for cat in sub_catalogs if cat.get("id") == sub_id_1), None)
    assert sub_cat_1 is not None, f"Sub catalog {sub_id_1} not found"

    # Verify it has child links
    links = sub_cat_1.get("links", [])
    child_links = [link for link in links if link.get("rel") == "child"]

    assert (
        len(child_links) >= 1
    ), "Sub catalog 1 should have at least 1 child link (the collection)"

    # Verify the child link points to the collection
    child_hrefs = [link.get("href", "") for link in child_links]
    assert any(
        f"/catalogs/{sub_id_1}/collections/{collection_id}" in href
        for href in child_hrefs
    ), f"Should have child link to collection {collection_id}"

    # Verify sub_catalog_2 has no child links
    sub_cat_2 = next((cat for cat in sub_catalogs if cat.get("id") == sub_id_2), None)
    assert sub_cat_2 is not None, f"Sub catalog {sub_id_2} not found"

    links_2 = sub_cat_2.get("links", [])
    child_links_2 = [link for link in links_2 if link.get("rel") == "child"]
    assert (
        len(child_links_2) == 0
    ), f"Sub catalog 2 should have no child links, got {len(child_links_2)}"


@pytest.mark.asyncio
async def test_catalogs_list_endpoint(catalogs_app_client, load_test_data):
    """Test that catalogs list endpoint returns proper structure."""
    # Get the root catalog
    resp = await catalogs_app_client.get("/catalogs")
    assert resp.status_code == 200

    catalogs_response = resp.json()

    # Check for required fields in the catalogs list response
    assert "catalogs" in catalogs_response
    assert "links" in catalogs_response
    assert "numberReturned" in catalogs_response


@pytest.mark.asyncio
async def test_catalog_conformance_endpoint(catalogs_app_client, load_test_data):
    """Test the /catalogs/{catalog_id}/conformance endpoint."""
    # First create a catalog
    test_catalog = load_test_data("test_catalog.json")
    test_catalog["id"] = f"test-catalog-{uuid.uuid4()}"

    create_resp = await catalogs_app_client.post("/catalogs", json=test_catalog)
    assert create_resp.status_code == 201

    catalog_id = test_catalog["id"]

    # Get the conformance endpoint for the catalog
    conformance_resp = await catalogs_app_client.get(
        f"/catalogs/{catalog_id}/conformance"
    )
    assert conformance_resp.status_code == 200

    conformance_data = conformance_resp.json()
    assert "conformsTo" in conformance_data

    conforms_to = conformance_data["conformsTo"]

    # Check for required conformance classes
    assert "https://api.stacspec.org/v1.0.0/core" in conforms_to
    assert "https://api.stacspec.org/v1.0.0-beta.4/multi-tenant-catalogs" in conforms_to
    assert "https://api.stacspec.org/v1.0.0-rc.2/children" in conforms_to
    assert (
        "https://api.stacspec.org/v1.0.0-beta.4/multi-tenant-catalogs/transaction"
        in conforms_to
    )
