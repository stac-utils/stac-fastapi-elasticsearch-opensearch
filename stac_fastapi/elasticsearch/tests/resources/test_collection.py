import pystac


async def test_create_and_delete_collection(app_client, load_test_data):
    """Test creation and deletion of a collection"""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "test"

    resp = await app_client.post("/collections", json=test_collection)
    assert resp.status_code == 200

    resp = await app_client.delete(f"/collections/{test_collection['id']}")
    assert resp.status_code == 204


async def test_create_collection_conflict(app_client, ctx):
    """Test creation of a collection which already exists"""
    # This collection ID is created in the fixture, so this should be a conflict
    resp = await app_client.post("/collections", json=ctx.collection)
    assert resp.status_code == 409


async def test_delete_missing_collection(app_client):
    """Test deletion of a collection which does not exist"""
    resp = await app_client.delete("/collections/missing-collection")
    assert resp.status_code == 404


async def test_update_collection_already_exists(ctx, app_client):
    """Test updating a collection which already exists"""
    ctx.collection["keywords"].append("test")
    resp = await app_client.put("/collections", json=ctx.collection)
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{ctx.collection['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert "test" in resp_json["keywords"]


async def test_update_new_collection(app_client, load_test_data):
    """Test updating a collection which does not exist (same as creation)"""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = "new-test-collection"

    resp = await app_client.put("/collections", json=test_collection)
    assert resp.status_code == 404


async def test_collection_not_found(app_client):
    """Test read a collection which does not exist"""
    resp = await app_client.get("/collections/does-not-exist")
    assert resp.status_code == 404


async def test_returns_valid_collection(ctx, app_client):
    """Test validates fetched collection with jsonschema"""
    resp = await app_client.put("/collections", json=ctx.collection)
    assert resp.status_code == 200

    resp = await app_client.get(f"/collections/{ctx.collection['id']}")
    assert resp.status_code == 200
    resp_json = resp.json()

    # Mock root to allow validation
    mock_root = pystac.Catalog(
        id="test", description="test desc", href="https://example.com"
    )
    collection = pystac.Collection.from_dict(
        resp_json, root=mock_root, preserve_dict=False
    )
    collection.validate()
