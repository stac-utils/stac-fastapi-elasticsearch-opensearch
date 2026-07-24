import uuid

import pytest

from ..conftest import create_collection, create_item


@pytest.mark.asyncio
async def test_search_pagination_uses_redis_cache(
    app_client, txn_client, load_test_data
):
    """Test Redis caching and navigation for the /search endpoint."""

    collection = load_test_data("test_collection.json")
    collection_id = f"test-pagination-collection-{uuid.uuid4()}"
    collection["id"] = collection_id
    await create_collection(txn_client, collection)

    for i in range(5):
        item = load_test_data("test_item.json")
        item["id"] = f"test-pagination-item-{uuid.uuid4()}"
        item["collection"] = collection_id
        await create_item(txn_client, item)

    resp = await app_client.get(f"/collections/{collection_id}/items?limit=1")
    resp_json = resp.json()

    next_link = next(link for link in resp_json["links"] if link["rel"] == "next")
    next_url = next_link["href"]

    resp2 = await app_client.get(next_url)
    resp2_json = resp2.json()

    prev_link = next(
        (link for link in resp2_json["links"] if link["rel"] == "previous"), None
    )
    assert prev_link is not None


@pytest.mark.asyncio
async def test_collections_pagination_uses_redis_cache(
    app_client, txn_client, load_test_data
):
    """Test Redis caching and navigation for the /collection endpoint."""

    collection_data = load_test_data("test_collection.json")
    for i in range(5):
        collection = collection_data.copy()
        collection["id"] = f"test-collection-pagination-{uuid.uuid4()}"
        collection["title"] = f"Test Collection Pagination {i}"
        await create_collection(txn_client, collection)

    resp = await app_client.get("/collections", params={"limit": 1})
    assert resp.status_code == 200
    resp1_json = resp.json()

    next_link = next(
        (link for link in resp1_json["links"] if link["rel"] == "next"), None
    )
    next_token = next_link["href"].split("token=")[1]

    resp2 = await app_client.get(
        "/collections", params={"limit": 1, "token": next_token}
    )
    assert resp2.status_code == 200
    resp2_json = resp2.json()

    prev_link = next(
        (link for link in resp2_json["links"] if link["rel"] == "previous"), None
    )
    assert prev_link is not None


@pytest.mark.asyncio
async def test_search_post_pagination_preserves_body_links(
    app_client, txn_client, load_test_data
):
    """Test POST /search pagination keeps self/previous link bodies."""

    collection = load_test_data("test_collection.json")
    collection_id = f"test-pagination-post-collection-{uuid.uuid4()}"
    collection["id"] = collection_id
    await create_collection(txn_client, collection)

    for i in range(5):
        item = load_test_data("test_item.json")
        item["id"] = f"test-pagination-post-item-{uuid.uuid4()}"
        item["collection"] = collection_id
        await create_item(txn_client, item)

    first_body = {"collections": [collection_id], "limit": 1}
    resp = await app_client.post("/search", json=first_body)
    resp_json = resp.json()

    self_link = next(link for link in resp_json["links"] if link["rel"] == "self")
    assert self_link["method"] == "POST"
    assert self_link["body"] == first_body

    next_link = next(link for link in resp_json["links"] if link["rel"] == "next")
    assert next_link["method"] == "POST"
    assert next_link["body"]["token"]

    resp2 = await app_client.post(next_link["href"], json=next_link["body"])
    resp2_json = resp2.json()

    self_link2 = next(link for link in resp2_json["links"] if link["rel"] == "self")
    assert self_link2["method"] == "POST"
    assert self_link2["body"] == next_link["body"]

    prev_link = next(
        (link for link in resp2_json["links"] if link["rel"] == "previous"), None
    )
    assert prev_link is not None
    assert prev_link["method"] == "POST"
    assert prev_link["body"] == first_body


@pytest.mark.asyncio
async def test_search_post_empty_body_previous_link_keeps_post_token(
    app_client, txn_client, load_test_data
):
    """Test POST /search empty-body pagination keeps previous as POST by page 3."""

    collection = load_test_data("test_collection.json")
    collection_id = f"test-pagination-empty-post-collection-{uuid.uuid4()}"
    collection["id"] = collection_id
    await create_collection(txn_client, collection)

    for i in range(50):
        item = load_test_data("test_item.json")
        item["id"] = f"test-pagination-empty-post-item-{uuid.uuid4()}"
        item["collection"] = collection_id
        await create_item(txn_client, item)

    resp1 = await app_client.post("/search", json={})
    resp1_json = resp1.json()
    next_link1 = next(link for link in resp1_json["links"] if link["rel"] == "next")

    resp2 = await app_client.post(next_link1["href"], json=next_link1["body"])
    resp2_json = resp2.json()
    next_link2 = next(link for link in resp2_json["links"] if link["rel"] == "next")

    resp3 = await app_client.post(next_link2["href"], json=next_link2["body"])
    resp3_json = resp3.json()

    prev_link = next(
        (link for link in resp3_json["links"] if link["rel"] == "previous"), None
    )
    assert prev_link is not None
    assert prev_link["method"] == "POST"
    assert prev_link["href"].endswith("/search")
    assert prev_link["body"] == next_link1["body"]
    assert prev_link["body"]["token"]
