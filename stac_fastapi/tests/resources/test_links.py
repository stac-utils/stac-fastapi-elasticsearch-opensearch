"""Tests for catalog and collection link handling.

These tests ensure that:
1. Absolute URLs in custom links are not mangled
2. Relative links are properly resolved
3. Dynamic links (self, parent, root) are correctly generated
4. Multiple levels of URL mangling are fixed
"""

import copy
import uuid

import pytest

from ..conftest import create_collection, create_item, delete_collections_and_items


@pytest.mark.asyncio
async def test_collection_custom_absolute_links(app_client, ctx, txn_client):
    """Test that absolute URLs in custom collection links are not mangled."""
    await delete_collections_and_items(txn_client)

    collection = copy.deepcopy(ctx.collection)
    collection["id"] = f"test-collection-{uuid.uuid4()}"

    # Add custom absolute URL links
    custom_links = [
        {
            "href": "https://sentinel.esa.int/documents/247904/690755/Sentinel_Data_Legal_Notice",
            "rel": "license",
            "type": "text/html",
        },
        {
            "href": "https://github.com/stac-utils/stac-sentinel",
            "rel": "about",
            "type": "text/html",
        },
        {
            "href": "https://example.com/documentation",
            "rel": "documentation",
            "type": "text/html",
        },
    ]
    collection["links"].extend(custom_links)

    await create_collection(txn_client, collection=collection)

    # Retrieve the collection
    response = await app_client.get(f"/collections/{collection['id']}")
    assert response.status_code == 200

    collection_data = response.json()
    links = collection_data["links"]

    # Verify custom links are present and not mangled
    for custom_link in custom_links:
        matching_links = [
            link
            for link in links
            if link["rel"] == custom_link["rel"]
            and link.get("href") == custom_link["href"]
        ]
        assert (
            len(matching_links) == 1
        ), f"Custom link {custom_link['rel']} not found or mangled. Got: {[link for link in links if link['rel'] == custom_link['rel']]}"


@pytest.mark.asyncio
async def test_collection_links_no_base_url_prefix(app_client, ctx, txn_client):
    """Test that absolute custom links don't have base URL prepended."""
    await delete_collections_and_items(txn_client)

    collection = copy.deepcopy(ctx.collection)
    collection["id"] = f"test-collection-{uuid.uuid4()}"

    # Add custom absolute URL links
    absolute_url = "https://example.com/resource"
    collection["links"].append(
        {
            "href": absolute_url,
            "rel": "custom",
            "type": "application/json",
        }
    )

    await create_collection(txn_client, collection=collection)

    response = await app_client.get(f"/collections/{collection['id']}")
    assert response.status_code == 200

    collection_data = response.json()
    custom_link = next(
        (link for link in collection_data["links"] if link["rel"] == "custom"), None
    )

    assert custom_link is not None, "Custom link not found"
    assert (
        custom_link["href"] == absolute_url
    ), f"Expected {absolute_url}, got {custom_link['href']}"

    # Ensure it doesn't start with base URL
    assert not custom_link["href"].startswith(
        "http://localhost:8080/http"
    ), "URL was mangled with base URL prefix"


@pytest.mark.asyncio
async def test_collection_dynamic_links_have_base_url(app_client, ctx, txn_client):
    """Test that dynamic links (self, parent, root) have the correct base URL."""
    await delete_collections_and_items(txn_client)

    collection = copy.deepcopy(ctx.collection)
    collection["id"] = f"test-collection-{uuid.uuid4()}"

    await create_collection(txn_client, collection=collection)

    response = await app_client.get(f"/collections/{collection['id']}")
    assert response.status_code == 200

    collection_data = response.json()
    links = collection_data["links"]

    # Check dynamic links
    self_link = next((link for link in links if link["rel"] == "self"), None)
    parent_link = next((link for link in links if link["rel"] == "parent"), None)
    root_link = next((link for link in links if link["rel"] == "root"), None)

    assert self_link is not None, "Self link not found"
    assert parent_link is not None, "Parent link not found"
    assert root_link is not None, "Root link not found"

    # Verify they have the correct base URL (test server uses http://test-server)
    assert self_link["href"] == f"http://test-server/collections/{collection['id']}"
    assert parent_link["href"] == "http://test-server"
    assert root_link["href"] == "http://test-server/"


@pytest.mark.asyncio
async def test_item_custom_absolute_links(app_client, ctx, txn_client):
    """Test that absolute URLs in custom item links are not mangled."""
    await delete_collections_and_items(txn_client)

    await create_collection(txn_client, collection=ctx.collection)

    item = copy.deepcopy(ctx.item)
    item["id"] = f"test-item-{uuid.uuid4()}"

    # Add custom absolute URL links
    custom_links = [
        {
            "href": "https://example.com/wms",
            "rel": "wms",
            "type": "image/png",
        },
        {
            "href": "https://example.com/data",
            "rel": "data",
            "type": "application/octet-stream",
        },
    ]
    item["links"].extend(custom_links)

    await create_item(txn_client, item=item)

    # Retrieve the item
    response = await app_client.get(
        f"/collections/{ctx.collection['id']}/items/{item['id']}"
    )
    assert response.status_code == 200

    item_data = response.json()
    links = item_data["links"]

    # Verify custom links are present and not mangled
    for custom_link in custom_links:
        matching_links = [
            link
            for link in links
            if link["rel"] == custom_link["rel"]
            and link.get("href") == custom_link["href"]
        ]
        assert (
            len(matching_links) == 1
        ), f"Custom link {custom_link['rel']} not found or mangled"


@pytest.mark.asyncio
async def test_collection_links_no_double_mangling(app_client, ctx, txn_client):
    """Test that URLs are not mangled multiple times.

    This test catches the case where a URL like:
    https://example.com -> http://localhost:8080/https://example.com
    -> http://localhost:8080/http://localhost:8080/https://example.com
    """
    await delete_collections_and_items(txn_client)

    collection = copy.deepcopy(ctx.collection)
    collection["id"] = f"test-collection-{uuid.uuid4()}"

    # Add a custom absolute URL
    absolute_url = "https://example.com/resource"
    collection["links"].append(
        {
            "href": absolute_url,
            "rel": "custom",
            "type": "application/json",
        }
    )

    await create_collection(txn_client, collection=collection)

    # Retrieve multiple times to ensure no progressive mangling
    for _ in range(3):
        response = await app_client.get(f"/collections/{collection['id']}")
        assert response.status_code == 200

        collection_data = response.json()
        custom_link = next(
            (link for link in collection_data["links"] if link["rel"] == "custom"),
            None,
        )

        assert custom_link is not None, "Custom link not found"
        assert (
            custom_link["href"] == absolute_url
        ), f"URL mangled on retrieval. Expected {absolute_url}, got {custom_link['href']}"

        # Count how many times the base URL appears in the href
        base_url_count = custom_link["href"].count("http://localhost:8080/")
        assert (
            base_url_count == 0
        ), f"Base URL appears {base_url_count} times in href: {custom_link['href']}"


@pytest.mark.asyncio
async def test_collection_links_mixed_absolute_and_relative(
    app_client, ctx, txn_client
):
    """Test that collections can have both absolute and relative custom links."""
    await delete_collections_and_items(txn_client)

    collection = copy.deepcopy(ctx.collection)
    collection["id"] = f"test-collection-{uuid.uuid4()}"

    # Add mixed links
    collection["links"].extend(
        [
            {
                "href": "https://example.com/license",
                "rel": "license",
                "type": "text/html",
            },
            {
                "href": "/relative/path",
                "rel": "alternate",
                "type": "text/html",
            },
        ]
    )

    await create_collection(txn_client, collection=collection)

    response = await app_client.get(f"/collections/{collection['id']}")
    assert response.status_code == 200

    collection_data = response.json()
    links = collection_data["links"]

    # Verify absolute link
    license_link = next((link for link in links if link["rel"] == "license"), None)
    assert license_link is not None
    assert license_link["href"] == "https://example.com/license"

    # Verify relative link is present (stored as-is, not resolved)
    alternate_link = next((link for link in links if link["rel"] == "alternate"), None)
    assert alternate_link is not None
    # Relative links are stored as-is in the database
    assert alternate_link["href"] == "/relative/path"
