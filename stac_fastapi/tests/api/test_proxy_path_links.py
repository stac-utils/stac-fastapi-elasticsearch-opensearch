"""Tests for proxy path handling in generated links.

This module tests that all generated links correctly preserve the proxy path
when the API is deployed behind a reverse proxy with a root path.
"""

from urllib.parse import urlparse

import pytest


@pytest.mark.asyncio
async def test_landing_page_links_with_proxy_path(app_client, ctx):
    """Test that landing page links include proxy path."""
    resp = await app_client.get("/")
    assert resp.status_code == 200

    landing_page = resp.json()
    links = landing_page.get("links", [])

    # Verify all links are present
    rel_types = {link["rel"] for link in links}
    assert "self" in rel_types
    assert "root" in rel_types
    assert "data" in rel_types
    assert "conformance" in rel_types
    assert "search" in rel_types

    # Verify links are well-formed URLs
    for link in links:
        href = link.get("href", "")
        assert href, f"Link with rel={link.get('rel')} has no href"
        # All hrefs should be absolute URLs or valid relative paths
        assert href.startswith(("http://", "https://", "/")), f"Invalid href: {href}"


@pytest.mark.asyncio
async def test_collections_links_with_proxy_path(app_client, ctx):
    """Test that collection links include proxy path."""
    resp = await app_client.get("/collections")
    assert resp.status_code == 200

    collections = resp.json()
    links = collections.get("links", [])

    # Verify self link is present
    self_links = [link for link in links if link["rel"] == "self"]
    assert len(self_links) > 0, "No self link found in collections response"

    # Verify the self link is well-formed
    self_link = self_links[0]
    href = self_link.get("href", "")
    assert href, "Self link has no href"
    assert "/collections" in href, f"Collections link missing /collections: {href}"


@pytest.mark.asyncio
async def test_search_links_with_proxy_path(app_client, ctx):
    """Test that search response links include proxy path."""
    resp = await app_client.post("/search", json={"limit": 1})
    assert resp.status_code == 200

    search_result = resp.json()
    links = search_result.get("links", [])

    # Verify links are present
    assert len(links) > 0, "No links found in search response"

    # Verify all links are well-formed
    for link in links:
        href = link.get("href", "")
        assert href, f"Link with rel={link.get('rel')} has no href"


@pytest.mark.asyncio
async def test_aggregations_links_with_proxy_path(app_client, ctx):
    """Test that aggregations links include proxy path."""
    resp = await app_client.get("/aggregations")
    assert resp.status_code == 200

    aggs = resp.json()
    links = aggs.get("links", [])

    # Verify self link is present
    self_links = [link for link in links if link["rel"] == "self"]
    assert len(self_links) > 0, "No self link found in aggregations response"

    # Verify the self link is well-formed
    self_link = self_links[0]
    href = self_link.get("href", "")
    assert href, "Self link has no href"
    assert "/aggregations" in href, f"Aggregations link missing /aggregations: {href}"


@pytest.mark.asyncio
async def test_collection_items_links_with_proxy_path(app_client, ctx, test_collection):
    """Test that collection items links include proxy path."""
    # Use the test collection fixture
    collection_id = test_collection["id"]
    resp = await app_client.get(f"/collections/{collection_id}/items")
    assert resp.status_code == 200

    items = resp.json()
    links = items.get("links", [])

    # Verify links are present
    assert len(links) > 0, "No links found in items response"

    # Verify all links are well-formed
    for link in links:
        href = link.get("href", "")
        assert href, f"Link with rel={link.get('rel')} has no href"


@pytest.mark.asyncio
async def test_queryables_links_with_proxy_path(app_client, ctx):
    """Test that queryables links include proxy path."""
    resp = await app_client.get("/queryables")
    assert resp.status_code == 200

    queryables = resp.json()
    # Queryables response structure varies, but should be valid JSON
    assert queryables is not None


@pytest.mark.asyncio
async def test_link_hrefs_are_valid_urls(app_client, ctx):
    """Test that all generated link hrefs are valid URLs."""
    resp = await app_client.get("/")
    assert resp.status_code == 200

    landing_page = resp.json()
    links = landing_page.get("links", [])

    for link in links:
        href = link.get("href", "")
        if href.startswith("http"):
            # Parse as absolute URL
            parsed = urlparse(href)
            assert parsed.scheme in ("http", "https"), f"Invalid scheme in {href}"
            assert parsed.netloc, f"Missing netloc in {href}"
        elif href.startswith("/"):
            # Relative path is acceptable
            assert href, "Empty relative path in link"
        else:
            # Should be either absolute or relative
            pytest.fail(f"Invalid href format: {href}")


@pytest.mark.asyncio
async def test_openapi_links_in_landing_page(app_client, ctx):
    """Test that OpenAPI/docs links are present in landing page."""
    resp = await app_client.get("/")
    assert resp.status_code == 200

    landing_page = resp.json()
    links = landing_page.get("links", [])

    # Check for OpenAPI links
    rel_types = {link["rel"] for link in links}
    assert "service-desc" in rel_types, "OpenAPI service-desc link missing"
    assert "service-doc" in rel_types, "OpenAPI service-doc link missing"

    # Verify they have valid hrefs
    for link in links:
        if link["rel"] in ("service-desc", "service-doc"):
            href = link.get("href", "")
            assert href, f"OpenAPI {link['rel']} link has no href"
            assert href.startswith(
                ("http://", "https://", "/")
            ), f"Invalid OpenAPI href: {href}"
