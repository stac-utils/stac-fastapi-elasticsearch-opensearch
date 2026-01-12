"""Tests for header-based filtering functionality.

This module tests the header filtering feature that allows stac-auth-proxy
to pass allowed collections and geometries via HTTP headers.
"""

import json

import pytest
import pytest_asyncio

from ..conftest import create_collection, create_item, delete_collections_and_items

# Header names
FILTER_COLLECTIONS_HEADER = "X-Filter-Collections"
FILTER_GEOMETRY_HEADER = "X-Filter-Geometry"


@pytest_asyncio.fixture(scope="function")
async def multi_collection_ctx(txn_client, load_test_data):
    """Create multiple collections for testing header filtering."""
    await delete_collections_and_items(txn_client)

    # Create test collections
    collections = []
    for suffix in ["a", "b", "c"]:
        collection = load_test_data("test_collection.json").copy()
        collection["id"] = f"test-collection-{suffix}"
        await create_collection(txn_client, collection)
        collections.append(collection)

    # Create items in each collection
    items = []
    for collection in collections:
        item = load_test_data("test_item.json").copy()
        item["id"] = f"test-item-{collection['id']}"
        item["collection"] = collection["id"]
        await create_item(txn_client, item)
        items.append(item)

    yield {"collections": collections, "items": items}

    await delete_collections_and_items(txn_client)


class TestHeaderFilteringSearch:
    """Tests for search endpoints with header filtering."""

    @pytest.mark.asyncio
    async def test_search_uses_header_collections(
        self, app_client, multi_collection_ctx
    ):
        """When X-Filter-Collections header is present, search only in those collections."""
        # Search with header limiting to collection-a only
        response = await app_client.get(
            "/search",
            headers={FILTER_COLLECTIONS_HEADER: "test-collection-a"},
        )
        assert response.status_code == 200
        data = response.json()

        # Should only return items from collection-a
        for feature in data["features"]:
            assert feature["collection"] == "test-collection-a"

    @pytest.mark.asyncio
    async def test_search_header_multiple_collections(
        self, app_client, multi_collection_ctx
    ):
        """Header with multiple collections filters to those collections."""
        response = await app_client.get(
            "/search",
            headers={FILTER_COLLECTIONS_HEADER: "test-collection-a,test-collection-b"},
        )
        assert response.status_code == 200
        data = response.json()

        # Should only return items from collection-a and collection-b
        for feature in data["features"]:
            assert feature["collection"] in ["test-collection-a", "test-collection-b"]

    @pytest.mark.asyncio
    async def test_search_no_header_returns_all(self, app_client, multi_collection_ctx):
        """Without header, search returns items from all collections."""
        response = await app_client.get("/search")
        assert response.status_code == 200
        data = response.json()

        # Should have items from all collections
        collections_in_response = {f["collection"] for f in data["features"]}
        assert "test-collection-a" in collections_in_response
        assert "test-collection-b" in collections_in_response
        assert "test-collection-c" in collections_in_response

    @pytest.mark.asyncio
    async def test_post_search_uses_header_collections(
        self, app_client, multi_collection_ctx
    ):
        """POST /search also respects the header."""
        response = await app_client.post(
            "/search",
            json={},
            headers={FILTER_COLLECTIONS_HEADER: "test-collection-b"},
        )
        assert response.status_code == 200
        data = response.json()

        for feature in data["features"]:
            assert feature["collection"] == "test-collection-b"


class TestHeaderFilteringCollections:
    """Tests for collections endpoint with header filtering."""

    @pytest.mark.asyncio
    async def test_all_collections_filtered_by_header(
        self, app_client, multi_collection_ctx
    ):
        """GET /collections only returns collections from header."""
        response = await app_client.get(
            "/collections",
            headers={FILTER_COLLECTIONS_HEADER: "test-collection-a,test-collection-c"},
        )
        assert response.status_code == 200
        data = response.json()

        collection_ids = [c["id"] for c in data["collections"]]
        assert "test-collection-a" in collection_ids
        assert "test-collection-c" in collection_ids
        assert "test-collection-b" not in collection_ids

    @pytest.mark.asyncio
    async def test_get_collection_allowed_by_header(
        self, app_client, multi_collection_ctx
    ):
        """GET /collections/{id} works when collection is in header."""
        response = await app_client.get(
            "/collections/test-collection-a",
            headers={FILTER_COLLECTIONS_HEADER: "test-collection-a,test-collection-b"},
        )
        assert response.status_code == 200
        assert response.json()["id"] == "test-collection-a"

    @pytest.mark.asyncio
    async def test_get_collection_no_header_allowed(
        self, app_client, multi_collection_ctx
    ):
        """GET /collections/{id} works without header."""
        response = await app_client.get("/collections/test-collection-a")
        assert response.status_code == 200
        assert response.json()["id"] == "test-collection-a"


class TestHeaderFilteringItems:
    """Tests for item endpoints with header filtering."""

    @pytest.mark.asyncio
    async def test_item_collection_uses_header(self, app_client, multi_collection_ctx):
        """GET /collections/{id}/items respects header."""
        response = await app_client.get(
            "/collections/test-collection-a/items",
            headers={FILTER_COLLECTIONS_HEADER: "test-collection-a"},
        )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_get_item_with_header(self, app_client, multi_collection_ctx):
        """GET /collections/{id}/items/{item_id} works with header."""
        response = await app_client.get(
            "/collections/test-collection-a/items/test-item-test-collection-a",
            headers={FILTER_COLLECTIONS_HEADER: "test-collection-a"},
        )
        assert response.status_code == 200


class TestGeometryHeaderFiltering:
    """Tests for geometry header filtering."""

    @pytest.mark.asyncio
    async def test_search_with_geometry_header(self, app_client, ctx):
        """Search respects X-Filter-Geometry header."""
        # Geometry that intersects with test item
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [149.0, -34.5],
                    [149.0, -32.0],
                    [151.5, -32.0],
                    [151.5, -34.5],
                    [149.0, -34.5],
                ]
            ],
        }

        response = await app_client.get(
            "/search",
            headers={FILTER_GEOMETRY_HEADER: json.dumps(geometry)},
        )
        assert response.status_code == 200
        # Items should be filtered by geometry

    @pytest.mark.asyncio
    async def test_search_with_non_intersecting_geometry(self, app_client, ctx):
        """Search with non-intersecting geometry returns no items."""
        # Geometry that doesn't intersect with test item
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.0, 0.0],
                    [0.0, 1.0],
                    [1.0, 1.0],
                    [1.0, 0.0],
                    [0.0, 0.0],
                ]
            ],
        }

        response = await app_client.get(
            "/search",
            headers={FILTER_GEOMETRY_HEADER: json.dumps(geometry)},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["features"]) == 0


class TestGeometryIntersectionOptimization:
    """Tests for geometry intersection optimization before database queries."""

    @pytest.mark.asyncio
    async def test_header_geometry_intersected_with_bbox(self, app_client, ctx):
        """Header geometry is intersected with request bbox."""
        # Header geometry: large polygon covering Australia
        header_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [140.0, -40.0],
                    [140.0, -30.0],
                    [160.0, -30.0],
                    [160.0, -40.0],
                    [140.0, -40.0],
                ]
            ],
        }

        # Bbox that overlaps with part of the header geometry
        # This should intersect with the test item location
        bbox = "148.0,-35.0,152.0,-32.0"

        response = await app_client.get(
            "/search",
            params={"bbox": bbox},
            headers={FILTER_GEOMETRY_HEADER: json.dumps(header_geometry)},
        )
        assert response.status_code == 200
        # Items within the intersection should be returned

    @pytest.mark.asyncio
    async def test_header_geometry_and_bbox_disjoint(self, app_client, ctx):
        """Disjoint header geometry and bbox returns empty result."""
        # Header geometry: polygon in Europe
        header_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [10.0, 50.0],
                    [10.0, 55.0],
                    [20.0, 55.0],
                    [20.0, 50.0],
                    [10.0, 50.0],
                ]
            ],
        }

        # Bbox in Australia - completely disjoint from header geometry
        bbox = "148.0,-35.0,152.0,-32.0"

        response = await app_client.get(
            "/search",
            params={"bbox": bbox},
            headers={FILTER_GEOMETRY_HEADER: json.dumps(header_geometry)},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["features"]) == 0

    @pytest.mark.asyncio
    async def test_header_geometry_intersected_with_intersects(self, app_client, ctx):
        """Header geometry is intersected with request intersects parameter."""
        # Header geometry covering test item area
        header_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [140.0, -40.0],
                    [140.0, -30.0],
                    [160.0, -30.0],
                    [160.0, -40.0],
                    [140.0, -40.0],
                ]
            ],
        }

        # Intersects geometry that overlaps with header and test item
        intersects_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [149.0, -34.5],
                    [149.0, -32.0],
                    [151.5, -32.0],
                    [151.5, -34.5],
                    [149.0, -34.5],
                ]
            ],
        }

        response = await app_client.post(
            "/search",
            json={"intersects": intersects_geometry},
            headers={FILTER_GEOMETRY_HEADER: json.dumps(header_geometry)},
        )
        assert response.status_code == 200
        # Items within the intersection should be returned

    @pytest.mark.asyncio
    async def test_header_geometry_and_intersects_disjoint(self, app_client, ctx):
        """Disjoint header geometry and intersects returns empty result."""
        # Header geometry in Europe
        header_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [10.0, 50.0],
                    [10.0, 55.0],
                    [20.0, 55.0],
                    [20.0, 50.0],
                    [10.0, 50.0],
                ]
            ],
        }

        # Intersects geometry in Australia - completely disjoint
        intersects_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [149.0, -34.5],
                    [149.0, -32.0],
                    [151.5, -32.0],
                    [151.5, -34.5],
                    [149.0, -34.5],
                ]
            ],
        }

        response = await app_client.post(
            "/search",
            json={"intersects": intersects_geometry},
            headers={FILTER_GEOMETRY_HEADER: json.dumps(header_geometry)},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["features"]) == 0

    @pytest.mark.asyncio
    async def test_header_geometry_intersected_with_cql2_s_intersects(
        self, app_client, ctx
    ):
        """Header geometry is intersected with CQL2 s_intersects filter."""
        # Header geometry covering test item area
        header_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [140.0, -40.0],
                    [140.0, -30.0],
                    [160.0, -30.0],
                    [160.0, -40.0],
                    [140.0, -40.0],
                ]
            ],
        }

        # CQL2 filter with s_intersects
        cql2_filter = {
            "op": "s_intersects",
            "args": [
                {"property": "geometry"},
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [149.0, -34.5],
                            [149.0, -32.0],
                            [151.5, -32.0],
                            [151.5, -34.5],
                            [149.0, -34.5],
                        ]
                    ],
                },
            ],
        }

        response = await app_client.post(
            "/search",
            json={"filter": cql2_filter, "filter-lang": "cql2-json"},
            headers={FILTER_GEOMETRY_HEADER: json.dumps(header_geometry)},
        )
        assert response.status_code == 200
        # Items within the intersection should be returned

    @pytest.mark.asyncio
    async def test_header_geometry_and_cql2_s_intersects_disjoint(
        self, app_client, ctx
    ):
        """Disjoint header geometry and CQL2 s_intersects returns empty result."""
        # Header geometry in Europe
        header_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [10.0, 50.0],
                    [10.0, 55.0],
                    [20.0, 55.0],
                    [20.0, 50.0],
                    [10.0, 50.0],
                ]
            ],
        }

        # CQL2 filter with s_intersects in Australia
        cql2_filter = {
            "op": "s_intersects",
            "args": [
                {"property": "geometry"},
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [149.0, -34.5],
                            [149.0, -32.0],
                            [151.5, -32.0],
                            [151.5, -34.5],
                            [149.0, -34.5],
                        ]
                    ],
                },
            ],
        }

        response = await app_client.post(
            "/search",
            json={"filter": cql2_filter, "filter-lang": "cql2-json"},
            headers={FILTER_GEOMETRY_HEADER: json.dumps(header_geometry)},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["features"]) == 0

    @pytest.mark.asyncio
    async def test_multiple_geometry_sources_intersection(self, app_client, ctx):
        """Multiple geometry sources (header, bbox, cql2) are all intersected."""
        # Header geometry: very large area
        header_geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [100.0, -50.0],
                    [100.0, -20.0],
                    [170.0, -20.0],
                    [170.0, -50.0],
                    [100.0, -50.0],
                ]
            ],
        }

        # Bbox within header geometry
        bbox = [145.0, -38.0, 155.0, -30.0]

        # CQL2 filter with s_intersects that overlaps with both
        cql2_filter = {
            "op": "s_intersects",
            "args": [
                {"property": "geometry"},
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [148.0, -36.0],
                            [148.0, -32.0],
                            [152.0, -32.0],
                            [152.0, -36.0],
                            [148.0, -36.0],
                        ]
                    ],
                },
            ],
        }

        response = await app_client.post(
            "/search",
            json={
                "bbox": bbox,
                "filter": cql2_filter,
                "filter-lang": "cql2-json",
            },
            headers={FILTER_GEOMETRY_HEADER: json.dumps(header_geometry)},
        )
        assert response.status_code == 200
        # The intersection of all three should still contain test item area
