import math
import uuid
from copy import deepcopy

import pytest

from ..conftest import create_collection, create_item


def create_circular_polygon_ring(
    num_vertices: int,
    center_lon: float = -120.0,
    center_lat: float = 40.0,
    radius: float = 5.0,
) -> list:
    """Create a circular polygon ring with specified number of vertices.

    Args:
        num_vertices: Number of vertices to create (excluding closing vertex).
        center_lon: Longitude of circle center.
        center_lat: Latitude of circle center.
        radius: Radius of circle in degrees.

    Returns:
        List of [lon, lat] coordinates forming a closed ring.
    """
    vertices = []
    for i in range(num_vertices):
        angle = (i / num_vertices) * 2 * math.pi
        lon = center_lon + radius * math.cos(angle)
        lat = center_lat + radius * math.sin(angle)
        vertices.append([lon, lat])
    vertices.append(vertices[0])  # Close the ring
    return vertices


@pytest.fixture(autouse=True)
def configure_validation_env(monkeypatch: pytest.MonkeyPatch):
    """Lock down env so validation tests always run synchronously."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    monkeypatch.setenv("ENABLE_REDIS_QUEUE", "false")


@pytest.mark.asyncio
async def test_stac_validator_allows_valid_datetime_range(txn_client, load_test_data):
    """Test that STAC validator allows valid datetime range with null datetime."""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-dt-range-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with null datetime but valid start/end_datetime (valid per STAC schema)
    valid_item = deepcopy(base_item)
    valid_item["id"] = "valid-datetime-range"
    valid_item["properties"]["datetime"] = None
    valid_item["properties"]["start_datetime"] = "2020-01-01T00:00:00Z"
    valid_item["properties"]["end_datetime"] = "2020-01-02T00:00:00Z"

    # This should succeed - valid Pydantic and STAC item
    await create_item(txn_client, valid_item)


@pytest.mark.asyncio
async def test_stac_validator_catches_eo_bands_in_assets(txn_client, load_test_data):
    """Test that STAC validator catches eo:bands in assets when using EO v2.0.0."""
    from fastapi import HTTPException

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-eo-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")

    # Create item with EO v2.0.0 extension which has stricter asset validation
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-eo-bands-in-assets"
    invalid_item["collection"] = test_collection["id"]
    invalid_item["stac_extensions"] = [
        "https://stac-extensions.github.io/eo/v2.0.0/schema.json"
    ]

    # EO v2.0.0 doesn't allow eo:bands in assets - should fail validation
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    # Verify the error message mentions the validation failure
    assert exc_info.value.status_code == 400
    assert "STAC validation failed" in exc_info.value.detail
    assert "invalid-eo-bands-in-assets" in exc_info.value.detail


@pytest.mark.asyncio
async def test_stac_validator_catches_invalid_cloud_cover(txn_client, load_test_data):
    """Test that STAC validator catches invalid eo:cloud_cover values."""
    from fastapi import HTTPException

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-cloud-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")

    # Create item with invalid cloud_cover (must be 0-100)
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-cloud-cover"
    invalid_item["collection"] = test_collection["id"]
    invalid_item["properties"]["eo:cloud_cover"] = 150  # Invalid: > 100

    # This should raise HTTPException due to STAC validation failure
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    # Verify the error message mentions the validation failure
    assert exc_info.value.status_code == 400
    assert "STAC validation failed" in exc_info.value.detail
    assert "invalid-cloud-cover" in exc_info.value.detail


@pytest.mark.asyncio
async def test_stac_validator_feature_collection_with_invalid_item_raise_on_error(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that STAC validator fails entire FeatureCollection when RAISE_ON_BULK_ERROR is true."""
    from fastapi import HTTPException

    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-fc-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")

    # Create FeatureCollection with 2 valid items and 1 invalid item
    features = []
    for i in range(2):
        item = deepcopy(base_item)
        item["id"] = f"valid-item-{i}"
        item["collection"] = test_collection["id"]
        features.append(item)

    # Add invalid item (invalid cloud_cover)
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-item-fc"
    invalid_item["collection"] = test_collection["id"]
    invalid_item["properties"]["eo:cloud_cover"] = 150  # Invalid: > 100

    features.append(invalid_item)

    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    # With RAISE_ON_BULK_ERROR=true, should fail on first invalid item
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, feature_collection)

    assert exc_info.value.status_code == 400

    # Verify the exact structured detail payload
    detail = exc_info.value.detail
    assert "Batch rejected. 1 items failed validation." in detail["message"]
    assert "errors" in detail
    error_keys = list(detail["errors"].keys())
    assert len(error_keys) == 1
    assert detail["errors"][error_keys[0]] == ["invalid-item-fc"]

    # Verify the bulk summary telemetry structure
    summary = detail["summary"]
    assert summary["input_count"] == 3
    assert summary["processed_count"] == 3
    assert summary["valid_count"] == 2
    assert summary["validation_error_count"] == 1
    assert summary["skipped_total"] == 1
    assert summary["conflict_count"] == 0
    assert summary["database_error_count"] == 0


@pytest.mark.asyncio
async def test_stac_validator_feature_collection_with_invalid_item_skip_on_error(
    txn_client, core_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that STAC validator skips invalid items when RAISE_ON_BULK_ERROR is false."""
    from ..conftest import MockRequest

    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-fc-skip-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")

    # Create FeatureCollection with 2 valid items and 1 invalid item
    features = []
    for i in range(2):
        item = deepcopy(base_item)
        item["id"] = f"valid-item-{i}"
        item["collection"] = test_collection["id"]
        # Remove eo:bands from properties (violates EO v1.0.0 spec - should only be in assets)
        if "eo:bands" in item.get("properties", {}):
            del item["properties"]["eo:bands"]
        features.append(item)

    # Add invalid item (invalid cloud_cover)
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-item-fc"
    invalid_item["collection"] = test_collection["id"]
    # Remove eo:bands from properties (violates EO v1.0.0 spec - should only be in assets)
    if "eo:bands" in invalid_item.get("properties", {}):
        del invalid_item["properties"]["eo:bands"]
    invalid_item["properties"]["eo:cloud_cover"] = 150  # Invalid: > 100

    features.append(invalid_item)
    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    # With RAISE_ON_BULK_ERROR=false, should skip invalid item and insert valid ones
    await create_item(txn_client, feature_collection)

    # Verify only 2 valid items exist in the collection
    fc = await core_client.item_collection(test_collection["id"], request=MockRequest())
    assert len(fc["features"]) == 2
    item_ids = {f["id"] for f in fc["features"]}
    assert item_ids == {"valid-item-0", "valid-item-1"}


@pytest.mark.asyncio
async def test_stac_validator_catches_invalid_snow_cover(txn_client, load_test_data):
    """Test that STAC validator catches invalid eo:snow_cover values."""
    from fastapi import HTTPException

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-snow-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")

    # Create item with invalid snow_cover (must be 0-100)
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-snow-cover"
    invalid_item["collection"] = test_collection["id"]
    invalid_item["properties"]["eo:snow_cover"] = -10  # Invalid: < 0

    # This should raise HTTPException due to STAC validation failure
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    # Verify the error message mentions the validation failure
    assert exc_info.value.status_code == 400
    assert "STAC validation failed" in exc_info.value.detail
    assert "invalid-snow-cover" in exc_info.value.detail


@pytest.mark.asyncio
async def test_stac_validator_allows_valid_item(txn_client, load_test_data):
    """Test that STAC validator allows valid STAC items."""
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-valid-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    valid_item = deepcopy(base_item)
    valid_item["id"] = "valid-stac-item"
    valid_item["collection"] = test_collection["id"]

    # This should succeed - valid STAC item (create_item doesn't return the item)
    await create_item(txn_client, valid_item)
    # If no exception is raised, the test passes


@pytest.mark.asyncio
async def test_stac_validator_returns_400_on_invalid_item(app_client, load_test_data):
    """Test that invalid STAC items return 400 Bad Request response."""
    # Create a test collection first
    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-400-{uuid.uuid4()}"

    resp = await app_client.post(
        "/collections",
        json=test_collection,
    )
    assert resp.status_code == 201

    # Create invalid item with EO v2.0.0 extension (eo:bands not allowed in assets)
    base_item = load_test_data("test_item.json")
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-item-400"
    invalid_item["collection"] = test_collection["id"]
    invalid_item["stac_extensions"] = [
        "https://stac-extensions.github.io/eo/v2.0.0/schema.json"
    ]
    # EO v2.0.0 doesn't allow eo:bands in assets - should fail validation

    # POST invalid item and verify 400 response
    resp = await app_client.post(
        f"/collections/{test_collection['id']}/items",
        json=invalid_item,
    )

    # Should return 400 Bad Request, not 500
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}: {resp.text}"

    # Verify the exact translated error string structure
    response_data = resp.json()
    assert "detail" in response_data
    assert (
        "Invalid item: STAC validation failed for 'invalid-item-400'"
        in response_data["detail"]
    )


@pytest.mark.asyncio
async def test_chunked_validation_with_max_batch_size(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that chunked validation is enabled when MAX_BATCH_SIZE is set.

    Verifies that the validation layer respects MAX_BATCH_SIZE and MAX_BATCH_ERROR_SIZE
    configuration for CPU optimization on high-volume ingestion.
    """
    monkeypatch.setenv("MAX_BATCH_SIZE", "100")  # Enable chunked validation
    monkeypatch.setenv("MAX_BATCH_ERROR_SIZE", "10")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-chunked-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]
    if "datetime" not in base_item.get("properties", {}):
        base_item["properties"]["datetime"] = "2020-01-01T00:00:00Z"

    # Create a batch of valid items
    items_to_post = []
    for i in range(5):
        valid_item = deepcopy(base_item)
        valid_item["id"] = f"chunked-valid-{i}"
        items_to_post.append(valid_item)

    feature_collection = {
        "type": "FeatureCollection",
        "features": items_to_post,
    }

    # Post items using the test helper (which handles request context)
    await create_item(txn_client, feature_collection)

    # Verify chunked validation allowed valid items through by checking database persistence
    db_item = await txn_client.database.get_one_item(
        item_id="chunked-valid-0", collection_id=test_collection["id"]
    )
    assert (
        db_item is not None
    ), "Chunked validation should allow valid items to be inserted"


@pytest.mark.asyncio
async def test_chunked_validation_exceeds_max_error_size(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that chunked validation fails fast when MAX_BATCH_ERROR_SIZE is breached.

    Verifies that the validation loop stops immediately and throws a 400 error
    when the error count exceeds the configured threshold, preventing wasted
    CPU cycles on hopelessly broken payloads.
    """
    from fastapi import HTTPException

    # Configure a tiny error gateway threshold
    monkeypatch.setenv("MAX_BATCH_SIZE", "2")
    monkeypatch.setenv("MAX_BATCH_ERROR_SIZE", "1")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-chunked-fail-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Generate 3 invalid items (cloud cover = 150, which exceeds valid range)
    # With a batch size of 2, the first chunk will contain 2 errors, immediately breaching the threshold of 1
    items_to_post = []
    for i in range(3):
        invalid_item = deepcopy(base_item)
        invalid_item["id"] = f"chunked-invalid-{i}"
        invalid_item["properties"]["eo:cloud_cover"] = 150
        items_to_post.append(invalid_item)

    feature_collection = {
        "type": "FeatureCollection",
        "features": items_to_post,
    }

    # Verify that the processor triggers the threshold cutoff
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, feature_collection)

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail

    # Assert on the custom threshold message string
    assert "Validation error threshold exceeded" in detail["message"]

    # Verify the summary reports exactly how far the loop got before terminating
    summary = detail["summary"]
    assert summary["input_count"] == 3
    assert (
        summary["processed_count"] == 2
    ), "Loop should cut off at chunk 1 (2 items processed)"
    assert summary["valid_count"] == 0
    assert (
        summary["validation_error_count"] == 2
    ), "Found 2 errors in chunk 1, which exceeds threshold of 1"


@pytest.mark.asyncio
async def test_topology_validation_detects_antimeridian_crossing(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that topology validation detects improper antimeridian crossing.

    Verifies that geometries with longitude jumps > 180 degrees are rejected
    when ENABLE_TOPOLOGY_VALIDATION is enabled.
    """
    from fastapi import HTTPException

    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-antimeridian-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with geometry that crosses antimeridian improperly
    # Longitude jumps from 170 to -170 (360 degree wrap, invalid)
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "antimeridian-crossing-item"
    invalid_item["geometry"] = {
        "type": "Polygon",
        "coordinates": [
            [
                [170.0, -10.0],
                [170.0, 10.0],
                [-170.0, 10.0],  # Jump of 340 degrees (> 180)
                [-170.0, -10.0],
                [170.0, -10.0],
            ]
        ],
    }

    # Verify that the topology validation rejects this item
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail
    assert "Invalid item geometry" in detail
    assert "antimeridian" in detail.lower()


@pytest.mark.asyncio
async def test_topology_validation_allows_valid_polygons(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that topology validation allows valid polygons.

    Verifies that properly formatted geometries pass topology validation
    when ENABLE_TOPOLOGY_VALIDATION is enabled.
    """
    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-valid-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with valid polygon geometry
    valid_item = deepcopy(base_item)
    valid_item["id"] = "valid-polygon-item"
    valid_item["geometry"] = {
        "type": "Polygon",
        "coordinates": [
            [
                [-120.0, 40.0],
                [-120.0, 50.0],
                [-110.0, 50.0],
                [-110.0, 40.0],
                [-120.0, 40.0],
            ]
        ],
    }

    # This should succeed - valid geometry
    await create_item(txn_client, valid_item)

    # Verify item was inserted by checking database persistence
    db_item = await txn_client.database.get_one_item(
        item_id="valid-polygon-item", collection_id=test_collection["id"]
    )
    assert db_item is not None, "Valid polygon should be inserted into database"


@pytest.mark.asyncio
async def test_topology_validation_detects_out_of_bounds_coordinates(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that topology validation detects out-of-bounds coordinates.

    Verifies that coordinates outside WGS84 bounds (±180° lon, ±90° lat)
    are rejected when ENABLE_TOPOLOGY_VALIDATION is enabled.
    """
    from fastapi import HTTPException

    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-bounds-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with out-of-bounds latitude
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "out-of-bounds-item"
    invalid_item["geometry"] = {
        "type": "Polygon",
        "coordinates": [
            [
                [-120.0, 40.0],
                [-120.0, 95.0],  # Latitude > 90 (invalid)
                [-110.0, 50.0],
                [-110.0, 40.0],
                [-120.0, 40.0],
            ]
        ],
    }

    # Verify that the topology validation rejects this item
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail
    assert "Invalid item geometry" in detail
    assert "WGS84 bounds" in detail


@pytest.mark.asyncio
async def test_topology_validation_detects_all_coordinates(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that topology validation checks all coordinates in a ring.

    Verifies that intermediate coordinates in a polygon ring are validated for WGS84 bounds,
    not just the first coordinate. This ensures the recursive bounds checking validates
    every coordinate pair in the geometry.
    """
    from fastapi import HTTPException

    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-final-coord-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item where an intermediate coordinate is out of bounds
    # This tests that all coordinates (not just the first) are validated
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "intermediate-coord-out-of-bounds"
    invalid_item["geometry"] = {
        "type": "Polygon",
        "coordinates": [
            [
                [-120.0, 40.0],
                [-120.0, 95.0],  # Intermediate coordinate has latitude > 90 (invalid)
                [-110.0, 50.0],
                [-110.0, 40.0],
                [-120.0, 40.0],  # Closing coordinate matches opening
            ]
        ],
    }

    # Verify that the topology validation rejects this item
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail
    assert "Invalid item geometry" in detail
    assert "WGS84 bounds" in detail


@pytest.mark.asyncio
async def test_topology_validation_disabled_by_default(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that topology validation is disabled by default.

    Verifies that items with invalid geometries are accepted when
    ENABLE_TOPOLOGY_VALIDATION is not set (defaults to false).
    """
    # Ensure topology validation is disabled (default)
    monkeypatch.delenv("ENABLE_TOPOLOGY_VALIDATION", raising=False)

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-disabled-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with invalid geometry (antimeridian crossing)
    item_with_invalid_geometry = deepcopy(base_item)
    item_with_invalid_geometry["id"] = "antimeridian-item-disabled"
    item_with_invalid_geometry["geometry"] = {
        "type": "Polygon",
        "coordinates": [
            [
                [170.0, -10.0],
                [170.0, 10.0],
                [-170.0, 10.0],  # Jump of 340 degrees
                [-170.0, -10.0],
                [170.0, -10.0],
            ]
        ],
    }

    # Should succeed because topology validation is disabled
    await create_item(txn_client, item_with_invalid_geometry)

    # Verify item was inserted despite invalid geometry (validation disabled)
    db_item = await txn_client.database.get_one_item(
        item_id="antimeridian-item-disabled", collection_id=test_collection["id"]
    )
    assert (
        db_item is not None
    ), "Item should be inserted when topology validation is disabled"


@pytest.mark.asyncio
async def test_bulk_topology_validation_filters_invalid_items_lenient(
    txn_client, core_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test bulk validation filters out antimeridian errors when strict mode is false.

    Verifies that when RAISE_ON_BULK_ERROR=false, items with topology errors are
    filtered out while valid items in the same batch are safely ingested.
    """
    from ..conftest import MockRequest

    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")
    monkeypatch.setenv("MAX_BATCH_SIZE", "10")
    monkeypatch.setenv("MAX_BATCH_ERROR_SIZE", "5")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-bulk-lenient-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Construct a batch: 2 valid items, 1 antimeridian wrap item
    features = []
    for i in range(2):
        item = deepcopy(base_item)
        item["id"] = f"bulk-lenient-valid-{i}"
        features.append(item)

    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "bulk-lenient-invalid-topo"
    invalid_item["geometry"] = {
        "type": "Polygon",
        "coordinates": [
            [
                [170.0, -10.0],
                [170.0, 10.0],
                [-170.0, 10.0],
                [-170.0, -10.0],
                [170.0, -10.0],
            ]
        ],
    }
    features.append(invalid_item)

    feature_collection = {"type": "FeatureCollection", "features": features}

    # Execute lenient batch post
    await create_item(txn_client, feature_collection)

    # Verify only the 2 valid items made it to the database
    fc = await core_client.item_collection(test_collection["id"], request=MockRequest())
    assert len(fc["features"]) == 2
    item_ids = {f["id"] for f in fc["features"]}
    assert item_ids == {"bulk-lenient-valid-0", "bulk-lenient-valid-1"}


@pytest.mark.asyncio
async def test_bulk_topology_validation_trips_circuit_breaker(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that bulk topology errors trip the batch circuit breaker fast-fail cutoff.

    Verifies that topology errors accurately feed the circuit breaker tally and cause
    a fast-fail shutdown of the chunk loop when MAX_BATCH_ERROR_SIZE is exceeded.
    """
    from fastapi import HTTPException

    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")
    monkeypatch.setenv("MAX_BATCH_SIZE", "2")
    monkeypatch.setenv("MAX_BATCH_ERROR_SIZE", "1")  # Trip if more than 1 error
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-breaker-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Generate 3 items with bad topology
    # Slicing at chunk size 2 means chunk #1 will immediately yield 2 errors,
    # breaching the ceiling of 1
    features = []
    for i in range(3):
        invalid_item = deepcopy(base_item)
        invalid_item["id"] = f"breaker-invalid-topo-{i}"
        invalid_item["geometry"] = {
            "type": "Polygon",
            "coordinates": [
                [
                    [170.0, -10.0],
                    [170.0, 10.0],
                    [-170.0, 10.0],
                    [-170.0, -10.0],
                    [170.0, -10.0],
                ]
            ],
        }
        features.append(invalid_item)

    feature_collection = {"type": "FeatureCollection", "features": features}

    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, feature_collection)

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail

    # Air-tight structural type assertions (Issue #4)
    assert isinstance(
        detail, dict
    ), f"Expected dictionary payload structure, got {type(detail)}"
    assert (
        "message" in detail
    ), "Error block missing authoritative 'message' string element"
    assert "summary" in detail, "Error block missing 'summary' telemetry object"

    assert "Validation error threshold exceeded" in detail["message"]

    summary = detail["summary"]
    assert summary["input_count"] == 3
    assert (
        summary["processed_count"] == 2
    ), "Loop should abort early after chunk 1 (2 items processed)"
    assert summary["validation_error_count"] == 2


@pytest.mark.asyncio
async def test_topology_validation_respects_max_vertices_limit(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that MAX_TOPOLOGY_VERTICES environment variable is respected.

    Verifies that the configurable vertex limit prevents geometries with excessive
    vertices from being ingested, protecting against DoS attacks with pathologically
    complex geometries.
    """
    from fastapi import HTTPException

    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")
    monkeypatch.setenv("MAX_TOPOLOGY_VERTICES", "10")  # Very low limit for testing

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-vertices-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with geometry that exceeds the vertex limit
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "too-many-vertices"
    # Create a ring with 16 vertices (15 + 1 closing, exceeds limit of 10)
    vertices = create_circular_polygon_ring(15)
    invalid_item["geometry"] = {
        "type": "Polygon",
        "coordinates": [vertices],
    }

    # Verify that the topology validation rejects this item
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail
    assert "Invalid item geometry" in detail
    assert "too many vertices" in detail.lower()
    assert "Maximum allowed is 10" in detail


@pytest.mark.asyncio
async def test_topology_validation_allows_items_within_vertex_limit(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that items within MAX_TOPOLOGY_VERTICES limit are accepted.

    Verifies that the vertex limit is configurable and allows valid geometries
    with many vertices when the limit is set appropriately.
    """
    monkeypatch.setenv("ENABLE_TOPOLOGY_VALIDATION", "true")
    monkeypatch.setenv("MAX_TOPOLOGY_VERTICES", "20")  # Increased limit

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-topology-within-limit-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with geometry that is within the vertex limit
    valid_item = deepcopy(base_item)
    valid_item["id"] = "within-vertex-limit"
    # Create a ring with 16 vertices (15 + 1 closing, within limit of 20)
    vertices = create_circular_polygon_ring(15)
    valid_item["geometry"] = {
        "type": "Polygon",
        "coordinates": [vertices],
    }

    # Should succeed because item is within the limit
    await create_item(txn_client, valid_item)

    # Verify item was inserted
    db_item = await txn_client.database.get_one_item(
        item_id="within-vertex-limit", collection_id=test_collection["id"]
    )
    assert db_item is not None, "Item should be inserted when within vertex limit"


@pytest.mark.asyncio
async def test_feature_collection_conflict_errors_formatted(
    app_client, txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that conflict errors are properly formatted in bulk responses.

    Verifies that when items already exist in the database, the conflict_errors
    response contains human-readable messages mapping item IDs to conflict descriptions.
    """
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-conflicts-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create and insert the first item
    item1 = deepcopy(base_item)
    item1["id"] = "conflict-test-item-1"
    await create_item(txn_client, item1)

    # Create a FeatureCollection with:
    # - 1 new item (should succeed)
    # - 1 duplicate item (should conflict)
    features = []

    # New item
    new_item = deepcopy(base_item)
    new_item["id"] = "conflict-test-item-2"
    features.append(new_item)

    # Duplicate item (already exists)
    duplicate_item = deepcopy(base_item)
    duplicate_item["id"] = "conflict-test-item-1"
    features.append(duplicate_item)

    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    # POST the FeatureCollection via HTTP - should succeed with 1 new item and 1 conflict
    resp = await app_client.post(
        f"/collections/{test_collection['id']}/items",
        json=feature_collection,
    )

    # Should return 201 when at least one item succeeds (RAISE_ON_BULK_ERROR=false)
    assert resp.status_code == 201
    response = resp.json()

    # Verify response structure
    assert isinstance(response, dict)
    assert "conflict_errors" in response
    assert isinstance(response["conflict_errors"], dict)

    # Verify conflict error format
    conflict_errors = response["conflict_errors"]
    assert "conflict-test-item-1" in conflict_errors
    conflict_msg = conflict_errors["conflict-test-item-1"]

    # Verify the message is human-readable and contains expected parts
    assert "already exists" in conflict_msg
    assert "conflict-test-item-1" in conflict_msg
    assert test_collection["id"] in conflict_msg


@pytest.mark.asyncio
async def test_datetime_validation_disabled_by_default(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that datetime validation is disabled by default.

    Verifies that items with invalid datetime ranges are accepted when
    ENABLE_STAC_VALIDATOR is not set (defaults to false).
    """
    # Ensure STAC validation is disabled (default)
    monkeypatch.delenv("ENABLE_STAC_VALIDATOR", raising=False)

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-datetime-disabled-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with invalid datetime range (start > end)
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-datetime-range-disabled"
    invalid_item["properties"]["datetime"] = None
    invalid_item["properties"]["start_datetime"] = "2020-12-31T00:00:00Z"
    invalid_item["properties"]["end_datetime"] = "2020-01-01T00:00:00Z"  # start > end

    # Should succeed because datetime validation is disabled
    await create_item(txn_client, invalid_item)

    # Verify item was inserted despite invalid datetime range
    db_item = await txn_client.database.get_one_item(
        item_id="invalid-datetime-range-disabled", collection_id=test_collection["id"]
    )
    assert (
        db_item is not None
    ), "Item should be inserted when datetime validation is disabled"


@pytest.mark.asyncio
async def test_datetime_validation_start_greater_than_end(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that datetime validation rejects start_datetime > end_datetime."""
    from fastapi import HTTPException

    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-datetime-range-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with start_datetime > end_datetime
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-start-greater-than-end"
    invalid_item["properties"]["datetime"] = None
    invalid_item["properties"]["start_datetime"] = "2020-12-31T00:00:00Z"
    invalid_item["properties"]["end_datetime"] = "2020-01-01T00:00:00Z"

    # Should raise HTTPException due to datetime validation
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    assert exc_info.value.status_code == 400
    assert "datetime" in exc_info.value.detail.lower()
    assert "must be <=" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_datetime_validation_missing_end_datetime(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that datetime validation requires both start and end datetime."""
    from pydantic import ValidationError

    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-datetime-missing-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with start_datetime but no end_datetime
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "missing-end-datetime"
    invalid_item["properties"]["datetime"] = None
    invalid_item["properties"]["start_datetime"] = "2020-01-01T00:00:00Z"
    invalid_item["properties"]["end_datetime"] = None

    # Should raise ValidationError (Pydantic validates before custom validation)
    with pytest.raises(ValidationError) as exc_info:
        await create_item(txn_client, invalid_item)

    # Verify error mentions datetime requirement
    assert "datetime" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_datetime_validation_valid_range(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that datetime validation allows valid datetime ranges."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-datetime-valid-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with valid datetime range
    valid_item = deepcopy(base_item)
    valid_item["id"] = "valid-datetime-range"
    valid_item["properties"]["datetime"] = None
    valid_item["properties"]["start_datetime"] = "2020-01-01T00:00:00Z"
    valid_item["properties"]["end_datetime"] = "2020-12-31T23:59:59Z"

    # Should succeed - valid datetime range
    await create_item(txn_client, valid_item)

    # Verify item was inserted
    db_item = await txn_client.database.get_one_item(
        item_id="valid-datetime-range", collection_id=test_collection["id"]
    )
    assert db_item is not None, "Valid datetime range should be inserted"


@pytest.mark.asyncio
async def test_datetime_validation_datetime_outside_range(
    txn_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that datetime validation rejects datetime outside start/end range."""
    from fastapi import HTTPException

    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-datetime-outside-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create item with datetime outside start/end range
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "datetime-outside-range"
    invalid_item["properties"]["datetime"] = "2021-06-15T00:00:00Z"  # Outside range
    invalid_item["properties"]["start_datetime"] = "2020-01-01T00:00:00Z"
    invalid_item["properties"]["end_datetime"] = "2020-12-31T23:59:59Z"

    # Should raise HTTPException due to datetime validation
    with pytest.raises(HTTPException) as exc_info:
        await create_item(txn_client, invalid_item)

    assert exc_info.value.status_code == 400
    assert "datetime" in exc_info.value.detail.lower()
    assert "must be <=" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_datetime_validation_feature_collection_with_invalid_datetime(
    txn_client, core_client, load_test_data, monkeypatch: pytest.MonkeyPatch
):
    """Test that datetime validation filters invalid items in FeatureCollection."""
    from ..conftest import MockRequest

    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    monkeypatch.setenv("RAISE_ON_BULK_ERROR", "false")

    test_collection = load_test_data("test_collection.json")
    test_collection["id"] = f"test-collection-datetime-fc-{uuid.uuid4()}"
    await create_collection(txn_client, collection=test_collection)

    base_item = load_test_data("test_item.json")
    base_item["collection"] = test_collection["id"]

    # Create FeatureCollection with 1 valid and 1 invalid datetime item
    features = []

    # Valid item
    valid_item = deepcopy(base_item)
    valid_item["id"] = "valid-datetime-fc"
    valid_item["properties"]["datetime"] = None
    valid_item["properties"]["start_datetime"] = "2020-01-01T00:00:00Z"
    valid_item["properties"]["end_datetime"] = "2020-12-31T23:59:59Z"
    features.append(valid_item)

    # Invalid item (start > end)
    invalid_item = deepcopy(base_item)
    invalid_item["id"] = "invalid-datetime-fc"
    invalid_item["properties"]["datetime"] = None
    invalid_item["properties"]["start_datetime"] = "2020-12-31T00:00:00Z"
    invalid_item["properties"]["end_datetime"] = "2020-01-01T00:00:00Z"
    features.append(invalid_item)

    feature_collection = {"type": "FeatureCollection", "features": features}

    # Post FeatureCollection - should skip invalid item
    await create_item(txn_client, feature_collection)

    # Verify only valid item was inserted
    fc = await core_client.item_collection(test_collection["id"], request=MockRequest())
    assert len(fc["features"]) == 1
    assert fc["features"][0]["id"] == "valid-datetime-fc"
