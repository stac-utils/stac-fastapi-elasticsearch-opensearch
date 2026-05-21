import uuid
from copy import deepcopy

import pytest

from ..conftest import create_collection, create_item


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
