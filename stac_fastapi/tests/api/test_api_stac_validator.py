import os
import uuid
from copy import deepcopy

import pytest

from ..conftest import create_collection, create_item


@pytest.mark.asyncio
async def test_stac_validator_allows_valid_datetime_range(txn_client, load_test_data):
    """Test that STAC validator allows valid datetime range with null datetime."""
    os.environ["ENABLE_FAST_VALIDATOR"] = "true"

    try:
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
    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_catches_eo_bands_in_assets(txn_client, load_test_data):
    """Test that STAC validator catches eo:bands in assets when using EO v2.0.0."""
    from fastapi import HTTPException

    os.environ["ENABLE_FAST_VALIDATOR"] = "true"

    try:
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
        assert (
            "Item validation failed" in str(exc_info.value)
            or "validation" in str(exc_info.value).lower()
        )
        assert exc_info.value.status_code == 400
    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_catches_invalid_cloud_cover(txn_client, load_test_data):
    """Test that STAC validator catches invalid eo:cloud_cover values."""
    from fastapi import HTTPException

    os.environ["ENABLE_FAST_VALIDATOR"] = "true"

    try:
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
        assert (
            "Item validation failed" in str(exc_info.value)
            or "validation" in str(exc_info.value).lower()
        )
    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_feature_collection_with_invalid_item_raise_on_error(
    txn_client, load_test_data
):
    """Test that STAC validator fails entire FeatureCollection when RAISE_ON_BULK_ERROR is true."""
    from fastapi import HTTPException

    os.environ["ENABLE_FAST_VALIDATOR"] = "true"
    os.environ["RAISE_ON_BULK_ERROR"] = "true"

    try:
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

        assert "Batch rejected" in str(exc_info.value)
        assert exc_info.value.status_code == 400
    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        os.environ.pop("RAISE_ON_BULK_ERROR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_feature_collection_with_invalid_item_skip_on_error(
    txn_client, core_client, load_test_data
):
    """Test that STAC validator skips invalid items when RAISE_ON_BULK_ERROR is false."""
    from ..conftest import MockRequest

    os.environ["ENABLE_FAST_VALIDATOR"] = "true"
    os.environ["RAISE_ON_BULK_ERROR"] = "false"

    try:
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

        # With RAISE_ON_BULK_ERROR=false, should skip invalid item and insert valid ones
        await create_item(txn_client, feature_collection)

        # Verify only 2 valid items exist in the collection
        fc = await core_client.item_collection(
            test_collection["id"], request=MockRequest()
        )
        assert len(fc["features"]) == 2
        item_ids = {f["id"] for f in fc["features"]}
        assert item_ids == {"valid-item-0", "valid-item-1"}
    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        os.environ.pop("RAISE_ON_BULK_ERROR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_catches_invalid_snow_cover(txn_client, load_test_data):
    """Test that STAC validator catches invalid eo:snow_cover values."""
    from fastapi import HTTPException

    os.environ["ENABLE_FAST_VALIDATOR"] = "true"

    try:
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
        assert (
            "Item validation failed" in str(exc_info.value)
            or "validation" in str(exc_info.value).lower()
        )
    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_allows_valid_item(txn_client, load_test_data):
    """Test that STAC validator allows valid STAC items."""
    os.environ["ENABLE_FAST_VALIDATOR"] = "true"

    try:
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
    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


def test_schema_cache_size_environment_variable():
    """Test that fast validator is properly configured."""
    # Test that ENABLE_FAST_VALIDATOR environment variable is read correctly
    original_value = os.environ.get("ENABLE_FAST_VALIDATOR")

    try:
        # Test that ENABLE_FAST_VALIDATOR can be set
        os.environ["ENABLE_FAST_VALIDATOR"] = "true"
        import importlib

        import stac_fastapi.core.validate as validate_module

        importlib.reload(validate_module)
        # If no exception is raised, the module loaded correctly with the env var set
        assert True
    finally:
        # Clean up and restore original value
        if original_value is not None:
            os.environ["ENABLE_FAST_VALIDATOR"] = original_value
        else:
            os.environ.pop("ENABLE_FAST_VALIDATOR", None)


@pytest.mark.asyncio
async def test_stac_validator_returns_400_on_invalid_item(app_client, load_test_data):
    """Test that invalid STAC items return 400 Bad Request response."""
    os.environ["ENABLE_FAST_VALIDATOR"] = "true"

    try:
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
        assert (
            resp.status_code == 400
        ), f"Expected 400, got {resp.status_code}: {resp.text}"

        # Verify error message mentions validation failure
        response_data = resp.json()
        assert "detail" in response_data
        # The detail should contain either a message or errors dict
        detail = response_data["detail"]
        assert isinstance(
            detail, (str, dict)
        ), f"Expected string or dict, got {type(detail)}"
        if isinstance(detail, dict):
            assert "message" in detail or "errors" in detail

    finally:
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        try:
            await app_client.delete(f"/collections/{test_collection['id']}")
        except Exception:
            pass
