import os
import uuid
from copy import deepcopy

import pytest

from ..conftest import create_collection, create_item


@pytest.mark.asyncio
async def test_stac_validator_catches_null_start_datetime(txn_client, load_test_data):
    """Test that STAC validator catches null start_datetime when datetime is null."""
    os.environ["ENABLE_STAC_VALIDATOR"] = "true"

    try:
        test_collection = load_test_data("test_collection.json")
        test_collection["id"] = f"test-collection-null-dt-{uuid.uuid4()}"
        await create_collection(txn_client, collection=test_collection)

        base_item = load_test_data("test_item.json")
        base_item["collection"] = test_collection["id"]

        # Create item with null datetime and null start_datetime (invalid per STAC schema)
        invalid_item = deepcopy(base_item)
        invalid_item["id"] = "invalid-null-start-datetime"
        invalid_item["properties"]["datetime"] = None
        invalid_item["properties"][
            "start_datetime"
        ] = None  # This should fail validation
        invalid_item["properties"]["end_datetime"] = "2020-01-02T00:00:00Z"

        # This should raise ValueError due to STAC validation failure
        with pytest.raises(ValueError):
            await create_item(txn_client, invalid_item)
    finally:
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_catches_eo_bands_in_assets(txn_client, load_test_data):
    """Test that STAC validator catches eo:bands in assets when using EO v2.0.0."""
    os.environ["ENABLE_STAC_VALIDATOR"] = "true"

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
        with pytest.raises(ValueError) as exc_info:
            await create_item(txn_client, invalid_item)

        # Verify the error message mentions the validation failure
        assert "STAC validation failed" in str(exc_info.value)
        assert "eo:bands" in str(exc_info.value)
    finally:
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_catches_invalid_cloud_cover(txn_client, load_test_data):
    """Test that STAC validator catches invalid eo:cloud_cover values."""
    os.environ["ENABLE_STAC_VALIDATOR"] = "true"

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

        # This should raise ValueError due to STAC validation failure
        with pytest.raises(ValueError) as exc_info:
            await create_item(txn_client, invalid_item)

        # Verify the error message mentions the validation failure
        assert "STAC validation failed" in str(exc_info.value)
    finally:
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_catches_invalid_snow_cover(txn_client, load_test_data):
    """Test that STAC validator catches invalid eo:snow_cover values."""
    os.environ["ENABLE_STAC_VALIDATOR"] = "true"

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

        # This should raise ValueError due to STAC validation failure
        with pytest.raises(ValueError) as exc_info:
            await create_item(txn_client, invalid_item)

        # Verify the error message mentions the validation failure
        assert "STAC validation failed" in str(exc_info.value)
    finally:
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_stac_validator_allows_valid_item(txn_client, load_test_data):
    """Test that STAC validator allows valid STAC items."""
    os.environ["ENABLE_STAC_VALIDATOR"] = "true"

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
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass
