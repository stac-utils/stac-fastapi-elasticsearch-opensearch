"""Unit tests for core STAC validation functions."""

from copy import deepcopy

import pytest

from stac_fastapi.core.validate import (
    async_validate_batch_with_stac_validator,
    async_validate_stac,
)


@pytest.mark.asyncio
async def test_async_validate_stac_valid_item(load_test_data, monkeypatch):
    """Test async_validate_stac directly with a fully compliant STAC Item dict."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    item_dict = load_test_data("test_item.json")

    # Act & Assert: This should complete successfully and return a parsed Pydantic object
    result = await async_validate_stac(item_dict)
    assert result is not None
    assert hasattr(result, "id")


@pytest.mark.asyncio
async def test_async_validate_stac_invalid_item(load_test_data, monkeypatch):
    """Test async_validate_stac directly raises a ValueError for schema violations."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    item_dict = load_test_data("test_item.json")

    invalid_item = deepcopy(item_dict)
    invalid_item["id"] = "bad-unit-item"
    invalid_item["properties"]["eo:cloud_cover"] = 150  # Out of range (0-100)

    # Act & Assert: Should explicitly bubble up a translated ValueError
    with pytest.raises(ValueError) as exc_info:
        await async_validate_stac(invalid_item)

    assert "STAC validation failed for 'bad-unit-item'" in str(exc_info.value)


@pytest.mark.asyncio
async def test_async_validate_batch_disabled(load_test_data, monkeypatch):
    """Test that batch validation returns input items unmodified when disabled globally."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "false")
    item_dict = load_test_data("test_item.json")
    batch = [item_dict]

    valid_items, invalid_items = await async_validate_batch_with_stac_validator(batch)

    assert valid_items == batch
    assert invalid_items == {}


@pytest.mark.asyncio
async def test_async_validate_batch_with_stac_validator_mixed_results(
    load_test_data, monkeypatch
):
    """Test async_validate_batch returns a clean segmentation of valid vs invalid entries."""
    monkeypatch.setenv("ENABLE_STAC_VALIDATOR", "true")
    item_dict = load_test_data("test_item.json")

    valid_item = deepcopy(item_dict)
    valid_item["id"] = "valid-unit-batch-1"

    invalid_item = deepcopy(item_dict)
    invalid_item["id"] = "invalid-unit-batch-2"
    invalid_item["properties"]["eo:cloud_cover"] = -5  # Out of range (< 0)

    batch = [valid_item, invalid_item]

    # Act
    valid_output, invalid_output = await async_validate_batch_with_stac_validator(batch)

    # Assert valid output metrics
    assert len(valid_output) == 1
    assert valid_output[0]["id"] == "valid-unit-batch-1"

    # Assert invalid telemetry layout maps correctly (Error String -> Affected Item IDs)
    assert len(invalid_output) == 1
    error_message_key = list(invalid_output.keys())[0]

    assert "invalid-unit-batch-2" in invalid_output[error_message_key]
