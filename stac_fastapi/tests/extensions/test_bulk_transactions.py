import os
import uuid
from copy import deepcopy

import pytest
from pydantic import ValidationError

from stac_fastapi.extensions.third_party.bulk_transactions import Items
from stac_fastapi.types.errors import ConflictError

from ..conftest import MockRequest, create_item

if os.getenv("BACKEND", "elasticsearch").lower() == "opensearch":
    from stac_fastapi.opensearch.config import OpensearchSettings as SearchSettings
else:
    from stac_fastapi.elasticsearch.config import (
        ElasticsearchSettings as SearchSettings,
    )


@pytest.mark.asyncio
async def test_bulk_item_insert(ctx, core_client, txn_client, bulk_txn_client):
    items = {}
    for _ in range(10):
        _item = deepcopy(ctx.item)
        _item["id"] = str(uuid.uuid4())
        items[_item["id"]] = _item

    # fc = es_core.item_collection(coll["id"], request=MockStarletteRequest)
    # assert len(fc["features"]) == 0

    bulk_txn_client.bulk_item_insert(Items(items=items), refresh=True)

    fc = await core_client.item_collection(ctx.collection["id"], request=MockRequest())
    assert len(fc["features"]) >= 10


@pytest.mark.asyncio
async def test_bulk_item_insert_with_raise_on_error(
    ctx, core_client, txn_client, bulk_txn_client
):
    """
    Test bulk_item_insert behavior with RAISE_ON_BULK_ERROR set to true and false.

    This test verifies that when RAISE_ON_BULK_ERROR is set to true, a ConflictError
    is raised for conflicting items. When set to false, the operation logs errors
    and continues gracefully.
    """

    # Insert an initial item to set up a conflict
    initial_item = deepcopy(ctx.item)
    initial_item["id"] = str(uuid.uuid4())
    await create_item(txn_client, initial_item)

    # Verify the initial item is inserted
    fc = await core_client.item_collection(ctx.collection["id"], request=MockRequest())
    assert len(fc["features"]) >= 1

    # Create conflicting items (same ID as the initial item)
    conflicting_items = {initial_item["id"]: deepcopy(initial_item)}

    # Test with RAISE_ON_BULK_ERROR set to true
    os.environ["RAISE_ON_BULK_ERROR"] = "true"
    bulk_txn_client.database.sync_settings = SearchSettings()

    with pytest.raises(ConflictError):
        bulk_txn_client.bulk_item_insert(Items(items=conflicting_items), refresh=True)

    # Test with RAISE_ON_BULK_ERROR set to false
    os.environ["RAISE_ON_BULK_ERROR"] = "false"
    bulk_txn_client.database.sync_settings = SearchSettings()  # Reinitialize settings
    result = bulk_txn_client.bulk_item_insert(
        Items(items=conflicting_items), refresh=True
    )

    # Validate the results
    assert "Successfully added/updated 1 Items" in result

    # Clean up the inserted item
    await txn_client.delete_item(initial_item["id"], ctx.item["collection"])


@pytest.mark.asyncio
async def test_feature_collection_insert(
    core_client,
    txn_client,
    ctx,
):
    features = []
    for _ in range(10):
        _item = deepcopy(ctx.item)
        _item["id"] = str(uuid.uuid4())
        features.append(_item)

    feature_collection = {"type": "FeatureCollection", "features": features}

    await create_item(txn_client, feature_collection)

    fc = await core_client.item_collection(ctx.collection["id"], request=MockRequest())
    assert len(fc["features"]) >= 10


@pytest.mark.asyncio
async def test_bulk_item_insert_validation_error(ctx, core_client, bulk_txn_client):
    items = {}
    # Add 9 valid items
    for _ in range(9):
        _item = deepcopy(ctx.item)
        _item["id"] = str(uuid.uuid4())
        items[_item["id"]] = _item

    # Add 1 invalid item (e.g., missing "datetime")
    invalid_item = deepcopy(ctx.item)
    invalid_item["id"] = str(uuid.uuid4())
    invalid_item["properties"].pop(
        "datetime", None
    )  # Remove datetime to make it invalid
    items[invalid_item["id"]] = invalid_item

    # The bulk insert should raise a ValidationError due to the invalid item
    with pytest.raises(ValidationError):
        bulk_txn_client.bulk_item_insert(Items(items=items), refresh=True)


@pytest.mark.asyncio
async def test_feature_collection_insert_validation_error(
    core_client,
    txn_client,
    ctx,
):
    features = []
    # Add 9 valid items
    for _ in range(9):
        _item = deepcopy(ctx.item)
        _item["id"] = str(uuid.uuid4())
        features.append(_item)

    # Add 1 invalid item (e.g., missing "datetime")
    invalid_item = deepcopy(ctx.item)
    invalid_item["id"] = str(uuid.uuid4())
    invalid_item["properties"].pop(
        "datetime", None
    )  # Remove datetime to make it invalid
    features.append(invalid_item)

    feature_collection = {"type": "FeatureCollection", "features": features}

    # Assert that a ValidationError is raised due to the invalid item
    with pytest.raises(ValidationError):
        await create_item(txn_client, feature_collection)


@pytest.mark.asyncio
async def test_feature_collection_insert_duplicate_detection(
    ctx, core_client, txn_client
):
    """
    Test that duplicate items are detected when inserting via FeatureCollection.

    This test verifies that when an item already exists in the collection,
    attempting to insert the same item ID via FeatureCollection will raise
    a ConflictError (when RAISE_ON_BULK_ERROR is true).
    """
    # ctx.item is already created in the fixture
    existing_item_id = ctx.item["id"]

    # Create a FeatureCollection with a duplicate item (same ID as existing)
    duplicate_item = deepcopy(ctx.item)
    # Change datetime to simulate the scenario where same item ID is sent with different datetime
    duplicate_item["properties"]["datetime"] = "2025-06-15T00:00:00Z"

    feature_collection = {"type": "FeatureCollection", "features": [duplicate_item]}

    # Set RAISE_ON_BULK_ERROR to true to get ConflictError
    os.environ["RAISE_ON_BULK_ERROR"] = "true"
    txn_client.database.sync_settings = SearchSettings()

    # Should raise ConflictError because item already exists
    with pytest.raises(ConflictError) as exc_info:
        await create_item(txn_client, feature_collection)

    assert existing_item_id in str(exc_info.value)


@pytest.mark.asyncio
async def test_feature_collection_insert_duplicate_with_different_datetime(
    ctx, core_client, txn_client
):
    """
    Test that duplicate detection works when item has different datetime.

    This test specifically verifies the fix for the datetime index filtering issue
    where items with different datetime values could potentially bypass duplicate
    detection when stored in different datetime-based indexes.
    """
    existing_item_id = ctx.item["id"]

    # Create a duplicate item with significantly different datetime
    duplicate_item = deepcopy(ctx.item)
    duplicate_item["properties"]["datetime"] = "2030-12-31T23:59:59Z"

    feature_collection = {"type": "FeatureCollection", "features": [duplicate_item]}

    os.environ["RAISE_ON_BULK_ERROR"] = "true"
    txn_client.database.sync_settings = SearchSettings()

    # Should still detect the duplicate even with different datetime
    with pytest.raises(ConflictError) as exc_info:
        await create_item(txn_client, feature_collection)

    assert existing_item_id in str(exc_info.value)


@pytest.mark.asyncio
async def test_bulk_sync_duplicate_detection(
    ctx, core_client, txn_client, bulk_txn_client
):
    """
    Test that bulk_sync_prep_create_item properly detects duplicates across indexes.

    This test verifies that the synchronous bulk insert also correctly detects
    duplicates when an item with the same ID already exists in the collection.
    """
    existing_item_id = ctx.item["id"]

    # Create a duplicate item with different datetime
    duplicate_item = deepcopy(ctx.item)
    duplicate_item["properties"]["datetime"] = "2028-07-20T12:00:00Z"

    conflicting_items = {existing_item_id: duplicate_item}

    # Test with RAISE_ON_BULK_ERROR set to true
    os.environ["RAISE_ON_BULK_ERROR"] = "true"
    bulk_txn_client.database.sync_settings = SearchSettings()

    with pytest.raises(ConflictError) as exc_info:
        bulk_txn_client.bulk_item_insert(Items(items=conflicting_items), refresh=True)

    assert existing_item_id in str(exc_info.value)


@pytest.mark.asyncio
async def test_bulk_insert_multiple_items_with_one_duplicate(
    ctx, core_client, txn_client, bulk_txn_client
):
    """
    Test bulk insert behavior when one item out of many is a duplicate.

    When RAISE_ON_BULK_ERROR is true, the entire batch should fail if any
    item is a duplicate.
    """
    existing_item_id = ctx.item["id"]

    # Create items: 2 new + 1 duplicate
    items = {}

    # Add 2 new valid items
    for i in range(2):
        new_item = deepcopy(ctx.item)
        new_item["id"] = str(uuid.uuid4())
        items[new_item["id"]] = new_item

    # Add 1 duplicate item (with different datetime)
    duplicate_item = deepcopy(ctx.item)
    duplicate_item["properties"]["datetime"] = "2027-03-15T08:30:00Z"
    items[existing_item_id] = duplicate_item

    os.environ["RAISE_ON_BULK_ERROR"] = "true"
    bulk_txn_client.database.sync_settings = SearchSettings()

    # Should fail on the duplicate
    with pytest.raises(ConflictError) as exc_info:
        bulk_txn_client.bulk_item_insert(Items(items=items), refresh=True)

    assert existing_item_id in str(exc_info.value)
