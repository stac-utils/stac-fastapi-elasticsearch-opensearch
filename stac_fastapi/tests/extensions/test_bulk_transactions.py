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
