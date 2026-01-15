"""Tests for database utility functions."""

import uuid
from copy import deepcopy

import pytest

from stac_fastapi.sfeos_helpers.database import (
    check_item_exists_in_alias,
    check_item_exists_in_alias_sync,
    index_alias_by_collection_id,
    mk_item_id,
)

from ..conftest import create_item, database


@pytest.mark.asyncio
async def test_check_item_exists_in_alias_returns_true_when_exists(ctx, txn_client):
    """Test that check_item_exists_in_alias returns True when item exists."""
    collection_id = ctx.collection["id"]
    item_id = ctx.item["id"]

    alias = index_alias_by_collection_id(collection_id)
    doc_id = mk_item_id(item_id, collection_id)
    assert doc_id == f"{item_id}|{collection_id}"

    result = await check_item_exists_in_alias(database.client, alias, doc_id)

    assert result is True


@pytest.mark.asyncio
async def test_check_item_exists_in_alias_returns_false_when_not_exists(ctx):
    """Test that check_item_exists_in_alias returns False when item doesn't exist."""
    collection_id = ctx.collection["id"]
    non_existent_item_id = str(uuid.uuid4())

    alias = index_alias_by_collection_id(collection_id)
    doc_id = mk_item_id(non_existent_item_id, collection_id)
    assert doc_id == f"{non_existent_item_id}|{collection_id}"

    result = await check_item_exists_in_alias(database.client, alias, doc_id)

    assert result is False


@pytest.mark.asyncio
async def test_check_item_exists_in_alias_sync_returns_true_when_exists(ctx):
    """Test that check_item_exists_in_alias_sync returns True when item exists."""
    collection_id = ctx.collection["id"]
    item_id = ctx.item["id"]

    alias = index_alias_by_collection_id(collection_id)
    doc_id = mk_item_id(item_id, collection_id)
    assert doc_id == f"{item_id}|{collection_id}"

    result = check_item_exists_in_alias_sync(database.sync_client, alias, doc_id)

    assert result is True


@pytest.mark.asyncio
async def test_check_item_exists_in_alias_sync_returns_false_when_not_exists(ctx):
    """Test that check_item_exists_in_alias_sync returns False when item doesn't exist."""
    collection_id = ctx.collection["id"]
    non_existent_item_id = str(uuid.uuid4())

    alias = index_alias_by_collection_id(collection_id)
    doc_id = mk_item_id(non_existent_item_id, collection_id)
    assert doc_id == f"{non_existent_item_id}|{collection_id}"

    result = check_item_exists_in_alias_sync(database.sync_client, alias, doc_id)

    assert result is False


@pytest.mark.asyncio
async def test_check_item_exists_in_alias_with_multiple_items(ctx, txn_client):
    """Test check_item_exists_in_alias works correctly with multiple items in collection."""
    collection_id = ctx.collection["id"]
    alias = index_alias_by_collection_id(collection_id)

    # Create additional items
    additional_item_ids = []
    for i in range(3):
        new_item = deepcopy(ctx.item)
        new_item["id"] = str(uuid.uuid4())
        await create_item(txn_client, new_item)
        additional_item_ids.append(new_item["id"])

    original_doc_id = mk_item_id(ctx.item["id"], collection_id)
    assert original_doc_id == f"{ctx.item['id']}|{collection_id}"
    assert (
        await check_item_exists_in_alias(database.client, alias, original_doc_id)
        is True
    )

    for item_id in additional_item_ids:
        doc_id = mk_item_id(item_id, collection_id)
        assert doc_id == f"{item_id}|{collection_id}"
        assert await check_item_exists_in_alias(database.client, alias, doc_id) is True

    non_existent_item_id = str(uuid.uuid4())
    non_existent_doc_id = mk_item_id(non_existent_item_id, collection_id)
    assert non_existent_doc_id == f"{non_existent_item_id}|{collection_id}"
    assert (
        await check_item_exists_in_alias(database.client, alias, non_existent_doc_id)
        is False
    )


@pytest.mark.asyncio
async def test_check_item_exists_with_different_datetime(ctx, txn_client):
    """
    Test that check_item_exists_in_alias finds items regardless of datetime value.

    This test verifies the core functionality that the optimized search query
    correctly finds items across different datetime partitions (when datetime
    index filtering is enabled).
    """
    collection_id = ctx.collection["id"]
    alias = index_alias_by_collection_id(collection_id)

    # Create an item with a significantly different datetime
    new_item = deepcopy(ctx.item)
    new_item["id"] = str(uuid.uuid4())
    new_item["properties"]["datetime"] = "2030-12-31T23:59:59Z"
    await create_item(txn_client, new_item)

    # Create another item with a different datetime
    another_item = deepcopy(ctx.item)
    another_item["id"] = str(uuid.uuid4())
    another_item["properties"]["datetime"] = "2020-01-01T00:00:00Z"
    await create_item(txn_client, another_item)

    doc_id_1 = mk_item_id(new_item["id"], collection_id)
    doc_id_2 = mk_item_id(another_item["id"], collection_id)
    assert doc_id_1 == f"{new_item['id']}|{collection_id}"
    assert doc_id_2 == f"{another_item['id']}|{collection_id}"

    assert await check_item_exists_in_alias(database.client, alias, doc_id_1) is True
    assert await check_item_exists_in_alias(database.client, alias, doc_id_2) is True

    # Sync versions should also work
    assert (
        check_item_exists_in_alias_sync(database.sync_client, alias, doc_id_1) is True
    )
    assert (
        check_item_exists_in_alias_sync(database.sync_client, alias, doc_id_2) is True
    )
