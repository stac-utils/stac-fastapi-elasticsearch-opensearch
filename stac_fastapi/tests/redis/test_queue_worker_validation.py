"""Tests for Redis queue worker validation.

Tests that the background worker correctly validates items pulled from the queue,
sends invalid items to the DLQ, and only inserts valid items into the database.
"""

import os
import uuid
from copy import deepcopy

import pytest

from scripts.item_queue_worker import ItemQueueWorker  # noqa: E402
from stac_fastapi.core.redis_utils import AsyncRedisQueueManager


@pytest.mark.asyncio
async def test_worker_validates_items_in_queue(txn_client, core_client, load_test_data):
    """Test that worker validates items pulled from queue and sends invalid to DLQ."""
    from ..conftest import create_collection

    os.environ["ENABLE_STAC_VALIDATOR"] = "true"
    os.environ["ENABLE_REDIS_QUEUE"] = "true"

    try:
        # Create a test collection
        test_collection = load_test_data("test_collection.json")
        test_collection["id"] = f"test-collection-worker-{uuid.uuid4()}"
        await create_collection(txn_client, test_collection)

        # Create a base item
        base_item = load_test_data("test_item.json")
        base_item["collection"] = test_collection["id"]
        if "datetime" not in base_item.get("properties", {}):
            base_item["properties"]["datetime"] = "2020-01-01T00:00:00Z"

        # Valid item 1
        valid_item_1 = deepcopy(base_item)
        valid_item_1["id"] = "valid-item-1"

        # Valid item 2
        valid_item_2 = deepcopy(base_item)
        valid_item_2["id"] = "valid-item-2"

        # Invalid item (STAC validator error)
        invalid_item = deepcopy(base_item)
        invalid_item["id"] = "invalid-item-worker"
        invalid_item["stac_extensions"] = [
            "https://stac-extensions.github.io/eo/v2.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        ]

        feature_collection = {
            "type": "FeatureCollection",
            "features": [valid_item_1, valid_item_2, invalid_item],
        }

        # 1. Queue the items
        from ..conftest import create_item

        result = await create_item(txn_client, feature_collection)
        if result is not None:
            assert "queued" in result.lower()

        # 2. Verify they actually made it to the queue
        queue_manager = await AsyncRedisQueueManager.create()
        try:
            pending_items = await queue_manager.get_pending_items(test_collection["id"])
            assert len(pending_items) == 3, "Items were not successfully queued"

            # 3. RUN THE REAL WORKER
            worker = ItemQueueWorker()
            await worker._init_queue_manager()
            try:
                # Tell the worker to process the collection
                await worker._flush_collection(test_collection["id"])
            finally:
                await worker.queue_manager.close()

            # 4. Verify DLQ has the invalid item (Direct Redis query!)
            failed_key = queue_manager._get_failed_set_key(test_collection["id"])
            failed_ids = await queue_manager.redis.smembers(failed_key)

            # Redis might return bytes, safely decode them
            failed_ids_str = {
                fid.decode("utf-8") if isinstance(fid, bytes) else fid
                for fid in failed_ids
            }
            assert (
                "invalid-item-worker" in failed_ids_str
            ), "Invalid item was not sent to DLQ"
            assert len(failed_ids_str) == 1

            # 5. Verify Database has ONLY the valid items
            db_items, _, _ = await core_client.database.execute_search(
                search=core_client.database.make_search(),
                limit=10,
                token=None,
                sort=None,
                collection_ids=[test_collection["id"]],
                datetime_search="",
            )
            db_item_ids = {item["id"] for item in list(db_items)}

            assert "valid-item-1" in db_item_ids
            assert "valid-item-2" in db_item_ids
            assert "invalid-item-worker" not in db_item_ids

        finally:
            await queue_manager.close()

    finally:
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        os.environ.pop("ENABLE_REDIS_QUEUE", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_worker_only_inserts_valid_items(txn_client, core_client, load_test_data):
    """Test a mixed batch with multiple valid and invalid items."""
    from ..conftest import create_collection

    os.environ["ENABLE_STAC_VALIDATOR"] = "true"
    os.environ["ENABLE_REDIS_QUEUE"] = "true"

    try:
        test_collection = load_test_data("test_collection.json")
        test_collection["id"] = f"test-collection-db-{uuid.uuid4()}"
        await create_collection(txn_client, test_collection)

        base_item = load_test_data("test_item.json")
        base_item["collection"] = test_collection["id"]
        if "datetime" not in base_item.get("properties", {}):
            base_item["properties"]["datetime"] = "2020-01-01T00:00:00Z"

        items_to_queue = []

        # 3 Valid items
        for i in range(3):
            valid_item = deepcopy(base_item)
            valid_item["id"] = f"valid-item-{i}"
            items_to_queue.append(valid_item)

        # 2 Invalid items
        for i in range(2):
            invalid_item = deepcopy(base_item)
            invalid_item["id"] = f"invalid-item-{i}"
            invalid_item["stac_extensions"] = [
                "https://stac-extensions.github.io/eo/v2.0.0/schema.json"
            ]
            items_to_queue.append(invalid_item)

        feature_collection = {
            "type": "FeatureCollection",
            "features": items_to_queue,
        }

        # 1. Queue items
        from ..conftest import create_item

        result = await create_item(txn_client, feature_collection)
        if result is not None:
            assert "queued" in result.lower()

        queue_manager = await AsyncRedisQueueManager.create()
        try:
            pending_items = await queue_manager.get_pending_items(test_collection["id"])
            assert len(pending_items) == 5

            # 2. RUN THE REAL WORKER
            worker = ItemQueueWorker()
            await worker._init_queue_manager()
            try:
                await worker._flush_collection(test_collection["id"])
            finally:
                await worker.queue_manager.close()

            # 3. Verify Database
            db_items, _, _ = await core_client.database.execute_search(
                search=core_client.database.make_search(),
                limit=10,
                token=None,
                sort=None,
                collection_ids=[test_collection["id"]],
                datetime_search="",
            )
            db_item_ids = {item["id"] for item in list(db_items)}

            assert len(db_item_ids) == 3
            assert {"valid-item-0", "valid-item-1", "valid-item-2"}.issubset(
                db_item_ids
            )

            # 4. Verify DLQ (Direct Redis query!)
            failed_key = queue_manager._get_failed_set_key(test_collection["id"])
            failed_ids = await queue_manager.redis.smembers(failed_key)
            failed_ids_str = {
                fid.decode("utf-8") if isinstance(fid, bytes) else fid
                for fid in failed_ids
            }

            assert len(failed_ids_str) == 2
            assert {"invalid-item-0", "invalid-item-1"}.issubset(failed_ids_str)

        finally:
            await queue_manager.close()

    finally:
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        os.environ.pop("ENABLE_REDIS_QUEUE", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_worker_handles_all_invalid_batch(
    txn_client, core_client, load_test_data
):
    """Test that worker safely skips database insertion if every item is invalid."""
    from ..conftest import create_collection

    os.environ["ENABLE_STAC_VALIDATOR"] = "true"
    os.environ["ENABLE_REDIS_QUEUE"] = "true"

    try:
        test_collection = load_test_data("test_collection.json")
        test_collection["id"] = f"test-collection-all-invalid-{uuid.uuid4()}"
        await create_collection(txn_client, test_collection)

        base_item = load_test_data("test_item.json")
        base_item["collection"] = test_collection["id"]
        if "datetime" not in base_item.get("properties", {}):
            base_item["properties"]["datetime"] = "2020-01-01T00:00:00Z"

        # 3 entirely invalid items
        items_to_queue = []
        for i in range(3):
            invalid_item = deepcopy(base_item)
            invalid_item["id"] = f"completely-invalid-{i}"
            invalid_item["stac_extensions"] = [
                "https://stac-extensions.github.io/eo/v2.0.0/schema.json"
            ]
            items_to_queue.append(invalid_item)

        feature_collection = {
            "type": "FeatureCollection",
            "features": items_to_queue,
        }

        # 1. Queue items
        from ..conftest import create_item

        result = await create_item(txn_client, feature_collection)
        if result is not None:
            assert "queued" in result.lower()

        queue_manager = await AsyncRedisQueueManager.create()
        try:
            pending_items = await queue_manager.get_pending_items(test_collection["id"])
            assert len(pending_items) == 3

            # 2. RUN THE REAL WORKER
            worker = ItemQueueWorker()
            await worker._init_queue_manager()
            try:
                await worker._flush_collection(test_collection["id"])
            finally:
                await worker.queue_manager.close()

            # 3. Verify Database is empty for this collection
            db_items, _, _ = await core_client.database.execute_search(
                search=core_client.database.make_search(),
                limit=10,
                token=None,
                sort=None,
                collection_ids=[test_collection["id"]],
                datetime_search="",
            )
            db_items_list = list(db_items)
            assert (
                len(db_items_list) == 0
            ), "Database should be empty since all items were invalid"

            # 4. Verify DLQ has everything (Direct Redis query!)
            failed_key = queue_manager._get_failed_set_key(test_collection["id"])
            failed_ids = await queue_manager.redis.smembers(failed_key)
            failed_ids_str = {
                fid.decode("utf-8") if isinstance(fid, bytes) else fid
                for fid in failed_ids
            }

            assert len(failed_ids_str) == 3
            assert {
                "completely-invalid-0",
                "completely-invalid-1",
                "completely-invalid-2",
            }.issubset(failed_ids_str)

        finally:
            await queue_manager.close()

    finally:
        os.environ.pop("ENABLE_STAC_VALIDATOR", None)
        os.environ.pop("ENABLE_REDIS_QUEUE", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass
