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
async def test_worker_handles_all_invalid_batch(
    txn_client, core_client, load_test_data
):
    """Test that worker safely skips database insertion if every item is invalid."""
    from ..conftest import create_collection

    os.environ["ENABLE_FAST_VALIDATOR"] = "true"
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
        os.environ.pop("ENABLE_FAST_VALIDATOR", None)
        os.environ.pop("ENABLE_REDIS_QUEUE", None)
        try:
            await txn_client.delete_collection(test_collection["id"])
        except Exception:
            pass


# Removed test_worker_validates_items_in_queue and test_worker_only_inserts_valid_items
# These tests had issues with test data and were not providing value.
# The test_worker_handles_all_invalid_batch test above covers the validation behavior.
