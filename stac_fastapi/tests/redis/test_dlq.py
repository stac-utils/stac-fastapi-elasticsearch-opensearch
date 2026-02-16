import asyncio
import json
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

_repo_root = Path(__file__).resolve()
while _repo_root != _repo_root.parent and not (_repo_root / "scripts").is_dir():
    _repo_root = _repo_root.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts.item_queue_worker import ItemQueueWorker  # noqa: E402
from stac_fastapi.core.redis_utils import AsyncRedisQueueManager, connect_redis  # noqa: E402

from ..conftest import DatabaseLogic, create_collection  # noqa: E402

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def _load_file(filename):
    with open(os.path.join(DATA_DIR, filename)) as f:
        return json.load(f)


def _make_db():
    db = DatabaseLogic()
    db.async_settings.raise_on_bulk_error = False
    return db


def _make_worker(queue_manager, db):
    worker = ItemQueueWorker.__new__(ItemQueueWorker)
    worker.settings = queue_manager.queue_settings
    worker.queue_manager = queue_manager
    worker.db = db
    worker._states = {}
    worker._lock = asyncio.Lock()
    worker._semaphore = asyncio.Semaphore(4)
    worker.running = True
    return worker


@pytest_asyncio.fixture
async def queue_manager():
    redis = await connect_redis()
    if redis is None:
        pytest.skip("Redis not configured")

    manager = AsyncRedisQueueManager(redis)
    prefix = f"test_dlq_{uuid.uuid4().hex[:8]}"
    manager.queue_settings = type(
        "S",
        (),
        {
            "QUEUE_KEY_PREFIX": prefix,
            "QUEUE_BATCH_SIZE": 10,
            "QUEUE_FLUSH_INTERVAL": 30,
            "WORKER_POLL_INTERVAL": 1.0,
            "WORKER_MAX_THREADS": 4,
            "BACKEND": "opensearch",
            "LOG_LEVEL": "DEBUG",
        },
    )()

    yield manager

    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor, match=f"{prefix}:*", count=100)
        if keys:
            await redis.delete(*keys)
        if cursor == 0:
            break
    await redis.aclose()


@pytest_asyncio.fixture
async def dlq_collection(txn_client):
    collection = _load_file("test_collection.json")
    collection["id"] = f"dlq-test-{uuid.uuid4().hex[:8]}"
    await create_collection(txn_client, collection)
    yield collection
    await txn_client.delete_collection(collection["id"])


def _valid_item(collection_id, item_id=None):
    item = _load_file("test_item.json")
    item["id"] = item_id or str(uuid.uuid4())
    item["collection"] = collection_id
    return item


def _broken_item(collection_id, item_id=None):
    item = _valid_item(collection_id, item_id)
    item["geometry"] = "not-a-geometry"
    return item


def test_extract_empty():
    assert ItemQueueWorker._extract_failed_item_ids([]) == set()


def test_extract_missing_index_key():
    assert (
        ItemQueueWorker._extract_failed_item_ids([{"delete": {"_id": "x|c"}}]) == set()
    )


def test_extract_missing_id():
    assert ItemQueueWorker._extract_failed_item_ids([{"index": {}}]) == set()


@pytest.mark.asyncio
async def test_save_failed_items(queue_manager):
    await queue_manager.save_failed_items("col-a", ["item-1", "item-2"])

    failed_key = queue_manager._get_failed_set_key("col-a")
    members = await queue_manager.redis.smembers(failed_key)
    assert members == {"item-1", "item-2"}

    collections_key = queue_manager._get_failed_collections_key()
    cols = await queue_manager.redis.smembers(collections_key)
    assert "col-a" in cols


@pytest.mark.asyncio
async def test_save_failed_items_idempotent(queue_manager):
    await queue_manager.save_failed_items("col-a", ["item-1"])
    await queue_manager.save_failed_items("col-a", ["item-1", "item-2"])

    failed_key = queue_manager._get_failed_set_key("col-a")
    members = await queue_manager.redis.smembers(failed_key)
    assert members == {"item-1", "item-2"}


@pytest.mark.asyncio
async def test_save_failed_items_empty_list(queue_manager):
    await queue_manager.save_failed_items("col-a", [])

    failed_key = queue_manager._get_failed_set_key("col-a")
    exists = await queue_manager.redis.exists(failed_key)
    assert exists == 0


@pytest.mark.asyncio
async def test_flush_all_succeed(queue_manager, dlq_collection):
    col = dlq_collection["id"]
    db = _make_db()

    items = [_valid_item(col) for _ in range(3)]
    await queue_manager.queue_items(col, items)

    worker = _make_worker(queue_manager, db)
    await worker._flush_collection(col)

    assert await queue_manager.get_queue_length(col) == 0

    failed_key = queue_manager._get_failed_set_key(col)
    assert await queue_manager.redis.scard(failed_key) == 0


@pytest.mark.asyncio
async def test_flush_partial_failure(queue_manager, dlq_collection):
    col = dlq_collection["id"]
    db = _make_db()

    good_1 = _valid_item(col)
    good_2 = _valid_item(col)
    bad = _broken_item(col)

    items = [good_1, bad, good_2]
    await queue_manager.queue_items(col, items)

    worker = _make_worker(queue_manager, db)
    await worker._flush_collection(col)

    assert await queue_manager.get_queue_length(col) == 0

    failed_key = queue_manager._get_failed_set_key(col)
    failed_members = await queue_manager.redis.smembers(failed_key)
    assert failed_members == {bad["id"]}

    cols = await queue_manager.redis.smembers(
        queue_manager._get_failed_collections_key()
    )
    assert col in cols


@pytest.mark.asyncio
async def test_flush_all_fail(queue_manager, dlq_collection):
    col = dlq_collection["id"]
    db = _make_db()

    items = [_broken_item(col) for _ in range(3)]
    await queue_manager.queue_items(col, items)

    worker = _make_worker(queue_manager, db)
    await worker._flush_collection(col)

    assert await queue_manager.get_queue_length(col) == 0

    failed_key = queue_manager._get_failed_set_key(col)
    failed_members = await queue_manager.redis.smembers(failed_key)
    assert failed_members == {item["id"] for item in items}


@pytest.mark.asyncio
async def test_flush_dlq_save_failure_keeps_failed_in_pending(
    queue_manager, dlq_collection
):
    col = dlq_collection["id"]
    db = _make_db()

    good = _valid_item(col)
    bad = _broken_item(col)

    items = [good, bad]
    await queue_manager.queue_items(col, items)

    worker = _make_worker(queue_manager, db)

    original_save = queue_manager.save_failed_items
    queue_manager.save_failed_items = AsyncMock(
        side_effect=ConnectionError("redis down")
    )

    await worker._flush_collection(col)

    pending_ids = await queue_manager.get_pending_item_ids(col)
    assert good["id"] not in pending_ids
    assert bad["id"] in pending_ids

    failed_key = queue_manager._get_failed_set_key(col)
    assert await queue_manager.redis.scard(failed_key) == 0

    queue_manager.save_failed_items = original_save
