import asyncio
import json
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

_repo_root = Path(__file__).resolve()
while _repo_root != _repo_root.parent and not (_repo_root / "scripts").is_dir():
    _repo_root = _repo_root.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts.item_queue_worker import ItemQueueWorker  # noqa: E402
from stac_fastapi.core.redis_utils import (  # noqa: E402
    AsyncRedisQueueManager,
    connect_redis,
)

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


def test_extract_delete_op():
    assert ItemQueueWorker._extract_failed_item_ids([{"delete": {"_id": "x|c"}}]) == {
        "x"
    }


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


@pytest.mark.asyncio
async def test_queue_items_populates_all_structures(queue_manager):
    col = "col-atomic-1"
    items = [_valid_item(col) for _ in range(3)]

    length = await queue_manager.queue_items(col, items)
    assert length == 3

    zset_key = queue_manager._get_zset_key(col)
    data_key = queue_manager._get_data_key(col)
    collections_key = queue_manager._get_collections_set_key()

    assert await queue_manager.redis.zcard(zset_key) == 3
    assert await queue_manager.redis.hlen(data_key) == 3
    assert col in await queue_manager.redis.smembers(collections_key)


@pytest.mark.asyncio
async def test_queue_items_deduplicates(queue_manager):
    col = "col-dedup"
    item = _valid_item(col, "dup-id")

    await queue_manager.queue_items(col, [item])
    length = await queue_manager.queue_items(col, [item])
    assert length == 1

    data_key = queue_manager._get_data_key(col)
    assert await queue_manager.redis.hlen(data_key) == 1


@pytest.mark.asyncio
async def test_queue_items_empty_list_returns_zero(queue_manager):
    assert await queue_manager.queue_items("col-empty", []) == 0


@pytest.mark.asyncio
async def test_queue_items_skips_items_without_id(queue_manager):
    col = "col-no-id"
    good = _valid_item(col)
    bad = {"collection": col, "properties": {"datetime": "2024-01-01T00:00:00Z"}}

    length = await queue_manager.queue_items(col, [good, bad])
    assert length == 1


@pytest.mark.asyncio
async def test_mark_processed_removes_from_zset_and_hash(queue_manager):
    col = "col-mark-1"
    items = [_valid_item(col) for _ in range(3)]
    await queue_manager.queue_items(col, items)

    to_remove = [items[0]["id"], items[1]["id"]]
    remaining = await queue_manager.mark_items_processed(col, to_remove)
    assert remaining == 1

    zset_key = queue_manager._get_zset_key(col)
    data_key = queue_manager._get_data_key(col)

    zset_members = await queue_manager.redis.zrange(zset_key, 0, -1)
    assert set(zset_members) == {items[2]["id"]}

    hash_keys = await queue_manager.redis.hkeys(data_key)
    assert set(hash_keys) == {items[2]["id"]}


@pytest.mark.asyncio
async def test_mark_processed_no_ghosts_in_zset(queue_manager):
    col = "col-no-ghost"
    items = [_valid_item(col) for _ in range(5)]
    await queue_manager.queue_items(col, items)

    ids_to_remove = [it["id"] for it in items[:3]]
    await queue_manager.mark_items_processed(col, ids_to_remove)

    zset_key = queue_manager._get_zset_key(col)
    data_key = queue_manager._get_data_key(col)

    zset_ids = set(await queue_manager.redis.zrange(zset_key, 0, -1))
    hash_ids = set(await queue_manager.redis.hkeys(data_key))
    assert zset_ids == hash_ids


@pytest.mark.asyncio
async def test_mark_processed_cleans_up_when_empty(queue_manager):
    col = "col-cleanup"
    items = [_valid_item(col) for _ in range(2)]
    await queue_manager.queue_items(col, items)

    all_ids = [it["id"] for it in items]
    remaining = await queue_manager.mark_items_processed(col, all_ids)
    assert remaining == 0

    collections_key = queue_manager._get_collections_set_key()
    assert col not in await queue_manager.redis.smembers(collections_key)

    data_key = queue_manager._get_data_key(col)
    assert await queue_manager.redis.exists(data_key) == 0


@pytest.mark.asyncio
async def test_mark_processed_empty_list_is_noop(queue_manager):
    col = "col-noop"
    items = [_valid_item(col) for _ in range(2)]
    await queue_manager.queue_items(col, items)

    remaining = await queue_manager.mark_items_processed(col, [])
    assert remaining == 2


@pytest.mark.asyncio
async def test_mark_processed_nonexistent_ids(queue_manager):
    col = "col-nonexist"
    items = [_valid_item(col) for _ in range(2)]
    await queue_manager.queue_items(col, items)

    remaining = await queue_manager.mark_items_processed(
        col, ["fake-id-1", "fake-id-2"]
    )
    assert remaining == 2


@pytest.mark.asyncio
async def test_remove_item_removes_from_both_structures(queue_manager):
    col = "col-rem-1"
    items = [_valid_item(col) for _ in range(2)]
    await queue_manager.queue_items(col, items)

    result = await queue_manager.remove_item(col, items[0]["id"])
    assert result is True

    zset_key = queue_manager._get_zset_key(col)
    data_key = queue_manager._get_data_key(col)

    zset_ids = set(await queue_manager.redis.zrange(zset_key, 0, -1))
    hash_ids = set(await queue_manager.redis.hkeys(data_key))
    assert zset_ids == hash_ids == {items[1]["id"]}


@pytest.mark.asyncio
async def test_remove_item_cleans_up_when_last(queue_manager):
    col = "col-rem-last"
    item = _valid_item(col)
    await queue_manager.queue_items(col, [item])

    result = await queue_manager.remove_item(col, item["id"])
    assert result is True

    collections_key = queue_manager._get_collections_set_key()
    assert col not in await queue_manager.redis.smembers(collections_key)


@pytest.mark.asyncio
async def test_remove_item_nonexistent_returns_false(queue_manager):
    col = "col-rem-none"
    item = _valid_item(col)
    await queue_manager.queue_items(col, [item])

    result = await queue_manager.remove_item(col, "does-not-exist")
    assert result is False
    assert await queue_manager.get_queue_length(col) == 1


@pytest.mark.asyncio
async def test_concurrent_mark_no_ghosts(queue_manager):
    col = "col-concurrent"
    items = [_valid_item(col) for _ in range(10)]
    await queue_manager.queue_items(col, items)

    half1 = [it["id"] for it in items[:5]]
    half2 = [it["id"] for it in items[5:]]

    await asyncio.gather(
        queue_manager.mark_items_processed(col, half1),
        queue_manager.mark_items_processed(col, half2),
    )

    zset_key = queue_manager._get_zset_key(col)
    data_key = queue_manager._get_data_key(col)

    zset_ids = set(await queue_manager.redis.zrange(zset_key, 0, -1))
    hash_ids = set(await queue_manager.redis.hkeys(data_key))
    assert zset_ids == hash_ids


@pytest.mark.asyncio
async def test_concurrent_remove_no_ghosts(queue_manager):
    col = "col-conc-rem"
    items = [_valid_item(col) for _ in range(10)]
    await queue_manager.queue_items(col, items)

    await asyncio.gather(*[queue_manager.remove_item(col, it["id"]) for it in items])

    zset_key = queue_manager._get_zset_key(col)
    data_key = queue_manager._get_data_key(col)

    assert await queue_manager.redis.zcard(zset_key) == 0
    assert await queue_manager.redis.hlen(data_key) == 0


@pytest.mark.asyncio
async def test_lock_refresh_calls_extend():
    mock_lock = AsyncMock()
    mock_lock.name = "test-lock"
    mock_lock.extend = AsyncMock()

    worker = ItemQueueWorker.__new__(ItemQueueWorker)

    task = asyncio.create_task(worker._lock_refresh_task(mock_lock, interval=0.05))
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert mock_lock.extend.call_count >= 2
    mock_lock.extend.assert_called_with(additional_time=300, replace_ttl=True)


@pytest.mark.asyncio
async def test_lock_refresh_stops_on_extend_failure():
    mock_lock = AsyncMock()
    mock_lock.name = "test-lock"
    mock_lock.extend = AsyncMock(side_effect=Exception("lock lost"))

    worker = ItemQueueWorker.__new__(ItemQueueWorker)
    lock_lost = asyncio.Event()

    task = asyncio.create_task(
        worker._lock_refresh_task(mock_lock, interval=0.05, lock_lost=lock_lost)
    )
    await asyncio.sleep(0.15)

    assert task.done()
    assert lock_lost.is_set()
    await task


@pytest.mark.asyncio
async def test_lock_refresh_cancellation():
    mock_lock = AsyncMock()
    mock_lock.name = "test-lock"
    mock_lock.extend = AsyncMock()

    worker = ItemQueueWorker.__new__(ItemQueueWorker)

    task = asyncio.create_task(worker._lock_refresh_task(mock_lock, interval=100))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_flush_starts_and_cancels_refresh(queue_manager):
    col = "col-lock-refresh"
    items = [_valid_item(col) for _ in range(2)]
    await queue_manager.queue_items(col, items)

    worker = _make_worker(queue_manager, MagicMock())
    worker.db.bulk_async = AsyncMock(return_value=(2, []))

    with patch.object(
        worker, "_lock_refresh_task", wraps=worker._lock_refresh_task
    ) as mock_refresh:
        await worker._flush_collection(col)
        mock_refresh.assert_called_once()


@pytest.mark.asyncio
async def test_flush_release_checks_owned(queue_manager):
    col = "col-owned-check"
    items = [_valid_item(col)]
    await queue_manager.queue_items(col, items)

    worker = _make_worker(queue_manager, MagicMock())
    worker.db.bulk_async = AsyncMock(return_value=(1, []))

    await worker._flush_collection(col)

    lock_key = worker._get_collection_lock_key(col)
    assert await queue_manager.redis.exists(lock_key) == 0
