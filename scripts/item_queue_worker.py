"""Redis queue worker for batch processing STAC items into the search engine.

Pulls items from Redis queues and inserts them into Elasticsearch/OpenSearch
in configurable batches. Different collections are processed concurrently,
but items within the same collection are processed sequentially.

Configuration via environment variables (managed by ItemQueueSettings):
    QUEUE_BATCH_SIZE (int): Number of items to trigger a flush (default: 50).
    QUEUE_FLUSH_INTERVAL (int): Seconds before flushing a partial batch (default: 30).
    WORKER_POLL_INTERVAL (float): Seconds between poll cycles (default: 1.0).
    WORKER_MAX_THREADS (int): Max concurrent collection flushes (default: 4).
    BACKEND (str): "opensearch" or "elasticsearch" (default: "opensearch").
    LOG_LEVEL (str): Logging level (default: "INFO").
"""

import asyncio
import logging
import time

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import LockError

from stac_fastapi.core.redis_utils import AsyncRedisQueueManager, ItemQueueSettings
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.core.validate import async_validate_batch_with_stac_validator

logger = logging.getLogger(__name__)


class CollectionFlushState:
    """Per-collection state for tracking flush timing and preventing concurrent flushes."""

    __slots__ = ("last_flush_time", "processing")

    def __init__(self) -> None:
        self.last_flush_time: float = time.monotonic()
        self.processing: bool = False


class ItemQueueWorker:
    """Worker that drains Redis item queues into the search engine in batches.

    Collections are processed concurrently via asyncio tasks. Within a single
    collection, batches are processed sequentially (one batch finishes before
    the next starts).
    """

    _LOCK_TIMEOUT = 300

    def __init__(self) -> None:
        self.settings = ItemQueueSettings()
        self.queue_manager: AsyncRedisQueueManager = None  # type: ignore[assignment]
        self.db = self._create_database_logic()
        self._states: dict[str, CollectionFlushState] = {}
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(self.settings.WORKER_MAX_THREADS)
        self.running = True

    async def _init_queue_manager(self) -> None:
        """Initialize the async Redis queue manager."""
        self.queue_manager = await AsyncRedisQueueManager.create()

    def _create_database_logic(self):  # type: ignore[no-untyped-def]
        """Create the appropriate DatabaseLogic based on BACKEND setting."""
        if self.settings.BACKEND == "elasticsearch":
            from stac_fastapi.elasticsearch.database_logic import DatabaseLogic
        else:
            from stac_fastapi.opensearch.database_logic import DatabaseLogic
        return DatabaseLogic()

    def _get_state(self, collection_id: str) -> CollectionFlushState:
        if collection_id not in self._states:
            self._states[collection_id] = CollectionFlushState()
        return self._states[collection_id]

    @staticmethod
    def _extract_failed_item_ids(errors: list[dict]) -> set[str]:
        """Extract item IDs from bulk_async error responses.

        Each error dict has the shape:
            {"<op_type>": {"_id": "item_id|collection_id", ...}}
        where op_type is one of: index, create, update, delete.

        Returns:
            Set of failed item IDs.
        """
        failed: set[str] = set()
        for error in errors:
            for op_type in ("index", "create", "update", "delete"):
                info = error.get(op_type)
                if info:
                    doc_id = info.get("_id", "")
                    if doc_id:
                        failed.add(doc_id.split("|")[0])
                    break
        return failed

    async def _should_flush(self, collection_id: str) -> bool:
        """Determine whether a collection's queue should be flushed.

        Returns True when the queue length >= batch size, or when the flush
        interval has elapsed and there are items waiting.
        """
        queue_length = await self.queue_manager.get_queue_length(collection_id)
        if queue_length == 0:
            return False

        if queue_length >= self.settings.QUEUE_BATCH_SIZE:
            return True

        state = self._get_state(collection_id)
        elapsed = time.monotonic() - state.last_flush_time
        return elapsed >= self.settings.QUEUE_FLUSH_INTERVAL

    def _get_collection_lock_key(self, collection_id: str) -> str:
        """Get Redis key for a collection's distributed lock."""
        return (
            f"{self.queue_manager.queue_settings.QUEUE_KEY_PREFIX}:lock:{collection_id}"
        )

    async def _lock_refresh_task(
        self,
        lock,
        interval: float = 60.0,
        lock_lost: asyncio.Event | None = None,
    ) -> None:
        """Periodically extend the distributed lock's TTL.

        Runs as a background asyncio task; cancelled by the caller when
        processing ends. Sets lock_lost event when extend fails so the
        processing loop can stop promptly.
        """
        while True:
            await asyncio.sleep(interval)
            try:
                await lock.extend(additional_time=self._LOCK_TIMEOUT, replace_ttl=True)
                logger.debug(f"Lock extended: {lock.name}")
            except LockError:
                logger.warning(
                    f"Lock lost (deleted or acquired by another process): {lock.name}",
                    exc_info=True,
                )
                if lock_lost is not None:
                    lock_lost.set()
                break
            except (RedisConnectionError, OSError):
                logger.error(
                    f"Redis connection error while extending lock: {lock.name}",
                    exc_info=True,
                )
                if lock_lost is not None:
                    lock_lost.set()
                break
            except Exception:
                logger.error(
                    f"Unexpected error extending lock: {lock.name}",
                    exc_info=True,
                )
                if lock_lost is not None:
                    lock_lost.set()
                break

    async def _flush_collection(self, collection_id: str) -> None:
        """Flush pending items for a collection in sequential batches.

        Acquires a Redis distributed lock per collection to prevent concurrent
        flushes across multiple worker processes. Keeps draining the queue in
        batch_size chunks until fewer than batch_size items remain.

        The lock TTL is periodically refreshed by a background task to prevent
        expiration during long-running batch processing.

        If strict validation is enabled via `ENABLE_STAC_VALIDATOR`, items are
        validated concurrently before database insertion. Invalid items are routed
        to the Dead Letter Queue (DLQ), and only valid items are inserted.
        """
        state = self._get_state(collection_id)

        async with self._lock:
            if state.processing:
                return
            state.processing = True

        lock_key = self._get_collection_lock_key(collection_id)
        redis_lock = self.queue_manager.redis.lock(
            lock_key,
            timeout=self._LOCK_TIMEOUT,
            blocking_timeout=5,
        )

        refresh_task = None
        lock_lost = asyncio.Event()
        try:
            if not await redis_lock.acquire(blocking=True):
                logger.info(
                    f"Collection '{collection_id}': skipping flush, another worker holds the lock"
                )
                return

            refresh_task = asyncio.create_task(
                self._lock_refresh_task(redis_lock, interval=60.0, lock_lost=lock_lost)
            )

            batch_size = self.settings.QUEUE_BATCH_SIZE
            batch_num = 0

            while self.running and not lock_lost.is_set():
                items = await self.queue_manager.get_pending_items(
                    collection_id, limit=batch_size
                )
                if not items:
                    break

                batch_num += 1

                logger.info(
                    f"Collection '{collection_id}' batch #{batch_num}: pulled {len(items)} items from queue"
                )

                # VALIDATION LAYER: Use batch validation for efficiency (if enabled)
                if get_bool_env("ENABLE_STAC_VALIDATOR"):
                    (
                        valid_items,
                        validation_errors,
                    ) = await async_validate_batch_with_stac_validator(items)

                    # Extract invalid item IDs from grouped validation errors
                    invalid_item_ids = set()
                    for error_msg, item_ids in validation_errors.items():
                        for item_id in item_ids:
                            invalid_item_ids.add(item_id)
                            logger.error(
                                f"Worker validation failed for '{item_id}' in collection '{collection_id}': {error_msg}"
                            )
                else:
                    # Skip STAC validation when disabled
                    valid_items = items
                    invalid_item_ids = set()

                # Handle invalid items (Dead Letter Queue)
                if invalid_item_ids:
                    try:
                        await self.queue_manager.save_failed_items(
                            collection_id, list(invalid_item_ids)
                        )
                        await self.queue_manager.mark_items_processed(
                            collection_id, list(invalid_item_ids)
                        )
                    except Exception:
                        logger.exception(
                            f"Collection '{collection_id}': failed to save {len(invalid_item_ids)} invalid items to DLQ"
                        )

                # If entire batch was invalid, skip database call
                if not valid_items:
                    logger.warning(
                        f"Collection '{collection_id}' batch #{batch_num}: All {len(items)} items failed STAC validation. Skipping DB insert."
                    )
                    state.last_flush_time = time.monotonic()
                    if len(items) < batch_size:
                        break
                    continue

                # DATABASE INSERTION: Only valid items reach the database
                try:
                    success, errors = await self.db.bulk_async(
                        collection_id=collection_id,
                        processed_items=valid_items,
                        op_type="index",
                    )
                except Exception:
                    logger.exception(
                        f"Collection '{collection_id}' batch #{batch_num}: bulk_async failed ({len(valid_items)} valid items)"
                    )
                    break

                # Handle database errors
                failed_db_ids = (
                    self._extract_failed_item_ids(errors) if errors else set()
                )
                successful_db_ids = [
                    item["id"]
                    for item in valid_items
                    if item["id"] not in failed_db_ids
                ]

                if errors:
                    logger.error(
                        f"Collection '{collection_id}' batch #{batch_num}: "
                        f"{len(failed_db_ids)} DB insert(s) failed, saving to DLQ. "
                        f"Bulk errors: {errors}"
                    )

                if successful_db_ids:
                    await self.queue_manager.mark_items_processed(
                        collection_id, successful_db_ids
                    )

                if failed_db_ids:
                    try:
                        await self.queue_manager.save_failed_items(
                            collection_id, list(failed_db_ids)
                        )
                        await self.queue_manager.mark_items_processed(
                            collection_id, list(failed_db_ids)
                        )
                    except Exception:
                        logger.exception(
                            f"Collection '{collection_id}': failed to save {len(failed_db_ids)} DB failures to DLQ"
                        )

                logger.info(
                    f"Collection '{collection_id}' batch #{batch_num}: {success} succeeded DB insert, "
                    f"{len(invalid_item_ids)} failed STAC validation, {len(failed_db_ids)} failed DB insert."
                )

                state.last_flush_time = time.monotonic()

                if len(items) < batch_size:
                    break

        except Exception:
            logger.exception(f"Unexpected error flushing collection '{collection_id}'")
        finally:
            if refresh_task is not None:
                refresh_task.cancel()
                try:
                    await refresh_task
                except asyncio.CancelledError:
                    pass
            try:
                if await redis_lock.owned():
                    await redis_lock.release()
            except Exception:
                pass
            async with self._lock:
                state.processing = False

    async def _flush_with_semaphore(self, collection_id: str) -> None:
        """Flush a collection while respecting the concurrency limit."""
        async with self._semaphore:
            await self._flush_collection(collection_id)

    async def run(self) -> None:
        """Main worker loop — polls Redis and dispatches collection flushes."""
        await self._init_queue_manager()

        logger.info(
            f"Starting item queue worker "
            f"(batch_size={self.settings.QUEUE_BATCH_SIZE}, flush_interval={self.settings.QUEUE_FLUSH_INTERVAL}s, "
            f"poll_interval={self.settings.WORKER_POLL_INTERVAL:.1f}s, max_concurrent={self.settings.WORKER_MAX_THREADS})"
        )

        active_tasks: dict[str, asyncio.Task] = {}

        while self.running:
            try:
                collections = await self.queue_manager.get_pending_collections()

                done_keys: set[str] = set()
                for cid, task in active_tasks.items():
                    if task.done():
                        done_keys.add(cid)
                for cid in done_keys:
                    del active_tasks[cid]

                for collection_id in collections:
                    if not self.running:
                        break

                    if collection_id in active_tasks:
                        continue

                    if await self._should_flush(collection_id):
                        active_tasks[collection_id] = asyncio.create_task(
                            self._flush_with_semaphore(collection_id)
                        )

            except Exception:
                logger.exception("Error in worker poll loop")

            await asyncio.sleep(self.settings.WORKER_POLL_INTERVAL)

        for task in active_tasks.values():
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks.values(), return_exceptions=True)

        logger.info("Worker stopped.")

    async def stop(self) -> None:
        """Signal the worker to shut down gracefully."""
        logger.info("Shutting down worker...")
        self.running = False
        if self.queue_manager:
            await self.queue_manager.close()


def main() -> None:
    """Entry point for the item queue worker."""
    settings = ItemQueueSettings()

    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    worker = ItemQueueWorker()
    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
