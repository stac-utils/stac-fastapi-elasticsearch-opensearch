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

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from typing import Dict

from stac_fastapi.core.redis_utils import ItemQueueSettings, SyncRedisQueueManager

logger = logging.getLogger(__name__)


class CollectionFlushState:
    """Per-collection state for tracking flush timing and preventing concurrent flushes."""

    __slots__ = ("last_flush_time", "processing")

    def __init__(self) -> None:
        self.last_flush_time: float = time.monotonic()
        self.processing: bool = False


class ItemQueueWorker:
    """Worker that drains Redis item queues into the search engine in batches.

    Collections are processed concurrently via a thread pool. Within a single
    collection, batches are processed sequentially (one batch finishes before
    the next starts).
    """

    def __init__(self) -> None:
        self.settings = ItemQueueSettings()
        self.queue_manager = SyncRedisQueueManager()
        self.db = self._create_database_logic()
        self._states: Dict[str, CollectionFlushState] = {}
        self._lock = Lock()
        self.running = True

    def _create_database_logic(self):  # type: ignore[no-untyped-def]
        """Create the appropriate DatabaseLogic based on BACKEND setting."""
        if self.settings.BACKEND == "elasticsearch":
            from stac_fastapi.elasticsearch.database_logic import DatabaseLogic
        else:
            from stac_fastapi.opensearch.database_logic import DatabaseLogic
        return DatabaseLogic()

    def _get_state(self, collection_id: str) -> CollectionFlushState:
        with self._lock:
            if collection_id not in self._states:
                self._states[collection_id] = CollectionFlushState()
            return self._states[collection_id]

    def _should_flush(self, collection_id: str) -> bool:
        """Determine whether a collection's queue should be flushed.

        Returns True when the queue length >= batch size, or when the flush
        interval has elapsed and there are items waiting.
        """
        queue_length = self.queue_manager.get_queue_length(collection_id)
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

    def _flush_collection(self, collection_id: str) -> None:
        """Flush pending items for a collection in sequential batches.

        Acquires a Redis distributed lock per collection to prevent concurrent
        flushes across multiple worker processes. Keeps draining the queue in
        batch_size chunks until fewer than batch_size items remain.
        """
        state = self._get_state(collection_id)

        with self._lock:
            if state.processing:
                return
            state.processing = True

        lock_key = self._get_collection_lock_key(collection_id)
        redis_lock = self.queue_manager.redis.lock(
            lock_key,
            timeout=300,
            blocking_timeout=5,
        )

        try:
            if not redis_lock.acquire(blocking=True):
                logger.info(
                    "Collection '%s': skipping flush, another worker holds the lock",
                    collection_id,
                )
                return

            batch_size = self.settings.QUEUE_BATCH_SIZE
            batch_num = 0

            while self.running:
                items = self.queue_manager.get_pending_items(
                    collection_id, limit=batch_size
                )
                if not items:
                    break

                batch_num += 1
                item_ids = [item["id"] for item in items]

                logger.info(
                    "Collection '%s' batch #%d: flushing %d items",
                    collection_id,
                    batch_num,
                    len(items),
                )

                try:
                    success, errors = self.db.bulk_sync(
                        collection_id=collection_id,
                        processed_items=items,
                    )
                except Exception:
                    logger.exception(
                        "Collection '%s' batch #%d: bulk_sync failed (%d items)",
                        collection_id,
                        batch_num,
                        len(items),
                    )
                    break

                self.queue_manager.mark_items_processed(collection_id, item_ids)

                logger.info(
                    "Collection '%s' batch #%d: %d succeeded, %d errors",
                    collection_id,
                    batch_num,
                    success,
                    len(errors),
                )
                if errors:
                    logger.error(
                        "Collection '%s' batch #%d errors: %s",
                        collection_id,
                        batch_num,
                        errors,
                    )

                state.last_flush_time = time.monotonic()

                if len(items) < batch_size:
                    break

        except Exception:
            logger.exception("Unexpected error flushing collection '%s'", collection_id)
        finally:
            try:
                redis_lock.release()
            except Exception:
                pass
            with self._lock:
                state.processing = False

    def run(self) -> None:
        """Main worker loop — polls Redis and dispatches collection flushes."""
        logger.info(
            "Starting item queue worker "
            "(batch_size=%d, flush_interval=%ds, poll_interval=%.1fs, max_workers=%d)",
            self.settings.QUEUE_BATCH_SIZE,
            self.settings.QUEUE_FLUSH_INTERVAL,
            self.settings.WORKER_POLL_INTERVAL,
            self.settings.WORKER_MAX_THREADS,
        )

        with ThreadPoolExecutor(
            max_workers=self.settings.WORKER_MAX_THREADS
        ) as executor:
            futures: Dict[str, Future] = {}

            while self.running:
                try:
                    collections = self.queue_manager.get_pending_collections()

                    for collection_id in collections:
                        if not self.running:
                            break

                        # Clean up completed futures
                        if collection_id in futures and futures[collection_id].done():
                            del futures[collection_id]

                        # Skip if already submitted and still running
                        if collection_id in futures:
                            continue

                        if self._should_flush(collection_id):
                            futures[collection_id] = executor.submit(
                                self._flush_collection, collection_id
                            )

                except Exception:
                    logger.exception("Error in worker poll loop")

                time.sleep(self.settings.WORKER_POLL_INTERVAL)

        logger.info("Worker stopped.")

    def stop(self) -> None:
        """Signal the worker to shut down gracefully."""
        logger.info("Shutting down worker...")
        self.running = False
        self.queue_manager.close()


def main() -> None:
    """Entry point for the item queue worker."""
    settings = ItemQueueSettings()

    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    worker = ItemQueueWorker()
    worker.run()


if __name__ == "__main__":
    main()
