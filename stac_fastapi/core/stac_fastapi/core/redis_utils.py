"""Utilities for connecting to and managing Redis connections."""

import json
import logging
from functools import wraps
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union, cast
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from redis import Redis as SyncRedis
from redis import asyncio as aioredis
from redis.asyncio.sentinel import Sentinel
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.sentinel import Sentinel as SyncSentinel
from retry import retry  # type: ignore

logger = logging.getLogger(__name__)


class RedisCommonSettings(BaseSettings):
    """Common configuration for Redis Sentinel and Redis Standalone."""

    REDIS_DB: int = 15
    REDIS_MAX_CONNECTIONS: Optional[int] = None
    REDIS_RETRY_TIMEOUT: bool = True
    REDIS_DECODE_RESPONSES: bool = True
    REDIS_CLIENT_NAME: str = "stac-fastapi-app"
    REDIS_HEALTH_CHECK_INTERVAL: int = Field(default=30, gt=0)
    REDIS_SELF_LINK_TTL: int = 1800

    REDIS_QUERY_RETRIES_NUM: int = Field(default=3, gt=0)
    REDIS_QUERY_INITIAL_DELAY: float = Field(default=1.0, gt=0)
    REDIS_QUERY_BACKOFF: float = Field(default=2.0, gt=1)

    @field_validator("REDIS_DB")
    @classmethod
    def validate_db(cls, v: int) -> int:
        """Validate REDIS_DB is not negative integer."""
        if v < 0:
            raise ValueError("REDIS_DB must be a positive integer")
        return v

    @field_validator("REDIS_MAX_CONNECTIONS", mode="before")
    @classmethod
    def validate_max_connections(cls, v):
        """Handle empty/None values for REDIS_MAX_CONNECTIONS."""
        if v in ["", "null", "Null", "NULL", "none", "None", "NONE", None]:
            return None
        return v

    @field_validator("REDIS_SELF_LINK_TTL")
    @classmethod
    def validate_self_link_ttl(cls, v: int) -> int:
        """Validate REDIS_SELF_LINK_TTL is negative."""
        if v < 0:
            raise ValueError("REDIS_SELF_LINK_TTL must be a positive integer")
        return v


class RedisSentinelSettings(RedisCommonSettings):
    """Configuration for connecting to Redis Sentinel."""

    REDIS_SENTINEL_HOSTS: str = ""
    REDIS_SENTINEL_PORTS: str = "26379"
    REDIS_SENTINEL_MASTER_NAME: str = "master"

    def get_sentinel_hosts(self) -> List[str]:
        """Parse Redis Sentinel hosts from string to list."""
        if not self.REDIS_SENTINEL_HOSTS:
            return []

        if self.REDIS_SENTINEL_HOSTS.strip().startswith("["):
            return json.loads(self.REDIS_SENTINEL_HOSTS)
        else:
            return [
                h.strip() for h in self.REDIS_SENTINEL_HOSTS.split(",") if h.strip()
            ]

    def get_sentinel_ports(self) -> List[int]:
        """Parse Redis Sentinel ports from string to list of integers."""
        if not self.REDIS_SENTINEL_PORTS:
            return [26379]

        if self.REDIS_SENTINEL_PORTS.strip().startswith("["):
            return json.loads(self.REDIS_SENTINEL_PORTS)
        else:
            ports_str_list = [
                p.strip() for p in self.REDIS_SENTINEL_PORTS.split(",") if p.strip()
            ]
            return [int(port) for port in ports_str_list]

    def get_sentinel_nodes(self) -> List[Tuple[str, int]]:
        """Get list of (host, port) tuples for Sentinel connection."""
        hosts = self.get_sentinel_hosts()
        ports = self.get_sentinel_ports()

        if not hosts:
            return []

        if len(ports) == 1 and len(hosts) > 1:
            ports = ports * len(hosts)

        if len(hosts) != len(ports):
            raise ValueError(
                f"Mismatch between hosts ({len(hosts)}) and ports ({len(ports)})"
            )

        return [(str(host), int(port)) for host, port in zip(hosts, ports)]


class RedisSettings(RedisCommonSettings):
    """Configuration for connecting Redis."""

    REDIS_HOST: str = ""
    REDIS_PORT: int = 6379

    @field_validator("REDIS_PORT")
    @classmethod
    def validate_port_standalone(cls, v: int) -> int:
        """Validate REDIS_PORT is not a negative integer."""
        if v < 0:
            raise ValueError("REDIS_PORT must be a positive integer")
        return v


sentinel_settings = RedisSentinelSettings()
settings: RedisCommonSettings = cast(
    RedisCommonSettings,
    sentinel_settings if sentinel_settings.REDIS_SENTINEL_HOSTS else RedisSettings(),
)


def redis_retry(func: Callable) -> Callable:
    """Retry with back-off decorator for Redis connections."""

    @wraps(func)
    @retry(
        exceptions=(RedisConnectionError, RedisTimeoutError),
        tries=settings.REDIS_QUERY_RETRIES_NUM,
        delay=settings.REDIS_QUERY_INITIAL_DELAY,
        backoff=settings.REDIS_QUERY_BACKOFF,
        logger=logger,
    )
    async def wrapper(*args, **kwargs):
        return await func(*args, **kwargs)

    return wrapper


@redis_retry
async def _connect_redis_internal() -> Optional[aioredis.Redis]:
    """Return a Redis connection Redis or Redis Sentinel."""
    if sentinel_settings.REDIS_SENTINEL_HOSTS:
        sentinel_nodes = settings.get_sentinel_nodes()
        sentinel = Sentinel(
            sentinel_nodes,
            decode_responses=settings.REDIS_DECODE_RESPONSES,
        )

        redis = sentinel.master_for(
            service_name=settings.REDIS_SENTINEL_MASTER_NAME,
            db=settings.REDIS_DB,
            decode_responses=settings.REDIS_DECODE_RESPONSES,
            retry_on_timeout=settings.REDIS_RETRY_TIMEOUT,
            client_name=settings.REDIS_CLIENT_NAME,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL,
        )
        logger.info("Connected to Redis Sentinel")

    elif settings.REDIS_HOST:
        pool = aioredis.ConnectionPool(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            decode_responses=settings.REDIS_DECODE_RESPONSES,
            retry_on_timeout=settings.REDIS_RETRY_TIMEOUT,
            health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL,
        )
        redis = aioredis.Redis(
            connection_pool=pool, client_name=settings.REDIS_CLIENT_NAME
        )
        logger.info("Connected to Redis")
    else:
        logger.warning("No Redis configuration found")
        return None

    return redis


async def connect_redis() -> Optional[aioredis.Redis]:
    """Handle Redis connection."""
    try:
        return await _connect_redis_internal()
    except (
        aioredis.ConnectionError,
        aioredis.TimeoutError,
    ) as e:
        logger.error(f"Redis connection failed after retries: {e}")
    except aioredis.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        return None
    except aioredis.AuthenticationError as e:
        logger.error(f"Redis authentication error: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return None
    return None


def get_redis_key(url: str, token: str) -> str:
    """Create Redis key using URL path and token."""
    parsed = urlparse(url)
    return f"nav:{parsed.path}:{token}"


def build_url_with_token(base_url: str, token: str) -> str:
    """Build URL with token parameter."""
    parsed = urlparse(base_url)
    query_params = parse_qs(parsed.query)

    query_params["token"] = [token]

    new_query = urlencode(query_params, doseq=True)

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        )
    )


@redis_retry
async def save_prev_link(
    redis: aioredis.Redis, next_url: str, current_url: str, next_token: str
) -> None:
    """Save the current page as the previous link for the next URL."""
    if next_url and next_token:
        if sentinel_settings.REDIS_SENTINEL_HOSTS:
            ttl_seconds = settings.REDIS_SELF_LINK_TTL
        elif settings.REDIS_HOST:
            ttl_seconds = settings.REDIS_SELF_LINK_TTL
        key = get_redis_key(next_url, next_token)
        await redis.setex(key, ttl_seconds, current_url)


@redis_retry
async def get_prev_link(
    redis: aioredis.Redis, current_url: str, current_token: str
) -> Optional[str]:
    """Get the previous page link for the current token."""
    if not current_url or not current_token:
        return None
    key = get_redis_key(current_url, current_token)
    return await redis.get(key)


async def redis_pagination_links(
    current_url: str, token: str, next_token: str, links: list
) -> None:
    """Handle Redis pagination."""
    redis = await connect_redis()
    if not redis:
        logger.warning("Redis connection failed.")
        return

    try:
        if next_token:
            next_url = build_url_with_token(current_url, next_token)
            await save_prev_link(redis, next_url, current_url, next_token)

        if token:
            prev_link = await get_prev_link(redis, current_url, token)
            if prev_link:
                links.insert(
                    0,
                    {
                        "rel": "previous",
                        "type": "application/json",
                        "method": "GET",
                        "href": prev_link,
                    },
                )
    except Exception as e:
        logger.warning(f"Redis pagination operation failed: {e}")
    finally:
        await redis.aclose()  # type: ignore


class ItemQueueSettings(BaseSettings):
    """Configuration for item queue behavior."""

    QUEUE_BATCH_SIZE: int = Field(default=50, gt=0)
    QUEUE_FLUSH_INTERVAL: int = Field(default=30, gt=0)
    QUEUE_KEY_PREFIX: str = "item_queue"
    WORKER_POLL_INTERVAL: float = Field(default=1.0, gt=0)
    WORKER_MAX_THREADS: int = Field(default=4, gt=0)
    BACKEND: Literal["opensearch", "elasticsearch"] = "opensearch"
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"


class SyncRedisQueueManager:
    """Synchronous Redis queue manager for the worker."""

    def __init__(self):
        """Initialize sync Redis connection using existing settings."""
        self.queue_settings = ItemQueueSettings()
        self.redis = self._connect()

    @staticmethod
    def _connect() -> SyncRedis:
        """Create sync Redis connection using RedisSentinelSettings."""
        sentinel_settings = RedisSentinelSettings()
        sentinel_nodes = sentinel_settings.get_sentinel_nodes()

        if sentinel_nodes:
            logger.info(f"Connecting to Redis Sentinel: {sentinel_nodes}")
            sentinel = SyncSentinel(sentinel_nodes, socket_timeout=5.0)
            return sentinel.master_for(
                sentinel_settings.REDIS_SENTINEL_MASTER_NAME,
                db=sentinel_settings.REDIS_DB,
                decode_responses=True,
            )
        else:
            standalone_settings = RedisSettings()
            logger.info(
                f"Connecting to standalone Redis: {standalone_settings.REDIS_HOST}:{standalone_settings.REDIS_PORT}"
            )
            return SyncRedis(
                host=standalone_settings.REDIS_HOST,
                port=standalone_settings.REDIS_PORT,
                db=standalone_settings.REDIS_DB,
                decode_responses=True,
            )

    def _get_queue_key(self, collection_id: str) -> str:
        """Get Redis key for collection's item queue (HASH)."""
        return f"{self.queue_settings.QUEUE_KEY_PREFIX}:{collection_id}"

    def _get_collections_set_key(self) -> str:
        """Get Redis key for set of collections with pending items."""
        return f"{self.queue_settings.QUEUE_KEY_PREFIX}:collections"

    def queue_items(self, collection_id: str, items: Union[dict, List[dict]]) -> int:
        """Queue one or more items for a collection. Deduplicates by item ID.

        If an item with the same ID already exists in the queue, it will be replaced.

        Args:
            collection_id: The collection identifier.
            items: Single item dict or list of item dicts to queue.

        Returns:
            int: The total number of items in the queue after operation.
        """
        if isinstance(items, dict):
            items = [items]

        if not items:
            return 0

        queue_key = self._get_queue_key(collection_id)
        collections_key = self._get_collections_set_key()

        self.redis.sadd(collections_key, collection_id)

        items_mapping = {}
        for item in items:
            item_id = item.get("id")
            if not item_id:
                logger.warning(f"Item without 'id' field skipped: {item}")
                continue
            items_mapping[item_id] = json.dumps(item)

        if items_mapping:
            self.redis.hset(queue_key, mapping=items_mapping)

        queue_length = self.redis.hlen(queue_key)

        logger.debug(
            f"Queued {len(items_mapping)} item(s) for collection '{collection_id}', "
            f"queue length: {queue_length}"
        )
        return queue_length

    def get_pending_collections(self) -> List[str]:
        """Get list of collections with pending items."""
        return list(self.redis.smembers(self._get_collections_set_key()))

    def get_pending_items(
        self, collection_id: str, limit: Optional[int] = None
    ) -> List[dict]:
        """Get pending items from the queue.

        Args:
            collection_id: The collection identifier.
            limit: Maximum number of items to return. If None, returns all.

        Returns:
            List of item dicts ready for processing.
        """
        queue_key = self._get_queue_key(collection_id)

        if limit is None:
            items_dict = self.redis.hgetall(queue_key)
            return [json.loads(item_json) for item_json in items_dict.values()]
        else:
            items: List[Dict[str, Any]] = []
            cursor = 0
            while len(items) < limit:
                cursor, data = self.redis.hscan(
                    queue_key, cursor, count=min(limit, 100)
                )
                for item_json in data.values():
                    items.append(json.loads(item_json))
                    if len(items) >= limit:
                        break
                if cursor == 0:
                    break
            return items[:limit]

    def get_pending_item_ids(
        self, collection_id: str, limit: Optional[int] = None
    ) -> List[str]:
        """Get IDs of pending items (useful for batch processing).

        Args:
            collection_id: The collection identifier.
            limit: Maximum number of IDs to return.

        Returns:
            List of item IDs.
        """
        queue_key = self._get_queue_key(collection_id)

        if limit is None:
            return list(self.redis.hkeys(queue_key))
        else:
            item_ids: List[str] = []
            cursor = 0
            while len(item_ids) < limit:
                cursor, data = self.redis.hscan(
                    queue_key, cursor, count=min(limit, 100)
                )
                item_ids.extend(data.keys())
                if cursor == 0:
                    break
            return item_ids[:limit]

    def get_queue_length(self, collection_id: str) -> int:
        """Get number of items in the queue."""
        return self.redis.hlen(self._get_queue_key(collection_id))

    def mark_items_processed(self, collection_id: str, item_ids: List[str]) -> int:
        """Remove processed items from the queue by their IDs.

        Args:
            collection_id: The collection identifier.
            item_ids: List of item IDs to remove.

        Returns:
            int: Number of remaining items in the queue.
        """
        if not item_ids:
            return self.get_queue_length(collection_id)

        queue_key = self._get_queue_key(collection_id)

        self.redis.hdel(queue_key, *item_ids)

        remaining = self.redis.hlen(queue_key)

        if remaining == 0:
            self.redis.srem(self._get_collections_set_key(), collection_id)

        return remaining

    def remove_item(self, collection_id: str, item_id: str) -> bool:
        """Remove a specific item from the queue.

        Args:
            collection_id: The collection identifier.
            item_id: The item ID to remove.

        Returns:
            bool: True if item was removed, False if it didn't exist.
        """
        queue_key = self._get_queue_key(collection_id)
        removed = self.redis.hdel(queue_key, item_id)

        if self.redis.hlen(queue_key) == 0:
            self.redis.srem(self._get_collections_set_key(), collection_id)

        return removed > 0

    def close(self):
        """Close Redis connection."""
        self.redis.close()
