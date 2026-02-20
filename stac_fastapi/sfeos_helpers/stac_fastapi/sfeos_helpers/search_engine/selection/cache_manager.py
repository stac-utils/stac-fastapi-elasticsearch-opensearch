"""Cache management for index selection strategies using Redis."""

import asyncio
import json
import logging
from typing import Any

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX

logger = logging.getLogger(__name__)

REDIS_DATA_KEY = "index_alias_cache:data"
REDIS_LOCK_KEY = "index_alias_cache:lock"


class IndexCacheManager:
    """Manages caching of index aliases in Redis."""

    def __init__(self, cache_ttl_seconds: int = 1800):
        """Initialize the cache manager.

        Args:
            cache_ttl_seconds (int): Time-to-live for cache entries in seconds.
        """
        self._ttl = cache_ttl_seconds
        self._redis: Any | None = None
        self._init_lock = asyncio.Lock()

    async def _ensure_redis(self):
        """Lazily initialize Redis connection."""
        if self._redis is None:
            async with self._init_lock:
                if self._redis is None:
                    from stac_fastapi.core.redis_utils import connect_redis

                    redis = await connect_redis()
                    if redis is None:
                        raise RuntimeError("Redis is required for index alias caching.")
                    self._redis = redis

    async def get_cache(self) -> dict[str, list[tuple[dict[str, str]]]] | None:
        """Get the current cache from Redis.

        Returns:
            dict[str, list[tuple[dict[str, str]]]] | None: Cache data if valid, None if missing.
        """
        await self._ensure_redis()
        raw = await self._redis.get(REDIS_DATA_KEY)
        if raw is None:
            return None
        data = json.loads(raw)
        return _deserialize_cache(data)

    async def set_cache(self, data: dict[str, list[tuple[dict[str, str]]]]) -> None:
        """Set cache data in Redis with TTL.

        Args:
            data (Dict[str, List[tuple[Dict[str, str]]]]): Cache data to store.
        """
        await self._ensure_redis()
        serialized = json.dumps(_serialize_cache(data))
        await self._redis.setex(REDIS_DATA_KEY, self._ttl, serialized)

    async def clear_cache(self) -> None:
        """Clear the cache in Redis."""
        await self._ensure_redis()
        await self._redis.delete(REDIS_DATA_KEY)

    async def acquire_refresh_lock(self) -> bool:
        """Try to acquire the distributed refresh lock.

        Returns:
            bool: True if lock was acquired, False otherwise.
        """
        await self._ensure_redis()
        return await self._redis.set(REDIS_LOCK_KEY, "1", nx=True, ex=30)

    async def release_refresh_lock(self) -> None:
        """Release the distributed refresh lock."""
        await self._ensure_redis()
        await self._redis.delete(REDIS_LOCK_KEY)


def _serialize_cache(
    data: dict[str, list[tuple[dict[str, str]]]]
) -> dict[str, list[list[dict[str, str]]]]:
    """Convert tuple values to lists for JSON serialization."""
    result = {}
    for key, value in data.items():
        result[key] = [list(t) for t in value]
    return result


def _deserialize_cache(
    data: dict[str, list[list[dict[str, str]]]]
) -> dict[str, list[tuple[dict[str, str]]]]:
    """Convert list values back to tuples after JSON deserialization."""
    result: dict[str, list[tuple[dict[str, str]]]] = {}
    for key, value in data.items():
        result[key] = [tuple(item) for item in value]  # type: ignore[misc]
    return result


class IndexAliasLoader:
    """Asynchronous loader for index aliases."""

    def __init__(self, client: Any, cache_manager: IndexCacheManager):
        """Initialize the async alias loader.

        Args:
            client: Async search engine client instance.
            cache_manager (IndexCacheManager): Cache manager instance.
        """
        self.client = client
        self.cache_manager = cache_manager

    async def load_aliases(self) -> dict[str, list[tuple[dict[str, str]]]]:
        """Load index aliases from search engine.

        Returns:
            Dict[str, List[tuple[Dict[str, str]]]]: Mapping of main collection aliases to their data.
        """
        response = await self.client.indices.get_alias(index=f"{ITEMS_INDEX_PREFIX}*")
        result: dict[str, list[tuple[dict[str, str]]]] = {}

        for index_name, index_info in response.items():
            aliases = index_info.get("aliases", {})
            items_aliases = sorted(
                [
                    alias
                    for alias in aliases.keys()
                    if alias.startswith(ITEMS_INDEX_PREFIX)
                ]
            )

            if items_aliases:
                main_alias = self._find_main_alias(items_aliases)
                aliases_dict = self._organize_aliases(items_aliases, main_alias)

                if aliases_dict:
                    if main_alias not in result:
                        result[main_alias] = []

                    result[main_alias].append((aliases_dict,))

        await self.cache_manager.set_cache(result)
        return result

    @staticmethod
    def _find_main_alias(aliases: list[str]) -> str:
        """Find the main collection alias (without temporal suffixes).

        Args:
            aliases (List[str]): List of all aliases for an index.

        Returns:
            str: The main collection alias.
        """
        temporal_keywords = ["datetime", "start_datetime", "end_datetime"]

        for alias in aliases:
            if not any(keyword in alias for keyword in temporal_keywords):
                return alias

        return aliases[0]

    @staticmethod
    def _organize_aliases(aliases: list[str], main_alias: str) -> dict[str, str]:
        """Organize temporal aliases into a dictionary with type as key.

        Args:
            aliases (List[str]): All aliases for the index.
            main_alias (str): The main collection alias.

        Returns:
            Dict[str, str]: Dictionary with datetime types as keys and alias names as values.
        """
        aliases_dict = {}

        for alias in aliases:
            if alias == main_alias:
                continue

            if "start_datetime" in alias:
                aliases_dict["start_datetime"] = alias
            elif "end_datetime" in alias:
                aliases_dict["end_datetime"] = alias
            elif "datetime" in alias:
                aliases_dict["datetime"] = alias

        return aliases_dict

    async def get_aliases(
        self, use_cache: bool = True
    ) -> dict[str, list[tuple[dict[str, str]]]]:
        """Get aliases from cache or load from search engine.

        When use_cache is False (insertion mode), always loads fresh data from
        the search engine and refreshes the Redis cache so that subsequent
        read queries see the latest aliases.

        Args:
            use_cache (bool): If True, try Redis cache first (read/search path).
                If False, always load from search engine and refresh cache (write/insertion path).

        Returns:
            Dict[str, List[tuple[Dict[str, str]]]]: Alias mapping data.
        """
        if not use_cache:
            return await self.load_aliases()

        cached = await self.cache_manager.get_cache()
        if cached is not None:
            return cached

        lock_acquired = await self.cache_manager.acquire_refresh_lock()
        if lock_acquired:
            try:
                cached = await self.cache_manager.get_cache()
                if cached is not None:
                    return cached
                return await self.load_aliases()
            finally:
                await self.cache_manager.release_refresh_lock()
        else:
            return await self.load_aliases()

    async def refresh_aliases(self) -> dict[str, list[tuple[dict[str, str]]]]:
        """Force refresh aliases from search engine.

        Returns:
            Dict[str, List[tuple[Dict[str, str]]]]: Fresh alias mapping data.
        """
        return await self.load_aliases()

    async def get_collection_indexes(
        self, collection_id: str, use_cache: bool = True
    ) -> list[tuple[dict[str, str]]]:
        """Get index information for a specific collection.

        Args:
            collection_id (str): Collection identifier.
            use_cache (bool): If True, use Redis cache (search path).
                If False, load fresh from search engine (insertion path).

        Returns:
            List[tuple[Dict[str, str]]]: List of tuples with alias dictionaries.
        """
        aliases = await self.get_aliases(use_cache=use_cache)
        main_alias = index_alias_by_collection_id(collection_id)
        return aliases.get(main_alias, [])
