"""Cache management for index selection strategies."""

import threading
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX


class IndexCacheManager:
    """Manages caching of index aliases with expiration."""

    def __init__(self, cache_ttl_seconds: int = 3600):
        """Initialize the cache manager.

        Args:
            cache_ttl_seconds (int): Time-to-live for cache entries in seconds.
        """
        self._cache: Optional[Dict[str, List[str]]] = None
        self._timestamp: float = 0
        self._ttl = cache_ttl_seconds
        self._lock = threading.Lock()

    @property
    def is_expired(self) -> bool:
        """Check if the cache has expired.

        Returns:
            bool: True if cache is expired, False otherwise.
        """
        return time.time() - self._timestamp > self._ttl

    def get_cache(self) -> Optional[Dict[str, List[str]]]:
        """Get the current cache if not expired.

        Returns:
            Optional[Dict[str, List[str]]]: Cache data if valid, None if expired.
        """
        with self._lock:
            if self.is_expired:
                return None
            return {k: v.copy() for k, v in self._cache.items()}

    def set_cache(self, data: Dict[str, List[str]]) -> None:
        """Set cache data and update timestamp.

        Args:
            data (Dict[str, List[str]]): Cache data to store.
        """
        self._cache = data
        self._timestamp = time.time()

    def clear_cache(self) -> None:
        """Clear the cache and reset timestamp."""
        self._cache = None
        self._timestamp = 0


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

    async def load_aliases(self) -> Dict[str, List[str]]:
        """Load index aliases from search engine.

        Returns:
            Dict[str, List[str]]: Mapping of base aliases to item aliases.
        """
        response = await self.client.indices.get_alias(index=f"{ITEMS_INDEX_PREFIX}*")
        result = defaultdict(list)
        for index_info in response.values():
            aliases = index_info.get("aliases", {})
            items_aliases = sorted(
                [
                    alias
                    for alias in aliases.keys()
                    if alias.startswith(ITEMS_INDEX_PREFIX)
                ]
            )

            if items_aliases:
                result[items_aliases[0]].extend(items_aliases[1:])

        self.cache_manager.set_cache(result)
        return result

    async def get_aliases(self) -> Dict[str, List[str]]:
        """Get aliases from cache or load if expired.

        Returns:
            Dict[str, List[str]]: Alias mapping data.
        """
        cached = self.cache_manager.get_cache()
        if cached is not None:
            return cached
        return await self.load_aliases()

    async def refresh_aliases(self) -> Dict[str, List[str]]:
        """Force refresh aliases from search engine.

        Returns:
            Dict[str, List[str]]: Fresh alias mapping data.
        """
        return await self.load_aliases()

    async def get_collection_indexes(self, collection_id: str) -> List[str]:
        """Get all index aliases for a specific collection.

        Args:
            collection_id (str): Collection identifier.

        Returns:
            List[str]: List of index aliases for the collection.
        """
        aliases = await self.get_aliases()
        return aliases.get(index_alias_by_collection_id(collection_id), [])
