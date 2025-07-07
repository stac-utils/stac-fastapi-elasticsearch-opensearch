"""Cache management for index selection strategies."""

import time
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
        if self.is_expired:
            return None
        return self._cache

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


class AsyncIndexAliasLoader:
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
        result: Dict[str, List[str]] = {}

        for index_info in response.values():
            aliases = index_info.get("aliases", {})
            base_alias = None
            items_aliases = []

            for alias_name in aliases.keys():
                if not alias_name.startswith(ITEMS_INDEX_PREFIX):
                    items_aliases.append(alias_name)
                else:
                    base_alias = alias_name

            if base_alias and items_aliases:
                result.setdefault(base_alias, []).extend(items_aliases)

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


class SyncIndexAliasLoader:
    """Synchronous loader for index aliases."""

    def __init__(self, client: Any, cache_manager: IndexCacheManager):
        """Initialize the sync alias loader.

        Args:
            client: Sync search engine client instance.
            cache_manager (IndexCacheManager): Cache manager instance.
        """
        self.client = client
        self.cache_manager = cache_manager

    def load_aliases(self) -> Dict[str, List[str]]:
        """Load index aliases from search engine synchronously.

        Returns:
            Dict[str, List[str]]: Mapping of base aliases to item aliases.
        """
        response = self.client.indices.get_alias(index=f"{ITEMS_INDEX_PREFIX}*")
        result: Dict[str, List[str]] = {}

        for index_info in response.values():
            aliases = index_info.get("aliases", {})
            base_alias = None
            items_aliases = []

            for alias_name in aliases.keys():
                if not alias_name.startswith(ITEMS_INDEX_PREFIX):
                    items_aliases.append(alias_name)
                else:
                    base_alias = alias_name

            if base_alias and items_aliases:
                result.setdefault(base_alias, []).extend(items_aliases)

        self.cache_manager.set_cache(result)
        return result

    def get_aliases(self) -> Dict[str, List[str]]:
        """Get aliases from cache or load if expired.

        Returns:
            Dict[str, List[str]]: Alias mapping data.
        """
        cached = self.cache_manager.get_cache()
        if cached is not None:
            return cached
        return self.load_aliases()

    def refresh_aliases(self) -> Dict[str, List[str]]:
        """Force refresh aliases from search engine.

        Returns:
            Dict[str, List[str]]: Fresh alias mapping data.
        """
        return self.load_aliases()

    def get_collection_indexes(self, collection_id: str) -> List[str]:
        """Get all index aliases for a specific collection.

        Args:
            collection_id (str): Collection identifier.

        Returns:
            List[str]: List of index aliases for the collection.
        """
        aliases = self.get_aliases()
        return aliases.get(index_alias_by_collection_id(collection_id), [])
