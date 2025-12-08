"""Cache management for index selection strategies."""

import copy
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX


class IndexCacheManager:
    """Manages caching of index aliases with expiration."""

    def __init__(self, cache_ttl_seconds: int = 3600):
        """Initialize the cache manager.

        Args:
            cache_ttl_seconds (int): Time-to-live for cache entries in seconds.
        """
        self._cache: Optional[Dict[str, List[Tuple[Dict[str, str]]]]] = None
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

    def get_cache(self) -> Optional[Dict[str, List[Tuple[Dict[str, str]]]]]:
        """Get the current cache if not expired.

        Returns:
            Optional[Dict[str, List[Tuple[Dict[str, str]]]]]: Cache data if valid, None if expired.
        """
        with self._lock:
            if self.is_expired:
                return None
            return copy.deepcopy(self._cache) if self._cache else None

    def set_cache(self, data: Dict[str, List[Tuple[Dict[str, str]]]]) -> None:
        """Set cache data and update timestamp.

        Args:
            data (Dict[str, List[Tuple[Dict[str, str]]]]): Cache data to store.
        """
        with self._lock:
            self._cache = data
            self._timestamp = time.time()

    def clear_cache(self) -> None:
        """Clear the cache and reset timestamp."""
        with self._lock:
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

    async def load_aliases(self) -> Dict[str, List[Tuple[Dict[str, str]]]]:
        """Load index aliases from search engine.

        Returns:
            Dict[str, List[Tuple[Dict[str, str]]]]: Mapping of main collection aliases to their data.
        """
        response = await self.client.indices.get_alias(index=f"{ITEMS_INDEX_PREFIX}*")
        result: Dict[str, List[Tuple[Dict[str, str]]]] = {}

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

        self.cache_manager.set_cache(result)
        return result

    @staticmethod
    def _find_main_alias(aliases: List[str]) -> str:
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
    def _organize_aliases(aliases: List[str], main_alias: str) -> Dict[str, str]:
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

    async def get_aliases(self) -> Dict[str, List[Tuple[Dict[str, str]]]]:
        """Get aliases from cache or load if expired.

        Returns:
            Dict[str, List[Tuple[Dict[str, str]]]]: Alias mapping data.
        """
        cached = self.cache_manager.get_cache()
        if cached is not None:
            return cached
        return await self.load_aliases()

    async def refresh_aliases(self) -> Dict[str, List[Tuple[Dict[str, str]]]]:
        """Force refresh aliases from search engine.

        Returns:
            Dict[str, List[Tuple[Dict[str, str]]]]: Fresh alias mapping data.
        """
        return await self.load_aliases()

    async def get_collection_indexes(
        self, collection_id: str
    ) -> List[Tuple[Dict[str, str]]]:
        """Get index information for a specific collection.

        Args:
            collection_id (str): Collection identifier.

        Returns:
            List[Tuple[Dict[str, str]]]: List of tuples with alias dictionaries.
        """
        aliases = await self.get_aliases()
        main_alias = index_alias_by_collection_id(collection_id)
        return aliases.get(main_alias, [])
