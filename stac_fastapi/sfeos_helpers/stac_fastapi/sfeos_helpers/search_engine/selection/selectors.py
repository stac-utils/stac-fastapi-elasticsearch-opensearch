"""Async index selectors with datetime-based filtering."""

from typing import Any, Dict, List, Optional

from stac_fastapi.sfeos_helpers.database import filter_indexes_by_datetime
from stac_fastapi.sfeos_helpers.mappings import ITEM_INDICES

from ...database import indices
from .base import BaseIndexSelector
from .cache_manager import IndexAliasLoader, IndexCacheManager


class DatetimeBasedIndexSelector(BaseIndexSelector):
    """Asynchronous index selector that filters indices based on datetime criteria with caching."""

    _instance = None

    def __new__(cls, client):
        """Create singleton instance.

        Args:
            client: Async search engine client instance.

        Returns:
            DatetimeBasedIndexSelector: Singleton instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, client: Any):
        """Initialize the datetime-based index selector.

        Args:
            client: Elasticsearch/OpenSearch client instance used for querying
                index aliases and metadata.
        """
        if not hasattr(self, "_initialized"):
            self.cache_manager = IndexCacheManager()
            self.alias_loader = IndexAliasLoader(client, self.cache_manager)
            self._initialized = True

    async def refresh_cache(self) -> Dict[str, List[str]]:
        """Force refresh of the aliases cache.

        Returns:
            Dict[str, List[str]]: Refreshed dictionary mapping base collection aliases
                to lists of their corresponding item index aliases.
        """
        return await self.alias_loader.refresh_aliases()

    async def get_collection_indexes(self, collection_id: str) -> List[str]:
        """Get all index aliases for a specific collection.

        Args:
            collection_id (str): The ID of the collection to retrieve indexes for.

        Returns:
            List[str]: List of index aliases associated with the collection.
                Returns empty list if collection is not found in cache.
        """
        return await self.alias_loader.get_collection_indexes(collection_id)

    async def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: Dict[str, Optional[str]],
    ) -> str:
        """Select indexes filtered by collection IDs and datetime criteria.

        For each specified collection, retrieves its associated indexes and filters
        them based on datetime range. If no collection IDs are provided, returns
        all item indices.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
                If None or empty, returns all item indices.
            datetime_search (Dict[str, Optional[str]]): Dictionary containing datetime
                search criteria with 'gte' and 'lte' keys for range filtering.

        Returns:
            str: Comma-separated string of selected index names that match the
                collection and datetime criteria. Returns empty string if no
                indexes match the criteria.
        """
        if collection_ids:
            selected_indexes = []
            for collection_id in collection_ids:
                collection_indexes = await self.get_collection_indexes(collection_id)
                filtered_indexes = filter_indexes_by_datetime(
                    collection_indexes,
                    datetime_search.get("gte"),
                    datetime_search.get("lte"),
                )
                selected_indexes.extend(filtered_indexes)

            return ",".join(selected_indexes) if selected_indexes else ""

        return ITEM_INDICES


class UnfilteredIndexSelector(BaseIndexSelector):
    """Index selector that returns all available indices without filtering."""

    async def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: Dict[str, Optional[str]],
    ) -> str:
        """Select all indices for given collections without datetime filtering.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
                If None, all collections are considered.
            datetime_search (Dict[str, Optional[str]]): Datetime search criteria
                (ignored by this implementation).

        Returns:
            str: Comma-separated string of all available index names for the collections.
        """
        return indices(collection_ids)

    async def refresh_cache(self):
        """Refresh cache (no-op for unfiltered selector).

        Note:
            Unfiltered selector doesn't use cache, so this is a no-op operation.
        """
        pass
