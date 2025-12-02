"""Async index selectors with datetime-based filtering."""

from typing import Any, Dict, List, Optional, Tuple

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.database import filter_indexes_by_datetime, return_date
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

    @property
    def use_datetime(self) -> bool:
        """Get USE_DATETIME setting dynamically."""
        return get_bool_env("USE_DATETIME", default=True)

    async def refresh_cache(self) -> Dict[str, List[Tuple[Dict[str, str]]]]:
        """Force refresh of the aliases cache.

        Returns:
            Dict[str, List[Tuple[Dict[str, str]]]]: Refreshed dictionary mapping base collection aliases
                to lists of their corresponding item index aliases.
        """
        return await self.alias_loader.refresh_aliases()

    async def get_collection_indexes(
        self, collection_id: str
    ) -> List[tuple[dict[str, str]]]:
        """Get all index aliases for a specific collection.

        Args:
            collection_id (str): The ID of the collection to retrieve indexes for.

        Returns:
            List[tuple[dict[str, str]]]: List of index aliases associated with the collection.
                Returns empty list if collection is not found in cache.
        """
        return await self.alias_loader.get_collection_indexes(collection_id)

    async def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: str,
        for_insertion: bool = False,
    ) -> str:
        """Select indexes filtered by collection IDs and datetime criteria.

        For each specified collection, retrieves its associated indexes and filters
        them based on datetime range. If no collection IDs are provided, returns
        all item indices.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
                If None or empty, returns all item indices.
            datetime_search (str): Datetime search criteria.
            for_insertion (bool): If True, selects indexes for inserting items into
                the database. If False, selects indexes for searching/querying items.
                Defaults to False (search mode).

        Returns:
            str: Comma-separated string of selected index names that match the
                collection and datetime criteria. Returns empty string if no
                indexes match the criteria.
        """
        datetime_filters = self.parse_datetime_filters(datetime_search, for_insertion)
        if collection_ids:
            selected_indexes = []
            for collection_id in collection_ids:
                collection_indexes = await self.get_collection_indexes(collection_id)
                filtered_indexes = filter_indexes_by_datetime(
                    collection_indexes, datetime_filters, self.use_datetime
                )
                selected_indexes.extend(filtered_indexes)

            return ",".join(selected_indexes) if selected_indexes else ""

        return ITEM_INDICES

    def parse_datetime_filters(
        self, datetime: str, for_insertion: bool
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """Parse datetime string into structured filter criteria.

        Args:
            datetime: Datetime search criteria string
            for_insertion (bool): If True, generates filters for inserting items.
                If False, generates filters for searching items. Defaults to False.

        Returns:
            Dictionary with datetime, start_datetime, and end_datetime filters
        """
        parsed_datetime = return_date(datetime)

        if for_insertion:
            return {
                "datetime": {
                    "gte": None,
                    "lte": datetime if self.use_datetime else None,
                },
                "start_datetime": {
                    "gte": None,
                    "lte": datetime if not self.use_datetime else None,
                },
                "end_datetime": {"gte": None, "lte": None},
            }

        return {
            "datetime": {
                "gte": parsed_datetime.get("gte") if self.use_datetime else None,
                "lte": parsed_datetime.get("lte") if self.use_datetime else None,
            },
            "start_datetime": {
                "gte": parsed_datetime.get("gte") if not self.use_datetime else None,
                "lte": None,
            },
            "end_datetime": {
                "gte": None,
                "lte": parsed_datetime.get("lte") if not self.use_datetime else None,
            },
        }


class UnfilteredIndexSelector(BaseIndexSelector):
    """Index selector that returns all available indices without filtering."""

    async def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: str,
        for_insertion: bool = False,
    ) -> str:
        """Select all indices for given collections without datetime filtering.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
                If None, all collections are considered.
            datetime_search (str): Datetime search criteria
                (ignored by this implementation).
            for_insertion (bool): If True, selects indexes for inserting items into
                the database. If False, selects indexes for searching/querying items.
                Defaults to False (search mode).

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
