"""Async index selectors with datetime-based filtering."""
import logging
from typing import Any, cast

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.database import (
    filter_indexes_by_datetime,
    filter_indexes_by_datetime_range,
    return_date,
)
from stac_fastapi.sfeos_helpers.mappings import ITEM_INDICES, ITEMS_INDEX_PREFIX

from ...database import indices
from .base import BaseIndexSelector
from .cache_manager import IndexAliasLoader, IndexCacheManager

logger = logging.getLogger(__name__)


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

    async def refresh_cache(self) -> dict[str, list[tuple[dict[str, str]]]]:
        """Force refresh of the aliases cache.

        Returns:
            dict[str, list[tuple[dict[str, str]]]]: Refreshed dictionary mapping base collection aliases
                to lists of their corresponding item index aliases.
        """
        return await self.alias_loader.refresh_aliases()

    async def get_collection_indexes(
        self, collection_id: str, use_cache: bool = True
    ) -> list[tuple[dict[str, str]]]:
        """Get all index aliases for a specific collection.

        Args:
            collection_id (str): The ID of the collection to retrieve indexes for.
            use_cache (bool): If True, use Redis cache (search path).
                If False, load fresh from search engine (insertion path).

        Returns:
            list[tuple[dict[str, str]]]: List of index aliases associated with the collection.
                Returns empty list if collection is not found in cache.
        """
        return await self.alias_loader.get_collection_indexes(
            collection_id, use_cache=use_cache
        )

    async def select_indexes(
        self,
        collection_ids: list[str] | None,
        datetime_search: str,
        for_insertion: bool = False,
    ) -> str:
        """Select indexes filtered by collection IDs and datetime criteria.

        For each specified collection, retrieves its associated indexes and filters
        them based on datetime range. If no collection IDs are provided but datetime
        criteria exist, filters across all collections by datetime. If neither is
        provided, returns all item indices.

        Args:
            collection_ids (list[str] | None): List of collection IDs to filter by.
                If None or empty, all collections are considered for datetime filtering.
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
            collections_indexes = [
                await self.get_collection_indexes(cid, use_cache=not for_insertion)
                for cid in collection_ids
            ]
        elif self._has_datetime_values(datetime_search):
            all_aliases = await self.alias_loader.get_aliases()
            collections_indexes = list(all_aliases.values())
        else:
            logger.info(f"Selected indexes: {ITEM_INDICES}")
            return ITEM_INDICES

        selected_indexes = []
        for collection_indexes in collections_indexes:
            selected_indexes.extend(
                self._filter_indexes(
                    collection_indexes, datetime_filters, for_insertion
                )
            )

        result = ",".join(selected_indexes) if selected_indexes else ""
        logger.info(f"Selected indexes: {result}")
        return result

    def _filter_indexes(
        self,
        collection_indexes: list[tuple[dict[str, str]]],
        datetime_filters: dict[str, dict[str, Any]],
        for_insertion: bool,
    ) -> list[str]:
        """Filter collection indexes by datetime criteria.

        Args:
            collection_indexes: Index aliases for a collection.
            datetime_filters: Parsed datetime filter criteria.
            for_insertion: Whether filtering for insertion or search.

        Returns:
            List of matching index alias names.
        """
        if for_insertion or self.use_datetime:
            return filter_indexes_by_datetime(
                collection_indexes, datetime_filters, self.use_datetime
            )
        return filter_indexes_by_datetime_range(collection_indexes, datetime_filters)

    @staticmethod
    def _has_datetime_values(datetime_search: str | dict | None) -> bool:
        """Check if datetime_search contains actual datetime values.

        Args:
            datetime_search: Datetime search criteria (dict with gte/lte or string).

        Returns:
            True if datetime_search has non-empty datetime values.
        """
        if isinstance(datetime_search, dict):
            return bool(datetime_search.get("gte") or datetime_search.get("lte"))
        return bool(datetime_search)

    def parse_datetime_filters(
        self, datetime: str | dict, for_insertion: bool
    ) -> dict[str, dict[str, Any]]:
        """Parse datetime string into structured filter criteria.

        Args:
            datetime: Datetime search criteria string or dict with gte/lte keys.
            for_insertion (bool): If True, generates filters for inserting items.
                If False, generates filters for searching items. Defaults to False.

        Returns:
            Dictionary with datetime, start_datetime, and end_datetime filters
        """
        parsed_datetime = return_date(datetime)

        if for_insertion:
            return {
                "datetime": {
                    "gte": datetime if self.use_datetime else None,
                    "lte": datetime if self.use_datetime else None,
                },
                "start_datetime": {
                    "gte": datetime if not self.use_datetime else None,
                    "lte": datetime if not self.use_datetime else None,
                },
                "end_datetime": {"gte": None, "lte": None},
            }

        dt_dict = cast(dict, datetime)
        return {
            "datetime": {
                "gte": parsed_datetime.get("gte") if self.use_datetime else None,
                "lte": parsed_datetime.get("lte") if self.use_datetime else None,
            },
            "start_datetime": {
                "gte": dt_dict.get("gte") if not self.use_datetime else None,
                "lte": None,
            },
            "end_datetime": {
                "gte": None,
                "lte": dt_dict.get("lte") if not self.use_datetime else None,
            },
        }

    async def get_all_collection_ids(self) -> list[str] | None:
        """Return all known collection IDs derived from cached alias keys.

        Strips the ITEMS_INDEX_PREFIX from each alias key to derive collection IDs.

        Returns:
            list[str] | None: List of collection IDs from the alias cache.
        """
        aliases = await self.alias_loader.get_aliases()
        prefix_len = len(ITEMS_INDEX_PREFIX)
        return [alias[prefix_len:] for alias in aliases.keys()]


class UnfilteredIndexSelector(BaseIndexSelector):
    """Index selector that returns all available indices without filtering."""

    async def select_indexes(
        self,
        collection_ids: list[str] | None,
        datetime_search: str,
        for_insertion: bool = False,
    ) -> str:
        """Select all indices for given collections without datetime filtering.

        Args:
            collection_ids (list[str] | None): List of collection IDs to filter by.
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

    async def get_all_collection_ids(self) -> list[str] | None:
        """Return None since unfiltered selector has no cache.

        Returns:
            None: No cached collection IDs available.
        """
        return None
