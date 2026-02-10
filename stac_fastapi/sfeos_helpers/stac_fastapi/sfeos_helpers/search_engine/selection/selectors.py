"""Async index selectors with datetime-based filtering."""
from typing import Any, Dict, List, Optional

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.database import filter_indexes_by_datetime, return_date
from stac_fastapi.sfeos_helpers.mappings import ITEM_INDICES

from ...database import indices
from .base import BaseIndexSelector, SyncBaseIndexSelector
from .cache_manager import IndexAliasLoader, SyncIndexAliasLoader


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
            self.alias_loader = IndexAliasLoader(client)
            self._initialized = True

    @property
    def use_datetime(self) -> bool:
        """Get USE_DATETIME setting dynamically."""
        return get_bool_env("USE_DATETIME", default=True)

    async def get_collection_indexes(
        self, collection_id: str
    ) -> List[tuple[dict[str, str]]]:
        """Get all index aliases for a specific collection.

        Args:
            collection_id (str): The ID of the collection to retrieve indexes for.

        Returns:
            List[tuple[dict[str, str]]]: List of index aliases associated with the collection.
        """
        return await self.alias_loader.get_collection_indexes(collection_id)

    async def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: str,
        for_insertion: bool = False,
    ) -> str:
        """Select indexes filtered by collection IDs and datetime criteria.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
                If None or empty, returns all item indices.
            datetime_search (str): Datetime search criteria.
            for_insertion (bool): If True, selects indexes for inserting items.
                Defaults to False (search mode).

        Returns:
            str: Comma-separated string of selected index names.
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
                    "gte": datetime if self.use_datetime else None,
                    "lte": datetime if self.use_datetime else None,
                },
                "start_datetime": {
                    "gte": datetime if not self.use_datetime else None,
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


class SyncDatetimeBasedIndexSelector(SyncBaseIndexSelector):
    """Sync index selector that filters indices based on datetime criteria with caching."""

    _instance = None

    def __new__(cls, client):
        """Create singleton instance.

        Args:
            client: sync search engine client instance.

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
            self.alias_loader = SyncIndexAliasLoader(client)
            self._initialized = True

    @property
    def use_datetime(self) -> bool:
        """Get USE_DATETIME setting dynamically."""
        return get_bool_env("USE_DATETIME", default=True)

    def get_collection_indexes(self, collection_id: str) -> List[tuple[dict[str, str]]]:
        """Get all index aliases for a specific collection.

        Args:
            collection_id (str): The ID of the collection to retrieve indexes for.

        Returns:
            List[tuple[dict[str, str]]]: List of index aliases associated with the collection.
        """
        return self.alias_loader.get_collection_indexes(collection_id)

    def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: str,
        for_insertion: bool = False,
    ) -> str:
        """Select indexes filtered by collection IDs and datetime criteria.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
                If None or empty, returns all item indices.
            datetime_search (str): Datetime search criteria.
            for_insertion (bool): If True, selects indexes for inserting items.
                Defaults to False (search mode).

        Returns:
            str: Comma-separated string of selected index names.
        """
        datetime_filters = self.parse_datetime_filters(datetime_search, for_insertion)
        if collection_ids:
            selected_indexes = []
            for collection_id in collection_ids:
                collection_indexes = self.get_collection_indexes(collection_id)
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
                    "gte": datetime if self.use_datetime else None,
                    "lte": datetime if self.use_datetime else None,
                },
                "start_datetime": {
                    "gte": datetime if not self.use_datetime else None,
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
