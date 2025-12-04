"""Base classes for index selection strategies."""

from abc import ABC, abstractmethod
from typing import List, Optional


class BaseIndexSelector(ABC):
    """Base class for async index selectors."""

    @abstractmethod
    async def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: str,
        for_insertion: bool = False,
    ) -> str:
        """Select appropriate indexes asynchronously.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
            datetime_search (str): Datetime search criteria.
            for_insertion (bool): If True, selects indexes for inserting items into
                the database. If False, selects indexes for searching/querying items.
                Defaults to False (search mode).

        Returns:
            str: Comma-separated string of selected index names.
        """
        pass

    @abstractmethod
    async def refresh_cache(self):
        """Refresh cache (no-op for unfiltered selector)."""
        pass
