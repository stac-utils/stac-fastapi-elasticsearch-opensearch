"""Base classes for index selection strategies."""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class BaseIndexSelector(ABC):
    """Base class for async index selectors."""

    @abstractmethod
    async def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: Dict[str, Optional[str]],
    ) -> str:
        """Select appropriate indexes asynchronously.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
            datetime_search (Dict[str, Optional[str]]): Datetime search criteria.

        Returns:
            str: Comma-separated string of selected index names.
        """
        pass

    @abstractmethod
    async def refresh_cache(self):
        """Refresh cache (no-op for unfiltered selector)."""
        pass
