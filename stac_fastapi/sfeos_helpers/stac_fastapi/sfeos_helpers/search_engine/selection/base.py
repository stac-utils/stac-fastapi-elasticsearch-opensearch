"""Base classes for index selection strategies."""

from abc import ABC, abstractmethod
from typing import Awaitable, Dict, List, Optional, Union


class IndexSelectionStrategy(ABC):
    """Abstract base class for index selection strategies."""

    @abstractmethod
    def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: Dict[str, Optional[str]],
    ) -> Union[str, Awaitable[str]]:
        """Select appropriate indexes based on collection IDs and datetime criteria.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
                If None, all collections are considered.
            datetime_search (Dict[str, Optional[str]]): Dictionary containing datetime
                search criteria with 'gte' and 'lte' keys for range filtering.

        Returns:
            Union[str, Awaitable[str]]: Comma-separated string of selected index names
                or awaitable that resolves to such string.
        """
        pass


class BaseAsyncIndexSelector(IndexSelectionStrategy):
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


class BaseSyncIndexSelector(IndexSelectionStrategy):
    """Base class for sync index selectors."""

    @abstractmethod
    def select_indexes(
        self,
        collection_ids: Optional[List[str]],
        datetime_search: Dict[str, Optional[str]],
    ) -> str:
        """Select appropriate indexes synchronously.

        Args:
            collection_ids (Optional[List[str]]): List of collection IDs to filter by.
            datetime_search (Dict[str, Optional[str]]): Datetime search criteria.

        Returns:
            str: Comma-separated string of selected index names.
        """
        pass
