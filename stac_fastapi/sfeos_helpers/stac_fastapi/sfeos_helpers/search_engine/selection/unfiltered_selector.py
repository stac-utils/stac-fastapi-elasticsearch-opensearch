"""Unfiltered index selector implementation."""

from typing import Dict, List, Optional

from stac_fastapi.sfeos_helpers.database import indices

from .base import IndexSelectionStrategy


class UnfilteredIndexSelector(IndexSelectionStrategy):
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
