"""Factory for creating index selection strategies."""

from typing import Any

from stac_fastapi.core.utilities import get_bool_env

from .async_selectors import AsyncDatetimeBasedIndexSelector
from .base import IndexSelectionStrategy
from .sync_selectors import SyncDatetimeBasedIndexSelector
from .unfiltered_selector import UnfilteredIndexSelector


class IndexSelectorFactory:
    """Factory class for creating index selector instances."""

    @staticmethod
    def create_async_selector(client: Any) -> IndexSelectionStrategy:
        """Create an appropriate asynchronous index selector based on environment configuration.

        Checks the ENABLE_DATETIME_INDEX_FILTERING environment variable to determine
        whether to use datetime-based filtering or return all available indices.

        Args:
            client: Asynchronous Elasticsearch/OpenSearch client instance, used only if datetime
                filtering is enabled.

        Returns:
            IndexSelectionStrategy: Either an AsyncDatetimeBasedIndexSelector if datetime
                filtering is enabled, or an UnfilteredIndexSelector otherwise.
        """
        use_datetime_filtering = get_bool_env(
            "ENABLE_DATETIME_INDEX_FILTERING", default="false"
        )

        return (
            AsyncDatetimeBasedIndexSelector(client)
            if use_datetime_filtering
            else UnfilteredIndexSelector()
        )

    @staticmethod
    def create_sync_selector(sync_client: Any) -> IndexSelectionStrategy:
        """Create an appropriate synchronous index selector based on environment configuration.

        Checks the ENABLE_DATETIME_INDEX_FILTERING environment variable to determine
        whether to use datetime-based filtering or return all available indices.

        Args:
            sync_client: Synchronous Elasticsearch/OpenSearch client instance, used only if datetime
                filtering is enabled.

        Returns:
            IndexSelectionStrategy: Either a SyncDatetimeBasedIndexSelector if datetime
                filtering is enabled, or an UnfilteredIndexSelector otherwise.
        """
        use_datetime_filtering = get_bool_env(
            "ENABLE_DATETIME_INDEX_FILTERING", default="false"
        )

        return (
            SyncDatetimeBasedIndexSelector(sync_client)
            if use_datetime_filtering
            else UnfilteredIndexSelector()
        )
