"""Factory for creating index insertion strategies."""

from typing import Any

from stac_fastapi.core.utilities import get_bool_env

from .async_inserters import AsyncDatetimeIndexInserter, AsyncSimpleIndexInserter
from .base import BaseAsyncIndexInserter, BaseSyncIndexInserter
from .index_operations import IndexOperations
from .sync_inserters import SyncDatetimeIndexInserter, SyncSimpleIndexInserter


class IndexInsertionFactory:
    """Factory for creating index insertion strategies."""

    @staticmethod
    def create_async_insertion_strategy(
        client: Any,
    ) -> BaseSyncIndexInserter | BaseAsyncIndexInserter:
        """Create async insertion strategy based on configuration.

        Args:
            client: Async search engine client instance.

        Returns:
            BaseSyncIndexInserter | BaseAsyncIndexInserter: Configured async insertion strategy.
        """
        index_operations = IndexOperations()

        use_datetime_partitioning = get_bool_env(
            "ENABLE_DATETIME_INDEX_FILTERING", default="false"
        )

        if use_datetime_partitioning:
            return AsyncDatetimeIndexInserter(client, index_operations)
        else:
            return AsyncSimpleIndexInserter(index_operations, client)

    @staticmethod
    def create_sync_insertion_strategy(
        sync_client: Any,
    ) -> BaseSyncIndexInserter | BaseAsyncIndexInserter:
        """Create sync insertion strategy based on configuration.

        Args:
            sync_client: Sync search engine client instance.

        Returns:
            BaseSyncIndexInserter | BaseAsyncIndexInserter: Configured sync insertion strategy.
        """
        index_operations = IndexOperations()

        use_datetime_partitioning = get_bool_env(
            "ENABLE_DATETIME_INDEX_FILTERING", default="false"
        )

        if use_datetime_partitioning:
            return SyncDatetimeIndexInserter(sync_client, index_operations)
        else:
            return SyncSimpleIndexInserter(index_operations, sync_client)
