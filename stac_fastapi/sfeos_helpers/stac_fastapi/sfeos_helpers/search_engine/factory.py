"""Factory for creating index insertion strategies."""

from typing import Any

from stac_fastapi.core.utilities import get_bool_env

from .adapters import SearchEngineAdapterFactory
from .async_inserters import AsyncDatetimeIndexInserter, AsyncSimpleIndexInserter
from .base import BaseIndexInserter
from .sync_inserters import SyncDatetimeIndexInserter, SyncSimpleIndexInserter


class IndexInsertionFactory:
    """Factory for creating index insertion strategies."""

    @staticmethod
    def create_async_insertion_strategy(client: Any) -> BaseIndexInserter:
        """Create async insertion strategy based on configuration.

        Args:
            client: Async search engine client instance.

        Returns:
            BaseIndexInserter: Configured async insertion strategy.
        """
        engine_type = SearchEngineAdapterFactory.detect_engine_type(client)
        search_adapter = SearchEngineAdapterFactory.create_adapter(engine_type)

        use_datetime_partitioning = get_bool_env(
            "ENABLE_DATETIME_INDEX_FILTERING", default="false"
        )

        if use_datetime_partitioning:
            return AsyncDatetimeIndexInserter(client, search_adapter)
        else:
            return AsyncSimpleIndexInserter(search_adapter, client)

    @staticmethod
    def create_sync_insertion_strategy(sync_client: Any) -> BaseIndexInserter:
        """Create sync insertion strategy based on configuration.

        Args:
            sync_client: Sync search engine client instance.

        Returns:
            BaseIndexInserter: Configured sync insertion strategy.
        """
        engine_type = SearchEngineAdapterFactory.detect_engine_type(sync_client)
        search_adapter = SearchEngineAdapterFactory.create_adapter(engine_type)

        use_datetime_partitioning = get_bool_env(
            "ENABLE_DATETIME_INDEX_FILTERING", default="false"
        )

        if use_datetime_partitioning:
            return SyncDatetimeIndexInserter(sync_client, search_adapter)
        else:
            return SyncSimpleIndexInserter(search_adapter, sync_client)
