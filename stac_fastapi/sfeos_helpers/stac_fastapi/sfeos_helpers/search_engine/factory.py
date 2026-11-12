"""Factory for creating index insertion strategies."""

from typing import Any

from stac_fastapi.core.utilities import items_index_strategy

from .base import BaseIndexInserter
from .index_operations import IndexOperations
from .inserters import DatetimeIndexInserter, SimpleIndexInserter


class IndexInsertionFactory:
    """Factory for creating index insertion strategies."""

    @staticmethod
    def create_insertion_strategy(
        client: Any,
    ) -> BaseIndexInserter:
        """Create async insertion strategy based on configuration.

        Args:
            client: Async search engine client instance.

        Returns:
            BaseIndexInserter: Configured async insertion strategy.
        """
        index_operations = IndexOperations()

        if items_index_strategy() == "datetime":
            return DatetimeIndexInserter(client, index_operations)
        else:
            return SimpleIndexInserter(index_operations, client)
