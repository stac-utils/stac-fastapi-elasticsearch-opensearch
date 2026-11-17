"""Factory for creating index insertion strategies."""

from typing import Any

from stac_fastapi.core.utilities import items_index_strategy
from stac_fastapi.sfeos_helpers.database import GENERATION_FIELD
from stac_fastapi.sfeos_helpers.mappings import (
    ES_ITEMS_MAPPINGS,
    ITEMS_ALIAS_PREFIX,
    ITEMS_INDEX_PREFIX,
)

from .base import BaseIndexInserter
from .index_operations import IndexOperations
from .inserters import DatetimeIndexInserter, SharedIndexInserter, SimpleIndexInserter


def validate_shared_index_config() -> None:
    """Refuse settings under which collection aliases on one index break.

    Searches across all collections read `<alias prefix>*`, which must match
    the shared index; the alias filters and the delete of a collection's items
    need one exact, indexed `collection` and `collection_generation` term
    per item.
    """
    if ITEMS_ALIAS_PREFIX != ITEMS_INDEX_PREFIX:
        raise ValueError(
            "ITEMS_INDEX_STRATEGY=shared requires STAC_ITEMS_ALIAS_PREFIX to equal "
            "STAC_ITEMS_INDEX_PREFIX"
        )
    properties = ES_ITEMS_MAPPINGS.get("properties", {})
    for field in ("collection", GENERATION_FIELD):
        mapping = properties.get(field, {})
        if (
            mapping.get("type") != "keyword"
            or "normalizer" in mapping
            or "ignore_above" in mapping
            or mapping.get("index") is False
        ):
            raise ValueError(
                f"ITEMS_INDEX_STRATEGY=shared requires the items mapping of `{field}` "
                f"to be a plain indexed keyword, got {mapping}"
            )


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

        strategy = items_index_strategy()
        if strategy == "datetime":
            return DatetimeIndexInserter(client, index_operations)
        if strategy == "shared":
            validate_shared_index_config()
            return SharedIndexInserter(index_operations, client)
        return SimpleIndexInserter(index_operations, client)
