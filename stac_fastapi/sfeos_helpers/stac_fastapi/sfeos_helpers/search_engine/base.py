"""Base classes for index inserters."""

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from fastapi import HTTPException, status

from stac_fastapi.sfeos_helpers.mappings import _ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE


class BaseIndexInserter(ABC):
    """Base async index inserter with common async methods."""

    @abstractmethod
    async def get_target_index(
        self, collection_id: str, product: dict[str, Any]
    ) -> str:
        """Get target index for a product asynchronously.

        Args:
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data.

        Returns:
            str: Target index name.
        """
        pass

    @abstractmethod
    async def prepare_bulk_actions(
        self, collection_id: str, items: List[Dict[str, Any]], op_type: str = "create"
    ) -> list[dict[str, Any]]:
        """Prepare bulk actions for multiple items asynchronously.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.
            op_type (str): The operation type for the bulk actions. "create" for insert-only
                (rejects duplicates), "index" for upsert. Defaults to "create".

        Returns:
            List[Dict[str, Any]]: List of bulk actions.
        """
        pass

    @abstractmethod
    async def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index asynchronously.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        pass

    @staticmethod
    @abstractmethod
    def should_create_collection_index() -> bool:
        """Whether this strategy requires collection index creation.

        Returns:
            bool: True if strategy creates collection indexes, False otherwise.
        """
        pass

    async def refresh_cache(self) -> None:
        """Refresh internal cache if applicable.

        Default implementation does nothing. Subclasses that maintain
        internal caches should override this method.
        """
        pass

    def validate_collection_id(self, collection_id: str) -> None:
        """Validate a collection id before the collection is created.

        IDs whose per-collection alias would equal another collection's shared
        alias (`collection_<sha256>`) are refused in every strategy.

        Args:
            collection_id (str): The id of the collection to be created.
        """
        cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
        if re.fullmatch(r"collection_[0-9a-f]{64}", cleaned):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Collection id '{collection_id}' is reserved for shared items index aliases",
            )

    def validate_datetime_field_update(self, field_path: str) -> None:
        """Validate if a datetime field can be updated.

        For datetime-based indexing, certain datetime fields cannot be modified
        because they determine the index where the item is stored.

        Args:
            field_path (str): The path of the field being updated (e.g., "properties.datetime").

        """
        pass
