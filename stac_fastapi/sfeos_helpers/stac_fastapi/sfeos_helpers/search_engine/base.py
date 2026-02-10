"""Base classes for index inserters."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseIndexInserter(ABC):
    """Base index inserter with common methods."""

    @abstractmethod
    def get_target_index(self, collection_id: str, product: Dict[str, Any]) -> str:
        """Get target index for a product.

        Args:
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data.

        Returns:
            str: Target index name.
        """
        pass

    @abstractmethod
    def prepare_bulk_actions(
        self, collection_id: str, items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Prepare bulk actions for multiple items.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            List[Dict[str, Any]]: List of bulk actions.
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

    def validate_datetime_field_update(self, field_path: str) -> None:
        """Validate if a datetime field can be updated.

        For datetime-based indexing, certain datetime fields cannot be modified
        because they determine the index where the item is stored.

        Args:
            field_path (str): The path of the field being updated (e.g., "properties.datetime").

        """
        pass
