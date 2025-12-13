"""Base classes for index inserters."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseIndexInserter(ABC):
    """Base async index inserter with common async methods."""

    @abstractmethod
    async def get_target_index(
        self, collection_id: str, product: Dict[str, Any]
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
        self, collection_id: str, items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Prepare bulk actions for multiple items asynchronously.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

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
