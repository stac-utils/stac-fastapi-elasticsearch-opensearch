"""Base classes for index inserters."""

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Dict, List, Union


class BaseIndexInserter(ABC):
    """Base class for index insertion strategies."""

    @abstractmethod
    def get_target_index(
        self, collection_id: str, product: Dict[str, Any]
    ) -> Union[str, Awaitable[str]]:
        """Get target index for a product.

        Args:
            collection_id (str): Collection identifier.
            product (Dict[str, Any]): Product data.

        Returns:
            Union[str, Awaitable[str]]: Target index name or awaitable.
        """
        pass

    @abstractmethod
    def prepare_bulk_actions(
        self, collection_id: str, items: List[Dict[str, Any]]
    ) -> Union[List[Dict[str, Any]], Awaitable[List[Dict[str, Any]]]]:
        """Prepare bulk actions for multiple items.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            Union[List[Dict[str, Any]], Awaitable[List[Dict[str, Any]]]]: List of bulk actions or awaitable.
        """
        pass

    @abstractmethod
    def should_create_collection_index(self) -> bool:
        """Whether this strategy requires collection index creation.

        Returns:
            bool: True if collection index should be created, False otherwise.
        """
        pass


class BaseAsyncIndexInserter(BaseIndexInserter):
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


class BaseSyncIndexInserter(BaseIndexInserter):
    """Base sync index inserter with common sync methods."""

    @abstractmethod
    def get_target_index(self, collection_id: str, product: Dict[str, Any]) -> str:
        """Get target index for a product synchronously.

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
        """Prepare bulk actions for multiple items synchronously.

        Args:
            collection_id (str): Collection identifier.
            items (List[Dict[str, Any]]): List of items to process.

        Returns:
            List[Dict[str, Any]]: List of bulk actions.
        """
        pass

    @abstractmethod
    def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index synchronously.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        pass
