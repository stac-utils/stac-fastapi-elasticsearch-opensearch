"""Base database logic."""

import abc
from typing import Any, Dict, Iterable, List, Optional, Tuple


class BaseDatabaseLogic(abc.ABC):
    """
    Abstract base class for database logic.

    This class defines the basic structure and operations for database interactions.
    Subclasses must provide implementations for these methods.
    """

    @abc.abstractmethod
    async def get_all_collections(
        self,
        token: Optional[str],
        limit: int,
        request: Any = None,
        sort: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of collections from the database, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            request (Any, optional): The FastAPI request object. Defaults to None.
            sort (Optional[List[Dict[str, Any]]], optional): Optional sort parameter. Defaults to None.

        Returns:
            A tuple of (collections, next pagination token if any).
        """
        pass

    @abc.abstractmethod
    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
        """Retrieve a single item from the database."""
        pass

    @abc.abstractmethod
    async def create_item(self, item: Dict, refresh: bool = False) -> None:
        """Create an item in the database."""
        pass

    @abc.abstractmethod
    async def merge_patch_item(
        self,
        collection_id: str,
        item_id: str,
        item: Dict,
        base_url: str,
        refresh: bool = True,
    ) -> Dict:
        """Patch a item in the database follows RF7396."""
        pass

    @abc.abstractmethod
    async def json_patch_item(
        self,
        collection_id: str,
        item_id: str,
        operations: List,
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> Dict:
        """Patch a item in the database follows RF6902."""
        pass

    @abc.abstractmethod
    async def delete_item(
        self, item_id: str, collection_id: str, refresh: bool = False
    ) -> None:
        """Delete an item from the database."""
        pass

    @abc.abstractmethod
    async def get_items_mapping(self, collection_id: str) -> Dict[str, Dict[str, Any]]:
        """Get the mapping for the items in the collection."""
        pass

    @abc.abstractmethod
    async def get_items_unique_values(
        self, collection_id: str, field_names: Iterable[str], *, limit: int = ...
    ) -> Dict[str, List[str]]:
        """Get the unique values for the given fields in the collection."""
        pass

    @abc.abstractmethod
    async def create_collection(self, collection: Dict, refresh: bool = False) -> None:
        """Create a collection in the database."""
        pass

    @abc.abstractmethod
    async def merge_patch_collection(
        self,
        collection_id: str,
        collection: Dict,
        base_url: str,
        refresh: bool = True,
    ) -> Dict:
        """Patch a collection in the database follows RF7396."""
        pass

    @abc.abstractmethod
    async def json_patch_collection(
        self,
        collection_id: str,
        operations: List,
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> Dict:
        """Patch a collection in the database follows RF6902."""
        pass

    @abc.abstractmethod
    async def find_collection(self, collection_id: str) -> Dict:
        """Find a collection in the database."""
        pass

    @abc.abstractmethod
    async def delete_collection(
        self, collection_id: str, refresh: bool = False
    ) -> None:
        """Delete a collection from the database."""
        pass
