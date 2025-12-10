"""Base database logic."""

import abc
from typing import Any, Dict, Iterable, List, Optional, Tuple

from stac_pydantic.shared import BBox


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
        bbox: Optional[BBox] = None,
        q: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Dict[str, Any]]] = None,
        datetime: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[int]]:
        """Retrieve a list of collections from the database, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            request (Any, optional): The FastAPI request object. Defaults to None.
            sort (Optional[List[Dict[str, Any]]], optional): Optional sort parameter. Defaults to None.
            bbox (Optional[BBox], optional): Bounding box to filter collections by spatial extent. Defaults to None.
            q (Optional[List[str]], optional): Free text search terms. Defaults to None.
            filter (Optional[Dict[str, Any]], optional): Structured query in CQL2 format. Defaults to None.
            query (Optional[Dict[str, Dict[str, Any]]], optional): Query extension parameters. Defaults to None.
            datetime (Optional[str], optional): Temporal filter. Defaults to None.

        Returns:
            A tuple of (collections, next pagination token if any, optional count).
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

    @abc.abstractmethod
    async def get_all_catalogs(
        self,
        token: Optional[str],
        limit: int,
        request: Any = None,
        sort: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[int]]:
        """Retrieve a list of catalogs from the database, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            request (Any, optional): The FastAPI request object. Defaults to None.
            sort (Optional[List[Dict[str, Any]]], optional): Optional sort parameter. Defaults to None.

        Returns:
            A tuple of (catalogs, next pagination token if any, optional count).
        """
        pass

    @abc.abstractmethod
    async def create_catalog(self, catalog: Dict, refresh: bool = False) -> None:
        """Create a catalog in the database."""
        pass

    @abc.abstractmethod
    async def find_catalog(self, catalog_id: str) -> Dict:
        """Find a catalog in the database."""
        pass

    @abc.abstractmethod
    async def delete_catalog(self, catalog_id: str, refresh: bool = False) -> None:
        """Delete a catalog from the database."""
        pass
