"""Base database logic."""

import abc
from typing import Any, Iterable

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
        token: str | None,
        limit: int,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
        bbox: BBox | None = None,
        q: list[str] | None = None,
        filter: dict[str, Any] | None = None,
        query: dict[str, dict[str, Any]] | None = None,
        datetime: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None, int | None]:
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
    async def get_one_item(self, collection_id: str, item_id: str) -> dict:
        """Retrieve a single item from the database."""
        pass

    @abc.abstractmethod
    async def create_item(self, item: dict, refresh: bool = False) -> None:
        """Create an item in the database."""
        pass

    @abc.abstractmethod
    async def merge_patch_item(
        self,
        collection_id: str,
        item_id: str,
        item: dict,
        base_url: str,
        refresh: bool = True,
    ) -> dict:
        """Patch a item in the database follows RF7396."""
        pass

    @abc.abstractmethod
    async def json_patch_item(
        self,
        collection_id: str,
        item_id: str,
        operations: list,
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> dict:
        """Patch a item in the database follows RF6902."""
        pass

    @abc.abstractmethod
    async def delete_item(
        self, item_id: str, collection_id: str, refresh: bool = False
    ) -> None:
        """Delete an item from the database."""
        pass

    @abc.abstractmethod
    async def get_items_mapping(self, collection_id: str) -> dict[str, dict[str, Any]]:
        """Get the mapping for the items in the collection."""
        pass

    @abc.abstractmethod
    async def get_items_unique_values(
        self, collection_id: str, field_names: Iterable[str], *, limit: int = ...
    ) -> dict[str, list[str]]:
        """Get the unique values for the given fields in the collection."""
        pass

    @abc.abstractmethod
    async def create_collection(self, collection: dict, refresh: bool = False) -> None:
        """Create a collection in the database."""
        pass

    @abc.abstractmethod
    async def merge_patch_collection(
        self,
        collection_id: str,
        collection: dict,
        base_url: str,
        refresh: bool = True,
    ) -> dict:
        """Patch a collection in the database follows RF7396."""
        pass

    @abc.abstractmethod
    async def json_patch_collection(
        self,
        collection_id: str,
        operations: list,
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> dict:
        """Patch a collection in the database follows RF6902."""
        pass

    @abc.abstractmethod
    async def find_collection(self, collection_id: str) -> dict:
        """Find a collection in the database."""
        pass

    @abc.abstractmethod
    async def delete_collection(
        self, collection_id: str, refresh: bool = False
    ) -> None:
        """Delete a collection from the database."""
        pass

    @abc.abstractmethod
    async def get_queryables_mapping(self, collection_id: str = "*") -> dict[str, Any]:
        """Retrieve mapping of Queryables for search."""
        pass

    async def get_all_catalogs(
        self,
        token: str | None,
        limit: int,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], str | None, int | None]:
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
    async def create_catalog(self, catalog: dict, refresh: bool = False) -> None:
        """Create a catalog in the database."""
        pass

    @abc.abstractmethod
    async def find_catalog(self, catalog_id: str) -> dict:
        """Find a catalog in the database."""
        pass

    @abc.abstractmethod
    async def delete_catalog(self, catalog_id: str, refresh: bool = False) -> None:
        """Delete a catalog from the database."""
        pass
