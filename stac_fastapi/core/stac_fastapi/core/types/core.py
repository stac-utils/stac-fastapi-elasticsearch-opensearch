"""Base clients. Takef from stac-fastapi.types.core v2.4.9."""

import abc
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import attr
from starlette.responses import Response

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.conformance import BASE_CONFORMANCE_CLASSES
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.search import (
    BaseCatalogSearchPostRequest,
    BaseCollectionSearchPostRequest,
    BaseDiscoverySearchPostRequest,
    BaseSearchPostRequest,
)
from stac_fastapi.types.stac import Conformance

NumType = Union[float, int]
StacType = Dict[str, Any]


@attr.s
class AsyncBaseTransactionsClient(abc.ABC):
    """Defines a pattern for implementing the STAC transaction extension."""

    database = attr.ib(default=BaseDatabaseLogic)

    @abc.abstractmethod
    async def create_item(
        self,
        catalog_path: str,
        collection_id: str,
        item: Union[stac_types.Item, stac_types.ItemCollection],
        **kwargs,
    ) -> Optional[Union[stac_types.Item, Response, None]]:
        """Create a new item.

        Called with `POST /collections/{collection_id}/items`.

        Args:
            item: the item or item collection
            collection_id: the id of the collection from the resource path

        Returns:
            The item that was created or None if item collection.
        """
        ...

    @abc.abstractmethod
    async def update_item(
        self,
        catalog_path: str,
        collection_id: str,
        item_id: str,
        item: stac_types.Item,
        **kwargs,
    ) -> Optional[Union[stac_types.Item, Response]]:
        """Perform a complete update on an existing item.

        Called with `PUT /collections/{collection_id}/items`. It is expected
        that this item already exists.  The update should do a diff against the
        saved item and perform any necessary updates.  Partial updates are not
        supported by the transactions extension.

        Args:
            item: the item (must be complete)

        Returns:
            The updated item.
        """
        ...

    @abc.abstractmethod
    async def delete_item(
        self, item_id: str, collection_id: str, catalog_path: str, **kwargs
    ) -> Optional[Union[stac_types.Item, Response]]:
        """Delete an item from a collection.

        Called with `DELETE /collections/{collection_id}/items/{item_id}`

        Args:
            item_id: id of the item.
            collection_id: id of the collection.

        Returns:
            The deleted item.
        """
        ...

    @abc.abstractmethod
    async def create_collection(
        self, catalog_path: str, collection: stac_types.Collection, **kwargs
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Create a new collection.

        Called with `POST /collections`.

        Args:
            collection: the collection

        Returns:
            The collection that was created.
        """
        ...

    @abc.abstractmethod
    async def update_collection(
        self,
        catalog_path: str,
        collection_id: str,
        collection: stac_types.Collection,
        **kwargs,
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Perform a complete update on an existing collection.

        Called with `PUT /collections`. It is expected that this item already
        exists.  The update should do a diff against the saved collection and
        perform any necessary updates.  Partial updates are not supported by the
        transactions extension.

        Args:
            collection: the collection (must be complete)

        Returns:
            The updated collection.
        """
        ...

    @abc.abstractmethod
    async def delete_collection(
        self, catalog_path: str, collection_id: str, **kwargs
    ) -> Optional[Union[stac_types.Collection, Response]]:
        """Delete a collection.

        Called with `DELETE /collections/{collection_id}`

        Args:
            collection_id: id of the collection.

        Returns:
            The deleted collection.
        """
        ...

    @abc.abstractmethod
    async def create_catalog(
        self, catalog: stac_types.Catalog, catalog_path: Optional[str], **kwargs
    ) -> Optional[stac_types.Catalog | Response]:
        """Create a new catalog.

        Called with `POST /catalogs`.

        Args:
            catalog: the catalog

        Returns:
            The catalog that was created.
        """
        ...

    @abc.abstractmethod
    async def update_catalog(
        self, catalog_path: str, catalog: stac_types.Catalog, **kwargs
    ) -> Optional[stac_types.Catalog | Response]:
        """Perform a complete update on an existing collection.

        Called with `PUT /collections`. It is expected that this item already
        exists.  The update should do a diff against the saved collection and
        perform any necessary updates.  Partial updates are not supported by the
        transactions extension.

        Args:
            collection: the collection (must be complete)

        Returns:
            The updated collection.
        """
        ...

    @abc.abstractmethod
    async def delete_catalog(
        self, catalog_path: str, **kwargs
    ) -> Optional[stac_types.Catalog | Response]:
        """Delete a collection.

        Called with `DELETE /collections/{collection_id}`

        Args:
            collection_id: id of the collection.

        Returns:
            The deleted collection.
        """
        ...


@attr.s  # type:ignore
class AsyncBaseCoreClient(abc.ABC):
    """Defines a pattern for implementing STAC api core endpoints.

    Attributes:
        extensions: list of registered api extensions.
    """

    database = attr.ib(default=BaseDatabaseLogic)

    base_conformance_classes: List[str] = attr.ib(
        factory=lambda: BASE_CONFORMANCE_CLASSES
    )
    extensions: List[ApiExtension] = attr.ib(default=attr.Factory(list))
    post_request_model = attr.ib(default=BaseSearchPostRequest)

    def conformance_classes(self) -> List[str]:
        """Generate conformance classes."""
        conformance_classes = self.base_conformance_classes.copy()

        for extension in self.extensions:
            extension_classes = getattr(extension, "conformance_classes", [])
            conformance_classes.extend(extension_classes)

        return list(set(conformance_classes))

    def extension_is_enabled(self, extension: str) -> bool:
        """Check if an api extension is enabled."""
        return any([type(ext).__name__ == extension for ext in self.extensions])

    async def conformance(self, **kwargs) -> stac_types.Conformance:
        """Conformance classes.

        Called with `GET /conformance`.

        Returns:
            Conformance classes which the server conforms to.
        """
        return Conformance(conformsTo=self.conformance_classes())

    @abc.abstractmethod
    async def post_global_search(
        self, search_request: BaseSearchPostRequest, **kwargs
    ) -> stac_types.ItemCollection:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        ...

    @abc.abstractmethod
    async def get_global_search(
        self,
        catalog_paths: Optional[List[str]] = None,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        intersects: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Cross catalog search (GET).

        Called with `GET /search`.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        ...

    @abc.abstractmethod
    async def post_search(
        self, catalog_path: str, search_request: BaseCatalogSearchPostRequest, **kwargs
    ) -> stac_types.ItemCollection:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        ...

    @abc.abstractmethod
    async def get_search(
        self,
        catalog_path: str,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        intersects: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Cross catalog search (GET).

        Called with `GET /search`.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        ...

    @abc.abstractmethod
    async def get_item(
        self, item_id: str, collection_id: str, catalog_path: str, **kwargs
    ) -> stac_types.Item:
        """Get item by id.

        Called with `GET /collections/{collection_id}/items/{item_id}`.

        Args:
            item_id: Id of the item.
            collection_id: Id of the collection.

        Returns:
            Item.
        """
        ...

    @abc.abstractmethod
    async def all_collections(self, **kwargs) -> stac_types.Collections:
        """Get all available collections.

        Called with `GET /collections`.

        Returns:
            A list of collections.
        """
        ...

    @abc.abstractmethod
    async def all_catalogs(
        self, catalog_path: Optional[str] = None, **kwargs
    ) -> stac_types.Catalogs:
        """Get all available catalogs.

        Called with `GET /catalogs`.

        Returns:
            A list of catalogs.
        """
        ...

    # @abc.abstractmethod
    # async def all_nested_catalogs(self, **kwargs) -> stac_types.Catalogs:
    #     """Get all available catalogs.

    #     Called with `GET {catalog_path}/catalogs`.

    #     Returns:
    #         A list of catalogs.
    #     """
    #     ...

    @abc.abstractmethod
    async def get_collection(
        self, catalog_path: str, collection_id: str, **kwargs
    ) -> stac_types.Collection:
        """Get collection by id.

        Called with `GET /collections/{collection_id}`.

        Args:
            collection_id: Id of the collection.

        Returns:
            Collection.
        """
        ...

    @abc.abstractmethod
    async def get_catalog(self, catalog_path: str, **kwargs) -> stac_types.Catalog:
        """Get catalog by id.

        Called with `GET /catalogs/{catalog_id}`.

        Args:
            catalog_id: Id of the catalog.
            super_catalog_id: Id of the top-level catalog.

        Returns:
            Catalog.
        """
        ...

    @abc.abstractmethod
    async def get_catalog_collections(
        self, catalog_path: str, **kwargs
    ) -> stac_types.Collections:
        """Get collections by catalog id.

        Called with `GET /catalogs/{catalog_id}/collections`.

        Args:
            catalog_id: Id of the catalog.
            super_catalog_id: Id of the top-level catalog.

        Returns:
            Collections.
        """
        ...

    @abc.abstractmethod
    async def item_collection(
        self,
        collection_id: str,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: int = 10,
        token: str = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Get all items from a specific collection.

        Called with `GET /collections/{collection_id}/items`

        Args:
            collection_id: id of the collection.
            limit: number of items to return.
            token: pagination token.

        Returns:
            An ItemCollection.
        """
        ...


@attr.s
class AsyncBaseFiltersClient(abc.ABC):
    """Defines a pattern for implementing the STAC filter extension."""

    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all queryables over all
        collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension
        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables
        """
        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://example.org/queryables",
            "type": "object",
            "title": "Queryables for Example STAC API",
            "description": "Queryable names for the example STAC API Item Search filter.",
            "properties": {},
        }


@attr.s
class AsyncCollectionSearchClient(abc.ABC):
    """Defines a pattern for implementing the STAC Collection Search extension."""

    async def get_all_collections(
        self,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = 10,
        q: Optional[List[str]] = None,
        **kwargs,
    ) -> stac_types.Collections:
        """Cross catalog search (GET) for collections.

        Called with `GET /collection-search`.

        Args:
            search_request: search request parameters.

        Returns:
            A list of collections matching search criteria.
        """
        ...

    async def post_all_collections(
        self, search_request: BaseCollectionSearchPostRequest, **kwargs
    ) -> stac_types.Collections:
        """Get all available collections.
        Called with `POST /collections`.
        Returns:
            A list of collections.
        """
        ...


@attr.s
class AsyncDiscoverySearchClient(abc.ABC):
    """Defines a pattern for implementing the STAC Collection Search extension."""

    async def get_discovery_search(
        self,
        q: Optional[List[str]] = None,
        limit: Optional[int] = 10,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Cross catalog search (GET) for collections and catalogs.

        Called with `GET /discovery-search`.

        Args:
            search_request: search request parameters.

        Returns:
            A list of collections and catalogs matching search criteria (next pagination token if any).
        """
        ...

    async def post_discovery_search(
        self, search_request: BaseDiscoverySearchPostRequest, **kwargs
    ) -> stac_types.Collections:
        """Cross catalog search (POST) for collections and catalogs.

        Called with `POST /discovery-search`.

        Args:
            search_request: search request parameters.

        Returns:
            A list of collections and catalogs matching search criteria (next pagination token if any).
        """
        ...
