"""Base database logic."""

import abc
from typing import Any, Iterable

from stac_pydantic.shared import BBox

from stac_fastapi.core.etag import DocumentVersion
from stac_fastapi.sfeos_helpers.database import (
    find_item_in_alias,
    index_alias_by_collection_id,
    mk_item_id,
)
from stac_fastapi.sfeos_helpers.search_engine import (
    BaseIndexInserter,
    DatetimeIndexInserter,
)
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.stac import Collection, Item


class BaseDatabaseLogic(abc.ABC):
    """
    Abstract base class for database logic.

    This class defines the basic structure and operations for database interactions.
    Subclasses must provide implementations for these methods.
    """

    client: Any
    async_index_inserter: BaseIndexInserter
    #: Search-engine exception raised when a document lookup misses. Concrete
    #: backends set this to their engine-specific not-found error class.
    not_found_error_class: type[Exception]

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
            token (str | None): The pagination token.
            limit (int): The number of results to return.
            request (Any, optional): The FastAPI request object. Defaults to None.
            sort (list[dict[str, Any]] | None, optional): Optional sort parameter. Defaults to None.
            bbox (BBox | None, optional): Bounding box to filter collections by spatial extent. Defaults to None.
            q (list[str] | None, optional): Free text search terms. Defaults to None.
            filter (dict[str, Any] | None, optional): Structured query in CQL2 format. Defaults to None.
            query (dict[str, dict[str, Any]] | None, optional): Query extension parameters. Defaults to None.
            datetime (str | None, optional): Temporal filter. Defaults to None.

        Returns:
            A tuple of (collections, next pagination token if any, optional count).
        """
        pass

    @abc.abstractmethod
    async def get_one_item(
        self, collection_id: str, item_id: str, *, include_hidden: bool = False
    ) -> dict:
        """Retrieve a single item from the database."""
        pass

    async def get_item_for_write(self, collection_id: str, item_id: str) -> dict:
        """Retrieve an item for a write without applying read visibility filters.

        Under datetime index filtering the real-time ``mget`` lookup is used
        for the same reason as in :meth:`get_item_hit_for_write`.
        """
        if isinstance(self.async_index_inserter, DatetimeIndexInserter):
            hit = await self.get_item_hit_for_write(collection_id, item_id)
            return hit["_source"]
        return await self.get_one_item(collection_id, item_id, include_hidden=True)

    async def get_item_hit_for_write(self, collection_id: str, item_id: str) -> dict:
        """Retrieve the write-lookup hit for an item, including its version.

        Returns a hit dict carrying ``_source``, ``_index``, ``_seq_no`` and
        ``_primary_term``, so callers that then write via ``create_item`` can
        pass the result back as ``existing_hit`` (the existence check is not
        repeated) or read the stored version from it.

        Under datetime index filtering, ``_id`` uniqueness is enforced per
        physical index and a just-written document may not be visible to
        ``_search`` yet, so the lookup is a real-time ``mget`` over the
        collection alias's concrete indexes instead.

        Raises:
            NotFoundError: If the item does not exist in the collection.
        """
        if isinstance(self.async_index_inserter, DatetimeIndexInserter):
            try:
                hit = await find_item_in_alias(
                    self.client,
                    index_alias_by_collection_id(collection_id),
                    mk_item_id(item_id, collection_id),
                )
            except self.not_found_error_class:
                hit = None
            if hit is None:
                raise NotFoundError(
                    f"Item {item_id} does not exist inside Collection {collection_id}"
                )
            return hit
        item, version = await self.get_one_item_with_version(
            collection_id, item_id, include_hidden=True
        )
        return {
            "_source": item,
            "_index": version.index,
            "_seq_no": version.seq_no,
            "_primary_term": version.primary_term,
        }

    @abc.abstractmethod
    async def get_one_item_with_version(
        self, collection_id: str, item_id: str, *, include_hidden: bool = False
    ) -> tuple[dict, DocumentVersion]:
        """Retrieve a single item and the version of its stored document.

        With `include_hidden`, HIDE_ITEM_PATH does not apply, as for a write.
        """
        pass

    @abc.abstractmethod
    async def create_item(
        self,
        item: dict,
        refresh: bool = False,
        version: DocumentVersion | None = None,
        **kwargs: Any,
    ) -> DocumentVersion | None:
        """Create an item in the database; return the version it stored.

        With `version` (If-Match), only that stored version is replaced; 412 otherwise.
        """
        pass

    @abc.abstractmethod
    async def async_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """Prep an item for insertion into the database."""
        pass

    @abc.abstractmethod
    async def merge_patch_item(
        self,
        collection_id: str,
        item_id: str,
        item: dict,
        base_url: str,
        refresh: bool = True,
        version: DocumentVersion | None = None,
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
        version: DocumentVersion | None = None,
    ) -> dict:
        """Patch a item in the database follows RF6902."""
        pass

    @abc.abstractmethod
    async def delete_item(
        self,
        item_id: str,
        collection_id: str,
        refresh: bool = False,
        version: DocumentVersion | None = None,
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
    async def create_collection(
        self, collection: dict, refresh: bool = False
    ) -> DocumentVersion | None:
        """Create a collection in the database; return the version it stored."""
        pass

    @abc.abstractmethod
    async def merge_patch_collection(
        self,
        collection_id: str,
        collection: dict,
        base_url: str,
        refresh: bool = True,
        version: DocumentVersion | None = None,
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
        version: DocumentVersion | None = None,
    ) -> dict:
        """Patch a collection in the database follows RF6902."""
        pass

    @abc.abstractmethod
    async def find_collection(self, collection_id: str) -> dict:
        """Find a collection in the database."""
        pass

    @abc.abstractmethod
    async def find_collection_with_version(
        self, collection_id: str
    ) -> tuple[dict, DocumentVersion]:
        """Find a collection and the version of its stored document."""
        pass

    @abc.abstractmethod
    async def update_collection(
        self,
        collection_id: str,
        collection: Collection,
        preserve_parent_ids: bool = False,
        version: DocumentVersion | None = None,
        **kwargs: Any,
    ) -> DocumentVersion | None:
        """Update a collection in the database."""
        pass

    @abc.abstractmethod
    async def update_parent_ids(
        self,
        doc_id: str,
        doc_type: str,
        parent_id: str,
        add: bool,
        refresh: bool | str = False,
    ) -> dict:
        """Atomically add or remove one parent id in a Collection's or Catalog's parent_ids.

        Returns:
            The stored document after the update.
        """
        pass

    @abc.abstractmethod
    async def delete_collection(
        self,
        collection_id: str,
        refresh: bool = False,
        version: DocumentVersion | None = None,
    ) -> None:
        """Delete a collection from the database."""
        pass

    @abc.abstractmethod
    async def get_queryables_mapping(self, collection_id: str = "*") -> dict[str, Any]:
        """Retrieve mapping of Queryables for search."""
        pass

    @abc.abstractmethod
    async def get_all_collection_queryables(self) -> list[dict]:
        """Retrieve all queryables schemas from all collections.

        Returns:
            A list of queryables dictionaries, one from each active collection.
        """
        pass

    async def get_all_catalogs(
        self,
        token: str | None,
        limit: int,
        request: Any = None,
        sort: list[dict[str, Any]] | None = None,
        filter: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], str | None, int | None]:
        """Retrieve a list of catalogs from the database, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            request (Any, optional): The FastAPI request object. Defaults to None.
            sort (Optional[List[Dict[str, Any]]], optional): Optional sort parameter. Defaults to None.
            filter (Optional[Dict[str, Any]], optional): A CQL2 JSON filter the catalogs must match. Defaults to None.

        Returns:
            A tuple of (catalogs, next pagination token if any, optional count).
        """
        pass

    @abc.abstractmethod
    async def create_catalog(
        self, catalog: dict, refresh: bool = False
    ) -> DocumentVersion | None:
        """Create a catalog; reject an id already used by any document.

        Returns the version of the stored document.
        """
        pass

    @abc.abstractmethod
    async def find_catalog(self, catalog_id: str) -> dict:
        """Find a catalog in the database."""
        pass

    @abc.abstractmethod
    async def find_catalog_with_version(
        self, catalog_id: str
    ) -> tuple[dict, DocumentVersion]:
        """Find a catalog and the version of its stored document."""
        pass

    @abc.abstractmethod
    async def delete_catalog(
        self,
        catalog_id: str,
        refresh: bool = False,
        version: DocumentVersion | None = None,
    ) -> None:
        """Delete a catalog from the database.

        Implementations MUST verify the document exists and is a Catalog
        before deleting, raising NotFoundError otherwise; deleting a catalog
        must never remove Collection or Item data. With `version` (If-Match),
        only that stored version is deleted, and nothing changes otherwise.
        """
        pass

    @abc.abstractmethod
    async def unlink_catalog_orphans(self, catalog_id: str) -> None:
        """Unlink children that still reference a catalog that no longer exists.

        Nothing is deleted. A retry of a catalog delete whose cleanup failed
        calls this, with or without If-Match.
        """
        pass

    @abc.abstractmethod
    async def get_catalog_children(
        self,
        catalog_id: str,
        limit: int,
        token: str | None,
        request: Any = None,
        resource_type: str | None = None,
        filter: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], int, str | None]:
        """Get children of a catalog, optionally matching a CQL2 JSON filter.

        Returns:
            Tuple of (children_list, total_count, next_token).
        """
        pass

    @abc.abstractmethod
    async def get_catalog_collections(
        self,
        catalog_id: str,
        limit: int,
        token: str | None,
        request: Any = None,
        filter: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], int, str | None]:
        """Get collections of a catalog, optionally matching a CQL2 JSON filter.

        Returns:
            Tuple of (collections_list, total_count, next_token).
        """
        pass

    @abc.abstractmethod
    async def get_catalog_catalogs(
        self,
        catalog_id: str,
        limit: int,
        token: str | None,
        request: Any = None,
        filter: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], int, str | None]:
        """Get sub-catalogs of a catalog, optionally matching a CQL2 JSON filter.

        Returns:
            Tuple of (catalogs_list, total_count, next_token).
        """
        pass

    @abc.abstractmethod
    async def create_catalog_catalog(
        self,
        catalog_id: str,
        catalog: Any,
        request: Any,
    ) -> Any:
        """Create a sub-catalog."""
        pass

    @abc.abstractmethod
    async def create_catalog_collection(
        self,
        catalog_id: str,
        collection: Any,
        request: Any,
    ) -> Any:
        """Create a collection in a catalog."""
        pass

    @abc.abstractmethod
    async def get_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Any,
    ) -> Any:
        """Get a collection from a catalog."""
        pass

    @abc.abstractmethod
    async def get_catalog_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: Any,
        bbox: list[float] | None = None,
        datetime: str | None = None,
        limit: int = 10,
        sortby: str | None = None,
        filter: dict[str, Any] | None = None,
        token: str | None = None,
        query: str | None = None,
        fields: list[str] | None = None,
    ) -> Any:
        """Get items from a collection in a catalog, optionally matching a CQL2 JSON filter."""
        pass

    @abc.abstractmethod
    async def update_catalog(
        self,
        catalog_id: str,
        catalog: dict,
        refresh: bool | str = False,
        version: DocumentVersion | None = None,
    ) -> DocumentVersion | None:
        """Replace a catalog, keeping its stored parent_ids; return the version stored.

        With `version` (If-Match), only that stored version is replaced; 412 otherwise.
        """
        pass

    @abc.abstractmethod
    async def get_catalog(
        self,
        catalog_id: str,
        request: Any,
        settings: dict,
        limit: int = 100,
    ) -> Any:
        """Get a specific catalog."""
        pass

    @abc.abstractmethod
    def make_search(self) -> Any:
        """Create a search instance."""
        pass

    @abc.abstractmethod
    def apply_ids_filter(self, search: Any, item_ids: list[str]) -> Any:
        """Apply IDs filter to the search."""
        pass

    @abc.abstractmethod
    def apply_collections_filter(self, search: Any, collection_ids: list[str]) -> Any:
        """Apply collections filter to the search."""
        pass

    @abc.abstractmethod
    def apply_datetime_filter(self, search: Any, datetime: str | None) -> Any:
        """Apply datetime filter to the search."""
        pass

    @abc.abstractmethod
    def apply_bbox_filter(self, search: Any, bbox: list) -> Any:
        """Apply bounding box filter to the search."""
        pass

    @abc.abstractmethod
    def apply_intersects_filter(self, search: Any, intersects: Any) -> Any:
        """Apply intersects filter to the search."""
        pass

    @abc.abstractmethod
    def apply_stacql_filter(
        self, search: Any, op: str, field: str, value: float
    ) -> Any:
        """Apply STACQL filter to the search."""
        pass

    @abc.abstractmethod
    def apply_free_text_filter(
        self, search: Any, free_text_queries: list[str] | None
    ) -> Any:
        """Apply free text filter to the search."""
        pass

    @abc.abstractmethod
    async def apply_cql2_filter(
        self, search: Any, _filter: dict[str, Any] | None
    ) -> Any:
        """Apply CQL2 filter to the search."""
        pass

    @abc.abstractmethod
    def populate_sort(self, sortby: list) -> Any:
        """Populate sort for the search."""
        pass

    @abc.abstractmethod
    async def execute_search(
        self,
        search: Any,
        limit: int,
        token: str | None,
        sort: dict[str, dict[str, str]] | None,
        collection_ids: list[str] | None,
        datetime_search: str,
        cql2_metadata: dict[str, Any] | None = None,
        ignore_unavailable: bool = True,
    ) -> tuple[Iterable[dict[str, Any]], int | None, str | None]:
        """Execute the search."""
        pass

    @abc.abstractmethod
    async def aggregate(
        self,
        collection_ids: list[str] | None,
        aggregations: list[str],
        search: Any,
        centroid_geohash_grid_precision: int,
        centroid_geohex_grid_precision: int,
        centroid_geotile_grid_precision: int,
        geometry_geohash_grid_precision: int,
        geometry_geotile_grid_precision: int,
        datetime_frequency_interval: str,
        datetime_search: str,
        ignore_unavailable: bool | None = True,
    ) -> Any:
        """Return aggregations of STAC Items."""
        pass

    @abc.abstractmethod
    async def bulk_async(
        self,
        collection_id: str,
        processed_items: list[Item],
        **kwargs: Any,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Perform a bulk insert of items into the database asynchronously."""
        pass

    @abc.abstractmethod
    def bulk_sync(
        self,
        collection_id: str,
        processed_items: list[Item],
        **kwargs: Any,
    ) -> tuple[int, list[dict[str, Any]]]:
        """Perform a bulk insert of items into the database synchronously."""
        pass

    @abc.abstractmethod
    async def bulk_async_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item | None:
        """Prepare an item for bulk insertion."""
        pass

    @abc.abstractmethod
    def bulk_sync_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item | None:
        """Prepare an item for insertion into the database."""
        pass

    @abc.abstractmethod
    async def check_collection_exists(self, collection_id: str) -> None:
        """Check if a collection exists."""
        pass

    @abc.abstractmethod
    def check_collection_exists_sync(self, collection_id: str) -> None:
        """Check if a collection exists, using the synchronous client."""
        pass

    @abc.abstractmethod
    async def delete_items(self) -> None:
        """Delete all items."""
        pass

    @abc.abstractmethod
    async def delete_collections(self) -> None:
        """Delete all collections."""
        pass
