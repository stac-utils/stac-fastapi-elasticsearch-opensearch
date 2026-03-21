"""Catalogs client implementation for multi-tenant catalogs extension."""

import logging
from datetime import datetime
from typing import Any, Literal

import attr
from fastapi import Request
from stac_pydantic.api.collections import Collections
from stac_pydantic.catalog import Catalog
from stac_pydantic.collection import Collection
from stac_pydantic.item import Item
from stac_pydantic.item_collection import ItemCollection
from starlette.responses import JSONResponse, Response

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.multi_tenant_catalogs.client import AsyncBaseCatalogsClient
from stac_fastapi.core.multi_tenant_catalogs.types import Catalogs, Children, ObjectUri
from stac_fastapi.core.serializers import (
    CatalogSerializer,
    CollectionSerializer,
    ItemSerializer,
)
from stac_fastapi.types.errors import NotFoundError

logger = logging.getLogger(__name__)


@attr.s
class CatalogsClient(AsyncBaseCatalogsClient):
    """Catalogs client implementation for the multi-tenant catalogs extension.

    This client implements the AsyncBaseCatalogsClient interface and delegates
    to the database layer for all catalog operations.
    """

    database: BaseDatabaseLogic = attr.ib()
    catalog_serializer: CatalogSerializer = attr.ib(default=CatalogSerializer)
    collection_serializer: CollectionSerializer = attr.ib(default=CollectionSerializer)
    item_serializer: ItemSerializer = attr.ib(default=ItemSerializer)

    def _get_base_url(self, request: Request | None) -> str:
        """Extract base URL from request with sensible default.

        Args:
            request: FastAPI request object or None.

        Returns:
            Base URL without trailing slash, or empty string if request is None.
        """
        if request is None:
            return ""
        return str(request.base_url).rstrip("/")

    @staticmethod
    def _to_dict(obj: Any) -> dict:
        """Convert Catalog/Collection/Item objects to dict.

        Args:
            obj: Object to convert (dict, Pydantic model, or other).

        Returns:
            Dictionary representation of the object.
        """
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")
        return dict(obj)

    @staticmethod
    def _link_to_dict(link: Any) -> dict:
        """Convert Link objects to dict, filtering None values.

        Args:
            link: Link object to convert.

        Returns:
            Dictionary representation of the link.
        """
        if hasattr(link, "model_dump"):
            return link.model_dump(exclude_none=True)
        if isinstance(link, dict):
            return {k: v for k, v in link.items() if v is not None}
        return {k: v for k, v in dict(link).items() if v is not None}

    @staticmethod
    def _add_parent_id(obj: dict, parent_id: str) -> None:
        """Safely add a parent ID to an object's parent_ids list.

        Args:
            obj: Object to update.
            parent_id: Parent ID to add.
        """
        if "parent_ids" not in obj:
            obj["parent_ids"] = []
        if parent_id not in obj["parent_ids"]:
            obj["parent_ids"].append(parent_id)

    @staticmethod
    def _remove_parent_id(obj: dict, parent_id: str) -> None:
        """Safely remove a parent ID from an object's parent_ids list.

        Args:
            obj: Object to update.
            parent_id: Parent ID to remove.
        """
        if "parent_ids" in obj:
            obj["parent_ids"] = [pid for pid in obj["parent_ids"] if pid != parent_id]

    async def get_catalogs(
        self,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> Catalogs | Response:
        """Get all catalogs with pagination support."""
        limit = limit or 10
        catalogs_list, next_token, total_hits = await self.database.get_all_catalogs(
            token=token,
            limit=limit,
            request=request,
            sort=[{"field": "id", "direction": "asc"}],
        )

        base_url = self._get_base_url(request)
        catalogs = [
            self.catalog_serializer.db_to_stac(
                cat, request, extensions=["CatalogsExtension"]
            )
            for cat in catalogs_list
        ]

        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {"rel": "parent", "type": "application/json", "href": base_url},
        ]
        if request:
            links.append(
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": str(request.url),
                }
            )

        return Catalogs(
            catalogs=catalogs,
            links=links,
            numberMatched=total_hits,
            numberReturned=len(catalogs),
        )

    async def create_catalog(
        self, catalog: Catalog, request: Request | None = None, **kwargs
    ) -> Catalog | Response:
        """Create a new catalog."""
        db_catalog_dict = self._to_dict(catalog)
        db_catalog_dict["type"] = "Catalog"
        db_catalog_dict["parent_ids"] = db_catalog_dict.get("parent_ids", [])

        # Filter out dynamic links
        if "links" in db_catalog_dict:
            db_catalog_dict["links"] = [
                link
                for link in db_catalog_dict["links"]
                if isinstance(link, dict)
                and link.get("rel") not in ("parent", "child", "children")
            ]

        await self.database.create_catalog(db_catalog_dict, refresh=True)
        created_obj = self.catalog_serializer.db_to_stac(
            db_catalog_dict, request, extensions=["CatalogsExtension"]
        )
        if isinstance(created_obj, dict):
            return Catalog(**created_obj)
        return created_obj

    async def get_catalog(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> Catalog | Response:
        """Get a specific catalog by ID."""
        catalog_dict = await self.database.find_catalog(catalog_id)
        catalog_obj = self.catalog_serializer.db_to_stac(
            catalog_dict, request, extensions=["CatalogsExtension"]
        )

        # Convert to dict if needed for link manipulation
        if isinstance(catalog_obj, dict):
            catalog_data = catalog_obj
        else:
            catalog_data = (
                catalog_obj.model_dump(mode="json")
                if hasattr(catalog_obj, "model_dump")
                else dict(catalog_obj)
            )

        # Add children endpoint link and child links
        base_url = self._get_base_url(request)
        catalog_links = list(catalog_data.get("links", []))

        # Add children endpoint link
        catalog_links.append(
            {
                "rel": "children",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/children",
            }
        )

        # Get children (catalogs and collections) for child links
        try:
            children_list, _, _ = await self.database.get_catalog_children(
                catalog_id=catalog_id,
                limit=100,
                token=None,
                request=request,
            )

            # Add child links for each child (up to 100)
            for child in children_list[:100]:
                child_id = child.get("id")
                if not child_id:
                    continue
                child_title = child.get("title", child_id)
                child_type = child.get("type", "Catalog")

                if child_type == "Catalog":
                    href = f"{base_url}/catalogs/{child_id}"
                else:
                    href = f"{base_url}/catalogs/{catalog_id}/collections/{child_id}"

                catalog_links.append(
                    {
                        "rel": "child",
                        "type": "application/json",
                        "href": href,
                        "title": child_title,
                    }
                )
        except NotFoundError:
            pass
        except Exception as e:
            logger.warning(f"Failed to fetch children for catalog {catalog_id}: {e}")

        catalog_data["links"] = catalog_links
        return Catalog(**catalog_data)

    async def update_catalog(
        self,
        catalog_id: str,
        catalog: Catalog,
        request: Request | None = None,
        **kwargs,
    ) -> Catalog | Response:
        """Update an existing catalog."""
        existing = await self.database.find_catalog(catalog_id)

        db_catalog_dict = self._to_dict(catalog)
        db_catalog_dict["type"] = "Catalog"
        db_catalog_dict["id"] = catalog_id
        db_catalog_dict["parent_ids"] = existing.get("parent_ids", [])

        # Filter out dynamic links
        if "links" in db_catalog_dict:
            db_catalog_dict["links"] = [
                link
                for link in db_catalog_dict["links"]
                if isinstance(link, dict)
                and link.get("rel") not in ("parent", "child", "children")
            ]

        await self.database.create_catalog(db_catalog_dict, refresh=True)
        updated = await self.database.find_catalog(catalog_id)
        updated_obj = self.catalog_serializer.db_to_stac(
            updated, request, extensions=["CatalogsExtension"]
        )

        # Ensure we return a Catalog object
        if isinstance(updated_obj, dict):
            return Catalog(**updated_obj)
        return updated_obj

    async def delete_catalog(
        self,
        catalog_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> None:
        """Delete a catalog."""
        await self.database.delete_catalog(catalog_id, refresh=True)

    async def get_catalog_collections(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> Collections | Response:
        """Get collections linked from a specific catalog."""
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        limit = limit or 10
        (
            collections_list,
            total_hits,
            next_token,
        ) = await self.database.get_catalog_collections(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )

        collections = [
            self.collection_serializer.db_to_stac_in_catalog(
                col, request, catalog_id=catalog_id, extensions=["CatalogsExtension"]
            )
            for col in collections_list
        ]

        base_url = self._get_base_url(request)
        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/collections",
            },
        ]

        if next_token:
            links.append(
                {
                    "rel": "next",
                    "type": "application/json",
                    "href": f"{base_url}/catalogs/{catalog_id}/collections?limit={limit}&token={next_token}",
                }
            )

        return Collections(
            collections=collections,
            links=links,
            numberMatched=total_hits,
            numberReturned=len(collections),
        )

    async def get_sub_catalogs(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> Catalogs | Response:
        """Get all sub-catalogs of a specific catalog with pagination."""
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        limit = limit or 10
        (
            catalogs_list,
            total_hits,
            next_token,
        ) = await self.database.get_catalog_catalogs(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
        )

        catalogs = [
            self.catalog_serializer.db_to_stac(
                cat, request, extensions=["CatalogsExtension"]
            )
            for cat in catalogs_list
        ]

        base_url = self._get_base_url(request)
        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/catalogs",
            },
        ]

        if next_token:
            links.append(
                {
                    "rel": "next",
                    "type": "application/json",
                    "href": f"{base_url}/catalogs/{catalog_id}/catalogs?limit={limit}&token={next_token}",
                }
            )

        return Catalogs(
            catalogs=catalogs,
            links=links,
            numberMatched=total_hits,
            numberReturned=len(catalogs),
        )

    async def create_sub_catalog(
        self,
        catalog_id: str,
        catalog: Catalog | ObjectUri,
        request: Request | None = None,
        **kwargs,
    ) -> Catalog | Response:
        """Create a new catalog or link an existing catalog as a sub-catalog."""
        # Check if it's an existing catalog or a new one
        cat_id = catalog.id if hasattr(catalog, "id") else catalog.get("id")

        try:
            existing = await self.database.find_catalog(cat_id)
            # Link existing catalog
            self._add_parent_id(existing, catalog_id)
            await self.database.create_catalog(existing, refresh=True)
            existing_obj = self.catalog_serializer.db_to_stac(
                existing, request, extensions=["CatalogsExtension"]
            )
            if isinstance(existing_obj, dict):
                return Catalog(**existing_obj)
            return existing_obj
        except NotFoundError:
            # Create new catalog
            db_catalog_dict = self._to_dict(catalog)
            db_catalog_dict["type"] = "Catalog"
            db_catalog_dict["parent_ids"] = [catalog_id]

            # Filter out dynamic links
            if "links" in db_catalog_dict:
                db_catalog_dict["links"] = [
                    link
                    for link in db_catalog_dict["links"]
                    if isinstance(link, dict)
                    and link.get("rel") not in ("parent", "child", "children")
                ]

            await self.database.create_catalog(db_catalog_dict, refresh=True)
            new_obj = self.catalog_serializer.db_to_stac(
                db_catalog_dict, request, extensions=["CatalogsExtension"]
            )
            if isinstance(new_obj, dict):
                return Catalog(**new_obj)
            return new_obj

    async def create_catalog_collection(
        self,
        catalog_id: str,
        collection: Collection | ObjectUri,
        request: Request | None = None,
        **kwargs,
    ) -> Collection | Response:
        """Create a new collection or link an existing collection to catalog."""
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        # Get collection ID safely
        if isinstance(collection, dict):
            col_id = collection.get("id")
            is_object_uri = len(collection) == 1 and "id" in collection
        else:
            col_id = collection.id
            # Check if this is an ObjectUri (only has id field)
            is_object_uri = (
                hasattr(collection, "__class__")
                and collection.__class__.__name__ == "ObjectUri"
            )

        # If only an ID was provided (ObjectUri), the collection must already exist
        if is_object_uri:
            try:
                existing = await self.database.find_collection(col_id)
                self._add_parent_id(existing, catalog_id)
                await self.database.update_collection(col_id, existing, refresh=True)
                collection_obj = self.collection_serializer.db_to_stac_in_catalog(
                    existing,
                    request,
                    catalog_id=catalog_id,
                    extensions=["CatalogsExtension"],
                )
                # Return 201 Created for all collection operations
                content = self._to_dict(collection_obj)
                return JSONResponse(content=content, status_code=201)
            except NotFoundError:
                raise NotFoundError(f"Collection {col_id} not found")

        # Full collection data provided - try to link existing or create new
        try:
            existing = await self.database.find_collection(col_id)
            self._add_parent_id(existing, catalog_id)
            await self.database.update_collection(col_id, existing, refresh=True)
            collection_obj = self.collection_serializer.db_to_stac_in_catalog(
                existing,
                request,
                catalog_id=catalog_id,
                extensions=["CatalogsExtension"],
            )
            # Return 201 Created for full collection data (even if linking existing)
            content = self._to_dict(collection_obj)
            return JSONResponse(content=content, status_code=201)
        except NotFoundError:
            # Create new collection
            col_dict = self._to_dict(collection)
            col_dict["parent_ids"] = [catalog_id]

            # Filter out dynamic links
            if "links" in col_dict:
                col_dict["links"] = [
                    link
                    for link in col_dict["links"]
                    if isinstance(link, dict)
                    and link.get("rel") not in ("parent", "child", "children")
                ]

            await self.database.create_collection(col_dict, refresh=True)
            collection_obj = self.collection_serializer.db_to_stac_in_catalog(
                col_dict,
                request,
                catalog_id=catalog_id,
                extensions=["CatalogsExtension"],
            )
            # Return 201 Created for new collection
            content = self._to_dict(collection_obj)
            return JSONResponse(content=content, status_code=201)

    async def get_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> Collection | Response:
        """Get a specific collection from a catalog (Scoped Route)."""
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception:
            raise NotFoundError(f"Catalog {catalog_id} not found")

        # Get collection and validate it belongs to this catalog
        try:
            collection_dict = await self.database.get_catalog_collection(
                catalog_id=catalog_id,
                collection_id=collection_id,
                request=request,
            )
        except Exception:
            raise NotFoundError(f"Collection {collection_id} not found")

        # Verify collection is in this catalog's parent_ids
        parent_ids = collection_dict.get("parent_ids", [])
        if catalog_id not in parent_ids:
            raise NotFoundError(
                f"Collection {collection_id} not linked to catalog {catalog_id}"
            )

        return self.collection_serializer.db_to_stac_in_catalog(
            collection_dict,
            request,
            catalog_id=catalog_id,
            extensions=["CatalogsExtension"],
        )

    async def unlink_catalog_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> None:
        """Unlink a collection from a catalog."""
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise

        # Get collection and validate it belongs to this catalog
        try:
            collection_dict = await self.database.get_catalog_collection(
                catalog_id=catalog_id,
                collection_id=collection_id,
                request=request,
            )
        except NotFoundError:
            raise

        # Verify collection is in this catalog's parent_ids
        parent_ids = collection_dict.get("parent_ids", [])
        if catalog_id not in parent_ids:
            raise NotFoundError(
                f"Collection {collection_id} not linked to catalog {catalog_id}"
            )

        # Remove this catalog from parent_ids
        self._remove_parent_id(collection_dict, catalog_id)
        await self.database.update_collection(
            collection_id, collection_dict, refresh=True
        )

    async def get_catalog_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        bbox: list[float] | None = None,
        datetime: str | datetime | None = None,
        limit: int | None = 10,
        token: str | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> ItemCollection | Response:
        """Get items from a collection in a catalog with search support."""
        # Validate and default limit
        limit = limit or 10
        if limit <= 0:
            limit = 10
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        # Validate collection exists and belongs to this catalog
        try:
            collection_dict = await self.database.get_catalog_collection(
                catalog_id=catalog_id,
                collection_id=collection_id,
                request=request,
            )
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Collection {collection_id} not found") from e

        # Verify collection is in this catalog's parent_ids
        parent_ids = collection_dict.get("parent_ids", [])
        if catalog_id not in parent_ids:
            raise NotFoundError(
                f"Collection {collection_id} not linked to catalog {catalog_id}"
            )

        # Convert datetime to string if needed
        datetime_str = None
        if datetime:
            if isinstance(datetime, str):
                datetime_str = datetime
            else:
                datetime_str = datetime.isoformat()

        items, total, next_token = await self.database.get_catalog_collection_items(
            catalog_id=catalog_id,
            collection_id=collection_id,
            bbox=bbox,
            datetime=datetime_str,
            limit=limit or 10,
            token=token,
            request=request,
        )

        base_url = self._get_base_url(request)

        serialized_items = []
        for item in items:
            item_id = item.get("id")
            if not item_id:
                continue

            # Create item without request to avoid urljoin errors, then add all links manually
            serialized_item = self.item_serializer.db_to_stac(item, None)

            # Create proper links for the item
            item_links = [
                {
                    "rel": "self",
                    "type": "application/geo+json",
                    "href": f"{base_url}/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}",
                },
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{base_url}/catalogs/{catalog_id}/collections/{collection_id}",
                },
                {
                    "rel": "collection",
                    "type": "application/json",
                    "href": f"{base_url}/collections/{collection_id}",
                },
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": base_url,
                },
            ]

            # Add any existing links that aren't self/parent/collection/root
            existing_links = None
            if isinstance(serialized_item, dict):
                existing_links = serialized_item.get("links", [])
            elif hasattr(serialized_item, "links"):
                existing_links = serialized_item.links

            if existing_links:
                for link in existing_links:
                    link_dict = self._link_to_dict(link)
                    # Skip standard links and links without href
                    if link_dict.get("rel") not in (
                        "self",
                        "parent",
                        "collection",
                        "root",
                    ) and link_dict.get("href"):
                        item_links.append(link_dict)

            # Update links in the item (handle both dict and object)
            if isinstance(serialized_item, dict):
                serialized_item["links"] = item_links
            else:
                serialized_item.links = item_links

            serialized_items.append(serialized_item)

        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/collections/{collection_id}",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/collections/{collection_id}/items",
            },
        ]

        if next_token:
            links.append(
                {
                    "rel": "next",
                    "type": "application/json",
                    "href": f"{base_url}/catalogs/{catalog_id}/collections/{collection_id}/items?limit={limit}&token={next_token}",
                }
            )

        return ItemCollection(
            type="FeatureCollection",
            features=serialized_items,
            links=links,
            numberMatched=total,
            numberReturned=len(serialized_items),
        )

    async def get_catalog_collection_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> Item | Response:
        """Get a specific item from a collection in a catalog."""
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        # Validate collection exists and belongs to this catalog
        try:
            collection_dict = await self.database.get_catalog_collection(
                catalog_id=catalog_id,
                collection_id=collection_id,
                request=request,
            )
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Collection {collection_id} not found") from e

        # Verify collection is in this catalog's parent_ids
        parent_ids = collection_dict.get("parent_ids", [])
        if catalog_id not in parent_ids:
            raise NotFoundError(
                f"Collection {collection_id} not linked to catalog {catalog_id}"
            )

        item_dict = await self.database.get_catalog_collection_item(
            catalog_id=catalog_id,
            collection_id=collection_id,
            item_id=item_id,
            request=request,
        )

        # Extract base URL as string for serializer
        base_url = self._get_base_url(request)

        # Create item without request to avoid urljoin errors, then add all links manually
        item = self.item_serializer.db_to_stac(item_dict, None)

        # Create proper links for the item
        item_links = [
            {
                "rel": "self",
                "type": "application/geo+json",
                "href": f"{base_url}/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}",
            },
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/collections/{collection_id}",
            },
            {
                "rel": "collection",
                "type": "application/json",
                "href": f"{base_url}/collections/{collection_id}",
            },
            {
                "rel": "root",
                "type": "application/json",
                "href": base_url,
            },
        ]

        # Add any existing links that aren't self/parent/collection/root
        existing_links = None
        if isinstance(item, dict):
            existing_links = item.get("links", [])
        elif hasattr(item, "links"):
            existing_links = item.links

        if existing_links:
            for link in existing_links:
                link_dict = self._link_to_dict(link)
                # Skip standard links and links without href
                if link_dict.get("rel") not in (
                    "self",
                    "parent",
                    "collection",
                    "root",
                ) and link_dict.get("href"):
                    item_links.append(link_dict)

        # Update links in the item (handle both dict and object)
        if isinstance(item, dict):
            item["links"] = item_links
        else:
            item.links = item_links

        return item

    async def get_catalog_children(
        self,
        catalog_id: str,
        limit: int | None = None,
        token: str | None = None,
        type: Literal["Catalog", "Collection"] | None = None,
        request: Request | None = None,
        **kwargs,
    ) -> Children | Response:
        """Get all children (Catalogs and Collections) of a specific catalog."""
        # Validate catalog exists
        try:
            catalog = await self.database.find_catalog(catalog_id)
            if not catalog:
                raise NotFoundError(f"Catalog {catalog_id} not found")
        except NotFoundError:
            raise
        except Exception as e:
            raise NotFoundError(f"Catalog {catalog_id} not found") from e

        limit = limit or 10
        (
            children_list,
            total_hits,
            next_token,
        ) = await self.database.get_catalog_children(
            catalog_id=catalog_id,
            limit=limit,
            token=token,
            request=request,
            resource_type=type,
        )

        children = []
        for child in children_list:
            if child.get("type") == "Catalog":
                children.append(
                    self.catalog_serializer.db_to_stac(
                        child, request, extensions=["CatalogsExtension"]
                    )
                )
            else:
                children.append(
                    self.collection_serializer.db_to_stac_in_catalog(
                        child,
                        request,
                        catalog_id=catalog_id,
                        extensions=["CatalogsExtension"],
                    )
                )

        base_url = self._get_base_url(request)
        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {
                "rel": "parent",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}",
            },
            {
                "rel": "self",
                "type": "application/json",
                "href": f"{base_url}/catalogs/{catalog_id}/children",
            },
        ]

        if next_token:
            links.append(
                {
                    "rel": "next",
                    "type": "application/json",
                    "href": f"{base_url}/catalogs/{catalog_id}/children?limit={limit}&token={next_token}",
                }
            )

        return Children(
            children=children,
            links=links,
            numberMatched=total_hits,
            numberReturned=len(children),
        )

    async def get_catalog_conformance(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> dict | Response:
        """Get conformance classes specific to this sub-catalog."""
        # Return standard conformance classes for now
        return {
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0-beta.4/multi-tenant-catalogs",
            ]
        }

    async def get_catalog_queryables(
        self, catalog_id: str, request: Request | None = None, **kwargs
    ) -> dict | Response:
        """Get queryable fields available for filtering in this sub-catalog."""
        # Delegate to database for queryables
        return await self.database.get_queryables_mapping(collection_id="*")

    async def unlink_sub_catalog(
        self,
        catalog_id: str,
        sub_catalog_id: str,
        request: Request | None = None,
        **kwargs,
    ) -> None:
        """Unlink a sub-catalog from its parent."""
        try:
            sub_catalog = await self.database.find_catalog(sub_catalog_id)
        except NotFoundError:
            raise NotFoundError(f"Catalog {sub_catalog_id} not found")

        self._remove_parent_id(sub_catalog, catalog_id)
        await self.database.create_catalog(sub_catalog, refresh=True)
