"""Catalogs extension."""

import asyncio
import base64
import json
import logging
from typing import Any, Type
from urllib.parse import urlencode

import attr
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from stac_pydantic import Collection
from starlette.responses import Response
from typing_extensions import TypedDict

from stac_fastapi.core.models import Catalog
from stac_fastapi.sfeos_helpers.database import (
    search_children_with_pagination_shared,
    search_collections_by_parent_id_shared,
    search_collections_by_parent_id_with_pagination_shared,
    search_sub_catalogs_with_pagination_shared,
    update_catalog_in_index_shared,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.extension import ApiExtension

logger = logging.getLogger(__name__)


def _decode_token(token: str | None) -> list | None:
    """Decode a Base64/JSON pagination token into a search_after list."""
    if not token:
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(token.encode()).decode())
    except Exception:
        logger.warning(f"Invalid pagination token provided: {token}")
        return None


def _encode_token(search_after: list | None) -> str | None:
    """Encode a search_after list into a Base64/JSON pagination token."""
    if not search_after:
        return None
    return base64.urlsafe_b64encode(json.dumps(search_after).encode()).decode()


class Catalogs(TypedDict, total=False):
    """Catalogs endpoint response.

    Similar to Collections but for catalogs.
    """

    catalogs: list[Catalog]
    links: list[dict]
    numberMatched: int | None
    numberReturned: int | None


@attr.s
class CatalogsExtension(ApiExtension):
    """Catalogs Extension.

    The Catalogs extension adds a /catalogs endpoint that returns a list of all catalogs
    in the database, similar to how /collections returns a list of collections.
    """

    client: BaseCoreClient = attr.ib(default=None)
    settings: dict = attr.ib(default=attr.Factory(dict))
    conformance_classes: list[str] = attr.ib(
        default=attr.Factory(lambda: ["https://api.stacspec.org/v1.0.0-rc.2/children"])
    )
    router: APIRouter = attr.ib(default=attr.Factory(APIRouter))
    response_class: Type[Response] = attr.ib(default=JSONResponse)

    @property
    def _active_extensions(self) -> list[str]:
        """Get list of active extensions, ensuring CatalogsExtension is included.

        Returns:
            List of extension class names.
        """
        exts = [type(ext).__name__ for ext in self.client.database.extensions]
        if "CatalogsExtension" not in exts:
            exts.append("CatalogsExtension")
        return exts

    @staticmethod
    def _create_child_link(base_url: str, parent_id: str, child: dict) -> dict:
        """Generate a STAC rel='child' link based on the child's resource type.

        Args:
            base_url: The base URL of the API.
            parent_id: The ID of the parent catalog.
            child: The child resource dictionary (catalog or collection).

        Returns:
            A link dictionary with rel='child'.
        """
        child_id = child.get("id")
        child_type = child.get("type", "Collection")
        child_title = child.get("title", child_id)

        if child_type == "Catalog":
            href = f"{base_url}catalogs/{child_id}"
        else:
            # Collection - use scoped URI
            href = f"{base_url}catalogs/{parent_id}/collections/{child_id}"

        return {
            "rel": "child",
            "type": "application/json",
            "href": href,
            "title": child_title,
        }

    def register(self, app: FastAPI, settings=None) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.
            settings: extension settings (unused for now).
        """
        self.settings = settings or {}
        self.router = APIRouter()

        self.router.add_api_route(
            path="/catalogs",
            endpoint=self.catalogs,
            methods=["GET"],
            response_model=Catalogs,
            response_class=self.response_class,
            summary="Get All Catalogs",
            description="Returns a list of all catalogs in the database.",
            tags=["Catalogs"],
        )

        # Add endpoint for creating catalogs
        self.router.add_api_route(
            path="/catalogs",
            endpoint=self.create_catalog,
            methods=["POST"],
            response_model=Catalog,
            response_class=self.response_class,
            status_code=201,
            summary="Create Catalog",
            description="Create a new STAC catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for getting individual catalogs
        self.router.add_api_route(
            path="/catalogs/{catalog_id}",
            endpoint=self.get_catalog,
            methods=["GET"],
            response_model=Catalog,
            response_class=self.response_class,
            summary="Get Catalog",
            description="Get a specific STAC catalog by ID.",
            tags=["Catalogs"],
        )

        # Add endpoint for getting collections in a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/collections",
            endpoint=self.get_catalog_collections,
            methods=["GET"],
            response_model=stac_types.Collections,
            response_class=self.response_class,
            summary="Get Catalog Collections",
            description="Get collections linked from a specific catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for getting sub-catalogs of a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/catalogs",
            endpoint=self.get_catalog_catalogs,
            methods=["GET"],
            response_model=Catalogs,
            response_class=self.response_class,
            summary="Get Catalog Sub-Catalogs",
            description="Get sub-catalogs linked from a specific catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for creating sub-catalogs in a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/catalogs",
            endpoint=self.create_catalog_catalog,
            methods=["POST"],
            response_model=Catalog,
            response_class=self.response_class,
            status_code=201,
            summary="Create Catalog Sub-Catalog",
            description="Create a new catalog and link it as a sub-catalog of a specific catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for creating collections in a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/collections",
            endpoint=self.create_catalog_collection,
            methods=["POST"],
            response_model=stac_types.Collection,
            response_class=self.response_class,
            status_code=201,
            summary="Create Catalog Collection",
            description="Create a new collection and link it to a specific catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for updating a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}",
            endpoint=self.update_catalog,
            methods=["PUT"],
            response_model=Catalog,
            response_class=self.response_class,
            summary="Update Catalog",
            description="Update an existing STAC catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for deleting a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}",
            endpoint=self.delete_catalog,
            methods=["DELETE"],
            response_class=self.response_class,
            status_code=204,
            summary="Delete Catalog",
            description="Delete a catalog. All linked collections are unlinked and adopted by root if orphaned.",
            tags=["Catalogs"],
        )

        # Add endpoint for getting a specific collection in a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/collections/{collection_id}",
            endpoint=self.get_catalog_collection,
            methods=["GET"],
            response_model=stac_types.Collection,
            response_class=self.response_class,
            summary="Get Catalog Collection",
            description="Get a specific collection from a catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for deleting a collection from a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/collections/{collection_id}",
            endpoint=self.delete_catalog_collection,
            methods=["DELETE"],
            response_class=self.response_class,
            status_code=204,
            summary="Delete Catalog Collection",
            description="Unlink a collection from a catalog. If the collection has multiple parent catalogs, only removes this catalog from parent_ids. If this is the only parent, the collection is adopted by the root catalog. Collection data is never deleted.",
            tags=["Catalogs"],
        )

        # Add endpoint for getting items in a collection within a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/collections/{collection_id}/items",
            endpoint=self.get_catalog_collection_items,
            methods=["GET"],
            response_model=stac_types.ItemCollection,
            response_class=self.response_class,
            summary="Get Catalog Collection Items",
            description="Get items from a collection in a catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for getting a specific item in a collection within a catalog
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}",
            endpoint=self.get_catalog_collection_item,
            methods=["GET"],
            response_model=stac_types.Item,
            response_class=self.response_class,
            summary="Get Catalog Collection Item",
            description="Get a specific item from a collection in a catalog.",
            tags=["Catalogs"],
        )

        # Add endpoint for Children Extension
        self.router.add_api_route(
            path="/catalogs/{catalog_id}/children",
            endpoint=self.get_catalog_children,
            methods=["GET"],
            response_class=self.response_class,
            summary="Get Catalog Children",
            description="Retrieve all children (Catalogs and Collections) of this catalog.",
            tags=["Catalogs"],
        )

        app.include_router(self.router, tags=["Catalogs"])

    async def _format_catalogs_with_links(
        self,
        catalogs_data: list[dict],
        request: Request,
        base_url: str,
    ) -> list[Catalog]:
        """Format catalog data with dynamic parent and child links.

        This helper method is shared between /catalogs and /catalogs/{id}/catalogs endpoints.
        It handles concurrent fetching of parent titles and children, then formats catalogs
        with dynamic links applied.

        Args:
            catalogs_data: List of catalog documents from the database.
            request: Request object for serialization.
            base_url: Base URL for building hrefs.

        Returns:
            List of formatted Catalog objects with dynamic links.
        """
        # --- 1. PRE-FETCH PARENT TITLES CONCURRENTLY ---
        unique_parent_ids = list(
            set(pid for c in catalogs_data for pid in c.get("parent_ids", []) if pid)
        )
        parent_id_to_title = {}

        if unique_parent_ids:

            async def fetch_title(pid):
                try:
                    parent_catalog = await self.client.database.find_catalog(pid)
                    return pid, parent_catalog.get("title", pid)
                except Exception as e:
                    logger.debug(
                        f"Could not fetch title for parent catalog {pid}, using ID as fallback: {e}"
                    )
                    return pid, pid

            title_results = await asyncio.gather(
                *(fetch_title(pid) for pid in unique_parent_ids)
            )
            parent_id_to_title = dict(title_results)

        # --- 2. PRE-FETCH CHILDREN CONCURRENTLY ---
        async def fetch_children(catalog_id):
            try:
                children_data, _, _ = await search_children_with_pagination_shared(
                    self.client.database.client,
                    catalog_id,
                    limit=100,
                    search_after=None,
                    resource_type=None,
                )
                return catalog_id, children_data
            except Exception as e:
                logger.warning(
                    f"Could not fetch children for catalog {catalog_id}: {e}"
                )
                return catalog_id, []

        children_results = await asyncio.gather(
            *(fetch_children(c.get("id")) for c in catalogs_data)
        )
        catalog_children_map = dict(children_results)

        # --- 3. FORMATTING LOOP (Now 100% In-Memory & Synchronous) ---
        catalog_stac_objects = []

        for catalog_data in catalogs_data:
            catalog_id = catalog_data.get("id")

            # The Serializer now handles parent, related, and canonical links automatically
            catalog_stac = self.client.catalog_serializer.db_to_stac(
                catalog_data,
                request,
                extensions=self._active_extensions,
            )

            catalog_dict = (
                catalog_stac.model_dump()
                if hasattr(catalog_stac, "model_dump")
                else dict(catalog_stac)
            )

            # Apply titles to both 'parent' AND 'related' links from pre-fetched map
            for link in catalog_dict.get("links", []):
                if link.get("rel") in ["parent", "related"] and "title" not in link:
                    # If it points to root (base_url), title is Root Catalog
                    if (
                        link.get("href") == base_url
                        or link.get("href") == f"{base_url}/"
                    ):
                        link["title"] = "Root Catalog"
                        continue

                    # Instead of parsing the URL, use our parent_ids list from the db_catalog
                    # This is more robust against trailing slashes or varying URL patterns
                    for pid in catalog_data.get("parent_ids", []):
                        if (
                            pid in parent_id_to_title
                            and f"/catalogs/{pid}" in link.get("href", "")
                        ):
                            link["title"] = parent_id_to_title[pid]
                            break

            # Apply child links from pre-fetched map
            children_data = catalog_children_map.get(catalog_id, [])
            for child in children_data:
                if child.get("id"):
                    catalog_dict["links"].append(
                        self._create_child_link(base_url, catalog_id, child)
                    )
                else:
                    logger.warning(
                        f"Child document missing id field in catalog {catalog_id}: {child}"
                    )

            # Convert back to Catalog object
            catalog_stac = Catalog(**catalog_dict)
            catalog_stac_objects.append(catalog_stac)

        return catalog_stac_objects

    async def catalogs(
        self,
        request: Request,
        limit: int
        | None = Query(
            10,
            ge=1,
            description=(
                "The maximum number of catalogs to return (page size). Defaults to 10."
            ),
        ),
        token: str
        | None = Query(
            None,
            description="Pagination token for the next page of results",
        ),
    ) -> Catalogs:
        """Get all catalogs with pagination support.

        Args:
            request: Request object.
            limit: The maximum number of catalogs to return (page size). Defaults to 10.
            token: Pagination token for the next page of results.

        Returns:
            Catalogs object containing catalogs and pagination links.
        """
        base_url = str(request.base_url)
        search_after = _decode_token(token)

        # Updated to pass decoded search_after
        (
            catalogs,
            next_search_after,
            total_hits,
        ) = await self.client.database.get_all_catalogs(
            token=search_after,
            limit=limit,
            request=request,
            sort=[{"field": "id", "direction": "asc"}],
        )

        catalog_stac_objects = await self._format_catalogs_with_links(
            catalogs, request, base_url
        )

        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {"rel": "parent", "type": "application/json", "href": base_url},
            {"rel": "self", "type": "application/json", "href": str(request.url)},
        ]

        if next_search_after:
            new_token = _encode_token(next_search_after)
            links.append(
                {
                    "rel": "next",
                    "href": f"{base_url}catalogs?{urlencode({'limit': limit, 'token': new_token})}",
                    "type": "application/json",
                    "title": "Next page",
                }
            )

        return Catalogs(
            catalogs=catalog_stac_objects,
            links=links,
            numberReturned=len(catalog_stac_objects),
            numberMatched=total_hits,
        )

    async def create_catalog(self, catalog: Catalog, request: Request) -> Catalog:
        """Create a new catalog.

        Args:
            catalog: The catalog to create.
            request: Request object.

        Returns:
            The created catalog.
        """
        # Convert STAC catalog to database format
        db_catalog = self.client.catalog_serializer.stac_to_db(catalog, request)

        # Convert to dict and ensure type is set to Catalog
        db_catalog_dict = db_catalog.model_dump()
        db_catalog_dict["type"] = "Catalog"

        # Filter out dynamic links (parent, child, children) - these are generated at read-time
        if "links" in db_catalog_dict:
            db_catalog_dict["links"] = [
                link
                for link in db_catalog_dict["links"]
                if link.get("rel") not in ("parent", "child", "children")
            ]

        # Initialize parent_ids for root-level catalogs (empty list means no parents)
        if "parent_ids" not in db_catalog_dict:
            db_catalog_dict["parent_ids"] = []

        # Create the catalog in the database with refresh to ensure immediate availability
        await self.client.database.create_catalog(db_catalog_dict, refresh=True)

        # Return the created catalog with extensions for proper serialization
        return self.client.catalog_serializer.db_to_stac(
            db_catalog_dict,
            request,
            extensions=[type(ext).__name__ for ext in self.client.database.extensions],
        )

    async def update_catalog(
        self, catalog_id: str, catalog: Catalog, request: Request
    ) -> Catalog:
        """Update an existing catalog.

        Args:
            catalog_id: The ID of the catalog to update.
            catalog: The updated catalog data.
            request: Request object.

        Returns:
            The updated catalog.

        Raises:
            HTTPException: If the catalog is not found.
        """
        try:
            # Verify the catalog exists
            existing_catalog_db = await self.client.database.find_catalog(catalog_id)

            # Convert STAC catalog to database format
            db_catalog = self.client.catalog_serializer.stac_to_db(catalog, request)
            db_catalog_dict = db_catalog.model_dump()
            db_catalog_dict["type"] = "Catalog"

            # Preserve parent_ids and other internal fields from the existing catalog
            if "parent_ids" in existing_catalog_db:
                db_catalog_dict["parent_ids"] = existing_catalog_db["parent_ids"]

            # Update the catalog in the database (upsert via create_catalog)
            await self.client.database.create_catalog(db_catalog_dict, refresh=True)

            # Return the updated catalog
            return catalog

        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Catalog {catalog_id} not found")
            logger.error(f"Error updating catalog {catalog_id}: {e}")
            raise e

    async def get_catalog(
        self,
        catalog_id: str,
        request: Request,
        limit: int = Query(
            100, ge=1, le=1000, description="Page size for child link pagination"
        ),
    ) -> Catalog:
        """Get a specific catalog by ID according to the Multi-Tenant spec.

        Args:
            catalog_id: The ID of the catalog to retrieve.
            request: Request object.
            limit: Page size for child link pagination.

        Returns:
            The requested catalog with spec-compliant HATEOAS links.
        """
        try:
            # 1. FETCH DATA
            db_catalog = await self.client.database.find_catalog(catalog_id)

            # 2. SERIALIZE & INITIALIZE DICT
            catalog = self.client.catalog_serializer.db_to_stac(
                db_catalog, request, extensions=self._active_extensions
            )
            base_url = str(request.base_url)

            if isinstance(catalog, dict):
                catalog_dict = catalog
            else:
                catalog_dict = (
                    catalog.model_dump()
                    if hasattr(catalog, "model_dump")
                    else dict(catalog)
                )

            # Clear existing structural links to ensure dynamic generation per spec
            catalog_dict["links"] = [
                link
                for link in catalog_dict.get("links", [])
                if link.get("rel") not in ["self", "parent", "root", "child", "related"]
            ]

            # 3. MANDATORY LINKS (Self & Root)
            catalog_dict["links"].extend(
                [
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": f"{base_url}catalogs/{catalog_id}",
                    },
                    {
                        "rel": "root",
                        "type": "application/json",
                        "href": base_url,
                    },
                ]
            )

            # 4. DYNAMIC PARENT & RELATED LINKS (Poly-hierarchy Logic)
            root_id = self.settings.get("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi")
            parent_ids = db_catalog.get("parent_ids", [])

            if not parent_ids:
                # Top-level sub-catalog: Point parent to Global Root
                catalog_dict["links"].append(
                    {
                        "rel": "parent",
                        "type": "application/json",
                        "href": base_url,
                    }
                )
            else:
                # First ID is Primary Parent
                primary_pid = parent_ids[0]
                parent_href = (
                    base_url
                    if primary_pid == root_id
                    else f"{base_url}catalogs/{primary_pid}"
                )
                catalog_dict["links"].append(
                    {
                        "rel": "parent",
                        "type": "application/json",
                        "href": parent_href,
                    }
                )

                # Additional parents become 'related'
                for pid in parent_ids[1:]:
                    related_href = (
                        base_url if pid == root_id else f"{base_url}catalogs/{pid}"
                    )
                    catalog_dict["links"].append(
                        {
                            "rel": "related",
                            "type": "application/json",
                            "href": related_href,
                            "title": f"Parent context: {pid}",
                        }
                    )

            # 5. DYNAMIC CHILD LINKS (Paginated, one level deep)
            try:
                current_search_after = None
                while True:
                    (
                        children_data,
                        _,
                        next_search_after,
                    ) = await search_children_with_pagination_shared(
                        self.client.database.client,
                        catalog_id,
                        limit=limit,
                        search_after=current_search_after,
                        resource_type=None,
                    )

                    for child in children_data:
                        if child.get("id"):
                            catalog_dict["links"].append(
                                self._create_child_link(base_url, catalog_id, child)
                            )

                    if not next_search_after:
                        break
                    current_search_after = next_search_after
            except Exception as e:
                logger.warning(f"Child link generation failed for {catalog_id}: {e}")

            # 6. CONVENIENCE CHILDREN LINK (Recommended by spec)
            catalog_dict["links"].append(
                {
                    "rel": "children",
                    "type": "application/json",
                    "href": f"{base_url}catalogs/{catalog_id}/children",
                    "title": "Child catalogs and collections",
                }
            )

            return Catalog(**catalog_dict)

        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Catalog {catalog_id} not found")
            logger.error(f"Internal error fetching catalog {catalog_id}: {e}")
            raise e

    async def delete_catalog(self, catalog_id: str, request: Request) -> None:
        """Delete a catalog (The Container).

        Deletes the Catalog document itself. All linked Collections are unlinked
        and adopted by Root if they become orphans. Collection data is NEVER deleted.

        Logic:
        1. Finds all Collections linked to this Catalog.
        2. Unlinks them (removes catalog_id from their parent_ids).
        3. If a Collection becomes an orphan, it is adopted by Root.
        4. PERMANENTLY DELETES the Catalog document itself.

        Args:
            catalog_id: The ID of the catalog to delete.
            request: Request object.

        Returns:
            None (204 No Content)

        Raises:
            HTTPException: If the catalog is not found.
        """
        try:
            # Verify the catalog exists
            await self.client.database.find_catalog(catalog_id)

            # Find all collections with this catalog in parent_ids
            children = await search_collections_by_parent_id_shared(
                self.client.database.client, catalog_id
            )

            # Safe Unlink: Remove catalog from all children's parent_ids
            # If a child becomes an orphan, adopt it to root
            root_id = self.settings.get("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi")

            for child in children:
                child_id = child.get("id")
                try:
                    parent_ids = child.get("parent_ids") or []
                    if catalog_id in parent_ids:
                        parent_ids.remove(catalog_id)

                        # If orphan, move to root
                        if len(parent_ids) == 0:
                            parent_ids.append(root_id)
                            logger.info(
                                f"Collection {child_id} adopted by root after catalog deletion."
                            )
                        else:
                            logger.info(
                                f"Removed catalog {catalog_id} from collection {child_id}; still belongs to {len(parent_ids)} other catalog(s)"
                            )

                        child["parent_ids"] = parent_ids
                        await self.client.database.update_collection(
                            collection_id=child_id, collection=child, refresh=False
                        )
                except Exception as e:
                    error_msg = str(e)
                    if "not found" in error_msg.lower():
                        logger.debug(
                            f"Collection {child_id} not found, skipping (may have been deleted elsewhere)"
                        )
                    else:
                        logger.warning(
                            f"Failed to process collection {child_id} during catalog deletion: {e}"
                        )

            # Delete the catalog itself
            await self.client.database.delete_catalog(catalog_id, refresh=True)
            logger.info(f"Deleted catalog {catalog_id}")

        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Catalog {catalog_id} not found")
            logger.error(f"Error deleting catalog {catalog_id}: {e}")
            raise e

    async def get_catalog_collections(
        self,
        catalog_id: str,
        request: Request,
        limit: int = Query(10, ge=1, le=100),
        token: str | None = Query(None),
    ) -> stac_types.Collections:
        """Get collections linked from a specific catalog with pagination.

        Args:
            catalog_id: The ID of the catalog.
            request: Request object.
            limit: Maximum number of results to return (default: 10, max: 100).
            token: Pagination token for cursor-based pagination.

        Returns:
            Collections object containing collections linked from the catalog.
        """
        try:
            # 1. Verify the catalog exists
            await self.client.database.find_catalog(catalog_id)

            # 2. Decode the pagination token if provided (Point 2: Robust Tokens)
            search_after = None
            if token:
                try:
                    search_after = json.loads(
                        base64.urlsafe_b64decode(token.encode()).decode()
                    )
                except Exception:
                    logger.warning(f"Invalid pagination token provided: {token}")
                    # We proceed without search_after rather than crashing

            # 3. Query collections by parent_ids field with pagination
            # We pass search_after directly to the database helper
            (
                collections_data,
                total_hits,
                next_search_after,
            ) = await search_collections_by_parent_id_with_pagination_shared(
                self.client.database.client, catalog_id, limit, search_after
            )

            # 4. Serialize the results (Point 1: Optimized N+1 Fix)
            # We loop through the 'collections_data' which already contains the DB records.
            # This saves us from making 'n' additional database calls.
            collections = []
            for collection_db in collections_data:
                try:
                    # Serialize with catalog context (sets parent to catalog, injects catalog link)
                    collection = (
                        self.client.collection_serializer.db_to_stac_in_catalog(
                            collection_db,
                            request,
                            catalog_id=catalog_id,
                            extensions=self._active_extensions,
                        )
                    )
                    collections.append(collection)
                except Exception as e:
                    logger.error(
                        f"Error serializing collection {collection_db.get('id')}: {e}"
                    )
                    # We continue to ensure one bad record doesn't break the whole page
                    continue

            # 5. Build HATEOAS links
            base_url = str(request.base_url)
            links = [
                {"rel": "root", "type": "application/json", "href": base_url},
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{base_url}catalogs/{catalog_id}",
                },
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": str(request.url),
                },
            ]

            # 6. Generate Base64 next link if more results exist
            if next_search_after:
                # Encode the sort keys into a URL-safe Base64 string
                encoded_token = base64.urlsafe_b64encode(
                    json.dumps(next_search_after).encode()
                ).decode()

                query_params = {"limit": limit, "token": encoded_token}
                links.append(
                    {
                        "rel": "next",
                        "href": f"{base_url}catalogs/{catalog_id}/collections?{urlencode(query_params)}",
                        "type": "application/json",
                        "title": "Next page",
                    }
                )

            # 7. Return in Collections format (Fixed context fields)
            return stac_types.Collections(
                collections=collections,
                links=links,
                numberMatched=total_hits,
                numberReturned=len(collections),
            )

        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Catalog {catalog_id} not found")
            logger.error(f"Error retrieving collections for {catalog_id}: {e}")
            raise e

    async def get_catalog_catalogs(
        self,
        catalog_id: str,
        request: Request,
        limit: int = Query(10, ge=1, le=100),
        token: str | None = Query(None),
    ) -> Catalogs:
        """Get all sub-catalogs of a specific catalog with pagination.

        Args:
            catalog_id: The ID of the parent catalog.
            request: Request object.
            limit: Maximum number of results to return (default: 10, max: 100).
            token: Pagination token for cursor-based pagination.

        Returns:
            A Catalogs response containing sub-catalogs with pagination links.

        Raises:
            HTTPException: If the catalog is not found.
        """
        try:
            await self.client.database.find_catalog(catalog_id)
            search_after = _decode_token(token)

            # Updated to pass decoded search_after
            (
                catalogs_data,
                total_hits,
                next_search_after,
            ) = await search_sub_catalogs_with_pagination_shared(
                self.client.database.client, catalog_id, limit, search_after
            )

            base_url = str(request.base_url)
            catalogs = await self._format_catalogs_with_links(
                catalogs_data, request, base_url
            )

            links = [
                {"rel": "root", "type": "application/json", "href": base_url},
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{base_url}catalogs/{catalog_id}",
                },
                {"rel": "self", "type": "application/json", "href": str(request.url)},
            ]

            if next_search_after:
                new_token = _encode_token(next_search_after)
                links.append(
                    {
                        "rel": "next",
                        "href": f"{base_url}catalogs/{catalog_id}/catalogs?{urlencode({'limit': limit, 'token': new_token})}",
                        "type": "application/json",
                    }
                )

            return {
                "catalogs": catalogs,
                "links": links,
                "numberReturned": len(catalogs),
                "numberMatched": total_hits,
            }
        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Catalog {catalog_id} not found")
            logger.error(f"Error in get_catalog_catalogs: {e}")
            raise e

    async def create_catalog_catalog(
        self, catalog_id: str, catalog: Catalog, request: Request
    ) -> Catalog:
        """Create a new catalog or link an existing catalog as a sub-catalog.

        Logic:
        1. Verifies the parent catalog exists.
        2. If the sub-catalog already exists: Appends the parent ID to its parent_ids
           (enabling poly-hierarchy - a catalog can have multiple parents).
        3. If the sub-catalog is new: Creates it with parent_ids initialized to [catalog_id].

        Args:
            catalog_id: The ID of the parent catalog.
            catalog: The catalog to create or link.
            request: Request object.

        Returns:
            The created or linked catalog.

        Raises:
            HTTPException: If the parent catalog is not found or operation fails.
        """
        try:
            # 1. Verify the parent catalog exists
            await self.client.database.find_catalog(catalog_id)

            # 2. Check if the sub-catalog already exists
            try:
                existing_catalog = await self.client.database.find_catalog(catalog.id)

                # --- UPDATE PATH (Existing Catalog) ---
                # We are linking an existing catalog to a new parent (poly-hierarchy)

                # Ensure parent_ids list exists
                if "parent_ids" not in existing_catalog:
                    existing_catalog["parent_ids"] = []

                # Append if not already present (use set to avoid duplicates in case of race conditions)
                parent_ids_set = set(existing_catalog["parent_ids"])
                if catalog_id not in parent_ids_set:
                    parent_ids_set.add(catalog_id)
                    existing_catalog["parent_ids"] = list(parent_ids_set)

                    # Persist the update
                    await update_catalog_in_index_shared(
                        self.client.database.client, catalog.id, existing_catalog
                    )
                    logger.info(
                        f"Linked existing catalog {catalog.id} to parent {catalog_id}"
                    )

                # Return the STAC object
                return self.client.catalog_serializer.db_to_stac(
                    existing_catalog,
                    request,
                    extensions=[
                        type(ext).__name__ for ext in self.client.database.extensions
                    ],
                )

            except NotFoundError:
                # --- CREATE PATH (New Catalog) ---
                # Catalog does not exist, so we create it

                # Convert STAC catalog to database format
                db_catalog = self.client.catalog_serializer.stac_to_db(catalog, request)

                # Convert to dict
                db_catalog_dict = db_catalog.model_dump()
                db_catalog_dict["type"] = "Catalog"

                # Filter out dynamic links (parent, child, children) - these are generated at read-time
                if "links" in db_catalog_dict:
                    db_catalog_dict["links"] = [
                        link
                        for link in db_catalog_dict["links"]
                        if link.get("rel") not in ("parent", "child", "children")
                    ]

                # Initialize parent_ids
                db_catalog_dict["parent_ids"] = [catalog_id]

                # Verify parent_ids is in the dict before storing
                if "parent_ids" not in db_catalog_dict:
                    raise ValueError(
                        f"parent_ids missing from catalog dict for {catalog.id}"
                    )

                # Create in DB
                await self.client.database.create_catalog(db_catalog_dict, refresh=True)
                logger.info(
                    f"Created new catalog {catalog.id} with parent {catalog_id}"
                )

                # Return the serialized STAC object with extensions
                return self.client.catalog_serializer.db_to_stac(
                    db_catalog_dict,
                    request,
                    extensions=[
                        type(ext).__name__ for ext in self.client.database.extensions
                    ],
                )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"Error processing sub-catalog {catalog.id} in parent {catalog_id}: {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process sub-catalog: {str(e)}",
            )

    async def create_catalog_collection(
        self, catalog_id: str, collection: Collection, request: Request
    ) -> stac_types.Collection:
        """Create a new collection and link it to a specific catalog.

        Args:
            catalog_id: The ID of the catalog to link the collection to.
            collection: The collection to create.
            request: Request object.

        Returns:
            The created collection.

        Raises:
            HTTPException: If the catalog is not found or collection creation fails.
        """
        try:
            # Verify the catalog exists
            await self.client.database.find_catalog(catalog_id)

            # Check if the collection already exists in the database
            try:
                existing_collection_db = await self.client.database.find_collection(
                    collection.id
                )
                # Collection exists, just add the parent ID if not already present
                existing_collection_dict = existing_collection_db

                # Ensure parent_ids field exists
                if "parent_ids" not in existing_collection_dict:
                    existing_collection_dict["parent_ids"] = []

                # Add catalog_id to parent_ids if not already present
                if catalog_id not in existing_collection_dict["parent_ids"]:
                    existing_collection_dict["parent_ids"].append(catalog_id)

                    # Update the collection in the database
                    await self.client.database.update_collection(
                        collection_id=collection.id,
                        collection=existing_collection_dict,
                        refresh=True,
                    )

                # Convert back to STAC format for the response
                updated_collection = (
                    self.client.database.collection_serializer.db_to_stac(
                        existing_collection_dict,
                        request,
                        extensions=[
                            type(ext).__name__
                            for ext in self.client.database.extensions
                        ],
                    )
                )

                return updated_collection

            except Exception as e:
                # Only proceed to create if collection truly doesn't exist
                error_msg = str(e)
                if "not found" not in error_msg.lower():
                    # Re-raise if it's a different error
                    raise
                # Collection doesn't exist, create it
                # Create the collection using the same pattern as TransactionsClient.create_collection
                # This handles the Collection model from stac_pydantic correctly
                collection_dict = collection.model_dump(mode="json")

                # Add the catalog ID to the parent_ids field
                if "parent_ids" not in collection_dict:
                    collection_dict["parent_ids"] = []

                if catalog_id not in collection_dict["parent_ids"]:
                    collection_dict["parent_ids"].append(catalog_id)

                # Note: We do NOT store catalog links in the database.
                # Catalog links are injected dynamically by the serializer based on context.
                # This allows the same collection to have different catalog links
                # depending on which catalog it's accessed from.

                # Now convert to database format (this will process the links)
                collection_db = self.client.database.collection_serializer.stac_to_db(
                    collection_dict, request
                )
                await self.client.database.create_collection(
                    collection=collection_db, refresh=True
                )

                # Convert back to STAC format for the response
                created_collection = (
                    self.client.database.collection_serializer.db_to_stac(
                        collection_db,
                        request,
                        extensions=[
                            type(ext).__name__
                            for ext in self.client.database.extensions
                        ],
                    )
                )

                return created_collection

        except HTTPException as e:
            # Re-raise HTTP exceptions (e.g., catalog not found, collection validation errors)
            raise e
        except Exception as e:
            # Check if this is a "not found" error from find_catalog
            error_msg = str(e)
            if "not found" in error_msg.lower():
                raise HTTPException(status_code=404, detail=error_msg)

            # Handle unexpected errors
            logger.error(f"Error creating collection in catalog {catalog_id}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create collection in catalog: {str(e)}",
            )

    async def get_catalog_collection(
        self, catalog_id: str, collection_id: str, request: Request
    ) -> stac_types.Collection:
        """Get a specific collection from a catalog.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection.
            request: Request object.

        Returns:
            The requested collection.
        """
        try:
            await self.client.database.find_catalog(catalog_id)
            collection_db = await self.client.database.find_collection(collection_id)
            parent_ids = collection_db.get("parent_ids") or []
            if catalog_id not in parent_ids:
                raise NotFoundError(
                    f"Collection {collection_id} does not belong to catalog {catalog_id}"
                )
            return self.client.collection_serializer.db_to_stac_in_catalog(
                collection_db,
                request,
                catalog_id=catalog_id,
                extensions=self._active_extensions,
            )
        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Resource not found: {e}")
            raise e

    async def get_catalog_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        bbox: list[float] | None = None,
        datetime: str | None = None,
        limit: int | None = None,
        sortby: str | None = None,
        filter_expr: str | None = None,
        filter_lang: str | None = None,
        token: str | None = None,
        query: str | None = None,
        fields: list[str] | None = None,
    ) -> stac_types.ItemCollection:
        """Get items from a collection in a catalog.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection.
            request: Request object.
            bbox: Optional bounding box filter.
            datetime: Optional datetime or interval filter.
            limit: Optional page size.
            sortby: Optional sort specification.
            filter_expr: Optional filter expression.
            filter_lang: Optional filter language.
            token: Optional pagination token.
            query: Optional query string.
            fields: Optional fields to include or exclude.

        Returns:
            ItemCollection containing items from the collection.
        """
        # Verify the catalog exists
        try:
            await self.client.database.find_catalog(catalog_id)
            return await self.client.item_collection(
                collection_id=collection_id,
                request=request,
                bbox=bbox,
                datetime=datetime,
                limit=limit,
                sortby=sortby,
                filter_expr=filter_expr,
                filter_lang=filter_lang,
                token=token,
                query=query,
                fields=fields,
            )
        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Catalog {catalog_id} not found")
            raise e

    async def get_catalog_collection_item(
        self, catalog_id: str, collection_id: str, item_id: str, request: Request
    ) -> stac_types.Item:
        """Get a specific item from a collection in a catalog.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection.
            item_id: The ID of the item.
            request: Request object.

        Returns:
            The requested item.
        """
        # Verify the catalog exists
        try:
            await self.client.database.find_catalog(catalog_id)
        except Exception:
            raise HTTPException(
                status_code=404, detail=f"Catalog {catalog_id} not found"
            )

        # Delegate to the core client's get_item method
        return await self.client.get_item(
            item_id=item_id, collection_id=collection_id, request=request
        )

    async def get_catalog_children(
        self,
        catalog_id: str,
        request: Request,
        limit: int = 10,
        token: str | None = None,
        type: str
        | None = Query(
            None, description="Filter by resource type (Catalog or Collection)"
        ),
    ) -> dict[str, Any]:
        """
        Get all children (Catalogs and Collections) of a specific catalog.

        This is a mixed content endpoint that returns both Catalogs and Collections.
        Catalogs are returned with dynamic parent and child links.
        """
        try:
            await self.client.database.find_catalog(catalog_id)
            search_after = _decode_token(token)

            # 1. Fetch data
            (
                children_data,
                total,
                next_search_after,
            ) = await search_children_with_pagination_shared(
                self.client.database.client,
                catalog_id,
                limit,
                search_after,
                resource_type=type,
            )

            base_url = str(request.base_url)

            # 2. SEPARATE: Pull out catalogs to format them as a batch
            catalog_docs = [
                doc for doc in children_data if doc.get("type") == "Catalog"
            ]

            # 3. BATCH PROCESS: One call to rule them all
            # This triggers the asyncio.gather inside the helper for ALL catalogs at once
            formatted_catalogs_list = []
            if catalog_docs:
                formatted_catalogs_list = await self._format_catalogs_with_links(
                    catalog_docs, request, base_url
                )

            # Create a lookup map for easy re-assembly: { "id": CatalogObject }
            catalog_lookup = {c.id: c for c in formatted_catalogs_list}

            # 4. RE-ASSEMBLE: Put them back in the original search order
            formatted_children = []
            for doc in children_data:
                doc_id = doc.get("id")
                if doc.get("type") == "Catalog":
                    formatted_children.append(catalog_lookup[doc_id])
                else:
                    # Collections don't need parent/child pre-fetching, so serialize normally
                    formatted_children.append(
                        self.client.collection_serializer.db_to_stac(doc, request)
                    )

            # 5. Build links (The rest remains the same)
            links = [
                {"rel": "self", "type": "application/json", "href": str(request.url)},
                {"rel": "root", "type": "application/json", "href": base_url},
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{base_url}catalogs/{catalog_id}",
                },
            ]

            if next_search_after:
                new_token = _encode_token(next_search_after)
                params = {"limit": limit, "token": new_token}
                if type:
                    params["type"] = type
                links.append(
                    {
                        "rel": "next",
                        "type": "application/json",
                        "href": f"{base_url}catalogs/{catalog_id}/children?{urlencode(params)}",
                        "title": "Next page",
                    }
                )

            return {
                "children": formatted_children,
                "links": links,
                "numberReturned": len(formatted_children),
                "numberMatched": total,
            }
        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Catalog {catalog_id} not found")
            logger.error(f"Error in get_catalog_children: {e}")
            raise e

    async def delete_catalog_collection(
        self, catalog_id: str, collection_id: str, request: Request
    ) -> None:
        """Delete a collection from a catalog (Unlink only).

        Removes the catalog from the collection's parent_ids.
        If the collection becomes an orphan (no parents), it is adopted by the Root.
        It NEVER deletes the collection data.

        Args:
            catalog_id: The ID of the catalog.
            collection_id: The ID of the collection.
            request: Request object.

        Raises:
            HTTPException: If the catalog or collection is not found, or if the
                         collection does not belong to the catalog.
        """
        try:
            # Verify the catalog exists
            await self.client.database.find_catalog(catalog_id)

            # Get the collection
            collection_db = await self.client.database.find_collection(collection_id)

            # Check if the catalog_id is in the collection's parent_ids
            parent_ids = collection_db.get("parent_ids") or []
            if catalog_id not in parent_ids:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection {collection_id} does not belong to catalog {catalog_id}",
                )

            # SAFE UNLINK LOGIC
            parent_ids = list(parent_ids)  # Make a copy to avoid modifying the original
            parent_ids.remove(catalog_id)

            # Check if it is now an orphan (empty list)
            if len(parent_ids) == 0:
                # Fallback to Root / Landing Page
                # You can hardcode 'root' or fetch the ID from settings
                root_id = self.settings.get(
                    "STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi"
                )
                parent_ids.append(root_id)
                logger.info(
                    f"Collection {collection_id} unlinked from {catalog_id}. Orphaned, so adopted by root ({root_id})."
                )
            else:
                logger.info(
                    f"Removed catalog {catalog_id} from collection {collection_id}; still belongs to {len(parent_ids)} other catalog(s)"
                )

            # Update the collection in the database
            collection_db["parent_ids"] = parent_ids
            await self.client.database.update_collection(
                collection_id=collection_id, collection=collection_db, refresh=True
            )

        except NotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower():
                raise NotFoundError(f"Resource not found: {e}")
            logger.error(f"Error unlinking collection: {e}")
            raise e
