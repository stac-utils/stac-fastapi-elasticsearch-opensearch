"""Catalogs extension."""

import logging
from typing import Any, Dict, List, Optional, Type
from urllib.parse import parse_qs, urlencode, urlparse

import attr
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from stac_pydantic import Collection
from starlette.responses import Response
from typing_extensions import TypedDict

from stac_fastapi.core.models import Catalog
from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.extension import ApiExtension

logger = logging.getLogger(__name__)


class Catalogs(TypedDict, total=False):
    """Catalogs endpoint response.

    Similar to Collections but for catalogs.
    """

    catalogs: List[Catalog]
    links: List[dict]
    numberMatched: Optional[int]
    numberReturned: Optional[int]


@attr.s
class CatalogsExtension(ApiExtension):
    """Catalogs Extension.

    The Catalogs extension adds a /catalogs endpoint that returns a list of all catalogs
    in the database, similar to how /collections returns a list of collections.
    """

    client: BaseCoreClient = attr.ib(default=None)
    settings: dict = attr.ib(default=attr.Factory(dict))
    conformance_classes: List[str] = attr.ib(
        default=attr.Factory(lambda: ["https://api.stacspec.org/v1.0.0-rc.2/children"])
    )
    router: APIRouter = attr.ib(default=attr.Factory(APIRouter))
    response_class: Type[Response] = attr.ib(default=JSONResponse)

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
            description="Delete a collection from a catalog. If the collection has multiple parent catalogs, only removes this catalog from parent_ids. If this is the only parent, deletes the collection entirely.",
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

    async def catalogs(
        self,
        request: Request,
        limit: Optional[int] = Query(
            10,
            ge=1,
            description=(
                "The maximum number of catalogs to return (page size). Defaults to 10."
            ),
        ),
        token: Optional[str] = Query(
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

        # Get all catalogs from database with pagination
        catalogs, next_token, _ = await self.client.database.get_all_catalogs(
            token=token,
            limit=limit,
            request=request,
            sort=[{"field": "id", "direction": "asc"}],
        )

        # Convert database catalogs to STAC format
        catalog_stac_objects = []
        for catalog in catalogs:
            catalog_stac = self.client.catalog_serializer.db_to_stac(catalog, request)
            catalog_stac_objects.append(catalog_stac)

        # Create pagination links
        links = [
            {"rel": "root", "type": "application/json", "href": base_url},
            {"rel": "parent", "type": "application/json", "href": base_url},
            {"rel": "self", "type": "application/json", "href": str(request.url)},
        ]

        # Add next link if there are more pages
        if next_token:
            query_params = {"limit": limit, "token": next_token}
            next_link = {
                "rel": "next",
                "href": f"{base_url}catalogs?{urlencode(query_params)}",
                "type": "application/json",
                "title": "Next page of catalogs",
            }
            links.append(next_link)

        # Return Catalogs object with catalogs
        return Catalogs(
            catalogs=catalog_stac_objects,
            links=links,
            numberReturned=len(catalog_stac_objects),
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

        # Create the catalog in the database with refresh to ensure immediate availability
        await self.client.database.create_catalog(db_catalog_dict, refresh=True)

        # Return the created catalog
        return catalog

    async def get_catalog(self, catalog_id: str, request: Request) -> Catalog:
        """Get a specific catalog by ID.

        Args:
            catalog_id: The ID of the catalog to retrieve.
            request: Request object.

        Returns:
            The requested catalog.
        """
        try:
            # Get the catalog from the database
            db_catalog = await self.client.database.find_catalog(catalog_id)

            # Convert to STAC format
            catalog = self.client.catalog_serializer.db_to_stac(db_catalog, request)

            # DYNAMIC INJECTION: Ensure the 'children' link exists
            # This link points to the /children endpoint which dynamically lists all children
            base_url = str(request.base_url)
            children_link = {
                "rel": "children",
                "type": "application/json",
                "href": f"{base_url}catalogs/{catalog_id}/children",
                "title": "Child catalogs and collections",
            }

            # Convert to dict if needed to manipulate links
            if isinstance(catalog, dict):
                catalog_dict = catalog
            else:
                catalog_dict = (
                    catalog.model_dump()
                    if hasattr(catalog, "model_dump")
                    else dict(catalog)
                )

            # Ensure catalog has a links array
            if "links" not in catalog_dict:
                catalog_dict["links"] = []

            # Add children link if it doesn't already exist
            if not any(
                link.get("rel") == "children" for link in catalog_dict.get("links", [])
            ):
                catalog_dict["links"].append(children_link)

            # Return as Catalog object
            return Catalog(**catalog_dict)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving catalog {catalog_id}: {e}")
            raise HTTPException(
                status_code=404, detail=f"Catalog {catalog_id} not found"
            )

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
            query_body = {"query": {"term": {"parent_ids": catalog_id}}, "size": 10000}
            search_result = await self.client.database.client.search(
                index=COLLECTIONS_INDEX, body=query_body
            )
            children = [hit["_source"] for hit in search_result["hits"]["hits"]]

            # Safe Unlink: Remove catalog from all children's parent_ids
            # If a child becomes an orphan, adopt it to root
            root_id = self.settings.get("STAC_FASTAPI_LANDING_PAGE_ID", "stac-fastapi")

            for child in children:
                child_id = child.get("id")
                try:
                    parent_ids = child.get("parent_ids", [])
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

        except Exception as e:
            error_msg = str(e)
            if "not found" in error_msg.lower():
                raise HTTPException(
                    status_code=404, detail=f"Catalog {catalog_id} not found"
                )
            logger.error(f"Error deleting catalog {catalog_id}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete catalog: {str(e)}",
            )

    async def get_catalog_collections(
        self, catalog_id: str, request: Request
    ) -> stac_types.Collections:
        """Get collections linked from a specific catalog.

        Args:
            catalog_id: The ID of the catalog.
            request: Request object.

        Returns:
            Collections object containing collections linked from the catalog.
        """
        try:
            # Verify the catalog exists
            await self.client.database.find_catalog(catalog_id)

            # Query collections by parent_ids field using Elasticsearch directly
            # This uses the parent_ids field in the collection mapping to find all
            # collections that have this catalog as a parent
            query_body = {"query": {"term": {"parent_ids": catalog_id}}}

            # Execute the search to get collection IDs
            try:
                search_result = await self.client.database.client.search(
                    index=COLLECTIONS_INDEX, body=query_body
                )
            except Exception as e:
                logger.error(
                    f"Error searching for collections with parent {catalog_id}: {e}"
                )
                search_result = {"hits": {"hits": []}}

            # Extract collection IDs from search results
            collection_ids = []
            hits = search_result.get("hits", {}).get("hits", [])
            for hit in hits:
                collection_ids.append(hit.get("_id"))

            # Fetch the collections
            collections = []
            for coll_id in collection_ids:
                try:
                    # Get the collection from database
                    collection_db = await self.client.database.find_collection(coll_id)
                    # Serialize with catalog context (sets parent to catalog, injects catalog link)
                    collection = (
                        self.client.collection_serializer.db_to_stac_in_catalog(
                            collection_db,
                            request,
                            catalog_id=catalog_id,
                            extensions=[
                                type(ext).__name__
                                for ext in self.client.database.extensions
                            ],
                        )
                    )
                    collections.append(collection)
                except HTTPException as e:
                    # Only skip collections that are not found (404)
                    if e.status_code == 404:
                        logger.debug(f"Collection {coll_id} not found, skipping")
                        continue
                    else:
                        # Re-raise other HTTP exceptions (5xx server errors, etc.)
                        logger.error(f"HTTP error retrieving collection {coll_id}: {e}")
                        raise
                except Exception as e:
                    # Log unexpected errors and re-raise them
                    logger.error(
                        f"Unexpected error retrieving collection {coll_id}: {e}"
                    )
                    raise

            # Return in Collections format
            base_url = str(request.base_url)
            return stac_types.Collections(
                collections=collections,
                links=[
                    {"rel": "root", "type": "application/json", "href": base_url},
                    {"rel": "parent", "type": "application/json", "href": base_url},
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": f"{base_url}catalogs/{catalog_id}/collections",
                    },
                ],
            )

        except HTTPException:
            # Re-raise HTTP exceptions as-is
            raise
        except Exception as e:
            logger.error(
                f"Error retrieving collections for catalog {catalog_id}: {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=404, detail=f"Catalog {catalog_id} not found"
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
        # Verify the catalog exists
        try:
            await self.client.database.find_catalog(catalog_id)
        except Exception:
            raise HTTPException(
                status_code=404, detail=f"Catalog {catalog_id} not found"
            )

        # Verify the collection exists and has the catalog as a parent
        try:
            collection_db = await self.client.database.find_collection(collection_id)

            # Check if the catalog_id is in the collection's parent_ids
            parent_ids = collection_db.get("parent_ids", [])
            if catalog_id not in parent_ids:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection {collection_id} does not belong to catalog {catalog_id}",
                )
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(
                status_code=404, detail=f"Collection {collection_id} not found"
            )

        # Return the collection with catalog context
        collection_db = await self.client.database.find_collection(collection_id)
        return self.client.collection_serializer.db_to_stac_in_catalog(
            collection_db,
            request,
            catalog_id=catalog_id,
            extensions=[type(ext).__name__ for ext in self.client.database.extensions],
        )

    async def get_catalog_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        bbox: Optional[List[float]] = None,
        datetime: Optional[str] = None,
        limit: Optional[int] = None,
        sortby: Optional[str] = None,
        filter_expr: Optional[str] = None,
        filter_lang: Optional[str] = None,
        token: Optional[str] = None,
        query: Optional[str] = None,
        fields: Optional[List[str]] = None,
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
        except Exception:
            raise HTTPException(
                status_code=404, detail=f"Catalog {catalog_id} not found"
            )

        # Delegate to the core client's item_collection method with all parameters
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
        token: str = None,
        type: Optional[str] = Query(
            None, description="Filter by resource type (Catalog or Collection)"
        ),
    ) -> Dict[str, Any]:
        """
        Get all children (Catalogs and Collections) of a specific catalog.

        This is a 'Union' endpoint that returns mixed content types.
        """
        # 1. Verify the parent catalog exists
        await self.client.database.find_catalog(catalog_id)

        # 2. Build the Search Query
        # We search the COLLECTIONS_INDEX because it holds both Catalogs and Collections

        # Base filter: Parent match
        # This finds anything where 'parent_ids' contains this catalog_id
        filter_queries = [{"term": {"parent_ids": catalog_id}}]

        # Optional filter: Type
        if type:
            # If user asks for ?type=Catalog, we only return Catalogs
            filter_queries.append({"term": {"type": type}})

        # 3. Calculate Pagination (Search After)
        body = {
            "query": {"bool": {"filter": filter_queries}},
            "sort": [{"id": {"order": "asc"}}],  # Stable sort for pagination
            "size": limit,
        }

        # Handle search_after token - split by '|' to get all sort values
        search_after: Optional[List[str]] = None
        if token:
            try:
                # The token should be a pipe-separated string of sort values
                # e.g., "collection-1"
                from typing import cast

                search_after_parts = cast(List[str], token.split("|"))
                # If the number of sort fields doesn't match token parts, ignore the token
                if len(search_after_parts) != len(body["sort"]):  # type: ignore
                    search_after = None
                else:
                    search_after = search_after_parts
            except Exception:
                search_after = None

            if search_after is not None:
                body["search_after"] = search_after

        # 4. Execute Search
        search_result = await self.client.database.client.search(
            index=COLLECTIONS_INDEX, body=body
        )

        # 5. Process Results
        hits = search_result.get("hits", {}).get("hits", [])
        total = search_result.get("hits", {}).get("total", {}).get("value", 0)

        children = []
        for hit in hits:
            doc = hit["_source"]
            resource_type = doc.get(
                "type", "Collection"
            )  # Default to Collection if missing

            # Serialize based on type
            # This ensures we hide internal fields like 'parent_ids' correctly
            if resource_type == "Catalog":
                child = self.client.catalog_serializer.db_to_stac(doc, request)
            else:
                child = self.client.collection_serializer.db_to_stac(doc, request)

            children.append(child)

        # 6. Format Response
        # The Children extension uses a specific response format
        response = {
            "children": children,
            "links": [
                {"rel": "self", "type": "application/json", "href": str(request.url)},
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": str(request.base_url),
                },
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{str(request.base_url)}catalogs/{catalog_id}",
                },
            ],
            "numberReturned": len(children),
            "numberMatched": total,
        }

        # 7. Generate Next Link
        next_token = None
        if len(hits) == limit:
            next_token_values = hits[-1].get("sort")
            if next_token_values:
                # Join all sort values with '|' to create the token
                next_token = "|".join(str(val) for val in next_token_values)

        if next_token:
            # Get existing query params
            parsed_url = urlparse(str(request.url))
            params = parse_qs(parsed_url.query)

            # Update params
            params["token"] = [next_token]
            params["limit"] = [str(limit)]
            if type:
                params["type"] = [type]

            # Flatten params for urlencode (parse_qs returns lists)
            flat_params = {
                k: v[0] if isinstance(v, list) else v for k, v in params.items()
            }

            next_link = {
                "rel": "next",
                "type": "application/json",
                "href": f"{request.base_url}catalogs/{catalog_id}/children?{urlencode(flat_params)}",
            }
            response["links"].append(next_link)

        return response

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
            parent_ids = collection_db.get("parent_ids", [])
            if catalog_id not in parent_ids:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection {collection_id} does not belong to catalog {catalog_id}",
                )

            # SAFE UNLINK LOGIC
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

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"Error removing collection {collection_id} from catalog {catalog_id}: {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to remove collection from catalog: {str(e)}",
            )
