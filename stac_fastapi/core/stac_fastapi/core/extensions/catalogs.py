"""Catalogs extension."""

import logging
from typing import List, Optional, Type
from urllib.parse import urlencode, urlparse

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
    conformance_classes: List[str] = attr.ib(default=attr.Factory(list))
    router: APIRouter = attr.ib(default=attr.Factory(APIRouter))
    response_class: Type[Response] = attr.ib(default=JSONResponse)

    def register(self, app: FastAPI, settings=None) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.
            settings: extension settings (unused for now).
        """
        self.settings = settings or {}

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

            return catalog
        except Exception:
            raise HTTPException(
                status_code=404, detail=f"Catalog {catalog_id} not found"
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
            # Get the catalog from the database
            db_catalog = await self.client.database.find_catalog(catalog_id)

            # Convert to STAC format to access links
            catalog = self.client.catalog_serializer.db_to_stac(db_catalog, request)

            # Extract collection IDs from catalog links
            #
            # FRAGILE IMPLEMENTATION WARNING:
            # This approach relies on parsing URL patterns to determine catalog-collection relationships.
            # This is fragile and will break if:
            # - URLs don't follow the expected /collections/{id} pattern
            # - Base URLs contain /collections/ in other segments
            # - Relative links are used instead of absolute URLs
            # - Links have trailing slashes or query parameters
            #
            # TODO: In a future version, this should be replaced with a proper database relationship
            # (e.g., parent_catalog_id field on Collection documents)
            #
            collection_ids = []
            if hasattr(catalog, "links") and catalog.links:
                base_url = str(request.base_url).rstrip("/")
                base_path = urlparse(base_url).path.rstrip("/")

                for link in catalog.links:
                    rel = (
                        link.get("rel")
                        if hasattr(link, "get")
                        else getattr(link, "rel", None)
                    )
                    if rel in ["child", "item"]:
                        # Extract collection ID from href using proper URL parsing
                        href = (
                            link.get("href", "")
                            if hasattr(link, "get")
                            else getattr(link, "href", "")
                        )
                        if href:
                            try:
                                parsed_url = urlparse(href)
                                path = parsed_url.path.rstrip("/")

                                # Resolve relative URLs against base URL
                                if not href.startswith(("http://", "https://")):
                                    full_path = (
                                        f"{base_path}{path}" if path else base_path
                                    )
                                else:
                                    # For absolute URLs, ensure they belong to our base domain
                                    if parsed_url.netloc != urlparse(base_url).netloc:
                                        continue
                                    full_path = path

                                # Look for collections endpoint at the end of the path
                                # This prevents false positives when /collections/ appears in base URL
                                collections_pattern = "/collections/"
                                if collections_pattern in full_path:
                                    # Find the LAST occurrence of /collections/ to avoid base URL conflicts
                                    last_collections_pos = full_path.rfind(
                                        collections_pattern
                                    )
                                    if last_collections_pos != -1:
                                        # Extract everything after the last /collections/
                                        after_collections = full_path[
                                            last_collections_pos
                                            + len(collections_pattern) :
                                        ]

                                        # Handle cases where there might be additional path segments
                                        # We only want the immediate collection ID
                                        collection_id = (
                                            after_collections.split("/")[0]
                                            if after_collections
                                            else None
                                        )

                                        if (
                                            collection_id
                                            and collection_id not in collection_ids
                                        ):
                                            collection_ids.append(collection_id)

                            except Exception:
                                # If URL parsing fails, skip this link
                                continue

            # Fetch the collections
            collections = []
            for coll_id in collection_ids:
                try:
                    collection = await self.client.get_collection(
                        coll_id, request=request
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

            # Create the collection using the same pattern as TransactionsClient.create_collection
            # This handles the Collection model from stac_pydantic correctly
            collection_dict = collection.model_dump(mode="json")

            # Add a link from the collection back to its parent catalog BEFORE saving to database
            base_url = str(request.base_url)
            catalog_link = {
                "rel": "catalog",
                "type": "application/json",
                "href": f"{base_url}catalogs/{catalog_id}",
                "title": catalog_id,
            }

            # Add the catalog link to the collection dict
            if "links" not in collection_dict:
                collection_dict["links"] = []

            # Check if the catalog link already exists
            catalog_href = catalog_link["href"]
            link_exists = any(
                link.get("href") == catalog_href and link.get("rel") == "catalog"
                for link in collection_dict.get("links", [])
            )

            if not link_exists:
                collection_dict["links"].append(catalog_link)

            # Now convert to database format (this will process the links)
            collection_db = self.client.database.collection_serializer.stac_to_db(
                collection_dict, request
            )
            await self.client.database.create_collection(
                collection=collection_db, refresh=True
            )

            # Convert back to STAC format for the response
            created_collection = self.client.database.collection_serializer.db_to_stac(
                collection_db,
                request,
                extensions=[
                    type(ext).__name__ for ext in self.client.database.extensions
                ],
            )

            # Update the catalog to include a link to the new collection
            await self._add_collection_to_catalog_links(
                catalog_id, collection.id, request
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

    async def _add_collection_to_catalog_links(
        self, catalog_id: str, collection_id: str, request: Request
    ) -> None:
        """Add a collection link to a catalog.

        This helper method updates a catalog's links to include a reference
        to a collection by reindexing the updated catalog document.

        Args:
            catalog_id: The ID of the catalog to update.
            collection_id: The ID of the collection to link.
            request: Request object for base URL construction.
        """
        try:
            # Get the current catalog
            db_catalog = await self.client.database.find_catalog(catalog_id)
            catalog = self.client.catalog_serializer.db_to_stac(db_catalog, request)

            # Create the collection link
            base_url = str(request.base_url)
            collection_link = {
                "rel": "child",
                "href": f"{base_url}collections/{collection_id}",
                "type": "application/json",
                "title": collection_id,
            }

            # Add the link to the catalog if it doesn't already exist
            catalog_links = (
                catalog.get("links")
                if isinstance(catalog, dict)
                else getattr(catalog, "links", None)
            )
            if not catalog_links:
                catalog_links = []
                if isinstance(catalog, dict):
                    catalog["links"] = catalog_links
                else:
                    catalog.links = catalog_links

            # Check if the collection link already exists
            collection_href = collection_link["href"]
            link_exists = any(
                (
                    link.get("href")
                    if hasattr(link, "get")
                    else getattr(link, "href", None)
                )
                == collection_href
                for link in catalog_links
            )

            if not link_exists:
                catalog_links.append(collection_link)

                # Update the catalog in the database by reindexing it
                # Convert back to database format
                updated_db_catalog = self.client.catalog_serializer.stac_to_db(
                    catalog, request
                )
                updated_db_catalog_dict = (
                    updated_db_catalog.model_dump()
                    if hasattr(updated_db_catalog, "model_dump")
                    else updated_db_catalog
                )
                updated_db_catalog_dict["type"] = "Catalog"

                # Use the same approach as create_catalog to update the document
                await self.client.database.client.index(
                    index=COLLECTIONS_INDEX,
                    id=catalog_id,
                    body=updated_db_catalog_dict,
                    refresh=True,
                )

                logger.info(
                    f"Updated catalog {catalog_id} to include link to collection {collection_id}"
                )

        except Exception as e:
            logger.error(
                f"Failed to update catalog {catalog_id} links: {e}", exc_info=True
            )
            # Don't fail the entire operation if link update fails
            # The collection was created successfully, just the catalog link is missing

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

        # Delegate to the core client's get_collection method
        return await self.client.get_collection(
            collection_id=collection_id, request=request
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
