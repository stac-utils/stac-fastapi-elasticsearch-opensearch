"""Catalogs extension."""

from typing import List, Optional, Type
from urllib.parse import urlparse

import attr
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from starlette.responses import Response

from stac_fastapi.core.models import Catalog
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.extension import ApiExtension


@attr.s
class CatalogsExtension(ApiExtension):
    """Catalogs Extension.

    The Catalogs extension adds a /catalogs endpoint that returns the root catalog
    containing child links to all catalogs in the database.
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
            response_model=Catalog,
            response_class=self.response_class,
            summary="Get Root Catalog",
            description="Returns the root catalog containing links to all catalogs.",
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
    ) -> Catalog:
        """Get root catalog with links to all catalogs.

        Args:
            request: Request object.
            limit: The maximum number of catalogs to return (page size). Defaults to 10.
            token: Pagination token for the next page of results.

        Returns:
            Root catalog containing child links to all catalogs in the database.
        """
        base_url = str(request.base_url)

        # Get all catalogs from database with pagination
        catalogs, _, _ = await self.client.database.get_all_catalogs(
            token=token,
            limit=limit,
            request=request,
            sort=[{"field": "id", "direction": "asc"}],
        )

        # Create child links to each catalog
        child_links = []
        for catalog in catalogs:
            catalog_id = catalog.get("id") if isinstance(catalog, dict) else catalog.id
            catalog_title = (
                catalog.get("title") or catalog_id
                if isinstance(catalog, dict)
                else catalog.title or catalog.id
            )
            child_links.append(
                {
                    "rel": "child",
                    "href": f"{base_url}catalogs/{catalog_id}",
                    "type": "application/json",
                    "title": catalog_title,
                }
            )

        # Create root catalog
        root_catalog = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "root",
            "title": "Root Catalog",
            "description": "Root catalog containing all available catalogs",
            "links": [
                {
                    "rel": "self",
                    "href": f"{base_url}catalogs",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": f"{base_url}catalogs",
                    "type": "application/json",
                },
                {
                    "rel": "parent",
                    "href": base_url.rstrip("/"),
                    "type": "application/json",
                },
            ]
            + child_links,
        }

        return Catalog(**root_catalog)

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
                for link in catalog.links:
                    if link.get("rel") in ["child", "item"]:
                        # Extract collection ID from href using proper URL parsing
                        href = link.get("href", "")
                        if href:
                            try:
                                parsed_url = urlparse(href)
                                path = parsed_url.path

                                # Verify this is our expected URL pattern by checking it starts with base_url
                                # or is a relative path that would resolve to our server
                                full_href = (
                                    href
                                    if href.startswith(("http://", "https://"))
                                    else f"{base_url}{href}"
                                )
                                if not full_href.startswith(base_url):
                                    continue

                                # Look for patterns like /collections/{id} or collections/{id}
                                if "/collections/" in path:
                                    # Split by /collections/ and take the last segment
                                    path_parts = path.split("/collections/")
                                    if len(path_parts) > 1:
                                        collection_id = path_parts[1].split("/")[0]
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
                except Exception:
                    # Skip collections that can't be found
                    continue

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

        except Exception:
            raise HTTPException(
                status_code=404, detail=f"Catalog {catalog_id} not found"
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
