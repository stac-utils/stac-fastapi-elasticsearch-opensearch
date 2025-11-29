"""Catalogs extension."""

from typing import List, Type

import attr
from fastapi import APIRouter, FastAPI, HTTPException, Request
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

        app.include_router(self.router, tags=["Catalogs"])

    async def catalogs(self, request: Request) -> Catalog:
        """Get root catalog with links to all catalogs.

        Args:
            request: Request object.

        Returns:
            Root catalog containing child links to all catalogs in the database.
        """
        base_url = str(request.base_url)

        # Get all catalogs from database
        catalogs, _, _ = await self.client.database.get_all_catalogs(
            token=None,
            limit=1000,  # Large limit to get all catalogs
            request=request,
            sort=[{"field": "id", "direction": "asc"}],
        )

        # Create child links to each catalog
        child_links = []
        for catalog in catalogs:
            child_links.append(
                {
                    "rel": "child",
                    "href": f"{base_url}catalogs/{catalog.id}",
                    "type": "application/json",
                    "title": catalog.title or catalog.id,
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

        # Create the catalog in the database
        await self.client.database.create_catalog(db_catalog.model_dump())

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
            collection_ids = []
            if hasattr(catalog, "links") and catalog.links:
                for link in catalog.links:
                    if link.get("rel") in ["child", "item"]:
                        # Extract collection ID from href
                        href = link.get("href", "")
                        # Look for patterns like /collections/{id} or collections/{id}
                        if "/collections/" in href:
                            collection_id = href.split("/collections/")[-1].split("/")[
                                0
                            ]
                            if collection_id and collection_id not in collection_ids:
                                collection_ids.append(collection_id)

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
