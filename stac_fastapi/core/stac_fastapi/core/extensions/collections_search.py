"""Collections search extension."""

from typing import List, Optional, Type, Union

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from stac_pydantic.api.search import ExtendedSearch
from starlette.responses import Response

from stac_fastapi.api.models import APIRequest
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.stac import Collections


class CollectionsSearchRequest(ExtendedSearch):
    """Extended search model for collections with free text search support."""

    q: Optional[Union[str, List[str]]] = None
    token: Optional[str] = None
    query: Optional[
        str
    ] = None  # Legacy query extension (deprecated but still supported)


class CollectionsSearchEndpointExtension(ApiExtension):
    """Collections search endpoint extension.

    This extension adds a dedicated /collections-search endpoint for collection search operations.
    """

    def __init__(
        self,
        client: Optional[BaseCoreClient] = None,
        settings: dict = None,
        GET: Optional[Type[Union[BaseModel, APIRequest]]] = None,
        POST: Optional[Type[Union[BaseModel, APIRequest]]] = None,
        conformance_classes: Optional[List[str]] = None,
    ):
        """Initialize the extension.

        Args:
            client: Optional BaseCoreClient instance to use for this extension.
            settings: Dictionary of settings to pass to the extension.
            GET: Optional GET request model.
            POST: Optional POST request model.
            conformance_classes: Optional list of conformance classes to add to the API.
        """
        super().__init__()
        self.client = client
        self.settings = settings or {}
        self.GET = GET
        self.POST = POST
        self.conformance_classes = conformance_classes or []
        self.router = APIRouter()
        self.create_endpoints()

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        app.include_router(self.router)

    def create_endpoints(self) -> None:
        """Create endpoints for the extension."""
        if self.GET:
            self.router.add_api_route(
                name="Get Collections Search",
                path="/collections-search",
                response_model=None,
                response_class=JSONResponse,
                methods=["GET"],
                endpoint=self.collections_search_get_endpoint,
                **(self.settings if isinstance(self.settings, dict) else {}),
            )

        if self.POST:
            self.router.add_api_route(
                name="Post Collections Search",
                path="/collections-search",
                response_model=None,
                response_class=JSONResponse,
                methods=["POST"],
                endpoint=self.collections_search_post_endpoint,
                **(self.settings if isinstance(self.settings, dict) else {}),
            )

    async def collections_search_get_endpoint(
        self, request: Request
    ) -> Union[Collections, Response]:
        """GET /collections-search endpoint.

        Args:
            request: Request object.

        Returns:
            Collections: Collections object.
        """
        # Extract query parameters from the request
        params = dict(request.query_params)

        # Convert query parameters to appropriate types
        if "limit" in params:
            try:
                params["limit"] = int(params["limit"])
            except ValueError:
                pass

        # Handle fields parameter
        if "fields" in params:
            fields_str = params.pop("fields")
            fields = fields_str.split(",")
            params["fields"] = fields

        # Handle sortby parameter
        if "sortby" in params:
            sortby_str = params.pop("sortby")
            sortby = sortby_str.split(",")
            params["sortby"] = sortby

        collections = await self.client.all_collections(request=request, **params)
        return collections

    async def collections_search_post_endpoint(
        self, request: Request, body: dict
    ) -> Union[Collections, Response]:
        """POST /collections-search endpoint.

        Args:
            request: Request object.
            body: Search request body.

        Returns:
            Collections: Collections object.
        """
        # Convert the dict to an ExtendedSearch model
        search_request = CollectionsSearchRequest.model_validate(body)

        # Check if fields are present in the body
        if "fields" in body:
            # Extract fields from body and add them to search_request
            if hasattr(search_request, "field"):
                from stac_pydantic.api.extensions.fields import FieldsExtension

                fields_data = body["fields"]
                search_request.field = FieldsExtension(
                    includes=fields_data.get("include"),
                    excludes=fields_data.get("exclude"),
                )

        # Set the postbody on the request for pagination links
        request.postbody = body

        collections = await self.client.post_all_collections(
            search_request=search_request, request=request
        )

        return collections

    @classmethod
    def from_extensions(
        cls, extensions: List[ApiExtension]
    ) -> "CollectionsSearchEndpointExtension":
        """Create a CollectionsSearchEndpointExtension from a list of extensions.

        Args:
            extensions: List of extensions to include in the CollectionsSearchEndpointExtension.

        Returns:
            CollectionsSearchEndpointExtension: A new CollectionsSearchEndpointExtension instance.
        """
        from stac_fastapi.api.models import (
            create_get_request_model,
            create_post_request_model,
        )

        get_model = create_get_request_model(extensions)
        post_model = create_post_request_model(extensions)

        return cls(
            GET=get_model,
            POST=post_model,
            conformance_classes=[
                ext.conformance_classes
                for ext in extensions
                if hasattr(ext, "conformance_classes")
            ],
        )
