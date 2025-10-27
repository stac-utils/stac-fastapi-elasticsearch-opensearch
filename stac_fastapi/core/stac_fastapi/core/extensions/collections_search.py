"""Collections search extension."""

from typing import Any, Dict, List, Optional, Type, Union

from fastapi import APIRouter, Body, FastAPI, Query, Request
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
    filter_expr: Optional[str] = None
    filter_lang: Optional[str] = None


def build_get_collections_search_doc(original_endpoint):
    """Return a documented GET endpoint wrapper for /collections-search."""

    async def documented_endpoint(
        request: Request,
        q: Optional[Union[str, List[str]]] = Query(
            None,
            description="Free text search query",
        ),
        query: Optional[str] = Query(
            None,
            description="Additional filtering expressed as a string (legacy support)",
            example="platform=landsat AND collection_category=level2",
        ),
        limit: int = Query(
            10,
            ge=1,
            description=(
                "The maximum number of collections to return (page size). Defaults to 10."
            ),
        ),
        token: Optional[str] = Query(
            None,
            description="Pagination token for the next page of results",
        ),
        bbox: Optional[str] = Query(
            None,
            description=(
                "Bounding box for spatial filtering in format 'minx,miny,maxx,maxy' "
                "or 'minx,miny,minz,maxx,maxy,maxz'"
            ),
        ),
        datetime: Optional[str] = Query(
            None,
            description=(
                "Temporal filter in ISO 8601 format (e.g., "
                "'2020-01-01T00:00:00Z/2021-01-01T00:00:00Z')"
            ),
        ),
        sortby: Optional[str] = Query(
            None,
            description=(
                "Sorting criteria in the format 'field' or '-field' for descending order"
            ),
        ),
        fields: Optional[List[str]] = Query(
            None,
            description=(
                "Comma-separated list of fields to include or exclude (use -field to exclude)"
            ),
            alias="fields[]",
        ),
        filter: Optional[str] = Query(
            None,
            description=(
                "Structured filter expression in CQL2 JSON or CQL2-text format"
            ),
            example='{"op": "=", "args": [{"property": "properties.category"}, "level2"]}',
        ),
        filter_lang: Optional[str] = Query(
            None,
            description=(
                "Filter language. Must be 'cql2-json' or 'cql2-text' if specified"
            ),
            example="cql2-json",
        ),
    ):
        # Delegate to original endpoint with parameters
        # Since FastAPI extracts parameters from the URL when they're defined as function parameters,
        # we need to create a request wrapper that provides our modified query_params

        # Create a mutable copy of query_params
        if hasattr(request, "_query_params"):
            query_params = dict(request._query_params)
        else:
            query_params = dict(request.query_params)

        # Add q parameter back to query_params if it was provided
        # Convert to list format to match /collections behavior
        if q is not None:
            if isinstance(q, str):
                # Single string should become a list with one element
                query_params["q"] = [q]
            elif isinstance(q, list):
                # Already a list, use as-is
                query_params["q"] = q

        # Add filter parameters back to query_params if they were provided
        if filter is not None:
            query_params["filter"] = filter
        if filter_lang is not None:
            query_params["filter-lang"] = filter_lang

        # Create a request wrapper that provides our modified query_params
        class RequestWrapper:
            def __init__(self, original_request, modified_query_params):
                self._original = original_request
                self._query_params = modified_query_params

            @property
            def query_params(self):
                return self._query_params

            def __getattr__(self, name):
                # Delegate all other attributes to the original request
                return getattr(self._original, name)

        wrapped_request = RequestWrapper(request, query_params)
        return await original_endpoint(wrapped_request)

    documented_endpoint.__name__ = original_endpoint.__name__
    return documented_endpoint


def build_post_collections_search_doc(original_post_endpoint):
    """Return a documented POST endpoint wrapper for /collections-search."""

    async def documented_post_endpoint(
        request: Request,
        body: Dict[str, Any] = Body(
            ...,
            description=(
                "Search parameters for collections.\n\n"
                "- `q`: Free text search query (string or list of strings)\n"
                "- `query`: Additional filtering expressed as a string (legacy support)\n"
                "- `filter`: Structured filter expression in CQL2 JSON or CQL2-text format\n"
                "- `filter_lang`: Filter language. Must be 'cql2-json' or 'cql2-text' if specified\n"
                "- `limit`: Maximum number of results to return (default: 10)\n"
                "- `token`: Pagination token for the next page of results\n"
                "- `bbox`: Bounding box [minx, miny, maxx, maxy] or [minx, miny, minz, maxx, maxy, maxz]\n"
                "- `datetime`: Temporal filter in ISO 8601 (e.g., '2020-01-01T00:00:00Z/2021-01-01T12:31:12Z')\n"
                "- `sortby`: List of sort criteria objects with 'field' and 'direction' (asc/desc)\n"
                "- `fields`: Object with 'include' and 'exclude' arrays for field selection"
            ),
            example={
                "q": "landsat",
                "query": "platform=landsat AND collection_category=level2",
                "filter": {
                    "op": "=",
                    "args": [{"property": "properties.category"}, "level2"],
                },
                "filter_lang": "cql2-json",
                "limit": 10,
                "token": "next-page-token",
                "bbox": [-180, -90, 180, 90],
                "datetime": "2020-01-01T00:00:00Z/2021-01-01T12:31:12Z",
                "sortby": [{"field": "id", "direction": "asc"}],
                "fields": {
                    "include": ["id", "title", "description"],
                    "exclude": ["properties"],
                },
            },
        ),
    ) -> Union[Collections, Response]:
        return await original_post_endpoint(request, body)

    documented_post_endpoint.__name__ = original_post_endpoint.__name__
    return documented_post_endpoint


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

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        # Remove any existing routes to avoid duplicates
        self.router.routes = []

        # Recreate endpoints with proper OpenAPI documentation
        if self.GET:
            original_endpoint = self.collections_search_get_endpoint
            documented_endpoint = build_get_collections_search_doc(original_endpoint)

            self.router.add_api_route(
                path="/collections-search",
                endpoint=documented_endpoint,
                response_model=None,
                response_class=JSONResponse,
                methods=["GET"],
                summary="Search collections",
                description=(
                    "Search for collections using query parameters. "
                    "Supports filtering, sorting, and field selection."
                ),
                response_description="A list of collections matching the search criteria",
                tags=["Collections Search Extension"],
                **(self.settings if isinstance(self.settings, dict) else {}),
            )

        if self.POST:
            original_post_endpoint = self.collections_search_post_endpoint
            documented_post_endpoint = build_post_collections_search_doc(
                original_post_endpoint
            )

            self.router.add_api_route(
                path="/collections-search",
                endpoint=documented_post_endpoint,
                response_model=None,
                response_class=JSONResponse,
                methods=["POST"],
                summary="Search collections",
                description=(
                    "Search for collections using a JSON request body. "
                    "Supports filtering, sorting, field selection, and pagination."
                ),
                tags=["Collections Search Extension"],
                **(self.settings if isinstance(self.settings, dict) else {}),
            )

        app.include_router(self.router)

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

        # Handle filter parameter mapping (fixed for collections-search)
        if "filter" in params:
            params["filter_expr"] = params.pop("filter")

        # Handle filter-lang parameter mapping (fixed for collections-search)
        if "filter-lang" in params:
            params["filter_lang"] = params.pop("filter-lang")

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
