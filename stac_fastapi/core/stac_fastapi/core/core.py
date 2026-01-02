"""Core client."""

import logging
import os
from datetime import datetime as datetime_type
from datetime import timezone
from enum import Enum
from typing import List, Optional, Set, Type, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
from fastapi import HTTPException, Request, Response
from overrides import overrides
from pydantic import TypeAdapter, ValidationError
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic import Collection, Item, ItemCollection
from stac_pydantic.links import Relations
from stac_pydantic.shared import BBox, MimeTypes
from stac_pydantic.version import STAC_VERSION

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.datetime_utils import format_datetime_range
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.core.serializers import (
    CatalogSerializer,
    CollectionSerializer,
    ItemSerializer,
)
from stac_fastapi.core.session import Session
from stac_fastapi.core.utilities import filter_fields, get_bool_env
from stac_fastapi.extensions.core.transaction import AsyncBaseTransactionsClient
from stac_fastapi.extensions.core.transaction.request import (
    PartialCollection,
    PartialItem,
    PatchOperation,
)
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    BulkTransactionMethod,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.conformance import BASE_CONFORMANCE_CLASSES
from stac_fastapi.types.core import AsyncBaseCoreClient
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.requests import get_base_url
from stac_fastapi.types.search import BaseSearchPostRequest

# VECTOR TILES
import mercantile, mapbox_vector_tile
from cachetools import TTLCache
from shapely.geometry import shape, mapping, box
from shapely.ops import transform
import pyproj
import time
import gzip

logger = logging.getLogger(__name__)

partialItemValidator = TypeAdapter(PartialItem)
partialCollectionValidator = TypeAdapter(PartialCollection)

PROJECT_4326_TO_3857 = pyproj.Transformer.from_crs(
    "EPSG:4326", "EPSG:3857", always_xy=True
)


@attr.s
class CoreClient(AsyncBaseCoreClient):
    """Client for core endpoints defined by the STAC specification.

    This class is a implementation of `AsyncBaseCoreClient` that implements the core endpoints
    defined by the STAC specification. It uses the `DatabaseLogic` class to interact with the
    database, and `ItemSerializer` and `CollectionSerializer` to convert between STAC objects and
    database records.

    Attributes:
        session (Session): A requests session instance to be used for all HTTP requests.
        item_serializer (Type[serializers.ItemSerializer]): A serializer class to be used to convert
            between STAC items and database records.
        collection_serializer (Type[serializers.CollectionSerializer]): A serializer class to be
            used to convert between STAC collections and database records.
        database (DatabaseLogic): An instance of the `DatabaseLogic` class that is used to interact
            with the database.
    """

    database: BaseDatabaseLogic = attr.ib()
    base_conformance_classes: List[str] = attr.ib(
        factory=lambda: BASE_CONFORMANCE_CLASSES
    )
    extensions: List[ApiExtension] = attr.ib(default=attr.Factory(list))

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    item_serializer: Type[ItemSerializer] = attr.ib(default=ItemSerializer)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )
    catalog_serializer: Type[CatalogSerializer] = attr.ib(default=CatalogSerializer)
    post_request_model = attr.ib(default=BaseSearchPostRequest)
    stac_version: str = attr.ib(default=STAC_VERSION)
    landing_page_id: str = attr.ib(default="stac-fastapi")
    title: str = attr.ib(default="stac-fastapi")
    description: str = attr.ib(default="stac-fastapi")

    def extension_is_enabled(self, extension_name: str) -> bool:
        """Check if an extension is enabled by checking self.extensions.

        Args:
            extension_name: Name of the extension class to check for.

        Returns:
            True if the extension is in self.extensions, False otherwise.
        """
        return any(ext.__class__.__name__ == extension_name for ext in self.extensions)

    def _landing_page(
        self,
        base_url: str,
        conformance_classes: List[str],
        extension_schemas: List[str],
    ) -> stac_types.LandingPage:
        landing_page = stac_types.LandingPage(
            type="Catalog",
            id=self.landing_page_id,
            title=self.title,
            description=self.description,
            stac_version=self.stac_version,
            conformsTo=conformance_classes,
            links=[
                {
                    "rel": Relations.self.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": Relations.root.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": "data",
                    "type": MimeTypes.json,
                    "href": urljoin(base_url, "collections"),
                },
                {
                    "rel": Relations.conformance.value,
                    "type": MimeTypes.json,
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": urljoin(base_url, "conformance"),
                },
                {
                    "rel": Relations.search.value,
                    "type": MimeTypes.geojson,
                    "title": "STAC search",
                    "href": urljoin(base_url, "search"),
                    "method": "GET",
                },
                {
                    "rel": Relations.search.value,
                    "type": MimeTypes.geojson,
                    "title": "STAC search",
                    "href": urljoin(base_url, "search"),
                    "method": "POST",
                },
            ],
            stac_extensions=extension_schemas,
        )
        return landing_page

    async def landing_page(self, **kwargs) -> stac_types.LandingPage:
        """Landing page.

        Called with `GET /`.

        Returns:
            API landing page, serving as an entry point to the API.
        """
        request: Request = kwargs["request"]

        base_url = get_base_url(request)
        landing_page = self._landing_page(
            base_url=base_url,
            conformance_classes=self.conformance_classes(),
            extension_schemas=[],
        )

        if self.extension_is_enabled("FilterExtension"):
            landing_page["links"].append(
                {
                    # TODO: replace this with Relations.queryables.value,
                    "rel": "queryables",
                    # TODO: replace this with MimeTypes.jsonschema,
                    "type": "application/schema+json",
                    "title": "Queryables",
                    "href": urljoin(base_url, "queryables"),
                }
            )

        if self.extension_is_enabled("AggregationExtension"):
            landing_page["links"].extend(
                [
                    {
                        "rel": "aggregate",
                        "type": "application/json",
                        "title": "Aggregate",
                        "href": urljoin(base_url, "aggregate"),
                    },
                    {
                        "rel": "aggregations",
                        "type": "application/json",
                        "title": "Aggregations",
                        "href": urljoin(base_url, "aggregations"),
                    },
                ]
            )

        if self.extension_is_enabled("CollectionsSearchEndpointExtension"):
            landing_page["links"].extend(
                [
                    {
                        "rel": "collections-search",
                        "type": "application/json",
                        "title": "Collections Search",
                        "href": urljoin(base_url, "collections-search"),
                        "method": "GET",
                    },
                    {
                        "rel": "collections-search",
                        "type": "application/json",
                        "title": "Collections Search",
                        "href": urljoin(base_url, "collections-search"),
                        "method": "POST",
                    },
                ]
            )

        if self.extension_is_enabled("CatalogsExtension"):
            landing_page["links"].append(
                {
                    "rel": "catalogs",
                    "type": "application/json",
                    "title": "Catalogs",
                    "href": urljoin(base_url, "catalogs"),
                }
            )

        # Add OpenAPI URL
        landing_page["links"].append(
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI service description",
                "href": urljoin(
                    str(request.base_url), request.app.openapi_url.lstrip("/")
                ),
            }
        )

        # Add human readable service-doc
        landing_page["links"].append(
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI service documentation",
                "href": urljoin(
                    str(request.base_url), request.app.docs_url.lstrip("/")
                ),
            }
        )

        return landing_page

    async def all_collections(
        self,
        limit: Optional[int] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[Union[str, List[str]]] = None,
        filter_expr: Optional[str] = None,
        filter_lang: Optional[str] = None,
        q: Optional[Union[str, List[str]]] = None,
        query: Optional[str] = None,
        request: Request = None,
        token: Optional[str] = None,
        **kwargs,
    ) -> stac_types.Collections:
        """Read all collections from the database.

        Args:
            limit (Optional[int]): Maximum number of collections to return.
            bbox (Optional[BBox]): Bounding box to filter collections by spatial extent.
            datetime (Optional[str]): Filter collections by datetime range.
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[Union[str, List[str]]]): Sorting options for the results.
            filter_expr (Optional[str]): Structured filter expression in CQL2 JSON or CQL2-text format.
            filter_lang (Optional[str]): Must be 'cql2-json' or 'cql2-text' if specified, other values will result in an error.
            q (Optional[Union[str, List[str]]]): Free text search terms.
            query (Optional[str]): Legacy query parameter (deprecated).
            request (Request): FastAPI Request object.
            token (Optional[str]): Pagination token for retrieving the next page of results.
            **kwargs: Keyword arguments from the request.

        Returns:
            A Collections object containing all the collections in the database and links to various resources.
        """
        base_url = str(request.base_url)
        redis_enable = get_bool_env("REDIS_ENABLE", default=False)

        global_max_limit = (
            int(os.getenv("STAC_GLOBAL_COLLECTION_MAX_LIMIT"))
            if os.getenv("STAC_GLOBAL_COLLECTION_MAX_LIMIT")
            else None
        )
        query_limit = request.query_params.get("limit")
        default_limit = int(os.getenv("STAC_DEFAULT_COLLECTION_LIMIT", 300))

        body_limit = None
        try:
            if request.method == "POST" and request.body():
                body_data = await request.json()
                body_limit = body_data.get("limit")
        except Exception:
            pass

        if body_limit is not None:
            limit = int(body_limit)
        elif query_limit:
            limit = int(query_limit)
        else:
            limit = default_limit

        if global_max_limit is not None:
            limit = min(limit, global_max_limit)

        # Get token from query params only if not already provided (for GET requests)
        if token is None:
            token = request.query_params.get("token")

        # Process fields parameter for filtering collection properties
        includes, excludes = set(), set()
        if fields:
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                else:
                    include_field = field[1:] if field[0] in "+ " else field
                    includes.add(include_field)

        sort = None
        if sortby:
            parsed_sort = []
            for raw in sortby:
                if not isinstance(raw, str):
                    continue
                s = raw.strip()
                if not s:
                    continue
                direction = "desc" if s[0] == "-" else "asc"
                field = s[1:] if s and s[0] in "+-" else s
                parsed_sort.append({"field": field, "direction": direction})
            if parsed_sort:
                sort = parsed_sort

        # Convert q to a list if it's a string
        q_list = None
        if q is not None:
            q_list = [q] if isinstance(q, str) else q

        # Parse the query parameter if provided
        parsed_query = None
        if query is not None:
            try:
                parsed_query = orjson.loads(query)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid query parameter: {e}"
                )

        # Parse the filter parameter if provided
        parsed_filter = None
        if filter_expr is not None:
            try:
                # Only raise an error for explicitly unsupported filter languages
                if filter_lang is not None and filter_lang not in [
                    "cql2-json",
                    "cql2-text",
                ]:
                    # Raise an error for unsupported filter languages
                    raise HTTPException(
                        status_code=400,
                        detail=f"Only 'cql2-json' and 'cql2-text' filter languages are supported for collections. Got '{filter_lang}'.",
                    )

                # Handle different filter formats
                try:
                    if filter_lang == "cql2-text" or filter_lang is None:
                        # For cql2-text or when no filter_lang is specified, try both formats
                        try:
                            # First try to parse as JSON
                            parsed_filter = orjson.loads(unquote_plus(filter_expr))
                        except Exception:
                            # If that fails, use pygeofilter to convert CQL2-text to CQL2-JSON
                            try:
                                # Parse CQL2-text and convert to CQL2-JSON
                                text_filter = unquote_plus(filter_expr)
                                parsed_ast = parse_cql2_text(text_filter)
                                parsed_filter = to_cql2(parsed_ast)
                            except Exception as e:
                                # If parsing fails, provide a helpful error message
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Invalid CQL2-text filter: {e}. Please check your syntax.",
                                )
                    else:
                        # For explicit cql2-json, parse as JSON
                        parsed_filter = orjson.loads(unquote_plus(filter_expr))
                except Exception as e:
                    # Catch any other parsing errors
                    raise HTTPException(
                        status_code=400, detail=f"Error parsing filter: {e}"
                    )

            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid filter parameter: {e}"
                )

        parsed_datetime = None
        if datetime:
            parsed_datetime = format_datetime_range(date_str=datetime)

        collections, next_token, maybe_count = await self.database.get_all_collections(
            token=token,
            limit=limit,
            request=request,
            sort=sort,
            bbox=bbox,
            q=q_list,
            filter=parsed_filter,
            query=parsed_query,
            datetime=parsed_datetime,
        )

        # Apply field filtering if fields parameter was provided
        if fields:
            filtered_collections = [
                filter_fields(collection, includes, excludes)
                for collection in collections
            ]
        else:
            filtered_collections = collections

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": base_url},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, "collections"),
            },
        ]

        if redis_enable:
            from stac_fastapi.core.redis_utils import redis_pagination_links

            await redis_pagination_links(
                current_url=str(request.url),
                token=token,
                next_token=next_token,
                links=links,
            )

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return stac_types.Collections(
            collections=filtered_collections,
            links=links,
            numberMatched=maybe_count,
            numberReturned=len(filtered_collections),
        )

    async def post_all_collections(
        self, search_request: BaseSearchPostRequest, request: Request, **kwargs
    ) -> stac_types.Collections:
        """Search collections with POST request.

        Args:
            search_request (BaseSearchPostRequest): The search request.
            request (Request): The request.

        Returns:
            A Collections object containing all the collections in the database and links to various resources.
        """
        request.postbody = search_request.model_dump(exclude_unset=True)

        fields = None

        # Check for field attribute (ExtendedSearch format)
        if hasattr(search_request, "field") and search_request.field:
            fields = []

            # Handle include fields
            if (
                hasattr(search_request.field, "includes")
                and search_request.field.includes
            ):
                for field in search_request.field.includes:
                    fields.append(f"+{field}")

            # Handle exclude fields
            if (
                hasattr(search_request.field, "excludes")
                and search_request.field.excludes
            ):
                for field in search_request.field.excludes:
                    fields.append(f"-{field}")

        # Convert sortby parameter from POST format to all_collections format
        sortby = None
        # Check for sortby attribute
        if hasattr(search_request, "sortby") and search_request.sortby:
            # Create a list of sort strings in the format expected by all_collections
            sortby = []
            for sort_item in search_request.sortby:
                # Handle different types of sort items
                if hasattr(sort_item, "field") and hasattr(sort_item, "direction"):
                    # This is a Pydantic model with field and direction attributes
                    field = sort_item.field
                    direction = sort_item.direction
                elif isinstance(sort_item, dict):
                    # This is a dictionary with field and direction keys
                    field = sort_item.get("field")
                    direction = sort_item.get("direction", "asc")
                else:
                    # Skip this item if we can't extract field and direction
                    continue

                if field:
                    # Create a sort string in the format expected by all_collections
                    # e.g., "-id" for descending sort on id field
                    prefix = "-" if direction.lower() == "desc" else ""
                    sortby.append(f"{prefix}{field}")

        # Pass all parameters from search_request to all_collections
        return await self.all_collections(
            limit=search_request.limit if hasattr(search_request, "limit") else None,
            bbox=search_request.bbox if hasattr(search_request, "bbox") else None,
            datetime=(
                search_request.datetime if hasattr(search_request, "datetime") else None
            ),
            token=search_request.token if hasattr(search_request, "token") else None,
            fields=fields,
            sortby=sortby,
            filter_expr=(
                search_request.filter if hasattr(search_request, "filter") else None
            ),
            filter_lang=(
                search_request.filter_lang
                if hasattr(search_request, "filter_lang")
                else None
            ),
            query=search_request.query if hasattr(search_request, "query") else None,
            q=search_request.q if hasattr(search_request, "q") else None,
            request=request,
            **kwargs,
        )

    async def get_collection(
        self, collection_id: str, **kwargs
    ) -> stac_types.Collection:
        """Get a collection from the database by its id.

        Args:
            collection_id (str): The id of the collection to retrieve.
            kwargs: Additional keyword arguments passed to the API call.

        Returns:
            Collection: A `Collection` object representing the requested collection.

        Raises:
            NotFoundError: If the collection with the given id cannot be found in the database.
        """
        request = kwargs["request"]
        collection = await self.database.find_collection(collection_id=collection_id)
        return self.collection_serializer.db_to_stac(
            collection=collection,
            request=request,
            extensions=[type(ext).__name__ for ext in self.extensions],
        )

    async def item_collection(
        self,
        collection_id: str,
        request: Request,
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
        limit: Optional[int] = None,
        sortby: Optional[str] = None,
        filter_expr: Optional[str] = None,
        filter_lang: Optional[str] = None,
        token: Optional[str] = None,
        query: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """List items within a specific collection.

        This endpoint delegates to ``get_search`` under the hood with
        ``collections=[collection_id]`` so that filtering, sorting and pagination
        behave identically to the Search endpoints.

        Args:
            collection_id (str): ID of the collection to list items from.
            request (Request): FastAPI Request object.
            bbox (Optional[BBox]): Optional bounding box filter.
            datetime (Optional[str]): Optional datetime or interval filter.
            limit (Optional[int]): Optional page size. Defaults to env `STAC_DEFAULT_ITEM_LIMIT` when unset.
            sortby (Optional[str]): Optional sort specification. Accepts repeated values
                like ``sortby=-properties.datetime`` or ``sortby=+id``. Bare fields (e.g. ``sortby=id``)
                imply ascending order.
            token (Optional[str]): Optional pagination token.
            query (Optional[str]): Optional query string.
            filter_expr (Optional[str]): Optional filter expression.
            filter_lang (Optional[str]): Optional filter language.
            fields (Optional[List[str]]): Fields to include or exclude from the results.

        Returns:
            ItemCollection: Feature collection with items, paging links, and counts.

        Raises:
            HTTPException: 404 if the collection does not exist.
        """
        try:
            await self.get_collection(collection_id=collection_id, request=request)
        except Exception:
            raise HTTPException(status_code=404, detail="Collection not found")

        # Delegate directly to GET search for consistency
        return await self.get_search(
            request=request,
            collections=[collection_id],
            bbox=bbox,
            datetime=datetime,
            limit=limit,
            token=token,
            sortby=sortby,
            query=query,
            filter_expr=filter_expr,
            filter_lang=filter_lang,
            fields=fields,
        )

    async def get_item(
        self, item_id: str, collection_id: str, **kwargs
    ) -> stac_types.Item:
        """Get an item from the database based on its id and collection id.

        Args:
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be retrieved.

        Returns:
            Item: An `Item` object representing the requested item.

        Raises:
            Exception: If any error occurs while getting the item from the database.
            NotFoundError: If the item does not exist in the specified collection.
        """
        base_url = str(kwargs["request"].base_url)
        item = await self.database.get_one_item(
            item_id=item_id, collection_id=collection_id
        )
        return self.item_serializer.db_to_stac(item, base_url)

    async def get_search(
        self,
        request: Request,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
        limit: Optional[int] = None,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        q: Optional[List[str]] = None,
        intersects: Optional[str] = None,
        filter_expr: Optional[str] = None,
        filter_lang: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Get search results from the database.

        Args:
            collections (Optional[List[str]]): List of collection IDs to search in.
            ids (Optional[List[str]]): List of item IDs to search for.
            bbox (Optional[BBox]): Bounding box to search in.
            datetime (Optional[str]): Filter items based on the datetime field.
            limit (Optional[int]): Maximum number of results to return.
            query (Optional[str]): Query string to filter the results.
            token (Optional[str]): Access token to use when searching the catalog.
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[str]): Sorting options for the results.
            q (Optional[List[str]]): Free text query to filter the results.
            intersects (Optional[str]): GeoJSON geometry to search in.
            kwargs: Additional parameters to be passed to the API.
        Returns:
            ItemCollection: Collection of `Item` objects representing the search results.

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": orjson.loads(query) if query else query,
            "q": q,
        }

        if datetime:
            base_args["datetime"] = format_datetime_range(date_str=datetime)

        if intersects:
            base_args["intersects"] = orjson.loads(unquote_plus(intersects))

        if sortby:
            parsed_sort = []
            for raw in sortby:
                if not isinstance(raw, str):
                    continue
                s = raw.strip()
                if not s:
                    continue
                direction = "desc" if s[0] == "-" else "asc"
                field = s[1:] if s and s[0] in "+-" else s
                parsed_sort.append({"field": field, "direction": direction})
            if parsed_sort:
                base_args["sortby"] = parsed_sort

        if filter_expr:
            base_args["filter_lang"] = "cql2-json"
            base_args["filter"] = orjson.loads(
                unquote_plus(filter_expr)
                if filter_lang == "cql2-json"
                else to_cql2(parse_cql2_text(filter_expr))
            )

        if fields:
            includes, excludes = set(), set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                else:
                    includes.add(field[1:] if field[0] in "+ " else field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid parameters provided: {e}"
            )
        resp = await self.post_search(search_request=search_request, request=request)

        return resp

    async def post_search(
        self, search_request: BaseSearchPostRequest, request: Request
    ) -> stac_types.ItemCollection:
        """
        Perform a POST search on the catalog.

        Args:
            search_request (BaseSearchPostRequest): Request object that includes the parameters for the search.
            kwargs: Keyword arguments passed to the function.

        Returns:
            ItemCollection: A collection of items matching the search criteria.

        Raises:
            HTTPException: If there is an error with the cql2_json filter.
        """
        global_max_limit = (
            int(os.getenv("STAC_GLOBAL_ITEM_MAX_LIMIT"))
            if os.getenv("STAC_GLOBAL_ITEM_MAX_LIMIT")
            else None
        )
        query_limit = request.query_params.get("limit")
        default_limit = int(os.getenv("STAC_DEFAULT_ITEM_LIMIT", 10))

        body_limit = None
        try:
            if request.method == "POST" and await request.body():
                body_data = await request.json()
                body_limit = body_data.get("limit")
        except Exception:
            pass

        if body_limit is not None:
            limit = int(body_limit)
        elif query_limit:
            limit = int(query_limit)
        else:
            limit = default_limit

        if global_max_limit:
            limit = min(limit, global_max_limit)

        search_request.limit = limit

        base_url = str(request.base_url)
        search = self.database.make_search()
        redis_enable = get_bool_env("REDIS_ENABLE", default=False)

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
            )

        if search_request.collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=search_request.collections
            )

        datetime_parsed = format_datetime_range(date_str=search_request.datetime)
        try:
            search, datetime_search = self.database.apply_datetime_filter(
                search=search, datetime=datetime_parsed
            )
        except (ValueError, TypeError) as e:
            # Handle invalid interval formats if return_date fails
            msg = f"Invalid interval format: {search_request.datetime}, error: {e}"
            logger.error(msg)
            raise HTTPException(status_code=400, detail=msg)

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if hasattr(search_request, "intersects") and getattr(
            search_request, "intersects"
        ):
            search = self.database.apply_intersects_filter(
                search=search, intersects=getattr(search_request, "intersects")
            )

        if hasattr(search_request, "query") and getattr(search_request, "query"):
            for field_name, expr in getattr(search_request, "query").items():
                field = "properties__" + field_name
                for op, value in expr.items():
                    # Convert enum to string
                    operator = op.value if isinstance(op, Enum) else op
                    search = self.database.apply_stacql_filter(
                        search=search, op=operator, field=field, value=value
                    )

        # Apply CQL2 filter (support both 'filter_expr' and canonical 'filter')
        cql2_filter = None
        if hasattr(search_request, "filter_expr"):
            cql2_filter = getattr(search_request, "filter_expr", None)
        if cql2_filter is None and hasattr(search_request, "filter"):
            cql2_filter = getattr(search_request, "filter", None)

        if cql2_filter is not None:
            try:
                search = await self.database.apply_cql2_filter(search, cql2_filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2 filter: {e}"
                )

        if hasattr(search_request, "q"):
            free_text_queries = getattr(search_request, "q", None)
            try:
                search = self.database.apply_free_text_filter(search, free_text_queries)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with free text query: {e}"
                )

        sort = None
        if hasattr(search_request, "sortby") and getattr(search_request, "sortby"):
            sort = self.database.populate_sort(getattr(search_request, "sortby"))

        if search_request.limit:
            limit = search_request.limit

        # Use token from the request if the model doesn't define it
        token_param = getattr(
            search_request, "token", None
        ) or request.query_params.get("token")
        items, maybe_count, next_token = await self.database.execute_search(
            search=search,
            limit=limit,
            token=token_param,
            sort=sort,
            collection_ids=getattr(search_request, "collections", None),
            datetime_search=datetime_search,
        )

        fields = getattr(search_request, "fields", None)
        include: Set[str] = fields.include if fields and fields.include else set()
        exclude: Set[str] = fields.exclude if fields and fields.exclude else set()

        items = [
            filter_fields(
                self.item_serializer.db_to_stac(item, base_url=base_url),
                include,
                exclude,
            )
            for item in items
        ]
        links = await PagingLinks(request=request, next=next_token).get_links()

        collection_links = []
        # Add "collection" and "parent" rels only for /collections/{collection_id}/items
        if search_request.collections and "/items" in str(request.url):
            for collection_id in search_request.collections:
                collection_links.extend(
                    [
                        {
                            "rel": "collection",
                            "type": "application/json",
                            "href": urljoin(base_url, f"collections/{collection_id}"),
                        },
                        {
                            "rel": "parent",
                            "type": "application/json",
                            "href": urljoin(base_url, f"collections/{collection_id}"),
                        },
                    ]
                )
        links.extend(collection_links)

        if redis_enable:
            from stac_fastapi.core.redis_utils import redis_pagination_links

            await redis_pagination_links(
                current_url=str(request.url),
                token=token_param,
                next_token=next_token,
                links=links,
            )

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numberReturned=len(items),
            numberMatched=maybe_count,
        )

    async def get_tilejson(self, collection_id: str, request: Request):
        collection = await self.get_collection(
            collection_id=collection_id, request=request
        )
        """
        Get tilejson metadata for a collection.

        Args:
            collection_id (str): The ID of the collection.
            request (Request): The HTTP request object.

        Returns:
            dict: The tilejson metadata for the collection.

        """

        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found")

        if (
            "extent" in collection
            and "spatial" in collection["extent"]
            and "bbox" in collection["extent"]["spatial"]
            and collection["extent"]["spatial"]["bbox"]
            and isinstance(collection["extent"]["spatial"]["bbox"][0], (list, tuple))
            and len(collection["extent"]["spatial"]["bbox"][0]) == 4
        ):
            bounds = collection["extent"]["spatial"]["bbox"][0]
        else:
            bounds = [-180.0, -90.0, 180.0, 90.0]

        geom_field = None
        if "geomField" in request.query_params:
            geom_field = request.query_params["geomField"]

        tile_url = f"{request.url.scheme}://{request.url.netloc}/collections/{collection_id}/tiles/{{z}}/{{x}}/{{y}}.mvt"
        if geom_field:
            tile_url += f"?geomField={geom_field}"

        tilejson = {
            "version": "1.0.0",
            "name": collection.get("title", collection_id),
            "description": collection.get("description", ""),
            "scheme": "xyz",
            "tiles": [tile_url],  # TODO expand to subdomains
            "minzoom": 0,
            "maxzoom": 22,
            "bounds": bounds,
            "attribution": "",  # TODO make config option
        }
        return tilejson

    VT_TTL = 60 * 10  # 10 mintues
    VT_MAX_SIZE = 100000
    VT_MAX_AGE = 60 * 60  # 1 hour
    tile_cache = TTLCache(maxsize=VT_MAX_SIZE, ttl=VT_TTL)

    def clear_tile_cache(self):
        """Clear the vector tile cache."""
        self.tile_cache.clear()
        logger.info("Tile cache cleared")

    async def get_tile(
        self, collection_id: str, z: int, x: int, y: int, request: Request
    ):
        """
        Get a vector tile for a specific collection and web mercator coordinates.

        Args:
            collection_id (str): The ID of the collection.
            z (int): The zoom level.
            x (int): The x coordinate.
            y (int): The y coordinate.
            request (Request): The HTTP request object.

        Returns:
            Response: A compressed (gzip) vector tile response.

        """

        geom_field = None
        if "geomField" in request.query_params:
            geom_field = request.query_params["geomField"]

        cache_key = (collection_id, z, x, y, geom_field)
        if "filter" in request.query_params:
            cache_key = cache_key + (request.query_params["filter"],)

        if cache_key in self.tile_cache:
            logger.info(f"cache hit {collection_id} z{z} x{x} y{y} {geom_field}")
            return Response(
                content=self.tile_cache[cache_key],
                media_type="application/vnd.mapbox-vector-tile",
                headers={
                    "Cache-Control": f"public, max-age={self.VT_MAX_AGE}",
                    "Content-Encoding": "gzip",
                    "X-Cache": "HIT",
                },
            )

        minx, miny, maxx, maxy = mercantile.bounds(x, y, z)
        bbox = [minx, miny, maxx, maxy]

        search = self.database.make_search()
        search = self.database.apply_collections_filter(
            search, collection_ids=[collection_id]
        )
        search = self.database.apply_bbox_filter(search, bbox=bbox)

        if "filter" in request.query_params:
            cql2_text = request.query_params["filter"]
            try:
                cql_ast = parse_cql2_text(cql2_text)
                cql2_json_str = to_cql2(cql_ast)
                cql2_json = (
                    orjson.loads(cql2_json_str)
                    if isinstance(cql2_json_str, str)
                    else cql2_json_str
                )
                search = await self.database.apply_cql2_filter(search, cql2_json)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2 filter: {e}"
                )

        now = time.time()

        # SANITY CHECK - are there any items at all in this tile
        items, _, _ = await self.database.execute_search(
            search=search,
            limit=1,
            sort=None,
            token=None,
            collection_ids=[collection_id],
            datetime_search=None,
        )
        items = list(items)

        if not items:
            self.tile_cache[cache_key] = b""  # cache of empty tile
            return Response(status_code=204)

        items, _, _ = await self.database.execute_search(
            search=search,
            limit=200000,
            sort=None,
            token=None,
            collection_ids=[collection_id],
            datetime_search=None,
        )
        items = list(items)

        logger.info(f"Fetched {len(items)} items in {time.time()-now:.2f} seconds")
        now = time.time()

        project = PROJECT_4326_TO_3857.transform

        # Calculate buffer as proportional to tile size in meters
        # minXWeb, minYWeb, maxXWeb, maxYWeb = tile_bbox_merc.bounds
        bounds_merc = mercantile.xy_bounds(x, y, z)
        minXWeb, minYWeb, maxXWeb, maxYWeb = (
            bounds_merc.left,
            bounds_merc.bottom,
            bounds_merc.right,
            bounds_merc.top,
        )
        tile_bbox_merc = box(minXWeb, minYWeb, maxXWeb, maxYWeb)
        # buffer_units = 5 too small
        buffer_units = 16
        tile_size_meters = (
            maxXWeb - minXWeb
        )  # width of the tile in meters (Web Mercator)
        buffer_meters = (buffer_units / 4096) * tile_size_meters

        # Expand the bounds by proportional buffer
        minx_buf = minXWeb - buffer_meters
        miny_buf = minYWeb - buffer_meters
        maxx_buf = maxXWeb + buffer_meters
        maxy_buf = maxYWeb + buffer_meters

        tile_bbox_merc_buffer = box(minx_buf, miny_buf, maxx_buf, maxy_buf)

        MVT_EXTENT = 4096

        def scale_coords(x, y):
            x_scaled = (
                (x - tile_bbox_merc.bounds[0])
                * MVT_EXTENT
                / (tile_bbox_merc.bounds[2] - tile_bbox_merc.bounds[0])
            )
            y_scaled = (
                (y - tile_bbox_merc.bounds[1])
                * MVT_EXTENT
                / (tile_bbox_merc.bounds[3] - tile_bbox_merc.bounds[1])
            )
            return (x_scaled, y_scaled)

        features = []
        for item in items:
            geometry = item["geometry"]
            if geom_field:
                if geom_field in item.get("properties", {}):
                    geometry = item["properties"][geom_field]
                else:
                    geometry = None
            if geometry is None:
                continue
            geom = shape(geometry)

            # simplification at low zooms - max_tolerance avoids curves turning into sharp angles
            if z < 12:
                tile_width_m = maxx - minx  # tile width in meters
                tolerance = (tile_width_m / 4096) * tile_width_m  # 0.001 tile units
                max_tolerance = 0.001
                min_tolerance = 10e-5
                tolerance = max(min(tolerance, max_tolerance), min_tolerance)
                logger.info(
                    f"Simplifying geometry for {item['id']} at z{z} with tolerance {tolerance}"
                )
                geom = geom.simplify(tolerance, preserve_topology=True)
            geom_merc = transform(project, geom)

            clipped_geom = geom_merc.intersection(tile_bbox_merc_buffer)
            if clipped_geom.is_empty:
                continue
            geom_tile = transform(scale_coords, clipped_geom)

            raw_props = item.get("properties", {})
            properties = {}
            for k, v in raw_props.items():
                if isinstance(v, dict):
                    continue
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                    continue
                if isinstance(v, list):
                    properties[k] = ",".join(str(x).strip() for x in v)
                else:
                    properties[k] = v

            properties["_id"] = item.get("id")
            features.append(
                {
                    "geometry": mapping(geom_tile),
                    "properties": properties,
                    # "id": item[
                    #     "id"
                    # ],  # TODO figure out how to create stable opensearch index unique integer ids
                }
            )

        mvt_bytes = mapbox_vector_tile.encode(
            [{"name": collection_id, "features": features}]
        )
        compressed = gzip.compress(mvt_bytes)
        self.tile_cache[cache_key] = compressed
        logger.info(
            f"Generated MVT for {collection_id} at z{z} x{x} y{y} in {time.time()-now:.2f} seconds"
        )
        return Response(
            content=compressed,
            media_type="application/vnd.mapbox-vector-tile",
            headers={
                "Cache-Control": f"public, max-age={self.VT_MAX_AGE}",
                "Content-Encoding": "gzip",
                "X-Cache": "MISS",
            },
        )

    async def get_stac_tile(self, z: int, x: int, y: int, request: Request):
        """
        Get a vector tile for all data_types in the STAC catalog and Web Mercator coordinates.
        Each data_type becomes a separate MVT layer.
        Uses properties.exposure_point as geometry when available.

        Args:
            z (int): Zoom level.
            x (int): X tile coordinate.
            y (int): Y tile coordinate.
            request (Request): HTTP request object.

        Returns:
            Response: Gzipped vector tile (MVT) for all data_types.
        """

        cache_key = (z, x, y)
        if "filter" in request.query_params:
            cache_key += (request.query_params["filter"],)

        if cache_key in self.tile_cache:
            logger.info(f"cache hit z{z} x{x} y{y}")
            return Response(
                content=self.tile_cache[cache_key],
                media_type="application/vnd.mapbox-vector-tile",
                headers={
                    "Cache-Control": f"public, max-age={self.VT_MAX_AGE}",
                    "Content-Encoding": "gzip",
                    "X-Cache": "HIT",
                },
            )

        # Tile bbox in EPSG:4326
        minx, miny, maxx, maxy = mercantile.bounds(x, y, z)
        bbox = [minx, miny, maxx, maxy]

        # Base search
        search = self.database.make_search()
        search = self.database.apply_bbox_filter(search, bbox=bbox)

        items, _, _ = await self.database.execute_search(
            search=search,
            limit=1,  # only fetch 1 item
            sort=None,
            token=None,
            collection_ids=None,
            datetime_search=None,
        )
        items = list(items)
        if not items:
            # Cache empty tile if desired
            self.tile_cache[cache_key] = b""  # optional
            return Response(status_code=204)

        # Optional CQL2 filter
        if "filter" in request.query_params:
            cql2_text = request.query_params["filter"]
            try:
                cql_ast = parse_cql2_text(cql2_text)
                cql2_json_str = to_cql2(cql_ast)
                cql2_json = (
                    orjson.loads(cql2_json_str)
                    if isinstance(cql2_json_str, str)
                    else cql2_json_str
                )
                search = await self.database.apply_cql2_filter(search, cql2_json)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2 filter: {e}"
                )

        items, _, _ = await self.database.execute_search(
            search=search,
            limit=200000,
            sort=None,
            token=None,
            collection_ids=None,  # search all collections internally
            datetime_search=None,
        )
        items = list(items)

        if not items:
            return Response(status_code=204)

        project = PROJECT_4326_TO_3857.transform
        bounds_merc = mercantile.xy_bounds(x, y, z)
        tile_bbox_merc = box(
            bounds_merc.left, bounds_merc.bottom, bounds_merc.right, bounds_merc.top
        )
        MVT_EXTENT = 4096

        def scale_coords(x, y):
            x_scaled = (
                (x - tile_bbox_merc.bounds[0])
                * MVT_EXTENT
                / (tile_bbox_merc.bounds[2] - tile_bbox_merc.bounds[0])
            )
            y_scaled = (
                (y - tile_bbox_merc.bounds[1])
                * MVT_EXTENT
                / (tile_bbox_merc.bounds[3] - tile_bbox_merc.bounds[1])
            )
            return x_scaled, y_scaled

        # Group features by data_type
        layers_dict = {}
        buffer_units = 16
        tile_size_meters = bounds_merc.right - bounds_merc.left
        buffer_meters = (buffer_units / 4096) * tile_size_meters
        tile_bbox_buffer = box(
            bounds_merc.left - buffer_meters,
            bounds_merc.bottom - buffer_meters,
            bounds_merc.right + buffer_meters,
            bounds_merc.top + buffer_meters,
        )

        for item in items:
            # Prefer properties.exposure_point if available
            geometry = item.get("properties", {}).get("exposure_point") or item.get(
                "geometry"
            )
            if not geometry:
                continue
            geom = shape(geometry)

            # Simplify geometry at low zooms
            if z < 12:
                tolerance = max(
                    min((tile_size_meters / 4096) * tile_size_meters, 0.001), 1e-5
                )
                geom = geom.simplify(tolerance, preserve_topology=True)

            # Project to Web Mercator and clip to tile bounds
            geom_merc = transform(project, geom)
            clipped_geom = geom_merc.intersection(tile_bbox_buffer)
            if clipped_geom.is_empty:
                continue
            geom_tile = transform(scale_coords, clipped_geom)

            # Prepare properties, skip nested dicts
            raw_props = item.get("properties", {})
            properties = {}
            for k, v in raw_props.items():
                if isinstance(v, dict):
                    continue
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                    continue
                if isinstance(v, list):
                    properties[k] = ",".join(str(x).strip() for x in v)
                else:
                    properties[k] = v
            properties["_id"] = item.get("id")

            # Include WKB if exposure_point is used
            if "exposure_point" in item.get("properties", {}):
                geom_root = shape(item["geometry"])
                properties["_wkb"] = geom_root.wkb.hex()

            # Group by data_type
            data_type = raw_props.get("data_type", "unknown")
            layers_dict.setdefault(data_type, []).append(
                {
                    "geometry": mapping(geom_tile),
                    "properties": properties,
                }
            )

        # Encode all layers into MVT
        mvt_layers = [
            {"name": dt, "features": feats} for dt, feats in layers_dict.items()
        ]
        if not mvt_layers:
            return Response(status_code=204)

        mvt_bytes = mapbox_vector_tile.encode(mvt_layers)
        compressed = gzip.compress(mvt_bytes)
        # self.tile_cache[cache_key] = compressed

        logger.info(f"Generated MVT z{z} x{x} y{y} with {len(items)} items")
        return Response(
            content=compressed,
            media_type="application/vnd.mapbox-vector-tile",
            headers={
                "Cache-Control": f"public, max-age={self.VT_MAX_AGE}",
                "Content-Encoding": "gzip",
                "X-Cache": "MISS",
            },
        )


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    @overrides
    async def create_item(
        self, collection_id: str, item: Union[Item, ItemCollection], **kwargs
    ) -> Union[stac_types.Item, str]:
        """
        Create an item or a feature collection of items in the specified collection.

        Args:
            collection_id (str): The ID of the collection to add the item(s) to.
            item (Union[Item, ItemCollection]): A single item or a collection of items to be added.
            **kwargs: Additional keyword arguments, such as `request` and `refresh`.

        Returns:
            Union[stac_types.Item, str]: The created item if a single item is added, or a summary string
            indicating the number of items successfully added and errors if a collection of items is added.

        Raises:
            NotFoundError: If the specified collection is not found in the database.
            ConflictError: If an item with the same ID already exists in the collection.
        """
        request = kwargs.get("request")
        base_url = str(request.base_url)

        # Convert Pydantic model to dict for uniform processing
        item_dict = item.model_dump(mode="json")

        # Handle FeatureCollection (bulk insert)
        if item_dict["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient(
                database=self.database, settings=self.settings
            )
            features = item_dict["features"]
            processed_items = [
                bulk_client.preprocess_item(
                    feature, base_url, BulkTransactionMethod.INSERT
                )
                for feature in features
            ]
            attempted = len(processed_items)

            success, errors = await self.database.bulk_async(
                collection_id=collection_id,
                processed_items=processed_items,
                **kwargs,
            )
            if errors:
                logger.error(
                    f"Bulk async operation encountered errors for collection {collection_id}: {errors} (attempted {attempted})"
                )
            else:
                logger.info(
                    f"Bulk async operation succeeded with {success} actions for collection {collection_id}."
                )
            return f"Successfully added {success} Items. {attempted - success} errors occurred."

        # Handle single item
        await self.database.create_item(
            item_dict, base_url=base_url, exist_ok=False, **kwargs
        )
        return ItemSerializer.db_to_stac(item_dict, base_url)

    @overrides
    async def update_item(
        self, collection_id: str, item_id: str, item: Item, **kwargs
    ) -> stac_types.Item:
        """Update an item in the collection.

        Args:
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be updated.
            item (stac_types.Item): The new item data.
            kwargs: Other optional arguments, including the request object.

        Returns:
            stac_types.Item: The updated item object.

        Raises:
            NotFound: If the specified collection is not found in the database.

        """
        item = item.model_dump(mode="json")
        base_url = str(kwargs["request"].base_url)

        now = datetime_type.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        item["properties"]["updated"] = now

        await self.database.create_item(
            item, base_url=base_url, exist_ok=True, **kwargs
        )

        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def patch_item(
        self,
        collection_id: str,
        item_id: str,
        patch: Union[PartialItem, List[PatchOperation]],
        **kwargs,
    ):
        """Patch an item in the collection.

        Args:
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be updated.
            patch (Union[PartialItem, List[PatchOperation]]): The item data or operations.
            kwargs: Other optional arguments, including the request object.

        Returns:
            stac_types.Item: The updated item object.

        Raises:
            NotFound: If the specified collection is not found in the database.

        """
        base_url = str(kwargs["request"].base_url)

        content_type = kwargs["request"].headers.get("content-type")

        item = None
        if isinstance(patch, list) and content_type == "application/json-patch+json":
            item = await self.database.json_patch_item(
                collection_id=collection_id,
                item_id=item_id,
                operations=patch,
                base_url=base_url,
            )

        if isinstance(patch, dict):
            patch = partialItemValidator.validate_python(patch)

        if isinstance(patch, PartialItem) and content_type in [
            "application/merge-patch+json",
            "application/json",
        ]:
            item = await self.database.merge_patch_item(
                collection_id=collection_id,
                item_id=item_id,
                item=patch,
                base_url=base_url,
            )

        if item:
            return ItemSerializer.db_to_stac(item, base_url=base_url)

        raise NotImplementedError(
            f"Content-Type: {content_type} and body: {patch} combination not implemented"
        )

    @overrides
    async def delete_item(self, item_id: str, collection_id: str, **kwargs) -> None:
        """Delete an item from a collection.

        Args:
            item_id (str): The identifier of the item to delete.
            collection_id (str): The identifier of the collection that contains the item.

        Returns:
            None: Returns 204 No Content on successful deletion
        """
        await self.database.delete_item(
            item_id=item_id, collection_id=collection_id, **kwargs
        )
        return None

    @overrides
    async def create_collection(
        self, collection: Collection, **kwargs
    ) -> stac_types.Collection:
        """Create a new collection in the database.

        Args:
            collection (stac_types.Collection): The collection to be created.
            kwargs: Additional keyword arguments.

        Returns:
            stac_types.Collection: The created collection object.

        Raises:
            ConflictError: If the collection already exists.
        """
        collection = collection.model_dump(mode="json")
        request = kwargs["request"]

        collection = self.database.collection_serializer.stac_to_db(collection, request)
        await self.database.create_collection(collection=collection, **kwargs)
        return CollectionSerializer.db_to_stac(
            collection,
            request,
            extensions=[type(ext).__name__ for ext in self.database.extensions],
        )

    @overrides
    async def update_collection(
        self, collection_id: str, collection: Collection, **kwargs
    ) -> stac_types.Collection:
        """
        Update a collection.

        This method updates an existing collection in the database by first finding
        the collection by the id given in the keyword argument `collection_id`.
        If no `collection_id` is given the id of the given collection object is used.
        If the object and keyword collection ids don't match the sub items
        collection id is updated else the items are left unchanged.
        The updated collection is then returned.

        Args:
            collection_id: id of the existing collection to be updated
            collection: A STAC collection that needs to be updated.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC collection that has been updated in the database.

        """
        collection = collection.model_dump(mode="json")

        request = kwargs["request"]

        collection = self.database.collection_serializer.stac_to_db(collection, request)
        await self.database.update_collection(
            collection_id=collection_id, collection=collection, **kwargs
        )

        return CollectionSerializer.db_to_stac(
            collection,
            request,
            extensions=[type(ext).__name__ for ext in self.database.extensions],
        )

    @overrides
    async def patch_collection(
        self,
        collection_id: str,
        patch: Union[PartialCollection, List[PatchOperation]],
        **kwargs,
    ):
        """Update a collection.

        Called with `PATCH /collections/{collection_id}`

        Args:
            collection_id: id of the collection.
            patch: either the partial collection or list of patch operations.

        Returns:
            The patched collection.
        """
        base_url = str(kwargs["request"].base_url)
        content_type = kwargs["request"].headers.get("content-type")

        collection = None
        if isinstance(patch, list) and content_type == "application/json-patch+json":
            collection = await self.database.json_patch_collection(
                collection_id=collection_id,
                operations=patch,
                base_url=base_url,
            )

        if isinstance(patch, dict):
            patch = partialCollectionValidator.validate_python(patch)

        if isinstance(patch, PartialCollection) and content_type in [
            "application/merge-patch+json",
            "application/json",
        ]:
            collection = await self.database.merge_patch_collection(
                collection_id=collection_id,
                collection=patch,
                base_url=base_url,
            )

        if collection:
            return CollectionSerializer.db_to_stac(
                collection,
                kwargs["request"],
                extensions=[type(ext).__name__ for ext in self.database.extensions],
            )

        raise NotImplementedError(
            f"Content-Type: {content_type} and body: {patch} combination not implemented"
        )

    @overrides
    async def delete_collection(self, collection_id: str, **kwargs) -> None:
        """
        Delete a collection.

        This method deletes an existing collection in the database.

        Args:
            collection_id (str): The identifier of the collection to delete

        Returns:
            None: Returns 204 No Content on successful deletion

        Raises:
            NotFoundError: If the collection doesn't exist
        """
        await self.database.delete_collection(collection_id=collection_id, **kwargs)
        return None


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """A client for posting bulk transactions.

    Attributes:
        session: An instance of `Session` to use for database connection.
        database: An instance of `DatabaseLogic` to perform database operations.
    """

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    def __attrs_post_init__(self):
        """Create es engine."""
        self.client = self.settings.create_client

    def preprocess_item(
        self, item: stac_types.Item, base_url, method: BulkTransactionMethod
    ) -> stac_types.Item:
        """Preprocess an item to match the data model.

        Args:
            item: The item to preprocess.
            base_url: The base URL of the request.
            method: The bulk transaction method.

        Returns:
            The preprocessed item.
        """
        exist_ok = method == BulkTransactionMethod.UPSERT
        return self.database.bulk_sync_prep_create_item(
            item=item, base_url=base_url, exist_ok=exist_ok
        )

    @overrides
    def bulk_item_insert(
        self, items: Items, chunk_size: Optional[int] = None, **kwargs
    ) -> str:
        """Perform a bulk insertion of items into the database using Elasticsearch.

        Args:
            items: The items to insert.
            chunk_size: The size of each chunk for bulk processing.
            **kwargs: Additional keyword arguments, such as `request` and `refresh`.

        Returns:
            A string indicating the number of items successfully added.
        """
        request = kwargs.get("request")

        if os.getenv("ENABLE_DATETIME_INDEX_FILTERING"):
            raise HTTPException(
                status_code=400,
                detail="The /collections/{collection_id}/bulk_items endpoint is invalid when ENABLE_DATETIME_INDEX_FILTERING is set to true. Try using the /collections/{collection_id}/items endpoint.",
            )

        if request:
            base_url = str(request.base_url)
        else:
            base_url = ""

        processed_items = []
        for item in items.items.values():
            try:
                validated = Item(**item) if not isinstance(item, Item) else item
                processed_items.append(
                    self.preprocess_item(
                        validated.model_dump(mode="json"), base_url, items.method
                    )
                )
            except ValidationError:
                # Immediately raise on the first invalid item (strict mode)
                raise

        collection_id = processed_items[0]["collection"]
        attempted = len(processed_items)
        success, errors = self.database.bulk_sync(
            collection_id,
            processed_items,
            **kwargs,
        )
        if errors:
            logger.error(f"Bulk sync operation encountered errors: {errors}")
        else:
            logger.info(f"Bulk sync operation succeeded with {success} actions.")

        return f"Successfully added/updated {success} Items. {attempted - success} errors occurred."
