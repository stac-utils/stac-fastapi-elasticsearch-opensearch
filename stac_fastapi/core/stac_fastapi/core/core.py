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
from fastapi import HTTPException, Request
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
from stac_fastapi.core.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.core.session import Session
from stac_fastapi.core.utilities import filter_fields
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

logger = logging.getLogger(__name__)

partialItemValidator = TypeAdapter(PartialItem)
partialCollectionValidator = TypeAdapter(PartialCollection)


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
    post_request_model = attr.ib(default=BaseSearchPostRequest)
    stac_version: str = attr.ib(default=STAC_VERSION)
    landing_page_id: str = attr.ib(default="stac-fastapi")
    title: str = attr.ib(default="stac-fastapi")
    description: str = attr.ib(default="stac-fastapi")

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

        collections = await self.all_collections(request=kwargs["request"])
        for collection in collections["collections"]:
            landing_page["links"].append(
                {
                    "rel": Relations.child.value,
                    "type": MimeTypes.json.value,
                    "title": collection.get("title") or collection.get("id"),
                    "href": urljoin(base_url, f"collections/{collection['id']}"),
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
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        q: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ) -> stac_types.Collections:
        """Read all collections from the database.

        Args:
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[str]): Sorting options for the results.
            q (Optional[List[str]]): Free text search terms.
            **kwargs: Keyword arguments from the request.

        Returns:
            A Collections object containing all the collections in the database and links to various resources.
        """
        request = kwargs["request"]
        base_url = str(request.base_url)
        limit = int(request.query_params.get("limit", os.getenv("STAC_ITEM_LIMIT", 10)))
        token = request.query_params.get("token")

        # Process fields parameter for filtering collection properties
        includes, excludes = set(), set()
        if fields and self.extension_is_enabled("FieldsExtension"):
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                else:
                    includes.add(field[1:] if field[0] in "+ " else field)

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

        collections, next_token = await self.database.get_all_collections(
            token=token, limit=limit, request=request, sort=sort, q=q_list
        )

        # Apply field filtering if fields parameter was provided
        if fields and self.extension_is_enabled("FieldsExtension"):
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

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return stac_types.Collections(collections=filtered_collections, links=links)

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
            limit (Optional[int]): Optional page size. Defaults to env ``STAC_ITEM_LIMIT`` when unset.
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
        limit = int(request.query_params.get("limit", os.getenv("STAC_ITEM_LIMIT", 10)))

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
        base_url = str(request.base_url)

        search = self.database.make_search()

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
            )

        if search_request.collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=search_request.collections
            )

        try:
            search, datetime_search = self.database.apply_datetime_filter(
                search=search, datetime=search_request.datetime
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
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
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

        limit = 10
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

        fields = (
            getattr(search_request, "fields", None)
            if self.extension_is_enabled("FieldsExtension")
            else None
        )
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

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numberReturned=len(items),
            numberMatched=maybe_count,
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
