"""Core client."""

import logging
from collections import deque
from datetime import datetime as datetime_type
from datetime import timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Set, Type, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
from fastapi import HTTPException, Request
from overrides import overrides
from pydantic import ValidationError
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic import Collection, Item, ItemCollection
from stac_pydantic.links import Relations
from stac_pydantic.shared import BBox, MimeTypes
from stac_pydantic.version import STAC_VERSION

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.core.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.core.session import Session
from stac_fastapi.core.utilities import filter_fields
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    BulkTransactionMethod,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.conformance import BASE_CONFORMANCE_CLASSES
from stac_fastapi.types.core import AsyncBaseCoreClient, AsyncBaseTransactionsClient
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.requests import get_base_url
from stac_fastapi.types.rfc3339 import DateTimeType, rfc3339_str_to_datetime
from stac_fastapi.types.search import BaseSearchPostRequest

logger = logging.getLogger(__name__)


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

    async def all_collections(self, **kwargs) -> stac_types.Collections:
        """Read all collections from the database.

        Args:
            **kwargs: Keyword arguments from the request.

        Returns:
            A Collections object containing all the collections in the database and links to various resources.
        """
        request = kwargs["request"]
        base_url = str(request.base_url)
        limit = int(request.query_params.get("limit", 10))
        token = request.query_params.get("token")

        collections, next_token = await self.database.get_all_collections(
            token=token, limit=limit, request=request
        )

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

        return stac_types.Collections(collections=collections, links=links)

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
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
        limit: Optional[int] = 10,
        token: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Read items from a specific collection in the database.

        Args:
            collection_id (str): The identifier of the collection to read items from.
            bbox (Optional[BBox]): The bounding box to filter items by.
            datetime (Optional[str]): The datetime range to filter items by.
            limit (int): The maximum number of items to return. The default value is 10.
            token (str): A token used for pagination.
            request (Request): The incoming request.

        Returns:
            ItemCollection: An `ItemCollection` object containing the items from the specified collection that meet
                the filter criteria and links to various resources.

        Raises:
            HTTPException: If the specified collection is not found.
            Exception: If any error occurs while reading the items from the database.
        """
        request: Request = kwargs["request"]
        token = request.query_params.get("token")

        base_url = str(request.base_url)

        collection = await self.get_collection(
            collection_id=collection_id, request=request
        )
        collection_id = collection.get("id")
        if collection_id is None:
            raise HTTPException(status_code=404, detail="Collection not found")

        search = self.database.make_search()
        search = self.database.apply_collections_filter(
            search=search, collection_ids=[collection_id]
        )

        if datetime:
            datetime_search = self._return_date(datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if bbox:
            bbox = [float(x) for x in bbox]
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        items, maybe_count, next_token = await self.database.execute_search(
            search=search,
            limit=limit,
            sort=None,
            token=token,
            collection_ids=[collection_id],
        )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        links = await PagingLinks(request=request, next=next_token).get_links()

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=maybe_count,
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

    @staticmethod
    def _return_date(
        interval: Optional[Union[DateTimeType, str]]
    ) -> Dict[str, Optional[str]]:
        """
        Convert a date interval.

        (which may be a datetime, a tuple of one or two datetimes a string
        representing a datetime or range, or None) into a dictionary for filtering
        search results with Elasticsearch.

        This function ensures the output dictionary contains 'gte' and 'lte' keys,
        even if they are set to None, to prevent KeyError in the consuming logic.

        Args:
            interval (Optional[Union[DateTimeType, str]]): The date interval, which might be a single datetime,
                a tuple with one or two datetimes, a string, or None.

        Returns:
            dict: A dictionary representing the date interval for use in filtering search results,
                always containing 'gte' and 'lte' keys.
        """
        result: Dict[str, Optional[str]] = {"gte": None, "lte": None}

        if interval is None:
            return result

        if isinstance(interval, str):
            if "/" in interval:
                parts = interval.split("/")
                result["gte"] = parts[0] if parts[0] != ".." else None
                result["lte"] = (
                    parts[1] if len(parts) > 1 and parts[1] != ".." else None
                )
            else:
                converted_time = interval if interval != ".." else None
                result["gte"] = result["lte"] = converted_time
            return result

        if isinstance(interval, datetime_type):
            datetime_iso = interval.isoformat()
            result["gte"] = result["lte"] = datetime_iso
        elif isinstance(interval, tuple):
            start, end = interval
            # Ensure datetimes are converted to UTC and formatted with 'Z'
            if start:
                result["gte"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            if end:
                result["lte"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        return result

    def _format_datetime_range(self, date_str: str) -> str:
        """
        Convert a datetime range string into a normalized UTC string for API requests using rfc3339_str_to_datetime.

        Args:
            date_str (str): A string containing two datetime values separated by a '/'.

        Returns:
            str: A string formatted as 'YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ', with '..' used if any element is None.
        """

        def normalize(dt):
            dt = dt.strip()
            if not dt or dt == "..":
                return ".."
            dt_obj = rfc3339_str_to_datetime(dt)
            dt_utc = dt_obj.astimezone(timezone.utc)
            return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        if not isinstance(date_str, str):
            return "../.."
        if "/" not in date_str:
            return f"{normalize(date_str)}/{normalize(date_str)}"
        try:
            start, end = date_str.split("/", 1)
        except Exception:
            return "../.."
        return f"{normalize(start)}/{normalize(end)}"

    async def get_search(
        self,
        request: Request,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[str] = None,
        limit: Optional[int] = 10,
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
            base_args["datetime"] = self._format_datetime_range(date_str=datetime)

        if intersects:
            base_args["intersects"] = orjson.loads(unquote_plus(intersects))

        if sortby:
            base_args["sortby"] = [
                {"field": sort[1:], "direction": "desc" if sort[0] == "-" else "asc"}
                for sort in sortby
            ]

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

        if search_request.datetime:
            datetime_search = self._return_date(search_request.datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if search_request.intersects:
            search = self.database.apply_intersects_filter(
                search=search, intersects=search_request.intersects
            )

        if search_request.query:
            for field_name, expr in search_request.query.items():
                field = "properties__" + field_name
                for op, value in expr.items():
                    # Convert enum to string
                    operator = op.value if isinstance(op, Enum) else op
                    search = self.database.apply_stacql_filter(
                        search=search, op=operator, field=field, value=value
                    )

        # only cql2_json is supported here
        if hasattr(search_request, "filter_expr"):
            cql2_filter = getattr(search_request, "filter_expr", None)
            try:
                search = self.database.apply_cql2_filter(search, cql2_filter)
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
        if search_request.sortby:
            sort = self.database.populate_sort(search_request.sortby)

        limit = 10
        if search_request.limit:
            limit = search_request.limit

        items, maybe_count, next_token = await self.database.execute_search(
            search=search,
            limit=limit,
            token=search_request.token,
            sort=sort,
            collection_ids=search_request.collections,
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
            numReturned=len(items),
            numMatched=maybe_count,
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
                collection_id,
                processed_items,
                refresh=kwargs.get("refresh", False),
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
            item_dict,
            refresh=kwargs.get("refresh", False),
            base_url=base_url,
            exist_ok=False,
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
            item, refresh=kwargs.get("refresh", False), base_url=base_url, exist_ok=True
        )

        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def delete_item(self, item_id: str, collection_id: str, **kwargs) -> None:
        """Delete an item from a collection.

        Args:
            item_id (str): The identifier of the item to delete.
            collection_id (str): The identifier of the collection that contains the item.

        Returns:
            None: Returns 204 No Content on successful deletion
        """
        await self.database.delete_item(item_id=item_id, collection_id=collection_id)
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
        await self.database.create_collection(collection=collection)
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
            collection_id=collection_id, collection=collection
        )

        return CollectionSerializer.db_to_stac(
            collection,
            request,
            extensions=[type(ext).__name__ for ext in self.database.extensions],
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
        await self.database.delete_collection(collection_id=collection_id)
        return None


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """A client for posting bulk transactions to a Postgres database.

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
            refresh=kwargs.get("refresh", False),
        )
        if errors:
            logger.error(f"Bulk sync operation encountered errors: {errors}")
        else:
            logger.info(f"Bulk sync operation succeeded with {success} actions.")

        return f"Successfully added/updated {success} Items. {attempted - success} errors occurred."


_DEFAULT_QUERYABLES: Dict[str, Dict[str, Any]] = {
    "id": {
        "description": "ID",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id",
    },
    "collection": {
        "description": "Collection",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/then/properties/collection",
    },
    "geometry": {
        "description": "Geometry",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/1/oneOf/0/properties/geometry",
    },
    "datetime": {
        "description": "Acquisition Timestamp",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/datetime",
    },
    "created": {
        "description": "Creation Timestamp",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/created",
    },
    "updated": {
        "description": "Creation Timestamp",
        "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/updated",
    },
    "cloud_cover": {
        "description": "Cloud Cover",
        "$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fields/properties/eo:cloud_cover",
    },
    "cloud_shadow_percentage": {
        "title": "Cloud Shadow Percentage",
        "description": "Cloud Shadow Percentage",
        "type": "number",
        "minimum": 0,
        "maximum": 100,
    },
    "nodata_pixel_percentage": {
        "title": "No Data Pixel Percentage",
        "description": "No Data Pixel Percentage",
        "type": "number",
        "minimum": 0,
        "maximum": 100,
    },
}

_ES_MAPPING_TYPE_TO_JSON: Dict[
    str, Literal["string", "number", "boolean", "object", "array", "null"]
] = {
    "date": "string",
    "date_nanos": "string",
    "keyword": "string",
    "match_only_text": "string",
    "text": "string",
    "wildcard": "string",
    "byte": "number",
    "double": "number",
    "float": "number",
    "half_float": "number",
    "long": "number",
    "scaled_float": "number",
    "short": "number",
    "token_count": "number",
    "unsigned_long": "number",
    "geo_point": "object",
    "geo_shape": "object",
    "nested": "array",
}


@attr.s
class EsAsyncBaseFiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    database: BaseDatabaseLogic = attr.ib()

    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables

        Args:
            collection_id (str, optional): The id of the collection to get queryables for.
            **kwargs: additional keyword arguments

        Returns:
            Dict[str, Any]: A dictionary containing the queryables for the given collection.
        """
        queryables: Dict[str, Any] = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://stac-api.example.com/queryables",
            "type": "object",
            "title": "Queryables for STAC API",
            "description": "Queryable names for the STAC API Item Search filter.",
            "properties": _DEFAULT_QUERYABLES,
            "additionalProperties": True,
        }
        if not collection_id:
            return queryables

        properties: Dict[str, Any] = queryables["properties"]
        queryables.update(
            {
                "properties": properties,
                "additionalProperties": False,
            }
        )

        mapping_data = await self.database.get_items_mapping(collection_id)
        mapping_properties = next(iter(mapping_data.values()))["mappings"]["properties"]
        stack = deque(mapping_properties.items())

        while stack:
            field_name, field_def = stack.popleft()

            # Iterate over nested fields
            field_properties = field_def.get("properties")
            if field_properties:
                # Fields in Item Properties should be exposed with their un-prefixed names,
                # and not require expressions to prefix them with properties,
                # e.g., eo:cloud_cover instead of properties.eo:cloud_cover.
                if field_name == "properties":
                    stack.extend(field_properties.items())
                else:
                    stack.extend(
                        (f"{field_name}.{k}", v) for k, v in field_properties.items()
                    )

            # Skip non-indexed or disabled fields
            field_type = field_def.get("type")
            if not field_type or not field_def.get("enabled", True):
                continue

            # Generate field properties
            field_result = _DEFAULT_QUERYABLES.get(field_name, {})
            properties[field_name] = field_result

            field_name_human = field_name.replace("_", " ").title()
            field_result.setdefault("title", field_name_human)

            field_type_json = _ES_MAPPING_TYPE_TO_JSON.get(field_type, field_type)
            field_result.setdefault("type", field_type_json)

            if field_type in {"date", "date_nanos"}:
                field_result.setdefault("format", "date-time")

        return queryables
