"""Item crud client."""
import json
import logging
from datetime import datetime as datetime_type
from datetime import timezone
from typing import Any, Dict, List, Optional, Set, Type, Union
from urllib.parse import urljoin

import attr
import stac_pydantic
from fastapi import HTTPException
from overrides import overrides
from pydantic import ValidationError
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes
from starlette.requests import Request

from stac_fastapi.elasticsearch import serializers
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import DatabaseLogic
from stac_fastapi.elasticsearch.models.links import PagingLinks
from stac_fastapi.elasticsearch.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.elasticsearch.session import Session
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.config import Settings
from stac_fastapi.types.core import (
    AsyncBaseCoreClient,
    AsyncBaseFiltersClient,
    AsyncBaseTransactionsClient,
)
from stac_fastapi.types.links import CollectionLinks
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreClient(AsyncBaseCoreClient):
    """Client for core endpoints defined by stac."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    item_serializer: Type[serializers.ItemSerializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.CollectionSerializer] = attr.ib(
        default=serializers.CollectionSerializer
    )
    database = DatabaseLogic()

    @overrides
    async def all_collections(self, **kwargs) -> Collections:
        """Read all collections from the database."""
        base_url = str(kwargs["request"].base_url)

        return Collections(
            collections=[
                self.collection_serializer.db_to_stac(c, base_url=base_url)
                for c in await self.database.get_all_collections()
            ],
            links=[
                {
                    "rel": Relations.root.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": Relations.parent.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": Relations.self.value,
                    "type": MimeTypes.json,
                    "href": urljoin(base_url, "collections"),
                },
            ],
        )

    @overrides
    async def get_collection(self, collection_id: str, **kwargs) -> Collection:
        """Get collection by id."""
        base_url = str(kwargs["request"].base_url)
        collection = await self.database.find_collection(collection_id=collection_id)
        return self.collection_serializer.db_to_stac(collection, base_url)

    @overrides
    async def item_collection(
        self,
        collection_id: str,
        bbox: Optional[List[NumType]] = None,
        datetime: Union[str, datetime_type, None] = None,
        limit: int = 10,
        token: str = None,
        **kwargs,
    ) -> ItemCollection:
        """Read an item collection from the database."""
        request: Request = kwargs["request"]
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
            token=token,  # type: ignore
            collection_ids=[collection_id],
        )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(items),
                "limit": limit,
            }
            if maybe_count is not None:
                context_obj["matched"] = maybe_count

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            context=context_obj,
        )

    @overrides
    async def get_item(self, item_id: str, collection_id: str, **kwargs) -> Item:
        """Get item by item id, collection id."""
        base_url = str(kwargs["request"].base_url)
        item = await self.database.get_one_item(
            item_id=item_id, collection_id=collection_id
        )
        return self.item_serializer.db_to_stac(item, base_url)

    @staticmethod
    def _return_date(interval_str):
        intervals = interval_str.split("/")
        if len(intervals) == 1:
            datetime = intervals[0][0:19] + "Z"
            return {"eq": datetime}
        else:
            start_date = intervals[0]
            end_date = intervals[1]
            if ".." not in intervals:
                start_date = start_date[0:19] + "Z"
                end_date = end_date[0:19] + "Z"
            elif start_date != "..":
                start_date = start_date[0:19] + "Z"
                end_date = "2200-12-01T12:31:12Z"
            elif end_date != "..":
                start_date = "1900-10-01T00:00:00Z"
                end_date = end_date[0:19] + "Z"
            else:
                start_date = "1900-10-01T00:00:00Z"
                end_date = "2200-12-01T12:31:12Z"

            return {"lte": end_date, "gte": start_date}

    @overrides
    async def get_search(
        self,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime_type]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        # filter: Optional[str] = None, # todo: requires fastapi > 2.3 unreleased
        # filter_lang: Optional[str] = None, # todo: requires fastapi > 2.3 unreleased
        **kwargs,
    ) -> ItemCollection:
        """GET search catalog."""
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": json.loads(query) if query else query,
        }

        if datetime:
            base_args["datetime"] = datetime

        if sortby:
            # https://github.com/radiantearth/stac-spec/tree/master/api-spec/extensions/sort#http-get-or-post-form
            sort_param = []
            for sort in sortby:
                sort_param.append(
                    {
                        "field": sort[1:],
                        "direction": "asc" if sort[0] == "+" else "desc",
                    }
                )
            base_args["sortby"] = sort_param

        # todo: requires fastapi > 2.3 unreleased
        # if filter:
        #     if filter_lang == "cql2-text":
        #         base_args["filter-lang"] = "cql2-json"
        #         base_args["filter"] = orjson.loads(to_cql2(parse_cql2_text(filter)))
        #         print(f'>>> {base_args["filter"]}')

        if fields:
            includes = set()
            excludes = set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = await self.post_search(search_request, request=kwargs["request"])

        return resp

    @overrides
    async def post_search(
        self, search_request: BaseSearchPostRequest, **kwargs
    ) -> ItemCollection:
        """POST search catalog."""
        request: Request = kwargs["request"]
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
            for (field_name, expr) in search_request.query.items():
                field = "properties__" + field_name
                for (op, value) in expr.items():
                    search = self.database.apply_stacql_filter(
                        search=search, op=op, field=field, value=value
                    )

        # only cql2_json is supported here
        if hasattr(search_request, "filter"):
            cql2_filter = getattr(search_request, "filter", None)
            try:
                search = self.database.apply_cql2_filter(search, cql2_filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
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
            token=search_request.token,  # type: ignore
            sort=sort,
            collection_ids=search_request.collections,
        )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        if self.extension_is_enabled("FieldsExtension"):
            if search_request.query is not None:
                query_include: Set[str] = set(
                    [
                        k if k in Settings.get().indexed_fields else f"properties.{k}"
                        for k in search_request.query.keys()
                    ]
                )
                if not search_request.fields.include:
                    search_request.fields.include = query_include
                else:
                    search_request.fields.include.union(query_include)

            filter_kwargs = search_request.fields.filter_fields

            items = [
                json.loads(stac_pydantic.Item(**feat).json(**filter_kwargs))
                for feat in items
            ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(items),
                "limit": limit,
            }
            if maybe_count is not None:
                context_obj["matched"] = maybe_count

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            context=context_obj,
        )


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    database = DatabaseLogic()

    @overrides
    async def create_item(
        self, collection_id: str, item: stac_types.Item, **kwargs
    ) -> stac_types.Item:
        """Create item."""
        base_url = str(kwargs["request"].base_url)

        # If a feature collection is posted
        if item["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient()
            processed_items = [
                bulk_client.preprocess_item(item, base_url) for item in item["features"]  # type: ignore
            ]

            await self.database.bulk_async(
                collection_id, processed_items, refresh=kwargs.get("refresh", False)
            )

            return None  # type: ignore
        else:
            item = await self.database.prep_create_item(item=item, base_url=base_url)
            await self.database.create_item(item, refresh=kwargs.get("refresh", False))
            return item

    @overrides
    async def update_item(
        self, collection_id: str, item_id: str, item: stac_types.Item, **kwargs
    ) -> stac_types.Item:
        """Update item."""
        base_url = str(kwargs["request"].base_url)

        now = datetime_type.now(timezone.utc).isoformat().replace("+00:00", "Z")
        item["properties"]["updated"] = str(now)

        await self.database.check_collection_exists(collection_id)
        # todo: index instead of delete and create
        await self.delete_item(item_id=item_id, collection_id=collection_id)
        await self.create_item(collection_id=collection_id, item=item, **kwargs)

        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def delete_item(
        self, item_id: str, collection_id: str, **kwargs
    ) -> stac_types.Item:
        """Delete item."""
        await self.database.delete_item(item_id=item_id, collection_id=collection_id)
        return None  # type: ignore

    @overrides
    async def create_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Create collection."""
        base_url = str(kwargs["request"].base_url)
        collection_links = CollectionLinks(
            collection_id=collection["id"], base_url=base_url
        ).create_links()
        collection["links"] = collection_links
        await self.database.create_collection(collection=collection)

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def update_collection(
        self, collection: stac_types.Collection, **kwargs
    ) -> stac_types.Collection:
        """Update collection."""
        base_url = str(kwargs["request"].base_url)

        await self.database.find_collection(collection_id=collection["id"])
        await self.delete_collection(collection["id"])
        await self.create_collection(collection, **kwargs)

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def delete_collection(
        self, collection_id: str, **kwargs
    ) -> stac_types.Collection:
        """Delete collection."""
        await self.database.delete_collection(collection_id=collection_id)
        return None  # type: ignore


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """Postgres bulk transactions."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    database = DatabaseLogic()

    def __attrs_post_init__(self):
        """Create es engine."""
        settings = ElasticsearchSettings()
        self.client = settings.create_client

    def preprocess_item(self, item: stac_types.Item, base_url) -> stac_types.Item:
        """Preprocess items to match data model."""
        return self.database.sync_prep_create_item(item=item, base_url=base_url)

    @overrides
    def bulk_item_insert(
        self, items: Items, chunk_size: Optional[int] = None, **kwargs
    ) -> str:
        """Bulk item insertion using es."""
        request = kwargs.get("request")
        if request:
            base_url = str(request.base_url)
        else:
            base_url = ""

        processed_items = [
            self.preprocess_item(item, base_url) for item in items.items.values()
        ]

        # not a great way to get the collection_id-- should be part of the method signature
        collection_id = processed_items[0]["collection"]

        self.database.bulk_sync(
            collection_id, processed_items, refresh=kwargs.get("refresh", False)
        )

        return f"Successfully added {len(processed_items)} Items."


@attr.s
class EsAsyncBaseFiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    # todo: use the ES _mapping endpoint to dynamically find what fields exist
    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables
        """
        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://stac-api.example.com/queryables",
            "type": "object",
            "title": "Queryables for Example STAC API",
            "description": "Queryable names for the example STAC API Item Search filter.",
            "properties": {
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
                    "description": "Cloud Shadow Percentage",
                    "title": "Cloud Shadow Percentage",
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                },
                "nodata_pixel_percentage": {
                    "description": "No Data Pixel Percentage",
                    "title": "No Data Pixel Percentage",
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                },
            },
            "additionalProperties": True,
        }
