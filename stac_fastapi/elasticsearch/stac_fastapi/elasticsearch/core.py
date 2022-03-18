"""Item crud client."""
import json
import logging
from datetime import datetime as datetime_type
from typing import List, Optional, Type, Union
from urllib.parse import urljoin

import attr
from fastapi import HTTPException
from overrides import overrides

# from geojson_pydantic.geometries import Polygon
from pydantic import ValidationError
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes

from stac_fastapi.elasticsearch import serializers
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import CoreDatabaseLogic
from stac_fastapi.elasticsearch.session import Session

# from stac_fastapi.elasticsearch.types.error_checks import ErrorChecks
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreCrudClient(BaseCoreClient):
    """Client for core endpoints defined by stac."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    item_serializer: Type[serializers.Serializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.Serializer] = attr.ib(
        default=serializers.CollectionSerializer
    )
    settings = ElasticsearchSettings()
    client = settings.create_client
    database = CoreDatabaseLogic()

    @overrides
    def all_collections(self, **kwargs) -> Collections:
        """Read all collections from the database."""
        base_url = str(kwargs["request"].base_url)
        serialized_collections = self.database.get_all_collections(base_url=base_url)

        links = [
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
        ]
        collection_list = Collections(
            collections=serialized_collections or [], links=links
        )
        return collection_list

    @overrides
    def get_collection(self, collection_id: str, **kwargs) -> Collection:
        """Get collection by id."""
        base_url = str(kwargs["request"].base_url)
        collection = self.database.get_one_collection(collection_id)
        return self.collection_serializer.db_to_stac(collection, base_url)

    @overrides
    def item_collection(
        self, collection_id: str, limit: int = 10, token: str = None, **kwargs
    ) -> ItemCollection:
        """Read an item collection from the database."""
        links = []
        base_url = str(kwargs["request"].base_url)

        serialized_children, count = self.database.get_item_collection(
            collection_id=collection_id, limit=limit, base_url=base_url
        )

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": count if count < limit else limit,
                "limit": limit,
                "matched": count,
            }

        return ItemCollection(
            type="FeatureCollection",
            features=serialized_children,
            links=links,
            context=context_obj,
        )

    @overrides
    def get_item(self, item_id: str, collection_id: str, **kwargs) -> Item:
        """Get item by item id, collection id."""
        base_url = str(kwargs["request"].base_url)
        item = self.database.get_one_item(item_id=item_id, collection_id=collection_id)
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
    def get_search(
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

        # if fields:
        #     includes = set()
        #     excludes = set()
        #     for field in fields:
        #         if field[0] == "-":
        #             excludes.add(field[1:])
        #         elif field[0] == "+":
        #             includes.add(field[1:])
        #         else:
        #             includes.add(field)
        #     base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = self.post_search(search_request, request=kwargs["request"])

        return resp

    def post_search(self, search_request, **kwargs) -> ItemCollection:
        """POST search catalog."""
        base_url = str(kwargs["request"].base_url)
        search = self.database.create_search_object()

        if search_request.query:
            if type(search_request.query) == str:
                search_request.query = json.loads(search_request.query)
            for (field_name, expr) in search_request.query.items():
                field = "properties__" + field_name
                for (op, value) in expr.items():
                    search = self.database.create_query_filter(
                        search=search, op=op, field=field, value=value
                    )

        if search_request.ids:
            search = self.database.search_ids(
                search=search, item_ids=search_request.ids
            )

        if search_request.collections:
            search = self.database.search_ids(search_request.collections)

        if search_request.datetime:
            datetime_search = self._return_date(search_request.datetime)
            search = self.database.search_datetime(datetime_search)

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.search_bbox(bbox=bbox)

        if search_request.intersects:
            self.database.search_intersects(search_request.intersects)

        if search_request.sortby:
            for sort in search_request.sortby:
                if sort.field == "datetime":
                    sort.field = "properties__datetime"
                field = sort.field + ".keyword"
                search = self.database.sort_field(
                    search=search, field=field, direction=sort.direction
                )

        count = self.database.search_count(search=search)

        response_features = self.database.execute_search(
            search=search, limit=search_request.limit, base_url=base_url
        )

        # if self.extension_is_enabled("FieldsExtension"):
        #     if search_request.query is not None:
        #         query_include: Set[str] = set(
        #             [
        #                 k if k in Settings.get().indexed_fields else f"properties.{k}"
        #                 for k in search_request.query.keys()
        #             ]
        #         )
        #         if not search_request.fields.include:
        #             search_request.fields.include = query_include
        #         else:
        #             search_request.fields.include.union(query_include)

        #     filter_kwargs = search_request.fields.filter_fields

        #     response_features = [
        #         json.loads(stac_pydantic.Item(**feat).json(**filter_kwargs))
        #         for feat in response_features
        #     ]

        if search_request.limit:
            limit = search_request.limit
            response_features = response_features[0:limit]
        else:
            limit = 10
            response_features = response_features[0:limit]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": count if count < limit else limit,
                "limit": limit,
                "matched": count,
            }

        links = []
        return ItemCollection(
            type="FeatureCollection",
            features=response_features,
            links=links,
            context=context_obj,
        )
