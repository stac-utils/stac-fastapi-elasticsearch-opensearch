"""Item crud client."""
import json
import logging
from datetime import datetime
from typing import List, Optional, Type, Union
from urllib.parse import urljoin

import attr
import elasticsearch
from elasticsearch_dsl import Q, Search
from fastapi import HTTPException

# from geojson_pydantic.geometries import Polygon
from pydantic import ValidationError
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes

from stac_fastapi.elasticsearch import serializers
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.session import Session

# from stac_fastapi.elasticsearch.types.error_checks import ErrorChecks
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.search import BaseSearchPostRequest
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

    @staticmethod
    def _lookup_id(id: str, table, session):
        """Lookup row by id."""
        pass

    def all_collections(self, **kwargs) -> Collections:
        """Read all collections from the database."""
        base_url = str(kwargs["request"].base_url)
        collections = self.client.search(
            index="stac_collections", doc_type="_doc", query={"match_all": {}}
        )
        serialized_collections = [
            self.collection_serializer.db_to_stac(
                collection["_source"], base_url=base_url
            )
            for collection in collections["hits"]["hits"]
        ]
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

    def get_collection(self, collection_id: str, **kwargs) -> Collection:
        """Get collection by id."""
        base_url = str(kwargs["request"].base_url)
        try:
            collection = self.client.get(index="stac_collections", id=collection_id)
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")

        return self.collection_serializer.db_to_stac(collection["_source"], base_url)

    def item_collection(
        self, collection_id: str, limit: int = 10, **kwargs
    ) -> ItemCollection:
        """Read an item collection from the database."""
        links = []
        base_url = str(kwargs["request"].base_url)

        search = Search(using=self.client, index="stac_items")

        collection_filter = Q(
            "bool", should=[Q("match_phrase", **{"collection": collection_id})]
        )
        search = search.query(collection_filter)

        count = search.count()
        # search = search.sort({"id.keyword" : {"order" : "asc"}})
        search = search.query()[0:limit]
        collection_children = search.execute().to_dict()

        serialized_children = [
            self.item_serializer.db_to_stac(item["_source"], base_url=base_url)
            for item in collection_children["hits"]["hits"]
        ]

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

    def get_item(self, item_id: str, collection_id: str, **kwargs) -> Item:
        """Get item by item id, collection id."""
        base_url = str(kwargs["request"].base_url)
        try:
            item = self.client.get(index="stac_items", id=item_id)
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError
        return self.item_serializer.db_to_stac(item["_source"], base_url)

    def _return_date(self, datetime):
        datetime = datetime.split("/")
        if len(datetime) == 1:
            datetime = datetime[0][0:19] + "Z"
            return {"eq": datetime}
        else:
            start_date = datetime[0]
            end_date = datetime[1]
            if ".." not in datetime:
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

    def get_search(
        self,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
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

    def bbox2poly(self, b0, b1, b2, b3):
        """Transform bbox to polygon."""
        poly = [[[b0, b1], [b2, b1], [b2, b3], [b0, b3], [b0, b1]]]
        return poly

    def post_search(
        self, search_request: BaseSearchPostRequest, **kwargs
    ) -> ItemCollection:
        """POST search catalog."""
        base_url = str(kwargs["request"].base_url)
        search = Search(using=self.client, index="stac_items")

        if search_request.query:
            if type(search_request.query) == str:
                search_request.query = json.loads(search_request.query)
            for (field_name, expr) in search_request.query.items():
                field = "properties__" + field_name
                for (op, value) in expr.items():
                    if op != "eq":
                        key_filter = {field: {f"{op}": value}}
                        search = search.query(Q("range", **key_filter))
                    else:
                        search = search.query("match_phrase", **{field: value})

        if search_request.ids:
            id_list = []
            for item_id in search_request.ids:
                id_list.append(Q("match_phrase", **{"id": item_id}))
            id_filter = Q("bool", should=id_list)
            search = search.query(id_filter)

        if search_request.collections:
            collection_list = []
            for collection_id in search_request.collections:
                collection_list.append(
                    Q("match_phrase", **{"collection": collection_id})
                )
            collection_filter = Q("bool", should=collection_list)
            search = search.query(collection_filter)

        if search_request.datetime:
            datetime_search = self._return_date(search_request.datetime)
            if "eq" in datetime_search:
                search = search.query(
                    "match_phrase", **{"properties__datetime": datetime_search["eq"]}
                )
            else:
                search = search.filter(
                    "range", properties__datetime={"lte": datetime_search["lte"]}
                )
                search = search.filter(
                    "range", properties__datetime={"gte": datetime_search["gte"]}
                )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]
            poly = self.bbox2poly(bbox[0], bbox[1], bbox[2], bbox[3])

            bbox_filter = Q(
                {
                    "geo_shape": {
                        "geometry": {
                            "shape": {"type": "polygon", "coordinates": poly},
                            "relation": "intersects",
                        }
                    }
                }
            )
            search = search.query(bbox_filter)

        if search_request.intersects:
            intersect_filter = Q(
                {
                    "geo_shape": {
                        "geometry": {
                            "shape": {
                                "type": search_request.intersects.type.lower(),
                                "coordinates": search_request.intersects.coordinates,
                            },
                            "relation": "intersects",
                        }
                    }
                }
            )
            search = search.query(intersect_filter)

        if search_request.sortby:
            for sort in search_request.sortby:
                if sort.field == "datetime":
                    sort.field = "properties__datetime"
                field = sort.field + ".keyword"
                search = search.sort({field: {"order": sort.direction}})

        count = search.count()
        # search = search.sort({"id.keyword" : {"order" : "asc"}})
        search = search.query()[0 : search_request.limit]
        response = search.execute().to_dict()

        if len(response["hits"]["hits"]) > 0:
            response_features = [
                self.item_serializer.db_to_stac(item["_source"], base_url=base_url)
                for item in response["hits"]["hits"]
            ]
        else:
            response_features = []

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
        limit = 10
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
