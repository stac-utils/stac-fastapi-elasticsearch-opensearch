"""Database logic."""
import logging
from typing import List, Optional, Tuple, Type, Union

import attr
import elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Q, Search
from geojson_pydantic.geometries import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

from stac_fastapi.elasticsearch import serializers
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.types.errors import ConflictError, ForeignKeyError, NotFoundError
from stac_fastapi.types.stac import Collection, Item

logger = logging.getLogger(__name__)

NumType = Union[float, int]

ITEMS_INDEX = "stac_items"
COLLECTIONS_INDEX = "stac_collections"


def mk_item_id(item_id: str, collection_id: str):
    """Make the Elasticsearch document _id value from the Item id and collection."""
    return f"{item_id}|{collection_id}"


@attr.s
class DatabaseLogic:
    """Database logic."""

    settings = ElasticsearchSettings()
    client = settings.create_client
    item_serializer: Type[serializers.ItemSerializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.CollectionSerializer] = attr.ib(
        default=serializers.CollectionSerializer
    )

    @staticmethod
    def bbox2poly(b0, b1, b2, b3):
        """Transform bbox to polygon."""
        poly = [[[b0, b1], [b2, b1], [b2, b3], [b0, b3], [b0, b1]]]
        return poly

    """CORE LOGIC"""

    def get_all_collections(self, base_url: str) -> List[Collection]:
        """Database logic to retrieve a list of all collections."""
        try:
            collections = self.client.search(
                index=COLLECTIONS_INDEX, query={"match_all": {}}
            )
        except elasticsearch.exceptions.NotFoundError:
            return []

        serialized_collections = [
            self.collection_serializer.db_to_stac(
                collection["_source"], base_url=base_url
            )
            for collection in collections["hits"]["hits"]
        ]

        return serialized_collections

    def get_item_collection(
        self, collection_id: str, limit: int, base_url: str
    ) -> Tuple[List[Item], Optional[int]]:
        """Database logic to retrieve an ItemCollection and a count of items contained."""
        search = self.create_search_object()
        search = self.search_collections(search, [collection_id])

        collection_filter = Q(
            "bool", should=[Q("match_phrase", **{"collection": collection_id})]
        )
        search = search.query(collection_filter)

        count = self.search_count(search)

        # search = search.sort({"id.keyword" : {"order" : "asc"}})
        search = search.query()[0:limit]

        body = search.to_dict()
        collection_children = self.client.search(
            index=ITEMS_INDEX, query=body["query"], sort=body.get("sort")
        )

        serialized_children = [
            self.item_serializer.db_to_stac(item["_source"], base_url=base_url)
            for item in collection_children["hits"]["hits"]
        ]

        return serialized_children, count

    def get_one_item(self, collection_id: str, item_id: str) -> Item:
        """Database logic to retrieve a single item."""
        try:
            item = self.client.get(
                index=ITEMS_INDEX, id=mk_item_id(item_id, collection_id)
            )
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist in Collection {collection_id}"
            )
        return item["_source"]

    @staticmethod
    def create_search_object():
        """Database logic to create a nosql Search instance."""
        return Search().sort(
            {"properties.datetime": {"order": "desc"}},
            {"id": {"order": "desc"}},
            {"collection": {"order": "desc"}},
        )

    @staticmethod
    def create_query_filter(search: Search, op: str, field: str, value: float):
        """Database logic to perform query for search endpoint."""
        if op != "eq":
            key_filter = {field: {f"{op}": value}}
            search = search.query(Q("range", **key_filter))
        else:
            search = search.query("match_phrase", **{field: value})

        return search

    @staticmethod
    def search_ids(search: Search, item_ids: List):
        """Database logic to search a list of STAC item ids."""
        id_list = []
        for item_id in item_ids:
            id_list.append(Q("match_phrase", **{"id": item_id}))
        id_filter = Q("bool", should=id_list)
        search = search.query(id_filter)

        return search

    @staticmethod
    def search_collections(search: Search, collection_ids: List):
        """Database logic to search a list of STAC collection ids."""
        collections_query = [Q("term", **{"collection": cid}) for cid in collection_ids]
        return search.query(Q("bool", should=collections_query))

    @staticmethod
    def search_datetime(search: Search, datetime_search):
        """Database logic to search datetime field."""
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
        return search

    @staticmethod
    def search_bbox(search: Search, bbox: List):
        """Database logic to search on bounding box."""
        poly = DatabaseLogic.bbox2poly(bbox[0], bbox[1], bbox[2], bbox[3])
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
        return search

    @staticmethod
    def search_intersects(
        search: Search,
        intersects: Union[
            Point,
            MultiPoint,
            LineString,
            MultiLineString,
            Polygon,
            MultiPolygon,
            GeometryCollection,
        ],
    ):
        """Database logic to search a geojson object."""
        intersect_filter = Q(
            {
                "geo_shape": {
                    "geometry": {
                        "shape": {
                            "type": intersects.type.lower(),
                            "coordinates": intersects.coordinates,
                        },
                        "relation": "intersects",
                    }
                }
            }
        )
        search = search.query(intersect_filter)
        return search

    @staticmethod
    def sort_field(search: Search, field, direction):
        """Database logic to sort search instance."""
        return search.sort({field: {"order": direction}})

    def search_count(self, search: Search) -> int:
        """Database logic to count search results."""
        try:
            return self.client.count(
                index=ITEMS_INDEX, body=search.to_dict(count=True)
            ).get("count")
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError("No items exist")

    def execute_search(self, search, limit: int, base_url: str) -> List:
        """Database logic to execute search with limit."""
        search = search.query()[0:limit]
        body = search.to_dict()
        response = self.client.search(
            index=ITEMS_INDEX, query=body["query"], sort=body.get("sort")
        )

        if len(response["hits"]["hits"]) > 0:
            response_features = [
                self.item_serializer.db_to_stac(item["_source"], base_url=base_url)
                for item in response["hits"]["hits"]
            ]
        else:
            response_features = []

        return response_features

    """ TRANSACTION LOGIC """

    def check_collection_exists(self, collection_id: str):
        """Database logic to check if a collection exists."""
        if not self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise ForeignKeyError(f"Collection {collection_id} does not exist")

    def prep_create_item(self, item: Item, base_url: str) -> Item:
        """Database logic for prepping an item for insertion."""
        self.check_collection_exists(collection_id=item["collection"])

        if self.client.exists(
            index=ITEMS_INDEX, id=mk_item_id(item["id"], item["collection"])
        ):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    def create_item(self, item: Item, refresh: bool = False):
        """Database logic for creating one item."""
        # todo: check if collection exists, but cache
        es_resp = self.client.index(
            index=ITEMS_INDEX,
            id=mk_item_id(item["id"], item["collection"]),
            document=item,
            refresh=refresh,
        )

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

    def delete_item(self, item_id: str, collection_id: str, refresh: bool = False):
        """Database logic for deleting one item."""
        try:
            self.client.delete(
                index=ITEMS_INDEX,
                id=mk_item_id(item_id, collection_id),
                refresh=refresh,
            )
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )

    def create_collection(self, collection: Collection, refresh: bool = False):
        """Database logic for creating one collection."""
        if self.client.exists(index=COLLECTIONS_INDEX, id=collection["id"]):
            raise ConflictError(f"Collection {collection['id']} already exists")

        self.client.index(
            index=COLLECTIONS_INDEX,
            id=collection["id"],
            document=collection,
            refresh=refresh,
        )

    def find_collection(self, collection_id: str) -> Collection:
        """Database logic to find and return a collection."""
        try:
            collection = self.client.get(index=COLLECTIONS_INDEX, id=collection_id)
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")

        return collection["_source"]

    def delete_collection(self, collection_id: str, refresh: bool = False):
        """Database logic for deleting one collection."""
        _ = self.find_collection(collection_id=collection_id)
        self.client.delete(index=COLLECTIONS_INDEX, id=collection_id, refresh=refresh)

    def bulk_sync(self, processed_items, refresh: bool = False):
        """Database logic for bulk item insertion."""
        actions = [
            {
                "_index": ITEMS_INDEX,
                "_id": mk_item_id(item["id"], item["collection"]),
                "_source": item,
            }
            for item in processed_items
        ]
        helpers.bulk(self.client, actions, refresh=refresh)

    # DANGER
    def delete_items(self) -> None:
        """Danger. this is only for tests."""
        self.client.delete_by_query(
            index=ITEMS_INDEX,
            body={"query": {"match_all": {}}},
            wait_for_completion=True,
        )

    # DANGER
    def delete_collections(self) -> None:
        """Danger. this is only for tests."""
        self.client.delete_by_query(
            index=COLLECTIONS_INDEX,
            body={"query": {"match_all": {}}},
            wait_for_completion=True,
        )
