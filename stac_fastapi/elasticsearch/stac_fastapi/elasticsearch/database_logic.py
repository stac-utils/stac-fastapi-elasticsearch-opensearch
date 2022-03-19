"""Database logic."""
import logging
from typing import List, Type, Union

import attr
import elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Q, Search

from stac_fastapi.elasticsearch import serializers
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.errors import ConflictError, ForeignKeyError, NotFoundError
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection

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
    item_serializer: Type[serializers.Serializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.Serializer] = attr.ib(
        default=serializers.CollectionSerializer
    )

    @staticmethod
    def bbox2poly(b0, b1, b2, b3):
        """Transform bbox to polygon."""
        poly = [[[b0, b1], [b2, b1], [b2, b3], [b0, b3], [b0, b1]]]
        return poly

    # Core Logic

    def get_all_collections(self, base_url: str) -> Collections:
        """Database logic to retrieve a list of all collections."""
        try:
            collections = self.client.search(
                index=COLLECTIONS_INDEX, query={"match_all": {}}
            )
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError("No collections exist")

        serialized_collections = [
            self.collection_serializer.db_to_stac(
                collection["_source"], base_url=base_url
            )
            for collection in collections["hits"]["hits"]
        ]

        return serialized_collections

    def get_one_collection(self, collection_id: str) -> Collection:
        """Database logic to retrieve a single collection."""
        collection = self.find_collection(collection_id=collection_id)
        return collection["_source"]

    def get_item_collection(
        self, collection_id: str, limit: int, base_url: str
    ) -> ItemCollection:
        """Database logic to retrieve an ItemCollection and a count of items contained."""
        search = Search(using=self.client, index="stac_items")

        collection_filter = Q(
            "bool", should=[Q("match_phrase", **{"collection": collection_id})]
        )
        search = search.query(collection_filter)

        count = self.search_count(search)

        # search = search.sort({"id.keyword" : {"order" : "asc"}})
        search = search.query()[0:limit]
        collection_children = search.execute().to_dict()

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

    def create_search_object(self):
        """Database logic to create a nosql Search instance."""
        search = (
            Search()
            .using(self.client)
            .index(ITEMS_INDEX)
            .sort(
                {"properties.datetime": {"order": "desc"}},
                {"id": {"order": "desc"}},
                {"collection": {"order": "desc"}},
            )
        )
        return search

    def create_query_filter(self, search, op: str, field: str, value: float):
        """Database logic to perform query for search endpoint."""
        if op != "eq":
            key_filter = {field: {f"{op}": value}}
            search = search.query(Q("range", **key_filter))
        else:
            search = search.query("match_phrase", **{field: value})

        return search

    def search_ids(self, search, item_ids: List):
        """Database logic to search a list of STAC item ids."""
        id_list = []
        for item_id in item_ids:
            id_list.append(Q("match_phrase", **{"id": item_id}))
        id_filter = Q("bool", should=id_list)
        search = search.query(id_filter)

        return search

    def search_collections(self, search, collection_ids: List):
        """Database logic to search a list of STAC collection ids."""
        collection_list = []
        for collection_id in collection_ids:
            collection_list.append(Q("match_phrase", **{"collection": collection_id}))
        collection_filter = Q("bool", should=collection_list)
        search = search.query(collection_filter)

        return search

    def search_datetime(self, search, datetime_search):
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

    def search_bbox(self, search, bbox: List):
        """Database logic to search on bounding box."""
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
        return search

    def search_intersects(self, search, intersects: dict):
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

    def sort_field(self, search, field, direction):
        """Database logic to sort nosql search instance."""
        search = search.sort({field: {"order": direction}})
        return search

    def search_count(self, search) -> int:
        """Database logic to count search results."""
        try:
            count = search.count()
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError("No items exist")

        return count

    def execute_search(self, search, limit: int, base_url: str) -> List:
        """Database logic to execute search with limit."""
        search = search.query()[0:limit]
        response = search.execute().to_dict()

        if len(response["hits"]["hits"]) > 0:
            response_features = [
                self.item_serializer.db_to_stac(item["_source"], base_url=base_url)
                for item in response["hits"]["hits"]
            ]
        else:
            response_features = []

        return response_features

    # Transaction Logic

    def check_collection_exists(self, collection_id: str):
        """Database logic to check if a collection exists."""
        if not self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise ForeignKeyError(f"Collection {collection_id} does not exist")

    def prep_create_item(self, item: stac_types.Item, base_url: str) -> stac_types.Item:
        """Database logic for prepping an item for insertion."""
        self.check_collection_exists(collection_id=item["collection"])

        if self.client.exists(
            index=ITEMS_INDEX, id=mk_item_id(item["id"], item["collection"])
        ):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    def create_item(self, item: stac_types.Item, base_url: str):
        """Database logic for creating one item."""
        # todo: check if collection exists, but cache
        es_resp = self.client.index(
            index=ITEMS_INDEX,
            id=mk_item_id(item["id"], item["collection"]),
            document=item,
        )

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

    def prep_update_item(self, item: stac_types.Item):
        """Database logic for prepping an item for updating."""
        self.check_collection_exists(collection_id=item["collection"])

    def delete_item(self, item_id: str, collection_id: str):
        """Database logic for deleting one item."""
        try:
            self.client.delete(index=ITEMS_INDEX, id=mk_item_id(item_id, collection_id))
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )

    def create_collection(self, collection: stac_types.Collection):
        """Database logic for creating one collection."""
        if self.client.exists(index=COLLECTIONS_INDEX, id=collection["id"]):
            raise ConflictError(f"Collection {collection['id']} already exists")

        self.client.index(
            index=COLLECTIONS_INDEX,
            id=collection["id"],
            document=collection,
        )

    def find_collection(self, collection_id: str) -> stac_types.Collection:
        """Database logic to find and return a collection."""
        try:
            collection = self.client.get(index=COLLECTIONS_INDEX, id=collection_id)
        except elasticsearch.exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")

        return collection

    def prep_update_collection(self, collection_id: str):
        """Database logic for prepping a collection for updating."""
        _ = self.find_collection(collection_id=collection_id)

    def delete_collection(self, collection_id: str):
        """Database logic for deleting one collection."""
        _ = self.find_collection(collection_id=collection_id)
        self.client.delete(index=COLLECTIONS_INDEX, id=collection_id)

    def bulk_sync(self, processed_items):
        """Database logic for bulk item insertion."""
        actions = [
            {
                "_index": ITEMS_INDEX,
                "_id": mk_item_id(item["id"], item["collection"]),
                "_source": item,
            }
            for item in processed_items
        ]
        helpers.bulk(self.client, actions)
