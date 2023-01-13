"""Database logic."""
import asyncio
import logging
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple, Type, Union

import attr
from elasticsearch_dsl import Q, Search

from elasticsearch import exceptions, helpers  # type: ignore
from stac_fastapi.elasticsearch import serializers
from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings
from stac_fastapi.elasticsearch.config import (
    ElasticsearchSettings as SyncElasticsearchSettings,
)
from stac_fastapi.elasticsearch.extensions import filter
from stac_fastapi.types.errors import ConflictError, NotFoundError
from stac_fastapi.types.stac import Collection, Item

logger = logging.getLogger(__name__)

NumType = Union[float, int]

COLLECTIONS_INDEX = os.getenv("STAC_COLLECTIONS_INDEX", "collections")
ITEMS_INDEX_PREFIX = os.getenv("STAC_ITEMS_INDEX_PREFIX", "items_")

DEFAULT_INDICES = f"*,-*kibana*,-{COLLECTIONS_INDEX}"

DEFAULT_SORT = {
    "properties.datetime": {"order": "desc"},
    "id": {"order": "desc"},
    "collection": {"order": "desc"},
}

ES_ITEMS_SETTINGS = {
    "index": {
        "sort.field": list(DEFAULT_SORT.keys()),
        "sort.order": [v["order"] for v in DEFAULT_SORT.values()],
    }
}

ES_MAPPINGS_DYNAMIC_TEMPLATES = [
    # Common https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md
    {
        "descriptions": {
            "match_mapping_type": "string",
            "match": "description",
            "mapping": {"type": "text"},
        }
    },
    {
        "titles": {
            "match_mapping_type": "string",
            "match": "title",
            "mapping": {"type": "text"},
        }
    },
    # Projection Extension https://github.com/stac-extensions/projection
    {"proj_epsg": {"match": "proj:epsg", "mapping": {"type": "integer"}}},
    {
        "proj_projjson": {
            "match": "proj:projjson",
            "mapping": {"type": "object", "enabled": False},
        }
    },
    {
        "proj_centroid": {
            "match": "proj:centroid",
            "mapping": {"type": "geo_point"},
        }
    },
    {
        "proj_geometry": {
            "match": "proj:geometry",
            "mapping": {"type": "geo_shape"},
        }
    },
    {
        "no_index_href": {
            "match": "href",
            "mapping": {"type": "text", "index": False},
        }
    },
    # Default all other strings not otherwise specified to keyword
    {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}},
    {"numerics": {"match_mapping_type": "long", "mapping": {"type": "float"}}},
]

ES_ITEMS_MAPPINGS = {
    "numeric_detection": False,
    "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "id": {"type": "keyword"},
        "collection": {"type": "keyword"},
        "geometry": {"type": "geo_shape"},
        "assets": {"type": "object", "enabled": False},
        "links": {"type": "object", "enabled": False},
        "properties": {
            "type": "object",
            "properties": {
                # Common https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md
                "datetime": {"type": "date"},
                "start_datetime": {"type": "date"},
                "end_datetime": {"type": "date"},
                "created": {"type": "date"},
                "updated": {"type": "date"},
                # Satellite Extension https://github.com/stac-extensions/sat
                "sat:absolute_orbit": {"type": "integer"},
                "sat:relative_orbit": {"type": "integer"},
            },
        },
    },
}

ES_COLLECTIONS_MAPPINGS = {
    "numeric_detection": False,
    "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "extent.spatial.bbox": {"type": "long"},
        "extent.temporal.interval": {"type": "date"},
        "providers": {"type": "object", "enabled": False},
        "links": {"type": "object", "enabled": False},
        "item_assets": {"type": "object", "enabled": False},
    },
}


def index_by_collection_id(collection_id: str) -> str:
    """Translate a collection id into an ES index name."""
    return f"{ITEMS_INDEX_PREFIX}{collection_id}"


def indices(collection_ids: Optional[List[str]]) -> str:
    """Get a comma-separated string value of indexes for a given list of collection ids."""
    if collection_ids is None:
        return DEFAULT_INDICES
    else:
        return ",".join([f"{ITEMS_INDEX_PREFIX}{c.strip()}" for c in collection_ids])


async def create_collection_index() -> None:
    """Create the index for Items and Collections."""
    await AsyncElasticsearchSettings().create_client.indices.create(
        index=COLLECTIONS_INDEX,
        mappings=ES_COLLECTIONS_MAPPINGS,
        ignore=400,  # ignore 400 already exists code
    )


async def create_item_index(collection_id: str):
    """Create the index for Items and Collections."""
    await AsyncElasticsearchSettings().create_client.indices.create(
        index=index_by_collection_id(collection_id),
        mappings=ES_ITEMS_MAPPINGS,
        settings=ES_ITEMS_SETTINGS,
        ignore=400,  # ignore 400 already exists code
    )


async def delete_item_index(collection_id: str):
    """Create the index for Items and Collections."""
    await AsyncElasticsearchSettings().create_client.indices.delete(
        index=index_by_collection_id(collection_id)
    )


def bbox2polygon(b0: float, b1: float, b2: float, b3: float) -> List[List[List[float]]]:
    """Transform bbox to polygon."""
    return [[[b0, b1], [b2, b1], [b2, b3], [b0, b3], [b0, b1]]]


def mk_item_id(item_id: str, collection_id: str):
    """Make the Elasticsearch document _id value from the Item id and collection."""
    return f"{item_id}|{collection_id}"


def mk_actions(collection_id: str, processed_items: List[Item]):
    """Make the Elasticsearch bulk action for a list of Items."""
    return [
        {
            "_index": index_by_collection_id(collection_id),
            "_id": mk_item_id(item["id"], item["collection"]),
            "_source": item,
        }
        for item in processed_items
    ]


# stac_pydantic classes extend _GeometryBase, which doesn't have a type field,
# So create our own Protocol for typing
# Union[ Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection]
class Geometry(Protocol):  # noqa
    type: str
    coordinates: Any


@attr.s
class DatabaseLogic:
    """Database logic."""

    client = AsyncElasticsearchSettings().create_client
    sync_client = SyncElasticsearchSettings().create_client

    item_serializer: Type[serializers.ItemSerializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.CollectionSerializer] = attr.ib(
        default=serializers.CollectionSerializer
    )

    """CORE LOGIC"""

    async def get_all_collections(self) -> Iterable[Dict[str, Any]]:
        """Database logic to retrieve a list of all collections."""
        # https://github.com/stac-utils/stac-fastapi-elasticsearch/issues/65
        # collections should be paginated, but at least return more than the default 10 for now
        collections = await self.client.search(index=COLLECTIONS_INDEX, size=1000)
        return (c["_source"] for c in collections["hits"]["hits"])

    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
        """Database logic to retrieve a single item."""
        try:
            item = await self.client.get(
                index=index_by_collection_id(collection_id),
                id=mk_item_id(item_id, collection_id),
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist in Collection {collection_id}"
            )
        return item["_source"]

    @staticmethod
    def make_search():
        """Database logic to create a Search instance."""
        return Search().sort(*DEFAULT_SORT)

    @staticmethod
    def apply_ids_filter(search: Search, item_ids: List[str]):
        """Database logic to search a list of STAC item ids."""
        return search.filter("terms", id=item_ids)

    @staticmethod
    def apply_collections_filter(search: Search, collection_ids: List[str]):
        """Database logic to search a list of STAC collection ids."""
        return search.filter("terms", collection=collection_ids)

    @staticmethod
    def apply_datetime_filter(search: Search, datetime_search):
        """Database logic to search datetime field."""
        if "eq" in datetime_search:
            search = search.filter(
                "term", **{"properties__datetime": datetime_search["eq"]}
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
    def apply_bbox_filter(search: Search, bbox: List):
        """Database logic to search on bounding box."""
        return search.filter(
            Q(
                {
                    "geo_shape": {
                        "geometry": {
                            "shape": {
                                "type": "polygon",
                                "coordinates": bbox2polygon(*bbox),
                            },
                            "relation": "intersects",
                        }
                    }
                }
            )
        )

    @staticmethod
    def apply_intersects_filter(
        search: Search,
        intersects: Geometry,
    ):
        """Database logic to search a geojson object."""
        return search.filter(
            Q(
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
        )

    @staticmethod
    def apply_stacql_filter(search: Search, op: str, field: str, value: float):
        """Database logic to perform query for search endpoint."""
        if op != "eq":
            key_filter = {field: {f"{op}": value}}
            search = search.filter(Q("range", **key_filter))
        else:
            search = search.filter("term", **{field: value})

        return search

    @staticmethod
    def apply_cql2_filter(search: Search, _filter: Optional[Dict[str, Any]]):
        """Database logic to perform query for search endpoint."""
        if _filter is not None:
            search = search.filter(filter.Clause.parse_obj(_filter).to_es())
        return search

    @staticmethod
    def populate_sort(sortby: List) -> Optional[Dict[str, Dict[str, str]]]:
        """Database logic to sort search instance."""
        if sortby:
            return {s.field: {"order": s.direction} for s in sortby}
        else:
            return None

    async def execute_search(
        self,
        search: Search,
        limit: int,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        collection_ids: Optional[List[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Database logic to execute search with limit."""
        search_after = None
        if token:
            search_after = urlsafe_b64decode(token.encode()).decode().split(",")

        query = search.query.to_dict() if search.query else None

        index_param = indices(collection_ids)

        search_task = asyncio.create_task(
            self.client.search(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=sort or DEFAULT_SORT,
                search_after=search_after,
                size=limit,
            )
        )

        count_task = asyncio.create_task(
            self.client.count(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                body=search.to_dict(count=True),
            )
        )

        try:
            es_response = await search_task
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collections '{collection_ids}' do not exist")

        hits = es_response["hits"]["hits"]
        items = (hit["_source"] for hit in hits)

        next_token = None
        if hits and (sort_array := hits[-1].get("sort")):
            next_token = urlsafe_b64encode(
                ",".join([str(x) for x in sort_array]).encode()
            ).decode()

        # (1) count should not block returning results, so don't wait for it to be done
        # (2) don't cancel the task so that it will populate the ES cache for subsequent counts
        maybe_count = None
        if count_task.done():
            try:
                maybe_count = count_task.result().get("count")
            except Exception as e:
                logger.error(f"Count task failed: {e}")

        return items, maybe_count, next_token

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str):
        """Database logic to check if a collection exists."""
        if not await self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise NotFoundError(f"Collection {collection_id} does not exist")

    async def prep_create_item(self, item: Item, base_url: str) -> Item:
        """Database logic for prepping an item for insertion."""
        await self.check_collection_exists(collection_id=item["collection"])

        if await self.client.exists(
            index=index_by_collection_id(item["collection"]),
            id=mk_item_id(item["id"], item["collection"]),
        ):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    def sync_prep_create_item(self, item: Item, base_url: str) -> Item:
        """Database logic for prepping an item for insertion."""
        item_id = item["id"]
        collection_id = item["collection"]
        if not self.sync_client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise NotFoundError(f"Collection {collection_id} does not exist")

        if self.sync_client.exists(
            index=index_by_collection_id(collection_id),
            id=mk_item_id(item_id, collection_id),
        ):
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    async def create_item(self, item: Item, refresh: bool = False):
        """Database logic for creating one item."""
        # todo: check if collection exists, but cache
        item_id = item["id"]
        collection_id = item["collection"]
        es_resp = await self.client.index(
            index=index_by_collection_id(collection_id),
            id=mk_item_id(item_id, collection_id),
            document=item,
            refresh=refresh,
        )

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} already exists"
            )

    async def delete_item(
        self, item_id: str, collection_id: str, refresh: bool = False
    ):
        """Database logic for deleting one item."""
        try:
            await self.client.delete(
                index=index_by_collection_id(collection_id),
                id=mk_item_id(item_id, collection_id),
                refresh=refresh,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )

    async def create_collection(self, collection: Collection, refresh: bool = False):
        """Database logic for creating one collection."""
        collection_id = collection["id"]

        if await self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise ConflictError(f"Collection {collection_id} already exists")

        await self.client.index(
            index=COLLECTIONS_INDEX,
            id=collection_id,
            document=collection,
            refresh=refresh,
        )

        await create_item_index(collection_id)

    async def find_collection(self, collection_id: str) -> Collection:
        """Database logic to find and return a collection."""
        try:
            collection = await self.client.get(
                index=COLLECTIONS_INDEX, id=collection_id
            )
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")

        return collection["_source"]

    async def delete_collection(self, collection_id: str, refresh: bool = False):
        """Database logic for deleting one collection."""
        await self.find_collection(collection_id=collection_id)
        await self.client.delete(
            index=COLLECTIONS_INDEX, id=collection_id, refresh=refresh
        )
        await delete_item_index(collection_id)

    async def bulk_async(
        self, collection_id: str, processed_items: List[Item], refresh: bool = False
    ) -> None:
        """Database logic for async bulk item insertion."""
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: helpers.bulk(
                self.sync_client,
                mk_actions(collection_id, processed_items),
                refresh=refresh,
                raise_on_error=False,
            ),
        )

    def bulk_sync(
        self, collection_id: str, processed_items: List[Item], refresh: bool = False
    ) -> None:
        """Database logic for sync bulk item insertion."""
        helpers.bulk(
            self.sync_client,
            mk_actions(collection_id, processed_items),
            refresh=refresh,
            raise_on_error=False,
        )

    # DANGER
    async def delete_items(self) -> None:
        """Danger. this is only for tests."""
        await self.client.delete_by_query(
            index=DEFAULT_INDICES,
            body={"query": {"match_all": {}}},
            wait_for_completion=True,
        )

    # DANGER
    async def delete_collections(self) -> None:
        """Danger. this is only for tests."""
        await self.client.delete_by_query(
            index=COLLECTIONS_INDEX,
            body={"query": {"match_all": {}}},
            wait_for_completion=True,
        )
