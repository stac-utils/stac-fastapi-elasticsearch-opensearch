"""Database logic."""

import asyncio
import logging
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple, Type, Union

import attr
from elasticsearch_dsl import Q, Search

from elasticsearch import exceptions, helpers  # type: ignore
from stac_fastapi.core.extensions import filter
from stac_fastapi.core.serializers import (
    CollectionSerializer,
    ItemSerializer,
    CatalogSerializer,
    CatalogCollectionSerializer,
)
from stac_fastapi.core.utilities import bbox2polygon
from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings
from stac_fastapi.elasticsearch.config import (
    ElasticsearchSettings as SyncElasticsearchSettings,
)
from stac_fastapi.types.errors import ConflictError, NotFoundError
from stac_fastapi.types.stac import Collection, Item, Catalog

logger = logging.getLogger(__name__)

NumType = Union[float, int]

CATALOGS_INDEX = os.getenv("STAC_CATALOGS_INDEX", "catalogs")
COLLECTIONS_INDEX = os.getenv("STAC_COLLECTIONS_INDEX", "collections")
COLLECTIONS_INDEX_PREFIX = os.getenv("STAC_COLLECTIONS_INDEX_PREFIX", "collections_")
ITEMS_INDEX_PREFIX = os.getenv("STAC_ITEMS_INDEX_PREFIX", "items_")
ES_INDEX_NAME_UNSUPPORTED_CHARS = {
    "\\",
    "/",
    "*",
    "?",
    '"',
    "<",
    ">",
    "|",
    " ",
    ",",
    "#",
    ":",
}

ITEM_INDICES = f"{ITEMS_INDEX_PREFIX}*,-*kibana*,-{COLLECTIONS_INDEX}*"
COLLECTION_INDICES = f"{COLLECTIONS_INDEX_PREFIX}*,-*kibana*,-{CATALOGS_INDEX}*"

DEFAULT_SORT = {
    "properties.datetime": {"order": "desc"},
    "id": {"order": "desc"},
    "collection": {"order": "desc"},
}

DEFAULT_COLLECTIONS_SORT = {
    "extent.temporal.interval": {"order": "desc"},
    "id": {"order": "desc"},
}

DEFAULT_DISCOVERY_SORT = {
    "id": {"order": "desc"},
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
            "mapping": {"type": "object", "enabled": False},
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
        "id": {"type": "keyword"},
        "extent.spatial.bbox": {"type": "double"},
        "extent.temporal.interval": {"type": "date"},
        "providers": {"type": "object", "enabled": False},
        "links": {"type": "object", "enabled": False},
        "item_assets": {"type": "object", "enabled": False},
        "keywords": {"type": "keyword"},
    },
    # Collection Search Extension https://github.com/stac-api-extensions/collection-search
    "runtime": {
        "collection_start_time": {
            "type": "date",
            "on_script_error": "continue",
            "script": {
                "source": """
                def times = params._source.extent.temporal.interval; 
                def time = times[0][0]; 
                if (time == null) { 
                    def datetime = ZonedDateTime.parse('0000-10-01T00:00:00Z'); 
                    emit(datetime.toInstant().toEpochMilli()); 
                } 
                else { 
                    def datetime = ZonedDateTime.parse(time); 
                    emit(datetime.toInstant().toEpochMilli())
                }"""
            },
        },
        "collection_end_time": {
            "type": "date",
            "on_script_error": "continue",
            "script": {
                "source": """
                def times = params._source.extent.temporal.interval; 
                def time = times[0][1]; 
                if (time == null) { 
                    def datetime = ZonedDateTime.parse('9900-12-01T12:31:12Z'); 
                    emit(datetime.toInstant().toEpochMilli()); 
                } 
                else { 
                    def datetime = ZonedDateTime.parse(time); 
                    emit(datetime.toInstant().toEpochMilli())
                }"""
            },
        },
        "geometry.shape": {
            "type": "keyword",
            "on_script_error": "continue",
            "script": {"source": "emit('Polygon')"},
        },
        "collection_min_lat": {
            "type": "double",
            "on_script_error": "continue",
            "script": {
                "source": "def bbox = params._source.extent.spatial.bbox; emit(bbox[0][0]);"
            },
        },
        "collection_min_lon": {
            "type": "double",
            "on_script_error": "continue",
            "script": {
                "source": "def bbox = params._source.extent.spatial.bbox; emit(bbox[0][1]);"
            },
        },
        "collection_max_lat": {
            "type": "double",
            "on_script_error": "continue",
            "script": {
                "source": "def bbox = params._source.extent.spatial.bbox; emit(bbox[0][2]);"
            },
        },
        "collection_max_lon": {
            "type": "double",
            "on_script_error": "continue",
            "script": {
                "source": "def bbox = params._source.extent.spatial.bbox; emit(bbox[0][3]);"
            },
        },
    },
}

ES_CATALOGS_MAPPINGS = {
    "numeric_detection": False,
    # "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "id": {"type": "keyword"},
        "links": {"type": "object", "enabled": False},
    },
}


def index_by_collection_id(
    collection_id: str = None, catalog_id: Optional[str] = None
) -> str:
    """
    Translate a collection id into an Elasticsearch index name.

    Args:
        collection_id (str): The collection id to translate into an index name.
        catalog_id (str): The catalog id to translate into an index name.

    Returns:
        str: The index name derived from the collection id and catalog id.
    """
    if not collection_id:
        collection_id = ""
    collection_and_catalog_id = collection_id
    if catalog_id:
        collection_and_catalog_id = collection_and_catalog_id + "_" + catalog_id
    return f"{ITEMS_INDEX_PREFIX}{''.join(c for c in collection_and_catalog_id.lower() if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS)}"


def index_by_catalog_id(catalog_id: str) -> str:
    """
    Translate a catalog id into an Elasticsearch index name.

    Args:
        catalog_id (str): The catalog id to translate into an index name.

    Returns:
        str: The index name derived from the catalog id.
    """
    return f"{COLLECTIONS_INDEX_PREFIX}{''.join(c for c in catalog_id.lower() if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS)}"


def indices(
    collection_ids: Optional[List[str]] = None, catalog_ids: Optional[List[str]] = None
) -> str:
    """
    Get a comma-separated string of index names for a given list of collection ids.

    Args:
        collection_ids: A list of collection ids.

    Returns:
        A string of comma-separated index names. If `collection_ids` is None, returns the default indices.
    """
    if not collection_ids and not catalog_ids:
        return ITEM_INDICES
    else:
        if not collection_ids:
            collection_ids = [None]
        return ",".join(
            [
                index_by_collection_id(collection_id=coll, catalog_id=cat)
                for coll in collection_ids
                for cat in catalog_ids
            ]
        )


def collection_indices(catalog_ids: Optional[List[str]]) -> str:
    """
    Get a comma-separated string of index names for a given list of catalog ids.

    Args:
        catalog_ids: A list of catalog ids.

    Returns:
        A string of comma-separated index names. If `catalog_ids` is None, returns the default collection indices.
    """
    if catalog_ids is None:
        return COLLECTION_INDICES
    else:
        return ",".join([index_by_catalog_id(c) for c in catalog_ids])


async def create_index_templates() -> None:
    """
    Create index templates for the Collection and Item indices.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client
    await client.indices.put_template(
        name=f"template_{COLLECTIONS_INDEX}",
        body={
            "index_patterns": [f"{COLLECTIONS_INDEX_PREFIX}*"],
            "mappings": ES_COLLECTIONS_MAPPINGS,
        },
    )
    await client.indices.put_template(
        name=f"template_{ITEMS_INDEX_PREFIX}",
        body={
            "index_patterns": [f"{ITEMS_INDEX_PREFIX}*"],
            "settings": ES_ITEMS_SETTINGS,
            "mappings": ES_ITEMS_MAPPINGS,
        },
    )
    await client.indices.put_template(
        name=f"template_{CATALOGS_INDEX}",
        body={
            "index_patterns": [f"{CATALOGS_INDEX}*"],
            "mappings": ES_CATALOGS_MAPPINGS,
        },
    )
    await client.close()


async def create_collection_index() -> None:
    """
    Create the index for a Collection. The settings of the index template will be used implicitly.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client

    await client.options(ignore_status=400).indices.create(
        index=f"{COLLECTIONS_INDEX}-000001",
        aliases={COLLECTIONS_INDEX: {}},
    )

    await client.close()


async def create_catalog_index() -> None:
    """
    Create the index for a Catalog. The settings of the index template will be used implicitly.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client

    await client.options(ignore_status=400).indices.create(
        index=f"{CATALOGS_INDEX}-000001",
        aliases={CATALOGS_INDEX: {}},
    )
    await client.close()


async def create_item_index(collection_id: str, catalog_id: str):
    """
    Create the index for Items. The settings of the index template will be used implicitly.

    Args:
        collection_id (str): Collection identifier.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client
    index_name = index_by_collection_id(
        collection_id=collection_id, catalog_id=catalog_id
    )

    await client.options(ignore_status=400).indices.create(
        index=f"{index_name}-000001",
        aliases={index_name: {}},
    )
    await client.close()


async def delete_item_index(collection_id: str, catalog_id: str):
    """Delete the index for items in a collection.

    Args:
        collection_id (str): The ID of the collection whose items index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_by_collection_id(collection_id=collection_id, catalog_id=catalog_id)
    resolved = await client.indices.resolve_index(name=name)
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name)
    await client.close()


async def delete_collection_index(catalog_id: str):
    """Delete the index for collections in a catalog.

    Args:
        catalog_id (str): The ID of the catalog whose collections index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_by_catalog_id(catalog_id)
    resolved = await client.indices.resolve_index(name=name)
    try:
        if "aliases" in resolved and resolved["aliases"]:
            [alias] = resolved["aliases"]
            await client.indices.delete_alias(
                index=alias["indices"], name=alias["name"]
            )
            await client.indices.delete(index=alias["indices"])
        else:
            await client.indices.delete(index=name)
        await client.close()
    except exceptions.NotFoundError:
        raise NotFoundError(
            f"Catalog {catalog_id} does not have any associated collections."
        )


def mk_item_id(item_id: str, collection_id: str, catalog_id: str):
    """Create the document id for an Item in Elasticsearch.

    Args:
        item_id (str): The id of the Item.
        collection_id (str): The id of the Collection that the Item belongs to.
        catalog_id (str): The id of the Catalog that the Collection belongs to.

    Returns:
        str: The document id for the Item, combining the Item id, the Collection id and the Catalog id, separated by `|` characters.
    """
    return f"{item_id}|{collection_id}|{catalog_id}"


def mk_collection_id(collection_id: str, catalog_id: str):
    """Create the document id for a collection in Elasticsearch.

    Args:
        collection_id (str): The id of the Collection.
        catalog_id (str): The id of the Catalog that the Collection belongs to.

    Returns:
        str: The document id for the Collection, combining the Collection id and the Catalog id, separated by a `|` character.
    """
    return f"{collection_id}|{catalog_id}"


def mk_actions(catalog_id: str, collection_id: str, processed_items: List[Item]):
    """Create Elasticsearch bulk actions for a list of processed items.

    Args:
        collection_id (str): The identifier for the collection the items belong to.
        processed_items (List[Item]): The list of processed items to be bulk indexed.

    Returns:
        List[Dict[str, Union[str, Dict]]]: The list of bulk actions to be executed,
        each action being a dictionary with the following keys:
        - `_index`: the index to store the document in.
        - `_id`: the document's identifier.
        - `_source`: the source of the document.
    """
    return [
        {
            "_index": index_by_collection_id(
                collection_id=collection_id, catalog_id=catalog_id
            ),
            "_id": mk_item_id(
                item_id=item["id"],
                collection_id=item["collection"],
                catalog_id=catalog_id,
            ),
            "_source": item,
        }
        for item in processed_items
    ]


def get_catalog_id_from_root(collection: Collection):
    collection_links = collection["links"]
    for link in collection_links:
        if link["rel"] == "root":
            root_href = link["href"]
            root_href_split = root_href.split("/")
            catalog_index = root_href_split.index("catalog.json")
            catalog_id = root_href_split[catalog_index - 1]
            return catalog_id
    return "uncatalogued-entries"


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

    item_serializer: Type[ItemSerializer] = attr.ib(default=ItemSerializer)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )
    catalog_serializer: Type[CatalogSerializer] = attr.ib(default=CatalogSerializer)
    catalog_collection_serializer: Type[CatalogCollectionSerializer] = attr.ib(
        default=CatalogCollectionSerializer
    )

    """CORE LOGIC"""

    async def get_all_collections(
        self, token: Optional[str], limit: int, base_url: str
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all collections from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.

        Returns:
            A tuple of (collections, next pagination token if any).
        """
        search_after = None
        if token:
            search_after = [token]

        response = await self.client.search(
            index=f"{COLLECTIONS_INDEX_PREFIX}*",
            body={
                "sort": [{"id": {"order": "asc"}}],
                "size": limit,
                "search_after": search_after,
            },
        )

        hits = response["hits"]["hits"]
        collections = [
            self.collection_serializer.db_to_stac(
                collection=hit["_source"], base_url=base_url
            )
            for hit in hits
        ]

        next_token = None
        if len(hits) == limit:
            next_token = hits[-1]["sort"][0]

        return collections, next_token

    async def get_catalog_collections(
        self, catalog_ids: List[str], token: Optional[str], limit: int, base_url: str
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all collections in a catalog from Elasticsearch, supporting pagination.

        Args:
            catalog_ids (Optional[List[str]]): The catalog ids to search.
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.

        Returns:
            A tuple of (collections, next pagination token if any).
        """
        search_after = None
        if token:
            search_after = [token]

        for catalog_id in catalog_ids:
            await self.check_catalog_exists(catalog_id=catalog_id)

        index_param = collection_indices(catalog_ids=catalog_ids)

        try:
            response = await self.client.search(
                index=index_param,
                body={
                    "sort": [{"id": {"order": "asc"}}],
                    "size": limit,
                    "search_after": search_after,
                },
            )
        except exceptions.NotFoundError:
            response = None
            collections = []
            hits = None

        if response:
            hits = response["hits"]["hits"]
            collections = [
                self.collection_serializer.db_to_stac(
                    collection=hit["_source"], base_url=base_url
                )
                for hit in hits
            ]

        next_token = None
        if hits and len(hits) == limit:
            next_token = hits[-1]["sort"][0]

        return collections, next_token

    async def get_all_catalogs(
        self, token: Optional[str], limit: int, base_url: str
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all catalogs from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.

        Returns:
            A tuple of (catalogs, next pagination token if any).
        """
        search_after = None
        if token:
            search_after = [token]

        response = await self.client.search(
            index=CATALOGS_INDEX,
            body={
                "sort": [{"id": {"order": "asc"}}],
                "size": limit,
                "search_after": search_after,
            },
        )

        hits = response["hits"]["hits"]
        catalogs = [
            self.catalog_serializer.db_to_stac(
                catalog=hit["_source"], base_url=base_url
            )
            for hit in hits
        ]

        next_token = None
        if len(hits) == limit:
            next_token = hits[-1]["sort"][0]

        return catalogs, next_token

    async def get_one_item(
        self, catalog_id: str, collection_id: str, item_id: str
    ) -> Dict:
        """Retrieve a single item from the database.

        Args:
            collection_id (str): The id of the Collection that the Item belongs to.
            item_id (str): The id of the Item.

        Returns:
            item (Dict): A dictionary containing the source data for the Item.

        Raises:
            NotFoundError: If the specified Item does not exist in the Collection.

        Notes:
            The Item is retrieved from the Elasticsearch database using the `client.get` method,
            with the index for the Collection as the target index and the combined `mk_item_id` as the document id.
        """
        try:
            item = await self.client.get(
                index=index_by_collection_id(
                    collection_id=collection_id, catalog_id=catalog_id
                ),
                id=mk_item_id(
                    item_id=item_id, collection_id=collection_id, catalog_id=catalog_id
                ),
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
    def make_collection_search():
        """Database logic to create a Search instance."""
        return Search().sort(*DEFAULT_COLLECTIONS_SORT)

    @staticmethod
    def make_discovery_search():
        """Database logic to create a Search instance."""
        return Search().sort(*DEFAULT_DISCOVERY_SORT)

    @staticmethod
    def apply_ids_filter(search: Search, item_ids: List[str]):
        """Database logic to search a list of STAC item ids."""
        return search.filter("terms", id=item_ids)

    @staticmethod
    def apply_collections_filter(search: Search, collection_ids: List[str]):
        """Database logic to search a list of STAC collection ids."""
        return search.filter("terms", collection=collection_ids)

    @staticmethod
    def apply_catalogs_filter(search: Search, catalog_ids: List[str]):
        """Database logic to search a list of STAC Catalog ids."""
        return search.filter("terms", catalog=catalog_ids)

    @staticmethod
    def apply_datetime_filter(search: Search, datetime_search):
        """Apply a filter to search based on datetime field.

        Args:
            search (Search): The search object to filter.
            datetime_search (dict): The datetime filter criteria.

        Returns:
            Search: The filtered search object.
        """
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
    def apply_datetime_collections_filter(search: Search, datetime_search):
        """Apply a filter to search collections based on datetime field.

        Args:
            search (Search): The search object to filter.
            datetime_search (dict): The datetime filter criteria.

        Returns:
            Search: The filtered search object.
        """
        if "eq" in datetime_search:
            search = search.filter(
                "range", **{"collection_start_time": {"lte": datetime_search["eq"]}}
            )
            search = search.filter(
                "range", **{"collection_end_time": {"gte": datetime_search["eq"]}}
            )

        else:
            should = []
            should.extend(
                [
                    Q(
                        "bool",
                        filter=[
                            Q(
                                "range",
                                collection_start_time={
                                    "lte": datetime_search["lte"],
                                    "gte": datetime_search["gte"],
                                },
                            ),
                        ],
                    ),
                    Q(
                        "bool",
                        filter=[
                            Q(
                                "range",
                                collection_end_time={
                                    "lte": datetime_search["lte"],
                                    "gte": datetime_search["gte"],
                                },
                            ),
                        ],
                    ),
                    Q(
                        "bool",
                        filter=[
                            Q(
                                "range",
                                collection_start_time={
                                    "lte": datetime_search["gte"],
                                },
                            ),
                            Q(
                                "range",
                                collection_end_time={
                                    "gte": datetime_search["lte"],
                                },
                            ),
                        ],
                    ),
                ]
            )
            search = search.query(Q("bool", filter=[Q("bool", should=should)]))

        return search

    @staticmethod
    def apply_bbox_filter(search: Search, bbox: List):
        """Filter search results based on bounding box.

        Args:
            search (Search): The search object to apply the filter to.
            bbox (List): The bounding box coordinates, represented as a list of four values [minx, miny, maxx, maxy].

        Returns:
            search (Search): The search object with the bounding box filter applied.

        Notes:
            The bounding box is transformed into a polygon using the `bbox2polygon` function and
            a geo_shape filter is added to the search object, set to intersect with the specified polygon.
        """
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
    def apply_bbox_collections_filter(search: Search, bbox: List):
        """Filter collections search results based on bounding box.

        Args:
            search (Search): The search object to apply the filter to.
            bbox (List): The bounding box coordinates, represented as a list of four values [minx, miny, maxx, maxy].

        Returns:
            search (Search): The search object with the bounding box filter applied.
        """

        must = []
        must.extend(
            [
                Q(
                    "bool",
                    filter=[
                        Q(
                            "range",
                            collection_max_lat={
                                "gte": bbox[0],
                            },
                        ),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q(
                            "range",
                            collection_min_lat={
                                "lte": bbox[2],
                            },
                        ),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q(
                            "range",
                            collection_max_lon={
                                "gte": bbox[1],
                            },
                        ),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q(
                            "range",
                            collection_min_lon={
                                "lte": bbox[3],
                            },
                        ),
                    ],
                ),
            ]
        )

        return search.query(Q("bool", filter=[Q("bool", must=must)]))

    @staticmethod
    def apply_keyword_collections_filter(search: Search, q: str):
        keyword_list = [keyword.strip() for keyword in q.split(",")]
        should = []
        should.extend(
            [
                Q(
                    "bool",
                    filter=[
                        Q(
                            "match",
                            title={"query": q},
                        ),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q(
                            "match",
                            description={"query": q},
                        ),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q("terms", keywords=keyword_list),
                    ],
                ),
            ]
        )

        search = search.query(Q("bool", filter=[Q("bool", should=should)]))

        return search

    @staticmethod
    def apply_keyword_discovery_filter(search: Search, q: str):
        keyword_list = [keyword.strip() for keyword in q.split(",")]
        # Construct search query for keywords
        # For catalogues and collections this searches title and description
        # For collections this also searches keywords
        should_filter = []
        should_filter.extend(
            [
                Q(
                    "bool",
                    filter=[
                        Q("match", title=q),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q("match", description=q),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q("terms", keywords=keyword_list),
                    ],
                ),
            ]
        )
        # The following query is then used to score the returned results
        # Calculate scoring for keyword field
        should_query = [{"term": {"keywords": keyword}} for keyword in keyword_list]
        # Calculate scoring for title and description fields
        should_query.extend(
            [
                {
                    "multi_match": {
                        "query": q,
                        "fields": ["title", "description"],
                        "type": "most_fields",
                    }
                }
            ]
        )

        search = search.query(
            Q("bool", filter=[Q("bool", should=should_filter)], should=should_query)
        )
        return search

    @staticmethod
    def apply_intersects_filter(
        search: Search,
        intersects: Geometry,
    ):
        """Filter search results based on intersecting geometry.

        Args:
            search (Search): The search object to apply the filter to.
            intersects (Geometry): The intersecting geometry, represented as a GeoJSON-like object.

        Returns:
            search (Search): The search object with the intersecting geometry filter applied.

        Notes:
            A geo_shape filter is added to the search object, set to intersect with the specified geometry.
        """
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
        """Filter search results based on a comparison between a field and a value.

        Args:
            search (Search): The search object to apply the filter to.
            op (str): The comparison operator to use. Can be 'eq' (equal), 'gt' (greater than), 'gte' (greater than or equal),
                'lt' (less than), or 'lte' (less than or equal).
            field (str): The field to perform the comparison on.
            value (float): The value to compare the field against.

        Returns:
            search (Search): The search object with the specified filter applied.
        """
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
        catalog_ids: Optional[List[str]],
        limit: int,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        collection_ids: Optional[List[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Execute a search query with limit and other optional parameters.

        Args:
            search (Search): The search query to be executed.
            limit (int): The maximum number of results to be returned.
            token (Optional[str]): The token used to return the next set of results.
            sort (Optional[Dict[str, Dict[str, str]]]): Specifies how the results should be sorted.
            collection_ids (Optional[List[str]]): The collection ids to search.
            ignore_unavailable (bool, optional): Whether to ignore unavailable collections. Defaults to True.

        Returns:
            Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]: A tuple containing:
                - An iterable of search results, where each result is a dictionary with keys and values representing the
                fields and values of each document.
                - The total number of results (if the count could be computed), or None if the count could not be
                computed.
                - The token to be used to retrieve the next set of results, or None if there are no more results.

        Raises:
            NotFoundError: If the collections specified in `collection_ids` do not exist.
        """

        # Can only provide a collection if you also provide the containing catalog
        if collection_ids and not catalog_ids:
            raise Exception(
                "To search specific collection, you must provide the containing catalog."
            )

        search_after = None
        if token:
            search_after = urlsafe_b64decode(token.encode()).decode().split(",")

        query = search.query.to_dict() if search.query else None

        index_param = f"{ITEMS_INDEX_PREFIX}*"

        if collection_ids and catalog_ids:
            index_param = indices(
                collection_ids=collection_ids, catalog_ids=catalog_ids
            )
        elif catalog_ids:
            index_param = indices(catalog_ids=catalog_ids).replace("items_", "items_*")

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

    async def execute_collection_search(
        self,
        search: Search,
        limit: int,
        base_url: str,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        collection_ids: Optional[List[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Execute a search query with limit and other optional parameters.

        Args:
            search (Search): The search query to be executed.
            limit (int): The maximum number of results to be returned.
            token (Optional[str]): The token used to return the next set of results.
            sort (Optional[Dict[str, Dict[str, str]]]): Specifies how the results should be sorted.
            collection_ids (Optional[List[str]]): The collection ids to search.
            ignore_unavailable (bool, optional): Whether to ignore unavailable collections. Defaults to True.

        Returns:
            Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]: A tuple containing:
                - An iterable of search results, where each result is a dictionary with keys and values representing the
                fields and values of each document.
                - The total number of results (if the count could be computed), or None if the count could not be
                computed.
                - The token to be used to retrieve the next set of results, or None if there are no more results.

        Raises:
            NotFoundError: If the collections specified in `collection_ids` do not exist.
        """
        search_after = None
        if token:
            search_after = urlsafe_b64decode(token.encode()).decode().split(",")

        query = search.query.to_dict() if search.query else None

        index_param = "document"  # indices(collection_ids)

        search_task = asyncio.create_task(
            self.client.search(
                index=f"{COLLECTIONS_INDEX_PREFIX}*",
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=sort or DEFAULT_COLLECTIONS_SORT,
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
        collections = [
            self.collection_serializer.db_to_stac(
                collection=hit["_source"], base_url=base_url
            )
            for hit in hits
        ]

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

        return collections, maybe_count, next_token

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str, catalog_id: str):
        """Database logic to check if a collection exists."""
        full_collection_id = mk_collection_id(
            collection_id=collection_id, catalog_id=catalog_id
        )
        index = index_by_catalog_id(catalog_id=catalog_id)
        if not await self.client.exists(index=index, id=full_collection_id):
            raise NotFoundError(
                f"Collection {collection_id} in catalog {catalog_id} does not exist"
            )

    async def check_catalog_exists(self, catalog_id: str):
        """Database logic to check if a catalog exists."""
        if not await self.client.exists(index=f"{CATALOGS_INDEX}", id=catalog_id):
            raise NotFoundError(f"Catalog {catalog_id} does not exist")

    async def prep_create_item(
        self, catalog_id: str, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Preps an item for insertion into the database.

        Args:
            item (Item): The item to be prepped for insertion.
            base_url (str): The base URL used to create the item's self URL.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The prepped item.

        Raises:
            ConflictError: If the item already exists in the database.

        """
        await self.check_collection_exists(
            collection_id=item["collection"], catalog_id=catalog_id
        )

        if not exist_ok and await self.client.exists(
            index=index_by_collection_id(
                collection_id=item["collection"], catalog_id=catalog_id
            ),
            id=mk_item_id(
                item_id=item["id"],
                collection_id=item["collection"],
                catalog_id=catalog_id,
            ),
        ):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    def sync_prep_create_item(
        self, catalog_id: str, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Prepare an item for insertion into the database.

        This method performs pre-insertion preparation on the given `item`,
        such as checking if the collection the item belongs to exists,
        and optionally verifying that an item with the same ID does not already exist in the database.

        Args:
            item (Item): The item to be inserted into the database.
            base_url (str): The base URL used for constructing URLs for the item.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The item after preparation is done.

        Raises:
            NotFoundError: If the collection that the item belongs to does not exist in the database.
            ConflictError: If an item with the same ID already exists in the collection.
        """
        item_id = item["id"]
        collection_id = item["collection"]
        if not self.sync_client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise NotFoundError(f"Collection {collection_id} does not exist")

        if not exist_ok and self.sync_client.exists(
            index=index_by_collection_id(
                collection_id=collection_id, catalog_id=catalog_id
            ),
            id=mk_item_id(
                item_id=item_id, collection_id=collection_id, catalog_id=catalog_id
            ),
        ):
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    async def create_item(self, catalog_id: str, item: Item, refresh: bool = False):
        """Database logic for creating one item.

        Args:
            item (Item): The item to be created.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to False.

        Raises:
            ConflictError: If the item already exists in the database.

        Returns:
            None
        """
        # todo: check if collection exists, but cache
        item_id = item["id"]
        collection_id = item["collection"]
        es_resp = await self.client.index(
            index=index_by_collection_id(
                collection_id=collection_id, catalog_id=catalog_id
            ),
            id=mk_item_id(
                item_id=item_id, collection_id=collection_id, catalog_id=catalog_id
            ),
            document=item,
            refresh=refresh,
        )

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} in catalog {catalog_id} already exists"
            )

    async def delete_item(
        self, item_id: str, collection_id: str, catalog_id: str, refresh: bool = False
    ):
        """Delete a single item from the database.

        Args:
            item_id (str): The id of the Item to be deleted.
            collection_id (str): The id of the Collection that the Item belongs to.
            refresh (bool, optional): Whether to refresh the index after the deletion. Default is False.

        Raises:
            NotFoundError: If the Item does not exist in the database.
        """
        try:
            await self.client.delete(
                index=index_by_collection_id(
                    collection_id=collection_id, catalog_id=catalog_id
                ),
                id=mk_item_id(
                    item_id=item_id, collection_id=collection_id, catalog_id=catalog_id
                ),
                refresh=refresh,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} in catalog {catalog_id} not found"
            )

    async def prep_create_collection(
        self,
        catalog_id: str,
        collection: Collection,
        base_url: str,
        exist_ok: bool = False,
    ) -> Item:
        """
        Preps a collection for insertion into the database.

        Args:
            catalog_id (str) : The id of the catalog into which the collection will be inserted.
            collection (Collection): The collection to be prepped for insertion.
            base_url (str): The base URL used to create the collection's self URL.
            exist_ok (bool): Indicates whether the collection can exist already.

        Returns:
            Collection: The prepped item.

        Raises:
            ConflictError: If the collection already exists in the catalog in the database.

        """
        await self.check_catalog_exists(catalog_id=catalog_id)
        if not exist_ok and await self.client.exists(
            index=index_by_catalog_id(catalog_id),
            id=mk_collection_id(collection["id"], catalog_id),
        ):
            raise ConflictError(
                f"Collection {collection['id']} in catalog {catalog_id} already exists"
            )

        return self.collection_serializer.stac_to_db(collection, base_url)

    def sync_prep_create_collection(
        self,
        catalog_id: str,
        collection: Collection,
        base_url: str,
        exist_ok: bool = False,
    ) -> Item:
        """
        Prepare an item for insertion into the database.

        This method performs pre-insertion preparation on the given `item`,
        such as checking if the collection the item belongs to exists,
        and optionally verifying that an item with the same ID does not already exist in the database.

        Args:
            catalog_id (str) : The id of the catalog into which the collection will be inserted.
            collection (Collection): The collection to be prepped for insertion.
            base_url (str): The base URL used for constructing URLs for the item.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The item after preparation is done.

        Raises:
            NotFoundError: If the collection that the item belongs to does not exist in the database.
            ConflictError: If a collection with the same ID already exists in the catalog.
        """
        collection_id = collection["id"]
        catalog_id = catalog_id
        if not self.sync_client.exists(index=CATALOGS_INDEX, id=catalog_id):
            raise NotFoundError(f"Catalog {catalog_id} does not exist")

        if not exist_ok and self.sync_client.exists(
            index=index_by_catalog_id(catalog_id),
            id=mk_collection_id(collection_id=collection_id, catalog_id=catalog_id),
        ):
            raise ConflictError(
                f"Collection {collection_id} in catalog {catalog_id} already exists"
            )

        return self.collection_serializer.stac_to_db(collection, base_url)

    async def create_collection(
        self, catalog_id: str, collection: Collection, refresh: bool = False
    ):
        """Database logic for creating one item.

        Args:
            catalog_id (str) : The id of the catalog into which the collection will be inserted.
            collection (Collection): The collection to be created.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to False.

        Raises:
            ConflictError: If the collection already exists in the catalog in the database.

        Returns:
            None
        """
        # todo: check if collection exists, but cache
        collection_id = collection["id"]
        es_resp = await self.client.index(
            index=index_by_catalog_id(catalog_id),
            id=mk_collection_id(collection_id, catalog_id),
            document=collection,
            refresh=refresh,
        )

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Collection {collection_id} in catalog {catalog_id} already exists"
            )

    async def find_collection(self, catalog_id: str, collection_id: str) -> Collection:
        """Find and return a collection from the database.

        Args:
            self: The instance of the object calling this function.
            catalog_id (str): The ID of the collection to be found.
            collection_id (str): The ID of the collection to be found.

        Returns:
            Collection: The found collection, represented as a `Collection` object.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the given catalog in the database.

        Notes:
            This function searches for a collection in the database using the specified `collection_id` and returns the found
            collection as a `Collection` object. If the collection is not found, a `NotFoundError` is raised.
        """
        full_collection_id = mk_collection_id(collection_id, catalog_id)
        try:
            collection = await self.client.get(
                index=index_by_catalog_id(catalog_id), id=full_collection_id
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Collection {collection_id} in catalog {catalog_id} not found"
            )

        return collection["_source"]

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        collection: Collection,
        refresh: bool = False,
    ):
        """Update a collection from the database.

        Args:
            self: The instance of the object calling this function.
            catalog_id (str): The ID of the catalog containing the collection to be updated.
            collection_id (str): The ID of the collection to be updated.
            collection (Collection): The Collection object to be used for the update.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not
            found in the database.

        Notes:
            This function updates the collection in the database using the specified
            `collection_id` and with the collection specified in the `Collection` object.
            If the collection is not found, a `NotFoundError` is raised.
        """
        await self.find_collection(catalog_id=catalog_id, collection_id=collection_id)

        if collection_id != collection["id"]:
            await self.create_collection(catalog_id, collection, refresh=refresh)

            await self.client.reindex(
                body={
                    "dest": {
                        "index": f"{ITEMS_INDEX_PREFIX}{collection['id']}_{catalog_id}"
                    },
                    "source": {
                        "index": f"{ITEMS_INDEX_PREFIX}{collection_id}_{catalog_id}"
                    },
                    "script": {
                        "lang": "painless",
                        "source": f"""ctx._id = ctx._id.replace('{collection_id}', '{collection["id"]}'); ctx._source.collection = '{collection["id"]}' ;""",
                    },
                },
                wait_for_completion=True,
                refresh=refresh,
            )

            await self.delete_collection(
                catalog_id=catalog_id, collection_id=collection_id
            )

        else:
            collections_index = index_by_catalog_id(catalog_id)
            await self.client.index(
                index=collections_index,
                id=mk_collection_id(collection_id, catalog_id),
                document=collection,
                refresh=refresh,
            )

    async def delete_collection(
        self, catalog_id: str, collection_id: str, refresh: bool = False
    ):
        """Delete a collection from the database.

        Parameters:
            self: The instance of the object calling this function.
            catalog_id (str): The ID of the catalog containing the collection to be deleted.
            collection_id (str): The ID of the collection to be deleted.
            refresh (bool): Whether to refresh the index after the deletion (default: False).

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.

        Notes:
            This function first verifies that the collection with the specified `collection_id` exists in the database, and then
            deletes the collection. If `refresh` is set to True, the index is refreshed after the deletion. Additionally, this
            function also calls `delete_item_index` to delete the index for the items in the collection.
        """
        await self.find_collection(catalog_id=catalog_id, collection_id=collection_id)
        await self.client.delete(
            index=index_by_catalog_id(catalog_id),
            id=mk_collection_id(collection_id, catalog_id),
            refresh=refresh,
        )
        await delete_item_index(collection_id=collection_id, catalog_id=catalog_id)

    async def create_catalog(self, catalog: Catalog, refresh: bool = False):
        """Create a single catalog in the database.

        Args:
            catalog (Catalog): The Catalog object to be created.
            refresh (bool, optional): Whether to refresh the index after the creation. Default is False.

        Raises:
            ConflictError: If a Catalog with the same id already exists in the database.

        Notes:
            A new index is created for the collections in the Catalog using the `create_catalog_index` function.
        """
        catalog_id = catalog["id"]

        if await self.client.exists(index=CATALOGS_INDEX, id=catalog_id):
            raise ConflictError(f"Catalog {catalog_id} already exists")

        await self.client.index(
            index=CATALOGS_INDEX,
            id=catalog_id,
            document=catalog,
            refresh=refresh,
        )

    async def find_catalog(self, catalog_id: str) -> Catalog:
        """Find and return a collection from the database.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be found.

        Returns:
            Collection: The found collection, represented as a `Collection` object.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.

        Notes:
            This function searches for a collection in the database using the specified `collection_id` and returns the found
            collection as a `Collection` object. If the collection is not found, a `NotFoundError` is raised.
        """
        try:
            catalog = await self.client.get(index=CATALOGS_INDEX, id=catalog_id)
        except exceptions.NotFoundError:
            raise NotFoundError(f"Catalog {catalog_id} not found")

        return catalog["_source"]

    async def update_catalog(
        self, catalog_id: str, catalog: Catalog, refresh: bool = False
    ):
        """Update a collection from the database.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be updated.
            collection (Collection): The Collection object to be used for the update.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not
            found in the database.

        Notes:
            This function updates the collection in the database using the specified
            `collection_id` and with the collection specified in the `Collection` object.
            If the collection is not found, a `NotFoundError` is raised.
        """
        await self.find_catalog(catalog_id=catalog_id)
        if catalog_id != catalog["id"]:
            await self.create_catalog(catalog, refresh=refresh)

            await self.client.reindex(
                body={
                    "dest": {"index": f"{COLLECTIONS_INDEX_PREFIX}{catalog['id']}"},
                    "source": {"index": index_by_catalog_id(catalog_id)},
                    "script": {
                        "lang": "painless",
                        "source": f"""ctx._id = ctx._id.replace('{catalog_id}', '{catalog["id"]}'); ctx._source.collection = '{catalog["id"]}' ;""",
                    },
                },
                wait_for_completion=True,
                refresh=refresh,
            )

            await self.delete_catalog(catalog_id)

        else:
            await self.client.index(
                index=CATALOGS_INDEX,
                id=catalog_id,
                document=catalog,
                refresh=refresh,
            )

    async def delete_catalog(self, catalog_id: str, refresh: bool = False):
        """Delete a collection from the database.

        Parameters:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be deleted.
            refresh (bool): Whether to refresh the index after the deletion (default: False).

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.

        Notes:
            This function first verifies that the collection with the specified `collection_id` exists in the database, and then
            deletes the collection. If `refresh` is set to True, the index is refreshed after the deletion. Additionally, this
            function also calls `delete_collection_index` to delete the index for the collections in the catalog.
        """
        await self.find_catalog(catalog_id=catalog_id)
        await self.client.delete(index=CATALOGS_INDEX, id=catalog_id, refresh=refresh)
        try:
            await delete_collection_index(catalog_id)
        except exceptions.NotFoundError:
            raise NotFoundError(f"Catalog {catalog_id} does not exist")

    async def bulk_async(
        self,
        catalog_id: str,
        collection_id: str,
        processed_items: List[Item],
        refresh: bool = False,
    ) -> None:
        """Perform a bulk insert of items into the database asynchronously.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            refresh (bool): Whether to refresh the index after the bulk insert (default: False).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`. The
            insert is performed asynchronously, and the event loop is used to run the operation in a separate executor. The
            `mk_actions` function is called to generate a list of actions for the bulk insert. If `refresh` is set to True, the
            index is refreshed after the bulk insert. The function does not return any value.
        """
        await helpers.async_bulk(
            self.client,
            mk_actions(catalog_id, collection_id, processed_items),
            refresh=refresh,
            raise_on_error=False,
        )

    def bulk_sync(
        self,
        catalog_id: str,
        collection_id: str,
        processed_items: List[Item],
        refresh: bool = False,
    ) -> None:
        """Perform a bulk insert of items into the database synchronously.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            refresh (bool): Whether to refresh the index after the bulk insert (default: False).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`. The
            insert is performed synchronously and blocking, meaning that the function does not return until the insert has
            completed. The `mk_actions` function is called to generate a list of actions for the bulk insert. If `refresh` is set to
            True, the index is refreshed after the bulk insert. The function does not return any value.
        """
        helpers.bulk(
            self.sync_client,
            mk_actions(catalog_id, collection_id, processed_items),
            refresh=refresh,
            raise_on_error=False,
        )

    # DANGER
    async def delete_items(self) -> None:
        """Danger. this is only for tests."""
        await self.client.delete_by_query(
            index=ITEM_INDICES,
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

    async def execute_discovery_search(
        self,
        search: Search,
        limit: int,
        base_url: str,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        catalog_ids: Optional[List[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Execute a search query with limit and other optional parameters.

        Args:
            search (Search): The search query to be executed.
            limit (int): The maximum number of results to be returned.
            token (Optional[str]): The token used to return the next set of results.
            sort (Optional[Dict[str, Dict[str, str]]]): Specifies how the results should be sorted.
            collection_ids (Optional[List[str]]): The collection ids to search.
            ignore_unavailable (bool, optional): Whether to ignore unavailable collections. Defaults to True.

        Returns:
            Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]: A tuple containing:
                - An iterable of search results, where each result is a dictionary with keys and values representing the
                fields and values of each document.
                - The total number of results (if the count could be computed), or None if the count could not be
                computed.
                - The token to be used to retrieve the next set of results, or None if there are no more results.

        Raises:
            NotFoundError: If the collections specified in `collection_ids` do not exist.
        """
        search_after = None
        if token:
            search_after = urlsafe_b64decode(token.encode()).decode().split(",")

        query = search.query.to_dict() if search.query else None

        index_param = "document"  # indices(collection_ids)

        search_task = asyncio.create_task(
            self.client.search(
                index=[CATALOGS_INDEX, f"{COLLECTIONS_INDEX_PREFIX}*"],
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=sort or DEFAULT_DISCOVERY_SORT,
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
            raise NotFoundError(f"Catalogs '{catalog_ids}' do not exist")

        hits = es_response["hits"]["hits"]
        data = [
            self.catalog_collection_serializer.db_to_stac(
                data=hit["_source"], base_url=base_url
            )
            for hit in hits
        ]

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

        return data, maybe_count, next_token
