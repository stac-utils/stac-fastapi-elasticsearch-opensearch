"""Database logic."""

import asyncio
import logging
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple, Type, Union

import attr
from elasticsearch_dsl import Q, Search

import stac_fastapi.types.search
from elasticsearch import exceptions, helpers  # type: ignore
from stac_fastapi.core.extensions import filter
from stac_fastapi.core.serializers import (
    CatalogCollectionSerializer,
    CatalogSerializer,
    CollectionSerializer,
    ItemSerializer,
)
from stac_fastapi.core.utilities import bbox2polygon
from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings
from stac_fastapi.elasticsearch.config import (
    ElasticsearchSettings as SyncElasticsearchSettings,
)
from stac_fastapi.types.errors import ConflictError, NotFoundError
from stac_fastapi.types.stac import Catalog, Collection, Item

logger = logging.getLogger(__name__)

NumType = Union[float, int]

NUMBER_OF_CATALOG_COLLECTIONS = os.getenv("NUMBER_OF_CATALOG_COLLECTIONS", 100)

CATALOG_SEPARATOR = os.getenv(
    "CATALOG_SEPARATOR", "____"
)  # 4 underscores, as this should not appear in any catalog or collection id

GROUP_SEPARATOR = os.getenv(
    "GROUP_SEPARATOR", "_xx_"
)  # unique identifier used to separete catalog and collection identifies in index names

ROOT_CATALOGS_INDEX = os.getenv("STAC_ROOT_CATALOGS_INDEX", "catalogs_root")
ROOT_CATALOGS_INDEX_PREFIX = os.getenv("STAC_ROOT_CATALOGS_INDEX", "root_")

CATALOGS_INDEX = os.getenv("STAC_CATALOGS_INDEX", "catalogs")
CATALOGS_INDEX_PREFIX = os.getenv("STAC_CATALOGS_INDEX", "catalogs_")

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
CATALOG_INDICES = f"{CATALOGS_INDEX_PREFIX}*,-*kibana*,-{CATALOGS_INDEX}*"

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
    collection_id: Optional[str] = None, catalog_path_list: Optional[List[str]] = None
) -> str:
    """
    Translate a collection id into an Elasticsearch index name.

    Args:
        collection_id (str): The collection id to translate into an index name.
        catalog_path_list (str): The catalog id to translate into an index name.

    Returns:
        str: The index name derived from the collection id and catalog id.
    """
    index = ITEMS_INDEX_PREFIX
    if collection_id:
        index += f"{''.join(c for c in collection_id.lower() if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS)}{GROUP_SEPARATOR}"  # GROUP_SEPARATOR means end of each group, e.g. items_collections_xx_catalogs
    else:
        index += f"*{GROUP_SEPARATOR}"
    if catalog_path_list:
        new_catalog_path_list = catalog_path_list.copy()
        new_catalog_path_list.reverse()
        new_catalog_path_list = CATALOG_SEPARATOR.join(
            "".join(
                c
                for c in catalog_id.lower()
                if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS
            )
            for catalog_id in new_catalog_path_list
        )
        index += new_catalog_path_list
    else:
        index += "*"
    return index


def index_collections_by_catalog_id(catalog_path_list: List[str]) -> str:
    """
    Translate a catalog id into an Elasticsearch index name.

    Args:
        super_catalog_id (str): The super_catalog id to translate into an index name.
        catalog_id (str): The catalog id to translate into an index name.

    Returns:
        str: The index name derived from the provided catalog ids.
    """
    new_catalog_path_list = catalog_path_list.copy()
    new_catalog_path_list.reverse()
    output_index = f"{COLLECTIONS_INDEX_PREFIX}{CATALOG_SEPARATOR.join(''.join(c for c in catalog_id.lower() if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS) for catalog_id in new_catalog_path_list)}"
    return output_index


def index_catalogs_by_catalog_id(catalog_path_list: Optional[List[str]] = None) -> str:
    """
    Translate a catalog id into an Elasticsearch index name.

    Args:
        catalog_path (list[str]): The catalog path to translate into an index name.

    Returns:
        str: The index name derived from the provided catalog ids.
    """
    if catalog_path_list:
        # want return to be catalogs_lower-catalog_upper-catalog_super-catalog
        new_catalog_path_list = catalog_path_list.copy()
        new_catalog_path_list.reverse()
        return f"{CATALOGS_INDEX_PREFIX}{CATALOG_SEPARATOR.join(''.join(c for c in catalog_id.lower() if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS) for catalog_id in new_catalog_path_list)}"
    # Potentially may only want top-level catalogs, so need to add BASE to index here
    return f"{CATALOGS_INDEX_PREFIX}*"


def indices(
    collection_ids: Optional[List[str]] = None,
    catalog_paths: Optional[List[List[str]]] = None,
) -> str:
    """
    Get a comma-separated string of item index names for a given list of collection and catalog ids.

    Args:
        collection_ids: A list of collection ids.
        catalog_paths: A list of catalog paths.

    Returns:
        A string of comma-separated item index names. If `collection_ids` is None, returns the default item indices.
    """
    if not (collection_ids or catalog_paths):
        return ITEM_INDICES
    if not collection_ids:
        collection_ids = [None]
    if not catalog_paths:
        raise Exception(
            "To identify collections, you must identify the containing catalog path."
        )
    return ",".join(
        [
            index_by_collection_id(collection_id=coll, catalog_path_list=cat_path)
            for coll in collection_ids
            for cat_path in catalog_paths
        ]
    )


def collection_indices(catalog_paths: Optional[List[List[str]]] = None) -> str:
    """
    Get a comma-separated string of index names for a given list of catalog ids.

    Args:
        catalog_ids: A list of catalog paths.

    Returns:
        A string of comma-separated index names. If `catalog_ids` is None, returns the default collection indices.
    """
    ## If neither provided, return index for all collections
    if not catalog_paths:
        return COLLECTION_INDICES
    return ",".join(
        [
            index_collections_by_catalog_id(catalog_path_list=cat_path)
            for cat_path in catalog_paths
        ]
    )


def catalog_indices(catalog_paths: Optional[List[List[str]]] = None) -> str:
    """
    Get a comma-separated string of index names for a given list of catalog ids.

    Args:
        catalog_ids: A list of catalog paths.

    Returns:
        A string of comma-separated index names. If `catalog_ids` is None, returns the default collection indices.
    """
    ## If neither provided, return index for all collections
    if not catalog_paths:
        return CATALOG_INDICES
    return ",".join(
        [
            index_catalogs_by_catalog_id(catalog_path_list=cat_path)
            for cat_path in catalog_paths
        ]
    )


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
            "index_patterns": [f"{CATALOGS_INDEX_PREFIX}*"],
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


async def create_item_index(collection_id: str, catalog_path_list: List[str]):
    """
    Create the index for Items. The settings of the index template will be used implicitly.

    Args:
        collection_id (str): Collection identifier.
        catalog_path (str): Catalog identifier, including path for nested catalogs

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client
    index_name = index_by_collection_id(
        collection_id=collection_id, catalog_path_list=catalog_path_list
    )

    await client.options(ignore_status=400).indices.create(
        index=f"{index_name}-000001",
        aliases={index_name: {}},
    )
    await client.close()


async def delete_item_index(collection_id: str, catalog_path_list: List[str]):
    """Delete the index for items in a collection, specifying the catalog and top-level catalog.

    Args:
        collection_id (str): The ID of the collection whose items index will be deleted.
        catalog_path (List[str]): The path for the catalog whose items index will be deleted.
        super_catalog_id (str): The ID of the top-level catalog whose items index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_by_collection_id(
        collection_id=collection_id, catalog_path_list=catalog_path_list
    )
    resolved = await client.indices.resolve_index(name=name)
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name)
    await client.close()


async def delete_collection_index(catalog_path_list: List[str]):
    """Delete the index for collections in a catalog, specifying the top-level catalog.

    Args:
        catalog_id (str): The ID of the catalog whose collections index will be deleted.
        super_catalog_id (str): The ID of the top-level catalog whose collections index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_collections_by_catalog_id(catalog_path_list=catalog_path_list)
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
            f"Catalog path {'/'.join(catalog_path_list)} does not have any associated collections."
        )


async def delete_catalog_index(catalog_path_list: List[str]):
    """Delete the index for collections in a catalog, specifying the top-level catalog.

    Args:
        catalog_path_list (List[str]): The ID of the catalog whose collections index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)
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
            f"Catalog path {'/'.join(catalog_path_list)} does not have any associated catalogs."
        )


def mk_item_id(item_id: str, collection_id: str, catalog_path_list: List[str]):
    """Create the document id for an Item in Elasticsearch.

    Args:
        item_id (str): The id of the Item.
        collection_id (str): The id of the Collection that the Item belongs to.
        catalog_id (str): The id of the Catalog that the Collection belongs to.
        super_catalog_id (str): The id of the Top-level Catalog that the Collection belongs to.

    Returns:
        str: The document id for the Item, combining the Item id, the Collection id, the Catalog id and top-level Catalog id, separated by `|` characters.
    """
    # Instead of indexing with all attributes, we can use the items_id and specify the attributes in the index containing the document instead
    # return f"{item_id}|{collection_id}|{catalog_id}|{super_catalog_id}"
    return item_id


def mk_collection_id(collection_id: str, catalog_path_list: List[str]):
    """Create the document id for a collection in Elasticsearch.

    Args:
        collection_id (str): The id of the Collection.
        catalog_id (str): The id of the Catalog that the Collection belongs to.
        super_catalog_id (str): The id of the Top-level Catalog that the Collection belongs to.

    Returns:
        str: The document id for the Collection, combining the Collection id, the Catalog id and Top-level Catalog id, separated by a `|` character.
    """
    # Instead of indexing with all attributes, we can use the collection_id and specify the attributes in the index containing the document instead
    # return f"{collection_id}|{catalog_id}|{super_catalog_id}"
    return collection_id


def mk_actions(
    catalog_path_list: List[str], collection_id: str, processed_items: List[Item]
):
    """Create Elasticsearch bulk actions for a list of processed items.

    Args:
        catalog_path_list (List[str]): The identifier for the catalog the items belong to.
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
                collection_id=collection_id, catalog_path_list=catalog_path_list
            ),
            "_id": mk_item_id(
                item_id=item["id"],
                collection_id=item["collection"],
                catalog_path_list=catalog_path_list,
            ),
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
        This goes across all catalogs and top-level catalogs.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            base_url (str): The base URL used to create the item's self URL.

        Returns:
            A tuple of (collections, next pagination token if any).
        """

        search_after = None
        if token:
            search_after = [token]

        # Logic to ensure next token only returned when further results are available
        max_result_window = stac_fastapi.types.search.Limit.le
        size_limit = min(limit + 1, max_result_window)

        response = await self.client.search(
            index=f"{COLLECTIONS_INDEX_PREFIX}*",
            body={
                "sort": [{"id": {"order": "asc"}}],
                "size": size_limit,
                "search_after": search_after,
            },
        )

        collections = []
        hit_tokens = []
        hits = response["hits"]["hits"]
        for hit in hits:
            catalog_path = hit["_index"].split("_", 1)[1]
            catalog_path_list = catalog_path.split(CATALOG_SEPARATOR)
            catalog_path_list.reverse()
            catalog_path = "/".join(catalog_path_list)
            collections.append(
                self.collection_serializer.db_to_stac(
                    collection=hit["_source"],
                    base_url=base_url,
                    catalog_path=catalog_path,
                )
            )
            if hit.get("sort"):
                hit_token = hit["sort"][0]
                hit_tokens.append(hit_token)
            else:
                hit_tokens.append(None)

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (hits[limit - 1].get("sort")):
                next_token = hits[limit - 1]["sort"][0]

        return collections, next_token, hit_tokens

    async def get_catalog_collections(
        self, catalog_path: str, token: Optional[str], limit: int, base_url: str
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all collections in a catalog from Elasticsearch, supporting pagination.

        Args:
            catalog_path (str): The path to catalog to search.
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            base_url (str): The base URL used to create the item's self URL.

        Returns:
            A tuple of (collections, next pagination token if any).
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        search_after = None
        if token:
            search_after = [token]

        # Logic to ensure next token only returned when further results are available
        max_result_window = stac_fastapi.types.search.Limit.le
        size_limit = min(limit + 1, max_result_window)

        await self.check_catalog_exists(catalog_path_list=catalog_path_list)

        index_param = collection_indices(catalog_paths=[catalog_path_list])

        try:
            response = await self.client.search(
                index=index_param,
                body={
                    "sort": [{"id": {"order": "asc"}}],
                    "size": size_limit,
                    "search_after": search_after,
                },
            )
        except exceptions.NotFoundError:
            # No collections underneath this catalog
            response = None
            collections = []
            hits = []
            hit_tokens = []

        if response:
            collections = []
            hit_tokens = []
            hits = response["hits"]["hits"]
            for hit in hits:
                catalog_path = hit["_index"].split("_", 1)[1]
                catalog_path_list = catalog_path.split(CATALOG_SEPARATOR)
                catalog_path_list.reverse()
                catalog_path = "/".join(catalog_path_list)
                collections.append(
                    self.collection_serializer.db_to_stac(
                        collection=hit["_source"],
                        base_url=base_url,
                        catalog_path=catalog_path,
                    )
                )
                if hit.get("sort"):
                    hit_token = hit["sort"][0]
                    hit_tokens.append(hit_token)
                else:
                    hit_tokens.append(None)

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (hits[limit - 1].get("sort")):
                next_token = hits[limit - 1]["sort"][0]

        return collections, next_token, hit_tokens

    async def get_all_catalogs(
        self,
        token: Optional[str],
        limit: Optional[int],
        base_url: str,
        user_index: int,
        catalog_path: Optional[str] = None,
        conformance_classes: list = [],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all catalogs from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            base_url (str): The base URL used to create the required links.
            catalog_path (Optional[str]): The parent catalog in which to search (search all top-level catalogs if blank).
            conformance_classes (list): The list of conformance classes to include in the catalog.

        Returns:
            A tuple of (catalogs, next pagination token if any).
        """

        if catalog_path:
            # Create list of nested catalog ids
            catalog_path_list = catalog_path.split("/")
        else:
            catalog_path_list = None

        params_index = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)

        search_after = None
        if token:
            search_after = [token]

        # Logic to ensure next token only returned when further results are available
        max_result_window = stac_fastapi.types.search.Limit.le
        size_limit = min(limit + 1, max_result_window)

        # Get all contained catalogs
        try:
            response = await self.client.search(
                index=params_index,
                body={
                    "sort": [{"id": {"order": "asc"}}],
                    "size": size_limit,
                    "search_after": search_after,
                },
            )
            hits = response["hits"]["hits"]
        except exceptions.NotFoundError:
            response = None
            catalogs = []
            hits = []

        # Construct async tasks
        catalog_indices_list = []
        for hit in hits:
            # Construct required catalog indices
            try:
                catalog_id = hit["_id"]
                catalog_index = hit["_index"].split("_", 1)[1]
                catalog_index_list = catalog_index.split(CATALOG_SEPARATOR)
                catalog_index_list.reverse()
                catalog_index_list.append(catalog_id)
                catalog_indices_list.append(catalog_index_list)
            except IndexError:
                catalog_index_list = [catalog_id]
                catalog_indices_list.append(catalog_index_list)

        sub_catalogs_results = await asyncio.gather(
            *[
                self.client.search(
                    index=index_catalogs_by_catalog_id(
                        catalog_path_list=catalog_index_list
                    ),
                    body={
                        "sort": [{"id": {"order": "asc"}}],
                    },
                )
                for catalog_index_list in catalog_indices_list
            ],
            return_exceptions=True,
        )
        collection_results = await asyncio.gather(
            *[
                self.client.search(
                    index=index_collections_by_catalog_id(
                        catalog_path_list=catalog_index_list
                    ),
                    body={
                        "sort": [{"id": {"order": "asc"}}],
                    },
                )
                for catalog_index_list in catalog_indices_list
            ],
            return_exceptions=True,
        )

        sub_catalogs_responses = [
            (
                sub_catalogs_result["hits"]["hits"]
                if not isinstance(sub_catalogs_result, Exception)
                else [{"_source": None}]
            )
            for sub_catalogs_result in sub_catalogs_results
        ]
        collection_responses = [
            (
                collection_result["hits"]["hits"]
                if not isinstance(collection_result, Exception)
                else [{"_source": None}]
            )
            for collection_result in collection_results
        ]

        child_data = list(zip(sub_catalogs_responses, collection_responses))

        catalogs = []
        hit_tokens = []
        for i, hit in enumerate(hits):
            catalog_path = hit["_index"].split("_", 1)[1]
            catalog_path_list = catalog_path.split(CATALOG_SEPARATOR)
            catalog_path_list.reverse()
            catalog_path = "/".join(catalog_path_list)
            sub_data_catalogs_and_collections = child_data[i]
            # Extract sub-catalogs
            sub_catalogs = []
            for catalog in sub_data_catalogs_and_collections[0]:
                if catalog["_source"]:
                    if int(catalog["_source"]["access_control"][-1]) or int(
                        catalog["_source"]["access_control"][user_index]
                    ):
                        sub_catalogs.append(catalog["_source"])
            # Extract collections
            collections = []
            for collection in sub_data_catalogs_and_collections[1]:
                if collection["_source"]:
                    if int(collection["_source"]["access_control"][-1]) or int(
                        collection["_source"]["access_control"][user_index]
                    ):
                        collections.append(collection["_source"])
            catalogs.append(
                self.catalog_serializer.db_to_stac(
                    catalog_path=catalog_path,
                    catalog=hit["_source"],
                    base_url=base_url,
                    sub_catalogs=sub_catalogs,
                    collections=collections,
                    conformance_classes=conformance_classes,
                )
            )
            if hit.get("sort"):
                hit_token = hit["sort"][0]
                hit_tokens.append(hit_token)
            else:
                hit_tokens.append(None)

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (hits[limit - 1].get("sort")):
                next_token = hits[limit - 1]["sort"][0]

        return catalogs, next_token, hit_tokens

    async def get_catalog_subcatalogs(
        self,
        token: Optional[str],
        limit: int,
        base_url: str,
        catalog_path: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all catalogs in a catalog from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            base_url (str): The base URL used to create the item's self URL.
            catalog_path (Optional[str]): The parent catalog in which to search (search all top-level catalogs if blank).

        Returns:
            A tuple of (collections, next pagination token if any).
        """

        if catalog_path:
            # Create list of nested catalog ids
            catalog_path_list = catalog_path.split("/")
            if len(catalog_path_list) > 1:
                parent_catalog_path = "/".join(catalog_path_list[:-1])
            else:
                parent_catalog_path = None
            index_param = index_catalogs_by_catalog_id(
                catalog_path_list=catalog_path_list
            )
            await self.check_catalog_exists(catalog_path_list=catalog_path_list)
        else:
            # This is a BASE catalog so index using top-level index
            index_param = ROOT_CATALOGS_INDEX
            parent_catalog_path = None

        search_after = None
        if token:
            search_after = [token]

        # Logic to ensure next token only returned when further results are available
        max_result_window = stac_fastapi.types.search.Limit.le
        size_limit = min(limit + 1, max_result_window)

        try:
            response = await self.client.search(
                index=index_param,
                body={
                    # "sort": [{"id": {"order": "asc"}}],
                    "size": size_limit,
                    "search_after": search_after,
                },
            )
        except exceptions.NotFoundError:
            response = None
            catalogs = []
            hits = []

        if response:
            hits = response["hits"]["hits"]
            catalogs = [
                self.catalog_serializer.db_to_stac(
                    catalog_path=parent_catalog_path,
                    catalog=hit["_source"],
                    base_url=base_url,
                )
                for hit in hits
            ]

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (hits[limit - 1].get("sort")):
                next_token = hits[limit - 1]["sort"][0]

        return catalogs, next_token

    async def get_one_item(
        self, catalog_path: str, collection_id: str, item_id: str
    ) -> Dict:
        """Retrieve a single item from the database.

        Args:
            catalog_path (Optional[str]): The parent catalog in which to search for the collection.
            collection_id (str): The id of the Collection that the Item belongs to.
            item_id (str): The id of the Item.

        Returns:
            item (Dict): A dictionary containing the source data for the Item.

        Raises:
            NotFoundError: If the specified Item does not exist in the Collection in the specified Catalog.

        Notes:
            The Item is retrieved from the Elasticsearch database using the `client.get` method,
            with the index for the Collection in the Catalog as the target index and the combined `mk_item_id` as the document id.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")
        try:
            item = await self.client.get(
                index=index_by_collection_id(
                    collection_id=collection_id, catalog_path_list=catalog_path_list
                ),
                id=mk_item_id(
                    item_id=item_id,
                    collection_id=collection_id,
                    catalog_path_list=catalog_path_list,
                ),
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist in Collection {collection_id} in Catalog path {'/'.join(catalog_path_list)}."
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
    def apply_keyword_collections_filter(search: Search, q: List[str]):
        q_str = ",".join(q)
        should = []
        should.extend(
            [
                Q(
                    "bool",
                    filter=[
                        Q(
                            "match",
                            title={"query": q_str},
                        ),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q(
                            "match",
                            description={"query": q_str},
                        ),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q("terms", keywords=q),
                    ],
                ),
            ]
        )

        search = search.query(Q("bool", filter=[Q("bool", should=should)]))

        return search

    @staticmethod
    def apply_keyword_discovery_filter(search: Search, q: List[str]):
        q_str = ",".join(q)
        # Construct search query for keywords
        # For catalogues and collections this searches title and description
        # For collections this also searches keywords
        should_filter = []
        should_filter.extend(
            [
                Q(
                    "bool",
                    filter=[
                        Q("match", title=q_str),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q("match", description=q_str),
                    ],
                ),
                Q(
                    "bool",
                    filter=[
                        Q("terms", keywords=q),
                    ],
                ),
            ]
        )
        # The following query is then used to score the returned results
        # Calculate scoring for keyword field
        should_query = [{"term": {"keywords": keyword}} for keyword in q]
        # Calculate scoring for title and description fields
        should_query.extend(
            [
                {
                    "multi_match": {
                        "query": q_str,
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
        limit: int,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        catalog_paths: Optional[List[str]],
        collection_ids: Optional[List[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Execute a search query with limit and other optional parameters.

        Args:
            search (Search): The search query to be executed.
            limit (int): The maximum number of results to be returned.
            token (Optional[str]): The token used to return the next set of results.
            sort (Optional[Dict[str, Dict[str, str]]]): Specifies how the results should be sorted.
            catalog_paths (str): The catalog paths to search
            collection_ids (Optional[List[str]]): The collection ids to search, only used when catalog_paths (single path) is provided.
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

        if catalog_paths:
            catalog_paths_list = [
                catalog_path.split("/") for catalog_path in catalog_paths
            ]

        search_after = None
        if token:
            search_after = urlsafe_b64decode(token.encode()).decode().split(",")

        query = search.query.to_dict() if search.query else None

        index_param = f"{ITEMS_INDEX_PREFIX}*"

        # Logic to ensure next token only returned when further results are available
        max_result_window = stac_fastapi.types.search.Limit.le
        size_limit = min(limit + 1, max_result_window)

        if collection_ids and catalog_paths:
            index_param = indices(
                collection_ids=collection_ids, catalog_paths=catalog_paths_list
            )
        elif catalog_paths:
            index_param = indices(catalog_paths=catalog_paths_list)

        search_task = asyncio.create_task(
            self.client.search(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=sort or DEFAULT_SORT,
                search_after=search_after,
                size=size_limit,
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
            raise NotFoundError(
                f"Either Collections '{','.join(collection_ids)}' or Catalog paths '{','.join(catalog_paths)}' do not exist"
            )

        hits = es_response["hits"]["hits"]

        # Need to identify catalog for each item
        items = []
        hit_tokens = []
        for hit in hits[:limit]:
            item_catalog_path = hit["_index"].split(GROUP_SEPARATOR, 1)[1]
            item_catalog_path_list = item_catalog_path.split(CATALOG_SEPARATOR)
            item_catalog_path_list.reverse()
            item_catalog_path = "/".join(item_catalog_path_list)
            items.append((hit["_source"], item_catalog_path))
            if sort_array := hit.get("sort"):
                hit_token = urlsafe_b64encode(
                    ",".join([str(x) for x in sort_array]).encode()
                ).decode()
                hit_tokens.append(hit_token)
            else:
                hit_tokens.append(None)

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(
                    ",".join([str(x) for x in sort_array]).encode()
                ).decode()

        # (1) count should not block returning results, so don't wait for it to be done
        # (2) don't cancel the task so that it will populate the ES cache for subsequent counts
        matched = (
            es_response["hits"]["total"]["value"]
            if es_response["hits"]["total"]["relation"] == "eq"
            else None
        )
        if count_task.done():
            try:
                matched = count_task.result().get("count")
            except Exception as e:
                logger.error(f"Count task failed: {e}")

        return items, matched, next_token, hit_tokens

    async def execute_collection_search(
        self,
        search: Search,
        limit: int,
        base_url: str,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
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

        # Logic to ensure next token only returned when further results are available
        max_result_window = stac_fastapi.types.search.Limit.le
        size_limit = min(limit + 1, max_result_window)

        search_task = asyncio.create_task(
            self.client.search(
                index=f"{COLLECTIONS_INDEX_PREFIX}*",
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=sort or DEFAULT_COLLECTIONS_SORT,
                search_after=search_after,
                size=size_limit,
            )
        )

        count_task = asyncio.create_task(
            self.client.count(
                index=f"{COLLECTIONS_INDEX_PREFIX}*",
                ignore_unavailable=ignore_unavailable,
                body=search.to_dict(count=True),
            )
        )

        es_response = await search_task

        collections = []
        hit_tokens = []
        hits = es_response["hits"]["hits"]
        for hit in hits:
            catalog_path = hit["_index"].split("_", 1)[1]
            catalog_path_list = catalog_path.split(CATALOG_SEPARATOR)
            catalog_path_list.reverse()
            catalog_path = "/".join(catalog_path_list)
            collections.append(
                self.collection_serializer.db_to_stac(
                    collection=hit["_source"],
                    base_url=base_url,
                    catalog_path=catalog_path,
                )
            )
            if sort_array := hit.get("sort"):
                hit_token = urlsafe_b64encode(
                    ",".join([str(x) for x in sort_array]).encode()
                ).decode()
                hit_tokens.append(hit_token)
            else:
                hit_tokens.append(None)

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(
                    ",".join([str(x) for x in sort_array]).encode()
                ).decode()

        # (1) count should not block returning results, so don't wait for it to be done
        # (2) don't cancel the task so that it will populate the ES cache for subsequent counts
        matched = (
            es_response["hits"]["total"]["value"]
            if es_response["hits"]["total"]["relation"] == "eq"
            else None
        )
        if count_task.done():
            try:
                matched = count_task.result().get("count")
            except Exception as e:
                logger.error(f"Count task failed: {e}")

        return collections, matched, next_token, hit_tokens

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str, catalog_path: str):
        """Database logic to check if a collection exists."""

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        index = index_collections_by_catalog_id(catalog_path_list=catalog_path_list)
        if not await self.client.exists(index=index, id=collection_id):
            raise NotFoundError(
                f"Collection {collection_id} in catalog {catalog_id} at path {catalog_path} does not exist"
            )

    async def check_catalog_exists(self, catalog_path_list: List[str] = None):
        """Database logic to check if a catalog exists."""

        catalog_id = catalog_path_list[-1]
        if len(catalog_path_list) > 1:
            search_catalog_path_list = catalog_path_list[:-1]
            index_param = index_catalogs_by_catalog_id(
                catalog_path_list=search_catalog_path_list
            )
        else:
            index_param = ROOT_CATALOGS_INDEX

        # Check if that catalog exists in the correct path
        if not await self.client.exists(index=index_param, id=catalog_id):
            raise NotFoundError(
                f"Catalog {catalog_id} does not exist at {'/'.join(catalog_path_list[:-1]) if len(catalog_path_list) > 1 else 'top-level'}"
            )

    async def prep_create_item(
        self, catalog_path: str, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Preps an item for insertion into the database.

        Args:
            catalog_path (str) : The path to the catalog into which the item will be inserted.
            item (Item): The item to be prepped for insertion.
            base_url (str): The base URL used to create the item's self URL.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The prepped item.

        Raises:
            ConflictError: If the item already exists in the database.

        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        await self.check_collection_exists(
            collection_id=item["collection"], catalog_path=catalog_path
        )

        if not exist_ok and await self.client.exists(
            index=index_by_collection_id(
                collection_id=item["collection"], catalog_path_list=catalog_path_list
            ),
            id=mk_item_id(
                item_id=item["id"],
                collection_id=item["collection"],
                catalog_path_list=catalog_path_list,
            ),
        ):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} in catalog {catalog_id} at path {catalog_path} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    def sync_prep_create_item(
        self, catalog_path: str, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Prepare an item for insertion into the database.

        This method performs pre-insertion preparation on the given `item`,
        such as checking if the collection the item belongs to exists,
        and optionally verifying that an item with the same ID does not already exist in the database.

        Args:
            catalog_path (str) : The path to the catalog into which the item will be inserted.
            item (Item): The item to be inserted into the database.
            base_url (str): The base URL used for constructing URLs for the item.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The item after preparation is done.

        Raises:
            NotFoundError: If the collection that the item belongs to does not exist in the database.
            ConflictError: If an item with the same ID already exists in the collection.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        item_id = item["id"]
        collection_id = item["collection"]
        index = (index_collections_by_catalog_id(catalog_path_list=catalog_path_list),)
        if not self.sync_client.exists(index=index, id=collection_id):
            raise NotFoundError(
                f"Collection {collection_id} does not exist in catalog {catalog_id} at path {catalog_path}"
            )

        if not exist_ok and self.sync_client.exists(
            index=index_by_collection_id(
                collection_id=collection_id, catalog_path_list=catalog_path_list
            ),
            id=mk_item_id(
                item_id=item_id,
                collection_id=collection_id,
                catalog_path_list=catalog_path_list,
            ),
        ):
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} in catalog {catalog_id} at path {catalog_path} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    async def create_item(self, catalog_path: str, item: Item, refresh: bool = False):
        """Database logic for creating one item.

        Args:
            catalog_path (str) : The path to the catalog into which the item will be inserted.
            item (Item): The item to be created.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to False.

        Raises:
            ConflictError: If the item already exists in the database.

        Returns:
            None
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        # todo: check if collection exists, but cache
        item_id = item["id"]
        collection_id = item["collection"]
        es_resp = await self.client.index(
            index=index_by_collection_id(
                collection_id=collection_id, catalog_path_list=catalog_path_list
            ),
            id=mk_item_id(
                item_id=item_id,
                collection_id=collection_id,
                catalog_path_list=catalog_path_list,
            ),
            document=item,
            refresh=refresh,
        )

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} in catalog {catalog_id} at path {catalog_path} already exists"
            )

    async def delete_item(
        self, item_id: str, collection_id: str, catalog_path: str, refresh: bool = False
    ):
        """Delete a single item from the database.

        Args:
            item_id (str): The id of the Item to be deleted.
            collection_id (str): The id of the Collection that the Item belongs to.
            catalog_path (str) : The path of the catalog that the Collectioj belongs to.
            refresh (bool, optional): Whether to refresh the index after the deletion. Default is False.

        Raises:
            NotFoundError: If the Item does not exist in the database.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        try:
            await self.client.delete(
                index=index_by_collection_id(
                    collection_id=collection_id, catalog_path_list=catalog_path_list
                ),
                id=mk_item_id(
                    item_id=item_id,
                    collection_id=collection_id,
                    catalog_path_list=catalog_path_list,
                ),
                refresh=refresh,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} in catalog {catalog_id} at path {catalog_path} not found"
            )

    async def prep_create_collection(
        self,
        catalog_path: str,
        collection: Collection,
        base_url: str,
        exist_ok: bool = False,
    ) -> Item:
        """
        Preps a collection for insertion into the database.

        Args:
            catalog_path (str): The parent catalog into which the Collection will be inserted.
            collection (Collection): The collection to be prepped for insertion.
            base_url (str): The base URL used to create the collection's self URL.
            exist_ok (bool): Indicates whether the collection can exist already.

        Returns:
            Collection: The prepped item.

        Raises:
            ConflictError: If the collection already exists in the catalog in the database.

        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        await self.check_catalog_exists(catalog_path_list=catalog_path_list)
        if not exist_ok and await self.client.exists(
            index=index_collections_by_catalog_id(catalog_path_list=catalog_path_list),
            id=collection["id"],
        ):
            raise ConflictError(
                f"Collection {collection['id']} in catalog {catalog_id} at {'/'.join(catalog_path_list[:-1])} already exists"
            )

        return self.collection_serializer.stac_to_db(collection, base_url)

    def sync_prep_create_collection(
        self,
        catalog_path: str,
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
            catalog_path (str): The parent catalog into which the Collection will be inserted.
            collection (Collection): The collection to be prepped for insertion.
            base_url (str): The base URL used for constructing URLs for the item.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The item after preparation is done.

        Raises:
            NotFoundError: If the collection that the item belongs to does not exist in the database.
            ConflictError: If a collection with the same ID already exists in the catalog.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        collection_id = collection["id"]
        index = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)
        if not self.sync_client.exists(index=index, id=catalog_id):
            raise NotFoundError(
                f"Catalog {catalog_id} at path {'/'.join(catalog_path_list[:-1])} does not exist"
            )

        if not exist_ok and self.sync_client.exists(
            index=index_collections_by_catalog_id(
                catalog_path_list=catalog_path_list, catalog_id=catalog_id
            ),
            id=collection_id,
        ):
            raise ConflictError(
                f"Collection {collection_id} in catalog {catalog_id} at path {'/'.join(catalog_path_list[:-1])} already exists"
            )

        return self.collection_serializer.stac_to_db(collection, base_url)

    async def update_parent_access_control(
        self,
        catalog_path_list: List[str],
        new_bitstring: str,
        collection_id: Optional[str] = None,
    ):
        """Update the access control bitstring for the parent catalog."""
        # Check if updating for collection or catalog
        # Currently nesting collections is not supported

        if collection_id:
            # Get current bitstring
            collection = await self.client.get(
                index=index_collections_by_catalog_id(
                    catalog_path_list=catalog_path_list
                ),
                id=collection_id,
            )
            return
            # TODO: Get all other child collections for this collection and compute OR bitwise operation
            # e.g. using self.get_collection_collections

        elif len(catalog_path_list) > 1:
            # Update the access control bitstring for the parent catalog
            parent_catalog_path_list = catalog_path_list[:-1]
            catalog_id = catalog_path_list[-1]
            catalog = await self.client.get(
                index=index_catalogs_by_catalog_id(
                    catalog_path_list=parent_catalog_path_list
                ),
                id=catalog_id,
            )
            old_bitstring = catalog["_source"]["access_control"]
            old_bitstring_int = int(
                old_bitstring, 2
            )  # Convert binary string to integer
            new_bitstring_int = int(
                new_bitstring, 2
            )  # Convert binary string to integer
            new_bitstring_int = old_bitstring_int | new_bitstring_int

            # Define the fixed length for the bitstrings
            fixed_length = max(len(old_bitstring), len(new_bitstring))

            new_bitstring = bin(new_bitstring_int)[2:].zfill(fixed_length)

            annotation = {"access_control": new_bitstring}
            _ = await self.client.update(
                index=index_catalogs_by_catalog_id(
                    catalog_path_list=parent_catalog_path_list
                ),
                id=catalog_id,
                body={"doc": annotation},
                refresh=True,
            )

            await self.update_parent_access_control(
                catalog_path_list=catalog_path_list[:-1], new_bitstring=new_bitstring
            )

        elif len(catalog_path_list) == 1:
            # Catalog path consists of a single catalog
            catalog_id = catalog_path_list[-1]
            catalog = await self.client.get(
                index=ROOT_CATALOGS_INDEX,
                id=catalog_id,
            )

            old_bitstring = catalog["_source"]["access_control"]
            old_bitstring_int = int(
                old_bitstring, 2
            )  # Convert binary string to integer
            new_bitstring_int = int(
                new_bitstring, 2
            )  # Convert binary string to integer
            new_bitstring_int = old_bitstring_int | new_bitstring_int

            # Define the fixed length for the bitstrings
            fixed_length = max(len(old_bitstring), len(new_bitstring))

            new_bitstring = bin(new_bitstring_int)[2:].zfill(fixed_length)

            annotation = {"access_control": new_bitstring}
            _ = await self.client.update(
                index=ROOT_CATALOGS_INDEX,
                id=catalog_id,
                body={"doc": annotation},
                refresh=True,
            )

    async def create_collection(
        self,
        catalog_path: str,
        collection: Collection,
        access_control: str,
        refresh: bool = False,
    ):
        """Database logic for creating one item.

        Args:
            catalog_path (str): The parent catalog into which the Collection will be inserted.
            collection (Collection): The collection to be created.
            access_control (int): Integer bitstring defining user access.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to False.

        Raises:
            ConflictError: If the collection already exists in the catalog in the database.

        Returns:
            None
        """
        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        # todo: check if collection exists, but cache
        collection_id = collection["id"]
        es_resp = await self.client.index(
            index=index_collections_by_catalog_id(catalog_path_list=catalog_path_list),
            id=collection_id,
            document=collection,
            refresh=refresh,
        )

        # Record access control bitstring for this document
        annotation = {"access_control": access_control}
        await self.client.update(
            index=index_collections_by_catalog_id(catalog_path_list=catalog_path_list),
            id=collection_id,
            body={"doc": annotation},
            refresh=refresh,
        )

        collection = await self.client.get(
            index=index_collections_by_catalog_id(catalog_path_list=catalog_path_list),
            id=collection_id,
        )

        # Update the access control bitstring for the parent catalog
        await self.update_parent_access_control(
            catalog_path_list=catalog_path_list, new_bitstring=access_control
        )

        collection = await self.client.get(
            index=index_collections_by_catalog_id(catalog_path_list=catalog_path_list),
            id=collection_id,
        )

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Collection {collection_id} in catalog {catalog_id} at path {'/'.join(catalog_path_list[:-1])} already exists"
            )

    async def find_collection(
        self, catalog_path: str, collection_id: str
    ) -> Collection:
        """Find and return a collection from the database.

        Args:
            self: The instance of the object calling this function.
            catalog_path (str): The parent catalog in which to search.
            collection_id (str): The ID of the collection to be found.

        Returns:
            Collection: The found collection, represented as a `Collection` object.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the given catalog in the database.

        Notes:
            This function searches for a collection in the database using the specified `collection_id` and returns the found
            collection as a `Collection` object. If the collection is not found, a `NotFoundError` is raised.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        full_collection_id = collection_id
        try:
            collection = await self.client.get(
                index=index_collections_by_catalog_id(
                    catalog_path_list=catalog_path_list
                ),
                id=full_collection_id,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Collection {collection_id} in catalog {catalog_id} at path {'/'.join(catalog_path_list[:-1])} not found"
            )

        return collection["_source"]

    async def update_collection(
        self,
        catalog_path: str,
        collection_id: str,
        collection: Collection,
        refresh: bool = False,
    ):
        """Update a collection from the database.

        Args:
            self: The instance of the object calling this function.
            catalog_path (str): The path of the catalog containing the collection to be updated, including parent catalogs, e.g. parentCat/cat
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

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        current_collection = await self.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )

        # Access current access control bitstring, as this will remain identical
        access_control = current_collection["access_control"]

        if collection_id != collection["id"]:
            await self.create_collection(
                catalog_path=catalog_path,
                collection=collection,
                refresh=refresh,
                access_control=access_control,
            )
            dest_index = index_by_collection_id(
                collection_id=collection["id"], catalog_path_list=catalog_path_list
            )
            source_index = index_by_collection_id(
                collection_id=collection_id, catalog_path_list=catalog_path_list
            )
            try:
                await self.client.reindex(
                    body={
                        "dest": {"index": dest_index},
                        "source": {"index": source_index},
                        "script": {
                            "lang": "painless",
                            "source": f"""ctx._id = ctx._id.replace('{collection_id}', '{collection["id"]}'); ctx._source.collection = '{collection["id"]}' ;""",
                        },
                    },
                    wait_for_completion=True,
                    refresh=refresh,
                )
            except exceptions.NotFoundError:
                logger.error(
                    f"Collection {collection_id} in catalog {catalog_path} has no items, so reindexing is not possible, continuing as normal."
                )

            await self.delete_collection(
                catalog_path=catalog_path, collection_id=collection_id
            )

        else:
            collections_index = index_collections_by_catalog_id(
                catalog_path_list=catalog_path_list
            )
            await self.client.index(
                index=collections_index,
                id=collection_id,
                document=collection,
                refresh=refresh,
            )

            # Record access control bitstring for this document
            annotation = {"access_control": access_control}
            await self.client.update(
                index=index_collections_by_catalog_id(
                    catalog_path_list=catalog_path_list
                ),
                id=collection_id,
                body={"doc": annotation},
                refresh=refresh,
            )

    async def delete_collection(
        self, catalog_path: str, collection_id: str, refresh: bool = False
    ):
        """Delete a collection from the database.

        Parameters:
            self: The instance of the object calling this function.
            catalog_path (str): The parent catalog into in which to delete the Collection.
            collection_id (str): The ID of the collection to be deleted.
            refresh (bool): Whether to refresh the index after the deletion (default: False).

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.

        Notes:
            This function first verifies that the collection with the specified `collection_id` exists in the database, and then
            deletes the collection. If `refresh` is set to True, the index is refreshed after the deletion. Additionally, this
            function also calls `delete_item_index` to delete the index for the items in the collection.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        await self.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )
        await self.client.delete(
            index=index_collections_by_catalog_id(catalog_path_list=catalog_path_list),
            id=collection_id,
            refresh=refresh,
        )
        try:
            await delete_item_index(
                collection_id=collection_id, catalog_path_list=catalog_path_list
            )
        except exceptions.NotFoundError:
            logger.info(
                f"Collection {collection_id} in catalog {catalog_id} at path {catalog_path} has no items, so index does not exist and cannot be deleted, continuing as normal."
            )

    async def prep_create_catalog(
        self,
        catalog_path: Optional[str],
        catalog: Catalog,
        base_url: str,
        exist_ok: bool = False,
    ) -> Item:
        """
        Preps a catalog for insertion into the database.

        Args:
            catalog_path (Optional[str]) : The path to the top-level catalog into which the catalog will be inserted.
            catalog (Catalog): The catalog to be prepped for insertion.
            base_url (str): The base URL used to create the catalog's self URL.
            exist_ok (bool): Indicates whether the catalog can exist already.

        Returns:
            Catalog: The prepped item.

        Raises:
            NotFoundError: If the catalog that the item belongs to does not exist in the database.
            ConflictError: If the catalog already exists in the catalog in the database.

        """

        if catalog_path:
            # Create list of nested catalog ids
            catalog_path_list = catalog_path.split("/")
            index = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)
            await self.check_catalog_exists(catalog_path_list=catalog_path_list)
        else:
            catalog_path_list = None
            # Creating a new BASE catalog so index using top-level index
            index = ROOT_CATALOGS_INDEX

        if not exist_ok and await self.client.exists(
            index=index,
            id=catalog["id"],
        ):
            raise ConflictError(
                f"Catalog {catalog['id']} in catalog {'/'.join(catalog_path_list) if catalog_path else 'top-level'} already exists"
            )

        return self.catalog_serializer.stac_to_db(catalog, base_url)

    def sync_prep_create_catalog(
        self,
        catalog: Catalog,
        base_url: str,
        catalog_path: Optional[str] = None,
        exist_ok: bool = False,
    ) -> Item:
        """
        Prepare an item for insertion into the database.

        This method performs pre-insertion preparation on the given `catalog`,
        such as checking if the parent catalog the catalog belongs to exists,
        and optionally verifying that a catalog with the same ID does not already exist in the database.

        Args:
            catalog (Catalog): The catalog to be prepped for insertion.
            base_url (str): The base URL used for constructing URLs for the item.
            catalog_path (str) : The path to the catalog into which the catalog will be inserted. Default is None.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Catalog: The catalog after preparation is done.

        Raises:
            NotFoundError: If the catalog that the item belongs to does not exist in the database.
            ConflictError: If a catalog with the same ID already exists in the catalog.
        """

        catalog_id = catalog["id"]

        if catalog_path:
            # Create list of nested catalog ids
            catalog_path_list = catalog_path.split("/")
            index = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)
            # Check if parent catalog exists
            if not self.sync_client.exists(index=index, id=catalog_id):
                raise NotFoundError(
                    f"Catalog {catalog_id} at path {'/'.join(catalog_path_list)} does not exist"
                )
        else:
            # Creating a new BASE catalog so index using top-level index
            index = ROOT_CATALOGS_INDEX

        if not exist_ok and self.sync_client.exists(
            index=index,
            id=catalog_id,
        ):
            raise ConflictError(
                f"Catalog {catalog_id} in catalog {'/'.join(catalog_path_list) if catalog_path else 'top-level'} already exists"
            )

        return self.catalog_serializer.stac_to_db(catalog, base_url)

    async def create_catalog(
        self,
        catalog: Catalog,
        access_control: str,
        catalog_path: Optional[str] = None,
        refresh: bool = False,
    ):
        """Create a single catalog in the database.

        Args:
            catalog (Catalog): The Catalog object to be created.
            access_control (str): String bitstring defining user access.
            catalog_path (Optional[str]): The path to the parent catalog into which the new catalog will be inserted. Default is None.
            refresh (bool, optional): Whether to refresh the index after the creation. Default is False.

        Raises:
            ConflictError: If a Catalog with the same id already exists in the database.

        Notes:
            A new index is created for the catalog in the parent catalog using the `create_catalog_index` function.
        """
        catalog_id = catalog["id"]
        if catalog_path:
            # Create list of nested catalog ids
            catalog_path_list = catalog_path.split("/")
            index = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)
        else:
            catalog_path_list = []
            # Creating a new BASE catalog so index using top-level index
            index = ROOT_CATALOGS_INDEX

        if await self.client.exists(index=index, id=catalog_id):
            raise ConflictError(
                f"Catalog {catalog_id} already exists at path {catalog_path if catalog_path else 'Top-Level'}"
            )

        await self.client.index(
            index=index,
            id=catalog_id,
            document=catalog,
            refresh=refresh,
        )

        # Record access control bitstring for this document
        annotation = {"access_control": access_control}
        await self.client.update(
            index=index,
            id=catalog_id,
            refresh=refresh,
            body={"doc": annotation},
        )

        catalog = await self.client.get(
            index=index,
            id=catalog_id,
        )

        # Update the access control bitstring for the parent catalog
        if catalog_path:
            await self.update_parent_access_control(
                catalog_path_list=catalog_path_list, new_bitstring=access_control
            )

        catalog = await self.client.get(
            index=index,
            id=catalog_id,
        )

    async def find_catalog(self, catalog_path: str) -> Catalog:
        """Find and return a collection from the database.

        Args:
            self: The instance of the object calling this function.
            catalog_path (str): The path and id of the Catalog to be found.

        Returns:
            Catalog: The found catalog, represented as a `Catalog` object.

        Raises:
            NotFoundError: If the catalog with the given `catalog_path` is not found in the database.

        Notes:
            This function searches for a catalog in the database using the specified `catalog_path` and returns the found
            catalog as a `Catalog` object. If the catalog is not found, a `NotFoundError` is raised.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        # Handle case where we are looking at nested catalog
        if len(catalog_path_list) > 1:
            search_catalog_path_list = catalog_path_list[:-1]
            index = index_catalogs_by_catalog_id(
                catalog_path_list=search_catalog_path_list
            )
        # Handle case where we are looking at base catalog
        else:
            index = ROOT_CATALOGS_INDEX

        try:
            catalog = await self.client.get(index=index, id=catalog_id)
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Catalog {catalog_id} at {'path ' + '/'.join(catalog_path_list[:-1]) if len(catalog_path_list) > 1 else 'top-level'} not found"
            )

        return catalog["_source"]

    async def reindex_sub_catalogs(
        self, catalog_path: str, new_catalog_path: str, refresh: bool = False
    ):
        """Updates index for all catalogs in the given catalog.

        Args:
            self: The instance of the object calling this function.
            catalog_path (str): The path to the top-level catalog.
            new_catalog_path (str): The updated path to the top-level catalog.
            refresh (bool): Whether to refresh the index after the deletion (default: False).
        """

        # Create list of nested catalog ids for old catalog id
        catalog_path_list = catalog_path.split("/")

        # Create list of nested catalog ids for new catalog id
        new_catalog_path_list = new_catalog_path.split("/")

        # Get full set of catalogs in this catalog
        params_index = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)
        try:
            response = await self.client.search(
                index=params_index,
                body={
                    "sort": [{"id": {"order": "asc"}}],
                },
            )
            hits = response["hits"]["hits"]
        except exceptions.NotFoundError:
            hits = []
        # Reindex all contained catalogs
        for hit in hits:
            # Get sub-catalog id
            sub_catalog_id = hit["_id"]

            # Construct new sub-catalog path
            new_sub_catalog_path = f"{new_catalog_path}/{sub_catalog_id}"

            # Calculate sub-catalog path for found catalog
            sub_catalog_path = hit["_index"].split("_", 1)[1]
            sub_catalog_path_list = sub_catalog_path.split(CATALOG_SEPARATOR)
            sub_catalog_path_list.reverse()
            sub_catalog_path_list.append(sub_catalog_id)
            sub_catalog_path = "/".join(sub_catalog_path_list)

            # This is the current index for this catalog
            source_index = index_catalogs_by_catalog_id(
                catalog_path_list=catalog_path_list
            )

            # This is the updated index for this catalog
            dest_index = index_catalogs_by_catalog_id(
                catalog_path_list=new_catalog_path_list
            )
            try:
                await self.client.reindex(
                    body={
                        "dest": {"index": dest_index},
                        "source": {"index": source_index},
                    },
                    wait_for_completion=True,
                    refresh=refresh,
                )
            except exceptions.NotFoundError:
                logger.info(
                    f"Catalog {sub_catalog_id} at path {catalog_path} has no collections, so index does not exist and cannot be updated, continuing as normal."
                )
            next_sub_catalog_path_list = catalog_path_list.copy()
            next_sub_catalog_path_list.append(sub_catalog_id)
            next_sub_catalog_path = "/".join(next_sub_catalog_path_list)
            await self.reindex_sub_catalogs(
                catalog_path=next_sub_catalog_path,
                new_catalog_path=new_sub_catalog_path,
                refresh=refresh,
            )

        # Reindex collections in this catalog with new catalog_id
        source_index = index_collections_by_catalog_id(
            catalog_path_list=catalog_path_list
        )
        dest_index = index_collections_by_catalog_id(
            catalog_path_list=new_catalog_path_list
        )
        try:
            await self.client.reindex(
                body={
                    "dest": {"index": dest_index},
                    "source": {"index": source_index},
                },
                wait_for_completion=True,
                refresh=refresh,
            )
        except exceptions.NotFoundError:
            logger.info(
                f"Catalog {catalog_path_list[-1]} at path {catalog_path} has no collections, so index does not exist and cannot be updated, continuing as normal."
            )

        # Reindex items within each collection in this catalog
        try:
            # Get all collections contained in this catalog
            index_param = collection_indices(catalog_paths=[catalog_path_list])
            response = await self.client.search(
                index=index_param,
                body={"sort": [{"id": {"order": "asc"}}]},
            )
            collection_ids = [hit["_id"] for hit in response["hits"]["hits"]]

            await asyncio.gather(
                *[
                    self.client.reindex(
                        body={
                            "dest": {
                                "index": index_by_collection_id(
                                    collection_id=collection_id,
                                    catalog_path_list=new_catalog_path_list,
                                )
                            },
                            "source": {
                                "index": index_by_collection_id(
                                    collection_id=collection_id,
                                    catalog_path_list=catalog_path_list,
                                )
                            },
                        },
                        wait_for_completion=True,
                        refresh=refresh,
                    )
                    for collection_id in collection_ids
                ],
                return_exceptions=True,
            )

        except exceptions.NotFoundError:
            logger.info(
                f"Catalog {catalog_path_list[-1]} at path {catalog_path} has no collections, or items, so index does not exist and cannot be updated, continuing as normal."
            )

    async def update_catalog(
        self, catalog_path: str, catalog: Catalog, refresh: bool = False
    ):
        """Update a collection from the database.

        Args:
            self: The instance of the object calling this function.
            catalog_path (str): The path and ID of the catalog to be updated.
            catalog (Catalog): The Catalog object to be used for the update.
            refresh (bool): Whether to refresh the index after the deletion (default: False).

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not
            found in the database.

        Notes:
            This function updates the catalog in the database using the specified
            `catalog_path` with the catalog specified in the `Catalog` object.
            If the catalog is not found, a `NotFoundError` is raised.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        await self.find_catalog(catalog_path=catalog_path)

        if catalog_id != catalog["id"]:
            old_catalog_path_list = catalog_path_list.copy()
            # Remove old catalog_id and replace with new one
            new_catalog_path_list = catalog_path_list[:-1]
            new_catalog_parent_path = "/".join(new_catalog_path_list)
            new_catalog_path_list.append(catalog["id"])

            await self.create_catalog(
                catalog_path=new_catalog_parent_path, catalog=catalog, refresh=refresh
            )

            # Recursively update all catalogs within this catalog
            # get full set of catalogs in this catalog
            params_index = index_catalogs_by_catalog_id(
                catalog_path_list=catalog_path_list
            )
            response = await self.client.search(
                index=params_index,
                body={
                    "sort": [{"id": {"order": "asc"}}],
                },
            )
            hits = response["hits"]["hits"]

            # Calculate catalog path for found catalog
            source_indices_list = []
            dest_indices_list = []
            old_catalog_path_lists = []
            for hit in hits:
                old_catalog_path = hit["_index"].split("_", 1)[1]
                sub_catalog_id = hit["_id"]
                old_catalog_path_list = old_catalog_path.split(CATALOG_SEPARATOR)
                # Reverse for ordered descending path
                old_catalog_path_list.reverse()
                old_catalog_path_lists.append(old_catalog_path_list)
                source_index = index_catalogs_by_catalog_id(
                    catalog_path_list=old_catalog_path_list
                )
                source_indices_list.append(source_index)
                dest_index = index_catalogs_by_catalog_id(
                    catalog_path_list=new_catalog_path_list
                )
                dest_indices_list.append(dest_index)

            await asyncio.gather(
                *[
                    self.client.reindex(
                        body={
                            "dest": {"index": dest_index},
                            "source": {"index": source_index},
                        },
                        wait_for_completion=True,
                        refresh=refresh,
                    )
                    for (dest_index, source_index) in zip(
                        dest_indices_list, source_indices_list
                    )
                ],
                return_exceptions=True,
            )

            # Reindex sub-catalogs, recursively
            old_sub_catalog_path_list = []
            new_sub_catalog_path_list = []
            for old_catalog_path_list in old_catalog_path_lists:
                old_catalog_path_list.append(sub_catalog_id)
                new_catalog_path_list.append(sub_catalog_id)
                old_sub_catalog_path = "/".join(old_catalog_path_list)
                old_sub_catalog_path_list.append(old_sub_catalog_path)
                new_sub_catalog_path = "/".join(new_catalog_path_list)
                new_sub_catalog_path_list.append(new_sub_catalog_path)

            await self.reindex_sub_catalogs(
                catalog_path=old_sub_catalog_path,
                new_catalog_path=new_sub_catalog_path,
                refresh=refresh,
            )

            await asyncio.gather(
                *[
                    self.reindex_sub_catalogs(
                        catalog_path=old_sub_catalog_path,
                        new_catalog_path=new_sub_catalog_path,
                        refresh=refresh,
                    )
                    for (old_sub_catalog_path, new_sub_catalog_path) in zip(
                        old_sub_catalog_path_list, new_sub_catalog_path_list
                    )
                ],
                return_exceptions=True,
            )

            # Reindex collections in this catalog with new catalog_id
            old_catalog_path_list = catalog_path_list
            new_catalog_path_list = catalog_path_list[:-1]
            new_catalog_path_list.append(catalog["id"])
            source_index = index_collections_by_catalog_id(
                catalog_path_list=old_catalog_path_list
            )
            dest_index = index_collections_by_catalog_id(
                catalog_path_list=new_catalog_path_list
            )
            try:
                await self.client.reindex(
                    body={
                        "dest": {"index": dest_index},
                        "source": {"index": source_index},
                        "script": {  # The catalog id in the collection itself is only updated for the first catalog
                            "lang": "painless",
                            "source": f"""ctx._id = ctx._id.replace('{catalog_id}', '{catalog["id"]}'); ctx._source.collection = '{catalog["id"]}' ;""",
                        },
                    },
                    wait_for_completion=True,
                    refresh=refresh,
                )
            except exceptions.NotFoundError:
                logger.info(
                    f"Catalog {catalog_id} at path {catalog_path} has no collections, so index does not exist and cannot be updated, continuing as normal."
                )

            # Reindex items within each collection in this catalog
            try:
                # Get all collections contained in this catalog
                index_param = collection_indices(catalog_paths=[old_catalog_path_list])
                response = await self.client.search(
                    index=index_param,
                    body={"sort": [{"id": {"order": "asc"}}]},
                )
                collection_ids = [hit["_id"] for hit in response["hits"]["hits"]]

                await asyncio.gather(
                    *[
                        self.client.reindex(
                            body={
                                "dest": {
                                    "index": index_by_collection_id(
                                        collection_id=collection_id,
                                        catalog_path_list=catalog_path_list,
                                    )
                                },
                                "source": {
                                    "index": index_by_collection_id(
                                        collection_id=collection_id,
                                        catalog_path_list=old_catalog_path_list,
                                    )
                                },
                            },
                            wait_for_completion=True,
                            refresh=refresh,
                        )
                        for collection_id in collection_ids
                    ],
                    return_exceptions=True,
                )
            except exceptions.NotFoundError:
                logger.info(
                    f"Catalog {catalog_id} at path {catalog_path} has no collections, or items, so index does not exist and cannot be updated, continuing as normal."
                )

            await self.delete_catalog(catalog_path=catalog_path)

        else:
            index_param = index_catalogs_by_catalog_id(
                catalog_path_list=catalog_path_list[:-1]
            )
            await self.client.index(
                index=index_param,
                id=catalog_id,
                document=catalog,
                refresh=refresh,
            )

    async def delete_catalog(self, catalog_path: str, refresh: bool = False):
        """Delete a catalog from the database.

        Parameters:
            self: The instance of the object calling this function.
            catalog_path (str): The path and id of the Catalog to be deleted.
            refresh (bool): Whether to refresh the index after the deletion (default: False).

        Raises:
            NotFoundError: If the catalog with the given `catalog_path` is not found in the database.

        Notes:
            This function first verifies that the catalog with the specified `catalog_path` exists in the database, and then
            deletes the catalog. If `refresh` is set to True, the index is refreshed after the deletion. Additionally, this
            function also calls `delete_catalog_index` to delete the index for the collections in the catalog.
        """
        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        catalog_id = catalog_path_list[-1]

        await self.find_catalog(catalog_path=catalog_path)

        # Handle case where we are looking at a nested catalog
        if len(catalog_path_list) > 1:
            search_catalog_path_list = catalog_path_list[:-1]
            index = index_catalogs_by_catalog_id(
                catalog_path_list=search_catalog_path_list
            )
        # Handle case where we are looking at base catalog
        else:
            index = ROOT_CATALOGS_INDEX

        await self.client.delete(index=index, id=catalog_id, refresh=refresh)
        # Need to delete all catalogs contained in this catalog
        index_param = index_catalogs_by_catalog_id(catalog_path_list=catalog_path_list)
        try:
            response = await self.client.search(
                index=index_param,
                body={"sort": [{"id": {"order": "asc"}}]},
            )
            # Delete each catalog recursively
            await asyncio.gather(
                *[
                    self.delete_catalog(catalog_path=f"{catalog_path}/{hit['_id']}")
                    for hit in response["hits"]["hits"]
                ],
                return_exceptions=True,
            )
        except exceptions.NotFoundError:
            logger.info(
                f"Catalog {catalog_id} at path {catalog_path} has no catalogs, so index does not exist and cannot be deleted, continuing as normal."
            )

        # Need to delete all collections contained in this catalog
        index_param = collection_indices(catalog_paths=[catalog_path_list])
        try:
            response = await self.client.search(
                index=index_param,
                body={"sort": [{"id": {"order": "asc"}}]},
            )
            collection_ids = [hit["_id"] for hit in response["hits"]["hits"]]
            await asyncio.gather(
                *[
                    delete_item_index(
                        collection_id=collection_id, catalog_path_list=catalog_path_list
                    )
                    for collection_id in collection_ids
                ],
                return_exceptions=True,
            )
        except exceptions.NotFoundError:
            logger.info(
                f"Some collection at path {catalog_path} has no items, so index does not exist and cannot be deleted, continuing as normal."
            )

        try:
            await delete_collection_index(catalog_path_list=catalog_path_list)
        except exceptions.NotFoundError:
            logger.info(
                f"Catalog {catalog_id} at path {catalog_path} has no collections, so index does not exist and cannot be deleted, continuing as normal."
            )
        try:
            await delete_catalog_index(catalog_path_list=catalog_path_list)
        except exceptions.NotFoundError:
            logger.info(
                f"Catalog {catalog_id} at path {catalog_path} has no catalogs, so index does not exist and cannot be deleted, continuing as normal."
            )

    async def bulk_async(
        self,
        catalog_path: str,
        collection_id: str,
        processed_items: List[Item],
        refresh: bool = False,
    ) -> None:
        """Perform a bulk insert of items into the database asynchronously.

        Args:
            self: The instance of the object calling this function.
            catalog_path (str): The path and id of the parent Catalog into which the items will be inserted.
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            refresh (bool): Whether to refresh the index after the bulk insert (default: False).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`. The
            insert is performed asynchronously, and the event loop is used to run the operation in a separate executor. The
            `mk_actions` function is called to generate a list of actions for the bulk insert. If `refresh` is set to True, the
            index is refreshed after the bulk insert. The function does not return any value.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        await helpers.async_bulk(
            self.client,
            mk_actions(
                catalog_path_list=catalog_path_list,
                collection_id=collection_id,
                processed_items=processed_items,
            ),
            refresh=refresh,
            raise_on_error=False,
        )

    def bulk_sync(
        self,
        catalog_path: str,
        collection_id: str,
        processed_items: List[Item],
        refresh: bool = False,
    ) -> None:
        """Perform a bulk insert of items into the database synchronously.

        Args:
            self: The instance of the object calling this function.
            catalog_path (str): The path and id of the parent Catalog into which the items will be inserted.
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            refresh (bool): Whether to refresh the index after the bulk insert (default: False).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`. The
            insert is performed synchronously and blocking, meaning that the function does not return until the insert has
            completed. The `mk_actions` function is called to generate a list of actions for the bulk insert. If `refresh` is set to
            True, the index is refreshed after the bulk insert. The function does not return any value.
        """

        # Create list of nested catalog ids
        catalog_path_list = catalog_path.split("/")

        helpers.bulk(
            self.sync_client,
            mk_actions(
                catalog_path_list=catalog_path_list,
                collection_id=collection_id,
                processed_items=processed_items,
            ),
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
        ignore_unavailable: bool = True,
        conformance_classes: list = [],
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

        # Logic to ensure next token only returned when further results are available
        max_result_window = stac_fastapi.types.search.Limit.le
        size_limit = min(limit + 1, max_result_window)

        search_task = asyncio.create_task(
            self.client.search(
                index=[f"{CATALOGS_INDEX_PREFIX}*", f"{COLLECTIONS_INDEX_PREFIX}*"],
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=DEFAULT_DISCOVERY_SORT,  # set to default for time being to support token pagination
                search_after=search_after,
                size=size_limit,
            )
        )

        count_task = asyncio.create_task(
            self.client.count(
                index=[f"{CATALOGS_INDEX_PREFIX}*", f"{COLLECTIONS_INDEX_PREFIX}*"],
                ignore_unavailable=ignore_unavailable,
                body=search.to_dict(count=True),
            )
        )

        es_response = await search_task

        hits = es_response["hits"]["hits"]

        data = []
        catalog_hits = []
        collection_hits = []
        catalog_index_lists_for_catalogs = []
        for hit in hits:
            if hit["_source"]["type"] == "Catalog":
                catalog_hits.append(hit)
                # Calculate catalog path for found catalog
                catalog_index = hit["_index"].split("_", 1)[1]
                catalog_id = hit["_id"]
                catalog_index_list = catalog_index.split(CATALOG_SEPARATOR)
                catalog_index_list.reverse()
                catalog_index_list.append(catalog_id)
                catalog_index_lists_for_catalogs.append(catalog_index_list)
                catalog_index = "/".join(catalog_index_list)
            else:
                collection_hits.append(hit)

        catalogs_results = await asyncio.gather(
            *[
                self.client.search(
                    index=index_catalogs_by_catalog_id(
                        catalog_path_list=catalog_index_list
                    ),
                    body={
                        "sort": [{"id": {"order": "asc"}}],
                    },
                )
                for catalog_index_list in catalog_index_lists_for_catalogs
            ],
            return_exceptions=True,
        )

        # Remove exceptions and replace with empty list
        catalogs_results = [
            (
                result
                if not isinstance(result, Exception)
                else {"hits": {"hits": [{"_source": {}}]}}
            )
            for result in catalogs_results
        ]
        sub_catalogs_responses = [
            sub_catalogs_response["hits"]["hits"]
            for sub_catalogs_response in catalogs_results
        ]
        # sub_catalogs = [[sub_catalogs_response["_source"]] for sub_catalogs_response in sub_catalogs_responses]

        collections_results = await asyncio.gather(
            *[
                self.client.search(
                    index=index_collections_by_catalog_id(
                        catalog_path_list=catalog_index_list
                    ),
                    body={
                        "sort": [{"id": {"order": "asc"}}],
                    },
                )
                for catalog_index_list in catalog_index_lists_for_catalogs
            ],
            return_exceptions=True,
        )

        # Remove exceptions and replace with empty list
        collections_results = [
            (
                result
                if not isinstance(result, Exception)
                else {"hits": {"hits": [{"_source": {}}]}}
            )
            for result in collections_results
        ]
        collection_responses = [
            collection_response["hits"]["hits"]
            for collection_response in collections_results
        ]

        child_data = list(zip(sub_catalogs_responses, collection_responses))

        hit_tokens = []
        for i, hit in enumerate(catalog_hits):
            catalog_index_list = hit["_index"].split("_", 1)[1].split(CATALOG_SEPARATOR)
            catalog_index_list.reverse()
            catalog_index = "/".join(catalog_index_list)
            sub_data_catalogs_and_collections = child_data[i]

            # Extract sub-catalogs
            sub_catalogs = []
            for catalog in sub_data_catalogs_and_collections[0]:
                sub_catalogs.append(catalog["_source"])
            # Extract collections
            collections = []
            for collection in sub_data_catalogs_and_collections[1]:
                collections.append(collection["_source"])
            data.append(
                self.catalog_collection_serializer.db_to_stac(
                    collection_serializer=self.collection_serializer,
                    catalog_serializer=self.catalog_serializer,
                    data=hit["_source"],
                    base_url=base_url,
                    catalog_path=catalog_index,
                    sub_catalogs=sub_catalogs,
                    collections=collections,
                    conformance_classes=conformance_classes,
                )
            )
            if sort_array := hit.get("sort"):
                hit_token = urlsafe_b64encode(
                    ",".join([str(x) for x in sort_array]).encode()
                ).decode()
                hit_tokens.append(hit_token)
            else:
                hit_tokens.append(None)
        for i, hit in enumerate(collection_hits):
            if hit["_source"]["type"] == "Collection":
                catalog_index = hit["_index"].split("_", 1)[1]
                catalog_index_list = catalog_index.split(CATALOG_SEPARATOR)
                catalog_index_list.reverse()
                catalog_index = "/".join(catalog_index_list)
                sub_catalogs = []
                collections = []
                data.append(
                    self.catalog_collection_serializer.db_to_stac(
                        collection_serializer=self.collection_serializer,
                        catalog_serializer=self.catalog_serializer,
                        data=hit["_source"],
                        base_url=base_url,
                        catalog_path=catalog_index,
                        sub_catalogs=sub_catalogs,
                        collections=collections,
                        conformance_classes=conformance_classes,
                    )
                )
                if sort_array := hit.get("sort"):
                    hit_token = urlsafe_b64encode(
                        ",".join([str(x) for x in sort_array]).encode()
                    ).decode()
                    hit_tokens.append(hit_token)
                else:
                    hit_tokens.append(None)

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(
                    ",".join([str(x) for x in sort_array]).encode()
                ).decode()

        # (1) count should not block returning results, so don't wait for it to be done
        # (2) don't cancel the task so that it will populate the ES cache for subsequent counts
        matched = (
            es_response["hits"]["total"]["value"]
            if es_response["hits"]["total"]["relation"] == "eq"
            else None
        )
        if count_task.done():
            try:
                maybe_count = count_task.result().get("count")
            except Exception as e:
                logger.error(f"Count task failed: {e}")

        return data, matched, next_token, hit_tokens
