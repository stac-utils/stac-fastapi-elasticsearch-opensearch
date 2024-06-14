"""Database logic."""

import asyncio
import logging
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime as datetime_type
from datetime import timezone
from mimetypes import MimeTypes
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    Union,
)
from urllib.parse import urljoin

import attr
import orjson
from elasticsearch_dsl import Q, Search
from fastapi import Request
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes

from elasticsearch import exceptions, helpers  # type: ignore
from stac_fastapi.core.extensions import filter
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.core.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.core.utilities import MAX_LIMIT, bbox2polygon, return_date
from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings
from stac_fastapi.elasticsearch.config import (
    ElasticsearchSettings as SyncElasticsearchSettings,
)
from stac_fastapi.types.config import Settings
from stac_fastapi.types.errors import ConflictError, NotFoundError
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection

logger = logging.getLogger(__name__)

NumType = Union[float, int]

COLLECTIONS_INDEX = os.getenv("STAC_COLLECTIONS_INDEX", "collections")
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
        "extent.spatial.bbox": {"type": "long"},
        "extent.temporal.interval": {"type": "date"},
        "providers": {"type": "object", "enabled": False},
        "links": {"type": "object", "enabled": False},
        "item_assets": {"type": "object", "enabled": False},
    },
}


def index_by_collection_id(collection_id: str) -> str:
    """
    Translate a collection id into an Elasticsearch index name.

    Args:
        collection_id (str): The collection id to translate into an index name.

    Returns:
        str: The index name derived from the collection id.
    """
    return f"{ITEMS_INDEX_PREFIX}{''.join(c for c in collection_id.lower() if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS)}"


def indices(collection_ids: Optional[List[str]]) -> str:
    """
    Get a comma-separated string of index names for a given list of collection ids.

    Args:
        collection_ids: A list of collection ids.

    Returns:
        A string of comma-separated index names. If `collection_ids` is None, returns the default indices.
    """
    if collection_ids is None:
        return ITEM_INDICES
    else:
        return ",".join([index_by_collection_id(c) for c in collection_ids])


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
            "index_patterns": [f"{COLLECTIONS_INDEX}*"],
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


async def create_item_index(collection_id: str):
    """
    Create the index for Items. The settings of the index template will be used implicitly.

    Args:
        collection_id (str): Collection identifier.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client
    index_name = index_by_collection_id(collection_id)

    await client.options(ignore_status=400).indices.create(
        index=f"{index_by_collection_id(collection_id)}-000001",
        aliases={index_name: {}},
    )
    await client.close()


async def delete_item_index(collection_id: str):
    """Delete the index for items in a collection.

    Args:
        collection_id (str): The ID of the collection whose items index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_by_collection_id(collection_id)
    resolved = await client.indices.resolve_index(name=name)
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name)
    await client.close()


def mk_item_id(item_id: str, collection_id: str):
    """Create the document id for an Item in Elasticsearch.

    Args:
        item_id (str): The id of the Item.
        collection_id (str): The id of the Collection that the Item belongs to.

    Returns:
        str: The document id for the Item, combining the Item id and the Collection id, separated by a `|` character.
    """
    return f"{item_id}|{collection_id}"


def mk_actions(collection_id: str, processed_items: List[Item]):
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
    extensions = attr.ib(default=[])

    item_serializer: Type[ItemSerializer] = attr.ib(default=ItemSerializer)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )

    """CORE LOGIC"""

    def load_extensions(self, extensions: list) -> None:
        """Add extensions to current extensions list.

        Args:
            extenstions (list): list of extensions to add.
        """
        self.extensions.extend(extensions)

    async def get_all_collections(
        self, request: Request
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all collections from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.

        Returns:
            A tuple of (collections, next pagination token if any).
        """
        base_url = str(request.base_url)
        limit = int(request.query_params.get("limit", 10))
        token = request.query_params.get("token")

        search_after = None
        if token:
            search_after = [token]

        response = await self.client.search(
            index=COLLECTIONS_INDEX,
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

        return Collections(collections=collections, links=links)

    async def get_item(
        self, collection_id: str, item_id: str, request: Request
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
                index=index_by_collection_id(collection_id),
                id=mk_item_id(item_id, collection_id),
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist in Collection {collection_id}"
            )
        return self.item_serializer.db_to_stac(
            item=item["_source"], base_url=str(request.base_url)
        )

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
            key_filter = {field: {op: value}}
            search = search.filter(Q("range", **key_filter))
        else:
            search = search.filter("term", **{field: value})

        return search

    @staticmethod
    def apply_cql2_filter(search: Search, _filter: Optional[Dict[str, Any]]):
        """
        Apply a CQL2 filter to an Elasticsearch Search object.

        This method transforms a dictionary representing a CQL2 filter into an Elasticsearch query
        and applies it to the provided Search object. If the filter is None, the original Search
        object is returned unmodified.

        Args:
            search (Search): The Elasticsearch Search object to which the filter will be applied.
            _filter (Optional[Dict[str, Any]]): The filter in dictionary form that needs to be applied
                                                to the search. The dictionary should follow the structure
                                                required by the `to_es` function which converts it
                                                to an Elasticsearch query.

        Returns:
            Search: The modified Search object with the filter applied if a filter is provided,
                    otherwise the original Search object.
        """
        if _filter is not None:
            es_query = filter.to_es(_filter)
            search = search.query(es_query)

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
        search_request: BaseSearchPostRequest,
        request: Request,
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
        search = self.make_search()

        if search_request.ids:
            search = self.apply_ids_filter(search=search, item_ids=search_request.ids)

        if search_request.collections:
            search = self.apply_collections_filter(
                search=search, collection_ids=search_request.collections
            )

        if search_request.datetime:
            datetime_search = return_date(search_request.datetime)
            search = self.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.apply_bbox_filter(search=search, bbox=bbox)

        if search_request.intersects:
            search = self.apply_intersects_filter(
                search=search, intersects=search_request.intersects
            )

        if search_request.query:
            for field_name, expr in search_request.query.items():
                field = "properties__" + field_name
                for op, value in expr.items():
                    # Convert enum to string
                    operator = op.value if isinstance(op, Enum) else op
                    search = self.apply_stacql_filter(
                        search=search, op=operator, field=field, value=value
                    )

        # only cql2_json is supported here
        if hasattr(search_request, "filter"):
            cql2_filter = getattr(search_request, "filter", None)
            try:
                search = self.apply_cql2_filter(search, cql2_filter)
            except Exception as exc:
                raise Exception("Error with cql2_json filter") from exc

        sort = None
        if search_request.sortby:
            sort = self.populate_sort(search_request.sortby)

        limit = 10
        if search_request.limit:
            limit = search_request.limit

        search_after = None
        if hasattr(search_request, "token"):
            search_after = (
                urlsafe_b64decode(search_request.token.encode()).decode().split(",")
            )

        query = search.query.to_dict() if search.query else None

        index_param = indices(search_request.collections)

        max_result_window = MAX_LIMIT

        size_limit = min(limit + 1, max_result_window)

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
        except exceptions.NotFoundError as exc:
            raise NotFoundError(
                f"Collections '{search_request.collections}' do not exist"
            ) from exc

        hits = es_response["hits"]["hits"]
        items = (hit["_source"] for hit in hits[:limit])

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(
                    ",".join([str(x) for x in sort_array]).encode()
                ).decode()

        matched = (
            es_response["hits"]["total"]["value"]
            if es_response["hits"]["total"]["relation"] == "eq"
            else None
        )
        if count_task.done():
            try:
                matched = count_task.result().get("count")
            except Exception as exc:
                logger.error("Count task failed: %s", exc)

        items = [
            self.item_serializer.db_to_stac(item, base_url=str(request.base_url))
            for item in items
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
                orjson.loads(item.json(**filter_kwargs, exclude_unset=True))
                for item in items
            ]

        links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=matched,
        )

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str):
        """Database logic to check if a collection exists."""
        if not await self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise NotFoundError(f"Collection {collection_id} does not exist")

    async def prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
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
        await self.check_collection_exists(collection_id=item["collection"])

        if not exist_ok and await self.client.exists(
            index=index_by_collection_id(item["collection"]),
            id=mk_item_id(item["id"], item["collection"]),
        ):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    def sync_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
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
            index=index_by_collection_id(collection_id),
            id=mk_item_id(item_id, collection_id),
        ):
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    async def create_item(self, item: Item, request: Request, refresh: bool = False):
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
        self.prep_create_item(item=item, base_url=request.base_url)

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

        return self.item_serializer.db_to_stac(item, request.base_url)

    async def delete_item(
        self, item_id: str, collection_id: str, refresh: bool = False
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
                index=index_by_collection_id(collection_id),
                id=mk_item_id(item_id, collection_id),
                refresh=refresh,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )

    async def update_item(
        self,
        item_id: str,
        collection_id: str,
        item: Item,
        request: Request,
        refresh: bool = False,
    ):
        """Update an item from the database.

        Args:
            item_id (str): The id of the Item to be updated.
            collection_id (str): The id of the Collection that the Item belongs to.
            item (Item): The updated item.
            refresh (bool, optional): Whether to refresh the index after the deletion. Default is False.

        Raises:
            NotFoundError: If the Item does not exist in the database.
        """
        item.properties["updated"] = (
            datetime_type.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )

        await self.check_collection_exists(collection_id)
        await self.delete_item(item_id=item_id, collection_id=collection_id)
        return await self.create_item(item=item, refresh=refresh, request=request)

    async def create_collection(
        self, collection: Collection, request: Request, refresh: bool = False
    ):
        """Create a single collection in the database.

        Args:
            collection (Collection): The Collection object to be created.
            refresh (bool, optional): Whether to refresh the index after the creation. Default is False.

        Raises:
            ConflictError: If a Collection with the same id already exists in the database.

        Notes:
            A new index is created for the items in the Collection using the `create_item_index` function.
        """
        collection = collection.model_dump(mode="json")
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

        return self.collection_serializer.db_to_stac(
            collection=collection, base_url=str(request.base_url)
        )

    async def get_collection(self, collection_id: str, request: Request) -> Collection:
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
            collection = await self.client.get(
                index=COLLECTIONS_INDEX, id=collection_id
            )
        except exceptions.NotFoundError as exc:
            raise NotFoundError(f"Collection {collection_id} not found") from exc

        return self.collection_serializer.db_to_stac(
            collection=collection["_source"], base_url=str(request.base_url)
        )

    async def update_collection(
        self,
        collection_id: str,
        collection: Collection,
        request: Request,
        refresh: bool = False,
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
        await self.get_collection(collection_id=collection_id, request=request)

        collection = collection.model_dump(mode="json")

        if collection_id != collection["id"]:
            await self.create_collection(
                collection=collection, request=request, refresh=refresh
            )

            await self.client.reindex(
                body={
                    "dest": {"index": f"{ITEMS_INDEX_PREFIX}{collection['id']}"},
                    "source": {"index": f"{ITEMS_INDEX_PREFIX}{collection_id}"},
                    "script": {
                        "lang": "painless",
                        "source": f"""ctx._id = ctx._id.replace('{collection_id}', '{collection["id"]}'); ctx._source.collection = '{collection["id"]}' ;""",
                    },
                },
                wait_for_completion=True,
                refresh=refresh,
            )

            await self.delete_collection(collection_id=collection_id, request=request)

        else:
            await self.client.index(
                index=COLLECTIONS_INDEX,
                id=collection_id,
                document=collection,
                refresh=refresh,
            )

        return self.collection_serializer.db_to_stac(
            collection=collection, base_url=str(request.base_url)
        )

    async def delete_collection(
        self, collection_id: str, request: Request, refresh: bool = False
    ):
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
            function also calls `delete_item_index` to delete the index for the items in the collection.
        """
        await self.get_collection(collection_id=collection_id, request=request)
        await self.client.delete(
            index=COLLECTIONS_INDEX, id=collection_id, refresh=refresh
        )
        await delete_item_index(collection_id)
        return None

    async def bulk_async(
        self, collection_id: str, processed_items: List[Item], refresh: bool = False
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
            mk_actions(collection_id, processed_items),
            refresh=refresh,
            raise_on_error=False,
        )

    def bulk_sync(
        self, collection_id: str, processed_items: List[Item], refresh: bool = False
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
            mk_actions(collection_id, processed_items),
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
