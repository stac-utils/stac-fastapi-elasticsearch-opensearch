"""Database logic."""

import asyncio
import logging
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Iterable
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Type

import attr
import orjson
from fastapi import HTTPException
from opensearchpy import exceptions, helpers
from opensearchpy.helpers.query import Q
from opensearchpy.helpers.search import Search
from starlette.requests import Request

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.core.utilities import bbox2polygon, get_bool_env, get_max_limit
from stac_fastapi.extensions.core.transaction.request import (
    PartialCollection,
    PartialItem,
    PatchOperation,
)
from stac_fastapi.opensearch.config import (
    AsyncOpensearchSettings as AsyncSearchSettings,
)
from stac_fastapi.opensearch.config import OpensearchSettings as SyncSearchSettings
from stac_fastapi.sfeos_helpers import filter as filter_module
from stac_fastapi.sfeos_helpers.database import (
    apply_free_text_filter_shared,
    apply_intersects_filter_shared,
    create_index_templates_shared,
    delete_item_index_shared,
    get_queryables_mapping_shared,
    index_alias_by_collection_id,
    mk_actions,
    mk_item_id,
    populate_sort_shared,
    return_date,
    validate_refresh,
)
from stac_fastapi.sfeos_helpers.database.query import (
    ES_MAX_URL_LENGTH,
    add_collections_to_body,
)
from stac_fastapi.sfeos_helpers.database.utils import (
    merge_to_operations,
    operations_to_script,
)
from stac_fastapi.sfeos_helpers.mappings import (
    AGGREGATION_MAPPING,
    COLLECTIONS_INDEX,
    DEFAULT_SORT,
    ES_COLLECTIONS_MAPPINGS,
    ITEM_INDICES,
    ITEMS_INDEX_PREFIX,
    Geometry,
)
from stac_fastapi.sfeos_helpers.search_engine import (
    BaseIndexInserter,
    BaseIndexSelector,
    IndexInsertionFactory,
    IndexSelectorFactory,
)
from stac_fastapi.types.errors import ConflictError, NotFoundError
from stac_fastapi.types.links import resolve_links
from stac_fastapi.types.stac import Collection, Item

logger = logging.getLogger(__name__)


async def create_index_templates() -> None:
    """
    Create index templates for the Collection and Item indices.

    Returns:
        None

    """
    await create_index_templates_shared(settings=AsyncSearchSettings())


async def create_collection_index() -> None:
    """
    Create the index for a Collection. The settings of the index template will be used implicitly.

    Returns:
        None

    """
    client = AsyncSearchSettings().create_client

    index = f"{COLLECTIONS_INDEX}-000001"

    exists = await client.indices.exists(index=index)
    if not exists:
        await client.indices.create(
            index=index,
            body={
                "aliases": {COLLECTIONS_INDEX: {}},
                "mappings": ES_COLLECTIONS_MAPPINGS,
            },
        )
    await client.close()


async def delete_item_index(collection_id: str) -> None:
    """Delete the index for items in a collection.

    Args:
        collection_id (str): The ID of the collection whose items index will be deleted.

    Notes:
        This function delegates to the shared implementation in delete_item_index_shared.
    """
    await delete_item_index_shared(
        settings=AsyncSearchSettings(), collection_id=collection_id
    )


@attr.s
class DatabaseLogic(BaseDatabaseLogic):
    """Database logic."""

    async_settings: AsyncSearchSettings = attr.ib(factory=AsyncSearchSettings)
    sync_settings: SyncSearchSettings = attr.ib(factory=SyncSearchSettings)

    async_index_selector: BaseIndexSelector = attr.ib(init=False)
    async_index_inserter: BaseIndexInserter = attr.ib(init=False)

    client = attr.ib(init=False)
    sync_client = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Initialize clients after the class is instantiated."""
        self.client = self.async_settings.create_client
        self.sync_client = self.sync_settings.create_client
        self.async_index_inserter = IndexInsertionFactory.create_insertion_strategy(
            self.client
        )
        self.async_index_selector = IndexSelectorFactory.create_selector(self.client)

    item_serializer: Type[ItemSerializer] = attr.ib(default=ItemSerializer)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )

    extensions: List[str] = attr.ib(default=attr.Factory(list))

    aggregation_mapping: Dict[str, Dict[str, Any]] = AGGREGATION_MAPPING

    """CORE LOGIC"""

    async def get_all_collections(
        self,
        token: Optional[str],
        limit: int,
        request: Request,
        sort: Optional[List[Dict[str, Any]]] = None,
        q: Optional[List[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of collections from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.
            request (Request): The FastAPI request object.
            sort (Optional[List[Dict[str, Any]]]): Optional sort parameter from the request.
            q (Optional[List[str]]): Free text search terms.

        Returns:
            A tuple of (collections, next pagination token if any).

        Raises:
            HTTPException: If sorting is requested on a field that is not sortable.
        """
        # Define sortable fields based on the ES_COLLECTIONS_MAPPINGS
        sortable_fields = ["id", "extent.temporal.interval", "temporal"]

        # Format the sort parameter
        formatted_sort = []
        if sort:
            for item in sort:
                field = item.get("field")
                direction = item.get("direction", "asc")
                if field:
                    # Validate that the field is sortable
                    if field not in sortable_fields:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Field '{field}' is not sortable. Sortable fields are: {', '.join(sortable_fields)}. "
                            + "Text fields are not sortable by default in OpenSearch. "
                            + "To make a field sortable, update the mapping to use 'keyword' type or add a '.keyword' subfield. ",
                        )
                    formatted_sort.append({field: {"order": direction}})
            # Always include id as a secondary sort to ensure consistent pagination
            if not any("id" in item for item in formatted_sort):
                formatted_sort.append({"id": {"order": "asc"}})
        else:
            formatted_sort = [{"id": {"order": "asc"}}]

        body = {
            "sort": formatted_sort,
            "size": limit,
        }

        if token:
            body["search_after"] = [token]

        # Apply free text query if provided
        if q:
            # For collections, we want to search across all relevant fields
            should_clauses = []

            # For each search term
            for term in q:
                # Create a multi_match query for each term
                for field in [
                    "id",
                    "title",
                    "description",
                    "keywords",
                    "summaries.platform",
                    "summaries.constellation",
                    "providers.name",
                    "providers.url",
                ]:
                    should_clauses.append(
                        {
                            "wildcard": {
                                field: {"value": f"*{term}*", "case_insensitive": True}
                            }
                        }
                    )

            # Add the query to the body using bool query with should clauses
            body["query"] = {
                "bool": {"should": should_clauses, "minimum_should_match": 1}
            }

        response = await self.client.search(
            index=COLLECTIONS_INDEX,
            body=body,
        )

        hits = response["hits"]["hits"]
        collections = [
            self.collection_serializer.db_to_stac(
                collection=hit["_source"], request=request, extensions=self.extensions
            )
            for hit in hits
        ]

        next_token = None
        if len(hits) == limit:
            # Ensure we have a valid sort value for next_token
            next_token_values = hits[-1].get("sort")
            if next_token_values:
                next_token = next_token_values[0]

        return collections, next_token

    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
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
            response = await self.client.search(
                index=index_alias_by_collection_id(collection_id),
                body={
                    "query": {"term": {"_id": mk_item_id(item_id, collection_id)}},
                    "size": 1,
                },
            )
            if response["hits"]["total"]["value"] == 0:
                raise NotFoundError(
                    f"Item {item_id} does not exist inside Collection {collection_id}"
                )

            return response["hits"]["hits"][0]["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist inside Collection {collection_id}"
            )

    async def get_queryables_mapping(self, collection_id: str = "*") -> dict:
        """Retrieve mapping of Queryables for search.

        Args:
            collection_id (str, optional): The id of the Collection the Queryables
            belongs to. Defaults to "*".

        Returns:
            dict: A dictionary containing the Queryables mappings.
        """
        mappings = await self.client.indices.get_mapping(
            index=f"{ITEMS_INDEX_PREFIX}{collection_id}",
        )
        return await get_queryables_mapping_shared(
            collection_id=collection_id, mappings=mappings
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
    def apply_free_text_filter(search: Search, free_text_queries: Optional[List[str]]):
        """Create a free text query for OpenSearch queries.

        This method delegates to the shared implementation in apply_free_text_filter_shared.

        Args:
            search (Search): The search object to apply the query to.
            free_text_queries (Optional[List[str]]): A list of text strings to search for in the properties.

        Returns:
            Search: The search object with the free text query applied, or the original search
                object if no free_text_queries were provided.
        """
        return apply_free_text_filter_shared(
            search=search, free_text_queries=free_text_queries
        )

    @staticmethod
    def apply_datetime_filter(
        search: Search, datetime: Optional[str]
    ) -> Tuple[Search, Dict[str, Optional[str]]]:
        """Apply a filter to search on datetime, start_datetime, and end_datetime fields.

        Args:
            search: The search object to filter.
            datetime: Optional[str]

        Returns:
            The filtered search object.
        """
        datetime_search = return_date(datetime)

        if not datetime_search:
            return search, datetime_search

        # USE_DATETIME env var
        # True: Search by datetime, if null search by start/end datetime
        # False: Always search only by start/end datetime
        USE_DATETIME = get_bool_env("USE_DATETIME", default=True)

        if USE_DATETIME:
            if "eq" in datetime_search:
                # For exact matches, include:
                # 1. Items with matching exact datetime
                # 2. Items with datetime:null where the time falls within their range
                should = [
                    Q(
                        "bool",
                        filter=[
                            Q("exists", field="properties.datetime"),
                            Q(
                                "term",
                                **{"properties__datetime": datetime_search["eq"]},
                            ),
                        ],
                    ),
                    Q(
                        "bool",
                        must_not=[Q("exists", field="properties.datetime")],
                        filter=[
                            Q("exists", field="properties.start_datetime"),
                            Q("exists", field="properties.end_datetime"),
                            Q(
                                "range",
                                properties__start_datetime={
                                    "lte": datetime_search["eq"]
                                },
                            ),
                            Q(
                                "range",
                                properties__end_datetime={"gte": datetime_search["eq"]},
                            ),
                        ],
                    ),
                ]
            else:
                # For date ranges, include:
                # 1. Items with datetime in the range
                # 2. Items with datetime:null that overlap the search range
                should = [
                    Q(
                        "bool",
                        filter=[
                            Q("exists", field="properties.datetime"),
                            Q(
                                "range",
                                properties__datetime={
                                    "gte": datetime_search["gte"],
                                    "lte": datetime_search["lte"],
                                },
                            ),
                        ],
                    ),
                    Q(
                        "bool",
                        must_not=[Q("exists", field="properties.datetime")],
                        filter=[
                            Q("exists", field="properties.start_datetime"),
                            Q("exists", field="properties.end_datetime"),
                            Q(
                                "range",
                                properties__start_datetime={
                                    "lte": datetime_search["lte"]
                                },
                            ),
                            Q(
                                "range",
                                properties__end_datetime={
                                    "gte": datetime_search["gte"]
                                },
                            ),
                        ],
                    ),
                ]

            return (
                search.query(Q("bool", should=should, minimum_should_match=1)),
                datetime_search,
            )
        else:
            if "eq" in datetime_search:
                filter_query = Q(
                    "bool",
                    filter=[
                        Q("exists", field="properties.start_datetime"),
                        Q("exists", field="properties.end_datetime"),
                        Q(
                            "range",
                            properties__start_datetime={"lte": datetime_search["eq"]},
                        ),
                        Q(
                            "range",
                            properties__end_datetime={"gte": datetime_search["eq"]},
                        ),
                    ],
                )
            else:
                filter_query = Q(
                    "bool",
                    filter=[
                        Q("exists", field="properties.start_datetime"),
                        Q("exists", field="properties.end_datetime"),
                        Q(
                            "range",
                            properties__start_datetime={"lte": datetime_search["lte"]},
                        ),
                        Q(
                            "range",
                            properties__end_datetime={"gte": datetime_search["gte"]},
                        ),
                    ],
                )
        return search.query(filter_query), datetime_search

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
        filter = apply_intersects_filter_shared(intersects=intersects)
        return search.filter(Q(filter))

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

    async def apply_cql2_filter(
        self, search: Search, _filter: Optional[Dict[str, Any]]
    ):
        """
        Apply a CQL2 filter to an Opensearch Search object.

        This method transforms a dictionary representing a CQL2 filter into an Opensearch query
        and applies it to the provided Search object. If the filter is None, the original Search
        object is returned unmodified.

        Args:
            search (Search): The Opensearch Search object to which the filter will be applied.
            _filter (Optional[Dict[str, Any]]): The filter in dictionary form that needs to be applied
                                                to the search. The dictionary should follow the structure
                                                required by the `to_es` function which converts it
                                                to an Opensearch query.

        Returns:
            Search: The modified Search object with the filter applied if a filter is provided,
                    otherwise the original Search object.
        """
        if _filter is not None:
            es_query = filter_module.to_es(await self.get_queryables_mapping(), _filter)
            search = search.filter(es_query)

        return search

    @staticmethod
    def populate_sort(sortby: List) -> Optional[Dict[str, Dict[str, str]]]:
        """Create a sort configuration for OpenSearch queries.

        This method delegates to the shared implementation in populate_sort_shared.

        Args:
            sortby (List): A list of sort specifications, each containing a field and direction.

        Returns:
            Optional[Dict[str, Dict[str, str]]]: A dictionary mapping field names to sort direction
                configurations, or None if no sort was specified.
        """
        return populate_sort_shared(sortby=sortby)

    async def execute_search(
        self,
        search: Search,
        limit: int,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        collection_ids: Optional[List[str]],
        datetime_search: Dict[str, Optional[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Execute a search query with limit and other optional parameters.

        Args:
            search (Search): The search query to be executed.
            limit (int): The maximum number of results to be returned.
            token (Optional[str]): The token used to return the next set of results.
            sort (Optional[Dict[str, Dict[str, str]]]): Specifies how the results should be sorted.
            collection_ids (Optional[List[str]]): The collection ids to search.
            datetime_search (Dict[str, Optional[str]]): Datetime range used for index selection.
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
        search_body: Dict[str, Any] = {}
        query = search.query.to_dict() if search.query else None

        index_param = await self.async_index_selector.select_indexes(
            collection_ids, datetime_search
        )
        if len(index_param) > ES_MAX_URL_LENGTH - 300:
            index_param = ITEM_INDICES
            query = add_collections_to_body(collection_ids, query)

        if query:
            search_body["query"] = query

        search_after = None

        if token:
            search_after = orjson.loads(urlsafe_b64decode(token))
        if search_after:
            search_body["search_after"] = search_after

        search_body["sort"] = sort if sort else DEFAULT_SORT

        max_result_window = get_max_limit()

        size_limit = min(limit + 1, max_result_window)

        search_task = asyncio.create_task(
            self.client.search(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                body=search_body,
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
            raise NotFoundError(f"Collections '{collection_ids}' do not exist")

        hits = es_response["hits"]["hits"]
        items = (hit["_source"] for hit in hits[:limit])

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(orjson.dumps(sort_array)).decode()

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

        return items, matched, next_token

    """ AGGREGATE LOGIC """

    async def aggregate(
        self,
        collection_ids: Optional[List[str]],
        aggregations: List[str],
        search: Search,
        centroid_geohash_grid_precision: int,
        centroid_geohex_grid_precision: int,
        centroid_geotile_grid_precision: int,
        geometry_geohash_grid_precision: int,
        geometry_geotile_grid_precision: int,
        datetime_frequency_interval: str,
        datetime_search,
        ignore_unavailable: Optional[bool] = True,
    ):
        """Return aggregations of STAC Items."""
        search_body: Dict[str, Any] = {}
        query = search.query.to_dict() if search.query else None
        if query:
            search_body["query"] = query

        def _fill_aggregation_parameters(name: str, agg: dict) -> dict:
            [key] = agg.keys()
            agg_precision = {
                "centroid_geohash_grid_frequency": centroid_geohash_grid_precision,
                "centroid_geohex_grid_frequency": centroid_geohex_grid_precision,
                "centroid_geotile_grid_frequency": centroid_geotile_grid_precision,
                "geometry_geohash_grid_frequency": geometry_geohash_grid_precision,
                "geometry_geotile_grid_frequency": geometry_geotile_grid_precision,
            }
            if name in agg_precision:
                agg[key]["precision"] = agg_precision[name]

            if key == "date_histogram":
                agg[key]["calendar_interval"] = datetime_frequency_interval

            return agg

        # include all aggregations specified
        # this will ignore aggregations with the wrong names
        search_body["aggregations"] = {
            k: _fill_aggregation_parameters(k, deepcopy(v))
            for k, v in self.aggregation_mapping.items()
            if k in aggregations
        }

        index_param = await self.async_index_selector.select_indexes(
            collection_ids, datetime_search
        )

        search_task = asyncio.create_task(
            self.client.search(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                body=search_body,
            )
        )

        try:
            db_response = await search_task
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collections '{collection_ids}' do not exist")

        return db_response

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str):
        """Database logic to check if a collection exists."""
        if not await self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise NotFoundError(f"Collection {collection_id} does not exist")

    async def async_prep_create_item(
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
        alias = index_alias_by_collection_id(item["collection"])
        doc_id = mk_item_id(item["id"], item["collection"])

        if not exist_ok:
            alias_exists = await self.client.indices.exists_alias(name=alias)

            if alias_exists:
                alias_info = await self.client.indices.get_alias(name=alias)
                indices = list(alias_info.keys())

                for index in indices:
                    if await self.client.exists(index=index, id=doc_id):
                        raise ConflictError(
                            f"Item {item['id']} in collection {item['collection']} already exists"
                        )

        return self.item_serializer.stac_to_db(item, base_url)

    async def bulk_async_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Prepare an item for insertion into the database.

        This method performs pre-insertion preparation on the given `item`, such as:
        - Verifying that the collection the item belongs to exists.
        - Optionally checking if an item with the same ID already exists in the database.
        - Serializing the item into a database-compatible format.

        Args:
            item (Item): The item to be prepared for insertion.
            base_url (str): The base URL used to construct the item's self URL.
            exist_ok (bool): Indicates whether the item can already exist in the database.
                            If False, a `ConflictError` is raised if the item exists.

        Returns:
            Item: The prepared item, serialized into a database-compatible format.

        Raises:
            NotFoundError: If the collection that the item belongs to does not exist in the database.
            ConflictError: If an item with the same ID already exists in the collection and `exist_ok` is False,
                        and `RAISE_ON_BULK_ERROR` is set to `true`.
        """
        logger.debug(f"Preparing item {item['id']} in collection {item['collection']}.")

        # Check if the collection exists
        await self.check_collection_exists(collection_id=item["collection"])

        # Check if the item already exists in the database
        if not exist_ok and await self.client.exists(
            index=index_alias_by_collection_id(item["collection"]),
            id=mk_item_id(item["id"], item["collection"]),
        ):
            error_message = (
                f"Item {item['id']} in collection {item['collection']} already exists."
            )
            if self.async_settings.raise_on_bulk_error:
                raise ConflictError(error_message)
            else:
                logger.warning(
                    f"{error_message} Continuing as `RAISE_ON_BULK_ERROR` is set to false."
                )
        # Serialize the item into a database-compatible format
        prepped_item = self.item_serializer.stac_to_db(item, base_url)
        logger.debug(f"Item {item['id']} prepared successfully.")
        return prepped_item

    def bulk_sync_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Prepare an item for insertion into the database.

        This method performs pre-insertion preparation on the given `item`, such as:
        - Verifying that the collection the item belongs to exists.
        - Optionally checking if an item with the same ID already exists in the database.
        - Serializing the item into a database-compatible format.

        Args:
            item (Item): The item to be prepared for insertion.
            base_url (str): The base URL used to construct the item's self URL.
            exist_ok (bool): Indicates whether the item can already exist in the database.
                            If False, a `ConflictError` is raised if the item exists.

        Returns:
            Item: The prepared item, serialized into a database-compatible format.

        Raises:
            NotFoundError: If the collection that the item belongs to does not exist in the database.
            ConflictError: If an item with the same ID already exists in the collection and `exist_ok` is False,
                        and `RAISE_ON_BULK_ERROR` is set to `true`.
        """
        logger.debug(f"Preparing item {item['id']} in collection {item['collection']}.")

        # Check if the collection exists
        if not self.sync_client.exists(index=COLLECTIONS_INDEX, id=item["collection"]):
            raise NotFoundError(f"Collection {item['collection']} does not exist")

        # Check if the item already exists in the database
        if not exist_ok and self.sync_client.exists(
            index=index_alias_by_collection_id(item["collection"]),
            id=mk_item_id(item["id"], item["collection"]),
        ):
            error_message = (
                f"Item {item['id']} in collection {item['collection']} already exists."
            )
            if self.sync_settings.raise_on_bulk_error:
                raise ConflictError(error_message)
            else:
                logger.warning(
                    f"{error_message} Continuing as `RAISE_ON_BULK_ERROR` is set to false."
                )

        # Serialize the item into a database-compatible format
        prepped_item = self.item_serializer.stac_to_db(item, base_url)
        logger.debug(f"Item {item['id']} prepared successfully.")
        return prepped_item

    async def create_item(
        self,
        item: Item,
        base_url: str = "",
        exist_ok: bool = False,
        **kwargs: Any,
    ):
        """Database logic for creating one item.

        Args:
            item (Item): The item to be created.
            base_url (str, optional): The base URL for the item. Defaults to an empty string.
            exist_ok (bool, optional): Whether to allow the item to exist already. Defaults to False.
            **kwargs: Additional keyword arguments like refresh.

        Raises:
            ConflictError: If the item already exists in the database.

        Returns:
            None
        """
        # todo: check if collection exists, but cache
        item_id = item["id"]
        collection_id = item["collection"]

        # Ensure kwargs is a dictionary
        kwargs = kwargs or {}

        # Resolve the `refresh` parameter
        refresh = kwargs.get("refresh", self.async_settings.database_refresh)
        refresh = validate_refresh(refresh)

        # Log the creation attempt
        logger.info(
            f"Creating item {item_id} in collection {collection_id} with refresh={refresh}"
        )

        item = await self.async_prep_create_item(
            item=item, base_url=base_url, exist_ok=exist_ok
        )

        target_index = await self.async_index_inserter.get_target_index(
            collection_id, item
        )

        await self.client.index(
            index=target_index,
            id=mk_item_id(item_id, collection_id),
            body=item,
            refresh=refresh,
        )

    async def merge_patch_item(
        self,
        collection_id: str,
        item_id: str,
        item: PartialItem,
        base_url: str,
        refresh: bool = True,
    ) -> Item:
        """Database logic for merge patching an item following RF7396.

        Args:
            collection_id(str): Collection that item belongs to.
            item_id(str): Id of item to be patched.
            item (PartialItem): The partial item to be updated.
            base_url: (str): The base URL used for constructing URLs for the item.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to True.

        Returns:
            patched item.
        """
        operations = merge_to_operations(item.model_dump())

        return await self.json_patch_item(
            collection_id=collection_id,
            item_id=item_id,
            operations=operations,
            base_url=base_url,
            create_nest=True,
            refresh=refresh,
        )

    async def json_patch_item(
        self,
        collection_id: str,
        item_id: str,
        operations: List[PatchOperation],
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> Item:
        """Database logic for json patching an item following RF6902.

        Args:
            collection_id(str): Collection that item belongs to.
            item_id(str): Id of item to be patched.
            operations (list): List of operations to run.
            base_url (str): The base URL used for constructing URLs for the item.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to True.

        Returns:
            patched item.
        """
        new_item_id = None
        new_collection_id = None
        script_operations = []

        for operation in operations:
            if operation.path in ["collection", "id"] and operation.op in [
                "add",
                "replace",
            ]:
                if operation.path == "collection" and collection_id != operation.value:
                    await self.check_collection_exists(collection_id=operation.value)
                    new_collection_id = operation.value

                if operation.path == "id" and item_id != operation.value:
                    new_item_id = operation.value

            else:
                script_operations.append(operation)

        script = operations_to_script(script_operations, create_nest=create_nest)

        try:
            search_response = await self.client.search(
                index=index_alias_by_collection_id(collection_id),
                body={
                    "query": {"term": {"_id": mk_item_id(item_id, collection_id)}},
                    "size": 1,
                },
            )
            if search_response["hits"]["total"]["value"] == 0:
                raise NotFoundError(
                    f"Item {item_id} does not exist inside Collection {collection_id}"
                )
            document_index = search_response["hits"]["hits"][0]["_index"]
            await self.client.update(
                index=document_index,
                id=mk_item_id(item_id, collection_id),
                body={"script": script},
                refresh=True,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist inside Collection {collection_id}"
            )
        except exceptions.RequestError as exc:
            raise HTTPException(
                status_code=400, detail=exc.info["error"]["caused_by"]
            ) from exc

        item = await self.get_one_item(collection_id, item_id)

        if new_collection_id:
            await self.client.reindex(
                body={
                    "dest": {"index": f"{ITEMS_INDEX_PREFIX}{new_collection_id}"},
                    "source": {
                        "index": f"{ITEMS_INDEX_PREFIX}{collection_id}",
                        "query": {"term": {"id": {"value": item_id}}},
                    },
                    "script": {
                        "lang": "painless",
                        "source": (
                            f"""ctx._id = ctx._id.replace('{collection_id}', '{new_collection_id}');"""
                            f"""ctx._source.collection = '{new_collection_id}';"""
                        ),
                    },
                },
                wait_for_completion=True,
                refresh=True,
            )

            await self.delete_item(
                item_id=item_id,
                collection_id=collection_id,
                refresh=refresh,
            )

            item["collection"] = new_collection_id
            collection_id = new_collection_id

        if new_item_id:
            item["id"] = new_item_id
            item = await self.async_prep_create_item(item=item, base_url=base_url)
            await self.create_item(item=item, refresh=True)

            await self.delete_item(
                item_id=item_id,
                collection_id=collection_id,
                refresh=refresh,
            )

        return item

    async def delete_item(self, item_id: str, collection_id: str, **kwargs: Any):
        """Delete a single item from the database.

        Args:
            item_id (str): The id of the Item to be deleted.
            collection_id (str): The id of the Collection that the Item belongs to.
            **kwargs: Additional keyword arguments like refresh.

        Raises:
            NotFoundError: If the Item does not exist in the database.
        """
        # Ensure kwargs is a dictionary
        kwargs = kwargs or {}

        # Resolve the `refresh` parameter
        refresh = kwargs.get("refresh", self.async_settings.database_refresh)
        refresh = validate_refresh(refresh)

        # Log the deletion attempt
        logger.info(
            f"Deleting item {item_id} from collection {collection_id} with refresh={refresh}"
        )

        try:
            await self.client.delete_by_query(
                index=index_alias_by_collection_id(collection_id),
                body={"query": {"term": {"_id": mk_item_id(item_id, collection_id)}}},
                refresh=refresh,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )

    async def get_items_mapping(self, collection_id: str) -> Dict[str, Any]:
        """Get the mapping for the specified collection's items index.

        Args:
            collection_id (str): The ID of the collection to get items mapping for.

        Returns:
            Dict[str, Any]: The mapping information.
        """
        index_name = index_alias_by_collection_id(collection_id)
        try:
            mapping = await self.client.indices.get_mapping(
                index=index_name, params={"allow_no_indices": "false"}
            )
            return mapping
        except exceptions.NotFoundError:
            raise NotFoundError(f"Mapping for index {index_name} not found")

    async def get_items_unique_values(
        self, collection_id: str, field_names: Iterable[str], *, limit: int = 100
    ) -> Dict[str, List[str]]:
        """Get the unique values for the given fields in the collection."""
        limit_plus_one = limit + 1
        index_name = index_alias_by_collection_id(collection_id)

        query = await self.client.search(
            index=index_name,
            body={
                "size": 0,
                "aggs": {
                    field: {"terms": {"field": field, "size": limit_plus_one}}
                    for field in field_names
                },
            },
        )

        result: Dict[str, List[str]] = {}
        for field, agg in query["aggregations"].items():
            if len(agg["buckets"]) > limit:
                logger.warning(
                    "Skipping enum field %s: exceeds limit of %d unique values. "
                    "Consider excluding this field from enumeration or increase the limit.",
                    field,
                    limit,
                )
                continue
            result[field] = [bucket["key"] for bucket in agg["buckets"]]
        return result

    async def create_collection(self, collection: Collection, **kwargs: Any):
        """Create a single collection in the database.

        Args:
            collection (Collection): The Collection object to be created.
            **kwargs: Additional keyword arguments like refresh.

        Raises:
            ConflictError: If a Collection with the same id already exists in the database.

        Notes:
            A new index is created for the items in the Collection using the `create_item_index` function.
        """
        collection_id = collection["id"]

        # Ensure kwargs is a dictionary
        kwargs = kwargs or {}

        # Resolve the `refresh` parameter
        refresh = kwargs.get("refresh", self.async_settings.database_refresh)
        refresh = validate_refresh(refresh)

        # Log the creation attempt
        logger.info(f"Creating collection {collection_id} with refresh={refresh}")

        if await self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise ConflictError(f"Collection {collection_id} already exists")

        await self.client.index(
            index=COLLECTIONS_INDEX,
            id=collection_id,
            body=collection,
            refresh=refresh,
        )
        if self.async_index_inserter.should_create_collection_index():
            await self.async_index_inserter.create_simple_index(
                self.client, collection_id
            )

    async def find_collection(self, collection_id: str) -> Collection:
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
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")

        return collection["_source"]

    async def update_collection(
        self, collection_id: str, collection: Collection, **kwargs: Any
    ):
        """Update a collection from the database.

        Args:
            collection_id (str): The ID of the collection to be updated.
            collection (Collection): The Collection object to be used for the update.
            **kwargs: Additional keyword arguments like refresh.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not
            found in the database.

        Notes:
            This function updates the collection in the database using the specified
            `collection_id` and with the collection specified in the `Collection` object.
            If the collection is not found, a `NotFoundError` is raised.
        """
        # Ensure kwargs is a dictionary
        kwargs = kwargs or {}

        # Resolve the `refresh` parameter
        refresh = kwargs.get("refresh", self.async_settings.database_refresh)
        refresh = validate_refresh(refresh)

        # Log the update attempt
        logger.info(f"Updating collection {collection_id} with refresh={refresh}")

        await self.find_collection(collection_id=collection_id)

        if collection_id != collection["id"]:
            logger.info(
                f"Collection ID change detected: {collection_id} -> {collection['id']}"
            )

            await self.create_collection(collection, refresh=refresh)

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

            await self.delete_collection(collection_id=collection_id, **kwargs)

        else:
            await self.client.index(
                index=COLLECTIONS_INDEX,
                id=collection_id,
                body=collection,
                refresh=refresh,
            )

    async def merge_patch_collection(
        self,
        collection_id: str,
        collection: PartialCollection,
        base_url: str,
        refresh: bool = True,
    ) -> Collection:
        """Database logic for merge patching a collection following RF7396.

        Args:
            collection_id(str): Id of collection to be patched.
            collection (PartialCollection): The partial collection to be updated.
            base_url: (str): The base URL used for constructing links.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to True.


        Returns:
            patched collection.
        """
        operations = merge_to_operations(collection.model_dump())

        return await self.json_patch_collection(
            collection_id=collection_id,
            operations=operations,
            base_url=base_url,
            create_nest=True,
            refresh=refresh,
        )

    async def json_patch_collection(
        self,
        collection_id: str,
        operations: List[PatchOperation],
        base_url: str,
        create_nest: bool = False,
        refresh: bool = True,
    ) -> Collection:
        """Database logic for json patching a collection following RF6902.

        Args:
            collection_id(str): Id of collection to be patched.
            operations (list): List of operations to run.
            base_url (str): The base URL used for constructing links.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to True.

        Returns:
            patched collection.
        """
        new_collection_id = None
        script_operations = []

        for operation in operations:
            if (
                operation.op in ["add", "replace"]
                and operation.path == "collection"
                and collection_id != operation.value
            ):
                new_collection_id = operation.value

            else:
                script_operations.append(operation)

        script = operations_to_script(script_operations, create_nest=create_nest)

        try:
            await self.client.update(
                index=COLLECTIONS_INDEX,
                id=collection_id,
                body={"script": script},
                refresh=True,
            )

        except exceptions.RequestError as exc:
            raise HTTPException(
                status_code=400, detail=exc.info["error"]["caused_by"]
            ) from exc

        collection = await self.find_collection(collection_id)

        if new_collection_id:
            collection["id"] = new_collection_id
            collection["links"] = resolve_links([], base_url)

            await self.update_collection(
                collection_id=collection_id,
                collection=collection,
                refresh=refresh,
            )

        return collection

    async def delete_collection(self, collection_id: str, **kwargs: Any):
        """Delete a collection from the database.

        Parameters:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be deleted.
            **kwargs: Additional keyword arguments like refresh.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.

        Notes:
            This function first verifies that the collection with the specified `collection_id` exists in the database, and then
            deletes the collection. If `refresh` is set to "true", "false", or "wait_for", the index is refreshed accordingly after
            the deletion. Additionally, this function also calls `delete_item_index` to delete the index for the items in the collection.
        """
        # Ensure kwargs is a dictionary
        kwargs = kwargs or {}

        await self.find_collection(collection_id=collection_id)

        # Resolve the `refresh` parameter
        refresh = kwargs.get("refresh", self.async_settings.database_refresh)
        refresh = validate_refresh(refresh)

        # Log the deletion attempt
        logger.info(f"Deleting collection {collection_id} with refresh={refresh}")

        await self.client.delete(
            index=COLLECTIONS_INDEX, id=collection_id, refresh=refresh
        )
        # Delete the item index for the collection
        await delete_item_index(collection_id)

    async def bulk_async(
        self,
        collection_id: str,
        processed_items: List[Item],
        **kwargs: Any,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Perform a bulk insert of items into the database asynchronously.

        Args:
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            **kwargs (Any): Additional keyword arguments, including:
                - refresh (str, optional): Whether to refresh the index after the bulk insert.
                Can be "true", "false", or "wait_for". Defaults to the value of `self.sync_settings.database_refresh`.
                - refresh (bool, optional): Whether to refresh the index after the bulk insert.
                - raise_on_error (bool, optional): Whether to raise an error if any of the bulk operations fail.
                Defaults to the value of `self.async_settings.raise_on_bulk_error`.

        Returns:
            Tuple[int, List[Dict[str, Any]]]: A tuple containing:
                - The number of successfully processed actions (`success`).
                - A list of errors encountered during the bulk operation (`errors`).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`.
            The insert is performed synchronously and blocking, meaning that the function does not return until the insert has
            completed. The `mk_actions` function is called to generate a list of actions for the bulk insert. The `refresh`
            parameter determines whether the index is refreshed after the bulk insert:
                - "true": Forces an immediate refresh of the index.
                - "false": Does not refresh the index immediately (default behavior).
                - "wait_for": Waits for the next refresh cycle to make the changes visible.
        """
        # Ensure kwargs is a dictionary
        kwargs = kwargs or {}

        # Resolve the `refresh` parameter
        refresh = kwargs.get("refresh", self.async_settings.database_refresh)
        refresh = validate_refresh(refresh)

        # Log the bulk insert attempt
        logger.info(
            f"Performing bulk insert for collection {collection_id} with refresh={refresh}"
        )

        # Handle empty processed_items
        if not processed_items:
            logger.warning(f"No items to insert for collection {collection_id}")
            return 0, []

        raise_on_error = self.async_settings.raise_on_bulk_error
        actions = await self.async_index_inserter.prepare_bulk_actions(
            collection_id, processed_items
        )

        success, errors = await helpers.async_bulk(
            self.client,
            actions,
            refresh=refresh,
            raise_on_error=raise_on_error,
        )
        # Log the result
        logger.info(
            f"Bulk insert completed for collection {collection_id}: {success} successes, {len(errors)} errors"
        )
        return success, errors

    def bulk_sync(
        self,
        collection_id: str,
        processed_items: List[Item],
        **kwargs: Any,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Perform a bulk insert of items into the database asynchronously.

        Args:
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            **kwargs (Any): Additional keyword arguments, including:
                - refresh (str, optional): Whether to refresh the index after the bulk insert.
                Can be "true", "false", or "wait_for". Defaults to the value of `self.sync_settings.database_refresh`.
                - refresh (bool, optional): Whether to refresh the index after the bulk insert.
                - raise_on_error (bool, optional): Whether to raise an error if any of the bulk operations fail.
                Defaults to the value of `self.async_settings.raise_on_bulk_error`.

        Returns:
            Tuple[int, List[Dict[str, Any]]]: A tuple containing:
                - The number of successfully processed actions (`success`).
                - A list of errors encountered during the bulk operation (`errors`).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`.
            The insert is performed synchronously and blocking, meaning that the function does not return until the insert has
            completed. The `mk_actions` function is called to generate a list of actions for the bulk insert. The `refresh`
            parameter determines whether the index is refreshed after the bulk insert:
                - "true": Forces an immediate refresh of the index.
                - "false": Does not refresh the index immediately (default behavior).
                - "wait_for": Waits for the next refresh cycle to make the changes visible.
        """
        # Ensure kwargs is a dictionary
        kwargs = kwargs or {}

        # Resolve the `refresh` parameter
        refresh = kwargs.get("refresh", self.async_settings.database_refresh)
        refresh = validate_refresh(refresh)

        # Log the bulk insert attempt
        logger.info(
            f"Performing bulk insert for collection {collection_id} with refresh={refresh}"
        )

        # Handle empty processed_items
        if not processed_items:
            logger.warning(f"No items to insert for collection {collection_id}")
            return 0, []

        # Handle empty processed_items
        if not processed_items:
            logger.warning(f"No items to insert for collection {collection_id}")
            return 0, []

        raise_on_error = self.sync_settings.raise_on_bulk_error
        success, errors = helpers.bulk(
            self.sync_client,
            mk_actions(collection_id, processed_items),
            refresh=refresh,
            raise_on_error=raise_on_error,
        )
        return success, errors

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
