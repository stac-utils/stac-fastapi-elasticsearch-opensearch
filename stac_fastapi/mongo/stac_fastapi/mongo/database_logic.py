"""Database logic."""
import base64
import json
import logging
import os
from base64 import urlsafe_b64decode
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple, Type, Union

import attr
from bson import ObjectId
from pymongo.errors import (  # CollectionInvalid,
    BulkWriteError,
    DuplicateKeyError,
    PyMongoError,
)

from stac_fastapi.core import serializers
from stac_fastapi.core.extensions import filter
from stac_fastapi.core.utilities import bbox2polygon
from stac_fastapi.mongo.config import AsyncMongoDBSettings as AsyncSearchSettings
from stac_fastapi.mongo.config import MongoDBSettings as SyncSearchSettings
from stac_fastapi.mongo.utilities import serialize_doc
from stac_fastapi.types.errors import ConflictError, NotFoundError
from stac_fastapi.types.stac import Collection, Item

logger = logging.getLogger(__name__)

NumType = Union[float, int]

COLLECTIONS_INDEX = os.getenv("STAC_COLLECTIONS_INDEX", "collections")
ITEMS_INDEX = os.getenv("STAC_ITEMS_INDEX", "items")
ITEMS_INDEX_PREFIX = os.getenv("STAC_ITEMS_INDEX_PREFIX", "items_")
DATABASE = os.getenv("MONGO_DB", "admin")


def index_by_collection_id(collection_id: str) -> str:
    """
    Translate a collection id into an Elasticsearch index name.

    Args:
        collection_id (str): The collection id to translate into an index name.

    Returns:
        str: The index name derived from the collection id.
    """
    unsupported_chars = set('/\\ ."*<>:|?$')
    sanitized = "".join(c for c in collection_id if c not in unsupported_chars)
    return f"{ITEMS_INDEX_PREFIX}{sanitized.lower()}"


def indices(collection_ids: Optional[List[str]]) -> str:
    """
    Get a comma-separated string of index names for a given list of collection ids.

    Args:
        collection_ids: A list of collection ids.

    Returns:
        A string of comma-separated index names. If `collection_ids` is None, returns the default indices.
    """
    if collection_ids is None:
        return COLLECTIONS_INDEX
    else:
        return ",".join([index_by_collection_id(c) for c in collection_ids])


async def create_collection_index():
    """
    Ensure indexes for the collections collection in MongoDB using the asynchronous client.

    Returns:
        None
    """
    client = AsyncSearchSettings().create_client
    if client:
        try:
            db = client[DATABASE]
            await db[COLLECTIONS_INDEX].create_index([("id", 1)], unique=True)
            print("Index created successfully.")
        except Exception as e:
            print(f"An error occurred while creating the index: {e}")
        finally:
            print(f"Closing client: {client}")
            client.close()
    else:
        print("Failed to create MongoDB client.")


async def create_item_index(collection_id: str):
    """
    Ensure indexes for a specific collection of items in MongoDB using the asynchronous client.

    Args:
        collection_id (str): Collection identifier used to derive the MongoDB collection name for items.

    Returns:
        None
    """
    client = AsyncSearchSettings.create_client
    db = client[DATABASE]

    # Derive the collection name for items based on the collection_id
    collection_name = index_by_collection_id(collection_id)

    try:
        await db[collection_name].create_index([("properties.datetime", -1)])
        await db[collection_name].create_index([("id", 1)], unique=True)
        await db[collection_name].create_index([("geometry", "2dsphere")])
        print(f"Indexes created successfully for collection: {collection_name}.")
    except Exception as e:
        # Handle exceptions, which could be due to existing index conflicts, etc.
        print(
            f"An error occurred while creating indexes for collection {collection_name}: {e}"
        )
    finally:
        await client.close()


async def delete_item_index(collection_id: str):
    """
    Drop the MongoDB collection corresponding to the specified collection ID.

    This operation is the MongoDB equivalent of deleting an Elasticsearch index, removing both the data and
    the structure for the specified collection's items.

    Args:
        collection_id (str): The ID of the collection whose associated MongoDB collection will be dropped.
    """
    client = AsyncSearchSettings.create_client
    db = client[DATABASE]

    # Derive the MongoDB collection name using the collection ID
    collection_name = index_by_collection_id(collection_id)

    try:
        # Drop the collection, removing both its data and structure
        await db[collection_name].drop()
        logger.info(f"Collection '{collection_name}' successfully dropped.")
    except Exception as e:
        logger.error(f"Error dropping collection '{collection_name}': {e}")
    finally:
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


class Geometry(Protocol):  # noqa
    type: str
    coordinates: Any


class MongoSearchAdapter:
    """
    Adapter class to manage search filters and sorting for MongoDB queries.

    Attributes:
        filters (list): A list of filter conditions to be applied to the MongoDB query.
        sort (list): A list of tuples specifying field names and their corresponding sort directions
                     for MongoDB sorting.

    Methods:
        add_filter(filter_condition): Adds a new filter condition to the filters list.
        set_sort(sort_conditions): Sets the sorting criteria based on a dictionary of field names
                                   and sort directions.
    """

    def __init__(self):
        """
        Initialize the MongoSearchAdapter with default sorting criteria.

        The default sort order is by 'properties.datetime' in descending order, followed by 'id' in descending order,
        and finally by 'collection' in descending order. This matches typical STAC item queries where the most recent items
        are retrieved first.
        """
        self.filters = []
        # MongoDB uses a list of tuples for sorting: [('field1', direction), ('field2', direction)]
        # Convert the DEFAULT_SORT dict to this format, considering MongoDB's sorting capabilities
        self.sort = [("properties.datetime", -1), ("id", -1), ("collection", -1)]

    def add_filter(self, filter_condition):
        """
        Add a filter condition to the query.

        This method appends a new filter condition to the list of existing filters. Each filter condition
        should be a dictionary representing a MongoDB query condition.

        Args:
            filter_condition (dict): A dictionary representing a MongoDB filter condition.
        """
        self.filters.append(filter_condition)

    def set_sort(self, sort_conditions):
        """
        Set the sorting criteria for the query based on provided conditions.

        This method translates a dictionary of field names and sort directions (asc or desc) into MongoDB's
        format for sorting queries. It overwrites any existing sort criteria with the new criteria provided.

        Args:
            sort_conditions (dict): A dictionary where keys are field names and values are dictionaries
                                    indicating sort direction ('asc' for ascending or 'desc' for descending).
        """
        self.sort = []
        for field, details in sort_conditions.items():
            direction = 1 if details["order"] == "asc" else -1
            self.sort.append((field, direction))


@attr.s
class DatabaseLogic:
    """Database logic."""

    client = AsyncSearchSettings().create_client
    sync_client = SyncSearchSettings().create_client

    item_serializer: Type[serializers.ItemSerializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.CollectionSerializer] = attr.ib(
        default=serializers.CollectionSerializer
    )

    """CORE LOGIC"""

    async def get_all_collections(
        self,
        token: Optional[str],
        limit: int,
    ) -> Iterable[Dict[str, Any]]:
        """Retrieve a list of all collections from the database.

        Args:
            token (Optional[str]): The token used to return the next set of results.
            limit (int): Number of results to return

        Returns:
            collections (Iterable[Dict[str, Any]]): A list of dictionaries containing the source data for each collection.

        Notes:
            The collections are retrieved from the Elasticsearch database using the `client.search` method,
            with the `COLLECTIONS_INDEX` as the target index and `size=limit` to retrieve records.
            The result is a generator of dictionaries containing the source data for each collection.
        """
        db = self.client[DATABASE]
        collections_collection = db[COLLECTIONS_INDEX]

        query: Dict[str, Any] = {}

        if token:
            # Assuming token is the last seen item ID; adjust based on your pagination strategy
            last_seen_id = json.loads(urlsafe_b64decode(token.encode()).decode())
            query = {"id": {"$gt": last_seen_id}}

        cursor = collections_collection.find(query).sort("id", 1).limit(limit)
        collections = []
        async for collection in cursor:
            collections.append(collection)

        return collections

    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
        """Retrieve a single item from the database.

        Args:
            collection_id (str): The id of the Collection that the Item belongs to.
            item_id (str): The id of the Item.

        Returns:
            item (Dict): A dictionary containing the source data for the Item.

        Raises:
            NotFoundError: If the specified Item does not exist in the Collection.

        """
        db = self.client[DATABASE]
        collection_name = index_by_collection_id(collection_id)

        try:
            # Attempt to find the item in the specified collection
            item = await db[collection_name].find_one({"id": item_id})
            if not item:
                # If the item is not found, raise NotFoundError
                raise NotFoundError(
                    f"Item {item_id} does not exist in Collection {collection_id}"
                )
            return item
        except Exception as e:
            # Log and re-raise any exceptions encountered during the operation
            logger.error(
                f"An error occurred while retrieving item {item_id} from collection {collection_id}: {e}"
            )
            raise

    @staticmethod
    def make_search():
        """Database logic to create a Search instance."""
        # return Search().sort(*DEFAULT_SORT)
        return MongoSearchAdapter()

    @staticmethod
    def apply_ids_filter(search: MongoSearchAdapter, item_ids: List[str]):
        """Database logic to search a list of STAC item ids."""
        search.add_filter({"_id": {"$in": item_ids}})
        return search

    @staticmethod
    def apply_collections_filter(search: MongoSearchAdapter, collection_ids: List[str]):
        """Database logic to search a list of STAC collection ids."""
        search.add_filter({"collection": {"$in": collection_ids}})
        return search

    @staticmethod
    def apply_datetime_filter(search: MongoSearchAdapter, datetime_search):
        """Apply a filter to search based on datetime field.

        Args:
            search (Search): The search object to filter.
            datetime_search (dict): The datetime filter criteria.

        Returns:
            Search: The filtered search object.
        """
        if "eq" in datetime_search:
            search.add_filter({"properties.datetime": datetime_search["eq"]})
        else:
            if "gte" in datetime_search:
                search.add_filter(
                    {"properties.datetime": {"$gte": datetime_search["gte"]}}
                )
            if "lte" in datetime_search:
                search.add_filter(
                    {"properties.datetime": {"$lte": datetime_search["lte"]}}
                )
        return search

    @staticmethod
    def apply_bbox_filter(search: MongoSearchAdapter, bbox: List):
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
        geojson_polygon = {"type": "Polygon", "coordinates": bbox2polygon(*bbox)}
        return search.add_filter(
            {
                "geometry": {
                    "$geoIntersects": {
                        "$geometry": geojson_polygon,
                    }
                }
            }
        )

    @staticmethod
    def apply_intersects_filter(
        search: MongoSearchAdapter,
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
        return search.add_filter(
            {"geometry": {"$geoIntersects": {"$geometry": intersects}}}
        )

    @staticmethod
    def apply_stacql_filter(
        search: MongoSearchAdapter, op: str, field: str, value: float
    ):
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
        # MongoDB comparison operators mapping
        op_mapping = {
            "eq": "$eq",
            "gt": "$gt",
            "gte": "$gte",
            "lt": "$lt",
            "lte": "$lte",
        }

        # Construct the MongoDB filter
        if op in op_mapping:
            mongo_op = op_mapping[op]
            filter_condition = {field: {mongo_op: value}}
        else:
            raise ValueError(f"Unsupported operation '{op}'")

        # Add the constructed filter to the search adapter's filters
        return search.add_filter(filter_condition)

    @staticmethod
    def translate_clause_to_mongo(clause: filter.Clause) -> dict:
        """Translate a CQL2 Clause object to a MongoDB query.

        Args:
            clause (Clause): The Clause object to translate.

        Returns:
            dict: The translated MongoDB query.
        """
        # This function needs to recursively translate CQL2 Clauses to MongoDB queries
        # Here we demonstrate a simple example of handling an "eq" operator
        if clause.op == filter.ComparisonOp.eq:
            # Direct translation of an "eq" operation to MongoDB's query syntax
            return {clause.args[0].property: {"$eq": clause.args[1]}}
        elif clause.op == filter.SpatialIntersectsOp.s_intersects:
            # Example of handling a spatial intersects operation
            return {
                clause.args[0].property: {
                    "$geoIntersects": {
                        "$geometry": clause.args[
                            1
                        ].__geo_interface__  # Assuming args[1] is a GeoJSON-pydantic model
                    }
                }
            }
        # Add additional elif blocks to handle other operators like "lt", "lte", "gt", "gte", "neq", etc.
        else:
            raise NotImplementedError(
                f"Operator {clause.op} not implemented for MongoDB translation."
            )

    @staticmethod
    def apply_cql2_filter(
        search_adapter: MongoSearchAdapter, _filter: Optional[filter.Clause]
    ):
        """Adapt database logic to apply a CQL2 filter for MongoDB search endpoint.

        Args:
            search_adapter (MongoSearchAdapter): The search adapter to which the filter will be applied.
            _filter (Optional[Clause]): A Clause representing the filter criteria.

        Returns:
            MongoSearchAdapter: The search adapter with the filter applied.
        """
        if _filter is None:
            return search_adapter

        # Translating the CQL2 Clause to a MongoDB query
        try:
            # Assuming _filter is a Clause object as defined above
            mongo_query = DatabaseLogic.translate_clause_to_mongo(_filter)
            search_adapter.add_filter(mongo_query)
        except Exception as e:
            # Handle translation errors or unsupported features
            print(f"Error translating CQL2 Clause to MongoDB query: {e}")

        return search_adapter

    @staticmethod
    def populate_sort(sortby: List[Dict[str, str]]) -> List[Tuple[str, int]]:
        """
        Transform a list of sort criteria into the format expected by MongoDB.

        Args:
            sortby (List[Dict[str, str]]): A list of dictionaries with 'field' and 'direction' keys, where
                                        'direction' can be 'asc' for ascending or 'desc' for descending.

        Returns:
            List[Tuple[str, int]]: A list of tuples where each tuple is (fieldname, direction), with
                                direction being 1 for 'asc' and -1 for 'desc'. Returns an empty list
                                if no sort criteria are provided.
        """
        if not sortby:
            return []

        # MongoDB expects a list of tuples for sorting. Each tuple is (Field Name, Direction)
        # where Direction is 1 for ascending and -1 for descending.
        mongo_sort = []
        for sort_field in sortby:
            field = sort_field["field"]  # The field name to sort by.
            # Convert the direction to MongoDB's expected format.
            direction = 1 if sort_field["direction"].lower() == "asc" else -1
            mongo_sort.append((field, direction))

        return mongo_sort

    async def execute_search(
        self,
        search: MongoSearchAdapter,
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
        db = self.client[DATABASE]
        collection = db["stac_items"]
        query = {"$and": search.filters} if search.filters else {}

        if collection_ids:
            query["collection"] = {"$in": collection_ids}

        sort_criteria = search.sort if search.sort else [("_id", 1)]  # Default sort

        try:
            if token:
                last_id = ObjectId(base64.urlsafe_b64decode(token.encode()).decode())
                query["_id"] = {"$gt": last_id}

            cursor = collection.find(query).sort(sort_criteria).limit(limit + 1)
            items = await cursor.to_list(length=limit + 1)

            next_token = None
            if len(items) > limit:
                next_token = base64.urlsafe_b64encode(
                    str(items[-1]["_id"]).encode()
                ).decode()
                items = items[:-1]

            maybe_count = None
            if not token:
                maybe_count = await collection.count_documents(query)

            return items, maybe_count, next_token
        except PyMongoError as e:
            print(f"Database operation failed: {e}")
            raise

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str):
        """
        Check if a specific collection exists in the MongoDB database.

        This method uses MongoDB's `list_collection_names` command with a filter
        to efficiently determine if a collection with the specified name exists.
        It is more efficient than retrieving all collection names and searching through
        them, especially beneficial in databases with a large number of collections.

        Args:
            collection_id (str): The name of the collection to check for existence.

        Raises:
            NotFoundError: If the collection specified by `collection_id` does not exist
                        in the database.

        Note:
            The `NotFoundError` should be appropriately defined or imported in your
            application to handle cases where the specified collection does not exist.
        """
        db = self.client[DATABASE]

        # Check for the collection's existence by filtering list_collection_names
        collections = db.list_collection_names(filter={"name": collection_id})
        if not collections:
            raise NotFoundError(f"Collection {collection_id} does not exist")

    async def prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Preps an item for insertion into the MongoDB database.

        Args:
            item (Item): The item to be prepped for insertion.
            base_url (str): The base URL used to create the item's self URL.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The prepped item.

        Raises:
            ConflictError: If the item already exists in the database and exist_ok is False.
            NotFoundError: If the collection specified by the item does not exist.
        """
        db = self.client[DATABASE]
        collections_collection = db[COLLECTIONS_INDEX]
        items_collection = db[ITEMS_INDEX]

        # Check if the collection exists
        collection_exists = await collections_collection.count_documents(
            {"id": item["collection"]}, limit=1
        )
        if not collection_exists:
            raise NotFoundError(f"Collection {item['collection']} does not exist")

        # Transform item using item_serializer for MongoDB compatibility
        mongo_item = self.item_serializer.stac_to_db(item, base_url)

        if not exist_ok:
            existing_item = await items_collection.find_one({"id": mongo_item["id"]})
            if existing_item:
                raise ConflictError(
                    f"Item {mongo_item['id']} in collection {mongo_item['collection']} already exists"
                )

        # Return the transformed item ready for insertion
        return mongo_item

    def sync_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Preps an item for insertion into the MongoDB database in a synchronous manner.

        Args:
            item (Item): The item to be prepped for insertion.
            base_url (str): The base URL used to create the item's self URL.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The prepped item.

        Raises:
            ConflictError: If the item already exists in the database and exist_ok is False.
            NotFoundError: If the collection specified by the item does not exist.
        """
        db = self.client[DATABASE]
        collections_collection = db[COLLECTIONS_INDEX]
        items_collection = db[index_by_collection_id(item.collection)]

        # Check if the collection exists
        collection_exists = collections_collection.count_documents(
            {"id": item.collection}, limit=1
        )
        if not collection_exists:
            raise NotFoundError(f"Collection {item.collection} does not exist")

        # Transform item using item_serializer for MongoDB compatibility
        mongo_item = self.item_serializer.stac_to_db(item, base_url)

        if not exist_ok:
            existing_item = items_collection.find_one({"id": mongo_item["id"]})
            if existing_item:
                raise ConflictError(
                    f"Item {mongo_item['id']} in collection {mongo_item['collection']} already exists"
                )

        # Return the transformed item ready for insertion
        return mongo_item

    async def create_item(self, item: Item, refresh: bool = False):
        """
        Asynchronously inserts a STAC item into MongoDB, ensuring the item does not already exist.

        Args:
            item (Item): The STAC item to be created.
            refresh (bool, optional): Not used for MongoDB, kept for compatibility with Elasticsearch interface.

        Raises:
            ConflictError: If the item with the same ID already exists within the collection.
            NotFoundError: If the specified collection does not exist in MongoDB.
        """
        db = self.client[DATABASE]
        items_collection = db[ITEMS_INDEX]

        # Convert STAC Item to a dictionary, preserving all its fields
        # item_dict = item.dict(by_alias=True)

        # Ensure the collection exists
        collections_collection = db[COLLECTIONS_INDEX]
        collection_exists = await collections_collection.count_documents(
            {"id": item["collection"]}, limit=1
        )
        if collection_exists == 0:
            raise NotFoundError(f"Collection {item['collection']} does not exist")

        # Attempt to insert the item, checking for duplicates
        try:
            await items_collection.insert_one(item)
        except DuplicateKeyError:
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

    async def delete_item(
        self, item_id: str, collection_id: str, refresh: bool = False
    ):
        """
        Delete a single item from the database.

        Args:
            item_id (str): The id of the Item to be deleted.
            collection_id (str): The id of the Collection that the Item belongs to.
            refresh (bool, optional): Whether to refresh the index after the deletion. Default is False.

        Raises:
            NotFoundError: If the Item does not exist in the database.
        """
        db = self.client[DATABASE]
        collection_name = index_by_collection_id(
            collection_id
        )  # Derive the MongoDB collection name
        collection = db[collection_name]

        try:
            # Attempt to delete the item from the collection
            result = await collection.delete_one({"id": item_id})
            if result.deleted_count == 0:
                # If no items were deleted, it means the item did not exist
                raise NotFoundError(
                    f"Item {item_id} in collection {collection_id} not found"
                )
        except PyMongoError as e:
            # Catch any MongoDB error and re-raise as NotFoundError for consistency with the original function's behavior
            raise NotFoundError(
                f"Error deleting item {item_id} in collection {collection_id}: {e}"
            )

    async def create_collection(self, collection: Collection, refresh: bool = False):
        """Create a single collection document in the database.

        Args:
            collection (Collection): The Collection object to be created.
            refresh (bool, optional): Whether to refresh the index after the creation. Default is False.

        Raises:
            ConflictError: If a Collection with the same id already exists in the database.
        """
        db = self.client[DATABASE]
        collections_collection = db[COLLECTIONS_INDEX]

        # Check if the collection already exists
        existing_collection = await collections_collection.find_one(
            {"id": collection["id"]}
        )
        if existing_collection:
            raise ConflictError(f"Collection {collection['id']} already exists")

        try:
            # Insert the new collection document into the collections collection
            await collections_collection.insert_one(collection)
        except PyMongoError as e:
            # Catch any MongoDB error and raise an appropriate error
            print(f"Failed to create collection {collection['id']}: {e}")
            raise ConflictError(f"Failed to create collection {collection['id']}: {e}")

    async def find_collection(self, collection_id: str) -> dict:
        """
        Find and return a collection from the database.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be found.

        Returns:
            dict: The found collection, represented as a dictionary.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.
        """
        db = self.client[DATABASE]
        collections_collection = db[COLLECTIONS_INDEX]

        try:
            collection = await collections_collection.find_one({"id": collection_id})
            if not collection:
                raise NotFoundError(f"Collection {collection_id} not found")
            serialized_collection = serialize_doc(collection)
            print("HELLO")
            return serialized_collection
        except PyMongoError as e:
            # This is a general catch-all for MongoDB errors; adjust as needed for more specific handling
            print(f"Failed to find collection {collection_id}: {e}")
            raise NotFoundError(f"Collection {collection_id} not found")

    async def update_collection(
        self, collection_id: str, collection: Collection, refresh: bool = False
    ):
        """
        Update a collection in the MongoDB database.

        Args:
            collection_id (str): The ID of the collection to be updated.
            collection (Collection): The new collection data to update.
            refresh (bool): Not applicable for MongoDB, kept for compatibility.

        Raises:
            NotFoundError: If the collection with the specified ID does not exist.
            ConflictError: If attempting to change the collection ID to one that already exists.
        """
        db = self.client[DATABASE]
        collections_collection = db[COLLECTIONS_INDEX]

        existing_collection = await self.find_collection(collection_id)
        if not existing_collection:
            raise NotFoundError(f"Collection {collection_id} not found")

        if collection_id != collection["id"]:
            # Check if the new ID already exists
            new_id_exists = await collections_collection.find_one(
                {"id": collection["id"]}
            )
            if new_id_exists:
                raise ConflictError(
                    f"Collection with ID {collection['id']} already exists"
                )

            # Update the collection ID in all related documents/items
            items_collection = db[ITEMS_INDEX_PREFIX + collection_id]
            await items_collection.update_many(
                {}, {"$set": {"collection": collection["id"]}}
            )

            # Insert the new collection and delete the old one
            await collections_collection.insert_one(collection)
            await collections_collection.delete_one({"id": collection_id})

            # Optionally, handle renaming or moving documents to a new collection if necessary
        else:
            # Update the existing collection with new data
            await collections_collection.update_one(
                {"id": collection_id}, {"$set": collection}
            )

    async def delete_collection(self, collection_id: str):
        """
        Delete a collection from the MongoDB database and all items associated with it.

        Args:
            collection_id (str): The ID of the collection to be deleted.
        """
        db = self.client[DATABASE]

        # Attempt to delete the collection document
        collection_result = await db["collections"].delete_one({"id": collection_id})
        if collection_result.deleted_count == 0:
            raise NotFoundError(f"Collection {collection_id} not found")

        # Delete all items associated with the collection
        await db["items"].delete_many({"collection": collection_id})

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
        db = self.client[DATABASE]
        items_collection = db["items"]

        # Prepare the documents for insertion
        documents = [item.dict(by_alias=True) for item in processed_items]

        try:
            await items_collection.insert_many(documents, ordered=False)
        except BulkWriteError as e:
            # Handle bulk write errors, e.g., due to duplicate keys
            raise ConflictError(f"Bulk insert operation failed: {e.details}")

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
        db = self.sync_client[DATABASE]
        items_collection = db["items"]

        # Prepare the documents for insertion
        documents = [item.dict(by_alias=True) for item in processed_items]

        try:
            items_collection.insert_many(documents, ordered=False)
        except BulkWriteError as e:
            # Handle bulk write errors, e.g., due to duplicate keys
            raise ConflictError(f"Bulk insert operation failed: {e.details}")

    async def delete_items(self) -> None:
        """
        Danger. this is only for tests.

        Deletes all items from the 'items' collection in MongoDB.
        """
        db = self.client[DATABASE]
        items_collection = db["items"]

        try:
            await items_collection.delete_many({})
            print("All items have been deleted.")
        except Exception as e:
            print(f"Error deleting items: {e}")

    async def delete_collections(self) -> None:
        """
        Danger. this is only for tests.

        Deletes all collections from the 'collections' collection in MongoDB.
        """
        db = self.client[DATABASE]
        collections_collection = db["collections"]

        try:
            await collections_collection.delete_many({})
            print("All collections have been deleted.")
        except Exception as e:
            print(f"Error deleting collections: {e}")
