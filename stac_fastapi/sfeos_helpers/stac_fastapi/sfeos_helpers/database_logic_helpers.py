"""Shared code for elasticsearch/ opensearch database logic.

This module contains shared functions used by both the Elasticsearch and OpenSearch
implementations of STAC FastAPI for database operations. It helps reduce code duplication
and ensures consistent behavior between the two implementations.

The sfeos_helpers package is organized as follows:
- database_logic_helpers.py: Shared database operations (this file)
- filter.py: Shared filter extension implementation
- mappings.py: Shared constants and mapping definitions
- utilities.py: Shared utility functions

When adding new functionality to this package, consider:
1. Will this code be used by both Elasticsearch and OpenSearch implementations?
2. Is the functionality stable and unlikely to diverge between implementations?
3. Is the function well-documented with clear input/output contracts?

Function Naming Conventions:
- All shared functions should end with `_shared` to clearly indicate they're meant to be used by both implementations
- Function names should be descriptive and indicate their purpose
- Parameter names should be consistent across similar functions
"""

from typing import Any, Dict, List, Optional

from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
    ES_ITEMS_SETTINGS,
    ITEMS_INDEX_PREFIX,
    Geometry,
)
from stac_fastapi.sfeos_helpers.utilities import index_alias_by_collection_id

# ============================================================================
# Index Management Functions
# ============================================================================


async def create_index_templates_shared(settings: Any) -> None:
    """Create index templates for Elasticsearch/OpenSearch Collection and Item indices.

    Args:
        settings (Any): The settings object containing the client configuration.
            Must have a create_client attribute that returns an Elasticsearch/OpenSearch client.

    Returns:
        None: This function doesn't return any value but creates index templates in the database.

    Notes:
        This function creates two index templates:
        1. A template for the Collections index with the appropriate mappings
        2. A template for the Items indices with both settings and mappings

        These templates ensure that any new indices created with matching patterns
        will automatically have the correct structure.
    """
    client = settings.create_client
    await client.indices.put_index_template(
        name=f"template_{COLLECTIONS_INDEX}",
        body={
            "index_patterns": [f"{COLLECTIONS_INDEX}*"],
            "template": {"mappings": ES_COLLECTIONS_MAPPINGS},
        },
    )
    await client.indices.put_index_template(
        name=f"template_{ITEMS_INDEX_PREFIX}",
        body={
            "index_patterns": [f"{ITEMS_INDEX_PREFIX}*"],
            "template": {"settings": ES_ITEMS_SETTINGS, "mappings": ES_ITEMS_MAPPINGS},
        },
    )
    await client.close()


async def delete_item_index_shared(settings: Any, collection_id: str) -> None:
    """Delete the index for items in a collection.

    Args:
        settings (Any): The settings object containing the client configuration.
            Must have a create_client attribute that returns an Elasticsearch/OpenSearch client.
        collection_id (str): The ID of the collection whose items index will be deleted.

    Returns:
        None: This function doesn't return any value but deletes an item index in the database.

    Notes:
        This function deletes an item index and its alias. It first resolves the alias to find
        the actual index name, then deletes both the alias and the index.
    """
    client = settings.create_client

    name = index_alias_by_collection_id(collection_id)
    resolved = await client.indices.resolve_index(name=name)
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name)
    await client.close()


# ============================================================================
# Query Building Functions
# ============================================================================


def apply_free_text_filter_shared(
    search: Any, free_text_queries: Optional[List[str]]
) -> Any:
    """Create a free text query for Elasticsearch/OpenSearch.

    Args:
        search (Any): The search object to apply the query to.
        free_text_queries (Optional[List[str]]): A list of text strings to search for in the properties.

    Returns:
        Any: The search object with the free text query applied, or the original search
            object if no free_text_queries were provided.

    Notes:
        This function creates a query_string query that searches for the specified text strings
        in all properties of the documents. The query strings are joined with OR operators.
    """
    if free_text_queries is not None:
        free_text_query_string = '" OR properties.\\*:"'.join(free_text_queries)
        search = search.query(
            "query_string", query=f'properties.\\*:"{free_text_query_string}"'
        )

    return search


def apply_intersects_filter_shared(
    intersects: Geometry,
) -> Dict[str, Dict]:
    """Create a geo_shape filter for intersecting geometry.

    Args:
        intersects (Geometry): The intersecting geometry, represented as a GeoJSON-like object.

    Returns:
        Dict[str, Dict]: A dictionary containing the geo_shape filter configuration
            that can be used with Elasticsearch/OpenSearch Q objects.

    Notes:
        This function creates a geo_shape filter configuration to find documents that intersect
        with the specified geometry. The returned dictionary should be wrapped in a Q object
        when applied to a search.
    """
    return {
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


def populate_sort_shared(sortby: List) -> Optional[Dict[str, Dict[str, str]]]:
    """Create a sort configuration for Elasticsearch/OpenSearch queries.

    Args:
        sortby (List): A list of sort specifications, each containing a field and direction.

    Returns:
        Optional[Dict[str, Dict[str, str]]]: A dictionary mapping field names to sort direction
            configurations, or None if no sort was specified.

    Notes:
        This function transforms a list of sort specifications into the format required by
        Elasticsearch/OpenSearch for sorting query results. The returned dictionary can be
        directly used in search requests.
    """
    if sortby:
        return {s.field: {"order": s.direction} for s in sortby}
    else:
        return None


# ============================================================================
# Mapping Functions
# ============================================================================


async def get_queryables_mapping_shared(
    mappings: Dict[str, Dict[str, Any]], collection_id: str = "*"
) -> Dict[str, str]:
    """Retrieve mapping of Queryables for search.

    Args:
        mappings (Dict[str, Dict[str, Any]]): The mapping information returned from
            Elasticsearch/OpenSearch client's indices.get_mapping() method.
            Expected structure is {index_name: {"mappings": {...}}}.
        collection_id (str, optional): The id of the Collection the Queryables
            belongs to. Defaults to "*".

    Returns:
        Dict[str, str]: A dictionary containing the Queryables mappings, where keys are
            field names and values are the corresponding paths in the Elasticsearch/OpenSearch
            document structure.
    """
    queryables_mapping = {}

    for mapping in mappings.values():
        fields = mapping["mappings"].get("properties", {})
        properties = fields.pop("properties", {}).get("properties", {}).keys()

        for field_key in fields:
            queryables_mapping[field_key] = field_key

        for property_key in properties:
            queryables_mapping[property_key] = f"properties.{property_key}"

    return queryables_mapping
