"""Query building functions for Elasticsearch/OpenSearch.

This module provides functions for building and manipulating Elasticsearch/OpenSearch queries.
"""

from typing import Any, Dict, List, Optional

from stac_fastapi.sfeos_helpers.mappings import Geometry

ES_MAX_URL_LENGTH = 4096


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
        Always includes 'id' as secondary sort to ensure unique pagination tokens.
    """
    if sortby:
        sort_config = {s.field: {"order": s.direction} for s in sortby}
        sort_config.setdefault("id", {"order": "asc"})
        return sort_config
    else:
        return {"id": {"order": "asc"}}


def add_collections_to_body(
    collection_ids: List[str], query: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Add a list of collection ids to the body of a query.

    Args:
        collection_ids (List[str]): A list of collections ids.
        query (Optional[Dict[str, Any]]): The query to add collections to. If none, create a query that filters
        the collection ids.

    Returns:
        Dict[str, Any]: A query that contains a filter on the given collection ids.

    Notes:
        This function is needed in the execute_search function when the size of the URL path will exceed the maximum of ES.
    """
    index_filter = {"terms": {"collection": collection_ids}}
    if query is None:
        query = {"query": {}}
    if "bool" not in query:
        query["bool"] = {}
    if "filter" not in query["bool"]:
        query["bool"]["filter"] = []

    filters = query["bool"]["filter"]
    if index_filter not in filters:
        filters.append(index_filter)
    return query
