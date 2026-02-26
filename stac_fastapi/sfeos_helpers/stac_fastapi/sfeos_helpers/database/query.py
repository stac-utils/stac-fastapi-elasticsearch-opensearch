"""Query building functions for Elasticsearch/OpenSearch.

This module provides functions for building and manipulating Elasticsearch/OpenSearch queries.
"""

import logging
import os
from typing import Any

from stac_fastapi.core.utilities import bbox2polygon
from stac_fastapi.sfeos_helpers.mappings import Geometry

ES_MAX_URL_LENGTH = 4096


def apply_free_text_filter_shared(
    search: Any, free_text_queries: list[str] | None
) -> Any:
    """Apply a flexible free-text search across configurable fields.

    This function uses multi_match queries to search across text fields with support for
    tokenization, lowercasing, partial word matching, and typo tolerance. Fields can be
    configured via the FREE_TEXT_FIELDS environment variable.

    Args:
        search (Any): The search object to apply the query to.
        free_text_queries (list[str] | None): A list of text strings to search for in the properties.

    Returns:
        Any: The search object with the free text query applied, or the original search
            object if no free_text_queries were provided.

    Environment Variables:
        FREE_TEXT_FIELDS: Comma-separated list of fields to search (e.g.,
            "properties.title,properties.standard_name,properties.description").
            If not set, uses default fields with title boosting.

    Notes:
        - Removes restrictive double quotes to enable text field analysis
        - Supports fuzziness for typo tolerance (e.g., "Temperatrue" -> "Temperature")
        - Allows field boosting (e.g., "properties.title^3" gives title 3x weight)
        - Works seamlessly with text-mapped fields in Elasticsearch/OpenSearch
    """
    if free_text_queries:
        # Combine all query terms into a single search string
        search_string = " ".join(free_text_queries)

        # Get fields from environment or use sensible defaults
        env_fields = os.getenv("FREE_TEXT_FIELDS")
        if env_fields:
            fields = [f.strip() for f in env_fields.split(",")]
            logging.debug(f"FREE_TEXT_FIELDS set to: {fields}")
        else:
            # Default "High-Performance" fields
            # To search custom properties, users should set FREE_TEXT_FIELDS environment variable
            fields = [
                "id",
                "collection",
                "properties.title^3",
                "properties.description",
                "properties.keywords",
            ]

        # Use multi_match for intelligent text analysis and field prioritization
        logging.debug(
            f"Applying free-text search with query='{search_string}' on fields={fields}"
        )
        search = search.query(
            "multi_match",
            query=search_string,
            fields=fields,
            type="best_fields",
            fuzziness="AUTO",
        )

    return search


def apply_collections_free_text_filter_shared(
    free_text_queries: Optional[List[str]],
) -> Optional[Dict[str, Any]]:
    """Apply free-text search for collections across core fields.

    This function uses multi_match queries to search across collection text fields with support for
    tokenization, lowercasing, and typo tolerance.

    Args:
        free_text_queries (Optional[List[str]]): A list of text strings to search for.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the multi_match query configuration
            that can be used with Elasticsearch/OpenSearch queries, or None if no queries provided.

    Notes:
        - Searches across: id, title (boosted 3x), description, keywords
        - Supports fuzziness for typo tolerance (e.g., "Temperatrue" -> "Temperature")
        - Works seamlessly with text-mapped fields in Elasticsearch/OpenSearch
    """
    if not free_text_queries:
        return None

    search_string = " ".join(free_text_queries)
    logging.debug(f"Applying collections free-text search with query='{search_string}'")

    return {
        "multi_match": {
            "query": search_string,
            "fields": [
                "id",
                "title^3",
                "description",
                "keywords",
            ],
            "type": "best_fields",
            "fuzziness": "AUTO",
        }
    }


def apply_intersects_filter_shared(
    intersects: Geometry,
) -> dict[str, dict]:
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


def apply_collections_datetime_filter_shared(
    datetime_str: str | None,
) -> dict[str, Any] | None:
    """Create a temporal filter for collections based on their extent.

    Args:
        datetime_str: The datetime parameter. Can be:
            - A single datetime string (e.g., "2020-01-01T00:00:00Z")
            - A datetime range with "/" separator (e.g., "2020-01-01T00:00:00Z/2021-01-01T00:00:00Z")
            - Open-ended ranges using ".." (e.g., "../2021-01-01T00:00:00Z" or "2020-01-01T00:00:00Z/..")
            - None if no datetime filter is provided

    Returns:
        dict[str, Any] | None: A dictionary containing the temporal filter configuration
            that can be used with Elasticsearch/OpenSearch queries, or None if datetime_str is None.
            Example return value:
            {
                "bool": {
                    "must": [
                        {"range": {"extent.temporal.interval": {"lte": "2021-01-01T00:00:00Z"}}},
                        {"range": {"extent.temporal.interval": {"gte": "2020-01-01T00:00:00Z"}}}
                    ]
                }
            }

    Notes:
        - This function is specifically for filtering collections by their temporal extent
        - It queries the extent.temporal.interval field
        - Open-ended ranges (..) are replaced with concrete dates (1800-01-01 for start, 2999-12-31 for end)
    """
    if not datetime_str:
        return None

    # Parse the datetime string into start and end
    if "/" in datetime_str:
        start, end = datetime_str.split("/")
        # Replace open-ended ranges with concrete dates
        if start == "..":
            # For open-ended start, use a very early date
            start = "1800-01-01T00:00:00Z"
        if end == "..":
            # For open-ended end, use a far future date
            end = "2999-12-31T23:59:59Z"
    else:
        # If it's just a single date, use it for both start and end
        start = end = datetime_str

    return {
        "bool": {
            "must": [
                # Check if any date in the array is less than or equal to the query end date
                # This will match if the collection's start date is before or equal to the query end date
                {"range": {"extent.temporal.interval": {"lte": end}}},
                # Check if any date in the array is greater than or equal to the query start date
                # This will match if the collection's end date is after or equal to the query start date
                {"range": {"extent.temporal.interval": {"gte": start}}},
            ]
        }
    }


def apply_collections_bbox_filter_shared(
    bbox: str | list[float] | None,
) -> dict[str, dict] | None:
    """Create a geo_shape filter for collections bbox search.

    This function handles bbox parsing from both GET requests (string format) and POST requests
    (list format), and constructs a geo_shape query for filtering collections by their bbox_shape field.

    Args:
        bbox: The bounding box parameter. Can be:
            - A string of comma-separated coordinates (from GET requests)
            - A list of floats [minx, miny, maxx, maxy] for 2D bbox
            - None if no bbox filter is provided

    Returns:
        dict[str, dict] | None: A dictionary containing the geo_shape filter configuration
            that can be used with Elasticsearch/OpenSearch queries, or None if bbox is invalid.
            Example return value:
            {
                "geo_shape": {
                    "bbox_shape": {
                        "shape": {
                            "type": "Polygon",
                            "coordinates": [[[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]]
                        },
                        "relation": "intersects"
                    }
                }
            }

    Notes:
        - This function is specifically for filtering collections by their spatial extent
        - It queries the bbox_shape field (not the geometry field used for items)
        - The bbox is expected to be 2D (4 values) after any 3D to 2D conversion in the API layer
    """
    logger = logging.getLogger(__name__)

    if not bbox:
        return None

    # Parse bbox if it's a string (from GET requests)
    if isinstance(bbox, str):
        try:
            bbox = [float(x.strip()) for x in bbox.split(",")]
        except (ValueError, AttributeError) as e:
            logger.error(f"Invalid bbox format: {bbox}, error: {e}")
            return None

    if not bbox or len(bbox) != 4:
        if bbox:
            logger.warning(
                f"bbox has incorrect number of coordinates (length={len(bbox)}), expected 4 (2D bbox)"
            )
        return None

    # Convert bbox to a polygon for geo_shape query
    bbox_polygon = {
        "type": "Polygon",
        "coordinates": bbox2polygon(bbox[0], bbox[1], bbox[2], bbox[3]),
    }

    # Return geo_shape query for bbox_shape field
    return {
        "geo_shape": {
            "bbox_shape": {
                "shape": bbox_polygon,
                "relation": "intersects",
            }
        }
    }


def populate_sort_shared(sortby: list) -> dict[str, dict[str, str]] | None:
    """Create a sort configuration for Elasticsearch/OpenSearch queries.

    Args:
        sortby (List): A list of sort specifications, each containing a field and direction.

    Returns:
        dict[str, dict[str, str]] | None: A dictionary mapping field names to sort direction
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
    collection_ids: list[str], query: dict[str, Any] | None
) -> dict[str, Any]:
    """Add a list of collection ids to the body of a query.

    Args:
        collection_ids (List[str]): A list of collections ids.
        query (dict[str, Any] | None): The query to add collections to. If none, create a query that filters
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
