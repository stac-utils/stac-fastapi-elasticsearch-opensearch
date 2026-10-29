"""Query building functions for Elasticsearch/OpenSearch.

This module provides functions for building and manipulating Elasticsearch/OpenSearch queries.
"""

import binascii
import json
import logging
import os
from base64 import urlsafe_b64decode
from typing import Any, Iterable

import orjson
from fastapi import HTTPException

from stac_fastapi.core.utilities import bbox2polygon
from stac_fastapi.sfeos_helpers.database.mapping import get_excluded_from_queryables
from stac_fastapi.sfeos_helpers.mappings import (
    DEFAULT_SORT,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
    ITEM_DATETIME_SORT,
    ITEM_DATETIME_SORT_FIELDS,
    Geometry,
)

ES_MAX_URL_LENGTH = int(os.getenv("ES_MAX_URL_LENGTH", "4096"))


def _parse_sort_field_remaps(
    env_var: str, file_env_var: str | None = None
) -> dict[str, str]:
    """Parse sort field remaps from env JSON or optional *_FILE JSON.

    Precedence is env_var first, then file_env_var.
    """
    value = os.getenv(env_var)
    source_name = env_var

    if not value and file_env_var:
        file_path = os.getenv(file_env_var)
        if file_path:
            try:
                with open(file_path, encoding="utf-8") as f:
                    value = f.read()
                source_name = file_env_var
            except OSError:
                logging.warning("Could not read %s path: %s", file_env_var, file_path)
                return {}

    if not value:
        return {}

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        logging.warning("Invalid JSON in %s; expected an object.", source_name)
        return {}

    if not isinstance(parsed, dict):
        logging.warning("Invalid value in %s; expected an object.", source_name)
        return {}

    remaps: dict[str, str] = {}
    for key, mapped in parsed.items():
        if isinstance(key, str) and isinstance(mapped, str) and key and mapped:
            remaps[key] = mapped

    return remaps


def _detect_keyword_sort_remaps_from_mapping(
    mapping: dict[str, Any],
) -> dict[str, str]:
    """Detect sortable keyword subfields from explicit mappings.

    For a text field with a keyword multi-field, this returns a remap like:
    {"title": "title.keyword"}
    """
    remaps: dict[str, str] = {}

    def walk(path: str, field_def: dict[str, Any]) -> None:
        field_type = field_def.get("type")
        multi_fields = field_def.get("fields", {})

        if field_type == "text" and isinstance(multi_fields, dict):
            for sub_name, sub_def in multi_fields.items():
                if isinstance(sub_def, dict) and sub_def.get("type") == "keyword":
                    remaps.setdefault(path, f"{path}.{sub_name}")
                    break

        nested = field_def.get("properties")
        if isinstance(nested, dict):
            for child_name, child_def in nested.items():
                if isinstance(child_def, dict):
                    child_path = f"{path}.{child_name}" if path else child_name
                    walk(child_path, child_def)

    properties = mapping.get("properties", {})
    if isinstance(properties, dict):
        for field_name, field_def in properties.items():
            if isinstance(field_def, dict):
                walk(field_name, field_def)

    return remaps


MANUAL_ITEMS_SORT_FIELD_REMAPS = _parse_sort_field_remaps(
    "STAC_FASTAPI_ITEMS_SORT_FIELD_REMAPS",
    "STAC_FASTAPI_ITEMS_SORT_FIELD_REMAPS_FILE",
)
MANUAL_COLLECTIONS_SORT_FIELD_REMAPS = _parse_sort_field_remaps(
    "STAC_FASTAPI_COLLECTIONS_SORT_FIELD_REMAPS",
    "STAC_FASTAPI_COLLECTIONS_SORT_FIELD_REMAPS_FILE",
)

AUTO_ITEMS_SORT_FIELD_REMAPS = _detect_keyword_sort_remaps_from_mapping(
    ES_ITEMS_MAPPINGS
)
AUTO_COLLECTIONS_SORT_FIELD_REMAPS = _detect_keyword_sort_remaps_from_mapping(
    ES_COLLECTIONS_MAPPINGS
)


def remap_sort_field_shared(field_name: str, is_collection: bool = False) -> str:
    """Remap API sort field names to engine fields via optional env config.

    Env vars (JSON objects):
    - STAC_FASTAPI_ITEMS_SORT_FIELD_REMAPS (items endpoints)
    - STAC_FASTAPI_ITEMS_SORT_FIELD_REMAPS_FILE (items endpoints)
    - STAC_FASTAPI_COLLECTIONS_SORT_FIELD_REMAPS (collection endpoints)
    - STAC_FASTAPI_COLLECTIONS_SORT_FIELD_REMAPS_FILE (collection endpoints)
    """
    if is_collection:
        if field_name in MANUAL_COLLECTIONS_SORT_FIELD_REMAPS:
            return MANUAL_COLLECTIONS_SORT_FIELD_REMAPS[field_name]
        return AUTO_COLLECTIONS_SORT_FIELD_REMAPS.get(field_name, field_name)

    if field_name in MANUAL_ITEMS_SORT_FIELD_REMAPS:
        return MANUAL_ITEMS_SORT_FIELD_REMAPS[field_name]
    return AUTO_ITEMS_SORT_FIELD_REMAPS.get(field_name, field_name)


def format_collection_sort_shared(
    sort: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Format collection `sortby` field/direction pairs for the engine.

    Validates each field via `validate_sort_fields_shared`, remaps it via
    `remap_sort_field_shared`, and appends an `id` tie-breaker so pages stay
    aligned when earlier keys tie.
    """
    validate_sort_fields_shared(
        item.get("field", "") for item in sort or [] if isinstance(item, dict)
    )
    formatted_sort = []
    for item in sort or []:
        field = remap_sort_field_shared(item.get("field", ""), is_collection=True)
        direction = item.get("direction", "asc")
        if field:
            formatted_sort.append({field: {"order": direction}})
    if not any("id" in item for item in formatted_sort):
        formatted_sort.append({"id": {"order": "asc"}})
    return formatted_sort


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
            lenient=True,
            operator="or",
        )

    return search


def apply_collections_free_text_filter_shared(
    free_text_queries: list[str] | None,
) -> dict[str, Any] | None:
    """Apply free-text search for collections across core fields.

    This function uses multi_match queries to search across collection text fields with support for
    tokenization, lowercasing, and typo tolerance.

    Args:
        free_text_queries (list[str] | None): A list of text strings to search for.

    Returns:
        dict[str, Any] | None: A dictionary containing the multi_match query configuration
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


def _serialize_coordinates(coords: Any) -> Any:
    """Recursively serialize coordinates to plain Python types.

    Converts geojson-pydantic Position2D and other Pydantic models to lists/tuples.
    """
    if isinstance(coords, (list, tuple)):
        return [_serialize_coordinates(c) for c in coords]
    elif hasattr(coords, "model_dump"):
        # Pydantic model - convert to dict then serialize
        return _serialize_coordinates(coords.model_dump())
    elif isinstance(coords, dict):
        return {k: _serialize_coordinates(v) for k, v in coords.items()}
    else:
        # Primitive type (int, float, str, etc.)
        return coords


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
    # Handle both dict and Pydantic model cases
    if isinstance(intersects, dict):
        geom_type = intersects.get("type", "").lower()
        coordinates = intersects.get("coordinates")
    else:
        # Convert Pydantic model to dict to ensure proper serialization
        if hasattr(intersects, "model_dump"):
            geom_dict = intersects.model_dump()
            geom_type = geom_dict.get("type", "").lower()
            coordinates = geom_dict.get("coordinates")
        else:
            geom_type = intersects.type.lower()
            coordinates = intersects.coordinates

    # Ensure coordinates are serializable (convert Pydantic models to plain types)
    coordinates = _serialize_coordinates(coordinates)

    return {
        "geo_shape": {
            "geometry": {
                "shape": {
                    "type": geom_type,
                    "coordinates": coordinates,
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


def validate_sort_fields_shared(field_names: Iterable[str]) -> None:
    """Reject sortby fields excluded via EXCLUDED_FROM_QUERYABLES.

    Sortables schemas with additionalProperties: false promise a 400 for
    undeclared fields. Excluded fields are the only mapped, engine-sortable
    fields the schemas omit, so reject them here to keep that contract.

    Args:
        field_names: field names from sortby entries, as document paths.

    Raises:
        HTTPException: 400 if any field matches an excluded prefix.
    """
    excluded = get_excluded_from_queryables()
    if not excluded:
        return
    for field in field_names:
        if any(
            field == prefix or field.startswith(prefix + ".") for prefix in excluded
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Field '{field}' is not sortable: it is excluded "
                    "via EXCLUDED_FROM_QUERYABLES."
                ),
            )


def populate_sort_shared(
    sortby: list,
    is_collection: bool = False,
    field_types: dict[str, str] | None = None,
) -> dict[str, dict[str, str]] | None:
    """Create a sort configuration for Elasticsearch/OpenSearch queries.

    Args:
        sortby (List): A list of sort specifications, each containing a field and direction.
        field_types (dict[str, str] | None): Optional map of sortable field names to
            their engine field type, used for unmapped_type. Defaults to keyword.

    Returns:
        dict[str, dict[str, str]] | None: A dictionary mapping field names to sort direction
            configurations, or an ascending 'id' sort if no sort was specified.

    Notes:
        This function transforms a list of sort specifications into the format required by
        Elasticsearch/OpenSearch for sorting query results. The returned dictionary can be
        directly used in search requests.
        A non-empty sortby always gets 'id', and 'collection' for items, as
        tie-breakers to ensure unique pagination tokens.
        Documents missing the sort field sort last in either direction.
    """
    if sortby:
        validate_sort_fields_shared(s.field for s in sortby)
        sort_config = {}
        for s in sortby:
            field_name = remap_sort_field_shared(s.field, is_collection=is_collection)
            direction = s.direction
            # Indices missing the field sort its documents as null rather than
            # failing, so cross-collection sorts never silently drop hits. The
            # unmapped type must match the field's mapped type — sorting a
            # double field with unmapped_type keyword still fails with
            # incompatible sort types across shards.
            unmapped_type = "keyword"
            if field_types:
                unmapped_type = (
                    field_types.get(s.field) or field_types.get(field_name) or "keyword"
                )
            sort_config[field_name] = {
                "order": direction,
                "unmapped_type": unmapped_type,
                "missing": "_last",
            }
            if not is_collection and field_name in ITEM_DATETIME_SORT_FIELDS:
                sort_config[field_name].update(ITEM_DATETIME_SORT)

        sort_config.setdefault("id", {"order": "asc"})
        # Item ids are unique only within a collection.
        if not is_collection:
            sort_config.setdefault("collection", {"order": "asc"})
        return sort_config
    else:
        return {"id": {"order": "asc"}}


def decode_search_after_token_shared(
    token: str | None, sort: dict[str, dict[str, Any]] | None
) -> list[Any] | None:
    """Decode an item search pagination token into search_after values.

    Args:
        token (str | None): Base64-encoded JSON list issued as the next-page token.
        sort (dict[str, dict[str, Any]] | None): The sort used for the search, or None for
            the default sort.

    Returns:
        list[Any] | None: The search_after values, or None if no token was given.

    Raises:
        HTTPException: 400 if the token is not a base64-encoded JSON list with one value
            per sort field.
    """
    if token is None:
        return None
    try:
        search_after = orjson.loads(urlsafe_b64decode(token))
    except (binascii.Error, ValueError, UnicodeDecodeError):
        search_after = None
    if not isinstance(search_after, list) or len(search_after) != len(
        sort or DEFAULT_SORT
    ):
        raise HTTPException(status_code=400, detail="Invalid pagination token.")
    return search_after


def add_collections_to_body(
    collection_ids: list[str] | None, query: dict[str, Any] | None
) -> dict[str, Any]:
    """Add a list of collection ids to the body of a query.

    Args:
        collection_ids (List[str] | None): A list of collections ids.
        query (dict[str, Any] | None): The query to add collections to. If none, create a query that filters
        the collection ids.

    Returns:
        Dict[str, Any]: A query that contains a filter on the given collection ids.

    Notes:
        This function is needed in the execute_search function when the size of the URL path will exceed the maximum of ES.
    """
    if not collection_ids:
        return query

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
