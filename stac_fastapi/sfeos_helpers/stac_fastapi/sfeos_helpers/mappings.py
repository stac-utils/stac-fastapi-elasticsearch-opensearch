"""Shared mappings for stac-fastapi elasticsearch and opensearch backends.

This module contains shared constants, mappings, and type definitions used by both
the Elasticsearch and OpenSearch implementations of STAC FastAPI. It includes:

1. Index name constants and character translation tables
2. Mapping definitions for Collections and Items
3. Aggregation mappings for search queries
4. Type conversion mappings between Elasticsearch/OpenSearch and JSON Schema types

The sfeos_helpers package is organized as follows:
- database_logic_helpers.py: Shared database operations
- filter.py: Shared filter extension implementation
- mappings.py: Shared constants and mapping definitions (this file)
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

import copy
import json
import logging
import os
from typing import Any, Dict, Literal, Optional, Protocol, Union

from stac_fastapi.core.utilities import get_bool_env

logger = logging.getLogger(__name__)


def merge_mappings(base: Dict[str, Any], custom: Dict[str, Any]) -> None:
    """Recursively merge custom mappings into base mappings.

    Custom mappings will overwrite base mappings if keys collide.
    Nested dictionaries are merged recursively.

    Args:
        base: The base mapping dictionary to merge into (modified in place).
        custom: The custom mapping dictionary to merge from.
    """
    for key, value in custom.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            merge_mappings(base[key], value)
        else:
            base[key] = value


def parse_dynamic_mapping_config(
    config_value: Optional[str],
) -> Union[bool, str]:
    """Parse the dynamic mapping configuration value.

    Args:
        config_value: The configuration value from environment variable.
            Can be "true", "false", "strict", or None.

    Returns:
        True for "true" (default), False for "false", or the string value
        for other settings like "strict".
    """
    if config_value is None:
        return True
    config_lower = config_value.lower()
    if config_lower == "true":
        return True
    elif config_lower == "false":
        return False
    else:
        return config_lower


def apply_custom_mappings(
    mappings: Dict[str, Any], custom_mappings_json: Optional[str]
) -> None:
    """Apply custom mappings from a JSON string to the mappings dictionary.

    The custom mappings JSON should have the same structure as ES_ITEMS_MAPPINGS.
    It will be recursively merged at the root level, allowing users to override
    any part of the mapping including properties, dynamic_templates, etc.

    Args:
        mappings: The mappings dictionary to modify (modified in place).
        custom_mappings_json: JSON string containing custom mappings.

    Raises:
        Logs error if JSON parsing or merging fails.
    """
    if not custom_mappings_json:
        return

    try:
        custom_mappings = json.loads(custom_mappings_json)
        merge_mappings(mappings, custom_mappings)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse STAC_FASTAPI_ES_CUSTOM_MAPPINGS JSON: {e}")
    except Exception as e:
        logger.error(f"Failed to merge STAC_FASTAPI_ES_CUSTOM_MAPPINGS: {e}")


def get_items_mappings(
    dynamic_mapping: Optional[str] = None, custom_mappings: Optional[str] = None
) -> Dict[str, Any]:
    """Get the ES_ITEMS_MAPPINGS with optional dynamic mapping and custom mappings applied.

    This function creates a fresh copy of the base mappings and applies the
    specified configuration. Useful for testing or programmatic configuration.

    Args:
        dynamic_mapping: Override for STAC_FASTAPI_ES_DYNAMIC_MAPPING.
            If None, reads from environment variable.
        custom_mappings: Override for STAC_FASTAPI_ES_CUSTOM_MAPPINGS.
            If None, reads from environment variable.

    Returns:
        A new dictionary containing the configured mappings.
    """
    mappings = copy.deepcopy(_BASE_ITEMS_MAPPINGS)

    # Apply dynamic mapping configuration
    dynamic_config = (
        dynamic_mapping
        if dynamic_mapping is not None
        else os.getenv("STAC_FASTAPI_ES_DYNAMIC_MAPPING", "true")
    )
    mappings["dynamic"] = parse_dynamic_mapping_config(dynamic_config)

    # Apply custom mappings
    custom_config = (
        custom_mappings
        if custom_mappings is not None
        else os.getenv("STAC_FASTAPI_ES_CUSTOM_MAPPINGS")
    )

    if custom_config is None:
        mappings_file = os.getenv("STAC_FASTAPI_ES_MAPPINGS_FILE")
        if mappings_file:
            try:
                with open(mappings_file, "r") as f:
                    custom_config = f.read()
            except Exception as e:
                logger.error(
                    f"Failed to read STAC_FASTAPI_ES_MAPPINGS_FILE at {mappings_file}: {e}"
                )

    apply_custom_mappings(mappings, custom_config)

    return mappings


# stac_pydantic classes extend _GeometryBase, which doesn't have a type field,
# So create our own Protocol for typing
# Union[ Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection]
class Geometry(Protocol):  # noqa
    type: str
    coordinates: Any


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

_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE = str.maketrans(
    "", "", "".join(ES_INDEX_NAME_UNSUPPORTED_CHARS)
)

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
    {"long_to_double": {"match_mapping_type": "long", "mapping": {"type": "double"}}},
    {
        "double_to_double": {
            "match_mapping_type": "double",
            "mapping": {"type": "double"},
        }
    },
]

# Base items mappings without dynamic configuration applied
_BASE_ITEMS_MAPPINGS = {
    "numeric_detection": False,
    "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "id": {"type": "keyword"},
        "collection": {"type": "keyword"},
        "geometry": {"type": "geo_shape"},
        "assets": {"type": "object", "enabled": get_bool_env("STAC_INDEX_ASSETS")},
        "links": {"type": "object", "enabled": False},
        "properties": {
            "type": "object",
            "properties": {
                # Common https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md
                "datetime": {"type": "date_nanos"},
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

# ES_ITEMS_MAPPINGS with environment-based configuration applied at module load time
ES_ITEMS_MAPPINGS = get_items_mappings()

ES_COLLECTIONS_MAPPINGS = {
    "numeric_detection": False,
    "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "id": {"type": "keyword"},
        "parent_ids": {"type": "keyword"},
        "bbox_shape": {"type": "geo_shape"},
        "extent.temporal.interval": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis",
        },
        "providers": {"type": "object", "enabled": False},
        "links": {"type": "object", "enabled": False},
        "item_assets": {"type": "object", "enabled": get_bool_env("STAC_INDEX_ASSETS")},
        # Field alias to allow sorting on 'temporal' (points to extent.temporal.interval)
        "temporal": {"type": "alias", "path": "extent.temporal.interval"},
    },
}

# Shared aggregation mapping for both Elasticsearch and OpenSearch
AGGREGATION_MAPPING: Dict[str, Dict[str, Any]] = {
    "total_count": {"value_count": {"field": "id"}},
    "collection_frequency": {"terms": {"field": "collection", "size": 100}},
    "platform_frequency": {"terms": {"field": "properties.platform", "size": 100}},
    "cloud_cover_frequency": {
        "range": {
            "field": "properties.eo:cloud_cover",
            "ranges": [
                {"to": 5},
                {"from": 5, "to": 15},
                {"from": 15, "to": 40},
                {"from": 40},
            ],
        }
    },
    "datetime_frequency": {
        "date_histogram": {
            "field": "properties.datetime",
            "calendar_interval": "month",
        }
    },
    "datetime_min": {"min": {"field": "properties.datetime"}},
    "datetime_max": {"max": {"field": "properties.datetime"}},
    "grid_code_frequency": {
        "terms": {
            "field": "properties.grid:code",
            "missing": "none",
            "size": 10000,
        }
    },
    "sun_elevation_frequency": {
        "histogram": {"field": "properties.view:sun_elevation", "interval": 5}
    },
    "sun_azimuth_frequency": {
        "histogram": {"field": "properties.view:sun_azimuth", "interval": 5}
    },
    "off_nadir_frequency": {
        "histogram": {"field": "properties.view:off_nadir", "interval": 5}
    },
    "centroid_geohash_grid_frequency": {
        "geohash_grid": {
            "field": "properties.proj:centroid",
            "precision": 1,
        }
    },
    "centroid_geohex_grid_frequency": {
        "geohex_grid": {
            "field": "properties.proj:centroid",
            "precision": 0,
        }
    },
    "centroid_geotile_grid_frequency": {
        "geotile_grid": {
            "field": "properties.proj:centroid",
            "precision": 0,
        }
    },
    "geometry_geohash_grid_frequency": {
        "geohash_grid": {
            "field": "geometry",
            "precision": 1,
        }
    },
    "geometry_geotile_grid_frequency": {
        "geotile_grid": {
            "field": "geometry",
            "precision": 0,
        }
    },
}

ES_MAPPING_TYPE_TO_JSON: Dict[
    str, Literal["string", "number", "boolean", "object", "array", "null"]
] = {
    "date": "string",
    "date_nanos": "string",
    "keyword": "string",
    "match_only_text": "string",
    "text": "string",
    "wildcard": "string",
    "byte": "number",
    "double": "number",
    "float": "number",
    "half_float": "number",
    "long": "number",
    "scaled_float": "number",
    "short": "number",
    "token_count": "number",
    "unsigned_long": "number",
    "geo_point": "object",
    "geo_shape": "object",
    "nested": "array",
}
