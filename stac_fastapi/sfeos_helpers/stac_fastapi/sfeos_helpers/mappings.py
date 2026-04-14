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
from typing import Any, Literal, Protocol

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.models.dynamic_template import DynamicTemplatesModel

logger = logging.getLogger(__name__)

COERCE_GLOBAL = get_bool_env("STAC_FASTAPI_ES_COERCE_GLOBAL", default=True)


def merge_mappings(base: dict[str, Any], custom: dict[str, Any]) -> None:
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


def merge_dynamic_templates(base: list[dict], custom: list[dict]) -> list[dict]:
    """Merge custom dynamic templates into base by matching template names.

    Custom dynamic templates will overwrite base dynamic templates if keys collide.

    Args:
        base: The base dynamic templates list to merge into.
        custom: The custom dynamic templates list to merge from.

    Returns:
        The merged dynamic templates list.
    """
    try:
        DynamicTemplatesModel(templates=custom)
        merged = {list(d.keys())[0]: list(d.values())[0] for d in base}
        for d in custom:
            key = list(d.keys())[0]
            merged[key] = d[key]
        return [{k: v} for k, v in merged.items()]
    except Exception as e:
        logger.error(f"Error occurred during custom dynamic template validation: {e}")
        return base  # Return base templates if validation fails


def parse_dynamic_mapping_config(
    config_value: str | None,
) -> bool | str:
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
    mappings: dict[str, Any], custom_mappings_json: str | None
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


def get_mappings(
    is_items: bool = True,
    dynamic_mapping: str | None = None,
    custom_mappings: str | None = None,
) -> dict[str, Any]:
    """Get MAPPINGS for either items or collections with optional dynamic mapping and custom mappings applied.

    This function creates a fresh copy of the base mappings and applies the
    specified configuration. Useful for testing or programmatic configuration.

    Args:
        is_items: Whether to return item mappings or collection mappings.
        dynamic_mapping: Override for STAC_FASTAPI_ES_DYNAMIC_MAPPING.
            If None, reads from environment variable.
        custom_mappings: Override for STAC_FASTAPI_ES_CUSTOM_MAPPINGS.
            If None, reads from environment variable.

    Returns:
        A new dictionary containing the configured mappings.
    """
    # Assign the appropriate base mappings and environment variable names based on whether we're configuring items or collections
    _BASE_MAPPINGS = _BASE_ITEMS_MAPPINGS if is_items else _BASE_ES_COLLECTIONS_MAPPINGS
    CUSTOM_MAPPINGS_ = (
        "STAC_FASTAPI_ES_CUSTOM_MAPPINGS"
        if is_items
        else "STAC_FASTAPI_ES_COLLECTIONS_CUSTOM_MAPPINGS"
    )
    CUSTOM_MAPPINGS_FILE = (
        "STAC_FASTAPI_ES_MAPPINGS_FILE"
        if is_items
        else "STAC_FASTAPI_ES_COLLECTIONS_MAPPINGS_FILE"
    )
    DYNAMIC_MAPPING = (
        "STAC_FASTAPI_ES_DYNAMIC_MAPPING"
        if is_items
        else "STAC_FASTAPI_ES_COLLECTIONS_DYNAMIC_MAPPING"
    )

    mappings = copy.deepcopy(_BASE_MAPPINGS)

    # Apply dynamic mapping configuration
    dynamic_config = (
        dynamic_mapping
        if dynamic_mapping is not None
        else os.getenv(DYNAMIC_MAPPING, "true")
    )
    mappings["dynamic"] = parse_dynamic_mapping_config(dynamic_config)

    # Apply custom mappings
    custom_config = get_custom_config(
        CUSTOM_MAPPINGS_, CUSTOM_MAPPINGS_FILE, custom_mappings
    )

    apply_custom_mappings(mappings, custom_config)

    return mappings


def get_custom_config(
    ENV_VAR_CUSTOM_MAPPINGS: str,
    ENV_VAR_CUSTOM_MAPPINGS_FILE: str,
    custom_mappings: str | None = None,
) -> str | None:
    """Get custom config from environment variables.

    This function checks for custom mappings configuration in the following order:
    it checks ENV_VAR_CUSTOM_MAPPINGS, if not found, it checks ENV_VAR_CUSTOM_MAPPINGS_FILE for a file path to read the configuration from.

    Args:
        ENV_VAR_CUSTOM_MAPPINGS: The environment variable name for custom mappings JSON string.
        ENV_VAR_CUSTOM_MAPPINGS_FILE: The environment variable name for custom mappings file path.
        custom_mappings: Optional override for custom mappings JSON string. If provided, this value takes precedence over environment variables.

    Returns: The custom mappings JSON string if found, otherwise None.
    """
    custom_config = (
        custom_mappings
        if custom_mappings is not None
        else os.getenv(ENV_VAR_CUSTOM_MAPPINGS)
    )

    if custom_config is None:
        mappings_file = os.getenv(ENV_VAR_CUSTOM_MAPPINGS_FILE)
        if mappings_file:
            try:
                with open(mappings_file, "r") as f:
                    custom_config = f.read()
            except Exception as e:
                logger.error(
                    f"Failed to read {ENV_VAR_CUSTOM_MAPPINGS_FILE} at {mappings_file}: {e}"
                )
    return custom_config


def get_dynamic_template(
    ENV_VAR_CUSTOM_MAPPINGS: str,
    ENV_VAR_CUSTOM_MAPPINGS_FILE: str,
    custom_mappings: str | None = None,
) -> list[dict] | None:
    """Get dynamic templates with custom configuration applied.

    Args:
        ENV_VAR_CUSTOM_MAPPINGS: The environment variable name for custom dynamic templates JSON string.
        ENV_VAR_CUSTOM_MAPPINGS_FILE: The environment variable name for custom dynamic templates file path.
        custom_mappings: Optional override for custom dynamic templates JSON string. If provided, this value

    Returns: The merged dynamic templates list with custom configuration applied.
    """
    mappings = copy.deepcopy(_BASE_ES_MAPPINGS_DYNAMIC_TEMPLATES)
    custom_config = get_custom_config(
        ENV_VAR_CUSTOM_MAPPINGS, ENV_VAR_CUSTOM_MAPPINGS_FILE, custom_mappings
    )

    custom_config_json = []  # type: list[dict[str, Any]]
    try:
        custom_config_json = json.loads(custom_config) if custom_config else []
    except Exception as e:
        logger.error(f"Failed to load {ENV_VAR_CUSTOM_MAPPINGS} as json list: {e}")

    if not isinstance(custom_config_json, list):
        logger.error(
            f"Custom dynamic templates configuration from {ENV_VAR_CUSTOM_MAPPINGS} is not a list: {custom_config_json}"
        )
        return mappings

    mappings = merge_dynamic_templates(mappings, custom_config_json)

    return mappings


# stac_pydantic classes extend _GeometryBase, which doesn't have a type field,
# So create our own Protocol for typing
# Point | MultiPoint | LineString | MultiLineString | Polygon | MultiPolygon | GeometryCollection
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
        "mapping.coerce": COERCE_GLOBAL,
    }
}

# Base dynamic templates to apply
_BASE_ES_MAPPINGS_DYNAMIC_TEMPLATES = [
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
# ES_MAPPINGS_DYNAMIC_TEMPLATES with environment-based configuration applied at module load time
ES_MAPPINGS_DYNAMIC_TEMPLATES = get_dynamic_template(
    "STAC_FASTAPI_ES_CUSTOM_DYNAMIC_TEMPLATES", "STAC_FASTAPI_ES_DYNAMIC_TEMPLATES_FILE"
)

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
                "start_datetime": {"type": "date_nanos"},
                "end_datetime": {"type": "date_nanos"},
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
ES_ITEMS_MAPPINGS = get_mappings(is_items=True)

_BASE_ES_COLLECTIONS_MAPPINGS = {
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

# ES_COLLECTIONS_MAPPINGS with environment-based configuration applied at module load time
ES_COLLECTIONS_MAPPINGS = get_mappings(is_items=False)

# Shared aggregation mapping for both Elasticsearch and OpenSearch
AGGREGATION_MAPPING: dict[str, dict[str, Any]] = {
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

ES_MAPPING_TYPE_TO_JSON: dict[
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
