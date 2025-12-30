#Edited: custome mappings to be edited as necessary. This file is imported if the environment variable STAC_USE_CUSTOM_MAPPINGS is set to "true"

from typing import Any, Dict, Literal, Protocol
from stac_fastapi.core.utilities import get_bool_env


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
            #"mapping": {"type": "text"}, #Default  - changed to keyword to allow sorting on title field
            "mapping": {"type": "keyword"},
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

ES_ITEMS_MAPPINGS = {
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

ES_COLLECTIONS_MAPPINGS = {
    "numeric_detection": False,
    "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "id": {"type": "keyword"},        
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


   