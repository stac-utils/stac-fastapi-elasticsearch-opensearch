"""index management client."""

import logging

import attr

from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.core import COLLECTIONS_INDEX, ITEMS_INDEX
from stac_fastapi.elasticsearch.session import Session

logger = logging.getLogger(__name__)


@attr.s
class IndexesClient:
    """Elasticsearch client to handle index creation."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    client = ElasticsearchSettings().create_client

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
                "mapping": {"type": "geo_shape"},
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
            "extent.spatial.bbox": {"type": "long"},
            "extent.temporal.interval": {"type": "date"},
            "providers": {"type": "object", "enabled": False},
            "links": {"type": "object", "enabled": False},
            "item_assets": {"type": "object", "enabled": False},
        },
    }

    def create_indexes(self):
        """Create the index for Items and Collections."""
        self.client.indices.create(
            index=ITEMS_INDEX,
            body={"mappings": self.ES_ITEMS_MAPPINGS},
            ignore=400,  # ignore 400 already exists code
        )
        self.client.indices.create(
            index=COLLECTIONS_INDEX,
            body={"mappings": self.ES_COLLECTIONS_MAPPINGS},
            ignore=400,  # ignore 400 already exists code
        )
