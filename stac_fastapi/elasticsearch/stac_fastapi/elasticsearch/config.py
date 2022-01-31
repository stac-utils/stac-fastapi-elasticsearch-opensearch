"""API configuration."""
import os
from typing import Set

from elasticsearch import Elasticsearch

from stac_fastapi.types.config import ApiSettings

DOMAIN = os.getenv("ES_HOST")
PORT = os.getenv("ES_PORT")


class ElasticsearchSettings(ApiSettings):
    """API settings."""

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = {"type"}

    # Fields which are item properties but indexed as distinct fields in the database model
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self):
        """Create es client."""
        # try:
        client = Elasticsearch([{"host": str(DOMAIN), "port": str(PORT)}])
        return client
