"""API configuration."""
import os
from typing import Any, Dict, Set

from elasticsearch import AsyncElasticsearch, Elasticsearch  # type: ignore
from stac_fastapi.types.config import ApiSettings


def _es_config() -> Dict[str, Any]:
    config = {
        "hosts": [{"host": os.getenv("ES_HOST"), "port": os.getenv("ES_PORT")}],
        "headers": {"accept": "application/vnd.elasticsearch+json; compatible-with=7"},
        "use_ssl": True,
        "verify_certs": True,
    }

    if (u := os.getenv("ES_USER")) and (p := os.getenv("ES_PASS")):
        config["http_auth"] = (u, p)

    if (v := os.getenv("ES_USE_SSL")) and v == "false":
        config["use_ssl"] = False

    if (v := os.getenv("ES_VERIFY_CERTS")) and v == "false":
        config["verify_certs"] = False

    if v := os.getenv("CURL_CA_BUNDLE"):
        config["ca_certs"] = v

    return config


_forbidden_fields: Set[str] = {"type"}


class ElasticsearchSettings(ApiSettings):
    """API settings."""

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self):
        """Create es client."""
        return Elasticsearch(**_es_config())


class AsyncElasticsearchSettings(ApiSettings):
    """API settings."""

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self):
        """Create async elasticsearch client."""
        return AsyncElasticsearch(**_es_config())
