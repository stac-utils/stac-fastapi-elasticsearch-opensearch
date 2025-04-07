"""API configuration."""
import os
import ssl
from typing import Any, Dict, Set

import certifi
from opensearchpy import AsyncOpenSearch, OpenSearch

from stac_fastapi.types.config import ApiSettings


def _es_config() -> Dict[str, Any]:
    # Determine the scheme (http or https)
    use_ssl = os.getenv("ES_USE_SSL", "true").lower() == "true"
    scheme = "https" if use_ssl else "http"

    # Configure the hosts parameter with the correct scheme
    es_hosts = os.getenv(
        "ES_HOST", "localhost"
    ).strip()  # Default to localhost if ES_HOST is not set
    es_port = os.getenv("ES_PORT", "9200")  # Default to 9200 if ES_PORT is not set

    # Validate ES_HOST
    if not es_hosts:
        raise ValueError("ES_HOST environment variable is empty or invalid.")

    hosts = [f"{scheme}://{host.strip()}:{es_port}" for host in es_hosts.split(",")]

    # Initialize the configuration dictionary
    config: Dict[str, Any] = {
        "hosts": hosts,
        "headers": {"accept": "application/json", "Content-Type": "application/json"},
    }

    http_compress = os.getenv("ES_HTTP_COMPRESS", "true").lower() == "true"
    if http_compress:
        config["http_compress"] = True

    # Explicitly exclude SSL settings when not using SSL
    if not use_ssl:
        return config

    # Include SSL settings if using https
    config["ssl_version"] = ssl.PROTOCOL_SSLv23  # type: ignore
    config["verify_certs"] = os.getenv("ES_VERIFY_CERTS", "true").lower() != "false"  # type: ignore

    # Include CA Certificates if verifying certs
    if config["verify_certs"]:
        config["ca_certs"] = os.getenv("CURL_CA_BUNDLE", certifi.where())

    # Handle authentication
    if (u := os.getenv("ES_USER")) and (p := os.getenv("ES_PASS")):
        config["http_auth"] = (u, p)

    if api_key := os.getenv("ES_API_KEY"):
        if isinstance(config["headers"], dict):
            headers = {**config["headers"], "x-api-key": api_key}

        else:
            config["headers"] = {"x-api-key": api_key}

        config["headers"] = headers

    return config


_forbidden_fields: Set[str] = {"type"}


class OpensearchSettings(ApiSettings):
    """API settings."""

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self):
        """Create es client."""
        return OpenSearch(**_es_config())


class AsyncOpensearchSettings(ApiSettings):
    """API settings."""

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}

    @property
    def create_client(self):
        """Create async elasticsearch client."""
        return AsyncOpenSearch(**_es_config())
