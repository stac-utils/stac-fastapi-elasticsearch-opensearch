"""API configuration."""

import logging
import os
import ssl
from typing import Any, Dict, Set, Union

import certifi
from opensearchpy import AsyncOpenSearch, OpenSearch

from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.database import validate_refresh
from stac_fastapi.types.config import ApiSettings


def _es_config() -> Dict[str, Any]:
    # Determine the scheme (http or https)
    use_ssl = get_bool_env("ES_USE_SSL", default=True)
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

    http_compress = get_bool_env("ES_HTTP_COMPRESS", default=True)
    if http_compress:
        config["http_compress"] = True

    # Handle authentication
    if (u := os.getenv("ES_USER")) and (p := os.getenv("ES_PASS")):
        config["http_auth"] = (u, p)

    if api_key := os.getenv("ES_API_KEY"):
        if isinstance(config["headers"], dict):
            headers = {**config["headers"], "x-api-key": api_key}

        else:
            config["headers"] = {"x-api-key": api_key}

        config["headers"] = headers

    # Include timeout setting if set
    if timeout := os.getenv("ES_TIMEOUT"):
        config["timeout"] = timeout

    # Explicitly exclude SSL settings when not using SSL
    if not use_ssl:
        return config

    # Include SSL settings if using https
    config["ssl_version"] = ssl.PROTOCOL_SSLv23
    config["verify_certs"] = get_bool_env("ES_VERIFY_CERTS", default=True)

    # Include CA Certificates if verifying certs
    if config["verify_certs"]:
        config["ca_certs"] = os.getenv("CURL_CA_BUNDLE", certifi.where())

    return config


_forbidden_fields: Set[str] = {"type"}


class OpensearchSettings(ApiSettings, ApiBaseSettings):
    """
    API settings.

    Set enable_direct_response via the ENABLE_DIRECT_RESPONSE environment variable.
    If enabled, all API routes use direct response for maximum performance, but ALL FastAPI dependencies (including authentication, custom status codes, and validation) are disabled.
    Default is False for safety.
    """

    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}
    enable_response_models: bool = False
    enable_direct_response: bool = get_bool_env("ENABLE_DIRECT_RESPONSE", default=False)
    raise_on_bulk_error: bool = get_bool_env("RAISE_ON_BULK_ERROR", default=False)

    @property
    def database_refresh(self) -> Union[bool, str]:
        """
        Get the value of the DATABASE_REFRESH environment variable.

        Returns:
            Union[bool, str]: The value of DATABASE_REFRESH, which can be True, False, or "wait_for".
        """
        value = os.getenv("DATABASE_REFRESH", "false")
        return validate_refresh(value)

    @property
    def create_client(self):
        """Create es client."""
        return OpenSearch(**_es_config())


class AsyncOpensearchSettings(ApiSettings, ApiBaseSettings):
    """
    API settings.

    Set enable_direct_response via the ENABLE_DIRECT_RESPONSE environment variable.
    If enabled, all API routes use direct response for maximum performance, but ALL FastAPI dependencies (including authentication, custom status codes, and validation) are disabled.
    Default is False for safety.
    """

    forbidden_fields: Set[str] = _forbidden_fields
    indexed_fields: Set[str] = {"datetime"}
    enable_response_models: bool = False
    enable_direct_response: bool = get_bool_env("ENABLE_DIRECT_RESPONSE", default=False)
    raise_on_bulk_error: bool = get_bool_env("RAISE_ON_BULK_ERROR", default=False)

    @property
    def database_refresh(self) -> Union[bool, str]:
        """
        Get the value of the DATABASE_REFRESH environment variable.

        Returns:
            Union[bool, str]: The value of DATABASE_REFRESH, which can be True, False, or "wait_for".
        """
        value = os.getenv("DATABASE_REFRESH", "false")
        return validate_refresh(value)

    @property
    def create_client(self):
        """Create async elasticsearch client."""
        return AsyncOpenSearch(**_es_config())


# Warn at import if direct response is enabled (applies to either settings class)
if (
    OpensearchSettings().enable_direct_response
    or AsyncOpensearchSettings().enable_direct_response
):
    logging.basicConfig(level=logging.WARNING)
    logging.warning(
        "ENABLE_DIRECT_RESPONSE is True: All FastAPI dependencies (including authentication) are DISABLED for all routes!"
    )
