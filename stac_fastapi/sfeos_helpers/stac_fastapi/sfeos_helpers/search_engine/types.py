"""Search engine types and enums."""

from enum import Enum


class SearchEngineType(Enum):
    """Supported search engine types."""

    ELASTICSEARCH = "elasticsearch"
    OPENSEARCH = "opensearch"
