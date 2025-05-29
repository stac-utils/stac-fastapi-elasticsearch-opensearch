"""Shared filter extension methods for stac-fastapi elasticsearch and opensearch backends.

This module provides shared functionality for implementing the STAC API Filter Extension
with Elasticsearch and OpenSearch. It includes:

1. Functions for converting CQL2 queries to Elasticsearch/OpenSearch query DSL
2. Helper functions for field mapping and query transformation
3. Base implementation of the AsyncBaseFiltersClient for Elasticsearch/OpenSearch

The filter package is organized as follows:
- cql2.py: CQL2 pattern conversion helpers
- transform.py: Query transformation functions
- client.py: Filter client implementation

When adding new functionality to this package, consider:
1. Will this code be used by both Elasticsearch and OpenSearch implementations?
2. Is the functionality stable and unlikely to diverge between implementations?
3. Is the function well-documented with clear input/output contracts?

Function Naming Conventions:
- Function names should be descriptive and indicate their purpose
- Parameter names should be consistent across similar functions
"""

from .client import EsAsyncBaseFiltersClient

# Re-export the main functions and classes for backward compatibility
from .cql2 import (
    _replace_like_patterns,
    cql2_like_patterns,
    cql2_like_to_es,
    valid_like_substitutions,
)
from .transform import to_es, to_es_field

__all__ = [
    "cql2_like_patterns",
    "valid_like_substitutions",
    "cql2_like_to_es",
    "_replace_like_patterns",
    "to_es_field",
    "to_es",
    "EsAsyncBaseFiltersClient",
]
