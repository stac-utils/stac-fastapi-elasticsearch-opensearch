"""Shared aggregation extension methods for stac-fastapi elasticsearch and opensearch backends.

This module provides shared functionality for implementing the STAC API Aggregation Extension
with Elasticsearch and OpenSearch. It includes:

1. Functions for formatting aggregation responses
2. Helper functions for handling aggregation parameters
3. Base implementation of the AsyncBaseAggregationClient for Elasticsearch/OpenSearch

The aggregation package is organized as follows:
- client.py: Aggregation client implementation
- format.py: Response formatting functions

When adding new functionality to this package, consider:
1. Will this code be used by both Elasticsearch and OpenSearch implementations?
2. Is the functionality stable and unlikely to diverge between implementations?
3. Is the function well-documented with clear input/output contracts?

Function Naming Conventions:
- Function names should be descriptive and indicate their purpose
- Parameter names should be consistent across similar functions
"""

from .client import EsAsyncBaseAggregationClient
from .format import frequency_agg, metric_agg

__all__ = [
    "EsAsyncBaseAggregationClient",
    "frequency_agg",
    "metric_agg",
]
