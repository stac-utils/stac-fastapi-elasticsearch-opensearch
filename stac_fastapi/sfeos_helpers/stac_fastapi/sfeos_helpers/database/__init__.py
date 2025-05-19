"""Shared database operations for stac-fastapi elasticsearch and opensearch backends.

This module provides shared database functionality used by both the Elasticsearch and OpenSearch
implementations of STAC FastAPI. It includes:

1. Index management functions for creating and deleting indices
2. Query building functions for constructing search queries
3. Mapping functions for working with Elasticsearch/OpenSearch mappings

The database package is organized as follows:
- index.py: Index management functions
- query.py: Query building functions
- mapping.py: Mapping functions

When adding new functionality to this package, consider:
1. Will this code be used by both Elasticsearch and OpenSearch implementations?
2. Is the functionality stable and unlikely to diverge between implementations?
3. Is the function well-documented with clear input/output contracts?

Function Naming Conventions:
- All shared functions should end with `_shared` to clearly indicate they're meant to be used by both implementations
- Function names should be descriptive and indicate their purpose
- Parameter names should be consistent across similar functions
"""

# Re-export all functions for backward compatibility
from .index import create_index_templates_shared, delete_item_index_shared
from .mapping import get_queryables_mapping_shared
from .query import (
    apply_free_text_filter_shared,
    apply_intersects_filter_shared,
    populate_sort_shared,
)

__all__ = [
    "create_index_templates_shared",
    "delete_item_index_shared",
    "apply_free_text_filter_shared",
    "apply_intersects_filter_shared",
    "populate_sort_shared",
    "get_queryables_mapping_shared",
]
