"""Shared database operations for stac-fastapi elasticsearch and opensearch backends.

This module provides shared database functionality used by both the Elasticsearch and OpenSearch
implementations of STAC FastAPI. It includes:

1. Index management functions for creating and deleting indices
2. Query building functions for constructing search queries
3. Mapping functions for working with Elasticsearch/OpenSearch mappings
4. Document operations for working with documents
5. Utility functions for database operations
6. Datetime utilities for query formatting

The database package is organized as follows:
- index.py: Index management functions
- query.py: Query building functions
- mapping.py: Mapping functions
- document.py: Document operations
- utils.py: Utility functions
- datetime.py: Datetime utilities for query formatting

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
from .datetime import extract_date, extract_first_date_from_index, return_date
from .document import mk_actions, mk_item_id
from .index import (
    create_index_templates_shared,
    delete_item_index_shared,
    filter_indexes_by_datetime,
    index_alias_by_collection_id,
    index_by_collection_id,
    indices,
)
from .mapping import get_queryables_mapping_shared
from .query import (
    apply_free_text_filter_shared,
    apply_intersects_filter_shared,
    populate_sort_shared,
)
from .utils import get_bool_env, validate_refresh

__all__ = [
    # Index operations
    "create_index_templates_shared",
    "delete_item_index_shared",
    "index_alias_by_collection_id",
    "index_by_collection_id",
    "filter_indexes_by_datetime",
    "indices",
    # Query operations
    "apply_free_text_filter_shared",
    "apply_intersects_filter_shared",
    "populate_sort_shared",
    # Mapping operations
    "get_queryables_mapping_shared",
    # Document operations
    "mk_item_id",
    "mk_actions",
    # Utility functions
    "validate_refresh",
    "get_bool_env",
    # Datetime utilities
    "return_date",
    "extract_date",
    "extract_first_date_from_index",
]
