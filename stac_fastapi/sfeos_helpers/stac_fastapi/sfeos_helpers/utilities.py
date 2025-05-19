"""Shared utilities functions for stac-fastapi elasticsearch and opensearch backends.

This module contains general utility functions used by both the Elasticsearch and OpenSearch
implementations of STAC FastAPI. These functions handle common tasks like parameter validation,
index naming, and document ID generation.

The sfeos_helpers package is organized as follows:
- database_logic_helpers.py: Shared database operations
- filter.py: Shared filter extension implementation
- mappings.py: Shared constants and mapping definitions
- utilities.py: Shared utility functions (this file)

When adding new functionality to this package, consider:
1. Will this code be used by both Elasticsearch and OpenSearch implementations?
2. Is the functionality stable and unlikely to diverge between implementations?
3. Is the function well-documented with clear input/output contracts?

Function Naming Conventions:
- All shared functions should end with `_shared` to clearly indicate they're meant to be used by both implementations
- Function names should be descriptive and indicate their purpose
- Parameter names should be consistent across similar functions
"""
import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from stac_fastapi.core.utilities import get_bool_env

# Import constants from mappings
from stac_fastapi.sfeos_helpers.mappings import (
    _ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE,
    ITEM_INDICES,
    ITEMS_INDEX_PREFIX,
)
from stac_fastapi.types.stac import Item

# ============================================================================
# Parameter Validation
# ============================================================================


def validate_refresh(value: Union[str, bool]) -> str:
    """
    Validate the `refresh` parameter value.

    Args:
        value (Union[str, bool]): The `refresh` parameter value, which can be a string or a boolean.

    Returns:
        str: The validated value of the `refresh` parameter, which can be "true", "false", or "wait_for".
    """
    logger = logging.getLogger(__name__)

    # Handle boolean-like values using get_bool_env
    if isinstance(value, bool) or value in {
        "true",
        "false",
        "1",
        "0",
        "yes",
        "no",
        "y",
        "n",
    }:
        is_true = get_bool_env("DATABASE_REFRESH", default=value)
        return "true" if is_true else "false"

    # Normalize to lowercase for case-insensitivity
    value = value.lower()

    # Handle "wait_for" explicitly
    if value == "wait_for":
        return "wait_for"

    # Log a warning for invalid values and default to "false"
    logger.warning(
        f"Invalid value for `refresh`: '{value}'. Expected 'true', 'false', or 'wait_for'. Defaulting to 'false'."
    )
    return "false"


# ============================================================================
# Index and Document ID Utilities
# ============================================================================


@lru_cache(256)
def index_by_collection_id(collection_id: str) -> str:
    """
    Translate a collection id into an Elasticsearch index name.

    Args:
        collection_id (str): The collection id to translate into an index name.

    Returns:
        str: The index name derived from the collection id.
    """
    cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
    return (
        f"{ITEMS_INDEX_PREFIX}{cleaned.lower()}_{collection_id.encode('utf-8').hex()}"
    )


@lru_cache(256)
def index_alias_by_collection_id(collection_id: str) -> str:
    """
    Translate a collection id into an Elasticsearch index alias.

    Args:
        collection_id (str): The collection id to translate into an index alias.

    Returns:
        str: The index alias derived from the collection id.
    """
    cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
    return f"{ITEMS_INDEX_PREFIX}{cleaned}"


def indices(collection_ids: Optional[List[str]]) -> str:
    """
    Get a comma-separated string of index names for a given list of collection ids.

    Args:
        collection_ids: A list of collection ids.

    Returns:
        A string of comma-separated index names. If `collection_ids` is empty, returns the default indices.
    """
    return (
        ",".join(map(index_alias_by_collection_id, collection_ids))
        if collection_ids
        else ITEM_INDICES
    )


# ============================================================================
# Document ID and Action Generation
# ============================================================================


def mk_item_id(item_id: str, collection_id: str) -> str:
    """Create the document id for an Item in Elasticsearch.

    Args:
        item_id (str): The id of the Item.
        collection_id (str): The id of the Collection that the Item belongs to.

    Returns:
        str: The document id for the Item, combining the Item id and the Collection id, separated by a `|` character.
    """
    return f"{item_id}|{collection_id}"


def mk_actions(collection_id: str, processed_items: List[Item]) -> List[Dict[str, Any]]:
    """Create Elasticsearch bulk actions for a list of processed items.

    Args:
        collection_id (str): The identifier for the collection the items belong to.
        processed_items (List[Item]): The list of processed items to be bulk indexed.

    Returns:
        List[Dict[str, Union[str, Dict]]]: The list of bulk actions to be executed,
        each action being a dictionary with the following keys:
        - `_index`: the index to store the document in.
        - `_id`: the document's identifier.
        - `_source`: the source of the document.
    """
    index_alias = index_alias_by_collection_id(collection_id)
    return [
        {
            "_index": index_alias,
            "_id": mk_item_id(item["id"], item["collection"]),
            "_source": item,
        }
        for item in processed_items
    ]
