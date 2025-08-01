"""Index management functions for Elasticsearch/OpenSearch.

This module provides functions for creating and managing indices in Elasticsearch/OpenSearch.
"""

import re
from datetime import datetime
from functools import lru_cache
from typing import Any, List, Optional

from dateutil.parser import parse  # type: ignore[import]

from stac_fastapi.sfeos_helpers.mappings import (
    _ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE,
    COLLECTIONS_INDEX,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
    ES_ITEMS_SETTINGS,
    ITEM_INDICES,
    ITEMS_INDEX_PREFIX,
)


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


def filter_indexes_by_datetime(
    indexes: List[str], gte: Optional[str], lte: Optional[str]
) -> List[str]:
    """Filter indexes based on datetime range extracted from index names.

    Args:
        indexes: List of index names containing dates
        gte: Greater than or equal date filter (ISO format, optional 'Z' suffix)
        lte: Less than or equal date filter (ISO format, optional 'Z' suffix)

    Returns:
        List of filtered index names
    """

    def parse_datetime(dt_str: str) -> datetime:
        """Parse datetime string, handling both with and without 'Z' suffix."""
        return parse(dt_str).replace(tzinfo=None)

    def extract_date_range_from_index(index_name: str) -> tuple:
        """Extract start and end dates from index name."""
        date_pattern = r"(\d{4}-\d{2}-\d{2})"
        dates = re.findall(date_pattern, index_name)

        if len(dates) == 1:
            start_date = datetime.strptime(dates[0], "%Y-%m-%d")
            max_date = datetime.max.replace(microsecond=0)
            return start_date, max_date
        else:
            start_date = datetime.strptime(dates[0], "%Y-%m-%d")
            end_date = datetime.strptime(dates[1], "%Y-%m-%d")
            return start_date, end_date

    def is_index_in_range(
        start_date: datetime, end_date: datetime, gte_dt: datetime, lte_dt: datetime
    ) -> bool:
        """Check if index date range overlaps with filter range."""
        return not (
            end_date.date() < gte_dt.date() or start_date.date() > lte_dt.date()
        )

    gte_dt = parse_datetime(gte) if gte else datetime.min.replace(microsecond=0)
    lte_dt = parse_datetime(lte) if lte else datetime.max.replace(microsecond=0)

    filtered_indexes = []

    for index in indexes:
        start_date, end_date = extract_date_range_from_index(index)
        if is_index_in_range(start_date, end_date, gte_dt, lte_dt):
            filtered_indexes.append(index)

    return filtered_indexes


async def create_index_templates_shared(settings: Any) -> None:
    """Create index templates for Elasticsearch/OpenSearch Collection and Item indices.

    Args:
        settings (Any): The settings object containing the client configuration.
            Must have a create_client attribute that returns an Elasticsearch/OpenSearch client.

    Returns:
        None: This function doesn't return any value but creates index templates in the database.

    Notes:
        This function creates two index templates:
        1. A template for the Collections index with the appropriate mappings
        2. A template for the Items indices with both settings and mappings

        These templates ensure that any new indices created with matching patterns
        will automatically have the correct structure.
    """
    client = settings.create_client
    await client.indices.put_index_template(
        name=f"template_{COLLECTIONS_INDEX}",
        body={
            "index_patterns": [f"{COLLECTIONS_INDEX}*"],
            "template": {"mappings": ES_COLLECTIONS_MAPPINGS},
        },
    )
    await client.indices.put_index_template(
        name=f"template_{ITEMS_INDEX_PREFIX}",
        body={
            "index_patterns": [f"{ITEMS_INDEX_PREFIX}*"],
            "template": {"settings": ES_ITEMS_SETTINGS, "mappings": ES_ITEMS_MAPPINGS},
        },
    )
    await client.close()


async def delete_item_index_shared(settings: Any, collection_id: str) -> None:
    """Delete the index for items in a collection.

    Args:
        settings (Any): The settings object containing the client configuration.
            Must have a create_client attribute that returns an Elasticsearch/OpenSearch client.
        collection_id (str): The ID of the collection whose items index will be deleted.

    Returns:
        None: This function doesn't return any value but deletes an item index in the database.

    Notes:
        This function deletes an item index and its alias. It first resolves the alias to find
        the actual index name, then deletes both the alias and the index.
    """
    client = settings.create_client

    name = index_alias_by_collection_id(collection_id)
    resolved = await client.indices.resolve_index(name=name, ignore=[404])
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name, ignore=[404])
    await client.close()
