"""Index management functions for Elasticsearch/OpenSearch.

This module provides functions for creating and managing indices in Elasticsearch/OpenSearch.
"""

import re
from datetime import datetime
from functools import lru_cache
from typing import Any, List, Optional, Tuple, Dict

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
    collection_indexes: List[Tuple[Dict[str, str], ...]],
    datetime_search: Dict[str, Dict[str, str | None]]
) -> List[str]:
    def extract_date_from_alias(alias: str) -> tuple[datetime, datetime] | None:
        date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        try:
            dates = date_pattern.findall(alias)

            if not dates:
                return None

            if len(dates) >= 2:
                return datetime.strptime(dates[-2], '%Y-%m-%d'), datetime.strptime(dates[-1], '%Y-%m-%d')
            else:
                date = datetime.strptime(dates[-1], '%Y-%m-%d')
                return date, date
        except (ValueError, IndexError):
            return None

    def parse_search_date(date_str: str | None) -> Optional[datetime.date]:
        if not date_str:
            return None
        return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()

    def check_criteria(value_begin: datetime.date, value_end: datetime.date, criteria: Dict) -> bool:
        gte = parse_search_date(criteria.get('gte'))
        lte = parse_search_date(criteria.get('lte'))

        if gte and value_begin.date() < gte:
            return False
        if lte and value_end.date() > lte:
            return False
        return True

    filtered_indexes = []

    for index_tuple in collection_indexes:
        if not index_tuple:
            continue

        index_dict = index_tuple[0]
        start_datetime_alias = index_dict.get('start_datetime')
        end_datetime_alias = index_dict.get('end_datetime')
        datetime_alias = index_dict.get('datetime')

        if not start_datetime_alias:
            continue

        start_range = extract_date_from_alias(start_datetime_alias)
        end_date = extract_date_from_alias(end_datetime_alias)
        datetime_date = extract_date_from_alias(datetime_alias)

        if not start_range or not end_date or not datetime_date:
            continue

        if not check_criteria(start_range[0], start_range[1], datetime_search.get('start_datetime', {})):
            continue
        if not check_criteria(end_date[0], end_date[1], datetime_search.get('end_datetime', {})):
            continue
        if not check_criteria(datetime_date[0], datetime_date[1], datetime_search.get('datetime', {})):
            continue

        filtered_indexes.append(start_datetime_alias)

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
