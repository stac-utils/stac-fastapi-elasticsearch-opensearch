"""Index management functions for Elasticsearch/OpenSearch.

This module provides functions for creating and managing indices in Elasticsearch/OpenSearch.
"""

import hashlib
import re
from datetime import datetime
from functools import lru_cache
from typing import Any

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
    hashed = hashlib.blake2s(collection_id.encode("utf-8")).hexdigest()[:8]
    return f"{ITEMS_INDEX_PREFIX}{cleaned.lower()}_{hashed}"


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


def indices(collection_ids: list[str] | None) -> str:
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


def _extract_date_from_alias(alias: str) -> tuple[datetime, datetime] | None:
    """Extract date range from an index alias name.

    Parses dates in YYYY-MM-DD format from the alias string. If two dates are found,
    returns them as a range. If one date is found, returns it as both start and end.

    Args:
        alias: Index alias name containing embedded dates.

    Returns:
        Tuple of (begin_date, end_date) or None if no dates found.
    """
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    try:
        dates = date_pattern.findall(alias)

        if not dates:
            return None

        if len(dates) >= 2:
            return datetime.strptime(dates[-2], "%Y-%m-%d"), datetime.strptime(
                dates[-1], "%Y-%m-%d"
            )
        else:
            date = datetime.strptime(dates[-1], "%Y-%m-%d")
            return date, date
    except (ValueError, IndexError):
        return None


def _parse_search_date(date_str: str | None) -> datetime | None:
    """Parse an ISO format datetime string into a timezone-naive datetime.

    Args:
        date_str: ISO format datetime string (e.g. "2020-01-01T00:00:00Z") or None.

    Returns:
        Timezone-naive datetime or None if input is empty.
    """
    if not date_str:
        return None
    return datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)


def filter_indexes_by_datetime(
    collection_indexes: list[tuple[dict[str, str], ...]],
    datetime_search: dict[str, dict[str, str | None]],
    use_datetime: bool,
) -> list[str]:
    """
    Filter Elasticsearch index aliases based on datetime search criteria.

    Filters a list of collection indexes by matching their datetime, start_datetime, and end_datetime
    aliases against the provided search criteria. Each criterion can have optional 'gte' (greater than
    or equal) and 'lte' (less than or equal) bounds.

    Args:
        collection_indexes (List[Tuple[Dict[str, str], ...]]): A list of tuples containing dictionaries
            with 'datetime', 'start_datetime', and 'end_datetime' aliases.
        datetime_search (dict[str, dict[str, str | None]]): A dictionary with keys 'datetime',
            'start_datetime', and 'end_datetime', each containing 'gte' and 'lte' criteria as ISO format
            datetime strings or None.
        use_datetime (bool): Flag determining which datetime field to filter on:
            - True: Filters using 'datetime' alias.
            - False: Filters using 'start_datetime' and 'end_datetime' aliases.

    Returns:
        list[str]: A list of start_datetime aliases that match all provided search criteria.
    """

    def check_criteria(
        value_begin: datetime,
        value_end: datetime,
        criteria: dict,
        start_value_begin: datetime | None = None,
    ) -> bool:
        gte = _parse_search_date(criteria.get("gte"))
        lte = _parse_search_date(criteria.get("lte"))

        if gte and value_end < gte:
            return False
        if start_value_begin:
            if lte and start_value_begin > lte:
                return False
        else:
            if lte and value_begin > lte:
                return False

        return True

    filtered_indexes = []

    for index_tuple in collection_indexes:
        if not index_tuple:
            continue

        index_dict = index_tuple[0]
        start_datetime_alias = index_dict.get("start_datetime")
        end_datetime_alias = index_dict.get("end_datetime")
        datetime_alias = index_dict.get("datetime")

        if start_datetime_alias:
            start_date = _extract_date_from_alias(start_datetime_alias)
            if not check_criteria(
                start_date[0], start_date[1], datetime_search.get("start_datetime", {})
            ):
                continue
        if end_datetime_alias:
            end_date = _extract_date_from_alias(end_datetime_alias)
            start_begin = start_date[0] if start_datetime_alias else None
            if not check_criteria(
                end_date[0],
                end_date[1],
                datetime_search.get("end_datetime", {}),
                start_begin,
            ):
                continue
        if datetime_alias:
            datetime_date = _extract_date_from_alias(datetime_alias)
            if not check_criteria(
                datetime_date[0], datetime_date[1], datetime_search.get("datetime", {})
            ):
                continue

        primary_datetime_alias = (
            datetime_alias if use_datetime else start_datetime_alias
        )

        if primary_datetime_alias is not None:
            filtered_indexes.append(primary_datetime_alias)

    return filtered_indexes


def filter_indexes_by_datetime_range(
    collection_indexes: list[tuple[dict[str, str], ...]],
    datetime_search: dict[str, dict[str, str | None]],
) -> list[str]:
    """Filter indexes by range intersection with query datetime range.

    Checks if the query's datetime range intersects with each index's
    [start_datetime, end_datetime] range. Two ranges [A_start, A_end] and
    [B_start, B_end] intersect if and only if A_start <= B_end AND A_end >= B_start.

    For example, if a product has start_datetime=2025-11-05 and end_datetime=2025-11-06,
    a query for 2025-11-06/2025-11-07 would match because the ranges overlap at 2025-11-06.

    Args:
        collection_indexes: List of tuples containing dictionaries with
            'start_datetime' and 'end_datetime' alias names that embed date ranges.
        datetime_search: Dictionary with 'start_datetime' and 'end_datetime' keys,
            each containing 'gte' and 'lte' criteria as ISO format datetime strings.
            The query range is derived from start_datetime.gte and end_datetime.lte.

    Returns:
        List of start_datetime alias names for indexes whose ranges intersect
        with the query range.
    """
    query_start = _parse_search_date(
        datetime_search.get("start_datetime", {}).get("gte")
    )
    query_end = _parse_search_date(datetime_search.get("end_datetime", {}).get("lte"))

    filtered_indexes = []

    for index_tuple in collection_indexes:
        if not index_tuple:
            continue

        index_dict = index_tuple[0]
        start_datetime_alias = index_dict.get("start_datetime")
        end_datetime_alias = index_dict.get("end_datetime")

        if not start_datetime_alias:
            continue

        if query_start and end_datetime_alias:
            end_dates = _extract_date_from_alias(end_datetime_alias)
            if end_dates and end_dates[1].date() < query_start.date():
                continue

        if query_end and start_datetime_alias:
            start_dates = _extract_date_from_alias(start_datetime_alias)
            if start_dates and start_dates[0].date() > query_end.date():
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
    if hasattr(client, "options"):
        resolved = await client.options(ignore_status=[404]).indices.resolve_index(
            name=name
        )
    else:
        resolved = await client.indices.resolve_index(name=name, ignore=[404])
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name, ignore=[404])
    await client.close()
