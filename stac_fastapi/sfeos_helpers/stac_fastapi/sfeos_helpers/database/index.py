"""Index management functions for Elasticsearch/OpenSearch.

This module provides functions for creating and managing indices in Elasticsearch/OpenSearch.
"""

import hashlib
import logging
import re
from datetime import datetime
from functools import lru_cache
from typing import Any
from urllib.parse import quote

from fastapi import HTTPException

from stac_fastapi.core.etag import DocumentVersion
from stac_fastapi.core.utilities import items_index_strategy
from stac_fastapi.sfeos_helpers.mappings import (
    _ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE,
    COLLECTIONS_INDEX,
    ES_COLLECTIONS_MAPPINGS,
    ES_ITEMS_MAPPINGS,
    ES_ITEMS_SETTINGS,
    ITEM_INDICES,
    ITEMS_ALIAS_PREFIX,
    ITEMS_INDEX_PREFIX,
    SHARED_ITEMS_INDEX,
)
from stac_fastapi.types.errors import ConflictError

logger = logging.getLogger(__name__)

GENERATION_FIELD = "collection_generation"
GENERATION_DISCOVERY_BATCH_SIZE = 1000


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


def _shared_alias_by_collection_id(collection_id: str) -> str:
    digest = hashlib.sha256(collection_id.encode("utf-8")).hexdigest()
    return f"{ITEMS_ALIAS_PREFIX}collection_{digest}"


def shared_alias_filter(collection_id: str, generation: str | None) -> dict:
    """Return the shared alias filter of one creation of a collection.

    A collection created before generations existed (`generation` None) keeps
    the term-only filter its alias was created with.
    """
    term = {"term": {"collection": collection_id}}
    if generation is None:
        return term
    return {"bool": {"filter": [term, {"term": {GENERATION_FIELD: generation}}]}}


def stamp_collection_generation(item: dict, generation: str | None) -> dict:
    """Set the item's generation to its collection's, or drop it if there is none."""
    if generation is None:
        item.pop(GENERATION_FIELD, None)
    else:
        item[GENERATION_FIELD] = generation
    return item


async def ensure_generation_mappings(client: Any) -> None:
    """Map the generation field on indexes created before it existed.

    Adding a field is allowed under strict dynamic mappings; an index created
    later gets it from its template.
    """
    quiet, ignore_404 = _ignoring_404(client)
    for index in (COLLECTIONS_INDEX, SHARED_ITEMS_INDEX):
        await quiet.indices.put_mapping(
            index=index,
            body={"properties": {GENERATION_FIELD: {"type": "keyword"}}},
            **ignore_404,
        )


async def reconcile_shared_alias(client: Any, collection_id: str) -> None:
    """Point the collection's shared alias at the generation stored now.

    Every alias writer re-reads after its put: one paused before its put while
    the id was deleted and created again would otherwise leave the old filter.
    A missing collection document stops it without putting anything.
    """
    quiet, ignore_404 = _ignoring_404(client)
    put = None
    while True:
        doc = await quiet.get(index=COLLECTIONS_INDEX, id=collection_id, **ignore_404)
        if not doc.get("found"):
            return
        wanted = shared_alias_filter(
            collection_id, doc["_source"].get(GENERATION_FIELD)
        )
        if wanted == put:
            return
        await client.indices.put_alias(
            index=SHARED_ITEMS_INDEX,
            name=_shared_alias_by_collection_id(collection_id),
            body={"filter": wanted},
        )
        put = wanted


async def restore_shared_alias(client: Any, collection_id: str) -> None:
    """Put back the alias of a live collection whose shared delete answered 409.

    Stateless, so it also covers a replayed delete that cannot see what an
    earlier attempt removed. Nothing is put while either alias name of the
    collection resolves; a stray index under the shared name is left to the
    retried DELETE.
    """
    quiet, ignore_404 = _ignoring_404(client)
    if not await client.exists(index=COLLECTIONS_INDEX, id=collection_id):
        return
    if not await client.indices.exists(index=SHARED_ITEMS_INDEX):
        return
    shared_name = _shared_alias_by_collection_id(collection_id)
    names = [shared_name]
    if ":" not in collection_id:
        cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
        names.append(f"{ITEMS_ALIAS_PREFIX}{cleaned}")
    for name in names:
        resolved = await quiet.indices.resolve_index(name=name, **ignore_404)
        if resolved.get("aliases") or resolved.get("indices"):
            if name == shared_name and resolved.get("indices"):
                logger.warning(
                    "Not restoring the alias of collection %s: index %s exists",
                    collection_id,
                    name,
                )
            return
    await reconcile_shared_alias(client, collection_id)


def index_alias_by_collection_id(collection_id: str) -> str:
    """
    Translate a collection id into an Elasticsearch index alias.

    Args:
        collection_id (str): The collection id to translate into an index alias.

    Returns:
        str: The index alias derived from the collection id.
    """
    if collection_id == "*":
        return f"{ITEMS_ALIAS_PREFIX}*"
    if items_index_strategy() == "shared":
        return _shared_alias_by_collection_id(collection_id)
    cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
    return f"{ITEMS_ALIAS_PREFIX}{cleaned}"


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


async def delete_item_index_shared(
    settings: Any,
    collection_id: str,
    expected: DocumentVersion | None = None,
    generation: str | None = None,
) -> None:
    """Delete the items of a collection: its index, or its docs in the shared index.

    The path follows what the collection alias resolves to, not the configured
    strategy: a legacy alias on another index deletes that index, as before; an alias
    on the shared index, or none at all (a retried delete), deletes the
    collection's documents from the shared index and any stray index named
    after the alias. The shared index itself is never deleted.

    Args:
        settings (Any): Settings whose `create_client` returns a new client.
        collection_id (str): The ID of the collection whose items are deleted.
        expected (DocumentVersion, optional): The collection version the delete
            started from; any other version, or none, raises `ConflictError`
            before each step that removes items.
        generation (str, optional): The generation of the collection document
            being deleted; only its alias is accepted. With `expected`, items
            of this generation, of any generation found while the document
            still exists, and items without one are deleted; without
            `expected`, only items of this generation (or, without one, only
            items that have no generation).
    """
    client = settings.create_client
    try:
        quiet, ignore_404 = _ignoring_404(client)
        shared_name = _shared_alias_by_collection_id(collection_id)
        name = shared_name
        resolved = await quiet.indices.resolve_index(name=name, **ignore_404)
        if not (resolved.get("aliases") or resolved.get("indices")) and not (
            items_index_strategy() == "shared" and ":" in collection_id
        ):
            cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
            name = f"{ITEMS_ALIAS_PREFIX}{'*' if collection_id == '*' else cleaned}"
            resolved = await quiet.indices.resolve_index(name=name, **ignore_404)
            if items_index_strategy() == "shared" and not (
                resolved.get("aliases") or resolved.get("indices")
            ):
                name = shared_name
        await _check_collection_version(client, collection_id, expected)

        # Exact names only: the alias of collection "*" is the pattern `items_*`.
        alias = next(
            (a for a in resolved.get("aliases", []) if a["name"] == name), None
        )
        if alias:
            if SHARED_ITEMS_INDEX in alias["indices"]:
                aliases = await client.indices.get_alias(name=name)
                alias_filter = aliases[SHARED_ITEMS_INDEX]["aliases"][name].get(
                    "filter"
                )
                owned = shared_alias_filter(collection_id, generation)
                if alias["indices"] != [SHARED_ITEMS_INDEX] or alias_filter != owned:
                    raise ConflictError(
                        f"Alias {name} does not belong to collection {collection_id}"
                    )
            elif name == shared_name:
                raise ConflictError(
                    f"Alias {name} does not target the shared items index"
                )
            await _check_collection_version(client, collection_id, expected)
            if SHARED_ITEMS_INDEX not in alias["indices"]:
                # Deleting the index drops its alias, so no request runs between
                # the version check and the removal of the items.
                await client.indices.delete(index=alias["indices"])
                return
            await client.indices.delete_alias(index=alias["indices"], name=name)

        await _delete_shared_items(client, collection_id, expected, generation)
        # A write to the removed alias, even one during the delete above,
        # auto-creates an index under the alias name.
        if "*" not in name and name != SHARED_ITEMS_INDEX:
            resolved = await quiet.indices.resolve_index(name=name, **ignore_404)
            # Put back by the 409 of an overlapping delete of the collection.
            if any(a["name"] == name for a in resolved.get("aliases", [])):
                raise ConflictError(
                    f"Collection {collection_id} changed during deletion; retry DELETE"
                )
            if any(i["name"] == name for i in resolved.get("indices", [])):
                await _check_collection_version(client, collection_id, expected)
                lenient, ignore = _ignoring(client, [400, 404])
                response = await lenient.indices.delete(index=name, **ignore)
                # The name became an alias after it was resolved.
                if response.get("status") == 400:
                    raise ConflictError(
                        f"Collection {collection_id} changed during deletion; "
                        "retry DELETE"
                    )
    finally:
        await client.close()


def _ignoring(client: Any, statuses: list[int]) -> tuple[Any, dict[str, Any]]:
    """Return the client and call arguments that make a request accept `statuses`."""
    if hasattr(client, "options"):
        return client.options(ignore_status=statuses), {}
    return client, {"ignore": statuses}


def _ignoring_404(client: Any) -> tuple[Any, dict[str, Any]]:
    """Return the client and call arguments that make a request accept a 404."""
    return _ignoring(client, [404])


async def check_legacy_collection_delete(client: Any, collection_id: str) -> None:
    """Keep canonical shared collection metadata until cleanup runs in shared mode."""
    if ":" not in collection_id:
        return
    quiet, ignore_404 = _ignoring_404(client)
    resolved = await quiet.indices.resolve_index(name=SHARED_ITEMS_INDEX, **ignore_404)
    if any(
        entry["name"] == SHARED_ITEMS_INDEX for entry in resolved.get("indices", [])
    ):
        raise ConflictError(
            f"Collection {collection_id} may use the shared items index; "
            "retry DELETE with ITEMS_INDEX_STRATEGY=shared"
        )


async def _check_collection_version(
    client: Any, collection_id: str, expected: DocumentVersion | None
) -> Any:
    if expected is None:
        return None
    quiet, ignore_404 = _ignoring_404(client)
    doc = await quiet.get(index=COLLECTIONS_INDEX, id=collection_id, **ignore_404)
    if not doc.get("found") or DocumentVersion.from_hit(doc) != expected:
        raise ConflictError(
            f"Collection {collection_id} changed during deletion; retry DELETE"
        )
    return doc


def _current_generation_query(collection_id: str, generation: str | None) -> dict:
    if generation is not None:
        return shared_alias_filter(collection_id, generation)
    return {
        "bool": {
            "filter": [{"term": {"collection": collection_id}}],
            "must_not": [{"exists": {"field": GENERATION_FIELD}}],
        }
    }


def _unfinished_delete(collection_id: str) -> HTTPException:
    return HTTPException(
        status_code=500,
        detail=f"Deleting the items of collection {collection_id} did not finish; "
        "retry DELETE",
    )


async def _seen_generations_query(
    client: Any,
    collection_id: str,
    expected: DocumentVersion,
    generation: str | None,
) -> tuple[dict | None, bool]:
    """Return a query for the generations of `collection_id` stored right now.

    The version check after the search proves the collection document existed
    throughout it, so every generation seen is the current one or a dead earlier
    one. Also returns whether more generations remain than one batch covers;
    the query is None when the shared index is gone.
    """
    lenient, ignore = _ignoring(client, [400, 404])
    response = await lenient.search(
        index=SHARED_ITEMS_INDEX,
        body={
            "size": 0,
            "query": {"term": {"collection": collection_id}},
            "aggs": {
                "generations": {
                    "terms": {
                        "field": GENERATION_FIELD,
                        "size": GENERATION_DISCOVERY_BATCH_SIZE,
                    }
                }
            },
        },
        **ignore,
    )
    current = await _check_collection_version(client, collection_id, expected)
    # A collections index rebuilt under the same name can repeat a version.
    if current["_source"].get(GENERATION_FIELD) != generation:
        raise ConflictError(
            f"Collection {collection_id} changed during deletion; retry DELETE"
        )
    if response.get("status") == 404:
        return None, False
    if response.get("status") == 400:
        # ponytail: e.g. a generation field without doc values can't be aggregated;
        # any 400 keeps the earlier current-generation delete, so stale rows stay.
        return _current_generation_query(collection_id, generation), False
    shards = response["_shards"]
    # OpenSearch counts an unavailable shard as neither successful nor failed.
    if response.get("timed_out") or shards["successful"] != shards["total"]:
        raise _unfinished_delete(collection_id)
    found = response["aggregations"]["generations"]
    seen = {bucket["key"] for bucket in found["buckets"]}
    if isinstance(generation, str):
        seen.add(generation)
    matches: list[dict] = [
        {"bool": {"must_not": [{"exists": {"field": GENERATION_FIELD}}]}}
    ]
    if seen:
        matches.append({"terms": {GENERATION_FIELD: sorted(seen)}})
    query = {
        "bool": {
            "filter": [{"term": {"collection": collection_id}}],
            "should": matches,
            "minimum_should_match": 1,
        }
    }
    return query, found["sum_other_doc_count"] > 0


async def _delete_shared_items(
    client: Any,
    collection_id: str,
    expected: DocumentVersion | None,
    generation: str | None,
) -> None:
    quiet, ignore_404 = _ignoring_404(client)
    conflict_retries = 1
    while True:
        # Search and delete_by_query only see refreshed documents.
        refreshed = await quiet.indices.refresh(index=SHARED_ITEMS_INDEX, **ignore_404)
        shards = refreshed.get("_shards")
        # A missing index has no shards; a shard with no refreshed copy is a miss.
        if shards and (shards.get("failed") or not shards.get("successful")):
            await _check_collection_version(client, collection_id, expected)
            raise _unfinished_delete(collection_id)
        if expected is None:
            # The document is already gone and the id may be live again, so
            # only the deleted generation's items go.
            query = _current_generation_query(collection_id, generation)
            has_more = False
        else:
            query, has_more = await _seen_generations_query(
                client, collection_id, expected, generation
            )
            if query is None:
                return
        result = await quiet.delete_by_query(
            index=SHARED_ITEMS_INDEX,
            body={"query": query},
            conflicts="proceed",
            refresh=True,
            wait_for_completion=True,
            slices="auto",
            **ignore_404,
        )
        if result.get("status") == 404:
            # A lost scroll context is also a 404, with the items still there.
            if "index_not_found" in str(result.get("error")):
                return
            raise _unfinished_delete(collection_id)
        # A cancelled slice is reported only in its own status.
        slices = [
            entry for entry in result.get("slices", []) if isinstance(entry, dict)
        ]
        if (
            result.get("failures")
            or result.get("timed_out")
            or any(status.get("canceled") for status in [result, *slices])
        ):
            raise _unfinished_delete(collection_id)
        if result.get("version_conflicts"):
            if not conflict_retries:
                raise _unfinished_delete(collection_id)
            conflict_retries -= 1
        elif not has_more:
            return


async def index_behind_alias(client: Any, alias: str, index: str) -> bool:
    """Tell whether `index` is one of the indices that `alias` resolves to.

    A conditional write or delete that targets the version's index calls this
    first: an alias moved to another index since the version was read means the
    version is gone, and the write would land in, or the cleanup touch, the
    wrong index.
    """
    resolved = await client.indices.resolve_index(name=alias)
    behind = {entry["name"] for entry in resolved.get("indices", [])}
    for entry in resolved.get("aliases", []):
        behind.update(entry.get("indices", []))
    return index in behind


# Keeps each resolve request line well under the 4 KB HTTP line limit.
_RESOLVE_BATCH_CHARS = 3000


async def resolve_aliases_shared(
    client: Any, aliases: list[str]
) -> dict[str, tuple[str, ...]]:
    """Map each existing alias or index name to its sorted backing indices.

    Names are resolved in comma-joined batches; missing names are left out.
    """
    # ES and OS clients pass ignore_unavailable differently.
    if hasattr(client, "options"):
        ignore_unavailable: dict[str, Any] = {"ignore_unavailable": True}
    else:
        ignore_unavailable = {"params": {"ignore_unavailable": "true"}}

    batches: list[list[str]] = []
    batch_chars = _RESOLVE_BATCH_CHARS
    for name in dict.fromkeys(aliases):
        # The client percent-encodes the names into the URL.
        name_chars = len(quote(name, safe="")) + 1
        if batch_chars + name_chars > _RESOLVE_BATCH_CHARS:
            batches.append([])
            batch_chars = 0
        batches[-1].append(name)
        batch_chars += name_chars

    backing: dict[str, tuple[str, ...]] = {}
    for batch in batches:
        resolved = await client.indices.resolve_index(
            name=",".join(batch), **ignore_unavailable
        )
        for entry in resolved.get("indices", []):
            backing[entry["name"]] = (entry["name"],)
        for entry in resolved.get("aliases", []):
            backing[entry["name"]] = tuple(sorted(entry.get("indices", [])))
    return backing
