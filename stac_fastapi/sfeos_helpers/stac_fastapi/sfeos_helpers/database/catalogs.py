"""Catalog-related database operations for Elasticsearch/OpenSearch.

This module provides helper functions for catalog operations that require
direct Elasticsearch/OpenSearch client access. These functions are used by
the CatalogsExtension to maintain database-agnostic code in the core module.
"""

import base64
import json
import logging
from collections.abc import Mapping
from typing import Any

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX
from stac_fastapi.types.errors import ConflictError

logger = logging.getLogger(__name__)


def decode_token_to_search_after(token: str | None) -> list | None:
    """Decode a base64-encoded pagination token to search_after list.

    Args:
        token: Base64-encoded JSON string representing search_after values.

    Returns:
        List of sort values for search_after, or None if token is invalid/empty.
    """
    if not token:
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(token.encode()).decode())
    except Exception:
        return None


def encode_search_after_to_token(search_after: list | None) -> str | None:
    """Encode search_after list to a base64-encoded pagination token.

    Args:
        search_after: List of sort values from the last hit of the previous page.

    Returns:
        Base64-encoded JSON string, or None if search_after is empty/invalid.
    """
    if not search_after:
        return None
    try:
        return base64.urlsafe_b64encode(json.dumps(search_after).encode()).decode()
    except Exception:
        return None


def _get_total_hits(hits_container: dict[str, Any]) -> int:
    """Help to extract total hits safely across ES/OpenSearch versions."""
    total_hits_data = hits_container.get("total", 0)
    if isinstance(total_hits_data, dict):
        return total_hits_data.get("value", 0)
    return total_hits_data


async def search_collections_by_parent_id_shared(
    es_client: Any, catalog_id: str, size: int = 10000
) -> list[dict[str, Any]]:
    """Search for collections that have a specific catalog as a parent.

    Args:
        es_client: Elasticsearch/OpenSearch client instance.
        catalog_id: The catalog ID to search for in parent_ids.
        size: Maximum number of results to return (default: 10000).

    Returns:
        List of collection documents from the search results.
    """
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"parent_ids": catalog_id}},
                    {"term": {"type": "Collection"}},
                ]
            }
        },
        "size": size,
    }
    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=query_body)
        return [hit["_source"] for hit in search_result["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Error searching for collections with parent {catalog_id}: {e}")
        raise


async def search_collections_by_parent_id_with_pagination_shared(
    es_client: Any,
    catalog_id: str,
    limit: int = 10,
    search_after: list | None = None,
) -> tuple[list[dict[str, Any]], int, list | None]:
    """Search for collections with a specific parent catalog using OpenSearch pagination.

    Args:
        es_client: Elasticsearch/OpenSearch client instance.
        catalog_id: The parent catalog ID to filter by.
        limit: Maximum number of results to return.
        search_after: A list of sort values from the last hit of the previous page.

    Returns:
        Tuple of (collections_list, total_hits_count, next_search_after_list).

    Raises:
        Exception: Re-raises any database connection or query errors to the caller.
    """
    query = {
        "bool": {
            "must": [
                {"term": {"parent_ids": catalog_id}},
                {"term": {"type": "Collection"}},
            ]
        }
    }

    search_params = {
        "query": query,
        "sort": [{"id": {"order": "asc"}}],
        "size": limit,
        "track_total_hits": True,
    }

    if search_after:
        search_params["search_after"] = search_after

    try:
        # 2. Use the 'body' parameter.
        # This is the most compatible way across ES and OpenSearch clients.
        search_result = await es_client.search(
            index=COLLECTIONS_INDEX, body=search_params  # DO NOT unpack here
        )
    except Exception as e:
        logger.error(f"Database error searching collections in {catalog_id}: {e}")
        raise

    hits_container = search_result.get("hits", {})
    total_hits = _get_total_hits(hits_container)
    hits = hits_container.get("hits", [])

    collections = [hit["_source"] for hit in hits]
    next_search_after = hits[-1].get("sort") if len(hits) == limit else None

    return collections, total_hits, next_search_after


async def search_sub_catalogs_with_pagination_shared(
    es_client: Any,
    catalog_id: str,
    limit: int = 10,
    search_after: list | None = None,
) -> tuple[list[dict[str, Any]], int, list | None]:
    """Search for sub-catalogs with pagination support.

    Args:
        es_client: Elasticsearch/OpenSearch client instance.
        catalog_id: The parent catalog ID.
        limit: Maximum number of results to return (default: 10).
        token: Pagination token for cursor-based pagination.

    Returns:
        Tuple of (catalogs, total_count, next_token).
    """
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"parent_ids": catalog_id}},
                    {"term": {"type": "Catalog"}},
                ]
            }
        },
        "sort": [{"id": {"order": "asc"}}],
        "size": limit,
        "track_total_hits": True,
    }
    if search_after:
        body["search_after"] = search_after

    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=body)
    except Exception as e:
        logger.error(f"Error searching for catalogs in {catalog_id}: {e}")
        raise

    hits_container = search_result.get("hits", {})
    total_hits = _get_total_hits(hits_container)
    hits = hits_container.get("hits", [])

    catalogs = [hit["_source"] for hit in hits]
    next_search_after = hits[-1].get("sort") if len(hits) == limit else None

    return catalogs, total_hits, next_search_after


async def search_children_with_pagination_shared(
    es_client: Any,
    catalog_id: str,
    limit: int = 10,
    search_after: list | None = None,
    resource_type: str | None = None,
) -> tuple[list[dict[str, Any]], int, list | None]:
    """Search for children (catalogs and collections) with pagination.

    Args:
        es_client: Elasticsearch/OpenSearch client instance.
        catalog_id: The parent catalog ID.
        limit: Maximum number of results to return (default: 10).
        token: Pagination token for cursor-based pagination.
        resource_type: Optional filter by type (Catalog or Collection).

    Returns:
        Tuple of (children, total_count, next_token).
    """
    filter_queries = [{"term": {"parent_ids": catalog_id}}]
    if resource_type:
        filter_queries.append({"term": {"type": resource_type}})

    body = {
        "query": {"bool": {"filter": filter_queries}},
        "sort": [{"id": {"order": "asc"}}],
        "size": limit,
        "track_total_hits": True,
    }
    if search_after:
        body["search_after"] = search_after

    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=body)
    except Exception as e:
        logger.error(f"Error searching for children of {catalog_id}: {e}")
        raise

    hits_container = search_result.get("hits", {})
    total_hits = _get_total_hits(hits_container)
    hits = hits_container.get("hits", [])

    children = [hit["_source"] for hit in hits]
    next_search_after = hits[-1].get("sort") if len(hits) == limit else None

    return children, total_hits, next_search_after


_PARENT_CLEANUP_BATCH_SIZE = 500
_PARENT_CLEANUP_MAX_PASSES = 3

# Adds (params.add) or removes params.parent_id in the parent_ids of one document
# of type params.type (Collection or Catalog), so a link/unlink never rewrites the
# rest of the document.
PARENT_ID_SCRIPT = """
    if (!params.type.equals(ctx._source.type)) {
        ctx.op = 'noop';
    } else if (params.add) {
        if (ctx._source.parent_ids == null) {
            ctx._source.parent_ids = new ArrayList();
        }
        if (ctx._source.parent_ids.contains(params.parent_id)) {
            ctx.op = 'noop';
        } else {
            ctx._source.parent_ids.add(params.parent_id);
        }
    } else {
        boolean removed = false;
        if (ctx._source.parent_ids instanceof List) {
            for (int i = ctx._source.parent_ids.size() - 1; i >= 0; i--) {
                if (params.parent_id.equals(ctx._source.parent_ids.get(i))) {
                    ctx._source.parent_ids.remove(i);
                    removed = true;
                }
            }
        }
        if (!removed) {
            ctx.op = 'noop';
        }
    }
"""


def _cleanup_response(response: Any) -> Mapping:
    """Unwrap Elasticsearch responses and reject incomplete response objects."""
    body = getattr(response, "body", response)
    if not isinstance(body, Mapping):
        raise RuntimeError("Malformed catalog parent cleanup response")
    return body


def _cleanup_counter(body: Mapping, key: str) -> int:
    """Require explicit nonnegative integer counters, excluding booleans."""
    value = body.get(key)
    if type(value) is not int or value < 0:
        raise RuntimeError(f"Invalid catalog parent cleanup counter: {key}")
    return value


def _check_cleanup_shards(response: Any, *, refresh: bool = False) -> Mapping:
    """Require complete successful shards for refresh and edge verification."""
    body = _cleanup_response(response)
    shards = _cleanup_response(body.get("_shards"))
    total = _cleanup_counter(shards, "total")
    successful = _cleanup_counter(shards, "successful")
    failed = _cleanup_counter(shards, "failed")
    if (
        not total
        or not successful
        or failed
        or successful > total
        or (not refresh and successful != total)
        or shards.get("failures")
    ):
        raise RuntimeError("Incomplete catalog parent cleanup shard response")
    return body


def _check_parent_cleanup_update(response: Any) -> int:
    """Reject failed or partial updates and return the version conflict count."""
    body = _cleanup_response(response)
    if body.get("timed_out") is not False:
        raise RuntimeError("Catalog parent cleanup timed out or omitted timeout status")
    counters = {
        key: _cleanup_counter(body, key)
        for key in (
            "total",
            "updated",
            "deleted",
            "noops",
            "version_conflicts",
            "batches",
        )
    }
    failures = body.get("failures")
    if not isinstance(failures, list):
        raise RuntimeError("Malformed catalog parent cleanup failures")
    for failure in failures:
        if (
            not isinstance(failure, Mapping)
            or failure.get("status") != 409
            or not isinstance(failure.get("cause"), Mapping)
            or failure["cause"].get("type") != "version_conflict_engine_exception"
        ):
            raise RuntimeError("Catalog parent cleanup update failed")
    conflicts = counters["version_conflicts"]
    if (
        counters["deleted"]
        or counters["total"] != counters["updated"] + counters["noops"] + conflicts
        or (counters["total"] and not counters["batches"])
        or len(failures) > conflicts
        or body.get("terminated_early", False) is not False
    ):
        raise RuntimeError("Incomplete catalog parent cleanup update")
    return conflicts


async def unlink_catalog_children_shared(es_client: Any, catalog_id: str) -> None:
    """Unlink a catalog's direct children before deletion, without deleting data.

    Successful updates are retained on failure, so retrying is idempotent. Callers
    must serialize graph mutations: verification cannot fence concurrent writers.
    This only cleans edges to the catalog being deleted, not historical orphans.

    Raises:
        ConflictError: Edges or version conflicts remain after three passes.
        RuntimeError: Cleanup or verification returned an incomplete/failed response.
    """
    query = {
        "bool": {
            "filter": [
                {"term": {"parent_ids": catalog_id}},
                {"terms": {"type": ["Catalog", "Collection"]}},
            ]
        }
    }
    script = {
        "lang": "painless",
        "source": """
            if (!(ctx._source.parent_ids instanceof List)) {
                throw new IllegalArgumentException('parent_ids must be a list');
            }
            for (int i = ctx._source.parent_ids.size() - 1; i >= 0; i--) {
                if (params.parent_id.equals(ctx._source.parent_ids.get(i))) {
                    ctx._source.parent_ids.remove(i);
                }
            }
        """,
        "params": {"parent_id": catalog_id},
    }
    for _ in range(_PARENT_CLEANUP_MAX_PASSES):
        # Refresh counts unassigned replicas in total; count verification below
        # requires every queried primary shard to succeed.
        _check_cleanup_shards(
            await es_client.indices.refresh(index=COLLECTIONS_INDEX), refresh=True
        )
        response = await es_client.update_by_query(
            index=COLLECTIONS_INDEX,
            body={"query": query, "script": script},
            scroll_size=_PARENT_CLEANUP_BATCH_SIZE,
            conflicts="proceed",
            wait_for_completion=True,
            refresh=True,
        )
        conflicts = _check_parent_cleanup_update(response)
        verification = _check_cleanup_shards(
            await es_client.count(index=COLLECTIONS_INDEX, body={"query": query})
        )
        if (
            verification.get("timed_out", False) is not False
            or verification.get("terminated_early", False) is not False
        ):
            raise RuntimeError("Incomplete catalog parent cleanup verification")
        remaining = _cleanup_counter(verification, "count")
        if not conflicts and not remaining:
            return
    raise ConflictError(
        f"Catalog {catalog_id} still has child relationships; retry deletion"
    )
