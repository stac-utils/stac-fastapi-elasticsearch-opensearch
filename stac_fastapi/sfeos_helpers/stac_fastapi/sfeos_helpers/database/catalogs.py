"""Catalog-related database operations for Elasticsearch/OpenSearch.

This module provides helper functions for catalog operations that require
direct Elasticsearch/OpenSearch client access. These functions are used by
the CatalogsExtension to maintain database-agnostic code in the core module.
"""

import logging
from typing import Any

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

logger = logging.getLogger(__name__)


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
    sort_fields = [{"id": {"order": "asc"}}]
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"parent_ids": catalog_id}},
                    {"term": {"type": "Collection"}},
                ]
            }
        },
        "sort": sort_fields,
        "size": limit,
        "track_total_hits": True,
    }
    if search_after:
        query_body["search_after"] = search_after

    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=query_body)
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
    query_body = {
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
        "track_total_hits": True,  # Added for accuracy
    }
    if search_after:
        query_body["search_after"] = search_after

    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=query_body)
    except Exception as e:
        logger.error(f"Error searching for catalogs in {catalog_id}: {e}")
        raise

    hits_container = search_result.get("hits", {})
    total_hits = _get_total_hits(hits_container)  # Fixed: Robust total hits
    hits = hits_container.get("hits", [])

    catalogs = [hit["_source"] for hit in hits]
    next_search_after = hits[-1].get("sort") if len(hits) == limit else None

    return catalogs, total_hits, next_search_after


async def update_catalog_in_index_shared(
    es_client: Any, catalog_id: str, catalog_data: dict[str, Any]
) -> None:
    """Update a catalog document in the index.

    Args:
        es_client: Elasticsearch/OpenSearch client instance.
        catalog_id: The catalog ID.
        catalog_data: The catalog document to update.
    """
    try:
        await es_client.index(
            index=COLLECTIONS_INDEX,
            id=catalog_id,
            body=catalog_data,
            refresh=True,
        )
    except Exception as e:
        logger.error(f"Error updating catalog {catalog_id} in index: {e}")
        raise


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

    query_body = {
        "query": {"bool": {"filter": filter_queries}},
        "sort": [{"id": {"order": "asc"}}],
        "size": limit,
        "track_total_hits": True,  # Added for accuracy
    }
    if search_after:
        query_body["search_after"] = search_after

    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=query_body)
    except Exception as e:
        logger.error(f"Error searching for children of {catalog_id}: {e}")
        raise

    hits_container = search_result.get("hits", {})
    total_hits = _get_total_hits(hits_container)  # Fixed: Robust total hits
    hits = hits_container.get("hits", [])

    children = [hit["_source"] for hit in hits]
    next_search_after = hits[-1].get("sort") if len(hits) == limit else None

    return children, total_hits, next_search_after
