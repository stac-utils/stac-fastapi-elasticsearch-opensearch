"""Catalog-related database operations for Elasticsearch/OpenSearch.

This module provides helper functions for catalog operations that require
direct Elasticsearch/OpenSearch client access. These functions are used by
the CatalogsExtension to maintain database-agnostic code in the core module.
"""

import logging
from typing import Any, Dict, List, Optional

from stac_fastapi.sfeos_helpers.mappings import COLLECTIONS_INDEX

logger = logging.getLogger(__name__)


async def search_collections_by_parent_id_shared(
    es_client: Any, catalog_id: str, size: int = 10000
) -> List[Dict[str, Any]]:
    """Search for collections that have a specific catalog as a parent.

    Args:
        es_client: Elasticsearch/OpenSearch client instance.
        catalog_id: The catalog ID to search for in parent_ids.
        size: Maximum number of results to return (default: 10000).

    Returns:
        List of collection documents from the search results.
    """
    query_body = {"query": {"term": {"parent_ids": catalog_id}}, "size": size}
    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=query_body)
        return [hit["_source"] for hit in search_result["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Error searching for collections with parent {catalog_id}: {e}")
        return []


async def search_sub_catalogs_with_pagination_shared(
    es_client: Any,
    catalog_id: str,
    limit: int = 10,
    token: Optional[str] = None,
) -> tuple[List[Dict[str, Any]], int, Optional[str]]:
    """Search for sub-catalogs with pagination support.

    Args:
        es_client: Elasticsearch/OpenSearch client instance.
        catalog_id: The parent catalog ID.
        limit: Maximum number of results to return (default: 10).
        token: Pagination token for cursor-based pagination.

    Returns:
        Tuple of (catalogs, total_count, next_token).
    """
    sort_fields: List[Dict[str, Any]] = [{"id": {"order": "asc"}}]
    query_body: Dict[str, Any] = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"parent_ids": catalog_id}},
                    {"term": {"type": "Catalog"}},
                ]
            }
        },
        "sort": sort_fields,
        "size": limit,
    }

    # Handle pagination cursor (token)
    # Token format: "value1|value2|..." matching the sort fields
    if token:
        try:
            search_after = token.split("|")
            if len(search_after) == len(sort_fields):
                query_body["search_after"] = search_after
        except Exception:
            logger.debug(f"Invalid pagination token: {token}")

    # Execute the search
    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=query_body)
    except Exception as e:
        logger.error(f"Error searching for catalogs with parent {catalog_id}: {e}")
        search_result = {"hits": {"hits": []}}

    # Process results
    hits = search_result.get("hits", {}).get("hits", [])
    total_hits = search_result.get("hits", {}).get("total", {}).get("value", 0)

    catalogs = [hit["_source"] for hit in hits]

    # Generate next token if more results exist
    next_token = None
    if len(hits) == limit and len(catalogs) > 0:
        last_hit_sort = hits[-1].get("sort")
        if last_hit_sort:
            next_token = "|".join(str(x) for x in last_hit_sort)

    return catalogs, total_hits, next_token


async def update_catalog_in_index_shared(
    es_client: Any, catalog_id: str, catalog_data: Dict[str, Any]
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
    token: Optional[str] = None,
    resource_type: Optional[str] = None,
) -> tuple[List[Dict[str, Any]], int, Optional[str]]:
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
    # Base filter: Parent match
    filter_queries = [{"term": {"parent_ids": catalog_id}}]

    # Optional filter: Type
    if resource_type:
        filter_queries.append({"term": {"type": resource_type}})

    body = {
        "query": {"bool": {"filter": filter_queries}},
        "sort": [{"id": {"order": "asc"}}],
        "size": limit,
    }

    # Handle search_after token
    search_after: Optional[List[str]] = None
    if token:
        try:
            search_after_parts = token.split("|")
            # If the number of sort fields doesn't match token parts, ignore the token
            if len(search_after_parts) == len(body["sort"]):  # type: ignore
                search_after = search_after_parts
        except Exception:
            search_after = None

        if search_after is not None:
            body["search_after"] = search_after

    # Execute search
    try:
        search_result = await es_client.search(index=COLLECTIONS_INDEX, body=body)
    except Exception as e:
        logger.error(f"Error searching for children of catalog {catalog_id}: {e}")
        search_result = {"hits": {"hits": []}}

    # Process results
    hits = search_result.get("hits", {}).get("hits", [])
    total = search_result.get("hits", {}).get("total", {}).get("value", 0)

    children = [hit["_source"] for hit in hits]

    # Generate next token if more results exist
    next_token = None
    if len(hits) == limit:
        next_token_values = hits[-1].get("sort")
        if next_token_values:
            next_token = "|".join(str(val) for val in next_token_values)

    return children, total, next_token
