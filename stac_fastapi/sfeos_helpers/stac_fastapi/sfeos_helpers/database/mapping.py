"""Mapping functions for Elasticsearch/OpenSearch.

This module provides functions for working with Elasticsearch/OpenSearch mappings.
"""

import os
from collections import deque
from typing import Any, Dict, Set


def _get_excluded_from_queryables() -> Set[str]:
    """Get fields to exclude from queryables endpoint and filtering.

    Reads from EXCLUDED_FROM_QUERYABLES environment variable.
    Supports comma-separated list of field names.

    For each exclusion pattern, both the original and the version with/without
    'properties.' prefix are included. This ensures fields are excluded regardless
    of whether they appear at the top level or under 'properties' in the mapping.

    Example:
        EXCLUDED_FROM_QUERYABLES="properties.auth:schemes,storage:schemes"

        This will exclude:
        - properties.auth:schemes (and children like properties.auth:schemes.s3.type)
        - auth:schemes (and children like auth:schemes.s3.type)
        - storage:schemes (and children)
        - properties.storage:schemes (and children)

    Returns:
        Set[str]: Set of field names to exclude from queryables
    """
    excluded = os.getenv("EXCLUDED_FROM_QUERYABLES", "")
    if not excluded:
        return set()

    result = set()
    for field in excluded.split(","):
        field = field.strip()
        if not field:
            continue

        result.add(field)

        if field.startswith("properties."):
            result.add(field.removeprefix("properties."))
        else:
            result.add(f"properties.{field}")

    return result


async def get_queryables_mapping_shared(
    mappings: Dict[str, Dict[str, Any]],
    collection_id: str = "*",
) -> Dict[str, str]:
    """Retrieve mapping of Queryables for search.

    Fields listed in the EXCLUDED_FROM_QUERYABLES environment variable will be
    excluded from the result, along with their children.

    Args:
        mappings (Dict[str, Dict[str, Any]]): The mapping information returned from
            Elasticsearch/OpenSearch client's indices.get_mapping() method.
            Expected structure is {index_name: {"mappings": {...}}}.
        collection_id (str, optional): The id of the Collection the Queryables
            belongs to. Defaults to "*".

    Returns:
        Dict[str, str]: A dictionary containing the Queryables mappings, where keys are
            field names (with 'properties.' prefix removed) and values are the
            corresponding paths in the Elasticsearch/OpenSearch document structure.
    """
    queryables_mapping = {}
    excluded = _get_excluded_from_queryables()

    def is_excluded(path: str) -> bool:
        """Check if the path starts with any excluded prefix."""
        return any(
            path == prefix or path.startswith(prefix + ".") for prefix in excluded
        )

    for mapping in mappings.values():
        mapping_properties = mapping["mappings"].get("properties", {})

        stack: deque[tuple[str, Dict[str, Any]]] = deque(mapping_properties.items())

        while stack:
            field_fqn, field_def = stack.popleft()

            nested_properties = field_def.get("properties")
            if nested_properties:
                stack.extend(
                    (f"{field_fqn}.{k}", v)
                    for k, v in nested_properties.items()
                    if v.get("enabled", True) and not is_excluded(f"{field_fqn}.{k}")
                )

            field_type = field_def.get("type")
            if (
                not field_type
                or not field_def.get("enabled", True)
                or is_excluded(field_fqn)
            ):
                continue

            field_name = field_fqn.removeprefix("properties.")

            queryables_mapping[field_name] = field_fqn

    return queryables_mapping
