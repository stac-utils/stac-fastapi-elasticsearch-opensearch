"""Mapping functions for Elasticsearch/OpenSearch.

This module provides functions for working with Elasticsearch/OpenSearch mappings.
"""

from typing import Any, Dict


async def get_queryables_mapping_shared(
    mappings: Dict[str, Dict[str, Any]], collection_id: str = "*"
) -> Dict[str, str]:
    """Retrieve mapping of Queryables for search.

    Args:
        mappings (Dict[str, Dict[str, Any]]): The mapping information returned from
            Elasticsearch/OpenSearch client's indices.get_mapping() method.
            Expected structure is {index_name: {"mappings": {...}}}.
        collection_id (str, optional): The id of the Collection the Queryables
            belongs to. Defaults to "*".

    Returns:
        Dict[str, str]: A dictionary containing the Queryables mappings, where keys are
            field names and values are the corresponding paths in the Elasticsearch/OpenSearch
            document structure.
    """
    queryables_mapping = {}

    for mapping in mappings.values():
        fields = mapping["mappings"].get("properties", {})
        properties = fields.pop("properties", {}).get("properties", {}).keys()

        for field_key in fields:
            queryables_mapping[field_key] = field_key

        for property_key in properties:
            queryables_mapping[property_key] = f"properties.{property_key}"

    return queryables_mapping
