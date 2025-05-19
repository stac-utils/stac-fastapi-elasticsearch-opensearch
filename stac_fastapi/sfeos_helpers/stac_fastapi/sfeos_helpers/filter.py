"""Shared filter extension methods for stac-fastapi elasticsearch and opensearch backends.

This module provides shared functionality for implementing the STAC API Filter Extension
with Elasticsearch and OpenSearch. It includes:

1. Functions for converting CQL2 queries to Elasticsearch/OpenSearch query DSL
2. Helper functions for field mapping and query transformation
3. Base implementation of the AsyncBaseFiltersClient for Elasticsearch/OpenSearch

The sfeos_helpers package is organized as follows:
- database_logic_helpers.py: Shared database operations
- filter.py: Shared filter extension implementation (this file)
- mappings.py: Shared constants and mapping definitions
- utilities.py: Shared utility functions

When adding new functionality to this package, consider:
1. Will this code be used by both Elasticsearch and OpenSearch implementations?
2. Is the functionality stable and unlikely to diverge between implementations?
3. Is the function well-documented with clear input/output contracts?

Function Naming Conventions:
- All shared functions should end with `_shared` to clearly indicate they're meant to be used by both implementations
- Function names should be descriptive and indicate their purpose
- Parameter names should be consistent across similar functions
"""

import re
from collections import deque
from typing import Any, Dict, Optional

import attr

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.extensions.filter import (
    DEFAULT_QUERYABLES,
    AdvancedComparisonOp,
    ComparisonOp,
    LogicalOp,
    SpatialOp,
    cql2_like_patterns,
    valid_like_substitutions,
)
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient

from .mappings import ES_MAPPING_TYPE_TO_JSON

# ============================================================================
# CQL2 Pattern Conversion Helpers
# ============================================================================


def _replace_like_patterns(match: re.Match) -> str:
    pattern = match.group()
    try:
        return valid_like_substitutions[pattern]
    except KeyError:
        raise ValueError(f"'{pattern}' is not a valid escape sequence")


def cql2_like_to_es(string: str) -> str:
    """
    Convert CQL2 "LIKE" characters to Elasticsearch "wildcard" characters.

    Args:
        string (str): The string containing CQL2 wildcard characters.

    Returns:
        str: The converted string with Elasticsearch compatible wildcards.

    Raises:
        ValueError: If an invalid escape sequence is encountered.
    """
    return cql2_like_patterns.sub(
        repl=_replace_like_patterns,
        string=string,
    )


# ============================================================================
# Query Transformation Functions
# ============================================================================


def to_es_field(queryables_mapping: Dict[str, Any], field: str) -> str:
    """
    Map a given field to its corresponding Elasticsearch field according to a predefined mapping.

    Args:
        field (str): The field name from a user query or filter.

    Returns:
        str: The mapped field name suitable for Elasticsearch queries.
    """
    return queryables_mapping.get(field, field)


def to_es(queryables_mapping: Dict[str, Any], query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a simplified CQL2 query structure to an Elasticsearch compatible query DSL.

    Args:
        query (Dict[str, Any]): The query dictionary containing 'op' and 'args'.

    Returns:
        Dict[str, Any]: The corresponding Elasticsearch query in the form of a dictionary.
    """
    if query["op"] in [LogicalOp.AND, LogicalOp.OR, LogicalOp.NOT]:
        bool_type = {
            LogicalOp.AND: "must",
            LogicalOp.OR: "should",
            LogicalOp.NOT: "must_not",
        }[query["op"]]
        return {
            "bool": {
                bool_type: [
                    to_es(queryables_mapping, sub_query) for sub_query in query["args"]
                ]
            }
        }

    elif query["op"] in [
        ComparisonOp.EQ,
        ComparisonOp.NEQ,
        ComparisonOp.LT,
        ComparisonOp.LTE,
        ComparisonOp.GT,
        ComparisonOp.GTE,
    ]:
        range_op = {
            ComparisonOp.LT: "lt",
            ComparisonOp.LTE: "lte",
            ComparisonOp.GT: "gt",
            ComparisonOp.GTE: "gte",
        }

        field = to_es_field(queryables_mapping, query["args"][0]["property"])
        value = query["args"][1]
        if isinstance(value, dict) and "timestamp" in value:
            value = value["timestamp"]
            if query["op"] == ComparisonOp.EQ:
                return {"range": {field: {"gte": value, "lte": value}}}
            elif query["op"] == ComparisonOp.NEQ:
                return {
                    "bool": {
                        "must_not": [{"range": {field: {"gte": value, "lte": value}}}]
                    }
                }
            else:
                return {"range": {field: {range_op[query["op"]]: value}}}
        else:
            if query["op"] == ComparisonOp.EQ:
                return {"term": {field: value}}
            elif query["op"] == ComparisonOp.NEQ:
                return {"bool": {"must_not": [{"term": {field: value}}]}}
            else:
                return {"range": {field: {range_op[query["op"]]: value}}}

    elif query["op"] == ComparisonOp.IS_NULL:
        field = to_es_field(queryables_mapping, query["args"][0]["property"])
        return {"bool": {"must_not": {"exists": {"field": field}}}}

    elif query["op"] == AdvancedComparisonOp.BETWEEN:
        field = to_es_field(queryables_mapping, query["args"][0]["property"])
        gte, lte = query["args"][1], query["args"][2]
        if isinstance(gte, dict) and "timestamp" in gte:
            gte = gte["timestamp"]
        if isinstance(lte, dict) and "timestamp" in lte:
            lte = lte["timestamp"]
        return {"range": {field: {"gte": gte, "lte": lte}}}

    elif query["op"] == AdvancedComparisonOp.IN:
        field = to_es_field(queryables_mapping, query["args"][0]["property"])
        values = query["args"][1]
        if not isinstance(values, list):
            raise ValueError(f"Arg {values} is not a list")
        return {"terms": {field: values}}

    elif query["op"] == AdvancedComparisonOp.LIKE:
        field = to_es_field(queryables_mapping, query["args"][0]["property"])
        pattern = cql2_like_to_es(query["args"][1])
        return {"wildcard": {field: {"value": pattern, "case_insensitive": True}}}

    elif query["op"] in [
        SpatialOp.S_INTERSECTS,
        SpatialOp.S_CONTAINS,
        SpatialOp.S_WITHIN,
        SpatialOp.S_DISJOINT,
    ]:
        field = to_es_field(queryables_mapping, query["args"][0]["property"])
        geometry = query["args"][1]

        relation_mapping = {
            SpatialOp.S_INTERSECTS: "intersects",
            SpatialOp.S_CONTAINS: "contains",
            SpatialOp.S_WITHIN: "within",
            SpatialOp.S_DISJOINT: "disjoint",
        }

        relation = relation_mapping[query["op"]]
        return {"geo_shape": {field: {"shape": geometry, "relation": relation}}}

    return {}


# ============================================================================
# Filter Client Implementation
# ============================================================================


@attr.s
class EsAsyncBaseFiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    database: BaseDatabaseLogic = attr.ib()

    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables

        Args:
            collection_id (str, optional): The id of the collection to get queryables for.
            **kwargs: additional keyword arguments

        Returns:
            Dict[str, Any]: A dictionary containing the queryables for the given collection.
        """
        queryables: Dict[str, Any] = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://stac-api.example.com/queryables",
            "type": "object",
            "title": "Queryables for STAC API",
            "description": "Queryable names for the STAC API Item Search filter.",
            "properties": DEFAULT_QUERYABLES,
            "additionalProperties": True,
        }
        if not collection_id:
            return queryables

        properties: Dict[str, Any] = queryables["properties"]
        queryables.update(
            {
                "properties": properties,
                "additionalProperties": False,
            }
        )

        mapping_data = await self.database.get_items_mapping(collection_id)
        mapping_properties = next(iter(mapping_data.values()))["mappings"]["properties"]
        stack = deque(mapping_properties.items())

        while stack:
            field_name, field_def = stack.popleft()

            # Iterate over nested fields
            field_properties = field_def.get("properties")
            if field_properties:
                # Fields in Item Properties should be exposed with their un-prefixed names,
                # and not require expressions to prefix them with properties,
                # e.g., eo:cloud_cover instead of properties.eo:cloud_cover.
                if field_name == "properties":
                    stack.extend(field_properties.items())
                else:
                    stack.extend(
                        (f"{field_name}.{k}", v) for k, v in field_properties.items()
                    )

            # Skip non-indexed or disabled fields
            field_type = field_def.get("type")
            if not field_type or not field_def.get("enabled", True):
                continue

            # Generate field properties
            field_result = DEFAULT_QUERYABLES.get(field_name, {})
            properties[field_name] = field_result

            field_name_human = field_name.replace("_", " ").title()
            field_result.setdefault("title", field_name_human)

            field_type_json = ES_MAPPING_TYPE_TO_JSON.get(field_type, field_type)
            field_result.setdefault("type", field_type_json)

            if field_type in {"date", "date_nanos"}:
                field_result.setdefault("format", "date-time")

        return queryables
