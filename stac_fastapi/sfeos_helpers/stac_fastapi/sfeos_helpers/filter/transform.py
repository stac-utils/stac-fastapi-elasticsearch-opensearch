"""Query transformation functions for Elasticsearch/OpenSearch."""

from typing import Any

from stac_fastapi.core.extensions.filter import (
    AdvancedComparisonOp,
    ComparisonOp,
    LogicalOp,
    SpatialOp,
)

from .cql2 import cql2_like_to_es


def to_es_field(queryables_mapping: dict[str, Any], field: str) -> list[str]:
    """
    Map a given field to its corresponding Elasticsearch field according to a predefined mapping.

    Args:
        field (str): The field name from a user query or filter.

    Returns:
        str: The mapped field name suitable for Elasticsearch queries.
    """
    # First, try to find the field as-is in the mapping
    if field in queryables_mapping:
        return queryables_mapping[field]

    # If field has 'properties.' prefix, try without it
    # This handles cases where users specify 'properties.eo:cloud_cover'
    # but queryables_mapping uses 'eo:cloud_cover' as the key
    if normalized_field := field.removeprefix("properties."):
        if normalized_field in queryables_mapping:
            return queryables_mapping[normalized_field]

    if normalized_field := field.removeprefix("assets."):
        if normalized_field in queryables_mapping:
            return queryables_mapping[normalized_field]

    # If not found, return the original field
    return [field]


def to_es(queryables_mapping: dict[str, Any], query: dict[str, Any]) -> dict[str, Any]:
    """
    Transform a simplified CQL2 query structure to an Elasticsearch compatible query DSL.

    Args:
        query (dict[str, Any]): The query dictionary containing 'op' and 'args'.

    Returns:
        dict[str, Any]: The corresponding Elasticsearch query in the form of a dictionary.
    """
    queries: list[dict[str, Any]] = [{}]
    if query["op"] in [LogicalOp.AND, LogicalOp.OR, LogicalOp.NOT]:
        bool_type = {
            LogicalOp.AND: "must",
            LogicalOp.OR: "should",
            LogicalOp.NOT: "must_not",
        }[query["op"]]
        queries = [
            {
                "bool": {
                    bool_type: [
                        sq
                        for sub_query in query["args"]
                        for sq in to_es(queryables_mapping, sub_query)
                    ]
                }
            }
        ]

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

        fields = to_es_field(queryables_mapping, query["args"][0]["property"])
        value = query["args"][1]
        if isinstance(value, dict) and "timestamp" in value:
            value = value["timestamp"]
            if query["op"] == ComparisonOp.EQ:
                queries = [
                    {"range": {field: {"gte": value, "lte": value}}} for field in fields
                ]
            elif query["op"] == ComparisonOp.NEQ:
                queries = [
                    {
                        "bool": {
                            "must_not": [
                                {"range": {field: {"gte": value, "lte": value}}}
                            ]
                        }
                    }
                    for field in fields
                ]
            else:
                queries = [
                    {"range": {field: {range_op[query["op"]]: value}}}
                    for field in fields
                ]
        else:
            if query["op"] == ComparisonOp.EQ:
                queries = [{"term": {field: value}} for field in fields]
            elif query["op"] == ComparisonOp.NEQ:
                queries = [
                    {"bool": {"must_not": [{"term": {field: value}}]}}
                    for field in fields
                ]
            else:
                queries = [
                    {"range": {field: {range_op[query["op"]]: value}}}
                    for field in fields
                ]

    elif query["op"] == ComparisonOp.IS_NULL:
        fields = to_es_field(queryables_mapping, query["args"][0]["property"])
        queries = [
            {"bool": {"must_not": {"exists": {"field": field}}}} for field in fields
        ]

    elif query["op"] == AdvancedComparisonOp.BETWEEN:
        fields = to_es_field(queryables_mapping, query["args"][0]["property"])

        # Handle both formats: [property, [lower, upper]] or [property, lower, upper]
        if len(query["args"]) == 2 and isinstance(query["args"][1], list):
            # Format: [{'property': '...'}, [lower, upper]]
            gte, lte = query["args"][1][0], query["args"][1][1]
        elif len(query["args"]) == 3:
            # Format: [{'property': '...'}, lower, upper]
            gte, lte = query["args"][1], query["args"][2]
        else:
            raise ValueError(
                f"BETWEEN operator expects 2 or 3 args, got {len(query['args'])}"
            )

        if isinstance(gte, dict) and "timestamp" in gte:
            gte = gte["timestamp"]
        if isinstance(lte, dict) and "timestamp" in lte:
            lte = lte["timestamp"]
        queries = [{"range": {field: {"gte": gte, "lte": lte}}} for field in fields]

    elif query["op"] == AdvancedComparisonOp.IN:
        fields = to_es_field(queryables_mapping, query["args"][0]["property"])
        values = query["args"][1]
        if not isinstance(values, list):
            raise ValueError(f"Arg {values} is not a list")
        queries = [{"terms": {field: values}} for field in fields]

    elif query["op"] == AdvancedComparisonOp.LIKE:
        fields = to_es_field(queryables_mapping, query["args"][0]["property"])
        pattern = cql2_like_to_es(query["args"][1])
        queries = [
            {"wildcard": {field: {"value": pattern, "case_insensitive": True}}}
            for field in fields
        ]

    elif query["op"] in [
        SpatialOp.S_INTERSECTS,
        SpatialOp.S_CONTAINS,
        SpatialOp.S_WITHIN,
        SpatialOp.S_DISJOINT,
    ]:
        fields = to_es_field(queryables_mapping, query["args"][0]["property"])
        geometry = query["args"][1]

        relation_mapping = {
            SpatialOp.S_INTERSECTS: "intersects",
            SpatialOp.S_CONTAINS: "contains",
            SpatialOp.S_WITHIN: "within",
            SpatialOp.S_DISJOINT: "disjoint",
        }

        relation = relation_mapping[query["op"]]
        queries = [
            {"geo_shape": {field: {"shape": geometry, "relation": relation}}}
            for field in fields
        ]

    return queries[0] if len(queries) == 1 else {"bool": {"should": queries}}
