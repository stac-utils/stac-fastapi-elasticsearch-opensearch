"""Query transformation functions for Elasticsearch/OpenSearch."""

from typing import Any, Dict

from stac_fastapi.core.extensions.filter import (
    AdvancedComparisonOp,
    ComparisonOp,
    LogicalOp,
    SpatialOp,
)

from .cql2 import cql2_like_to_es


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
