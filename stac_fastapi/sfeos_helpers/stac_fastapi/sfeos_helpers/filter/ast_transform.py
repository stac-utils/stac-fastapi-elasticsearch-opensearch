"""AST-based query transformation for Elasticsearch/OpenSearch."""

import os
from typing import Any, Dict, Union

from stac_fastapi.core.extensions.filter import (
    AdvancedComparisonNode,
    AdvancedComparisonOp,
    ComparisonNode,
    ComparisonOp,
    CqlNode,
    LogicalNode,
    LogicalOp,
    SpatialNode,
    SpatialOp,
)

from .transform import to_es_field

# Field path constants (should match those in database_logic.py)
PROPERTIES_DATETIME_FIELD = os.getenv("STAC_FIELD_PROP_DATETIME", "properties.datetime")
PROPERTIES_START_DATETIME_FIELD = os.getenv(
    "STAC_FIELD_PROP_START_DATETIME", "properties.start_datetime"
)
PROPERTIES_END_DATETIME_FIELD = os.getenv(
    "STAC_FIELD_PROP_END_DATETIME", "properties.end_datetime"
)
COLLECTION_FIELD = os.getenv("STAC_FIELD_COLLECTION", "collection")
GEOMETRY_FIELD = os.getenv("STAC_FIELD_GEOMETRY", "geometry")


def _get_es_field_path(field: str) -> str:
    """Get the correct Elasticsearch field path for a given logical field."""
    field_mapping = {
        "datetime": PROPERTIES_DATETIME_FIELD,
        "start_datetime": PROPERTIES_START_DATETIME_FIELD,
        "end_datetime": PROPERTIES_END_DATETIME_FIELD,
        "collection": COLLECTION_FIELD,
        "geometry": GEOMETRY_FIELD,
    }
    return field_mapping.get(field, field)


def to_es_via_ast(
    queryables_mapping: Dict[str, Any], query: Union[Dict[str, Any], CqlNode]
) -> Dict[str, Any]:
    """Transform CQL2 query to Elasticsearch/Opensearch query via AST."""
    from .ast_parser import Cql2AstParser

    if isinstance(query, CqlNode):
        ast = query
    else:
        parser = Cql2AstParser()
        ast = parser.parse(query)
    result = _transform_ast_node(queryables_mapping, ast)
    return result


def _transform_ast_node(
    queryables_mapping: Dict[str, Any], node: Any
) -> Dict[str, Any]:
    """Transform AST node to Elasticsearch/Opensearch query."""
    if isinstance(node, LogicalNode):
        bool_type = {
            LogicalOp.AND: "must",
            LogicalOp.OR: "should",
            LogicalOp.NOT: "must_not",
        }[node.op]

        if node.op == LogicalOp.NOT:
            return {
                "bool": {
                    bool_type: _transform_ast_node(queryables_mapping, node.children[0])
                }
            }
        else:
            return {
                "bool": {
                    bool_type: [
                        _transform_ast_node(queryables_mapping, child)
                        for child in node.children
                    ]
                }
            }

    elif isinstance(node, ComparisonNode):
        # Map the field using queryables_mapping
        fields = to_es_field(queryables_mapping, node.field)
        value = node.value

        if isinstance(value, dict) and "timestamp" in value:
            value = value["timestamp"]

        # Build queries for each mapped field
        queries = []
        for field in fields:
            if node.op == ComparisonOp.EQ:
                queries.append({"term": {field: value}})
            elif node.op == ComparisonOp.NEQ:
                queries.append({"bool": {"must_not": [{"term": {field: value}}]}})
            elif node.op in [
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
                }[node.op]
                queries.append({"range": {field: {range_op: value}}})
            elif node.op == ComparisonOp.IS_NULL:
                queries.append({"bool": {"must_not": {"exists": {"field": field}}}})

        return queries[0] if len(queries) == 1 else {"bool": {"should": queries}}

    elif isinstance(node, AdvancedComparisonNode):
        fields = to_es_field(queryables_mapping, node.field)

        if node.op == AdvancedComparisonOp.BETWEEN:
            if isinstance(node.value, (list, tuple)) and len(node.value) == 2:
                gte, lte = node.value[0], node.value[1]
                if isinstance(gte, dict) and "timestamp" in gte:
                    gte = gte["timestamp"]
                if isinstance(lte, dict) and "timestamp" in lte:
                    lte = lte["timestamp"]
                queries = [
                    {"range": {field: {"gte": gte, "lte": lte}}} for field in fields
                ]
                return (
                    queries[0] if len(queries) == 1 else {"bool": {"should": queries}}
                )

        elif node.op == AdvancedComparisonOp.IN:
            if not isinstance(node.value, list):
                raise ValueError(f"IN operator expects list, got {type(node.value)}")
            queries = [{"terms": {field: node.value}} for field in fields]
            return queries[0] if len(queries) == 1 else {"bool": {"should": queries}}

        elif node.op == AdvancedComparisonOp.LIKE:
            pattern = str(node.value)

            es_pattern = ""
            i = 0
            while i < len(pattern):
                if pattern[i] == "\\" and i + 1 < len(pattern):
                    i += 1
                    if pattern[i] == "%":
                        es_pattern += "%"
                    elif pattern[i] == "_":
                        es_pattern += "_"
                    elif pattern[i] == "\\":
                        es_pattern += "\\"
                    else:
                        es_pattern += "\\" + pattern[i]
                elif pattern[i] == "%":
                    es_pattern += "*"
                elif pattern[i] == "_":
                    es_pattern += "?"
                else:
                    es_pattern += pattern[i]
                i += 1

            queries = [
                {"wildcard": {field: {"value": es_pattern, "case_insensitive": True}}}
                for field in fields
            ]
            return queries[0] if len(queries) == 1 else {"bool": {"should": queries}}

    elif isinstance(node, SpatialNode):
        fields = to_es_field(queryables_mapping, node.field)

        relation_mapping = {
            SpatialOp.S_INTERSECTS: "intersects",
            SpatialOp.S_CONTAINS: "contains",
            SpatialOp.S_WITHIN: "within",
            SpatialOp.S_DISJOINT: "disjoint",
        }

        relation = relation_mapping[node.op]
        queries = [
            {"geo_shape": {field: {"shape": node.geometry, "relation": relation}}}
            for field in fields
        ]
        return queries[0] if len(queries) == 1 else {"bool": {"should": queries}}

    raise ValueError("Unsupported AST node")
