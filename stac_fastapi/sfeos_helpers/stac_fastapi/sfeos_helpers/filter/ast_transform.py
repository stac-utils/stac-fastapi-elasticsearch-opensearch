"""AST-based query transformation for Elasticsearch/OpenSearch."""

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


def to_es_via_ast(
    queryables_mapping: Dict[str, Any], query: Union[Dict[str, Any], CqlNode]
) -> Dict[str, Any]:
    """Transform CQL2 query to Elasticsearch/Opensearch query via AST."""
    from .ast_parser import Cql2AstParser

    if isinstance(query, CqlNode):
        ast = query
    else:
        parser = Cql2AstParser(queryables_mapping)
        ast = parser.parse(query)

    result = _transform_ast_node(ast, queryables_mapping)
    return result


def _transform_ast_node(
    node: Any, queryables_mapping: Dict[str, Any]
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
                    bool_type: _transform_ast_node(node.children[0], queryables_mapping)
                }
            }
        else:
            return {
                "bool": {
                    bool_type: [
                        _transform_ast_node(child, queryables_mapping)
                        for child in node.children
                    ]
                }
            }

    elif isinstance(node, ComparisonNode):
        field = _to_es_field(queryables_mapping, node.field)
        value = node.value

        if isinstance(value, dict) and "timestamp" in value:
            value = value["timestamp"]

        if node.op == ComparisonOp.EQ:
            return {"term": {field: value}}
        elif node.op == ComparisonOp.NEQ:
            return {"bool": {"must_not": [{"term": {field: value}}]}}
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
            return {"range": {field: {range_op: value}}}
        elif node.op == ComparisonOp.IS_NULL:
            return {"bool": {"must_not": {"exists": {"field": field}}}}

    elif isinstance(node, AdvancedComparisonNode):
        field = _to_es_field(queryables_mapping, node.field)

        if node.op == AdvancedComparisonOp.BETWEEN:
            if isinstance(node.value, (list, tuple)) and len(node.value) == 2:
                gte, lte = node.value[0], node.value[1]
                if isinstance(gte, dict) and "timestamp" in gte:
                    gte = gte["timestamp"]
                if isinstance(lte, dict) and "timestamp" in lte:
                    lte = lte["timestamp"]
                return {"range": {field: {"gte": gte, "lte": lte}}}

        elif node.op == AdvancedComparisonOp.IN:
            if not isinstance(node.value, list):
                raise ValueError(f"IN operator expects list, got {type(node.value)}")
            return {"terms": {field: node.value}}

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

            return {
                "wildcard": {field: {"value": es_pattern, "case_insensitive": True}}
            }

    elif isinstance(node, SpatialNode):
        field = _to_es_field(queryables_mapping, node.field)

        relation_mapping = {
            SpatialOp.S_INTERSECTS: "intersects",
            SpatialOp.S_CONTAINS: "contains",
            SpatialOp.S_WITHIN: "within",
            SpatialOp.S_DISJOINT: "disjoint",
        }

        relation = relation_mapping[node.op]
        return {"geo_shape": {field: {"shape": node.geometry, "relation": relation}}}

    raise ValueError("Unsupported AST node")


def _to_es_field(queryables_mapping: Dict[str, Any], field: str) -> str:
    """Map field name using queryables mapping."""
    return queryables_mapping.get(field, field)
