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

from .transform import to_es_field


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


def _convert_cql2_geometry_to_geojson(geometry):
    """Convert CQL2 geometry format to GeoJSON format."""
    import logging

    logger = logging.getLogger(__name__)

    if isinstance(geometry, dict) and "type" in geometry and "coordinates" in geometry:
        logger.debug(f"Geometry already in GeoJSON format: {geometry}")
        return geometry

    if isinstance(geometry, dict) and "op" in geometry:
        op = geometry["op"]
        args = geometry.get("args", [])
        logger.debug(f"Converting CQL2 geometry: op={op}, args={args}")

        if op == "polygon":
            if not args:
                logger.warning("Polygon geometry has no args")
                return geometry

            coordinates = args[0] if args else []

            if not isinstance(coordinates, list):
                logger.warning(
                    f"Polygon coordinates is not a list: {type(coordinates)}"
                )
                return geometry

            if (
                len(coordinates) > 0
                and isinstance(coordinates[0], list)
                and len(coordinates[0]) > 0
                and isinstance(coordinates[0][0], list)
            ):
                logger.debug("Polygon already in correct format (list of rings)")
                pass

            elif (
                len(coordinates) > 0
                and isinstance(coordinates[0], list)
                and len(coordinates[0]) > 0
                and not isinstance(coordinates[0][0], list)
            ):
                logger.debug("Wrapping single ring in outer array")
                coordinates = [coordinates]

            elif len(coordinates) > 0 and not isinstance(coordinates[0], list):
                logger.warning(f"Unexpected polygon coordinate format: {coordinates}")

                if len(coordinates) >= 6 and len(coordinates) % 2 == 0:
                    ring = []
                    for i in range(0, len(coordinates), 2):
                        ring.append([coordinates[i], coordinates[i + 1]])
                    coordinates = [ring]

            result = {"type": "Polygon", "coordinates": coordinates}
            logger.debug(f"Converted polygon to: {result}")
            return result

        elif op == "point":
            if len(args) >= 2:
                result = {"type": "Point", "coordinates": [args[0], args[1]]}
                logger.debug(f"Converted point to: {result}")
                return result

        elif op == "linestring":
            if args:
                coordinates = args[0] if args else []
                result = {"type": "LineString", "coordinates": coordinates}
                logger.debug(f"Converted linestring to: {result}")
                return result

    logger.warning(f"Unable to convert geometry to GeoJSON: {geometry}")
    return geometry


def _transform_ast_node(
    queryables_mapping: Dict[str, Any], node: Any
) -> Dict[str, Any]:
    """Transform AST node to Elasticsearch/Opensearch query."""
    if isinstance(node, LogicalNode):
        if node.op == LogicalOp.AND:
            must_clauses = []
            must_not_clauses = []

            for child in node.children:
                child_query = _transform_ast_node(queryables_mapping, child)

                if isinstance(child_query, dict) and "bool" in child_query:
                    bool_query = child_query["bool"]

                    if len(bool_query) == 1 and "must_not" in bool_query:
                        must_not_clauses.append(bool_query["must_not"])
                    else:
                        must_clauses.append(child_query)
                else:
                    must_clauses.append(child_query)

            bool_query = {}
            if must_clauses:
                bool_query["must"] = (
                    must_clauses if len(must_clauses) > 1 else must_clauses[0]
                )
            if must_not_clauses:
                bool_query["must_not"] = (
                    must_not_clauses
                    if len(must_not_clauses) > 1
                    else must_not_clauses[0]
                )

            return {"bool": bool_query}

        elif node.op == LogicalOp.OR:
            return {
                "bool": {
                    "should": [
                        _transform_ast_node(queryables_mapping, child)
                        for child in node.children
                    ]
                }
            }
        elif node.op == LogicalOp.NOT:
            child_query = _transform_ast_node(queryables_mapping, node.children[0])
            return {"bool": {"must_not": child_query}}

    elif isinstance(node, ComparisonNode):
        fields = to_es_field(queryables_mapping, node.field)
        value = node.value

        if isinstance(value, dict) and "timestamp" in value:
            value = value["timestamp"]

        queries = []
        for field in fields:
            if node.op == ComparisonOp.EQ:
                queries.append({"term": {field: value}})
            elif node.op == ComparisonOp.NEQ:
                queries.append({"term": {field: value}})
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

        if node.op == ComparisonOp.NEQ:
            if len(queries) == 1:
                return {"bool": {"must_not": queries[0]}}
            else:
                return {"bool": {"must_not": {"bool": {"should": queries}}}}
        else:
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
        geometry = _convert_cql2_geometry_to_geojson(node.geometry)
        queries = [
            {"geo_shape": {field: {"shape": geometry, "relation": relation}}}
            for field in fields
        ]
        return queries[0] if len(queries) == 1 else {"bool": {"should": queries}}

    raise ValueError("Unsupported AST node")
