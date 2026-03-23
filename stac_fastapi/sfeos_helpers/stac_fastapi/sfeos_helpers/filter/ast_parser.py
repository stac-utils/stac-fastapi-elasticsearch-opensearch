"""AST parser for CQL2 queries."""

import json
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


class Cql2AstParser:
    """Parse CQL2 into AST tree."""

    def __init__(self):
        """Initialize the CQL2 AST parser."""
        pass

    def parse(self, cql: Union[str, Dict[str, Any]]) -> CqlNode:
        """Parse CQL2 into AST tree.

        Args:
            cql: CQL2 expression as string/dictionary

        Returns:
            Node of AST tree
        """
        if isinstance(cql, str):
            data: Dict[str, Any] = json.loads(cql)
            return self._parse_node(data)

        return self._parse_node(cql)

    def _parse_node(self, node: Dict[str, Any]) -> CqlNode:
        """Parse a single CQL2 node into AST."""
        if "op" in node and node["op"] in ["and", "or", "not"]:
            op = LogicalOp(node["op"])
            args = node.get("args", [])

            if op == LogicalOp.NOT:
                children = [self._parse_node(args[0])] if args else []
            else:
                children = [self._parse_node(arg) for arg in args]

            return LogicalNode(op=op, children=children)

        elif "op" in node and node["op"] in ["=", "<>", "<", "<=", ">", ">=", "isNull"]:
            op = ComparisonOp(node["op"])
            args = node.get("args", [])

            if isinstance(args[0], dict) and "property" in args[0]:
                field = args[0]["property"]
            else:
                field = str(args[0])

            value = args[1] if len(args) > 1 else None

            return ComparisonNode(op=op, field=field, value=value)

        elif "op" in node and node["op"] in ["like", "between", "in"]:
            op = AdvancedComparisonOp(node["op"])
            args = node.get("args", [])

            if isinstance(args[0], dict) and "property" in args[0]:
                field = args[0]["property"]
            else:
                field = str(args[0])

            if op == AdvancedComparisonOp.BETWEEN:
                # Format: [{'property': '...'}, [lower, upper]]
                if len(args) == 2 and isinstance(args[1], list) and len(args[1]) == 2:
                    value = (args[1][0], args[1][1])
                # Format: [{'property': '...'}, lower, upper]
                elif len(args) == 3:
                    value = (args[1], args[2])
                else:
                    raise ValueError(
                        f"BETWEEN operator supports either [property, [lower, upper]] or [property, lower, upper], "
                        f"got format with {len(args)} args: {args}"
                    )

            elif op == AdvancedComparisonOp.IN:
                if len(args) != 2:
                    raise ValueError(
                        f"IN operator expects [property, values_list], got {len(args)} args"
                    )
                if not isinstance(args[1], list):
                    raise ValueError(f"IN operator expects list, got {type(args[1])}")
                value = args[1]

            elif op == AdvancedComparisonOp.LIKE:
                if len(args) != 2:
                    raise ValueError(
                        f"LIKE operator expects [property, pattern], got {len(args)} args"
                    )
                value = args[1]
            return AdvancedComparisonNode(op=op, field=field, value=value)

        elif "op" in node and node["op"] in [
            "s_intersects",
            "s_contains",
            "s_within",
            "s_disjoint",
        ]:
            op = SpatialOp(node["op"])
            args = node.get("args", [])

            if isinstance(args[0], dict) and "property" in args[0]:
                field = args[0]["property"]
            else:
                field = str(args[0])

            geometry = args[1]

            return SpatialNode(op=op, field=field, geometry=geometry)
