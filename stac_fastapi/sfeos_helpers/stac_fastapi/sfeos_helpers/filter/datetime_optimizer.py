"""Extracts datetime patterns from CQL2 AST."""

from typing import Any, List, Tuple, Union

from stac_fastapi.core.extensions.filter import (
    AdvancedComparisonNode,
    AdvancedComparisonOp,
    ComparisonNode,
    ComparisonOp,
    CqlNode,
    DateTimeExactNode,
    DateTimeRangeNode,
    LogicalNode,
    LogicalOp,
)


class DatetimeOptimizer:
    """Extract datetime nodes from CQL2 AST."""

    def __init__(self) -> None:
        """Initialize the datetime optimizer."""
        self.datetime_nodes: List[Union[DateTimeRangeNode, DateTimeExactNode]] = []

    def extract_datetime_nodes(
        self, node: CqlNode
    ) -> List[Union[DateTimeRangeNode, DateTimeExactNode]]:
        """Extract all datetime nodes from AST."""
        datetime_nodes = []

        def _traverse(current: CqlNode):
            if isinstance(current, ComparisonNode):
                if self._is_datetime_field(current.field):
                    if current.op == ComparisonOp.EQ:
                        datetime_nodes.append(
                            DateTimeExactNode(field=current.field, value=current.value)
                        )
                    elif current.op == ComparisonOp.GTE:
                        datetime_nodes.append(
                            DateTimeRangeNode(field=current.field, start=current.value)
                        )
                    elif current.op == ComparisonOp.LTE:
                        datetime_nodes.append(
                            DateTimeRangeNode(field=current.field, end=current.value)
                        )

            elif isinstance(current, AdvancedComparisonNode):
                if (
                    current.op == AdvancedComparisonOp.BETWEEN
                    and self._is_datetime_field(current.field)
                ):
                    if (
                        isinstance(current.value, (list, tuple))
                        and len(current.value) == 2
                    ):
                        datetime_nodes.append(
                            DateTimeRangeNode(
                                field=current.field,
                                start=current.value[0],
                                end=current.value[1],
                            )
                        )

            if isinstance(current, LogicalNode):
                for child in current.children:
                    _traverse(child)

        _traverse(node)
        return datetime_nodes

    def _is_datetime_field(self, field: str) -> bool:
        """Check if a field is a datetime field."""
        field_lower = field.lower()
        return any(
            dt_field in field_lower
            for dt_field in ["datetime", "start_datetime", "end_datetime"]
        )

    def optimize_query_structure(self, ast: CqlNode) -> CqlNode:
        """Optimize AST structure for better query performance.

        Reorders AND clauses to put datetime filters first for better execution.
        """
        return self._reorder_for_datetime_priority(ast)

    def _reorder_for_datetime_priority(self, node: CqlNode) -> CqlNode:
        """Reorder query tree to prioritize datetime filters."""
        if isinstance(node, LogicalNode):
            if node.op == LogicalOp.AND:
                datetime_children = []
                other_children = []

                for child in node.children:
                    processed_child = self._reorder_for_datetime_priority(child)
                    if self._contains_datetime(processed_child):
                        datetime_children.append(processed_child)
                    else:
                        other_children.append(processed_child)

                reordered_children = datetime_children + other_children

                if len(reordered_children) == 1:
                    return reordered_children[0]

                return LogicalNode(op=LogicalOp.AND, children=reordered_children)

            elif node.op in [LogicalOp.OR, LogicalOp.NOT]:
                processed_children = [
                    self._reorder_for_datetime_priority(child)
                    for child in node.children
                ]
                return LogicalNode(op=node.op, children=processed_children)

        return node

    def _contains_datetime(self, node: CqlNode) -> bool:
        """Check if node contains datetime filter."""
        if isinstance(node, (ComparisonNode, AdvancedComparisonNode)):
            return self._is_datetime_field(node.field)

        elif isinstance(node, LogicalNode):
            return any(self._contains_datetime(child) for child in node.children)

        return False


def extract_from_ast(node: CqlNode, field_name: str) -> List[Any]:
    """Extract all values for a specific field from a CQL AST."""
    values = []

    def recurse(n):
        """Recursively traverse AST nodes to extract field values."""
        if hasattr(n, "children"):
            for child in n.children:
                recurse(child)

        if hasattr(n, "field") and hasattr(n, "value"):
            if n.field == field_name:
                if isinstance(n.value, list):
                    values.extend(n.value)
                else:
                    values.append(n.value)
        # Handle datetime range
        elif hasattr(n, "op") and n.op == LogicalOp.AND and hasattr(n, "children"):
            datetime_nodes = []
            for child in n.children:
                if (
                    hasattr(child, "field")
                    and child.field == "datetime"
                    and hasattr(child, "op")
                    and hasattr(child, "value")
                ):
                    datetime_nodes.append(child)

            if len(datetime_nodes) == 2:
                gte_node = None
                lte_node = None
                for d_node in datetime_nodes:
                    if d_node.op == ComparisonOp.GTE:
                        gte_node = d_node
                    elif d_node.op == ComparisonOp.LTE:
                        lte_node = d_node

                if gte_node and lte_node:
                    values.append(
                        {
                            "type": "range",
                            "start": gte_node.value,
                            "end": lte_node.value,
                        }
                    )

    recurse(node)
    return values if values else None


def extract_collection_datetime(node: CqlNode) -> List[Tuple[List[str], str]]:
    """Extract (collections, datetime_range) from CQL AST.

    Returns:
        List of tuples where each tuple contains:
        - collections: List[str] collection id(s)
        - datetime_range: str or empty string if no datetime constraint
    """
    pairs = []

    def recurse(n):
        # Handle AND node
        if hasattr(n, "op") and hasattr(n.op, "value") and n.op.value == "and":
            collections = []
            gte_date = None
            lte_date = None

            if hasattr(n, "children"):
                for child in n.children:
                    if (
                        hasattr(child, "op")
                        and hasattr(child, "field")
                        and hasattr(child, "value")
                    ):
                        if child.field == "collection":
                            if isinstance(child.value, list):
                                collections.extend(child.value)
                            else:
                                collections.append(child.value)
                        elif child.field in [
                            "datetime",
                            "start_datetime",
                            "end_datetime",
                        ]:
                            if hasattr(child.op, "value"):
                                if child.op.value == ">=":
                                    gte_date = child.value
                                elif child.op.value == "<=":
                                    lte_date = child.value
            if gte_date or lte_date:
                if gte_date and lte_date:
                    date_range = f"{gte_date}/{lte_date}"
                elif gte_date:
                    date_range = f"{gte_date}/.."
                elif lte_date:
                    date_range = f"../{lte_date}"
                else:
                    date_range = ""
                pairs.append((collections, date_range))
            elif collections:
                pairs.append((collections, ""))
        if hasattr(n, "children"):
            for child in n.children:
                recurse(child)

    recurse(node)
    return pairs
