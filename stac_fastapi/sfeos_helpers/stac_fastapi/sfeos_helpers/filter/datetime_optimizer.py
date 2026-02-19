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
    """Extract datetime from CQL2 AST."""

    def __init__(self) -> None:
        """Initialize the datetime optimizer."""
        self.datetime_nodes: List[Union[DateTimeRangeNode, DateTimeExactNode]] = []

    def extract_datetime_nodes(
        self, node: CqlNode
    ) -> List[Union[DateTimeRangeNode, DateTimeExactNode]]:
        """Extract datetime nodes from AST."""
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
        """Datetime field checker."""
        field_lower = field.lower()
        return any(
            dt_field in field_lower
            for dt_field in ["datetime", "start_datetime", "end_datetime"]
        )

    def optimize_query_structure(self, ast: CqlNode) -> CqlNode:
        """Optimize AST structure for better query performance."""
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

    def recurse(n: CqlNode):
        """Recursively traverse AST nodes to extract field values."""
        if isinstance(n, LogicalNode):
            for child in n.children:
                recurse(child)
            if n.op == LogicalOp.AND:
                datetime_nodes: List[ComparisonNode] = []
                for child in n.children:
                    if isinstance(child, ComparisonNode) and child.field == "datetime":
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
        elif isinstance(n, ComparisonNode):
            if n.field == field_name:
                if isinstance(n.value, list):
                    values.extend(n.value)
                else:
                    values.append(n.value)

        elif isinstance(n, AdvancedComparisonNode):
            if n.field == field_name:
                if isinstance(n.value, list):
                    values.extend(n.value)
                else:
                    values.append(n.value)

    recurse(node)
    return values if values else None

def extract_collection_datetime(node: CqlNode) -> List[Tuple[List[str], str]]:
    """Extract collections, datetime range from CQL AST.

    Returns:
        List of tuples where each tuple contains:
        - collections: List[str] collection id(s)
        - datetime_range: str or empty string if no datetime constraint
    """
    pairs = []
    processed_nodes = set()

    def extract_datetime_constraints(child: CqlNode) -> List[Tuple]:
        """Extract datetime constraints from a node.
        Returns list of (start, end, is_exclude, op_type, comp_type) tuples."""
        constraints = []
        
        if isinstance(child, ComparisonNode):
            if child.field in ["datetime", "start_datetime", "end_datetime"]:
                if child.op == ComparisonOp.EQ:
                    constraints.append((child.value, child.value, False, 'eq', None))
                elif child.op == ComparisonOp.GT:
                    constraints.append((child.value, None, False, 'range', 'gt'))
                elif child.op == ComparisonOp.GTE:
                    constraints.append((child.value, None, False, 'range', 'gte'))
                elif child.op == ComparisonOp.LT:
                    constraints.append((None, child.value, False, 'range', 'lt'))
                elif child.op == ComparisonOp.LTE:
                    constraints.append((None, child.value, False, 'range', 'lte'))
                elif child.op == ComparisonOp.NEQ:
                    constraints.append((None, child.value, True, 'neq', None))
                    constraints.append((child.value, None, True, 'neq', None))
                    
        elif isinstance(child, AdvancedComparisonNode):
            if child.field in ["datetime", "start_datetime", "end_datetime"]:
                if child.op == AdvancedComparisonOp.BETWEEN:
                    if isinstance(child.value, (list, tuple)) and len(child.value) == 2:
                        constraints.append((child.value[0], child.value[1], False, 'range', None))
        
        elif isinstance(child, LogicalNode):
            if child.op == LogicalOp.OR:
                or_constraints = []
                for or_child in child.children:
                    branch_constraints = extract_datetime_constraints(or_child)
                    or_constraints.extend(branch_constraints)
                if or_constraints:
                    constraints.append((None, None, False, 'or', None, or_constraints))
            
            elif child.op == LogicalOp.AND:
                and_constraints = []
                for and_child in child.children:
                    branch_constraints = extract_datetime_constraints(and_child)
                    and_constraints.extend(branch_constraints)
                if and_constraints:
                    constraints.append((None, None, False, 'and', None, and_constraints))
            
            elif child.op == LogicalOp.NOT:
                for inner_child in child.children:
                    inner_constraints = extract_datetime_constraints(inner_child)
                    for constraint in inner_constraints:
                        if len(constraint) > 5 and constraint[3] == 'and':
                            and_constraints = constraint[5]
                            earliest_start = None
                            latest_end = None
                            for and_cons in and_constraints:
                                start, end, is_exclude, op_type, comp_type = and_cons[:5]
                                if op_type == 'range':
                                    if comp_type in ['gt', 'gte'] and start:
                                        if earliest_start is None or start < earliest_start:
                                            earliest_start = start
                                    elif comp_type in ['lt', 'lte'] and end:
                                        if latest_end is None or end > latest_end:
                                            latest_end = end
                            
                            or_branches = []
                            if earliest_start:
                                or_branches.append((None, earliest_start, False, 'range', 'lt'))
                            if latest_end:
                                or_branches.append((latest_end, None, False, 'range', 'gt'))
                            if or_branches:
                                constraints.append((None, None, False, 'or', None, or_branches))
                        
                        elif len(constraint) > 5 and constraint[3] == 'or':
                            or_constraints = constraint[5]
                            and_branches = []
                            for or_cons in or_constraints:
                                start, end, is_exclude, op_type, comp_type = or_cons[:5]
                                if op_type == 'eq':
                                    and_branches.append((None, start, True, 'neq', None))
                                    and_branches.append((start, None, True, 'neq', None))
                                elif op_type == 'range':
                                    if comp_type in ['gt', 'gte']:
                                        and_branches.append((None, start, False, 'range', 'lte'))
                                    elif comp_type in ['lt', 'lte']:
                                        and_branches.append((start, None, False, 'range', 'gte'))
                            if and_branches:
                                constraints.append((None, None, False, 'and', None, and_branches))
                        else:
                            start, end, is_exclude, op_type, comp_type = constraint[:5]
                            if op_type == 'eq':
                                or_branches = [
                                    (None, start, False, 'range', 'lt'),
                                    (start, None, False, 'range', 'gt')
                                ]
                                constraints.append((None, None, False, 'or', None, or_branches))
                            elif op_type == 'range':
                                if comp_type in ['gt', 'gte']:
                                    constraints.append((None, start, False, 'range', 'lte'))
                                elif comp_type in ['lt', 'lte']:
                                    constraints.append((start, None, False, 'range', 'gte'))
        
        return constraints

    def collect_from_node(n: CqlNode) -> List[Tuple[List[str], List]]:
        """Recursively collect collections and datetime constraints from node.
        Returns a list of (collections, constraints_list) tuples."""
        results = []
        
        if isinstance(n, LogicalNode) and n.op == LogicalOp.AND:
            current_collections = []
            all_constraints = []
            
            for child in n.children:
                if isinstance(child, (ComparisonNode, AdvancedComparisonNode)):
                    if child.field == "collection":
                        if isinstance(child.value, list):
                            current_collections.extend(child.value)
                        else:
                            current_collections.append(child.value)
                    else:
                        constraints = extract_datetime_constraints(child)
                        all_constraints.extend(constraints)
                
                elif isinstance(child, LogicalNode):
                    constraints = extract_datetime_constraints(child)
                    all_constraints.extend(constraints)
                    
                    nested_results = collect_from_node(child)
                    for nested_collections, nested_constraints in nested_results:
                        if nested_collections:
                            current_collections.extend(nested_collections)
                        if nested_constraints:
                            all_constraints.extend(nested_constraints)
            
            if current_collections:
                if all_constraints:
                    complex_constraints = [c for c in all_constraints if len(c) > 5 and c[3] in ['or', 'and']]
                    other_constraints = [c for c in all_constraints if not (len(c) > 5 and c[3] in ['or', 'and'])]
                    
                    if complex_constraints:
                        for complex_item in complex_constraints:
                            if complex_item[3] == 'or':
                                or_branches = complex_item[5]
                                for branch in or_branches:
                                    branch_list = [branch]
                                    if other_constraints:
                                        branch_list.extend(other_constraints)
                                    results.append((current_collections.copy(), branch_list))
                            elif complex_item[3] == 'and':
                                and_branches = complex_item[5]
                                combined = list(and_branches)
                                if other_constraints:
                                    combined.extend(other_constraints)
                                results.append((current_collections.copy(), combined))
                    else:
                        results.append((current_collections.copy(), all_constraints))
                else:
                    results.append((current_collections, []))
        
        return results

    def should_process_node(n: CqlNode) -> bool:
        """Determine if this node should be processed based on its parent context."""
        if id(n) in processed_nodes:
            return False
        processed_nodes.add(id(n))
        return True

    def recurse(n: CqlNode, parent_is_and: bool = False):
        if isinstance(n, LogicalNode):
            if n.op == LogicalOp.AND and not parent_is_and and should_process_node(n):
                results = collect_from_node(n)
                
                for collections, constraints in results:
                    if not collections:
                        continue
                        
                    if constraints:
                        for constraint in constraints:
                            if len(constraint) > 5 and constraint[3] in ['or', 'and']:
                                continue
                            
                            start, end, is_exclude, op_type, comp_type = constraint[:5]
                            
                            if is_exclude or op_type == 'neq':
                                if start is None and end is not None:
                                    date_range = f"../{end}"
                                    pairs.append((collections, date_range))
                                elif start is not None and end is None:
                                    date_range = f"{start}/.."
                                    pairs.append((collections, date_range))
                                elif start is not None and end is not None and start == end:
                                    pairs.append((collections, f"../{start}"))
                                    pairs.append((collections, f"{start}/.."))
                            else:
                                if start or end:
                                    if start and end:
                                        if start == end and op_type == 'eq':
                                            date_range = start
                                        else:
                                            if comp_type in ['gt', 'lt', 'gte', 'lte']:
                                                if comp_type in ['gt', 'gte']:
                                                    date_range = f"{start}/.."
                                                elif comp_type in ['lt', 'lte']:
                                                    date_range = f"../{end}"
                                                else:
                                                    date_range = f"{start}/{end}"
                                            else:
                                                date_range = f"{start}/{end}"
                                    elif start:
                                        date_range = f"{start}/.."
                                    elif end:
                                        date_range = f"../{end}"
                                    else:
                                        date_range = ""
                                    pairs.append((collections, date_range))
                    else:
                        pairs.append((collections, ""))
            
            for child in n.children:
                recurse(child, parent_is_and=(n.op == LogicalOp.AND))

    recurse(node)
    
    unique_pairs = []
    seen = set()
    for collections, date_range in pairs:
        if not collections:
            continue
        key = (tuple(sorted(collections)), date_range)
        if key not in seen:
            seen.add(key)
            unique_pairs.append((collections, date_range))
    
    return unique_pairs
