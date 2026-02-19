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
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info(f"Extracting collection datetime from AST: {node}")
    print(f"\n=== EXTRACTING COLLECTION DATETIME FROM AST ===")
    print(f"AST Node type: {type(node).__name__}")
    
    pairs = []
    processed_nodes = set()

    def extract_datetime_constraints(child: CqlNode) -> List[Tuple]:
        """Extract datetime constraints from a node."""
        constraints = []
        node_type = type(child).__name__
        print(f"  Processing node: {node_type}")
        
        if isinstance(child, ComparisonNode):
            print(f"    ComparisonNode: field={child.field}, op={child.op}, value={child.value}")
            if child.field in ["datetime", "start_datetime", "end_datetime"]:
                if child.op == ComparisonOp.EQ:
                    constraints.append((child.value, child.value, False, 'eq', None))
                    print(f"      Added EQ constraint: {child.value}")
                elif child.op == ComparisonOp.GT:
                    constraints.append((child.value, None, False, 'range', 'gt'))
                    print(f"      Added GT constraint: {child.value}")
                elif child.op == ComparisonOp.GTE:
                    constraints.append((child.value, None, False, 'range', 'gte'))
                    print(f"      Added GTE constraint: {child.value}")
                elif child.op == ComparisonOp.LT:
                    constraints.append((None, child.value, False, 'range', 'lt'))
                    print(f"      Added LT constraint: {child.value}")
                elif child.op == ComparisonOp.LTE:
                    constraints.append((None, child.value, False, 'range', 'lte'))
                    print(f"      Added LTE constraint: {child.value}")
                elif child.op == ComparisonOp.NEQ:
                    constraints.append((None, child.value, True, 'neq', None))
                    constraints.append((child.value, None, True, 'neq', None))
                    print(f"      Added NEQ constraints: before and after {child.value}")
        
        elif isinstance(child, LogicalNode):
            print(f"    LogicalNode: op={child.op}")
            if child.op == LogicalOp.OR:
                print(f"      Processing OR with {len(child.children)} children")
                or_constraints = []
                for i, or_child in enumerate(child.children):
                    print(f"        OR branch {i+1}:")
                    branch_constraints = extract_datetime_constraints(or_child)
                    if branch_constraints:
                        or_constraints.extend(branch_constraints)
                if or_constraints:
                    constraints.append((None, None, False, 'or', None, or_constraints))
                    print(f"      Added OR with {len(or_constraints)} total constraints")
            
            elif child.op == LogicalOp.AND:
                print(f"      Processing AND with {len(child.children)} children")
                and_constraints = []
                for i, and_child in enumerate(child.children):
                    print(f"        AND child {i+1}:")
                    branch_constraints = extract_datetime_constraints(and_child)
                    if branch_constraints:
                        and_constraints.extend(branch_constraints)
                if and_constraints:
                    if len(and_constraints) == 1:
                        constraints.append(and_constraints[0])
                        print(f"      Added single AND constraint")
                    else:
                        constraints.append((None, None, False, 'and', None, and_constraints))
                        print(f"      Added AND with {len(and_constraints)} constraints")
        
        return constraints

    def expand_constraints(constraints_list: List) -> List[List]:
        """Expand complex constraints into lists of simple constraints."""
        expanded = []
        
        for constraint in constraints_list:
            if len(constraint) > 5 and constraint[3] in ['or', 'and']:
                if constraint[3] == 'or':
                    # For OR, each branch becomes a separate result
                    branches = constraint[5]
                    for branch in branches:
                        branch_expanded = expand_constraints([branch])
                        expanded.extend(branch_expanded)
                elif constraint[3] == 'and':
                    # For AND, we need to combine all branches into a single constraint set
                    branches = constraint[5]
                    
                    # Collect all simple constraints from this AND
                    all_simple = []
                    for branch in branches:
                        if len(branch) > 5 and branch[3] in ['or', 'and']:
                            # Recursively expand nested complex constraints
                            branch_expanded = expand_constraints([branch])
                            for be in branch_expanded:
                                all_simple.extend(be)
                        else:
                            # Simple constraint
                            all_simple.append(branch)
                    
                    # Now combine start and end constraints from this AND
                    start_vals = []
                    end_vals = []
                    other_constraints = []
                    is_exclude = False
                    
                    for c in all_simple:
                        start, end, ie, op_type, comp_type = c[:5]
                        is_exclude = is_exclude or ie
                        if op_type == 'range':
                            if start is not None and end is None:
                                start_vals.append((start, comp_type))
                            elif start is None and end is not None:
                                end_vals.append((end, comp_type))
                            else:
                                other_constraints.append(c)
                        else:
                            other_constraints.append(c)
                    
                    # Take the most restrictive values
                    combined = []
                    if start_vals:
                        # For start, take the maximum value (most restrictive)
                        max_start = max(start_vals, key=lambda x: x[0])
                        combined.append((max_start[0], None, is_exclude, 'range', max_start[1]))
                    
                    if end_vals:
                        # For end, take the minimum value (most restrictive)
                        min_end = min(end_vals, key=lambda x: x[0])
                        combined.append((None, min_end[0], is_exclude, 'range', min_end[1]))
                    
                    combined.extend(other_constraints)
                    
                    # For AND, all constraints go into ONE result set
                    if combined:
                        # If we have both start and end in the same set, they should be combined later
                        expanded.append(combined)
            else:
                # Simple constraint
                expanded.append([constraint])
        
        return expanded

    def collect_from_node(n: CqlNode) -> List[Tuple[List[str], List]]:
        """Recursively collect collections and datetime constraints from node."""
        results = []
        
        if isinstance(n, LogicalNode) and n.op == LogicalOp.AND:
            print(f"\n  Collecting from AND node:")
            current_collections = []
            all_constraints = []
            
            for child in n.children:
                if isinstance(child, (ComparisonNode, AdvancedComparisonNode)):
                    if child.field == "collection":
                        if isinstance(child.value, list):
                            current_collections.extend(child.value)
                        else:
                            current_collections.append(child.value)
                        print(f"    Found collection: {child.value}")
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
                print(f"    Current collections: {current_collections}")
                print(f"    All constraints count: {len(all_constraints)}")
                
                if all_constraints:
                    # Expand all constraints
                    expanded_constraints = expand_constraints(all_constraints)
                    print(f"    Expanded into {len(expanded_constraints)} constraint sets")
                    
                    for constraint_set in expanded_constraints:
                        results.append((current_collections.copy(), constraint_set))
                else:
                    print(f"    No datetime constraints, just collections")
                    results.append((current_collections, []))
        
        return results

    def should_process_node(n: CqlNode) -> bool:
        if id(n) in processed_nodes:
            return False
        processed_nodes.add(id(n))
        return True

    def recurse(n: CqlNode, parent_is_and: bool = False):
        if isinstance(n, LogicalNode):
            if n.op == LogicalOp.AND and not parent_is_and and should_process_node(n):
                print(f"\n=== PROCESSING TOP-LEVEL AND NODE ===")
                results = collect_from_node(n)
                
                print(f"Results from collect_from_node: {len(results)}")
                for i, (collections, constraints) in enumerate(results):
                    print(f"  Result {i+1}: collections={collections}, constraints={len(constraints)}")
                    
                    if not collections:
                        continue
                        
                    if constraints:
                        for j, constraint in enumerate(constraints):
                            start, end, is_exclude, op_type, comp_type = constraint[:5]
                            print(f"    Constraint {j+1}: start={start}, end={end}, is_exclude={is_exclude}, op_type={op_type}")
                            
                            if is_exclude or op_type == 'neq':
                                if start is None and end is not None:
                                    date_range = f"../{end}"
                                    print(f"      Adding exclude range before: {date_range}")
                                    pairs.append((collections, date_range))
                                elif start is not None and end is None:
                                    date_range = f"{start}/.."
                                    print(f"      Adding exclude range after: {date_range}")
                                    pairs.append((collections, date_range))
                                elif start is not None and end is not None and start == end:
                                    print(f"      Adding both exclude ranges for {start}")
                                    pairs.append((collections, f"../{start}"))
                                    pairs.append((collections, f"{start}/.."))
                            else:
                                if start or end:
                                    if start and end:
                                        if start == end and op_type == 'eq':
                                            date_range = start
                                            print(f"      Adding exact date: {date_range}")
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
                                            print(f"      Adding range: {date_range}")
                                    elif start:
                                        date_range = f"{start}/.."
                                        print(f"      Adding start range: {date_range}")
                                    elif end:
                                        date_range = f"../{end}"
                                        print(f"      Adding end range: {date_range}")
                                    else:
                                        date_range = ""
                                    pairs.append((collections, date_range))
                    else:
                        print(f"    No constraints, adding collections only")
                        pairs.append((collections, ""))
            
            for child in n.children:
                recurse(child, parent_is_and=(n.op == LogicalOp.AND))

    print("\n=== STARTING RECURSION ===")
    recurse(node)
    
    print(f"\n=== FINAL PAIRS BEFORE DEDUP: {len(pairs)} ===")
    for i, (collections, date_range) in enumerate(pairs):
        print(f"  {i+1}: collections={collections}, range='{date_range}'")
    
    # Remove duplicates and filter out entries without collections
    unique_pairs = []
    seen = set()
    for collections, date_range in pairs:
        if not collections:
            continue
        key = (tuple(sorted(collections)), date_range)
        if key not in seen:
            seen.add(key)
            unique_pairs.append((collections, date_range))
    
    print(f"\n=== UNIQUE FINAL PAIRS: {len(unique_pairs)} ===")
    for i, (collections, date_range) in enumerate(unique_pairs):
        print(f"  {i+1}: collections={collections}, range='{date_range}'")
    
    return unique_pairs
