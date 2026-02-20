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
    # pairs: List[Tuple[List[str], str]] = []

    def extract_from_node(
        n: CqlNode, current_collections: List[str] = None
    ) -> List[Tuple[List[str], str]]:
        """Recursively extract collections and datetime ranges from node."""
        if current_collections is None:
            current_collections = []

        results: List[Tuple[List[str], str]] = []

        if isinstance(n, LogicalNode):
            if n.op == LogicalOp.AND:
                # For AND, collect all child results first
                child_results_list = []
                for child in n.children:
                    child_results = extract_from_node(child, current_collections.copy())
                    child_results_list.append(child_results)

                if not child_results_list:
                    return results

                # If there's only one child, just return its results
                if len(child_results_list) == 1:
                    return child_results_list[0]

                # Combine results from multiple children
                # Start with results from first child
                combined_results = child_results_list[0]

                # For each subsequent child, combine with existing results
                for next_child_results in child_results_list[1:]:
                    new_combined = []

                    # Special handling for combining two ranges in an AND
                    # This is the most common case: GTE and LTE
                    if len(combined_results) == 1 and len(next_child_results) == 1:
                        coll1, range1 = combined_results[0]
                        coll2, range2 = next_child_results[0]

                        # Merge collections
                        merged_coll = list(set(coll1 + coll2))
                        merged_coll.sort()

                        # Check if we have one start range and one end range
                        is_start1 = range1.endswith("/..")
                        is_end1 = range1.startswith("../")
                        is_start2 = range2.endswith("/..")
                        is_end2 = range2.startswith("../")

                        # If we have both a start and end range, combine them
                        if (is_start1 and is_end2) or (is_start2 and is_end1):
                            if is_start1 and is_end2:
                                start_value = range1[:-3]  # Remove "/.."
                                end_value = range2[3:]  # Remove "../"
                            else:
                                start_value = range2[:-3]  # Remove "/.."
                                end_value = range1[3:]  # Remove "../"

                            new_combined.append(
                                (merged_coll, f"{start_value}/{end_value}")
                            )
                        else:
                            # Fall back to normal processing
                            for existing_coll, existing_range in combined_results:
                                for new_coll, new_range in next_child_results:
                                    # Merge collections
                                    merged_coll = list(set(existing_coll + new_coll))
                                    merged_coll.sort()

                                    # Merge ranges based on type
                                    if not existing_range and not new_range:
                                        # No ranges, just collections
                                        new_combined.append((merged_coll, ""))
                                    elif existing_range and not new_range:
                                        # Only existing has range
                                        new_combined.append(
                                            (merged_coll, existing_range)
                                        )
                                    elif not existing_range and new_range:
                                        # Only new has range
                                        new_combined.append((merged_coll, new_range))
                                    else:
                                        # Both have ranges - need to combine with AND logic
                                        if ".." in existing_range or ".." in new_range:
                                            # Handle NEQ ranges - keep them separate as they represent exclusions
                                            if ".." in existing_range:
                                                new_combined.append(
                                                    (merged_coll, existing_range)
                                                )
                                            if ".." in new_range:
                                                new_combined.append(
                                                    (merged_coll, new_range)
                                                )
                                        else:
                                            # Both are regular ranges or exact dates
                                            if "/" in existing_range:
                                                e_parts = existing_range.split("/")
                                                e_start = (
                                                    None
                                                    if e_parts[0] == ".."
                                                    else e_parts[0]
                                                )
                                                e_end = (
                                                    None
                                                    if e_parts[1] == ".."
                                                    else e_parts[1]
                                                )
                                            else:
                                                e_start = e_end = existing_range

                                            if "/" in new_range:
                                                n_parts = new_range.split("/")
                                                n_start = (
                                                    None
                                                    if n_parts[0] == ".."
                                                    else n_parts[0]
                                                )
                                                n_end = (
                                                    None
                                                    if n_parts[1] == ".."
                                                    else n_parts[1]
                                                )
                                            else:
                                                n_start = n_end = new_range

                                            # Take the most restrictive
                                            start = None
                                            end = None
                                            if e_start and n_start:
                                                start = max(e_start, n_start)
                                            elif e_start:
                                                start = e_start
                                            elif n_start:
                                                start = n_start

                                            if e_end and n_end:
                                                end = min(e_end, n_end)
                                            elif e_end:
                                                end = e_end
                                            elif n_end:
                                                end = n_end

                                            if start and end:
                                                if start <= end:
                                                    if start == end:
                                                        new_combined.append(
                                                            (merged_coll, start)
                                                        )
                                                    else:
                                                        new_combined.append(
                                                            (
                                                                merged_coll,
                                                                f"{start}/{end}",
                                                            )
                                                        )
                                            elif start:
                                                new_combined.append(
                                                    (merged_coll, f"{start}/..")
                                                )
                                            elif end:
                                                new_combined.append(
                                                    (merged_coll, f"../{end}")
                                                )
                    else:
                        # Multiple results case - use the existing logic
                        for existing_coll, existing_range in combined_results:
                            for new_coll, new_range in next_child_results:
                                # Merge collections
                                merged_coll = list(set(existing_coll + new_coll))
                                merged_coll.sort()

                                # Merge ranges based on type
                                if not existing_range and not new_range:
                                    new_combined.append((merged_coll, ""))
                                elif existing_range and not new_range:
                                    new_combined.append((merged_coll, existing_range))
                                elif not existing_range and new_range:
                                    new_combined.append((merged_coll, new_range))
                                else:
                                    if ".." in existing_range or ".." in new_range:
                                        if ".." in existing_range:
                                            new_combined.append(
                                                (merged_coll, existing_range)
                                            )
                                        if ".." in new_range:
                                            new_combined.append(
                                                (merged_coll, new_range)
                                            )
                                    else:
                                        new_combined.append(
                                            (
                                                merged_coll,
                                                f"{existing_range}/{new_range}",
                                            )
                                        )

                    combined_results = new_combined

                results.extend(combined_results)

            elif n.op == LogicalOp.OR:
                for child in n.children:
                    child_results = extract_from_node(child, current_collections.copy())
                    results.extend(child_results)

            elif n.op == LogicalOp.NOT:
                # Handle NOT operator by inverting the meaning of the child results
                for child in n.children:
                    child_results = extract_from_node(child, current_collections.copy())
                    for coll, rng in child_results:
                        if rng:
                            if "/" in rng:
                                parts = rng.split("/")
                                if len(parts) == 2:
                                    if parts[0] == "..":
                                        # NOT (../X) becomes X/..
                                        results.append((coll, f"{parts[1]}/.."))
                                    elif parts[1] == "..":
                                        # NOT (X/..) becomes ../X
                                        results.append((coll, f"../{parts[0]}"))
                                    else:
                                        # NOT (X/Y) becomes ../X and Y/..
                                        results.append((coll, f"../{parts[0]}"))
                                        results.append((coll, f"{parts[1]}/.."))
                            else:
                                # NOT (exact date) becomes ../X and X/..
                                results.append((coll, f"../{rng}"))
                                results.append((coll, f"{rng}/.."))
                        else:
                            # If the child had no range (just collections), NOT doesn't change that
                            results.append((coll, ""))

        elif isinstance(n, ComparisonNode):
            if n.field == "collection":
                new_collections = current_collections.copy()
                if isinstance(n.value, list):
                    new_collections.extend(n.value)
                else:
                    new_collections.append(n.value)
                results.append((new_collections, ""))

            elif n.field in ["datetime", "start_datetime", "end_datetime"]:
                if n.op == ComparisonOp.EQ:
                    results.append((current_collections.copy(), n.value))
                elif n.op == ComparisonOp.GT:
                    results.append((current_collections.copy(), f"{n.value}/.."))
                elif n.op == ComparisonOp.GTE:
                    results.append((current_collections.copy(), f"{n.value}/.."))
                elif n.op == ComparisonOp.LT:
                    results.append((current_collections.copy(), f"../{n.value}"))
                elif n.op == ComparisonOp.LTE:
                    results.append((current_collections.copy(), f"../{n.value}"))
                elif n.op == ComparisonOp.NEQ:
                    results.append((current_collections.copy(), f"../{n.value}"))
                    results.append((current_collections.copy(), f"{n.value}/.."))

        elif isinstance(n, AdvancedComparisonNode):
            if n.field in ["datetime", "start_datetime", "end_datetime"]:
                if n.op == AdvancedComparisonOp.BETWEEN:
                    if isinstance(n.value, (list, tuple)) and len(n.value) == 2:
                        results.append(
                            (current_collections.copy(), f"{n.value[0]}/{n.value[1]}")
                        )
                elif n.op == AdvancedComparisonOp.IN:
                    if isinstance(n.value, list):
                        for date_value in n.value:
                            results.append((current_collections.copy(), date_value))

        return results

    all_results = extract_from_node(node)

    # Process results
    final_results = []
    seen = set()

    for collections, date_range in all_results:
        if not collections:
            continue

        unique_collections = list(set(collections))
        unique_collections.sort()

        if date_range is not None:
            key = (tuple(unique_collections), date_range)
            if key not in seen:
                seen.add(key)
                final_results.append((unique_collections, date_range))

    return final_results
