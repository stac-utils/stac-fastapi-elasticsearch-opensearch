"""Extracts datetime patterns from CQL2 AST."""

from typing import Any, List, Optional, Tuple, Union

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
        ast = self._apply_demorgan(ast)
        ast = self._flatten_and_conditions(ast)

        # Step 3: Reorder for datetime priority (safe operation)
        ast = self._reorder_for_datetime_priority(ast)

        return ast

    def _apply_demorgan(self, node: CqlNode) -> CqlNode:
        """Apply De Morgan's law to transform NOT (A AND B) into (NOT A) OR (NOT B)."""
        if isinstance(node, LogicalNode):
            if node.op == LogicalOp.NOT and len(node.children) == 1:
                child = node.children[0]

                # Case: NOT (AND ...)
                if isinstance(child, LogicalNode) and child.op == LogicalOp.AND:
                    # Create OR of NOTs for each child
                    not_children = []
                    for and_child in child.children:
                        not_children.append(
                            LogicalNode(op=LogicalOp.NOT, children=[and_child])
                        )

                    # Recursively process the new NOT nodes
                    not_children = [self._apply_demorgan(c) for c in not_children]

                    # Return OR node
                    return LogicalNode(op=LogicalOp.OR, children=not_children)

                # Case: NOT (OR ...) = (NOT A) AND (NOT B)
                elif isinstance(child, LogicalNode) and child.op == LogicalOp.OR:
                    not_children = []
                    for or_child in child.children:
                        not_children.append(
                            LogicalNode(op=LogicalOp.NOT, children=[or_child])
                        )

                    # Recursively process the new NOT nodes
                    not_children = [self._apply_demorgan(c) for c in not_children]

                    # Return AND node
                    return LogicalNode(op=LogicalOp.AND, children=not_children)

                else:
                    # Regular NOT, process children recursively
                    processed_children = [
                        self._apply_demorgan(child) for child in node.children
                    ]
                    return LogicalNode(op=node.op, children=processed_children)

            else:
                # Recursively process children for other logical nodes
                processed_children = [
                    self._apply_demorgan(child) for child in node.children
                ]
                return LogicalNode(op=node.op, children=processed_children)

        return node

    def _flatten_and_conditions(self, node: CqlNode) -> CqlNode:
        """Flatten nested AND operations in the AST."""
        if isinstance(node, LogicalNode):
            if node.op == LogicalOp.AND:
                flattened_children = []
                for child in node.children:
                    flattened_child = self._flatten_and_conditions(child)

                    if (
                        isinstance(flattened_child, LogicalNode)
                        and flattened_child.op == LogicalOp.AND
                    ):
                        flattened_children.extend(flattened_child.children)
                    else:
                        flattened_children.append(flattened_child)

                if len(flattened_children) == 1:
                    return flattened_children[0]

                return LogicalNode(op=LogicalOp.AND, children=flattened_children)

            elif node.op in [LogicalOp.OR, LogicalOp.NOT]:
                processed_children = [
                    self._flatten_and_conditions(child) for child in node.children
                ]
                return LogicalNode(op=node.op, children=processed_children)
        return node

    def _reorder_for_datetime_priority(self, node: CqlNode) -> CqlNode:
        """Reorder query tree to prioritize datetime filters.

        Note: Only reorders within AND nodes, preserves OR and NOT structure.
        """
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
                # For OR and NOT, we cannot reorder children as it would change logic
                # But we can still process them recursively
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


def extract_collection_datetime(
    node: CqlNode,
    all_collection_ids: Optional[List[str]] = None,
) -> List[Tuple[List[str], str]]:
    """Extract collections, datetime range from CQL AST.

    Args:
        node: The CQL AST node
        all_collection_ids: List of all collection IDs from database (required for NOT collection handling)

    Returns:
        List of tuples where each tuple contains:
        - collections: List[str] collection id(s)
        - datetime_range: str or empty string if no datetime constraint
    """

    def extract_from_node(
        n: CqlNode, current_collections: List[str] = None
    ) -> List[Tuple[List[str], str]]:
        """Recursively extract collections and datetime ranges from node."""
        if current_collections is None:
            current_collections = []

        results: List[Tuple[List[str], str]] = []

        if isinstance(n, LogicalNode):
            # ===== FIXED: HANDLE OR NODES CORRECTLY =====
            if n.op == LogicalOp.OR:
                all_or_results = []

                for child in n.children:
                    child_results = extract_from_node(child, current_collections.copy())
                    all_or_results.extend(child_results)

                # For OR, we need to check if ANY branch has no datetime constraint
                # If so, we need ALL indexes
                for coll, rng in all_or_results:
                    if rng == "":  # Empty string means no datetime constraint
                        return [([], "")]  # Signal to search ALL indexes

                # Otherwise, return all OR results
                results.extend(all_or_results)

            elif n.op == LogicalOp.AND:
                # For AND, collect all child results first
                child_results_list = []
                for child in n.children:
                    child_results = extract_from_node(child, current_collections.copy())
                    if child_results:
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

            elif n.op == LogicalOp.NOT:
                # ===== FIXED: HANDLE NOT NODES CORRECTLY WITH PROPER RANGE GENERATION =====
                for child in n.children:
                    # Handle NOT on collection field
                    if (
                        isinstance(child, ComparisonNode)
                        and child.field == "collection"
                        and child.op == ComparisonOp.EQ
                    ):
                        if all_collection_ids:
                            excluded = child.value
                            if isinstance(excluded, list):
                                excluded_list = excluded
                            else:
                                excluded_list = [excluded]

                            # Get all collections except the excluded ones
                            included_collections = [
                                c for c in all_collection_ids if c not in excluded_list
                            ]

                            if included_collections:
                                # Return with empty date range (no datetime constraint)
                                results.append((included_collections, ""))
                            continue

                    # ===== NEW: Handle NOT on BETWEEN =====
                    elif (
                        isinstance(child, AdvancedComparisonNode)
                        and child.op == AdvancedComparisonOp.BETWEEN
                        and child.field
                        in ["datetime", "start_datetime", "end_datetime"]
                    ):
                        if (
                            isinstance(child.value, (list, tuple))
                            and len(child.value) == 2
                        ):
                            start_date, end_date = child.value[0], child.value[1]
                            # NOT BETWEEN means: before start OR after end
                            results.append(
                                (current_collections.copy(), f"../{start_date}")
                            )
                            results.append(
                                (current_collections.copy(), f"{end_date}/..")
                            )

                    # ===== FIXED: Handle NOT on AdvancedComparisonNode (IN operator) with multiple ranges =====
                    elif (
                        isinstance(child, AdvancedComparisonNode)
                        and child.op == AdvancedComparisonOp.IN
                        and child.field
                        in ["datetime", "start_datetime", "end_datetime"]
                    ):
                        if isinstance(child.value, list) and len(child.value) > 0:
                            # Sort the dates to create proper ranges
                            dates = sorted(child.value)

                            # Add range before the first date
                            results.append(
                                (current_collections.copy(), f"../{dates[0]}")
                            )

                            # Add ranges between consecutive dates
                            for i in range(len(dates) - 1):
                                results.append(
                                    (
                                        current_collections.copy(),
                                        f"{dates[i]}/{dates[i+1]}",
                                    )
                                )

                            # Add range after the last date
                            results.append(
                                (current_collections.copy(), f"{dates[-1]}/..")
                            )

                    # Handle NOT on datetime fields (existing code)
                    elif isinstance(child, ComparisonNode) and child.field in [
                        "datetime",
                        "start_datetime",
                        "end_datetime",
                    ]:
                        if child.op == ComparisonOp.LT:
                            # NOT (datetime < X) = datetime >= X
                            results.append(
                                (current_collections.copy(), f"{child.value}/..")
                            )
                        elif child.op == ComparisonOp.LTE:
                            # NOT (datetime <= X) = datetime > X
                            results.append(
                                (current_collections.copy(), f"{child.value}/..")
                            )
                        elif child.op == ComparisonOp.GT:
                            # NOT (datetime > X) = datetime <= X
                            results.append(
                                (current_collections.copy(), f"../{child.value}")
                            )
                        elif child.op == ComparisonOp.GTE:
                            # NOT (datetime >= X) = datetime < X
                            results.append(
                                (current_collections.copy(), f"../{child.value}")
                            )
                        elif child.op == ComparisonOp.EQ:
                            # NOT (datetime = X) = datetime < X OR datetime > X
                            # This creates two possible ranges
                            results.append(
                                (current_collections.copy(), f"../{child.value}")
                            )
                            results.append(
                                (current_collections.copy(), f"{child.value}/..")
                            )
                        else:
                            # For any other datetime comparison, we can't optimize
                            results.append((current_collections.copy(), ""))

                    # Handle NOT on other fields (like sat:orbit_state)
                    elif (
                        isinstance(child, ComparisonNode)
                        and child.field != "collection"
                    ):
                        # For non-datetime, non-collection fields, there's no datetime constraint
                        # This means we need to include ALL time ranges
                        results.append((current_collections.copy(), ""))

                    # Handle NOT on LogicalNode (should have been transformed by optimizer)
                    elif isinstance(child, LogicalNode):
                        # This case should ideally be handled by the optimizer
                        # But as fallback, recursively process
                        child_results = extract_from_node(
                            child, current_collections.copy()
                        )
                        for coll, rng in child_results:
                            if rng:
                                # Invert the range logic
                                if "/" in rng and ".." not in rng:
                                    parts = rng.split("/")
                                    if len(parts) == 2:
                                        results.append((coll, f"../{parts[0]}"))
                                        results.append((coll, f"{parts[1]}/.."))
                                elif "/" in rng:
                                    parts = rng.split("/")
                                    if len(parts) == 2:
                                        if parts[0] == "..":
                                            results.append((coll, f"{parts[1]}/.."))
                                        elif parts[1] == "..":
                                            results.append((coll, f"../{parts[0]}"))
                                        else:
                                            results.append((coll, f"../{parts[0]}"))
                                            results.append((coll, f"{parts[1]}/.."))
                                else:
                                    results.append((coll, f"../{rng}"))
                                    results.append((coll, f"{rng}/.."))
                            else:
                                results.append((coll, ""))

                    # Handle any other cases
                    else:
                        results.append((current_collections.copy(), ""))

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
                    # Not equal to a specific datetime means either before or after
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

    # Check if this is a datetime-only query (no collections in any result)
    has_collections = any(colls for colls, _ in all_results)

    if not has_collections:
        # DATETIME-ONLY CASE: Return all datetime conditions with empty collections
        datetime_only_results: List[Tuple[List[Any], Optional[Any]]] = []
        seen_dates = set()

        for _, date_range in all_results:
            if date_range and date_range not in seen_dates:
                seen_dates.add(date_range)
                datetime_only_results.append(([], date_range))

        return datetime_only_results

    # ===== FIXED: CHECK FOR ALL-INDEXES MARKER =====
    # If we have an empty collections + empty datetime range result, that means
    # we need to search ALL indexes (no optimization possible)
    needs_all_indexes = any(
        not colls and dt_range == "" for colls, dt_range in all_results
    )

    if needs_all_indexes:
        # Return a single result that will trigger full index scan
        return [([], "")]

    # Regular case with collections
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
