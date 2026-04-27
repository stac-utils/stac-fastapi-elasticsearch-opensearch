"""Extracts datetime patterns from CQL2 AST."""

from typing import Any, List, Optional, Tuple, Union

from stac_fastapi.core.extensions.filter import (
    AdvancedComparisonNode,
    AdvancedComparisonOp,
    ComparisonNode,
    ComparisonOp,
    CqlNode,
    LogicalNode,
    LogicalOp,
)


class DatetimeOptimizer:
    """Extract datetime from CQL2 AST."""

    def __init__(self) -> None:
        """Initialize the datetime optimizer."""
        self.datetime_nodes: List[Union[ComparisonNode, AdvancedComparisonNode]] = []

    def _is_datetime_field(self, field: str) -> bool:
        """Datetime field checker."""
        field_lower = field.lower()
        return field_lower in {"datetime", "start_datetime", "end_datetime"}

    def optimize_query_structure(self, ast: CqlNode) -> CqlNode:
        """Optimize AST structure for better query performance."""
        ast = self._apply_demorgan(ast)
        ast = self._flatten_and_conditions(ast)

        ast = self._reorder_for_datetime_priority(ast)

        return ast

    def _apply_demorgan(self, node: CqlNode) -> CqlNode:
        """Apply De Morgan's law special case to transform NOT (A AND B) into (NOT A) OR (NOT B)."""
        if isinstance(node, LogicalNode):
            if node.op == LogicalOp.NOT and len(node.children) == 1:
                child = node.children[0]

                if isinstance(child, LogicalNode) and child.op == LogicalOp.AND:
                    not_children = []
                    for and_child in child.children:
                        not_children.append(
                            LogicalNode(op=LogicalOp.NOT, children=[and_child])
                        )
                    not_children = [self._apply_demorgan(c) for c in not_children]

                    return LogicalNode(op=LogicalOp.OR, children=not_children)

                elif isinstance(child, LogicalNode) and child.op == LogicalOp.OR:
                    not_children = []
                    for or_child in child.children:
                        not_children.append(
                            LogicalNode(op=LogicalOp.NOT, children=[or_child])
                        )

                    not_children = [self._apply_demorgan(c) for c in not_children]

                    return LogicalNode(op=LogicalOp.AND, children=not_children)

                else:
                    processed_children = [
                        self._apply_demorgan(child) for child in node.children
                    ]
                    return LogicalNode(op=node.op, children=processed_children)

            else:
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


def _merge_date_ranges(range1: str, range2: str) -> Tuple[Optional[str], List[str]]:
    """Merge two date range strings and return the merged range and any additional ranges.

    Args:
        range1: First date range string
        range2: Second date range string

    Returns:
        Tuple of (merged_range, additional_ranges) where merged_range is the intersection or combination,
        and additional_ranges are any ranges that couldn't be merged
    """
    additional_ranges = []

    if ".." in range1 or ".." in range2:
        if ".." in range1:
            additional_ranges.append(range1)
        if ".." in range2:
            additional_ranges.append(range2)
        return None, additional_ranges

    if "/" in range1:
        parts1 = range1.split("/")
        start1 = None if parts1[0] == ".." else parts1[0]
        end1 = None if parts1[1] == ".." else parts1[1]
    else:
        start1 = end1 = range1

    if "/" in range2:
        parts2 = range2.split("/")
        start2 = None if parts2[0] == ".." else parts2[0]
        end2 = None if parts2[1] == ".." else parts2[1]
    else:
        start2 = end2 = range2

    start = None
    end = None

    if start1 and start2:
        start = max(start1, start2)
    elif start1:
        start = start1
    elif start2:
        start = start2

    if end1 and end2:
        end = min(end1, end2)
    elif end1:
        end = end1
    elif end2:
        end = end2

    if start and end:
        if start <= end:
            if start == end:
                return start, []
            else:
                return f"{start}/{end}", []
    elif start:
        return f"{start}/..", []
    elif end:
        return f"../{end}", []

    return None, []


def _merge_and_results(
    results1: List[Tuple[List[str], str]], results2: List[Tuple[List[str], str]]
) -> List[Tuple[List[str], str]]:
    """Merge two sets of AND branch results."""
    if not results1 or not results2:
        return []

    merged_results = []

    for coll1, range1 in results1:
        for coll2, range2 in results2:
            merged_coll = list(set(coll1 + coll2))
            merged_coll.sort()

            if not range1 and not range2:
                merged_results.append((merged_coll, ""))
            elif range1 and not range2:
                merged_results.append((merged_coll, range1))
            elif not range1 and range2:
                merged_results.append((merged_coll, range2))
            else:
                merged_range, additional = _merge_date_ranges(range1, range2)
                if merged_range:
                    merged_results.append((merged_coll, merged_range))
                if additional:
                    for add_range in additional:
                        merged_results.append((merged_coll, add_range))

    return merged_results


def _is_or_of_nots(node: CqlNode) -> bool:
    """Check if node is an OR operation where children are NOT operations."""
    if isinstance(node, LogicalNode) and node.op == LogicalOp.OR:
        return all(
            isinstance(child, LogicalNode) and child.op == LogicalOp.NOT
            for child in node.children
        )
    return False


def _negated_datetime_ranges(op: ComparisonOp, value: Any) -> List[str]:
    """Convert a negated datetime comparison into one or more datetime ranges."""
    if op in [ComparisonOp.GT, ComparisonOp.GTE]:
        return [f"../{value}"]
    if op in [ComparisonOp.LT, ComparisonOp.LTE]:
        return [f"{value}/.."]
    if op == ComparisonOp.EQ:
        return [f"../{value}", f"{value}/.."]
    return []


def _negated_advanced_datetime_ranges(
    op: AdvancedComparisonOp, value: Any
) -> List[str]:
    """Convert a negated advanced datetime comparison into one or more datetime ranges."""
    if op == AdvancedComparisonOp.BETWEEN:
        if isinstance(value, (list, tuple)) and len(value) == 2:
            return [f"../{value[0]}", f"{value[1]}/.."]
        return []

    if op == AdvancedComparisonOp.IN:
        if isinstance(value, list) and value:
            dates = sorted(value)
            ranges = [f"../{dates[0]}"]
            ranges.extend(f"{dates[i]}/{dates[i + 1]}" for i in range(len(dates) - 1))
            ranges.append(f"{dates[-1]}/..")
            return ranges
        return []

    return []


def extract_collection_datetime(
    node: CqlNode,
    all_collection_ids: Optional[List[str]] = None,
) -> List[Tuple[List[str], str]]:
    """Extract collections, datetime range from CQL AST.

    Args:
        node: The CQL AST node
        all_collection_ids: List of all collection ids for collection exclusion logic

    Returns:
        List of tuples where each tuple contains:
        - collections: List[str] collection id(s)
        - datetime_range: str or empty string if no datetime constraint
    """
    # Check for OR of NOTs at the root level (De Morgan's law)
    if _is_or_of_nots(node):
        datetime_ranges = []
        excluded_collections = set()

        for child in node.children:
            if isinstance(child, LogicalNode) and child.op == LogicalOp.NOT:
                for grandchild in child.children:
                    if isinstance(grandchild, ComparisonNode):
                        if (
                            grandchild.field == "collection"
                            and grandchild.op == ComparisonOp.EQ
                        ):
                            if isinstance(grandchild.value, list):
                                excluded_collections.update(grandchild.value)
                            else:
                                excluded_collections.add(grandchild.value)
                        elif grandchild.field in [
                            "datetime",
                            "start_datetime",
                            "end_datetime",
                        ]:
                            datetime_ranges.extend(
                                _negated_datetime_ranges(
                                    grandchild.op,
                                    grandchild.value,
                                )
                            )
                    elif isinstance(grandchild, AdvancedComparisonNode):
                        if (
                            grandchild.field == "collection"
                            and grandchild.op == AdvancedComparisonOp.IN
                        ):
                            if isinstance(grandchild.value, list):
                                excluded_collections.update(grandchild.value)
                        elif grandchild.field in [
                            "datetime",
                            "start_datetime",
                            "end_datetime",
                        ]:
                            datetime_ranges.extend(
                                _negated_advanced_datetime_ranges(
                                    grandchild.op,
                                    grandchild.value,
                                )
                            )

        if all_collection_ids and (datetime_ranges or excluded_collections):
            results = []

            included_collections = [
                c for c in all_collection_ids if c not in excluded_collections
            ]
            if excluded_collections and included_collections:
                results.append((included_collections, ""))

            seen_ranges = set()
            for datetime_range in datetime_ranges:
                if datetime_range not in seen_ranges:
                    seen_ranges.add(datetime_range)
                    results.append((all_collection_ids, datetime_range))

            if results:
                return results

    def extract_from_node(
        n: CqlNode, current_collections: List[str] = None
    ) -> List[Tuple[List[str], str]]:
        """Recursively extract collections and datetime ranges from node."""
        if current_collections is None:
            current_collections = []

        results: List[Tuple[List[str], str]] = []

        if isinstance(n, LogicalNode):
            if n.op == LogicalOp.OR:
                all_or_results = []

                for child in n.children:
                    child_results = extract_from_node(child, current_collections.copy())
                    all_or_results.extend(child_results)

                for coll, rng in all_or_results:
                    if not coll and rng == "":
                        return [([], "")]

                results.extend(all_or_results)

            elif n.op == LogicalOp.AND:
                child_results_list = []
                for child in n.children:
                    child_results = extract_from_node(child, current_collections.copy())
                    if child_results:
                        child_results_list.append(child_results)

                if not child_results_list:
                    return results

                if len(child_results_list) == 1:
                    return child_results_list[0]

                combined_results = child_results_list[0]

                for next_child_results in child_results_list[1:]:
                    combined_results = _merge_and_results(
                        combined_results, next_child_results
                    )

                results.extend(combined_results)

            elif n.op == LogicalOp.NOT:
                for child in n.children:
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
                            included_collections = [
                                c for c in all_collection_ids if c not in excluded_list
                            ]
                            if included_collections:
                                results.append((included_collections, ""))
                            continue

                    elif (
                        isinstance(child, AdvancedComparisonNode)
                        and child.field == "collection"
                        and child.op == AdvancedComparisonOp.IN
                    ):
                        if all_collection_ids:
                            excluded = (
                                child.value if isinstance(child.value, list) else []
                            )
                            included_collections = [
                                c for c in all_collection_ids if c not in excluded
                            ]
                            if included_collections:
                                results.append((included_collections, ""))
                            continue

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
                            results.append(
                                (current_collections.copy(), f"../{start_date}")
                            )
                            results.append(
                                (current_collections.copy(), f"{end_date}/..")
                            )

                    elif (
                        isinstance(child, AdvancedComparisonNode)
                        and child.op == AdvancedComparisonOp.IN
                        and child.field
                        in ["datetime", "start_datetime", "end_datetime"]
                    ):
                        if isinstance(child.value, list) and len(child.value) > 0:
                            dates = sorted(child.value)

                            results.append(
                                (current_collections.copy(), f"../{dates[0]}")
                            )

                            for i in range(len(dates) - 1):
                                results.append(
                                    (
                                        current_collections.copy(),
                                        f"{dates[i]}/{dates[i + 1]}",
                                    )
                                )

                            results.append(
                                (current_collections.copy(), f"{dates[-1]}/..")
                            )

                    elif isinstance(child, ComparisonNode) and child.field in [
                        "datetime",
                        "start_datetime",
                        "end_datetime",
                    ]:
                        if child.op == ComparisonOp.LT:
                            results.append(
                                (current_collections.copy(), f"{child.value}/..")
                            )
                        elif child.op == ComparisonOp.LTE:
                            results.append(
                                (current_collections.copy(), f"{child.value}/..")
                            )
                        elif child.op == ComparisonOp.GT:
                            results.append(
                                (current_collections.copy(), f"../{child.value}")
                            )
                        elif child.op == ComparisonOp.GTE:
                            results.append(
                                (current_collections.copy(), f"../{child.value}")
                            )
                        elif child.op == ComparisonOp.EQ:
                            results.append(
                                (current_collections.copy(), f"../{child.value}")
                            )
                            results.append(
                                (current_collections.copy(), f"{child.value}/..")
                            )
                        else:
                            results.append((current_collections.copy(), ""))

                    elif (
                        isinstance(child, ComparisonNode)
                        and child.field != "collection"
                    ):
                        results.append((current_collections.copy(), ""))

                    elif isinstance(child, LogicalNode):
                        child_results = extract_from_node(
                            child, current_collections.copy()
                        )
                        for coll, rng in child_results:
                            if rng:
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

    final_results = []
    seen = set()

    has_collections = any(colls for colls, _ in all_results)

    if not has_collections:
        datetime_only_results: List[Tuple[List[Any], Optional[Any]]] = []
        seen_dates = set()

        for _, date_range in all_results:
            if date_range and date_range not in seen_dates:
                seen_dates.add(date_range)
                datetime_only_results.append(([], date_range))

        return datetime_only_results

    needs_all_indexes = any(
        not colls and dt_range == "" for colls, dt_range in all_results
    )

    if needs_all_indexes:
        return [([], "")]

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
