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
                    if rng == "":
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
                    new_combined = []

                    if len(combined_results) == 1 and len(next_child_results) == 1:
                        coll1, range1 = combined_results[0]
                        coll2, range2 = next_child_results[0]

                        merged_coll = list(set(coll1 + coll2))
                        merged_coll.sort()

                        is_start1 = range1.endswith("/..")
                        is_end1 = range1.startswith("../")
                        is_start2 = range2.endswith("/..")
                        is_end2 = range2.startswith("../")

                        if (is_start1 and is_end2) or (is_start2 and is_end1):
                            if is_start1 and is_end2:
                                start_value = range1[:-3]
                                end_value = range2[3:]
                            else:
                                start_value = range2[:-3]
                                end_value = range1[3:]

                            new_combined.append(
                                (merged_coll, f"{start_value}/{end_value}")
                            )
                        else:
                            for existing_coll, existing_range in combined_results:
                                for new_coll, new_range in next_child_results:

                                    merged_coll = list(set(existing_coll + new_coll))
                                    merged_coll.sort()

                                    if not existing_range and not new_range:
                                        new_combined.append((merged_coll, ""))

                                    elif existing_range and not new_range:
                                        new_combined.append(
                                            (merged_coll, existing_range)
                                        )
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
                        for existing_coll, existing_range in combined_results:
                            for new_coll, new_range in next_child_results:
                                merged_coll = list(set(existing_coll + new_coll))
                                merged_coll.sort()

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
                                        f"{dates[i]}/{dates[i+1]}",
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
