"""Module for geospatial processing functions.

This module contains functions for transforming geospatial coordinates,
such as converting bounding boxes to polygon representations.
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional, Set, Union

from stac_fastapi.types.stac import Item

MAX_LIMIT = 10000


def get_bool_env(name: str, default: Union[bool, str] = False) -> bool:
    """
    Retrieve a boolean value from an environment variable.

    Args:
        name (str): The name of the environment variable.
        default (Union[bool, str], optional): The default value to use if the variable is not set or unrecognized. Defaults to False.

    Returns:
        bool: The boolean value parsed from the environment variable.
    """
    true_values = ("true", "1", "yes", "y")
    false_values = ("false", "0", "no", "n")

    # Normalize the default value
    if isinstance(default, bool):
        default_str = "true" if default else "false"
    elif isinstance(default, str):
        default_str = default.lower()
    else:
        logger = logging.getLogger(__name__)
        logger.warning(
            f"The `default` parameter must be a boolean or string, got {type(default).__name__}. "
            f"Falling back to `False`."
        )
        default_str = "false"

    # Retrieve and normalize the environment variable value
    value = os.getenv(name, default_str)
    if value.lower() in true_values:
        return True
    elif value.lower() in false_values:
        return False
    else:
        logger = logging.getLogger(__name__)
        logger.warning(
            f"Environment variable '{name}' has unrecognized value '{value}'. "
            f"Expected one of {true_values + false_values}. Using default: {default_str}"
        )
        return default_str in true_values


def bbox2polygon(b0: float, b1: float, b2: float, b3: float) -> List[List[List[float]]]:
    """Transform a bounding box represented by its four coordinates `b0`, `b1`, `b2`, and `b3` into a polygon.

    Args:
        b0 (float): The x-coordinate of the lower-left corner of the bounding box.
        b1 (float): The y-coordinate of the lower-left corner of the bounding box.
        b2 (float): The x-coordinate of the upper-right corner of the bounding box.
        b3 (float): The y-coordinate of the upper-right corner of the bounding box.

    Returns:
        List[List[List[float]]]: A polygon represented as a list of lists of coordinates.
    """
    return [[[b0, b1], [b2, b1], [b2, b3], [b0, b3], [b0, b1]]]


def filter_fields(  # noqa: C901
    item: Union[Item, Dict[str, Any]],
    include: Optional[Set[str]] = None,
    exclude: Optional[Set[str]] = None,
) -> Item:
    """Preserve and remove fields as indicated by the fields extension include/exclude sets.

    Returns a shallow copy of the Item with the fields filtered.

    This will not perform a deep copy; values of the original item will be referenced
    in the return item.
    """
    if not include and not exclude:
        return item

    def match_pattern(pattern: str, key: str) -> bool:
        """Check if a key matches a wildcard pattern."""
        regex_pattern = "^" + re.escape(pattern).replace(r"\*", ".*") + "$"
        return bool(re.match(regex_pattern, key))

    def get_matching_keys(source: Dict[str, Any], pattern: str) -> List[str]:
        """Get all keys that match the pattern."""
        if not isinstance(source, dict):
            return []
        return [key for key in source.keys() if match_pattern(pattern, key)]

    def include_fields(
        source: Dict[str, Any], fields: Optional[Set[str]]
    ) -> Dict[str, Any]:
        """Include only the specified fields from the source dictionary."""
        if not fields:
            return source

        def recursive_include(
            source: Dict[str, Any], path_parts: List[str]
        ) -> Dict[str, Any]:
            """Recursively include fields matching the pattern path."""
            if not path_parts:
                return source

            if not isinstance(source, dict):
                return {}

            current_pattern = path_parts[0]
            remaining_parts = path_parts[1:]

            matching_keys = get_matching_keys(source, current_pattern)

            if not matching_keys:
                return {}

            result: Dict[str, Any] = {}
            for key in matching_keys:
                if remaining_parts:
                    if isinstance(source[key], dict):
                        value = recursive_include(source[key], remaining_parts)
                        if value:
                            result[key] = value
                else:
                    result[key] = source[key]

            return result

        clean_item: Dict[str, Any] = {}
        for key_path in fields or []:
            key_path_parts = key_path.split(".")
            included_values = recursive_include(source, key_path_parts)

            for key, value in included_values.items():
                if isinstance(clean_item.get(key), dict) and isinstance(value, dict):
                    dict_deep_update(clean_item[key], value)
                else:
                    clean_item[key] = value

        return clean_item

    def exclude_fields(
        source: Dict[str, Any],
        fields: Optional[Set[str]],
        included_fields: Optional[Set[str]] = None,
    ) -> None:
        """Exclude fields from source, but preserve any fields that were explicitly included."""

        def is_path_included(current_path: str) -> bool:
            """Check if a path matches any of the included field patterns."""
            if not included_fields:
                return False

            for include_pattern in included_fields:
                include_parts = include_pattern.split(".")
                current_parts = current_path.split(".")

                # Check if current path matches the include pattern
                if len(include_parts) != len(current_parts):
                    continue

                match = True
                for include_part, current_part in zip(include_parts, current_parts):
                    if not match_pattern(include_part, current_part):
                        match = False
                        break

                if match:
                    return True

            return False

        def recursive_exclude(
            source: Dict[str, Any], path_parts: List[str], current_path: str = ""
        ) -> None:
            """Recursively exclude fields matching the pattern path."""
            if not path_parts or not isinstance(source, dict):
                return

            current_pattern = path_parts[0]
            remaining_parts = path_parts[1:]

            matching_keys = get_matching_keys(source, current_pattern)

            for key in list(matching_keys):
                if key not in source:
                    continue

                # Build the full path for this key
                full_path = f"{current_path}.{key}" if current_path else key

                # Skip exclusion if this path was explicitly included
                if is_path_included(full_path):
                    continue

                if remaining_parts:
                    if isinstance(source[key], dict):
                        recursive_exclude(source[key], remaining_parts, full_path)
                        if not source[key]:
                            del source[key]
                else:
                    source.pop(key, None)

        for key_path in fields or []:
            key_path_parts = key_path.split(".")
            recursive_exclude(source, key_path_parts)

    item = dict(item)

    clean_item = include_fields(item, include)

    if not clean_item:
        return Item({"id": item["id"], "collection": item["collection"]})

    exclude_fields(clean_item, exclude, include)

    return Item(**clean_item)


def dict_deep_update(merge_to: Dict[str, Any], merge_from: Dict[str, Any]) -> None:
    """Perform a deep update of two dicts.

    merge_to is updated in-place with the values from merge_from.
    merge_from values take precedence over existing values in merge_to.
    """
    for k, v in merge_from.items():
        if (
            k in merge_to
            and isinstance(merge_to[k], dict)
            and isinstance(merge_from[k], dict)
        ):
            dict_deep_update(merge_to[k], merge_from[k])
        else:
            merge_to[k] = v


def get_excluded_from_items(obj: dict, field_path: str) -> None:
    """Remove a field from items.

    The field is removed in-place from the dictionary if it exists.
    If any intermediate path does not exist or is not a dictionary,
    the function returns without making any changes.
    """
    *path, final = field_path.split(".")
    current = obj
    for part in path:
        current = current.get(part, {})
        if not isinstance(current, dict):
            return

    current.pop(final, None)
