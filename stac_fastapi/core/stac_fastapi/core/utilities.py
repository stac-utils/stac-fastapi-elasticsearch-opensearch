"""Module for geospatial processing functions.

This module contains functions for transforming geospatial coordinates,
such as converting bounding boxes to polygon representations.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Set, Union

from stac_fastapi.core.models.patch import ElasticPath, ESCommandSet
from stac_fastapi.types.stac import (
    Item,
    PatchAddReplaceTest,
    PatchOperation,
    PatchRemove,
)

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


# copied from stac-fastapi-pgstac
# https://github.com/stac-utils/stac-fastapi-pgstac/blob/26f6d918eb933a90833f30e69e21ba3b4e8a7151/stac_fastapi/pgstac/utils.py#L10-L116
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

    # Build a shallow copy of included fields on an item, or a sub-tree of an item
    def include_fields(
        source: Dict[str, Any], fields: Optional[Set[str]]
    ) -> Dict[str, Any]:
        if not fields:
            return source

        clean_item: Dict[str, Any] = {}
        for key_path in fields or []:
            key_path_parts = key_path.split(".")
            key_root = key_path_parts[0]
            if key_root in source:
                if isinstance(source[key_root], dict) and len(key_path_parts) > 1:
                    # The root of this key path on the item is a dict, and the
                    # key path indicates a sub-key to be included. Walk the dict
                    # from the root key and get the full nested value to include.
                    value = include_fields(
                        source[key_root], fields={".".join(key_path_parts[1:])}
                    )

                    if isinstance(clean_item.get(key_root), dict):
                        # A previously specified key and sub-keys may have been included
                        # already, so do a deep merge update if the root key already exists.
                        dict_deep_update(clean_item[key_root], value)
                    else:
                        # The root key does not exist, so add it. Fields
                        # extension only allows nested referencing on dicts, so
                        # this won't overwrite anything.
                        clean_item[key_root] = value
                else:
                    # The item value to include is not a dict, or, it is a dict but the
                    # key path is for the whole value, not a sub-key. Include the entire
                    # value in the cleaned item.
                    clean_item[key_root] = source[key_root]
            else:
                # The key, or root key of a multi-part key, is not present in the item,
                # so it is ignored
                pass
        return clean_item

    # For an item built up for included fields, remove excluded fields. This
    # modifies `source` in place.
    def exclude_fields(source: Dict[str, Any], fields: Optional[Set[str]]) -> None:
        for key_path in fields or []:
            key_path_part = key_path.split(".")
            key_root = key_path_part[0]
            if key_root in source:
                if isinstance(source[key_root], dict) and len(key_path_part) > 1:
                    # Walk the nested path of this key to remove the leaf-key
                    exclude_fields(
                        source[key_root], fields={".".join(key_path_part[1:])}
                    )
                    # If, after removing the leaf-key, the root is now an empty
                    # dict, remove it entirely
                    if not source[key_root]:
                        del source[key_root]
                else:
                    # The key's value is not a dict, or there is no sub-key to remove. The
                    # entire key can be removed from the source.
                    source.pop(key_root, None)

    # Coalesce incoming type to a dict
    item = dict(item)

    clean_item = include_fields(item, include)

    # If, after including all the specified fields, there are no included properties,
    # return just id and collection.
    if not clean_item:
        return Item({"id": item["id"], "collection": item["collection"]})

    exclude_fields(clean_item, exclude)

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


def merge_to_operations(data: Dict) -> List:
    """Convert merge operation to list of RF6902 operations.

    Args:
        data: dictionary to convert.

    Returns:
        List: list of RF6902 operations.
    """
    operations = []

    for key, value in data.copy().items():

        if value is None:
            operations.append(PatchRemove(op="remove", path=key))

        elif isinstance(value, dict):
            nested_operations = merge_to_operations(value)

            for nested_operation in nested_operations:
                nested_operation.path = f"{key}.{nested_operation.path}"
                operations.append(nested_operation)

        else:
            operations.append(PatchAddReplaceTest(op="add", path=key, value=value))

    return operations


def check_commands(
    commands: ESCommandSet,
    op: str,
    path: ElasticPath,
    from_path: bool = False,
) -> None:
    """Add Elasticsearch checks to operation.

    Args:
        commands (List[str]): current commands
        op (str): the operation of script
        path (Dict): path of variable to run operation on
        from_path (bool): True if path is a from path

    """
    if path.nest:
        commands.add(
            f"if (!ctx._source.containsKey('{path.nest}'))"
            f"{{Debug.explain('{path.nest} does not exist');}}"
        )

    if path.index or op in ["remove", "replace", "test"] or from_path:
        commands.add(
            f"if (!ctx._source{path.es_nest}.containsKey('{path.key}'))"
            f"{{Debug.explain('{path.key}  does not exist in {path.nest}');}}"
        )

    if from_path and path.index is not None:
        commands.add(
            f"if ((ctx._source{path.es_location} instanceof ArrayList"
            f" && ctx._source{path.es_location}.size() < {path.index})"
            f" || (!(ctx._source{path.es_location} instanceof ArrayList)"
            f" && !ctx._source{path.es_location}.containsKey('{path.index}')))"
            f"{{Debug.explain('{path.path} does not exist');}}"
        )


def remove_commands(commands: ESCommandSet, path: ElasticPath) -> None:
    """Remove value at path.

    Args:
        commands (List[str]): current commands
        path (ElasticPath): Path to value to be removed

    """
    if path.index is not None:
        commands.add(
            f"def {path.variable_name} = ctx._source{path.es_location}.remove({path.index});"
        )

    else:
        commands.add(
            f"def {path.variable_name} = ctx._source{path.es_nest}.remove('{path.key}');"
        )


def add_commands(
    commands: ESCommandSet,
    operation: PatchOperation,
    path: ElasticPath,
    from_path: ElasticPath,
    params: Dict,
) -> None:
    """Add value at path.

    Args:
        commands (List[str]): current commands
        operation (PatchOperation): operation to run
        path (ElasticPath): path for value to be added

    """
    if from_path is not None:
        value = (
            from_path.variable_name
            if operation.op == "move"
            else f"ctx._source.{from_path.es_path}"
        )
    else:
        value = f"params.{path.param_key}"
        params[path.param_key] = operation.value

    if path.index is not None:
        commands.add(
            f"if (ctx._source{path.es_location} instanceof ArrayList)"
            f"{{ctx._source{path.es_location}.{'add' if operation.op in ['add', 'move'] else 'set'}({path.index}, {value})}}"
            f"else{{ctx._source.{path.es_path} = {value}}}"
        )

    else:
        commands.add(f"ctx._source.{path.es_path} = {value};")


def test_commands(
    commands: ESCommandSet, operation: PatchOperation, path: ElasticPath, params: Dict
) -> None:
    """Test value at path.

    Args:
        commands (List[str]): current commands
        operation (PatchOperation): operation to run
        path (ElasticPath): path for value to be tested
    """
    value = f"params.{path.param_key}"
    params[path.param_key] = operation.value

    commands.add(
        f"if (ctx._source.{path.es_path} != {value})"
        f"{{Debug.explain('Test failed `{path.path}` | "
        f"{operation.json_value} != ' + ctx._source.{path.es_path});}}"
    )


def operations_to_script(operations: List) -> Dict:
    """Convert list of operation to painless script.

    Args:
        operations: List of RF6902 operations.

    Returns:
        Dict: elasticsearch update script.
    """
    commands: ESCommandSet = ESCommandSet()
    params: Dict = {}

    for operation in operations:
        path = ElasticPath(path=operation.path)
        from_path = (
            ElasticPath(path=operation.from_) if hasattr(operation, "from_") else None
        )

        check_commands(commands=commands, op=operation.op, path=path)
        if from_path is not None:
            check_commands(
                commands=commands, op=operation.op, path=from_path, from_path=True
            )

        if operation.op in ["remove", "move"]:
            remove_path = from_path if from_path else path
            remove_commands(commands=commands, path=remove_path)

        if operation.op in ["add", "replace", "copy", "move"]:
            add_commands(
                commands=commands,
                operation=operation,
                path=path,
                from_path=from_path,
                params=params,
            )

        if operation.op == "test":
            test_commands(
                commands=commands, operation=operation, path=path, params=params
            )

        source = "".join(commands)

    return {
        "source": source,
        "lang": "painless",
        "params": params,
    }
