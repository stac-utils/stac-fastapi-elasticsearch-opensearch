"""Utility functions for database operations in Elasticsearch/OpenSearch.

This module provides utility functions for working with database operations
in Elasticsearch/OpenSearch, such as parameter validation.
"""

import logging
from typing import Any

from stac_fastapi.core.utilities import bbox2polygon, get_bool_env
from stac_fastapi.extensions.core.transaction.request import (
    PatchAddReplaceTest,
    PatchOperation,
    PatchRemove,
)
from stac_fastapi.sfeos_helpers.models.patch import ElasticPath, ESCommandSet
from stac_fastapi.types.errors import ConflictError

logger = logging.getLogger(__name__)


class ItemAlreadyExistsError(ConflictError):
    """Error raised when attempting to create an item that already exists.

    Attributes:
        item_id: The ID of the item that already exists.
        collection_id: The ID of the collection containing the item.
    """

    def __init__(self, item_id: str, collection_id: str):
        """Initialize the error with item and collection IDs."""
        self.item_id = item_id
        self.collection_id = collection_id
        message = f"Item {item_id} in collection {collection_id} already exists"
        super().__init__(message)


async def check_item_exists_in_alias(client: Any, alias: str, doc_id: str) -> bool:
    """Check if an item exists across all indexes for an alias.

    Args:
        client: The async Elasticsearch/OpenSearch client.
        alias: The index alias to search against.
        doc_id: The document ID to check for existence.

    Returns:
        bool: True if the item exists in any index under the alias, False otherwise.
    """
    resp = await client.search(
        index=alias,
        body={
            "query": {"ids": {"values": [doc_id]}},
            "_source": False,
        },
        size=0,
        terminate_after=1,
    )
    return bool(resp["hits"]["total"]["value"])


def check_item_exists_in_alias_sync(client: Any, alias: str, doc_id: str) -> bool:
    """Check if an item exists across all indexes for an alias (sync).

    Args:
        client: The sync Elasticsearch/OpenSearch client.
        alias: The index alias to search against.
        doc_id: The document ID to check for existence.

    Returns:
        bool: True if the item exists in any index under the alias, False otherwise.
    """
    resp = client.search(
        index=alias,
        body={
            "query": {"ids": {"values": [doc_id]}},
            "_source": False,
        },
        size=0,
        terminate_after=1,
    )
    return bool(resp["hits"]["total"]["value"])


def add_bbox_shape_to_collection(collection: dict[str, Any]) -> bool:
    """Add bbox_shape field to a collection document for spatial queries.

    This function extracts the bounding box from a collection's spatial extent
    and converts it to a GeoJSON polygon shape that can be used for geospatial
    queries in Elasticsearch/OpenSearch.

    Args:
        collection: Collection document dictionary to modify in-place.

    Returns:
        bool: True if bbox_shape was added, False if it was skipped (already exists,
            no spatial extent, or invalid bbox).

    Notes:
        - Modifies the collection dictionary in-place by adding a 'bbox_shape' field
        - Handles both 2D [minx, miny, maxx, maxy] and 3D [minx, miny, minz, maxx, maxy, maxz] bboxes
        - Uses the first bbox if multiple are present in the collection
        - Logs warnings for collections with invalid or missing bbox data
    """
    collection_id = collection.get("id", "unknown")

    # Check if bbox_shape already exists
    if "bbox_shape" in collection:
        logger.debug(
            f"Collection '{collection_id}' already has bbox_shape field, skipping"
        )
        return False

    # Check if collection has spatial extent
    if "extent" not in collection or "spatial" not in collection["extent"]:
        logger.warning(f"Collection '{collection_id}' has no spatial extent, skipping")
        return False

    spatial_extent = collection["extent"]["spatial"]
    if "bbox" not in spatial_extent or not spatial_extent["bbox"]:
        logger.warning(
            f"Collection '{collection_id}' has no bbox in spatial extent, skipping"
        )
        return False

    # Get the first bbox (collections can have multiple bboxes, but we use the first one)
    bbox = (
        spatial_extent["bbox"][0]
        if isinstance(spatial_extent["bbox"][0], list)
        else spatial_extent["bbox"]
    )

    if len(bbox) < 4:
        logger.warning(
            f"Collection '{collection_id}': bbox has insufficient coordinates (length={len(bbox)}), expected at least 4"
        )
        return False

    # Extract 2D coordinates (bbox can be 2D [minx, miny, maxx, maxy] or 3D [minx, miny, minz, maxx, maxy, maxz])
    # For 2D polygon, we only need the x,y coordinates and discard altitude (z) values
    minx, miny = bbox[0], bbox[1]
    if len(bbox) == 4:
        # 2D bbox: [minx, miny, maxx, maxy]
        maxx, maxy = bbox[2], bbox[3]
    else:
        # 3D bbox: [minx, miny, minz, maxx, maxy, maxz]
        # Extract indices 3,4 for maxx,maxy - discarding altitude at indices 2 (minz) and 5 (maxz)
        maxx, maxy = bbox[3], bbox[4]

    # Convert bbox to GeoJSON polygon
    bbox_polygon_coords = bbox2polygon(minx, miny, maxx, maxy)
    collection["bbox_shape"] = {
        "type": "Polygon",
        "coordinates": bbox_polygon_coords,
    }

    logger.debug(f"Collection '{collection_id}': Added bbox_shape field")
    return True


def validate_refresh(value: str | bool) -> str:
    """
    Validate the `refresh` parameter value.

    Args:
        value (str | bool): The `refresh` parameter value, which can be a string or a boolean.

    Returns:
        str: The validated value of the `refresh` parameter, which can be "true", "false", or "wait_for".
    """
    logger = logging.getLogger(__name__)

    # Handle boolean-like values using get_bool_env
    if isinstance(value, bool) or value in {
        "true",
        "false",
        "1",
        "0",
        "yes",
        "no",
        "y",
        "n",
    }:
        is_true = get_bool_env("DATABASE_REFRESH", default=value)
        return "true" if is_true else "false"

    # Normalize to lowercase for case-insensitivity
    value = value.lower()

    # Handle "wait_for" explicitly
    if value == "wait_for":
        return "wait_for"

    # Log a warning for invalid values and default to "false"
    logger.warning(
        f"Invalid value for `refresh`: '{value}'. Expected 'true', 'false', or 'wait_for'. Defaulting to 'false'."
    )
    return "false"


def merge_to_operations(data: dict) -> list:
    """Convert merge operation to list of RF6902 operations.

    Args:
        data: dictionary to convert.

    Returns:
        list: list of RF6902 operations.
    """
    operations = []

    for key, value in data.copy().items():

        if value is None:
            operations.append(PatchRemove(op="remove", path=key))

        elif isinstance(value, dict):
            nested_operations = merge_to_operations(value)

            for nested_operation in nested_operations:
                nested_operation.path = f"{key}/{nested_operation.path}"
                operations.append(nested_operation)

        else:
            operations.append(PatchAddReplaceTest(op="add", path=key, value=value))

    return operations


def check_commands(
    commands: ESCommandSet,
    op: str,
    path: ElasticPath,
    from_path: bool = False,
    create_nest: bool = False,
) -> None:
    """Add Elasticsearch checks to operation.

    Args:
        commands (List[str]): current commands
        op (str): the operation of script
        path (Dict): path of variable to run operation on
        from_path (bool): True if path is a from path

    """
    if path.nest:
        part_nest = ""
        for index, path_part in enumerate(path.parts):

            # Create nested dictionaries if not present for merge operations
            if create_nest and not from_path:
                value = "[:]"
                for sub_part in reversed(path.parts[index + 1 :]):
                    value = f"['{sub_part}': {value}]"

                commands.add(
                    f"if (!ctx._source{part_nest}.containsKey('{path_part}'))"
                    f"{{ctx._source{part_nest}['{path_part}'] = {value};}}"
                    f"{'' if index == len(path.parts) - 1 else' else '}"  # noqa: E275
                )

            else:
                commands.add(
                    f"if (!ctx._source{part_nest}.containsKey('{path_part}'))"
                    f"{{Debug.explain('{path_part} in {path.path} does not exist');}}"
                )

            part_nest += f"['{path_part}']"

    if from_path or op in ["remove", "replace", "test"]:

        if isinstance(path.key, int):
            commands.add(
                f"if ((ctx._source{path.es_nest} instanceof ArrayList"
                f" && ctx._source{path.es_nest}.size() < {abs(path.key)})"
                f" || (!(ctx._source{path.es_nest} instanceof ArrayList)"
                f" && !ctx._source{path.es_nest}.containsKey('{path.key}')))"
                f"{{Debug.explain('{path.key} does not exist in {path.nest}');}}"  # noqa: E713
            )
        else:
            commands.add(
                f"if (!ctx._source{path.es_nest}.containsKey('{path.key}'))"
                f"{{Debug.explain('{path.key} does not exist in {path.nest}');}}"  # noqa: E713
            )


def remove_commands(commands: ESCommandSet, path: ElasticPath) -> None:
    """Remove value at path.

    Args:
        commands (List[str]): current commands
        path (ElasticPath): Path to value to be removed

    """
    commands.add(f"def {path.variable_name};")
    if isinstance(path.key, int):
        commands.add(
            f"if (ctx._source{path.es_nest} instanceof ArrayList)"
            f"{{{path.variable_name} = ctx._source{path.es_nest}.remove({path.es_key});}} else "
        )

    commands.add(
        f"{path.variable_name} = ctx._source{path.es_nest}.remove('{path.key}');"
    )


def add_commands(
    commands: ESCommandSet,
    operation: PatchOperation,
    path: ElasticPath,
    from_path: ElasticPath,
    params: dict,
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
            else f"ctx._source{from_path.es_path}"
        )

    else:
        value = f"params.{path.param_key}"
        params[path.param_key] = operation.value

    if isinstance(path.key, int):
        commands.add(
            f"if (ctx._source{path.es_nest} instanceof ArrayList)"
            f"{{ctx._source{path.es_nest}.{'add' if operation.op in ['add', 'move'] else 'set'}({path.es_key}, {value});}}"
            f" else ctx._source{path.es_nest}['{path.es_key}'] = {value};"
        )

    else:
        commands.add(f"ctx._source{path.es_path} = {value};")


def test_commands(
    commands: ESCommandSet, operation: PatchOperation, path: ElasticPath, params: dict
) -> None:
    """Test value at path.

    Args:
        commands (List[str]): current commands
        operation (PatchOperation): operation to run
        path (ElasticPath): path for value to be tested
    """
    value = f"params.{path.param_key}"
    params[path.param_key] = operation.value

    if isinstance(path.key, int):
        commands.add(
            f"if (ctx._source{path.es_nest} instanceof ArrayList)"
            f"{{if (ctx._source{path.es_nest}[{path.es_key}] != {value})"
            f"{{Debug.explain('Test failed `{path.path}`"
            f" != ' + ctx._source{path.es_path});}}"
            f"}} else "
        )

    commands.add(
        f"if (ctx._source{path.es_path} != {value})"
        f"{{Debug.explain('Test failed `{path.path}`"
        f" != ' + ctx._source{path.es_path});}}"
    )


def operations_to_script(operations: list, create_nest: bool = False) -> dict:
    """Convert list of operation to painless script.

    Args:
        operations: List of RF6902 operations.

    Returns:
        dict: elasticsearch update script.
    """
    commands: ESCommandSet = ESCommandSet()
    params: dict = {}

    for operation in operations:
        path = ElasticPath(path=operation.path)
        from_path = (
            ElasticPath(path=operation.from_) if hasattr(operation, "from_") else None
        )

        check_commands(
            commands=commands, op=operation.op, path=path, create_nest=create_nest
        )
        if from_path is not None:
            check_commands(
                commands=commands,
                op=operation.op,
                path=from_path,
                from_path=True,
                create_nest=create_nest,
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


def sentry_initialize(
    dsn: str,
    environment: str = "production",
    traces_sample_rate: float = 1.0,
    **kwargs,
) -> None:
    """
    Initialize Sentry SDK for error and performance monitoring.

    Args:
        dsn: Data Source Name - The Sentry project DSN URL
        environment: Deployment environment (e.g., "production", "staging", "development")
        traces_sample_rate: Sample rate for performance traces (0.0 to 1.0)
        Additional Sentry SDK configuration parameters.
    """
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration

    sentry_config = {
        "dsn": dsn,
        "environment": environment,
        "traces_sample_rate": traces_sample_rate,
        "integrations": [FastApiIntegration()],
    }
    sentry_config.update(kwargs)

    sentry_sdk.init(**sentry_config)

    logger.info(f"Sentry initialized for environment: {environment}")


def add_hidden_filter(
    query: dict[str, Any] | None = None, hide_item_path: str | None = None
) -> dict[str, Any]:
    """Add hidden filter to a query to exclude hidden items.

    Args:
        query: Elasticsearch query to combine with hidden filter
        hide_item_path: Path to the hidden field (e.g., "properties._private.hidden")
                       If None or empty, return original query (no filtering)

    Returns:
        Query with hidden filter applied
    """
    if not hide_item_path:
        return query or {"match_all": {}}

    hidden_filter = {
        "bool": {
            "should": [
                {"term": {hide_item_path: False}},
                {"bool": {"must_not": {"exists": {"field": hide_item_path}}}},
            ],
            "minimum_should_match": 1,
        }
    }

    if query:
        return {"bool": {"must": [query, hidden_filter]}}
    else:
        return hidden_filter
