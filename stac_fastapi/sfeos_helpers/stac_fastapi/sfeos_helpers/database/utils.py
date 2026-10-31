"""Utility functions for database operations in Elasticsearch/OpenSearch.

This module provides utility functions for working with database operations
in Elasticsearch/OpenSearch, such as parameter validation.
"""

import asyncio
import logging
import os
from functools import wraps
from typing import Any, Callable, Iterator

from stac_fastapi.core.utilities import bbox2polygon, get_bool_env
from stac_fastapi.extensions.transaction.request import (
    PatchAddReplaceTest,
    PatchOperation,
    PatchRemove,
)
from stac_fastapi.sfeos_helpers.models.patch import ElasticPath, ESCommandSet
from stac_fastapi.types.errors import ConflictError, NotFoundError

try:
    from opensearchpy.exceptions import ConnectionError, ConnectionTimeout
except ImportError:
    from elasticsearch.exceptions import ConnectionError, ConnectionTimeout

from tenacity import (
    AsyncRetrying,
    before_sleep_log,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

logger = logging.getLogger(__name__)

RETRY_MAX_ATTEMPTS_CONNECTION_ERROR = int(
    os.getenv("RETRY_MAX_ATTEMPTS_CONNECTION_ERROR", "5")
)
RETRY_MAX_ATTEMPTS_NOT_FOUND_ERROR = int(
    os.getenv("RETRY_MAX_ATTEMPTS_NOT_FOUND_ERROR", "3")
)
RETRY_WAIT_SECONDS = float(os.getenv("RETRY_WAIT_SECONDS", "0.5"))
RETRY_RERAISE = get_bool_env("RETRY_RERAISE", default=True)

CONNECTION_RETRY_STRATEGY = AsyncRetrying(
    stop=stop_after_attempt(RETRY_MAX_ATTEMPTS_CONNECTION_ERROR),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=retry_if_exception_type((ConnectionError, ConnectionTimeout)),
    reraise=RETRY_RERAISE,
    before_sleep=before_sleep_log(logger, logging.WARNING),
)

DATETIME_RETRY_STRATEGY = AsyncRetrying(
    stop=stop_after_attempt(RETRY_MAX_ATTEMPTS_NOT_FOUND_ERROR),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=retry_if_exception(
        lambda e: isinstance(e, NotFoundError)
        and "Collections '" in str(e)
        and "' do not exist" in str(e)
    ),
    reraise=RETRY_RERAISE,
    before_sleep=before_sleep_log(logger, logging.WARNING),
)


def separate_bulk_conflict_errors(
    errors: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Separate 409 conflict errors from other errors in bulk operation results.

    Args:
        errors: List of error dicts from ES/OS bulk helpers.

    Returns:
        Tuple of (conflict_errors, other_errors).
    """
    conflict_errors = []
    other_errors = []
    for error in errors:
        action_data: dict[str, Any] = next(iter(error.values()), {})
        if action_data.get("status") == 409:
            conflict_errors.append(error)
        else:
            other_errors.append(error)
    return conflict_errors, other_errors


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


class BulkIndexError(Exception):
    """Error raised when non-conflict errors occur during a bulk indexing operation.

    Raised in strict mode (RAISE_ON_BULK_ERROR=true) when the bulk operation
    encounters errors other than 409 conflicts (e.g., mapping errors, connection
    errors). Conflict errors are handled separately as skips or
    ItemAlreadyExistsError.

    Attributes:
        errors: The list of non-conflict error dicts from the bulk operation.
        collection_id: The ID of the collection being indexed.
    """

    def __init__(self, errors: list[dict[str, Any]], collection_id: str):
        """Initialize the error with the list of errors and collection ID."""
        self.errors = errors
        self.collection_id = collection_id
        message = (
            f"Bulk indexing for collection {collection_id} encountered "
            f"{len(errors)} non-conflict error(s): {errors}"
        )
        super().__init__(message)


async def _alias_index_names(client: Any, alias: str) -> list[str]:
    """Return the concrete index names behind an alias.

    Args:
        client: The async Elasticsearch/OpenSearch client.
        alias: The index alias to resolve.

    Returns:
        list[str]: The concrete index names carrying the alias.

    Raises:
        The search engine's NotFoundError when no index carries the alias.
    """
    resp = await client.indices.get_alias(name=alias)
    return list(resp.keys())


def _alias_index_names_sync(client: Any, alias: str) -> list[str]:
    """Return the concrete index names behind an alias (sync).

    Args:
        client: The sync Elasticsearch/OpenSearch client.
        alias: The index alias to resolve.

    Returns:
        list[str]: The concrete index names carrying the alias.

    Raises:
        The search engine's NotFoundError when no index carries the alias.
    """
    resp = client.indices.get_alias(name=alias)
    return list(resp.keys())


async def find_item_in_alias(
    client: Any, alias: str, doc_id: str
) -> dict[str, Any] | None:
    """Real-time lookup of a document across all indexes under an alias.

    Uses ``mget`` over the alias's concrete indexes, which is real-time and
    therefore sees documents that have not been refreshed yet. This matters
    for duplicate checks and update validation when ``DATABASE_REFRESH`` is
    false.

    Args:
        client: The async Elasticsearch/OpenSearch client.
        alias: The index alias to look the document up under.
        doc_id: The document ID to look up.

    Returns:
        dict | None: The matching ``mget`` hit (carrying ``_index``, ``_id``,
            ``_source``, ...) if found, None otherwise.

    Raises:
        The search engine's NotFoundError when no index carries the alias.

    Note:
        This is a check-then-act: a concurrent write can insert the same
        ``_id`` into another index after this returns but before the caller's
        write lands. The search engine cannot enforce uniqueness across
        indexes, so closing that window entirely would require external
        locking.
    """
    resp = await client.mget(
        body={
            "docs": [
                {"_index": index, "_id": doc_id}
                for index in await _alias_index_names(client, alias)
            ]
        }
    )
    for doc in resp.get("docs", []):
        if doc.get("found") and doc.get("_source") is not None:
            return doc
    return None


async def check_item_exists_in_alias(client: Any, alias: str, doc_id: str) -> bool:
    """Check if an item exists across all indexes for an alias.

    Uses ``mget`` over the alias's concrete indexes so the check is real-time
    and sees documents that have not been refreshed yet.

    Args:
        client: The async Elasticsearch/OpenSearch client.
        alias: The index alias to check against.
        doc_id: The document ID to check for existence.

    Returns:
        bool: True if the item exists in any index under the alias, False otherwise.

    Raises:
        The search engine's NotFoundError when no index carries the alias.
    """
    resp = await client.mget(
        body={
            "docs": [
                {"_index": index, "_id": doc_id, "_source": False}
                for index in await _alias_index_names(client, alias)
            ]
        }
    )
    return any(doc.get("found") for doc in resp.get("docs", []))


def check_item_exists_in_alias_sync(client: Any, alias: str, doc_id: str) -> bool:
    """Check if an item exists across all indexes for an alias (sync).

    Uses ``mget`` over the alias's concrete indexes so the check is real-time
    and sees documents that have not been refreshed yet.

    Args:
        client: The sync Elasticsearch/OpenSearch client.
        alias: The index alias to check against.
        doc_id: The document ID to check for existence.

    Returns:
        bool: True if the item exists in any index under the alias, False otherwise.

    Raises:
        The search engine's NotFoundError when no index carries the alias.
    """
    resp = client.mget(
        body={
            "docs": [
                {"_index": index, "_id": doc_id, "_source": False}
                for index in _alias_index_names_sync(client, alias)
            ]
        }
    )
    return any(doc.get("found") for doc in resp.get("docs", []))


async def find_existing_item_ids(
    client: Any, alias: str, doc_ids: list[str]
) -> set[str]:
    """Return the subset of document IDs that exist in any index under an alias.

    Used to detect duplicates across all physical indexes of a collection
    (e.g., datetime-sharded indexes), where ``_id`` uniqueness enforced by the
    search engine only applies within a single index.

    Uses per-index ``mget`` calls, which are real-time (seeing unrefreshed
    documents) and not subject to ``max_result_window``, so arbitrarily large
    batches are safe. The per-index lookups run concurrently.

    Args:
        client: The async Elasticsearch/OpenSearch client.
        alias: The index alias to look the documents up under.
        doc_ids: The document IDs to look up.

    Returns:
        set[str]: The document IDs found under the alias.

    Raises:
        The search engine's NotFoundError when no index carries the alias.

    Note:
        This is a check-then-act: a concurrent write can insert the same
        ``_id`` into another index after this returns but before the caller's
        write lands. The search engine cannot enforce uniqueness across
        indexes, so closing that window entirely would require external
        locking.
    """
    if not doc_ids:
        return set()
    doc_ids = list(dict.fromkeys(doc_ids))
    indices = await _alias_index_names(client, alias)

    async def _find_in_index(index: str) -> set[str]:
        resp = await client.mget(
            body={
                "docs": [
                    {"_index": index, "_id": doc_id, "_source": False}
                    for doc_id in doc_ids
                ]
            }
        )
        return {doc["_id"] for doc in resp.get("docs", []) if doc.get("found")}

    existing: set[str] = set()
    for found_ids in await asyncio.gather(
        *(_find_in_index(index) for index in indices)
    ):
        existing.update(found_ids)
    return existing


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


def validate_datetime_operations(
    operations: list, existing_source: dict, validator: Callable
) -> None:
    """Validate datetime field changes in patch operations.

    Compares operation values against existing document values.
    Only validates when the value actually changes.

    Args:
        operations: List of patch operations.
        existing_source: The existing document source from the search response.
        validator: Callable that validates a field path.
    """
    for operation in operations:
        if operation.op == "remove":
            validator(operation.path.strip("/"))
        elif operation.op in ["add", "replace"]:
            path_parts = operation.path.strip("/").split("/")
            existing_value = existing_source
            for part in path_parts:
                if isinstance(existing_value, dict):
                    existing_value = existing_value.get(part)
                else:
                    existing_value = None
                    break
            if existing_value != operation.value:
                validator(operation.path.strip("/"))


def protect_datetime_script(script: dict, use_datetime: bool) -> None:
    """Reject changed index dates atomically against the actual script effects."""
    key = "__stac_datetime"
    while key in script["source"] or key in script["params"]:
        key += "_"
    script["params"][key] = (
        ["datetime"] if use_datetime else ["start_datetime", "end_datetime"]
    )
    properties = "ctx._source.properties instanceof Map ? ctx._source.properties : [:]"
    script["source"] = (
        f"def {key} = [:]; def {key}Props = {properties};"
        f"for (def {key}Field : params.{key}) {{"
        f"{key}[{key}Field] = [{key}Props.containsKey({key}Field),"
        f"{key}Props.get({key}Field)];}}"
        + script["source"]
        + f"{key}Props = {properties};"
        f"for (def {key}Field : params.{key}) {{"
        f"if (!{key}[{key}Field].equals([{key}Props.containsKey({key}Field),"
        f"{key}Props.get({key}Field)])) {{throw new IllegalArgumentException("
        "'Updating datetime fields is not yet supported for datetime-based indexing');}}"
    )


def merge_to_operations(data: dict) -> list:
    """Convert merge operation to list of RF6902 operations.

    Args:
        data: dictionary to convert.

    Returns:
        list: list of RF6902 operations.
    """
    operations = []

    for key, value in data.copy().items():
        key = key.replace("~", "~0").replace("/", "~1")

        if value is None:
            operations.append(PatchRemove(op="remove", path=key))

        elif isinstance(value, dict) and value:
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


def _allocate_script_param(params: dict, value: Any) -> str:
    """Add a value to the script params under the next available local key."""
    index = 0
    while f"p{index}" in params:
        index += 1

    key = f"p{index}"
    params[key] = value
    return key


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
        param_key = _allocate_script_param(params, operation.value)
        value = f"params.{param_key}"

    if isinstance(path.key, int):
        commands.add(
            f"if (ctx._source{path.es_nest} instanceof ArrayList)"
            f"{{ctx._source{path.es_nest}.{'add' if operation.op in ['add', 'copy', 'move'] else 'set'}({path.es_key}, {value});}}"
            f" else ctx._source{path.es_nest}['{path.raw_key}'] = {value};"
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
    param_key = _allocate_script_param(params, operation.value)
    value = f"params.{param_key}"

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
    if create_nest:
        # Merge members are literal string keys, never array indexes or script text.
        return {
            "source": """
                for (def operation : params.operations) {
                    def target = ctx._source;
                    def parts = operation.path;
                    for (int i = 0; i < parts.size() - 1; i++) {
                        def key = parts[i];
                        if (!(target[key] instanceof Map)) {
                            target[key] = [:];
                        }
                        target = target[key];
                    }
                    def key = parts[parts.size() - 1];
                    if (operation.op == 'remove') {
                        target.remove(key);
                    } else if (operation.value instanceof Map && operation.value.isEmpty()) {
                        if (!(target[key] instanceof Map)) {
                            target[key] = [:];
                        }
                    } else {
                        target[key] = operation.value;
                    }
                }
            """,
            "lang": "painless",
            "params": {
                "operations": [
                    {
                        "op": operation.op,
                        "path": [
                            part.replace("~1", "/").replace("~0", "~")
                            for part in operation.path.split("/")
                        ],
                        "value": getattr(operation, "value", None),
                    }
                    for operation in operations
                ]
            },
        }

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


def retry_on_datetime_not_found(func) -> Callable:
    """Retries datetime search queries on NotFoundError.

    Args:
        func: The async function to be decorated.

    Returns:
        A decorated function with retry logic for datetime-based searches.

    Note:
        Retry logic is only applied when `datetime_search` is provided as a
        non-empty dictionary. For non-datetime queries or empty datetime
        dictionaries, the function executes without retry attempts.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        from stac_fastapi.sfeos_helpers.search_engine.inserters import (
            DatetimeIndexInserter,
        )

        datetime_search = kwargs.get("datetime_search")
        if (
            isinstance(self.async_index_inserter, DatetimeIndexInserter)
            and datetime_search
        ):
            async for attempt in DATETIME_RETRY_STRATEGY.copy():
                with attempt:
                    if attempt.retry_state.attempt_number > 1:
                        await self.async_index_inserter.refresh_cache()
                    return await func(self, *args, **kwargs)

        return await func(self, *args, **kwargs)

    return wrapper


def retry_on_connection_error(func) -> Callable:
    """Retries on ConnectionError, ConnectionTimeout etc.

    Args:
        func: The async function to be decorate.

    Returns:
        A decorated function with retry logic for connection errors .
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        async for attempt in CONNECTION_RETRY_STRATEGY.copy():
            with attempt:
                return await func(*args, **kwargs)

    return wrapper


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


def _values(node: Any, parts: list[str]) -> Iterator[Any]:
    """Yield every leaf value the engine indexes at a dotted path.

    Arrays are flattened at any depth, and every spelling of the path counts:
    nested objects, flat dotted keys, and mixes of the two.
    """
    if isinstance(node, list):
        for element in node:
            yield from _values(element, parts)
    elif not parts:
        yield node
    elif isinstance(node, dict):
        for i in range(1, len(parts) + 1):
            key = ".".join(parts[:i])
            if key in node:
                yield from _values(node[key], parts[i:])


def is_hidden(source: dict[str, Any], hide_item_path: str | None) -> bool:
    """Tell whether a stored document is hidden, as `add_hidden_filter` decides it.

    Visible when the field has no value, or when any of its values is false
    (`false`, `"false"` or `""`, which a boolean mapping coerces to false; a
    term query matches any indexed value). Hidden otherwise.
    """
    if not hide_item_path:
        return False
    values = [v for v in _values(source, hide_item_path.split(".")) if v is not None]
    return bool(values) and not any(v is False or v in ("false", "") for v in values)
