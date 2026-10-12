"""Sortables client implementation for Elasticsearch/OpenSearch backends."""

import asyncio
import logging
import os
import time
from collections import deque
from typing import Any

import attr
from starlette.requests import Request

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.extensions.sort.client import AsyncBaseSortablesClient
from stac_fastapi.sfeos_helpers.database.mapping import get_excluded_from_queryables
from stac_fastapi.sfeos_helpers.mappings import (
    COLLECTIONS_INDEX,
    ES_MAPPING_TYPE_TO_JSON,
)
from stac_fastapi.types.errors import NotFoundError

logger = logging.getLogger(__name__)

# Engine NotFoundError classes. Either or both backends may be installed, so
# collect every available variant rather than picking one by import order.
_ENGINE_NOT_FOUND_ERRORS: tuple[type[Exception], ...] = ()
try:
    from elasticsearch.exceptions import NotFoundError as _EsNotFoundError

    _ENGINE_NOT_FOUND_ERRORS += (_EsNotFoundError,)
except ImportError:
    pass
try:
    from opensearchpy.exceptions import NotFoundError as _OsNotFoundError

    _ENGINE_NOT_FOUND_ERRORS += (_OsNotFoundError,)
except ImportError:
    pass

SORTABLES_JSON_SCHEMA = "https://json-schema.org/draft/2020-12/schema"

# Engine field types that can appear in a sort clause. `text` fields are handled
# separately: they are sortable only through a keyword multi-field, which
# remap_sort_field_shared resolves transparently (e.g. title -> title.keyword).
SORTABLE_FIELD_TYPES = frozenset(
    {
        "boolean",
        "byte",
        "constant_keyword",
        "date",
        "date_nanos",
        "double",
        "float",
        "half_float",
        "integer",
        "ip",
        "keyword",
        "long",
        "match_only_text",
        "scaled_float",
        "short",
        "unsigned_long",
        "version",
        "wildcard",
    }
)

# Sortable STAC core fields, mirroring DEFAULT_QUERYABLES minus geometry.
# Names are document paths, as the sortby parameter expects them.
DEFAULT_SORTABLES: dict[str, dict[str, Any]] = {
    "id": {"type": "string", "title": "Id"},
    "collection": {"type": "string", "title": "Collection"},
    "properties.datetime": {
        "type": "string",
        "format": "date-time",
        "title": "Datetime",
    },
    "properties.created": {
        "type": "string",
        "format": "date-time",
        "title": "Created",
    },
    "properties.updated": {
        "type": "string",
        "format": "date-time",
        "title": "Updated",
    },
}


def _is_sortable(field_def: dict[str, Any]) -> bool:
    """Check whether a mapped field can appear in an engine sort clause."""
    if not field_def.get("enabled", True):
        return False
    field_type = field_def.get("type")
    if field_type == "text":
        # Mirrors _detect_keyword_sort_remaps_from_mapping: a text field sorts
        # through a keyword multi-field under its public field name.
        return any(
            isinstance(sub_def, dict) and sub_def.get("type") == "keyword"
            for sub_def in field_def.get("fields", {}).values()
        )
    return field_type in SORTABLE_FIELD_TYPES


def _to_property_schema(field_fqn: str, field_type: str | None) -> dict[str, Any]:
    """Build the sortables JSON Schema fragment for a field."""
    schema: dict[str, Any] = {
        "title": field_fqn.removeprefix("properties.")
        .removeprefix("assets.")
        .replace("_", " ")
        .replace("-", " ")
        .title()
    }
    if field_type in ES_MAPPING_TYPE_TO_JSON:
        schema["type"] = ES_MAPPING_TYPE_TO_JSON[field_type]
    if field_type in ("date", "date_nanos"):
        schema["format"] = "date-time"
    return schema


def _resolve_alias(
    mapping_properties: dict[str, dict[str, Any]], field_def: dict[str, Any]
) -> dict[str, Any] | None:
    """Resolve an alias field to the field definition its path points at."""
    node: Any = {"properties": mapping_properties}
    for part in str(field_def.get("path", "")).split("."):
        node = node.get("properties", {}).get(part) if isinstance(node, dict) else None
        if node is None:
            return None
    return node if isinstance(node, dict) else None


def get_sortable_properties_shared(
    mapping_properties: dict[str, dict[str, Any]],
    excluded: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Walk index mapping properties and keep the fields an engine can sort on.

    Fields listed in the EXCLUDED_FROM_QUERYABLES environment variable are
    excluded, along with their children. Field names are kept as document paths
    (e.g. properties.datetime) — the same values the sortby parameter accepts.

    Args:
        mapping_properties: the "properties" section of an index mapping.
        excluded: field names to exclude; defaults to EXCLUDED_FROM_QUERYABLES.

    Returns:
        A dict of sortable field names to their JSON Schema fragment.
    """
    if excluded is None:
        excluded = get_excluded_from_queryables()

    def is_excluded(path: str) -> bool:
        """Check if the path starts with any excluded prefix."""
        return any(
            path == prefix or path.startswith(prefix + ".") for prefix in excluded
        )

    sortables: dict[str, dict[str, Any]] = {}
    stack: deque[tuple[str, dict[str, Any]]] = deque(mapping_properties.items())

    while stack:
        field_fqn, field_def = stack.popleft()
        if not isinstance(field_def, dict):
            continue

        nested_properties = field_def.get("properties")
        if isinstance(nested_properties, dict):
            stack.extend(
                (f"{field_fqn}.{k}", v)
                for k, v in nested_properties.items()
                if isinstance(v, dict) and not is_excluded(f"{field_fqn}.{k}")
            )

        if is_excluded(field_fqn) or not field_def.get("enabled", True):
            continue

        if field_def.get("type") == "alias":
            # Sorting on an alias sorts on its target field, so advertise the
            # alias under the target's type only when the target is sortable.
            field_def = _resolve_alias(mapping_properties, field_def)
            if field_def is None:
                continue

        if not _is_sortable(field_def):
            continue

        sortables[field_fqn] = _to_property_schema(field_fqn, field_def.get("type"))

    return sortables


def _mapping_properties(mappings: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Merge the properties of every index in a get_mapping response."""
    properties: dict[str, dict[str, Any]] = {}
    if not isinstance(mappings, dict):
        return properties
    for index_mapping in mappings.values():
        if not isinstance(index_mapping, dict):
            continue
        index_properties = (index_mapping.get("mappings") or {}).get("properties") or {}
        for name, field_def in index_properties.items():
            properties.setdefault(name, field_def)
    return properties


class SortablesCache:
    """A time-based cache for the union of every collection's sortable fields."""

    def __init__(self, database_logic: BaseDatabaseLogic) -> None:
        """Initialize the cache with a TTL from SORTABLES_CACHE_TTL seconds."""
        self._db_logic = database_logic
        self._properties: dict[str, dict[str, Any]] = {}
        self._last_updated: float | None = None
        self._lock = asyncio.Lock()
        self.cache_ttl = int(os.getenv("SORTABLES_CACHE_TTL", "1800"))

    def _is_fresh(self) -> bool:
        """Return True once populated and still within the TTL window."""
        return (
            self._last_updated is not None
            and time.monotonic() - self._last_updated < self.cache_ttl
        )

    async def _update_cache(self) -> None:
        """Rebuild the union of sortable fields across all item indexes."""
        async with self._lock:
            if self._is_fresh():
                return

            collection_ids = (
                await self._db_logic.async_index_selector.get_all_collection_ids()
            ) or []
            properties: dict[str, dict[str, Any]] = {}
            for collection_id in collection_ids:
                try:
                    mappings = await self._db_logic.get_items_mapping(collection_id)
                except NotFoundError:
                    continue
                for name, field_schema in get_sortable_properties_shared(
                    _mapping_properties(mappings)
                ).items():
                    properties.setdefault(name, field_schema)

            self._properties = properties
            self._last_updated = time.monotonic()

    async def get_union_properties(self) -> dict[str, dict[str, Any]]:
        """Return the union of sortable fields across all item indexes."""
        if not self._is_fresh():
            await self._update_cache()
        return self._properties


@attr.s
class EsAsyncBaseSortablesClient(AsyncBaseSortablesClient):
    """Elasticsearch/OpenSearch implementation of the Sortables endpoints."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    sortables_cache: SortablesCache = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        """Create the union sortables cache."""
        self.sortables_cache = SortablesCache(self.database)

    @staticmethod
    def _sortables_schema(
        request: Request | None,
        properties: dict[str, dict[str, Any]],
        *,
        additional_properties: bool,
    ) -> dict[str, Any]:
        """Build a Sortables JSON Schema response."""
        return {
            "$schema": SORTABLES_JSON_SCHEMA,
            "$id": str(request.url) if request is not None else "",
            "type": "object",
            "title": "Sortables",
            "properties": properties,
            "additionalProperties": additional_properties,
        }

    async def get_sortables(
        self, request: Request | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Get the sortables available for item search.

        Returns the core STAC sort fields. When ROOT_QUERYABLES_UNION is
        enabled, also unions every collection's sortable fields. The union is
        cached for SORTABLES_CACHE_TTL seconds (default 1800) since it walks
        every collection's index mapping.
        """
        if not self.settings.root_queryables_union:
            return self._sortables_schema(
                request, dict(DEFAULT_SORTABLES), additional_properties=True
            )

        properties = dict(DEFAULT_SORTABLES)
        for name, field_schema in (
            await self.sortables_cache.get_union_properties()
        ).items():
            properties.setdefault(name, field_schema)
        return self._sortables_schema(request, properties, additional_properties=False)

    async def get_collection_sortables(
        self, collection_id: str, request: Request | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Get the sortables available for a collection's items."""
        mappings = await self.database.get_items_mapping(collection_id)
        properties = get_sortable_properties_shared(_mapping_properties(mappings))
        return self._sortables_schema(request, properties, additional_properties=False)

    async def get_collections_sortables(
        self, request: Request | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Get the sortables available for collection search."""
        try:
            response = await self.database.client.indices.get_mapping(
                index=COLLECTIONS_INDEX
            )
        except _ENGINE_NOT_FOUND_ERRORS as e:
            raise NotFoundError(
                f"Mapping for index {COLLECTIONS_INDEX} not found"
            ) from e
        properties = get_sortable_properties_shared(
            _mapping_properties(getattr(response, "body", response))
        )
        return self._sortables_schema(request, properties, additional_properties=False)
