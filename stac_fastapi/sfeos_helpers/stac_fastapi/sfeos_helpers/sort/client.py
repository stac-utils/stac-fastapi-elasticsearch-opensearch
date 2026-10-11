"""Sortables client implementation for Elasticsearch/OpenSearch backends."""

import logging
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

SORTABLES_JSON_SCHEMA = "https://json-schema.org/draft/2020-12/schema"

# Engine field types that can appear in a sort clause. `text` fields are handled
# separately: they are sortable only through a keyword multi-field, which
# remap_sort_field_shared resolves transparently (e.g. title -> title.keyword).
SORTABLE_FIELD_TYPES = frozenset(
    {
        "alias",
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

        if is_excluded(field_fqn) or not _is_sortable(field_def):
            continue

        sortables[field_fqn] = _to_property_schema(field_fqn, field_def.get("type"))

    return sortables


@attr.s
class EsAsyncBaseSortablesClient(AsyncBaseSortablesClient):
    """Elasticsearch/OpenSearch implementation of the Sortables endpoints."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()

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

    @staticmethod
    def _mapping_properties(mappings: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Merge the properties of every index in a get_mapping response."""
        properties: dict[str, dict[str, Any]] = {}
        for index_mapping in mappings.values():
            for name, field_def in (
                index_mapping.get("mappings", {}).get("properties", {}).items()
            ):
                properties.setdefault(name, field_def)
        return properties

    async def get_sortables(
        self, request: Request | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Get the sortables available for item search.

        Returns the core STAC sort fields. When ROOT_QUERYABLES_UNION is
        enabled, also unions every collection's sortable fields.
        """
        if not self.settings.root_queryables_union:
            return self._sortables_schema(
                request, dict(DEFAULT_SORTABLES), additional_properties=True
            )

        collection_ids = (
            await self.database.async_index_selector.get_all_collection_ids()
        ) or []
        properties = dict(DEFAULT_SORTABLES)
        for collection_id in collection_ids:
            try:
                mappings = await self.database.get_items_mapping(collection_id)
            except NotFoundError:
                continue
            for name, field_schema in get_sortable_properties_shared(
                self._mapping_properties(mappings)
            ).items():
                properties.setdefault(name, field_schema)
        return self._sortables_schema(request, properties, additional_properties=False)

    async def get_collection_sortables(
        self, collection_id: str, request: Request | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Get the sortables available for a collection's items."""
        mappings = await self.database.get_items_mapping(collection_id)
        properties = get_sortable_properties_shared(self._mapping_properties(mappings))
        return self._sortables_schema(request, properties, additional_properties=False)

    async def get_collections_sortables(
        self, request: Request | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        """Get the sortables available for collection search."""
        response = await self.database.client.indices.get_mapping(
            index=COLLECTIONS_INDEX
        )
        properties = get_sortable_properties_shared(
            self._mapping_properties(getattr(response, "body", response))
        )
        return self._sortables_schema(request, properties, additional_properties=False)
