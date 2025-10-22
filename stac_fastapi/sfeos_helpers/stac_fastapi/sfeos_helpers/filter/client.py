"""Filter client implementation for Elasticsearch/OpenSearch."""

import os
from collections import deque
from typing import Any, Optional

import attr
from fastapi import Request

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.extensions.filter import ALL_QUERYABLES, DEFAULT_QUERYABLES
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient
from stac_fastapi.sfeos_helpers.mappings import ES_MAPPING_TYPE_TO_JSON


@attr.s
class EsAsyncBaseFiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    database: BaseDatabaseLogic = attr.ib()

    @staticmethod
    def _get_excluded_from_queryables() -> set[str]:
        """Get fields to exclude from queryables endpoint and filtering.

        Reads from EXCLUDED_FROM_QUERYABLES environment variable.
        Supports comma-separated list of field names.

        Example:
            EXCLUDED_FROM_QUERYABLES="auth:schemes,storage:schemes"

        Returns:
            Set[str]: Set of field names to exclude from queryables
        """
        excluded = os.getenv("EXCLUDED_FROM_QUERYABLES", "")
        if not excluded:
            return set()
        return {field.strip() for field in excluded.split(",") if field.strip()}

    async def get_queryables(
        self,
        collection_id: Optional[str] = None,  # noqa: UP045
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables

        Args:
            collection_id (str, optional): The id of the collection to get queryables for.
            **kwargs: additional keyword arguments

        Returns:
            Dict[str, Any]: A dictionary containing the queryables for the given collection.
        """
        request: Optional[Request] = kwargs.get("request")  # noqa: UP045
        url_str = str(request.url) if request else ""

        queryables: dict[str, Any] = {
            "$schema": "https://json-schema.org/draft-07/schema",
            "$id": url_str,
            "type": "object",
            "title": "Queryables for STAC API",
            "description": "Queryable names for the STAC API Item Search filter.",
            "properties": DEFAULT_QUERYABLES,
            "additionalProperties": True,
        }

        if not collection_id:
            return queryables

        properties = queryables["properties"].copy()
        queryables.update(
            {
                "properties": properties,
                "additionalProperties": False,
            }
        )

        mapping_data = await self.database.get_items_mapping(collection_id)
        mapping_properties = next(iter(mapping_data.values()))["mappings"]["properties"]
        stack: deque[tuple[str, dict[str, Any]]] = deque(mapping_properties.items())
        enum_fields: dict[str, dict[str, Any]] = {}
        excluded_fields = self._get_excluded_from_queryables()

        while stack:
            field_fqn, field_def = stack.popleft()

            # Iterate over nested fields
            field_properties = field_def.get("properties")
            if field_properties:
                stack.extend(
                    (f"{field_fqn}.{k}", v)
                    for k, v in field_properties.items()
                    if v.get("enabled", True)
                    and f"{field_fqn}.{k}" not in excluded_fields
                )

            # Skip non-indexed or disabled fields
            field_type = field_def.get("type")
            if (
                not field_type
                or not field_def.get("enabled", True)
                or field_fqn in excluded_fields
            ):
                continue

            # Fields in Item Properties should be exposed with their un-prefixed names,
            # and not require expressions to prefix them with properties,
            # e.g., eo:cloud_cover instead of properties.eo:cloud_cover.
            field_name = field_fqn.removeprefix("properties.")

            # Generate field properties
            field_result = ALL_QUERYABLES.get(field_name, {}).copy()
            properties[field_name] = field_result

            field_name_human = field_name.replace("_", " ").title()
            field_result.setdefault("title", field_name_human)

            field_type_json = ES_MAPPING_TYPE_TO_JSON.get(field_type, field_type)
            field_result.setdefault("type", field_type_json)

            if field_type in {"date", "date_nanos"}:
                field_result.setdefault("format", "date-time")

            if field_result.pop("$enum", False):
                enum_fields[field_fqn] = field_result

        if enum_fields:
            unique_values = await self.database.get_items_unique_values(
                collection_id, enum_fields
            )
            for field_fqn, values in unique_values.items():
                enum_fields[field_fqn]["enum"] = values

        return queryables
