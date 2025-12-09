"""A module for managing queryable attributes."""

import asyncio
import os
import time
from typing import Any, Dict, List, Set

from fastapi import HTTPException


class QueryablesCache:
    """A thread-safe, time-based cache for queryable properties."""

    def __init__(self, database_logic: Any):
        """
        Initialize the QueryablesCache.

        Args:
            database_logic: An instance of a class with a `get_queryables_mapping` method.
        """
        self._db_logic = database_logic
        self._cache: Dict[str, List[str]] = {}
        self._all_queryables: Set[str] = set()
        self._last_updated: float = 0
        self._lock = asyncio.Lock()
        self.validation_enabled: bool = False
        self.cache_ttl: int = 1800  # How often to refresh cache (in seconds)
        self.reload_settings()

    def reload_settings(self):
        """Reload settings from environment variables."""
        self.validation_enabled = (
            os.getenv("VALIDATE_QUERYABLES", "false").lower() == "true"
        )
        self.cache_ttl = int(os.getenv("QUERYABLES_CACHE_TTL", "1800"))

    async def _update_cache(self):
        """Update the cache with the latest queryables from the database."""
        if not self.validation_enabled:
            return

        async with self._lock:
            if (time.time() - self._last_updated < self.cache_ttl) and self._cache:
                return

            queryables_mapping = await self._db_logic.get_queryables_mapping()
            all_queryables_set = set(queryables_mapping.keys())

            self._all_queryables = all_queryables_set

            self._cache = {"*": list(all_queryables_set)}
            self._last_updated = time.time()

    async def get_all_queryables(self) -> Set[str]:
        """
        Return a set of all queryable attributes across all collections.

        This method will update the cache if it's stale or has been cleared.
        """
        if not self.validation_enabled:
            return set()

        if (time.time() - self._last_updated >= self.cache_ttl) or not self._cache:
            await self._update_cache()
        return self._all_queryables

    async def validate(self, fields: Set[str]) -> None:
        """
        Validate if the provided fields are queryable.

        Raises HTTPException if invalid fields are found.
        """
        if not self.validation_enabled:
            return

        allowed_fields = await self.get_all_queryables()
        invalid_fields = fields - allowed_fields
        if invalid_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query fields: {', '.join(sorted(invalid_fields))}. "
                "These fields are not defined in the collection's queryables. "
                "Use the /queryables endpoint to see available fields.",
            )


def get_properties_from_cql2_filter(cql2_filter: Dict[str, Any]) -> Set[str]:
    """Recursively extract property names from a CQL2 filter.

    Property names are normalized by stripping the 'properties.' prefix
    if present, to match queryables stored without the prefix.
    """
    props: Set[str] = set()
    if "op" in cql2_filter and "args" in cql2_filter:
        for arg in cql2_filter["args"]:
            if isinstance(arg, dict):
                if "op" in arg:
                    props.update(get_properties_from_cql2_filter(arg))
                elif "property" in arg:
                    prop_name = arg["property"]
                    # Strip 'properties.' prefix if present
                    if prop_name.startswith("properties."):
                        prop_name = prop_name[11:]
                    props.add(prop_name)
    return props
