"""Index alias loaders for index selection strategies."""

import logging
from typing import Any, Dict, List, Tuple

from stac_fastapi.sfeos_helpers.database import index_alias_by_collection_id
from stac_fastapi.sfeos_helpers.mappings import ITEMS_INDEX_PREFIX

logger = logging.getLogger(__name__)


class IndexAliasLoader:
    """Asynchronous loader for index aliases."""

    def __init__(self, client: Any):
        """Initialize the async alias loader.

        Args:
            client: Async search engine client instance.
        """
        self.client = client

    async def load_aliases(self) -> Dict[str, List[Tuple[Dict[str, str]]]]:
        """Load index aliases from search engine.

        Returns:
            Dict[str, List[Tuple[Dict[str, str]]]]: Mapping of main collection aliases to their data.
        """
        response = await self.client.indices.get_alias(index=f"{ITEMS_INDEX_PREFIX}*")
        return self._parse_alias_response(response)

    async def get_collection_indexes(
        self, collection_id: str
    ) -> List[Tuple[Dict[str, str]]]:
        """Get index information for a specific collection.

        Args:
            collection_id (str): Collection identifier.

        Returns:
            List[Tuple[Dict[str, str]]]: List of tuples with alias dictionaries.
        """
        aliases = await self.load_aliases()
        main_alias = index_alias_by_collection_id(collection_id)
        return aliases.get(main_alias, [])

    @staticmethod
    def _parse_alias_response(
        response: Dict,
    ) -> Dict[str, List[Tuple[Dict[str, str]]]]:
        """Parse ES/OS get_alias response into structured data.

        Args:
            response: Raw response from indices.get_alias.

        Returns:
            Dict mapping main collection aliases to lists of alias dicts.
        """
        result: Dict[str, List[Tuple[Dict[str, str]]]] = {}

        for index_name, index_info in response.items():
            aliases = index_info.get("aliases", {})
            items_aliases = sorted(
                alias
                for alias in aliases.keys()
                if alias.startswith(ITEMS_INDEX_PREFIX)
            )

            if not items_aliases:
                continue

            main_alias = _find_main_alias(items_aliases)
            aliases_dict = _organize_aliases(items_aliases, main_alias)

            if aliases_dict:
                if main_alias not in result:
                    result[main_alias] = []
                result[main_alias].append((aliases_dict,))

        return result


class SyncIndexAliasLoader:
    """Synchronous loader for index aliases."""

    def __init__(self, client: Any):
        """Initialize the sync alias loader.

        Args:
            client: Sync search engine client instance.
        """
        self.client = client

    def load_aliases(self) -> Dict[str, List[Tuple[Dict[str, str]]]]:
        """Load index aliases from search engine.

        Returns:
            Dict[str, List[Tuple[Dict[str, str]]]]: Mapping of main collection aliases to their data.
        """
        response = self.client.indices.get_alias(index=f"{ITEMS_INDEX_PREFIX}*")
        return IndexAliasLoader._parse_alias_response(response)

    def get_collection_indexes(self, collection_id: str) -> List[Tuple[Dict[str, str]]]:
        """Get index information for a specific collection.

        Args:
            collection_id (str): Collection identifier.

        Returns:
            List[Tuple[Dict[str, str]]]: List of tuples with alias dictionaries.
        """
        aliases = self.load_aliases()
        main_alias = index_alias_by_collection_id(collection_id)
        return aliases.get(main_alias, [])


def _find_main_alias(aliases: List[str]) -> str:
    """Find the main collection alias (without temporal suffixes).

    Args:
        aliases: List of all aliases for an index.

    Returns:
        The main collection alias.
    """
    temporal_keywords = ["datetime", "start_datetime", "end_datetime"]

    for alias in aliases:
        if not any(keyword in alias for keyword in temporal_keywords):
            return alias

    return aliases[0]


def _organize_aliases(aliases: List[str], main_alias: str) -> Dict[str, str]:
    """Organize temporal aliases into a dictionary with type as key.

    Args:
        aliases: All aliases for the index.
        main_alias: The main collection alias.

    Returns:
        Dictionary with datetime types as keys and alias names as values.
    """
    aliases_dict = {}

    for alias in aliases:
        if alias == main_alias:
            continue

        if "start_datetime" in alias:
            aliases_dict["start_datetime"] = alias
        elif "end_datetime" in alias:
            aliases_dict["end_datetime"] = alias
        elif "datetime" in alias:
            aliases_dict["datetime"] = alias

    return aliases_dict
