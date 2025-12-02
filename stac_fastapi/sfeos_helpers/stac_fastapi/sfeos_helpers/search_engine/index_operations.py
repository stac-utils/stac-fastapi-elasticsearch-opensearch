"""Search engine adapters for different implementations."""

import uuid
from typing import Any, Dict, List, Literal

from stac_fastapi.core.utilities import get_bool_env
from stac_fastapi.sfeos_helpers.database import (
    index_alias_by_collection_id,
    index_by_collection_id,
)
from stac_fastapi.sfeos_helpers.mappings import (
    _ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE,
    ES_ITEMS_MAPPINGS,
    ES_ITEMS_SETTINGS,
    ITEMS_INDEX_PREFIX,
)


class IndexOperations:
    """Base class for search engine adapters with common implementations."""

    @property
    def use_datetime(self) -> bool:
        """Get USE_DATETIME setting dynamically."""
        return get_bool_env("USE_DATETIME", default=True)

    @property
    def primary_datetime_name(self) -> str:
        """Get primary datetime field name based on current USE_DATETIME setting."""
        return "datetime" if self.use_datetime else "start_datetime"

    async def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index for the given collection.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        index_name = f"{index_by_collection_id(collection_id)}-000001"
        alias_name = index_alias_by_collection_id(collection_id)

        await client.indices.create(
            index=index_name,
            body=self._create_index_body({alias_name: {}}),
            params={"ignore": [400]},
        )
        return index_name

    async def create_datetime_index(
        self,
        client: Any,
        collection_id: str,
        start_datetime: str | None,
        datetime: str | None,
        end_datetime: str | None,
    ) -> str:
        """Create a datetime-based index for the given collection.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.
            start_datetime (str | None): Start datetime for the index alias.
            datetime (str | None): Datetime for the datetime alias.
            end_datetime (str | None): End datetime for the index alias.

        Returns:
            str: Created datetime alias name.
        """
        index_name = self.create_index_name(collection_id)
        collection_alias = index_alias_by_collection_id(collection_id)

        aliases: Dict[str, Any] = {
            collection_alias: {},
        }

        if start_datetime:
            alias_start_date = self.create_alias_name(
                collection_id, "start_datetime", start_datetime
            )
            alias_end_date = self.create_alias_name(
                collection_id, "end_datetime", end_datetime
            )
            aliases[alias_start_date] = {}
            aliases[alias_end_date] = {}
            created_alias = alias_start_date
        else:
            created_alias = self.create_alias_name(collection_id, "datetime", datetime)
            aliases[created_alias] = {}

        await client.indices.create(
            index=index_name,
            body=self._create_index_body(aliases),
        )
        return created_alias

    @staticmethod
    async def update_index_alias(client: Any, end_date: str, old_alias: str) -> str:
        """Update index alias with new end date.

        Args:
            client: Search engine client instance.
            end_date (str): End date for the alias.
            old_alias (str): Current alias name.

        Returns:
            str: New alias name.
        """
        new_alias = f"{old_alias}-{end_date}"
        aliases_info = await client.indices.get_alias(name=old_alias)
        actions = []

        for index_name in aliases_info.keys():
            actions.append({"remove": {"index": index_name, "alias": old_alias}})
            actions.append({"add": {"index": index_name, "alias": new_alias}})

        await client.indices.update_aliases(body={"actions": actions})
        return new_alias

    @staticmethod
    async def change_alias_name(
        client: Any,
        old_start_datetime_alias: str,
        aliases_to_change: List[str],
        aliases_to_create: List[str],
    ) -> None:
        """Change alias names by removing old aliases and adding new ones.

        Args:
            client: Search engine client instance.
            old_start_datetime_alias (str): Current start_datetime alias name to identify the index.
            aliases_to_change (List[str]): List of old alias names to remove.
            aliases_to_create (List[str]): List of new alias names to add.

        Returns:
            None
        """
        aliases_info = await client.indices.get_alias(name=old_start_datetime_alias)
        index_name = list(aliases_info.keys())[0]
        actions = []

        for new_alias in aliases_to_create:
            actions.append({"add": {"index": index_name, "alias": new_alias}})

        for old_alias in aliases_to_change:
            actions.append({"remove": {"index": index_name, "alias": old_alias}})

        await client.indices.update_aliases(body={"actions": actions})

    @staticmethod
    def create_index_name(collection_id: str) -> str:
        """Create index name from collection ID and uuid4.

        Args:
            collection_id (str): Collection identifier.

        Returns:
            str: Formatted index name.
        """
        cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
        return f"{ITEMS_INDEX_PREFIX}{cleaned.lower()}_{uuid.uuid4()}"

    @staticmethod
    def create_alias_name(
        collection_id: str,
        name: Literal["start_datetime", "datetime", "end_datetime"],
        start_date: str,
    ) -> str:
        """Create alias name from collection ID and date.

        Args:
            collection_id (str): Collection identifier.
            name (Literal["start_datetime", "datetime", "end_datetime"]): Type of alias to create.
            start_date (str): Date value for the alias.

        Returns:
            str: Formatted alias name with prefix, type, collection ID, and date.
        """
        cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
        return f"{ITEMS_INDEX_PREFIX}{name}_{cleaned.lower()}_{start_date}"

    @staticmethod
    def _create_index_body(aliases: Dict[str, Dict]) -> Dict[str, Any]:
        """Create index body with common settings.

        Args:
            aliases (Dict[str, Dict]): Aliases configuration.

        Returns:
            Dict[str, Any]: Index body configuration.
        """
        return {
            "aliases": aliases,
            "mappings": ES_ITEMS_MAPPINGS,
            "settings": ES_ITEMS_SETTINGS,
        }

    async def find_latest_item_in_index(
        self, client: Any, index_name: str
    ) -> dict[str, Any]:
        """Find the latest item in the specified index.

        Args:
            client: Search engine client instance.
            index_name (str): Name of the index to query.

        Returns:
            dict[str, Any]: Latest item document from the index with metadata.
        """
        query = {
            "size": 1,
            "sort": [{f"properties.{self.primary_datetime_name}": {"order": "desc"}}],
            "_source": [
                "properties.start_datetime",
                "properties.datetime",
                "properties.end_datetime",
            ],
        }

        response = await client.search(index=index_name, body=query)
        return response["hits"]["hits"][0]
