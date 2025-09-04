"""Search engine adapters for different implementations."""

import uuid
from typing import Any, Dict

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
        self, client: Any, collection_id: str, start_date: str
    ) -> str:
        """Create a datetime-based index for the given collection.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.
            start_date (str): Start date for the alias.

        Returns:
            str: Created index alias name.
        """
        index_name = self.create_index_name(collection_id)
        alias_name = self.create_alias_name(collection_id, start_date)
        collection_alias = index_alias_by_collection_id(collection_id)
        await client.indices.create(
            index=index_name,
            body=self._create_index_body({collection_alias: {}, alias_name: {}}),
        )
        return alias_name

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
    async def change_alias_name(client: Any, old_alias: str, new_alias: str) -> None:
        """Change alias name from old to new.

        Args:
            client: Search engine client instance.
            old_alias (str): Current alias name.
            new_alias (str): New alias name.

        Returns:
            None
        """
        aliases_info = await client.indices.get_alias(name=old_alias)
        actions = []

        for index_name in aliases_info.keys():
            actions.append({"remove": {"index": index_name, "alias": old_alias}})
            actions.append({"add": {"index": index_name, "alias": new_alias}})
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
    def create_alias_name(collection_id: str, start_date: str) -> str:
        """Create index name from collection ID and uuid4.

        Args:
            collection_id (str): Collection identifier.
            start_date (str): Start date for the alias.

        Returns:
            str: Alias name with initial date.
        """
        cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
        return f"{ITEMS_INDEX_PREFIX}{cleaned.lower()}_{start_date}"

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

    @staticmethod
    async def find_latest_item_in_index(client: Any, index_name: str) -> dict[str, Any]:
        """Find the latest item date in the specified index.

        Args:
            client: Search engine client instance.
            index_name (str): Name of the index to query.

        Returns:
            datetime: Date of the latest item in the index.
        """
        query = {
            "size": 1,
            "sort": [{"properties.datetime": {"order": "desc"}}],
            "_source": ["properties.datetime"],
        }

        response = await client.search(index=index_name, body=query)
        return response["hits"]["hits"][0]
