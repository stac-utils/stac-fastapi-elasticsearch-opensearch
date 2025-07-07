"""Search engine adapters for different implementations."""

from abc import ABC, abstractmethod
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

from .types import SearchEngineType


class SearchEngineAdapter(ABC):
    """Abstract base class for search engine adapters."""

    @abstractmethod
    async def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index for the given collection.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        pass

    @abstractmethod
    async def create_datetime_index(
        self, client: Any, collection_id: str, start_date: str
    ) -> str:
        """Create a datetime-based index for the given collection.

        Args:
            client: Search engine client instance.
            collection_id (str): Collection identifier.
            start_date (str): Start date for the index.

        Returns:
            str: Created index alias name.
        """
        pass

    @abstractmethod
    def create_simple_index_sync(self, sync_client: Any, collection_id: str) -> str:
        """Create a simple index synchronously.

        Args:
            sync_client: Synchronous search engine client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        pass

    @abstractmethod
    def create_datetime_index_sync(
        self, sync_client: Any, collection_id: str, start_date: str
    ) -> str:
        """Create a datetime-based index synchronously.

        Args:
            sync_client: Synchronous search engine client instance.
            collection_id (str): Collection identifier.
            start_date (str): Start date for the index.

        Returns:
            str: Created index alias name.
        """
        pass

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
        index = ITEMS_INDEX_PREFIX + old_alias
        new_alias = f"{old_alias}-{end_date}"

        await client.indices.update_aliases(
            body={
                "actions": [
                    {"remove": {"index": index, "alias": old_alias}},
                    {"add": {"index": index, "alias": new_alias}},
                ]
            }
        )
        return new_alias

    @staticmethod
    def update_index_alias_sync(client: Any, end_date: str, old_alias: str) -> str:
        """Update index alias synchronously.

        Args:
            client: Search engine client instance.
            end_date (str): End date for the alias.
            old_alias (str): Current alias name.

        Returns:
            str: New alias name.
        """
        index = ITEMS_INDEX_PREFIX + old_alias
        new_alias = f"{old_alias}-{end_date}"

        client.indices.update_aliases(
            body={
                "actions": [
                    {"remove": {"index": index, "alias": old_alias}},
                    {"add": {"index": index, "alias": new_alias}},
                ]
            }
        )
        return new_alias

    @staticmethod
    def create_index_name(collection_id: str, start_date: str) -> str:
        """Create index name from collection ID and start date.

        Args:
            collection_id (str): Collection identifier.
            start_date (str): Start date for the index.

        Returns:
            str: Formatted index name.
        """
        cleaned = collection_id.translate(_ES_INDEX_NAME_UNSUPPORTED_CHARS_TABLE)
        return f"{ITEMS_INDEX_PREFIX}{cleaned.lower()}_{start_date}"


class ElasticsearchAdapter(SearchEngineAdapter):
    """Elasticsearch-specific adapter implementation."""

    async def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index for Elasticsearch.

        Args:
            client: Elasticsearch client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        index_name = f"{index_by_collection_id(collection_id)}-000001"
        alias_name = index_alias_by_collection_id(collection_id)

        await client.options(ignore_status=400).indices.create(
            index=index_name,
            body={"aliases": {alias_name: {}}},
        )
        return index_name

    async def create_datetime_index(
        self, client: Any, collection_id: str, start_date: str
    ) -> str:
        """Create a datetime-based index for Elasticsearch.

        Args:
            client: Elasticsearch client instance.
            collection_id (str): Collection identifier.
            start_date (str): Start date for the index.

        Returns:
            str: Created index alias name.
        """
        index_name = self.create_index_name(collection_id, start_date)
        alias_name = index_name.removeprefix(ITEMS_INDEX_PREFIX)
        collection_alias = index_alias_by_collection_id(collection_id)

        await client.options(ignore_status=400).indices.create(
            index=index_name,
            body={"aliases": {collection_alias: {}, alias_name: {}}},
        )
        return alias_name

    def create_simple_index_sync(self, sync_client: Any, collection_id: str) -> str:
        """Create a simple index for Elasticsearch synchronously.

        Args:
            sync_client: Synchronous Elasticsearch client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        index_name = f"{index_by_collection_id(collection_id)}-000001"
        alias_name = index_alias_by_collection_id(collection_id)

        sync_client.options(ignore_status=400).indices.create(
            index=index_name,
            body={"aliases": {alias_name: {}}},
        )
        return index_name

    def create_datetime_index_sync(
        self, sync_client: Any, collection_id: str, start_date: str
    ) -> str:
        """Create a datetime-based index for Elasticsearch synchronously.

        Args:
            sync_client: Synchronous Elasticsearch client instance.
            collection_id (str): Collection identifier.
            start_date (str): Start date for the index.

        Returns:
            str: Created index alias name.
        """
        index_name = self.create_index_name(collection_id, start_date)
        alias_name = index_name.removeprefix(ITEMS_INDEX_PREFIX)
        collection_alias = index_alias_by_collection_id(collection_id)

        sync_client.options(ignore_status=400).indices.create(
            index=index_name,
            body={"aliases": {collection_alias: {}, alias_name: {}}},
        )
        return alias_name


class OpenSearchAdapter(SearchEngineAdapter):
    """OpenSearch-specific adapter implementation."""

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

    async def create_simple_index(self, client: Any, collection_id: str) -> str:
        """Create a simple index for OpenSearch.

        Args:
            client: OpenSearch client instance.
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
        """Create a datetime-based index for OpenSearch.

        Args:
            client: OpenSearch client instance.
            collection_id (str): Collection identifier.
            start_date (str): Start date for the index.

        Returns:
            str: Created index alias name.
        """
        index_name = self.create_index_name(collection_id, start_date)
        alias_name = index_name.removeprefix(ITEMS_INDEX_PREFIX)
        collection_alias = index_alias_by_collection_id(collection_id)

        await client.indices.create(
            index=index_name,
            body=self._create_index_body({collection_alias: {}, alias_name: {}}),
            params={"ignore": [400]},
        )
        return alias_name

    def create_simple_index_sync(self, sync_client: Any, collection_id: str) -> str:
        """Create a simple index for OpenSearch synchronously.

        Args:
            sync_client: Synchronous OpenSearch client instance.
            collection_id (str): Collection identifier.

        Returns:
            str: Created index name.
        """
        index_name = f"{index_by_collection_id(collection_id)}-000001"
        alias_name = index_alias_by_collection_id(collection_id)

        sync_client.indices.create(
            index=index_name,
            body=self._create_index_body({alias_name: {}}),
            params={"ignore": [400]},
        )
        return index_name

    def create_datetime_index_sync(
        self, sync_client: Any, collection_id: str, start_date: str
    ) -> str:
        """Create a datetime-based index for OpenSearch synchronously.

        Args:
            sync_client: Synchronous OpenSearch client instance.
            collection_id (str): Collection identifier.
            start_date (str): Start date for the index.

        Returns:
            str: Created index alias name.
        """
        index_name = self.create_index_name(collection_id, start_date)
        alias_name = index_name.removeprefix(ITEMS_INDEX_PREFIX)
        collection_alias = index_alias_by_collection_id(collection_id)

        sync_client.indices.create(
            index=index_name,
            body=self._create_index_body({collection_alias: {}, alias_name: {}}),
            params={"ignore": [400]},
        )
        return alias_name


class SearchEngineAdapterFactory:
    """Factory for creating search engine adapters."""

    @staticmethod
    def create_adapter(engine_type: SearchEngineType) -> SearchEngineAdapter:
        """Create appropriate adapter based on engine type.

        Args:
            engine_type (SearchEngineType): Type of search engine.

        Returns:
            SearchEngineAdapter: Adapter instance for the specified engine type.
        """
        adapters = {
            SearchEngineType.ELASTICSEARCH: ElasticsearchAdapter,
            SearchEngineType.OPENSEARCH: OpenSearchAdapter,
        }
        return adapters[engine_type]()

    @staticmethod
    def detect_engine_type(client: Any) -> SearchEngineType:
        """Detect engine type from client class name.

        Args:
            client: Search engine client instance.

        Returns:
            SearchEngineType: Detected engine type.
        """
        return (
            SearchEngineType.OPENSEARCH
            if "opensearch" in str(client.__class__).lower()
            else SearchEngineType.ELASTICSEARCH
        )
