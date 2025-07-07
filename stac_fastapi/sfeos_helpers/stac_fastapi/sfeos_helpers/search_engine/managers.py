"""Index management utilities."""

import os
from datetime import datetime, timedelta
from typing import Any, Dict

from fastapi import HTTPException, status

from stac_fastapi.sfeos_helpers.database import (
    extract_date,
    extract_first_date_from_index,
)

from .adapters import SearchEngineAdapter


class IndexSizeManager:
    """Manages index size limits and operations."""

    def __init__(self, client: Any):
        """Initialize the index size manager.

        Args:
            client: Search engine client instance.
        """
        self.client = client
        self.max_size_gb = float(os.getenv("DATETIME_INDEX_MAX_SIZE_GB", "25"))

    async def get_index_size_in_gb(self, index_name: str) -> float:
        """Get index size in gigabytes asynchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            float: Size of the index in gigabytes.
        """
        data = await self.client.indices.stats(index=index_name)
        return data["_all"]["primaries"]["store"]["size_in_bytes"] / 1e9

    def get_index_size_in_gb_sync(self, index_name: str) -> float:
        """Get index size in gigabytes synchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            float: Size of the index in gigabytes.
        """
        data = self.client.indices.stats(index=index_name)
        return data["_all"]["primaries"]["store"]["size_in_bytes"] / 1e9

    async def is_index_oversized(self, index_name: str) -> bool:
        """Check if index exceeds size limit asynchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            bool: True if index exceeds size limit, False otherwise.
        """
        size_gb = await self.get_index_size_in_gb(index_name)
        return size_gb > self.max_size_gb

    def is_index_oversized_sync(self, index_name: str) -> bool:
        """Check if index exceeds size limit synchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            bool: True if index exceeds size limit, False otherwise.
        """
        size_gb = self.get_index_size_in_gb_sync(index_name)
        return size_gb > self.max_size_gb


class DatetimeIndexManager:
    """Manages datetime-based index operations."""

    def __init__(self, client: Any, search_adapter: SearchEngineAdapter):
        """Initialize the datetime index manager.

        Args:
            client: Search engine client instance.
            search_adapter (SearchEngineAdapter): Search engine adapter instance.
        """
        self.client = client
        self.search_adapter = search_adapter
        self.size_manager = IndexSizeManager(client)

    @staticmethod
    def _validate_product_datetime(product: Dict[str, Any]) -> str:
        """Validate and extract datetime from product.

        Args:
            product (Dict[str, Any]): Product data containing datetime information.

        Returns:
            str: Validated product datetime.

        Raises:
            HTTPException: If product datetime is missing or invalid.
        """
        product_datetime = product["properties"]["datetime"]
        if not product_datetime:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Product datetime is required for indexing",
            )
        return product_datetime

    async def handle_new_collection(
        self, collection_id: str, product_datetime: str
    ) -> str:
        """Handle index creation for new collection asynchronously.

        Args:
            collection_id (str): Collection identifier.
            product_datetime (str): Product datetime for index naming.

        Returns:
            str: Created index name.
        """
        target_index = await self.search_adapter.create_datetime_index(
            self.client, collection_id, extract_date(product_datetime)
        )
        return target_index

    def handle_new_collection_sync(
        self, collection_id: str, product_datetime: str
    ) -> str:
        """Handle index creation for new collection synchronously.

        Args:
            collection_id (str): Collection identifier.
            product_datetime (str): Product datetime for index naming.

        Returns:
            str: Created index name.
        """
        target_index = self.search_adapter.create_datetime_index_sync(
            self.client, collection_id, extract_date(product_datetime)
        )
        return target_index

    async def handle_early_date(
        self, collection_id: str, start_date: datetime, end_date: datetime
    ) -> str:
        """Handle product with date earlier than existing indexes asynchronously.

        Args:
            collection_id (str): Collection identifier.
            start_date (datetime): Start date for the new index.
            end_date (datetime): End date for alias update.

        Returns:
            str: Updated alias name.
        """
        target_index = await self.search_adapter.create_datetime_index(
            self.client, collection_id, str(start_date)
        )
        alias = await self.search_adapter.update_index_alias(
            self.client, str(end_date - timedelta(days=1)), target_index
        )
        return alias

    def handle_early_date_sync(
        self, collection_id: str, start_date: datetime, end_date: datetime
    ) -> str:
        """Handle product with date earlier than existing indexes synchronously.

        Args:
            collection_id (str): Collection identifier.
            start_date (datetime): Start date for the new index.
            end_date (datetime): End date for alias update.

        Returns:
            str: Updated alias name.
        """
        target_index = self.search_adapter.create_datetime_index_sync(
            self.client, collection_id, str(start_date)
        )
        alias = self.search_adapter.update_index_alias_sync(
            self.client, str(end_date - timedelta(days=1)), target_index
        )
        return alias

    async def handle_oversized_index(
        self, collection_id: str, target_index: str, product_datetime: str
    ) -> str:
        """Handle index that exceeds size limit asynchronously.

        Args:
            collection_id (str): Collection identifier.
            target_index (str): Current target index name.
            product_datetime (str): Product datetime for new index.

        Returns:
            str: New or updated index name.
        """
        end_date = extract_date(product_datetime)
        latest_index_start = extract_first_date_from_index(target_index)

        if end_date != latest_index_start:
            await self.search_adapter.update_index_alias(
                self.client, str(end_date), target_index
            )
            target_index = await self.search_adapter.create_datetime_index(
                self.client, collection_id, str(end_date + timedelta(days=1))
            )

        return target_index

    def handle_oversized_index_sync(
        self, collection_id: str, target_index: str, product_datetime: str
    ) -> str:
        """Handle index that exceeds size limit synchronously.

        Args:
            collection_id (str): Collection identifier.
            target_index (str): Current target index name.
            product_datetime (str): Product datetime for new index.

        Returns:
            str: New or updated index name.
        """
        end_date = extract_date(product_datetime)
        latest_index_start = extract_first_date_from_index(target_index)

        if end_date != latest_index_start:
            self.search_adapter.update_index_alias_sync(
                self.client, str(end_date), target_index
            )
            target_index = self.search_adapter.create_datetime_index_sync(
                self.client, collection_id, str(end_date + timedelta(days=1))
            )

        return target_index
