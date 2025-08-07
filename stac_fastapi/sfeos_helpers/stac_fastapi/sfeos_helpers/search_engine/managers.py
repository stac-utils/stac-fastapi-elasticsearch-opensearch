"""Index management utilities."""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from fastapi import HTTPException, status

from stac_fastapi.sfeos_helpers.database import (
    extract_date,
    extract_first_date_from_index,
)

from .index_operations import IndexOperations

logger = logging.getLogger(__name__)


class IndexSizeManager:
    """Manages index size limits and operations."""

    def __init__(self, client: Any):
        """Initialize the index size manager.

        Args:
            client: Search engine client instance.
        """
        self.client = client
        self.max_size_gb = self._get_max_size_from_env()

    async def get_index_size_in_gb(self, index_name: str) -> float:
        """Get index size in gigabytes asynchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            float: Size of the index in gigabytes.
        """
        data = await self.client.indices.stats(index=index_name)
        return data["_all"]["primaries"]["store"]["size_in_bytes"] / 1e9

    async def is_index_oversized(self, index_name: str) -> bool:
        """Check if index exceeds size limit asynchronously.

        Args:
            index_name (str): Name of the index to check.

        Returns:
            bool: True if index exceeds size limit, False otherwise.
        """
        size_gb = await self.get_index_size_in_gb(index_name)
        is_oversized = size_gb > self.max_size_gb
        gb_milestone = int(size_gb)
        if gb_milestone > 0:
            logger.info(f"Index '{index_name}' size: {gb_milestone}GB")

        if is_oversized:
            logger.warning(
                f"Index '{index_name}' is oversized: {size_gb:.2f} GB "
                f"(limit: {self.max_size_gb} GB)"
            )

        return is_oversized

    @staticmethod
    def _get_max_size_from_env() -> float:
        """Get max size from environment variable with error handling.

        Returns:
            float: Maximum index size in GB.

        Raises:
            ValueError: If environment variable contains invalid value.
        """
        env_value = os.getenv("DATETIME_INDEX_MAX_SIZE_GB", "25")

        try:
            max_size = float(env_value)
            if max_size <= 0:
                raise ValueError(
                    f"DATETIME_INDEX_MAX_SIZE_GB must be positive, got: {max_size}"
                )
            return max_size
        except (ValueError, TypeError):
            error_msg = (
                f"Invalid value for DATETIME_INDEX_MAX_SIZE_GB environment variable: "
                f"'{env_value}'. Must be a positive number. Using default value 25.0 GB."
            )
            logger.warning(error_msg)

        return 25.0


class DatetimeIndexManager:
    """Manages datetime-based index operations."""

    def __init__(self, client: Any, index_operations: IndexOperations):
        """Initialize the datetime index manager.

        Args:
            client: Search engine client instance.
            index_operations (IndexOperations): Search engine adapter instance.
        """
        self.client = client
        self.index_operations = index_operations
        self.size_manager = IndexSizeManager(client)

    @staticmethod
    def validate_product_datetime(product: Dict[str, Any]) -> str:
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
        target_index = await self.index_operations.create_datetime_index(
            self.client, collection_id, extract_date(product_datetime)
        )
        logger.info(
            f"Successfully created index '{target_index}' for collection '{collection_id}'"
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
        old_alias = self.index_operations.create_alias_name(
            collection_id, str(end_date)
        )
        new_alias = self.index_operations.create_alias_name(
            collection_id, str(start_date)
        )
        await self.index_operations.change_alias_name(self.client, old_alias, new_alias)
        return new_alias

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
            await self.index_operations.update_index_alias(
                self.client, str(end_date), target_index
            )
            target_index = await self.index_operations.create_datetime_index(
                self.client, collection_id, str(end_date + timedelta(days=1))
            )

        return target_index
